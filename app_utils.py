
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime, timedelta, date
from core.time_utils import (
    MINUTES_PER_HOUR, MINUTES_PER_DAY, LOCAL_TZ,
    REGULAR_HOURS_LIMIT, OVERTIME_125_LIMIT,
    FRIDAY, SATURDAY,
    span_minutes, to_local_date, _get_shabbat_boundaries,
)
from utils.utils import overlap_minutes, to_gematria, month_range_ts, merge_intervals, find_uncovered_intervals
from convertdate import hebrew
import logging
import psycopg2.extras

from core.history import (
    get_apartment_type_for_month, get_person_status_for_month,
    get_all_shift_rates_for_month
)
from core.sick_days import _identify_sick_day_sequences, get_sick_payment_rate

# =============================================================================
# Import constants from single source of truth (core/constants.py)
# =============================================================================
from core.constants import (
    # Shift IDs
    FRIDAY_SHIFT_ID,
    SHABBAT_SHIFT_ID,
    NIGHT_SHIFT_ID,
    TAGBUR_FRIDAY_SHIFT_ID,
    TAGBUR_SHABBAT_SHIFT_ID,
    HOSPITAL_ESCORT_SHIFT_ID,
    # Shift ID groups
    SHABBAT_SHIFT_IDS,
    TAGBUR_SHIFT_IDS,
    # Apartment types
    REGULAR_APT_TYPE,
    THERAPEUTIC_APT_TYPE,
    # Standby constants
    MAX_CANCELLED_STANDBY_DEDUCTION,
    STANDBY_CANCEL_OVERLAP_THRESHOLD,
    DEFAULT_STANDBY_RATE,
    # Break/Chain constants
    BREAK_THRESHOLD_MINUTES,
    # Night shift overtime thresholds
    NIGHT_REGULAR_HOURS_LIMIT,
    NIGHT_OVERTIME_125_LIMIT,
    # Helper functions
    is_tagbur_shift,
    is_night_shift,
    is_shabbat_shift,
    is_hospital_escort_shift,
    is_implicit_tagbur,
    qualifies_as_night_shift,
    calculate_night_hours_in_segment,
    # Night hours threshold
    NIGHT_HOURS_THRESHOLD,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Data Access Functions (moved from core/logic.py to fix circular dependency)
# =============================================================================

def get_standby_rate(conn, segment_id: int, apartment_type_id: int | None, is_married: bool, year: int = None, month: int = None) -> float:
    """
    Get standby rate from standby_rates table.
    Priority: specific apartment_type (priority=10) > general (priority=0)
    If year/month provided, checks historical rates first.
    """
    marital_status = "married" if is_married else "single"

    # If year/month provided, try historical rates first
    if year is not None and month is not None:
        from core.history import get_standby_rate_for_month
        historical_amount = get_standby_rate_for_month(
            conn, segment_id, apartment_type_id, marital_status, year, month
        )
        if historical_amount is not None:
            return float(historical_amount) / 100

    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # First try specific rate for apartment type (priority=10)
    if apartment_type_id is not None:
        cursor.execute("""
            SELECT amount FROM standby_rates
            WHERE segment_id = %s AND apartment_type_id = %s AND marital_status = %s AND priority = 10
            LIMIT 1
        """, (segment_id, apartment_type_id, marital_status))
        row = cursor.fetchone()
        if row:
            cursor.close()
            return float(row["amount"]) / 100

    # Fallback to general rate (priority=0)
    cursor.execute("""
        SELECT amount FROM standby_rates
        WHERE segment_id = %s AND apartment_type_id IS NULL AND marital_status = %s AND priority = 0
        LIMIT 1
    """, (segment_id, marital_status))
    row = cursor.fetchone()
    cursor.close()

    if row:
        return float(row["amount"]) / 100

    return DEFAULT_STANDBY_RATE


# =============================================================================
# Wage Rate Calculation (moved from core/wage_calculator.py)
# =============================================================================

def calculate_wage_rate(
    minutes_in_chain: int,
    is_shabbat: bool,
    is_night_shift: bool = False
) -> str:
    """
    Determine the wage rate label based on hours worked in chain and Shabbat status.

    Args:
        minutes_in_chain: Total minutes worked so far in the current chain
        is_shabbat: Whether this minute falls within Shabbat hours
        is_night_shift: Whether this is a night shift (uses 7-hour day instead of 8)

    Returns:
        Rate label: "100%", "125%", "150%", "175%", or "200%"
    """
    # Use night shift thresholds if applicable (7 hours instead of 8)
    regular_limit = NIGHT_REGULAR_HOURS_LIMIT if is_night_shift else REGULAR_HOURS_LIMIT
    overtime_limit = NIGHT_OVERTIME_125_LIMIT if is_night_shift else OVERTIME_125_LIMIT

    if minutes_in_chain <= regular_limit:
        return "150%" if is_shabbat else "100%"
    elif minutes_in_chain <= overtime_limit:
        return "175%" if is_shabbat else "125%"
    else:
        return "200%" if is_shabbat else "150%"


# =============================================================================
# Chain Wage Calculation (moved from core/wage_calculator.py)
# =============================================================================

def _calculate_chain_wages(
    chain_segments: List[Tuple[int, int, int, date]],
    shabbat_cache: Dict[str, Dict[str, str]],
    minutes_offset: int = 0,
    is_night_shift: bool = False
) -> Dict[str, Any]:
    """
    חישוב שכר לרצף עבודה (chain) בשיטת בלוקים.

    במקום לעבור דקה-דקה, מחשב בלוקים לפי גבולות:
    - 480 דקות (מעבר 100% -> 125%) - או 420 למשמרת לילה
    - 600 דקות (מעבר 125% -> 150%) - או 540 למשמרת לילה
    - גבולות שבת (כניסה/יציאה)

    Args:
        chain_segments: List of (start_min, end_min, shift_id, actual_date) tuples
        shabbat_cache: Cache of Shabbat times
        minutes_offset: Minutes already worked in this chain (from previous day's carryover)
        is_night_shift: Whether this is a night shift (uses 7-hour day instead of 8)

    Returns:
        Dict with calc100, calc125, calc150, calc175, calc200,
        calc150_shabbat, calc150_overtime, calc150_shabbat_100, calc150_shabbat_50,
        and segments_detail - list of (start_min, end_min, label, is_shabbat) for display
    """
    result = {
        "calc100": 0, "calc125": 0, "calc150": 0, "calc175": 0, "calc200": 0,
        "calc150_shabbat": 0, "calc150_overtime": 0,
        "calc150_shabbat_100": 0, "calc150_shabbat_50": 0,
        "segments_detail": []  # For display: list of (start_min, end_min, label, is_shabbat)
    }

    if not chain_segments:
        return result

    # Flatten all segments into a list of (abs_start, abs_end, actual_date) in continuous minutes
    # and calculate total chain minutes
    total_chain_minutes = 0
    flat_segments = []

    for seg_start, seg_end, seg_shift_id, seg_actual_date in chain_segments:
        flat_segments.append((seg_start, seg_end, seg_actual_date))
        total_chain_minutes += (seg_end - seg_start)

    # Process in blocks based on overtime thresholds
    # Use night shift thresholds if applicable (7 hours instead of 8)
    regular_limit = NIGHT_REGULAR_HOURS_LIMIT if is_night_shift else REGULAR_HOURS_LIMIT
    overtime_limit = NIGHT_OVERTIME_125_LIMIT if is_night_shift else OVERTIME_125_LIMIT
    # Start from offset if this chain continues from previous day
    minutes_processed = minutes_offset

    for seg_start, seg_end, seg_actual_date in flat_segments:
        seg_duration = seg_end - seg_start
        seg_offset = 0

        # Get Shabbat/Holiday boundaries for THIS segment's actual date
        # הפונקציה מחזירה (-1, -1) אם היום אינו שבת/חג/ערב שבת/ערב חג
        seg_weekday = seg_actual_date.weekday()
        shabbat_enter, shabbat_exit = _get_shabbat_boundaries(seg_actual_date, shabbat_cache)
        seg_is_shabbat_or_holiday = (shabbat_enter > 0)

        # בדיקה אם היום הוא שבת/חג (לא ערב שבת/חג)
        # שבת: weekday == SATURDAY
        # חג: יום שבו כל השעות הן שעות חג (לא ערב חג)
        # ערב חג/שישי: היום שלפני החג/שבת
        # בדיקה אם זה ערב חג: הכניסה היא ב"היום" אבל החג מתחיל מחר
        # ערב חג = יום שיש לו enter וזה לא שבת (יום שישי או ערב חג)
        # חג = יום שבת, או יום עם exit, או יום ביניים בחג
        seg_is_eve = False
        if seg_is_shabbat_or_holiday:
            if seg_weekday == FRIDAY:
                # יום שישי = תמיד ערב שבת
                seg_is_eve = True
            elif seg_weekday == SATURDAY:
                # שבת = תמיד יום קודש
                seg_is_eve = False
            else:
                # ימי חול - צריך לבדוק אם זה ערב חג או חג
                # ערב חג = היום שבו מדליקים נרות (כניסה היא היום, החג מחר)
                # נבדוק אם מחר יש רשומת חג שהכניסה שלה מכוונת להיום
                from core.time_utils import _find_holiday_record_for_date
                holiday_date, holiday_info = _find_holiday_record_for_date(seg_actual_date, shabbat_cache)
                if holiday_date:
                    days_to_holiday_record = (holiday_date - seg_actual_date).days
                    # ערב חג = היום שבו מדליקים נרות (enter)
                    # חג = כל הימים אחרי הדלקת הנרות עד ה-exit
                    if days_to_holiday_record == 0:
                        # הרשומה היא היום - זה היום האחרון של החג
                        seg_is_eve = False
                    elif days_to_holiday_record == 1:
                        # הרשומה היא מחר
                        # נבדוק אם יש רשומה להיום עצמו - אם יש, זה יום חג
                        today_str = seg_actual_date.strftime("%Y-%m-%d")
                        today_info = shabbat_cache.get(today_str)
                        if today_info:
                            seg_is_eve = False
                        else:
                            # אין רשומה להיום
                            # נבדוק אם אתמול היה חלק מאותו חג (יש לו shabbat_boundaries חיוביים)
                            yesterday = seg_actual_date - timedelta(days=1)
                            yesterday_enter, _ = _get_shabbat_boundaries(yesterday, shabbat_cache)
                            if yesterday_enter > 0:
                                # אתמול היה חלק מחג/שבת - היום הוא יום חג
                                seg_is_eve = False
                            else:
                                # אתמול לא היה חג - היום הוא ערב חג
                                seg_is_eve = True
                    else:
                        # מרחק 2+ ימים
                        # נבדוק אם אתמול היה חלק מאותו חג
                        yesterday = seg_actual_date - timedelta(days=1)
                        yesterday_enter, _ = _get_shabbat_boundaries(yesterday, shabbat_cache)
                        if yesterday_enter > 0:
                            # אתמול היה חלק מחג/שבת - היום הוא יום ביניים (חג)
                            seg_is_eve = False
                        else:
                            # אתמול לא היה חג - היום הוא ערב
                            seg_is_eve = True

        seg_is_holy_day = seg_is_shabbat_or_holiday and not seg_is_eve

        while seg_offset < seg_duration:
            current_abs_minute = seg_start + seg_offset
            current_chain_minute = minutes_processed + 1  # 1-based for wage calculation

            # Determine which overtime tier we're in
            if current_chain_minute <= regular_limit:
                tier_end = regular_limit
                base_rate = "100%"
                shabbat_rate = "150%"
            elif current_chain_minute <= overtime_limit:
                tier_end = overtime_limit
                base_rate = "125%"
                shabbat_rate = "175%"
            else:
                tier_end = float('inf')
                base_rate = "150%"
                shabbat_rate = "200%"

            # How many minutes until we hit the next tier?
            minutes_until_tier_change = tier_end - minutes_processed

            # How many minutes left in this segment?
            minutes_left_in_seg = seg_duration - seg_offset

            # Take the minimum
            block_size = min(minutes_until_tier_change, minutes_left_in_seg)

            # Now check Shabbat/Holiday boundaries within this block
            if seg_is_shabbat_or_holiday:
                block_abs_start = current_abs_minute
                block_abs_end = current_abs_minute + block_size

                # נרמול זמנים - זמנים מעל 1440 הם בבוקר (אחרי חצות)
                # לדוגמה: 1830 = 06:30 בבוקר של היום הבא
                actual_block_start = block_abs_start % MINUTES_PER_DAY
                actual_block_end = block_abs_end % MINUTES_PER_DAY
                # אם הסגמנט חוצה חצות, end יהיה קטן מ-start
                if actual_block_end <= actual_block_start and block_abs_end > block_abs_start:
                    actual_block_end = block_abs_end % MINUTES_PER_DAY or MINUTES_PER_DAY

                # Adjust for day offset (if segment crosses midnight)
                # day_offset מייצג את המרחק מחצות יום שישי
                # - יום שישי: offset = 0 (כל הזמנים ביום שישי הם לפני או אחרי כניסת שבת)
                # - יום שבת: offset = 1440 (כל הזמנים הם ביחס לחצות שישי + 1440)
                #
                # חשוב: משתמשים ב-actual_block_start/end (הזמן האמיתי ביום 0-1440)
                # ולא ב-block_abs_start/end (הזמן המנורמל שיכול להיות 1440+)
                # כי אנחנו רוצים לדעת מה השעה בפועל ביום הספציפי
                day_offset_start = 0
                day_offset_end = 0
                if seg_is_holy_day:
                    # ביום שבת/חג, כל הזמנים הם ביחס לחצות הערב + 1440
                    # זמנים בבוקר (00:00-08:00) עדיין שייכים לשבת/חג
                    # הבדיקה אם זה אחרי צאת שבת/חג תתבצע מול shabbat_exit
                    day_offset_start = MINUTES_PER_DAY
                    day_offset_end = MINUTES_PER_DAY
                # עבור ערב שבת/חג - לא צריך offset, הזמנים כבר ביחס לחצות הערב

                abs_start_from_fri = actual_block_start + day_offset_start
                abs_end_from_fri = actual_block_end + day_offset_end

                # Helper to add segment detail
                def add_segment_detail(start_min, end_min, rate_label, is_shabbat):
                    result["segments_detail"].append((start_min, end_min, rate_label, is_shabbat))

                # Split block at Shabbat boundaries
                # Case 1: Entirely before Shabbat
                if abs_end_from_fri <= shabbat_enter:
                    if base_rate == "100%":
                        result["calc100"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "100%", False)
                    elif base_rate == "125%":
                        result["calc125"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "125%", False)
                    else:
                        result["calc150"] += block_size
                        result["calc150_overtime"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "150%", False)

                # Case 2: Entirely during Shabbat
                elif abs_start_from_fri >= shabbat_enter and abs_end_from_fri <= shabbat_exit:
                    if shabbat_rate == "150%":
                        result["calc150"] += block_size
                        result["calc150_shabbat"] += block_size
                        result["calc150_shabbat_100"] += block_size
                        result["calc150_shabbat_50"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "150% שבת", True)
                    elif shabbat_rate == "175%":
                        result["calc175"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "175% שבת", True)
                    else:
                        result["calc200"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "200% שבת", True)

                # Case 3: Entirely after Shabbat
                elif abs_start_from_fri >= shabbat_exit:
                    if base_rate == "100%":
                        result["calc100"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "100%", False)
                    elif base_rate == "125%":
                        result["calc125"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "125%", False)
                    else:
                        result["calc150"] += block_size
                        result["calc150_overtime"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "150%", False)

                # Case 4: Block crosses Shabbat start
                elif abs_start_from_fri < shabbat_enter < abs_end_from_fri:
                    before_shabbat = shabbat_enter - abs_start_from_fri
                    during_shabbat = abs_end_from_fri - shabbat_enter

                    # Before Shabbat part
                    if base_rate == "100%":
                        result["calc100"] += before_shabbat
                        add_segment_detail(block_abs_start, block_abs_start + before_shabbat, "100%", False)
                    elif base_rate == "125%":
                        result["calc125"] += before_shabbat
                        add_segment_detail(block_abs_start, block_abs_start + before_shabbat, "125%", False)
                    else:
                        result["calc150"] += before_shabbat
                        result["calc150_overtime"] += before_shabbat
                        add_segment_detail(block_abs_start, block_abs_start + before_shabbat, "150%", False)

                    # During Shabbat part
                    shabbat_start_abs = block_abs_start + before_shabbat
                    if shabbat_rate == "150%":
                        result["calc150"] += during_shabbat
                        result["calc150_shabbat"] += during_shabbat
                        result["calc150_shabbat_100"] += during_shabbat
                        result["calc150_shabbat_50"] += during_shabbat
                        add_segment_detail(shabbat_start_abs, block_abs_end, "150% שבת", True)
                    elif shabbat_rate == "175%":
                        result["calc175"] += during_shabbat
                        add_segment_detail(shabbat_start_abs, block_abs_end, "175% שבת", True)
                    else:
                        result["calc200"] += during_shabbat
                        add_segment_detail(shabbat_start_abs, block_abs_end, "200% שבת", True)

                # Case 5: Block crosses Shabbat end
                elif abs_start_from_fri < shabbat_exit < abs_end_from_fri:
                    during_shabbat = shabbat_exit - abs_start_from_fri
                    after_shabbat = abs_end_from_fri - shabbat_exit

                    # During Shabbat part
                    if shabbat_rate == "150%":
                        result["calc150"] += during_shabbat
                        result["calc150_shabbat"] += during_shabbat
                        result["calc150_shabbat_100"] += during_shabbat
                        result["calc150_shabbat_50"] += during_shabbat
                        add_segment_detail(block_abs_start, block_abs_start + during_shabbat, "150% שבת", True)
                    elif shabbat_rate == "175%":
                        result["calc175"] += during_shabbat
                        add_segment_detail(block_abs_start, block_abs_start + during_shabbat, "175% שבת", True)
                    else:
                        result["calc200"] += during_shabbat
                        add_segment_detail(block_abs_start, block_abs_start + during_shabbat, "200% שבת", True)

                    # After Shabbat part
                    after_start_abs = block_abs_start + during_shabbat
                    if base_rate == "100%":
                        result["calc100"] += after_shabbat
                        add_segment_detail(after_start_abs, block_abs_end, "100%", False)
                    elif base_rate == "125%":
                        result["calc125"] += after_shabbat
                        add_segment_detail(after_start_abs, block_abs_end, "125%", False)
                    else:
                        result["calc150"] += after_shabbat
                        result["calc150_overtime"] += after_shabbat
                        add_segment_detail(after_start_abs, block_abs_end, "150%", False)

                else:
                    # Fallback - shouldn't happen but just in case
                    if base_rate == "100%":
                        result["calc100"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "100%", False)
                    elif base_rate == "125%":
                        result["calc125"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "125%", False)
                    else:
                        result["calc150"] += block_size
                        result["calc150_overtime"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "150%", False)
            else:
                # Not Friday or Saturday - simple calculation
                if base_rate == "100%":
                    result["calc100"] += block_size
                    result["segments_detail"].append((current_abs_minute, current_abs_minute + block_size, "100%", False))
                elif base_rate == "125%":
                    result["calc125"] += block_size
                    result["segments_detail"].append((current_abs_minute, current_abs_minute + block_size, "125%", False))
                else:
                    result["calc150"] += block_size
                    result["calc150_overtime"] += block_size
                    result["segments_detail"].append((current_abs_minute, current_abs_minute + block_size, "150%", False))

            seg_offset += block_size
            minutes_processed += block_size

    # Merge adjacent segments with the same label for cleaner display
    merged_segments = []
    for seg in result["segments_detail"]:
        if merged_segments and merged_segments[-1][2] == seg[2] and merged_segments[-1][1] == seg[0]:
            # Merge with previous segment
            merged_segments[-1] = (merged_segments[-1][0], seg[1], seg[2], seg[3])
        else:
            merged_segments.append(seg)
    result["segments_detail"] = merged_segments

    return result


# =============================================================================
# Helper Functions
# =============================================================================

def get_effective_hourly_rate(report, minimum_wage: float) -> float:
    """
    Get the effective hourly rate for a shift.
    If the shift has a custom rate defined, use that rate.
    Otherwise, use the minimum wage.

    Args:
        report: The report dict containing shift_rate and shift_is_minimum_wage
        minimum_wage: The default minimum wage rate

    Returns:
        The effective hourly rate to use for payment calculation
    """
    shift_rate = report.get('shift_rate')

    # If shift has a custom rate, use it (regardless of is_minimum_wage flag)
    if shift_rate:
        rate = float(shift_rate) / 100  # shift_rate is stored in agorot
        # Validate rate is positive
        if rate > 0:
            return rate
        # Invalid rate - log warning and fall back to minimum wage
        logging.warning(f"Invalid shift_rate {shift_rate} for shift, using minimum wage")

    return minimum_wage


def _calculate_previous_month_carryover(conn, person_id: int, year: int, month: int, minimum_wage: float = 0) -> tuple[int, int, int | None, int]:
    """
    חישוב carryover מהחודש הקודם - חיפוש איטרטיבי אחורה עד שבירת רצף.

    הפונקציה מחפשת אחורה מהיום האחרון של החודש הקודם, יום אחר יום,
    עד שמוצאת יום ללא דיווחים (שבירת רצף).

    מחזיר את סך הדקות שנצברו ברצף האחרון, זמן הסיום שלו, shift_id של הרצף,
    ושעות הלילה ברצף (לקביעה אם זה רצף לילה).

    חשוב: הלוגיקה זהה ללוגיקת אמצע החודש:
    - כוננות שוברת רצף רק אם אין עבודה שחופפת לה
    - הפסקה > 60 דקות שוברת רצף
    - שינוי תעריף בין משמרות שוברת רצף (אבל מעביר offset)

    Args:
        conn: חיבור לDB
        person_id: מזהה העובד
        year: שנה נוכחית
        month: חודש נוכחי
        minimum_wage: שכר מינימום לחישוב תעריפים

    Returns:
        tuple של (דקות ברצף, זמן סיום הרצף בדקות מנורמלות, shift_id של הרצף האחרון, דקות לילה ברצף)
        או (0, 0, None, 0) אם אין carryover
    """
    # חישוב היום האחרון של החודש הקודם
    if month == 1:
        prev_year = year - 1
        prev_month = 12
    else:
        prev_year = year
        prev_month = month - 1

    # מציאת היום האחרון של החודש הקודם
    if prev_month == 12:
        last_day = 31
    elif prev_month in (4, 6, 9, 11):
        last_day = 30
    elif prev_month == 2:
        # בדיקת שנה מעוברת
        if (prev_year % 4 == 0 and prev_year % 100 != 0) or (prev_year % 400 == 0):
            last_day = 29
        else:
            last_day = 28
    else:
        last_day = 31

    last_day_date = date(prev_year, prev_month, last_day)

    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # חיפוש איטרטיבי אחורה - מוצאים את היום הראשון עם דיווחים
    # והולכים אחורה עד שמוצאים יום ללא דיווחים (שבירת רצף)
    # מגבלת בטיחות: מקסימום 31 ימים (חודש שלם)
    MAX_LOOKBACK_DAYS = 31
    earliest_date = last_day_date

    for days_back in range(MAX_LOOKBACK_DAYS):
        check_date = last_day_date - timedelta(days=days_back)

        # בדיקה אם יש דיווחים ביום הזה
        cursor.execute("""
            SELECT COUNT(*) as cnt FROM time_reports
            WHERE person_id = %s AND date = %s
        """, (person_id, check_date))
        row = cursor.fetchone()

        if row["cnt"] == 0:
            # אין דיווחים ביום הזה - זה גבול הרצף (יום ללא עבודה)
            break
        earliest_date = check_date

    # שליפת כל הדיווחים מהטווח שמצאנו
    cursor.execute("""
        SELECT tr.date, tr.start_time, tr.end_time, tr.shift_type_id, tr.apartment_id,
               st.rate AS shift_rate, st.is_minimum_wage AS shift_is_minimum_wage
        FROM time_reports tr
        LEFT JOIN shift_types st ON st.id = tr.shift_type_id
        WHERE tr.person_id = %s AND tr.date >= %s AND tr.date <= %s
        ORDER BY tr.date, tr.start_time
    """, (person_id, earliest_date, last_day_date))
    all_reports = cursor.fetchall()

    if not all_reports:
        cursor.close()
        return (0, 0, None, 0)

    # שליפת סגמנטים של כל סוגי המשמרות הרלוונטיים
    shift_ids = list({r["shift_type_id"] for r in all_reports if r["shift_type_id"]})
    if not shift_ids:
        cursor.close()
        return (0, 0, None, 0)

    placeholders = ",".join(["%s"] * len(shift_ids))
    cursor.execute(f"""
        SELECT shift_type_id, segment_type, start_time, end_time
        FROM shift_time_segments
        WHERE shift_type_id IN ({placeholders})
        ORDER BY shift_type_id, order_index
    """, tuple(shift_ids))
    shift_segments = cursor.fetchall()
    cursor.close()

    # בניית מפה של סגמנטים לפי סוג משמרת
    segments_by_shift = {}
    for seg in shift_segments:
        shift_id = seg["shift_type_id"]
        if shift_id not in segments_by_shift:
            segments_by_shift[shift_id] = []
        segments_by_shift[shift_id].append({
            "type": seg["segment_type"],
            "start": seg["start_time"],
            "end": seg["end_time"]
        })

    # בניית מפת תעריפים לפי shift_id
    shift_rates = {}
    for r in all_reports:
        shift_id = r.get("shift_type_id")
        if shift_id and shift_id not in shift_rates:
            if r.get("shift_is_minimum_wage"):
                shift_rates[shift_id] = minimum_wage
            elif r.get("shift_rate"):
                shift_rates[shift_id] = float(r["shift_rate"])
            else:
                shift_rates[shift_id] = minimum_wage

    # ארגון דיווחים לפי ימים - בסדר כרונולוגי
    reports_by_day = {}
    for r in all_reports:
        r_date = r["date"]
        if isinstance(r_date, datetime):
            r_date = r_date.date()
        if r_date not in reports_by_day:
            reports_by_day[r_date] = []
        reports_by_day[r_date].append(r)

    # מיון הימים בסדר כרונולוגי
    sorted_days = sorted(reports_by_day.keys())

    # בניית רשימת אירועים לכל יום (בציר מנורמל 08:00-08:00)
    # כל יום מקבל offset של 1440 דקות ביחס ליום הקודם
    all_events = []
    work_segments_all = []  # כל סגמנטי העבודה לבדיקת חפיפה עם כוננויות
    day_base_offset = 0  # offset מצטבר לכל יום

    for day_idx, day_date in enumerate(sorted_days):
        # חישוב offset ביחס ליום הראשון
        if day_idx == 0:
            day_base_offset = 0
        else:
            # כל יום מקבל 1440 דקות נוספות
            prev_day = sorted_days[day_idx - 1]
            days_diff = (day_date - prev_day).days
            day_base_offset += days_diff * MINUTES_PER_DAY

        day_reports = reports_by_day[day_date]

        for r in day_reports:
            report_start_str = r["start_time"]
            report_end_str = r["end_time"]
            shift_id = r["shift_type_id"]

            # המרת זמני דיווח לדקות
            rs_parts = report_start_str.split(":")
            report_start_min = int(rs_parts[0]) * 60 + int(rs_parts[1])
            re_parts = report_end_str.split(":")
            report_end_min = int(re_parts[0]) * 60 + int(re_parts[1])

            # בדיקה אם זו משמרת בוקר של אותו יום (לפני 08:00)
            # משמרת כזו לא רלוונטית ל-carryover כי היא לא חלק מיום העבודה 08:00-08:00
            is_morning_only_shift = (
                report_start_min < 480 and
                report_end_min < 480 and
                report_end_min > report_start_min
            )
            if is_morning_only_shift:
                continue

            # נרמול לציר 08:00-08:00 של היום
            if report_start_min < 480:
                report_start_min += MINUTES_PER_DAY
            if report_end_min <= 480:
                report_end_min += MINUTES_PER_DAY
            if report_end_min <= report_start_min:
                report_end_min += MINUTES_PER_DAY

            # הוספת offset של היום
            report_start_min += day_base_offset
            report_end_min += day_base_offset

            # בדיקה אם יש סגמנטים מוגדרים למשמרת
            if shift_id in segments_by_shift:
                for seg in segments_by_shift[shift_id]:
                    seg_start_parts = seg["start"].split(":")
                    seg_start_min = int(seg_start_parts[0]) * 60 + int(seg_start_parts[1])
                    seg_end_parts = seg["end"].split(":")
                    seg_end_min = int(seg_end_parts[0]) * 60 + int(seg_end_parts[1])

                    # נרמול
                    if seg_start_min < 480:
                        seg_start_min += MINUTES_PER_DAY
                    if seg_end_min <= 480:
                        seg_end_min += MINUTES_PER_DAY
                    if seg_end_min <= seg_start_min:
                        seg_end_min += MINUTES_PER_DAY

                    # הוספת offset של היום
                    seg_start_min += day_base_offset
                    seg_end_min += day_base_offset

                    # בדיקת חפיפה עם הדיווח
                    overlap_start = max(report_start_min, seg_start_min)
                    overlap_end = min(report_end_min, seg_end_min)

                    if overlap_end > overlap_start:
                        event = {
                            "start": overlap_start,
                            "end": overlap_end,
                            "type": seg["type"],
                            "shift_id": shift_id
                        }
                        all_events.append(event)
                        if seg["type"] == "work":
                            work_segments_all.append((overlap_start, overlap_end))
            else:
                # אין סגמנטים מוגדרים - כל הדיווח הוא עבודה
                event = {
                    "start": report_start_min,
                    "end": report_end_min,
                    "type": "work",
                    "shift_id": shift_id
                }
                all_events.append(event)
                work_segments_all.append((report_start_min, report_end_min))

    if not all_events:
        return (0, 0, None, 0)

    # מיון לפי זמן התחלה
    all_events.sort(key=lambda x: x["start"])

    # בניית רצפי עבודה - רצף נשבר על ידי:
    # 1. הפסקה > 60 דקות
    # 2. כוננות (רק אם אין עבודה שחופפת לה)
    # 3. שינוי תעריף (אבל מעביר offset)
    current_chain = []
    current_chain_shift_id = None
    last_work_end = None
    chain_total = 0  # סה"כ דקות שנצברו (כולל מ-chains קודמים שנשברו בגלל תעריף)

    for evt in all_events:
        if evt["type"] == "standby":
            # כוננות שוברת רצף רק אם אין עבודה שחופפת לה
            standby_overlaps_work = any(
                ws[0] < evt["end"] and ws[1] > evt["start"]
                for ws in work_segments_all
            )
            if not standby_overlaps_work:
                # כוננות שוברת רצף
                if current_chain:
                    chain_total = 0  # כוננות מאפסת לגמרי
                    current_chain = []
                    current_chain_shift_id = None
                last_work_end = None
        else:
            # עבודה
            should_break = False
            break_reason = ""

            # בדיקת הפסקה גדולה
            if last_work_end is not None:
                gap = evt["start"] - last_work_end
                if gap > BREAK_THRESHOLD_MINUTES:
                    should_break = True
                    break_reason = "gap"

            # בדיקת שינוי תעריף
            if not should_break and current_chain_shift_id is not None:
                current_rate = shift_rates.get(current_chain_shift_id, minimum_wage)
                new_rate = shift_rates.get(evt["shift_id"], minimum_wage)
                if current_rate != new_rate:
                    should_break = True
                    break_reason = "rate_change"

            if should_break:
                if current_chain:
                    chain_minutes = sum(seg[1] - seg[0] for seg in current_chain)
                    if break_reason == "rate_change":
                        # שינוי תעריף מעביר offset
                        chain_total += chain_minutes
                    else:
                        # הפסקה מאפסת
                        chain_total = 0
                    current_chain = []
                    current_chain_shift_id = None

            current_chain.append((evt["start"], evt["end"]))
            current_chain_shift_id = evt["shift_id"]
            last_work_end = evt["end"]

    # סגירת רצף אחרון
    if not current_chain:
        return (0, 0, None, 0)

    # חישוב סך הדקות ברצף האחרון + offset מרצפים קודמים
    last_chain_minutes = sum(seg[1] - seg[0] for seg in current_chain)
    chain_total_minutes = chain_total + last_chain_minutes

    # זמן הסיום של הרצף האחרון - נרמול לציר יום בודד (08:00-08:00)
    # מחזירים את הזמן ביחס ליום האחרון בלבד
    last_end_time_raw = current_chain[-1][1]
    # ננרמל ל-1920 (08:00 ביום הבא) כמו הקוד המקורי
    last_end_time = last_end_time_raw % MINUTES_PER_DAY
    if last_end_time <= 480:
        last_end_time += MINUTES_PER_DAY  # מנרמל לציר 08:00-08:00

    # חישוב שעות לילה ברצף (22:00-06:00)
    chain_night_minutes = 0
    for seg_start, seg_end in current_chain:
        # המרה מציר מצטבר לציר 00:00-24:00
        real_start = (seg_start + 480) % 1440
        real_end = (seg_end + 480) % 1440
        # טיפול בסגמנטים שחוצים חצות
        if real_end <= real_start and seg_end > seg_start:
            real_end += 1440
        chain_night_minutes += calculate_night_hours_in_segment(real_start, real_end)

    # מחזיר את הדקות, זמן הסיום, shift_id של הרצף האחרון, ודקות לילה
    return (chain_total_minutes, last_end_time, current_chain_shift_id, chain_night_minutes)


def get_daily_segments_data(conn, person_id: int, year: int, month: int, shabbat_cache: Dict, minimum_wage: float):
    """
    Calculates detailed daily segments for a given employee and month.
    Used by guide_view and simple_summary_view.
    """
    start_dt, end_dt = month_range_ts(year, month)
    
    # Convert datetime to date for PostgreSQL date column
    start_date = start_dt.date()
    end_date = end_dt.date()
    
    # Fetch reports
    reports = conn.execute("""
        SELECT tr.*,
               st.name AS shift_name,
               st.color AS shift_color,
               st.for_friday_eve,
               st.for_shabbat_holiday,
               st.rate AS shift_rate,
               st.is_minimum_wage AS shift_is_minimum_wage,
               st.is_special_hourly AS shift_is_special_hourly,
               ap.name AS apartment_name,
               ap.apartment_type_id,
               p.is_married,
               p.name as person_name
        FROM time_reports tr
        LEFT JOIN shift_types st ON st.id = tr.shift_type_id
        LEFT JOIN apartments ap ON ap.id = tr.apartment_id
        LEFT JOIN people p ON p.id = tr.person_id
        WHERE tr.person_id = %s AND tr.date >= %s AND tr.date < %s
        ORDER BY tr.date, tr.start_time
    """, (person_id, start_date, end_date)).fetchall()
    
    person_name = reports[0]["person_name"] if reports else ""

    # Override apartment types and marital status with historical data
    # Build apartment historical cache
    apartment_ids = {r["apartment_id"] for r in reports if r["apartment_id"]}
    apartment_type_cache = {}
    for apt_id in apartment_ids:
        hist_type = get_apartment_type_for_month(conn, apt_id, year, month)
        if hist_type is not None:
            apartment_type_cache[apt_id] = hist_type

    # Historical marital status
    historical_person = get_person_status_for_month(conn, person_id, year, month)
    historical_is_married = historical_person.get("is_married")

    # Build shift rates historical cache
    shift_rates_cache = get_all_shift_rates_for_month(conn, year, month)

    # Apply historical overrides to reports
    processed_reports = []
    for r in reports:
        r_dict = dict(r)

        # Save actual apartment type for visual indicator (from apartments table)
        r_dict["actual_apartment_type_id"] = r_dict.get("apartment_type_id")

        # Override apartment_type_id for rate calculation
        # Priority: rate_apartment_type_id (if set) > historical > current
        rate_apt_type = r_dict.get("rate_apartment_type_id")
        if rate_apt_type:
            # Use the explicit rate_apartment_type_id from the report
            r_dict["apartment_type_id"] = rate_apt_type
        else:
            # Fall back to historical apartment type
            apt_id = r_dict.get("apartment_id")
            if apt_id and apt_id in apartment_type_cache:
                r_dict["apartment_type_id"] = apartment_type_cache[apt_id]
        
        # Override is_married
        if historical_is_married is not None:
            r_dict["is_married"] = historical_is_married
            
        # Override shift rate with historical value if available
        # Only override if the historical rate is not None (otherwise keep current rate)
        shift_type_id = r_dict.get("shift_type_id")
        if shift_type_id and shift_type_id in shift_rates_cache:
            rate_info = shift_rates_cache[shift_type_id]
            historical_rate = rate_info.get("rate")
            if historical_rate is not None:
                r_dict["shift_rate"] = historical_rate
                r_dict["shift_is_minimum_wage"] = rate_info.get("is_minimum_wage")
            
        processed_reports.append(r_dict)

    reports = processed_reports

    # זיהוי רצפי ימי מחלה לחישוב אחוזי תשלום מדורגים
    sick_day_sequence = _identify_sick_day_sequences(reports)

    # Fetch segments
    shift_ids = {r["shift_type_id"] for r in reports if r["shift_type_id"]}
    shift_segments = []
    if shift_ids:
        placeholders = ",".join(["%s"] * len(shift_ids))
        shift_segments = conn.execute(
            f"""
            SELECT seg.*, st.name AS shift_name
            FROM shift_time_segments seg
            JOIN shift_types st ON st.id = seg.shift_type_id
            WHERE seg.shift_type_id IN ({placeholders})
            ORDER BY seg.shift_type_id, seg.order_index, seg.id
            """,
            tuple(shift_ids),
        ).fetchall()
        
    segments_by_shift = {}
    for seg in shift_segments:
        segments_by_shift.setdefault(seg["shift_type_id"], []).append(seg)
    
    # Build a map of shift_type_id -> effective hourly rate
    # This allows using custom rates for special shifts (like cleaning)
    shift_rates = {}
    shift_names_map = {}  # Map shift_id -> shift_name
    shift_is_special_hourly = {}  # Map shift_id -> is_special_hourly (for variable rate tracking)
    shabbat_shifts = set()  # Track which shifts are Shabbat/holiday shifts
    for r in reports:
        shift_id = r.get("shift_type_id")
        if shift_id:
            if shift_id not in shift_rates:
                shift_rates[shift_id] = get_effective_hourly_rate(r, minimum_wage)
            if shift_id not in shift_names_map:
                shift_names_map[shift_id] = r.get("shift_name", "")
            if shift_id not in shift_is_special_hourly:
                shift_is_special_hourly[shift_id] = r.get("shift_is_special_hourly", False)
            if r.get("for_shabbat_holiday"):
                shabbat_shifts.add(shift_id)

    daily_map = {}
    
    for r in reports:
        if not r["shift_type_id"]:
            continue

        # בדיקה אם יש שעות בדיווח
        has_times = r["start_time"] and r["end_time"]

        # אם אין שעות - בודקים אם יש סגמנטים מוגדרים למשמרת (למשל יום מחלה/חופשה)
        if not has_times:
            seg_list_check = segments_by_shift.get(r["shift_type_id"], [])
            if seg_list_check:
                # יש סגמנטים - נשתמש בשעות מהסגמנט הראשון
                first_seg = seg_list_check[0]
                r = dict(r)  # יצירת עותק כדי לא לשנות את המקור
                r["start_time"] = first_seg["start_time"]
                r["end_time"] = first_seg["end_time"]
            else:
                # אין סגמנטים ואין שעות - דלג
                continue

        # Split shifts across midnight
        rep_start_orig, rep_end_orig = span_minutes(r["start_time"], r["end_time"])
        r_date = to_local_date(r["date"])
        
        # משמרת לווי רפואי (148) - לפחות שעה עבודה
        is_medical_escort = (r["shift_type_id"] == 148)
        escort_bonus_minutes = 0
        if is_medical_escort:
            duration = rep_end_orig - rep_start_orig
            if duration < 60:
                escort_bonus_minutes = 60 - duration
        
        parts = []
        if rep_end_orig <= MINUTES_PER_DAY:
            parts.append((r_date, rep_start_orig, rep_end_orig, escort_bonus_minutes))
        else:
            # בפיצול חצות, הבונוס בדרך כלל שייך ליום ההתחלה, אבל נצמיד אותו לחלק הראשון
            parts.append((r_date, rep_start_orig, MINUTES_PER_DAY, escort_bonus_minutes))
            next_day = r_date + timedelta(days=1)
            parts.append((next_day, 0, rep_end_orig - MINUTES_PER_DAY, 0))

        seg_list = segments_by_shift.get(r["shift_type_id"], [])
        if not seg_list:
            # אין סגמנטים מוגדרים - יצירת סגמנט דינמי
            # wage_percent=None מסמן שהאחוז יחושב לפי מנגנון הרצפים/שבת
            seg_list = [{
                "start_time": r["start_time"],
                "end_time": r["end_time"],
                "wage_percent": None,  # יחושב דינמית לפי שעות שבת
                "segment_type": "work",
                "id": None
            }]
            
        work_type = None
        shift_name_str = (r["shift_name"] or "")
        is_sick_report = ("מחלה" in shift_name_str)
        is_vacation_report = ("חופשה" in shift_name_str)

        # משמרות עם סגמנטים קבועים - משתמשים בסגמנטים המוגדרים ישירות (לא לפי שעות דיווח)
        # כולל: משמרות תגבור, יום חופשה, יום מחלה
        shift_type_id = r.get("shift_type_id")
        is_fixed_segments_shift = is_tagbur_shift(shift_type_id) or is_vacation_report or is_sick_report

        # משמרת לילה - סגמנטים דינמיים לפי זמן הכניסה בפועל
        # החוק: 2 שעות ראשונות עבודה, עד 06:30 כוננות, 06:30-08:00 עבודה
        is_night = is_night_shift(shift_type_id)
        if is_night:
            # יצירת סגמנטים דינמיים לפי זמן הכניסה בפועל
            entry_time = rep_start_orig  # זמן הכניסה בדקות
            exit_time = rep_end_orig if rep_end_orig > entry_time else rep_end_orig + MINUTES_PER_DAY

            WORK_FIRST_HOURS = 120  # 2 שעות ראשונות = עבודה
            STANDBY_END = 390  # 06:30 = 390 דקות
            MORNING_WORK_START = 390  # 06:30
            MORNING_WORK_END = 480  # 08:00

            # מציאת ה-seg_id של כוננות לילה מהסגמנטים המוגדרים בטבלה
            night_standby_seg_id = None
            if shift_type_id in segments_by_shift:
                for seg in segments_by_shift[shift_type_id]:
                    if seg.get("segment_type") == "standby":
                        night_standby_seg_id = seg.get("id")
                        break

            # חישוב הסגמנטים הדינמיים
            dynamic_segments = []

            # סגמנט 1: 2 שעות ראשונות עבודה
            work1_start = entry_time
            work1_end = min(entry_time + WORK_FIRST_HOURS, exit_time)
            if work1_end > work1_start:
                dynamic_segments.append({
                    "start_time": f"{(work1_start // 60) % 24:02d}:{work1_start % 60:02d}",
                    "end_time": f"{(work1_end // 60) % 24:02d}:{work1_end % 60:02d}",
                    "wage_percent": 100,
                    "segment_type": "work",
                    "id": None
                })

            # סגמנט 2: כוננות מסוף 2 שעות עבודה עד 06:30
            standby_start = work1_end
            # 06:30 - אם הכניסה אחרי חצות, 06:30 הוא באותו יום; אחרת ביום הבא
            standby_end_time = STANDBY_END if entry_time < MINUTES_PER_DAY else STANDBY_END + MINUTES_PER_DAY
            if entry_time >= 720:  # אם נכנס אחרי 12:00, 06:30 הוא למחרת
                standby_end_time = STANDBY_END + MINUTES_PER_DAY
            standby_end = min(standby_end_time, exit_time)
            if standby_end > standby_start:
                dynamic_segments.append({
                    "start_time": f"{(standby_start // 60) % 24:02d}:{standby_start % 60:02d}",
                    "end_time": f"{(standby_end // 60) % 24:02d}:{standby_end % 60:02d}",
                    "wage_percent": 24,
                    "segment_type": "standby",
                    "id": night_standby_seg_id
                })

            # סגמנט 3: עבודה 06:30-08:00
            morning_start = standby_end_time
            morning_end_time = MORNING_WORK_END if entry_time < MINUTES_PER_DAY else MORNING_WORK_END + MINUTES_PER_DAY
            if entry_time >= 720:  # אם נכנס אחרי 12:00, 08:00 הוא למחרת
                morning_end_time = MORNING_WORK_END + MINUTES_PER_DAY
            morning_end = min(morning_end_time, exit_time)
            if morning_end > morning_start and morning_start < exit_time:
                dynamic_segments.append({
                    "start_time": f"{(morning_start // 60) % 24:02d}:{morning_start % 60:02d}",
                    "end_time": f"{(morning_end // 60) % 24:02d}:{morning_end % 60:02d}",
                    "wage_percent": 100,
                    "segment_type": "work",
                    "id": None
                })

            # החלפת רשימת הסגמנטים בסגמנטים הדינמיים
            seg_list = dynamic_segments

        # משמרת תגבור - סגמנטים דינמיים לפי זמני שבת
        # תגבור שישי (108): סגמנט ראשון מתחיל שעה לפני כניסת שבת
        # תגבור שבת (109): סגמנט אחרון מסתיים שעתיים אחרי צאת שבת
        # שאר הסגמנטים נשארים במקום - הפיצול לשבת מתבצע אוטומטית
        if is_tagbur_shift(shift_type_id) and seg_list:
            # יצירת עותק של seg_list כדי לא לשנות את המקור
            seg_list = [dict(seg) for seg in seg_list]

            # קבלת זמני שבת לתאריך הדיווח
            shabbat_enter, shabbat_exit = _get_shabbat_boundaries(r_date, shabbat_cache)

            if shift_type_id == TAGBUR_FRIDAY_SHIFT_ID and shabbat_enter > 0:
                # תגבור שישי - סגמנט ראשון מתחיל שעה לפני כניסת שבת
                # שאר הסגמנטים נשארים במקום
                new_first_start = shabbat_enter - 60  # שעה לפני כניסת שבת

                if seg_list:
                    first_seg = seg_list[0]
                    first_seg_start, first_seg_end = span_minutes(first_seg["start_time"], first_seg["end_time"])

                    # הסגמנט הראשון מתחיל שעה לפני כניסת שבת
                    # ונגמר בזמן המקורי או בתחילת הסגמנט הבא (מה שקודם)
                    if len(seg_list) > 1:
                        second_seg_start, _ = span_minutes(seg_list[1]["start_time"], seg_list[1]["end_time"])
                        new_first_end = second_seg_start
                    else:
                        new_first_end = first_seg_end

                    seg_list[0] = {
                        **first_seg,
                        "start_time": f"{(new_first_start // 60) % 24:02d}:{new_first_start % 60:02d}",
                        "end_time": f"{(new_first_end // 60) % 24:02d}:{new_first_end % 60:02d}",
                    }

            elif shift_type_id == TAGBUR_SHABBAT_SHIFT_ID and shabbat_exit > 0:
                # תגבור שבת - סגמנט אחרון מסתיים שעתיים אחרי צאת שבת
                # שאר הסגמנטים נשארים במקום
                # צאת שבת היא ביום שבת, צריך להמיר לדקות מ-00:00 של שבת
                new_last_end = (shabbat_exit % MINUTES_PER_DAY) + 120  # שעתיים אחרי צאת שבת

                if seg_list:
                    last_seg = seg_list[-1]
                    last_seg_start, last_seg_end = span_minutes(last_seg["start_time"], last_seg["end_time"])

                    # הסגמנט האחרון מתחיל בזמן המקורי ונגמר שעתיים אחרי צאת שבת
                    seg_list[-1] = {
                        **last_seg,
                        "start_time": last_seg["start_time"],  # נשאר במקום
                        "end_time": f"{(new_last_end // 60) % 24:02d}:{new_last_end % 60:02d}",
                    }

        # אם זו משמרת תגבור - מוסיפים את הסגמנטים ישירות בלי לחשב חפיפה עם שעות הדיווח
        if is_fixed_segments_shift and seg_list:
            CUTOFF = 480  # 08:00
            display_date = r_date  # יום הדיווח
            day_key = display_date.strftime("%d/%m/%Y")
            entry = daily_map.setdefault(day_key, {"buckets": {}, "shifts": set(), "segments": [], "is_fixed_segments": False, "escort_bonus_minutes": 0, "day_shift_types": set()})
            entry["is_fixed_segments"] = True  # סימון שזו משמרת קבועה
            entry["day_shift_types"].add(r["shift_type_id"])  # Track shift types for Shabbat detection
            if r["shift_name"]:
                entry["shifts"].add(r["shift_name"])

            # מעקב אחרי זמן הסיום של הסגמנט הקודם לזיהוי מעבר יום
            prev_seg_end = None
            days_offset = 0  # כמה ימים עברו מתחילת המשמרת

            for seg in seg_list:
                seg_start, seg_end = span_minutes(seg["start_time"], seg["end_time"])
                duration = seg_end - seg_start

                # זיהוי מעבר יום: אם זמן ההתחלה קטן מזמן הסיום של הסגמנט הקודם
                # זה אומר שעברנו חצות והסגמנט הזה הוא ביום הבא
                if prev_seg_end is not None and seg_start < prev_seg_end:
                    days_offset += 1
                prev_seg_end = seg_end

                # קביעת התאריך האמיתי של הסגמנט
                actual_seg_date = r_date + timedelta(days=days_offset)

                # קביעת סוג אפקטיבי
                if is_sick_report:
                    effective_seg_type = "sick"
                elif is_vacation_report:
                    effective_seg_type = "vacation"
                else:
                    effective_seg_type = seg["segment_type"]

                # קביעת תווית
                if effective_seg_type == "standby":
                    label = "כוננות"
                elif effective_seg_type == "vacation":
                    label = "חופשה"
                elif effective_seg_type == "sick":
                    label = "מחלה"
                elif seg["wage_percent"] == 100:
                    label = "100%"
                elif seg["wage_percent"] == 125:
                    label = "125%"
                elif seg["wage_percent"] == 150:
                    label = "150%"
                elif seg["wage_percent"] == 175:
                    label = "175%"
                elif seg["wage_percent"] == 200:
                    label = "200%"
                else:
                    label = f"{seg['wage_percent']}%"

                entry["buckets"].setdefault(label, 0)
                entry["buckets"][label] += duration

                segment_id = seg.get("id")
                apartment_type_id = r.get("apartment_type_id")  # For rate calculation
                actual_apartment_type_id = r.get("actual_apartment_type_id")  # For visual indicator
                is_married = r.get("is_married")
                apartment_name = r.get("apartment_name", "")

                # For fixed segment shifts (tagbur/vacation/sick), standby_defined_end = seg_end (full standby)
                standby_defined_end = seg_end if effective_seg_type == "standby" else None
                entry["segments"].append((seg_start, seg_end, effective_seg_type, label, r["shift_type_id"], segment_id, apartment_type_id, is_married, apartment_name, actual_seg_date, actual_apartment_type_id, standby_defined_end))

            continue  # דלג על העיבוד הרגיל עבור משמרת זו

        for p_date, p_start, p_end, p_escort_bonus in parts:
            # Split segments crossing 08:00 cutoff
            CUTOFF = 480  # 08:00
            sub_parts = []
            if p_start < CUTOFF < p_end:
                sub_parts.append((p_start, CUTOFF))
                sub_parts.append((CUTOFF, p_end))
            else:
                sub_parts.append((p_start, p_end))

            for s_start, s_end in sub_parts:
                # Assign to workday and normalize times
                # דיווח ששעת הסיום שלו לפני 08:00 שייך ליום העבודה הקודם
                # אבל רק אם זה המשך של משמרת שהתחילה לפני חצות
                # דיווח עצמאי = הדיווח המקורי התחיל אחרי חצות (00:00-08:00) ביום הנוכחי
                # לדוגמה: דיווח 02:00-06:30 הוא עצמאי ולא המשך משמרת
                is_standalone_night_shift = (p_date == r_date and rep_start_orig < CUTOFF)
                if s_end <= CUTOFF and not is_standalone_night_shift:
                    # Belongs to previous day's workday (continuation of shift)
                    display_date = p_date - timedelta(days=1)
                    norm_start = s_start + MINUTES_PER_DAY
                    norm_end = s_end + MINUTES_PER_DAY
                else:
                    # Belongs to current day's workday
                    display_date = p_date
                    norm_start = s_start
                    norm_end = s_end

                if display_date.year != year or display_date.month != month:
                    logger.debug(f"Skipping report outside month: person_id={person_id}, date={display_date}, requested={year}-{month:02d}")
                    continue

                day_key = display_date.strftime("%d/%m/%Y")
                if day_key not in daily_map:
                    daily_map[day_key] = {
                        "buckets": {},
                        "shifts": set(),
                        "segments": [],
                        "is_fixed_segments": False,
                        "escort_bonus_minutes": 0,
                        "day_shift_types": set()
                    }
                entry = daily_map[day_key]
                entry["day_shift_types"].add(r["shift_type_id"])  # Track shift types for Shabbat detection

                # Add bonus only once per part
                if s_start == p_start:
                    entry["escort_bonus_minutes"] += p_escort_bonus

                if r["shift_name"]:
                    entry["shifts"].add(r["shift_name"])
                    
                minutes_covered = 0
                covered_intervals = []  # לאיסוף אינטרוולים מכוסים לחישוב "חורים" בהמשך
                is_second_day = (p_date > r_date)
                
                # Sort segments chronologically by start time
                seg_list_sorted = sorted(seg_list, key=lambda s: span_minutes(s["start_time"], s["end_time"])[0])

                # Rotate the list so that the segment corresponding to the report start time comes first
                # This ensures that normalization flows correctly (e.g. 06:30-08:00 is end of shift, not start)
                rotate_idx = 0
                rep_start_min = rep_start_orig % MINUTES_PER_DAY

                # Find the segment that starts closest to (and before/at) the report start time
                best_start_diff = -1

                # Define threshold for morning segments: segments before 08:00 might be "next day" segments
                MORNING_CUTOFF = 480  # 08:00

                for i, seg in enumerate(seg_list_sorted):
                    seg_start_min, _ = span_minutes(seg["start_time"], seg["end_time"])

                    # Fix for bug: When report starts in afternoon (e.g. 15:00) and a segment starts
                    # in early morning (e.g. 06:30), that segment is likely NEXT DAY, not before report.
                    # This prevents treating 06:30-08:00 as the first segment for a 15:00-08:00 report.
                    is_morning_segment = seg_start_min < MORNING_CUTOFF
                    is_afternoon_report = rep_start_min >= 720  # 12:00

                    if is_morning_segment and is_afternoon_report:
                        # Skip this morning segment - it's next day, not before the report
                        continue

                    if seg_start_min <= rep_start_min:
                        if seg_start_min > best_start_diff:
                            best_start_diff = seg_start_min
                            rotate_idx = i
                    elif best_start_diff == -1:
                        # If we haven't found any starting before, and this is the first one,
                        # checking implies we might need to wrap around.
                        # But we continue to see if there are others.
                        pass

                # If no segment starts before report time:
                # - If report starts BEFORE the first segment of the shift definition,
                #   keep rotate_idx=0 (start from the first segment)
                # - If report starts AFTER all segments (late in day),
                #   then it might belong to the LAST segment wrapping around
                # For a report 08:00-08:00 with first segment at 12:00,
                # the 08:00-12:00 gap is just waiting time, so start from segment 0
                if best_start_diff == -1 and seg_list_sorted:
                    first_seg_start, _ = span_minutes(seg_list_sorted[0]["start_time"], seg_list_sorted[0]["end_time"])

                    # For afternoon reports, find first non-morning segment
                    if rep_start_min >= 720:  # Report is in afternoon/evening
                        first_afternoon_idx = None
                        for i, seg in enumerate(seg_list_sorted):
                            seg_start_min, _ = span_minutes(seg["start_time"], seg["end_time"])
                            if seg_start_min >= MORNING_CUTOFF:
                                first_afternoon_idx = i
                                break

                        if first_afternoon_idx is not None:
                            rotate_idx = first_afternoon_idx
                        else:
                            # All segments are morning - unusual case, use first
                            rotate_idx = 0
                    else:
                        # Report is in morning/early hours, use standard logic
                        if rep_start_min < first_seg_start:
                            rotate_idx = 0
                        else:
                            # Report starts late in morning (e.g. 05:00)
                            rotate_idx = len(seg_list_sorted) - 1

                seg_list_ordered = seg_list_sorted[rotate_idx:] + seg_list_sorted[:rotate_idx]
                
                # Normalize segments from shift definition to be continuous
                last_s_end_norm = -1
                for seg in seg_list_ordered:
                    # Use unique variable names to avoid shadowing
                    orig_s_start, orig_s_end = span_minutes(seg["start_time"], seg["end_time"])
                    
                    # Make segments continuous relative to the first one
                    if last_s_end_norm == -1:
                        # First segment: align to report start day roughly
                        # If orig_s_start is far from rep_start_min, adjust? 
                        # Actually, just start with it as is (or +1440 if needed?)
                        # No, simple normalization should work if we start with the "right" segment.
                        pass
                    else:
                        while orig_s_start < last_s_end_norm:
                            orig_s_start += MINUTES_PER_DAY
                            orig_s_end += MINUTES_PER_DAY
                    
                    last_s_end_norm = orig_s_end
                    
                    # Adjust segments to the timeline of the current report part
                    if is_second_day:
                        current_seg_start = orig_s_start - MINUTES_PER_DAY
                        current_seg_end = orig_s_end - MINUTES_PER_DAY
                    else:
                        current_seg_start = orig_s_start
                        current_seg_end = orig_s_end
                        
                    # Calculate overlap between report part (s_start, s_end) and segment
                    overlap = overlap_minutes(s_start, s_end, current_seg_start, current_seg_end)
                    if overlap <= 0:
                        continue
                        
                    minutes_covered += overlap

                    # שמירת אינטרוול מכוסה לחישוב "חורים" בהמשך
                    inter_start = max(s_start, current_seg_start)
                    inter_end = min(s_end, current_seg_end)
                    if inter_start < inter_end:
                        covered_intervals.append((inter_start, inter_end))

                    # Determine effective type
                    if is_sick_report:
                         effective_seg_type = "sick"
                    elif is_vacation_report:
                         effective_seg_type = "vacation"
                    else:
                         effective_seg_type = seg["segment_type"]
                    
                    if effective_seg_type == "standby":
                        label = "כוננות"
                    elif effective_seg_type == "vacation":
                        label = "חופשה"
                    elif effective_seg_type == "sick":
                        label = "מחלה"
                    elif seg["wage_percent"] is None:
                        # סגמנט דינמי - האחוז יחושב לפי מנגנון הרצפים/שבת
                        label = "work"
                    elif seg["wage_percent"] == 100:
                        label = "100%"
                    elif seg["wage_percent"] == 125:
                        label = "125%"
                    elif seg["wage_percent"] == 150:
                        label = "150%"
                    elif seg["wage_percent"] == 175:
                        label = "175%"
                    elif seg["wage_percent"] == 200:
                        label = "200%"
                    else:
                        label = f"{seg['wage_percent']}%"
                    
                    entry["buckets"].setdefault(label, 0)
                    entry["buckets"][label] += overlap
                    
                    # Calculate effective normalized start/end for the segment
                    eff_start_in_part = max(current_seg_start, s_start)
                    eff_end_in_part = min(current_seg_end, s_end)
                    
                    # Apply same normalization to segment boundaries
                    if s_end <= CUTOFF:
                        eff_start = eff_start_in_part + MINUTES_PER_DAY
                        eff_end = eff_end_in_part + MINUTES_PER_DAY
                    else:
                        eff_start = eff_start_in_part
                        eff_end = eff_end_in_part
                    
                    segment_id = seg.get("id")
                    apartment_type_id = r.get("apartment_type_id")
                    actual_apartment_type_id = r.get("actual_apartment_type_id")
                    is_married = r.get("is_married")
                    apartment_name = r.get("apartment_name", "")

                    # Store actual_date (p_date) for correct Shabbat calculation even when displayed under different day
                    # For standby segments, also store the defined end time (before min with report end)
                    # to detect early exit: if eff_end < standby_defined_end, it's early exit
                    standby_defined_end = current_seg_end if effective_seg_type == "standby" else None
                    entry["segments"].append((eff_start, eff_end, effective_seg_type, label, r["shift_type_id"], segment_id, apartment_type_id, is_married, apartment_name, p_date, actual_apartment_type_id, standby_defined_end))
                    
                # Uncovered minutes -> work
                # חישוב שעות עבודה שלא מכוסות ע"י סגמנטים מוגדרים
                total_part_minutes = s_end - s_start
                remaining = total_part_minutes - minutes_covered

                if remaining > 0:
                    # מיזוג אינטרוולים חופפים ומציאת זמנים לא מכוסים
                    merged_covered = merge_intervals(covered_intervals)
                    uncovered_intervals = find_uncovered_intervals(merged_covered, s_start, s_end)

                    # יצירת סגמנטי עבודה לכל זמן לא מכוסה
                    segment_id = None
                    apartment_type_id = r.get("apartment_type_id")
                    actual_apartment_type_id = r.get("actual_apartment_type_id") or apartment_type_id
                    is_married = r.get("is_married")
                    apartment_name = r.get("apartment_name", "")

                    for uncov_start, uncov_end in uncovered_intervals:
                        uncov_duration = uncov_end - uncov_start
                        if uncov_duration <= 0:
                            continue

                        # נרמול זמנים לפי יום עבודה
                        if s_end <= CUTOFF:
                            eff_uncov_start = uncov_start + MINUTES_PER_DAY
                            eff_uncov_end = uncov_end + MINUTES_PER_DAY
                        else:
                            eff_uncov_start = uncov_start
                            eff_uncov_end = uncov_end

                        # הוספת סגמנט עבודה - האחוז יחושב ע"י מנגנון הרצפים
                        entry["segments"].append((
                            eff_uncov_start, eff_uncov_end, "work", "work",
                            r["shift_type_id"], segment_id,
                            apartment_type_id, is_married,
                            apartment_name, p_date, actual_apartment_type_id, None  # standby_defined_end=None for work
                        ))

    # Process Daily Segments
    daily_segments = []

    # We need access to is_shabbat_time and calculate_wage_rate which are in logic.py
    # They are imported.

    # Track carryover minutes from previous day's chain ending
    # This is used when a work chain continues from 06:30-08:00 to 08:00-...
    # חישוב carryover מהחודש הקודם
    prev_month_carryover_minutes, prev_month_chain_end, prev_month_chain_shift_id, prev_month_night_minutes = _calculate_previous_month_carryover(conn, person_id, year, month, minimum_wage)
    prev_day_carryover_minutes = prev_month_carryover_minutes
    prev_day_chain_end_time = prev_month_chain_end  # זמן סיום הרצף מהחודש הקודם
    prev_day_chain_shift_id = prev_month_chain_shift_id  # shift_id של הרצף האחרון - לבדיקת שינוי תעריף
    prev_day_night_minutes = prev_month_night_minutes  # דקות לילה ברצף הקודם - לקביעת רצף לילה

    # לעקוב אחרי התאריך הקודם - מאתחלים ליום האחרון של החודש הקודם
    # כדי שהבדיקה הראשונה תזהה רציפות נכונה
    if month == 1:
        prev_day_date = date(year - 1, 12, 31)
    else:
        # מציאת היום האחרון של החודש הקודם
        first_of_month = date(year, month, 1)
        prev_day_date = first_of_month - timedelta(days=1)

    for day, entry in sorted(daily_map.items()):
        buckets = entry["buckets"]
        shift_names = sorted(entry["shifts"])
        day_shift_ids = entry.get("day_shift_types", set())  # IDs של המשמרות ביום הזה
        is_fixed_segments = entry.get("is_fixed_segments", False)

        day_parts = day.split("/")
        day_date = datetime(int(day_parts[2]), int(day_parts[1]), int(day_parts[0]), tzinfo=LOCAL_TZ).date()

        # בדיקה אם הימים רציפים - אם לא, לאפס carryover
        if prev_day_date is not None:
            days_diff = (day_date - prev_day_date).days
            if days_diff != 1:
                # הימים לא רציפים - אין carryover
                prev_day_carryover_minutes = 0
        
        # Prepare Hebrew Date and Day Name
        days_map = {0: "שני", 1: "שלישי", 2: "רביעי", 3: "חמישי", 4: "שישי", 5: "שבת", 6: "ראשון"}
        day_name_he = days_map.get(day_date.weekday(), "")
        
        h_year, h_month, h_day = hebrew.from_gregorian(day_date.year, day_date.month, day_date.day)
        hebrew_months = {
            1: "ניסן", 2: "אייר", 3: "סיוון", 4: "תמוז", 5: "אב", 6: "אלול",
            7: "תשרי", 8: "חשוון", 9: "כסלו", 10: "טבת", 11: "שבט", 12: "אדר",
            13: "אדר ב'"
        }
        month_name = hebrew_months.get(h_month, str(h_month))
        if h_month == 12 and hebrew.leap(h_year): month_name = "אדר א'"
        elif h_month == 13: month_name = "אדר ב'"
        hebrew_date_str = f"{to_gematria(h_day)} ב{month_name} {to_gematria(h_year)}"
        
        
        # Shabbat / Holiday name
        special_day_name = ""
        day_str = day_date.strftime("%Y-%m-%d")
        
        # Check current day for holiday or parsha
        day_info = shabbat_cache.get(day_str)
        if day_info:
            if day_info.get("holiday"):
                special_day_name = day_info["holiday"]
            elif day_info.get("parsha"):
                special_day_name = day_info["parsha"]
        
        # If Friday and no holiday found, check Saturday for parsha
        if not special_day_name and day_date.weekday() == 4: # Friday
            sat_date = day_date + timedelta(days=1)
            sat_str = sat_date.strftime("%Y-%m-%d")
            sat_info = shabbat_cache.get(sat_str)
            if sat_info and sat_info.get("parsha"):
                special_day_name = sat_info["parsha"]
        
        if special_day_name:
            day_name_he = f"{day_name_he}, {special_day_name}"
        
        # Sort and Dedup Segments
        # entry["segments"]: (start, end, type, label, shift_id, seg_id, apt_type, married, apt_name, actual_date)
        raw_segments = entry["segments"]

        work_segments = []
        standby_segments = []
        vacation_segments = []
        sick_segments = []

        for seg_entry in raw_segments:
            # Normalize length to 12 (now includes standby_defined_end for early exit detection)
            if len(seg_entry) < 12:
                # Pad with None
                seg_entry = seg_entry + (None,) * (12 - len(seg_entry))

            s_start, s_end, s_type, label, sid, seg_id, apt_type, married, apt_name, actual_date, actual_apt_type, standby_defined_end = seg_entry

            if s_type == "standby":
                # Include shift_type_id (sid) for priority selection when merging
                # Include standby_defined_end for early exit detection
                standby_segments.append((s_start, s_end, seg_id, apt_type, married, actual_date, sid, actual_apt_type, standby_defined_end))
            elif s_type == "vacation":
                vacation_segments.append((s_start, s_end, actual_date))
            elif s_type == "sick":
                sick_segments.append((s_start, s_end, actual_date))
            else:
                work_segments.append((s_start, s_end, label, sid, apt_name, actual_date, apt_type, actual_apt_type))
                
        work_segments.sort(key=lambda x: x[0])
        standby_segments.sort(key=lambda x: x[0])
        vacation_segments.sort(key=lambda x: x[0])
        sick_segments.sort(key=lambda x: x[0])
        
        # Dedup work - include shift_id to not merge different shifts at same time
        deduped = []
        seen = set()
        for w in work_segments:
            k = (w[0], w[1], w[3])  # (start, end, shift_id)
            if k not in seen:
                deduped.append(w)
                seen.add(k)
        work_segments = deduped  # Each is (start, end, label, sid, apt_name, actual_date, apt_type, actual_apt_type)

        # Note: Night chain detection is now done per-chain, not per-day
        # A chain is a "night chain" if it has 2+ hours in 22:00-06:00 range
        # This includes carryover hours from previous day/month

        # Dedup standby - now includes shift_type_id and standby_defined_end (9 elements)
        deduped_sb = []
        seen_sb = set()
        for sb in standby_segments:
            k = (sb[0], sb[1], sb[2])  # (start, end, seg_id)
            if k not in seen_sb:
                deduped_sb.append(sb)
                seen_sb.add(k)
        standby_segments = deduped_sb

        # Merge continuous standby segments BEFORE cancellation check
        # This ensures we check the FULL standby period, not individual fragments
        # Each standby keeps its original seg_id (from its shift type) for correct rate calculation
        # Also keep the max standby_defined_end for early exit detection
        standby_segments.sort(key=lambda x: x[0])
        merged_standbys = []

        for sb in standby_segments:
            sb_start, sb_end, seg_id, apt_type, married, actual_date, shift_type_id, actual_apt_type, standby_defined_end = sb

            if merged_standbys and sb_start <= merged_standbys[-1][1]:  # Overlapping or adjacent
                # Extend the previous merged standby, keep original seg_id
                # Keep the max standby_defined_end for early exit detection
                prev = merged_standbys[-1]
                new_defined_end = max(prev[8] or 0, standby_defined_end or 0) if (prev[8] or standby_defined_end) else None
                merged_standbys[-1] = (prev[0], max(prev[1], sb_end), prev[2], prev[3], prev[4], prev[5], prev[6], prev[7], new_defined_end)
            else:
                merged_standbys.append((sb_start, sb_end, seg_id, apt_type, married, actual_date, shift_type_id, actual_apt_type, standby_defined_end))

        # Standby Trim Logic - subtract work time from standby instead of cancelling
        # NEW: Early exit detection - if standby ends before its defined end due to early exit,
        # convert partial standby to work hours (continues the chain)
        cancelled_standbys = []
        trimmed_standbys = []
        early_exit_work_segments = []  # כוננויות חלקיות בגלל יציאה מוקדמת - יהפכו לעבודה

        for sb in merged_standbys:
            sb_start, sb_end, seg_id, apt_type, married, actual_date, shift_type_id, actual_apt_type, standby_defined_end = sb
            duration = sb_end - sb_start
            if duration <= 0: continue

            # Calculate total overlap with work
            total_overlap = 0
            for w in work_segments:
                total_overlap += overlap_minutes(sb_start, sb_end, w[0], w[1])

            ratio = total_overlap / duration if duration > 0 else 0

            # בדיקת יציאה מוקדמת: אם שעת סיום הכוננות בפועל < שעת סיום הכוננות המוגדרת
            # ואין עבודה שחופפת לכוננות = יציאה מוקדמת
            is_early_exit = (
                standby_defined_end is not None and
                sb_end < standby_defined_end and
                total_overlap == 0  # אין עבודה בתוך הכוננות
            )

            if is_early_exit:
                # יציאה מוקדמת - הכוננות החלקית הופכת לשעות עבודה שממשיכות את הרצף
                # הוספה לרשימת סגמנטי עבודה במקום כוננות
                early_exit_work_segments.append((
                    sb_start, sb_end, "כוננות חלקית", shift_type_id,
                    "", actual_date, apt_type, actual_apt_type
                ))
                # לא מוסיפים ל-trimmed_standbys ולא ל-cancelled_standbys
                continue

            if ratio >= STANDBY_CANCEL_OVERLAP_THRESHOLD:
                # כוננות מתבטלת - מורידים עד 70₪, משלמים את ההפרש
                standby_rate = get_standby_rate(conn, seg_id or 0, apt_type, bool(married), year, month) if seg_id else DEFAULT_STANDBY_RATE
                partial_pay = max(0, standby_rate - MAX_CANCELLED_STANDBY_DEDUCTION)

                # בחודשים 11/2025 ו-12/2025: אם הכוננות בוטלה בגלל חפיפה עם משמרת שמירה על דייר (149) - ביטול מלא ללא תשלום
                # אבל לא ביום שישי, שבת או חג
                NIGHT_WATCH_SHIFT_ID = 149  # שמירה על דייר בלילה
                if (year == 2025 and month in (11, 12)):
                    # בדיקה אם היום הוא לא שישי, שבת או חג
                    day_str = actual_date.strftime("%Y-%m-%d") if actual_date else None
                    day_info = shabbat_cache.get(day_str) if day_str else None
                    is_shabbat_or_holiday = actual_date and (
                        actual_date.weekday() in (FRIDAY, SATURDAY) or
                        (day_info and (day_info.get("enter") or day_info.get("exit")))
                    )
                    if not is_shabbat_or_holiday:
                        # בדיקה אם יש חפיפה עם משמרת שמירה על דייר
                        has_night_watch_overlap = any(
                            w[3] == NIGHT_WATCH_SHIFT_ID and overlap_minutes(sb_start, sb_end, w[0], w[1]) > 0
                            for w in work_segments
                        )
                        if has_night_watch_overlap:
                            partial_pay = 0

                reason = f"חפיפה ({int(ratio*100)}%)"
                if partial_pay > 0:
                    reason += f" - שולם {partial_pay:.0f}₪"
                cancelled_standbys.append({
                    "start": sb_start % MINUTES_PER_DAY,
                    "end": sb_end % MINUTES_PER_DAY,
                    "reason": reason,
                    "partial_pay": partial_pay
                })
            else:
                # Trim: subtract work segments from standby
                remaining_parts = [(sb_start, sb_end)]

                for w in work_segments:
                    w_start, w_end = w[0], w[1]
                    new_parts = []
                    for r_start, r_end in remaining_parts:
                        inter_start = max(r_start, w_start)
                        inter_end = min(r_end, w_end)

                        if inter_start < inter_end:
                            # There is overlap - subtract it
                            if r_start < inter_start:
                                new_parts.append((r_start, inter_start))
                            if inter_end < r_end:
                                new_parts.append((inter_end, r_end))
                        else:
                            # No overlap - keep as is
                            new_parts.append((r_start, r_end))
                    remaining_parts = new_parts

                # Add trimmed parts (keep shift_type_id, actual_apt_type, and standby_defined_end)
                for r_start, r_end in remaining_parts:
                    if r_end > r_start:
                        trimmed_standbys.append((r_start, r_end, seg_id, apt_type, married, actual_date, shift_type_id, actual_apt_type, standby_defined_end))

        standby_segments = trimmed_standbys

        # הוספת סגמנטי כוננות חלקית (יציאה מוקדמת) לרשימת העבודה
        # הם ייכנסו לרצף העבודה ויחושבו כשעות עבודה
        if early_exit_work_segments:
            work_segments.extend(early_exit_work_segments)
            work_segments.sort(key=lambda x: x[0])
        
        # Calculate Chains
        chains_detail = []

        # משמרת קבועה לגמרי = רק חופשה/מחלה (ללא משמרות עבודה כלל)
        # תגבור עכשיו חלק מהרצף הרגיל לחישוב שעות נוספות
        has_only_vacation_or_sick = is_fixed_segments and len(work_segments) == 0
        is_fully_fixed = has_only_vacation_or_sick

        d_calc100 = 0; d_calc125 = 0; d_calc150 = 0; d_calc175 = 0; d_calc200 = 0
        d_payment = 0; d_standby_pay = 0
        chains = []
        # cancelled_standbys נבנה למעלה בשלב ה-Standby Trim Logic - לא לאתחל מחדש!
        paid_standby_ids = set()  # Track paid standbys to avoid double payment

        # עיבוד חופשה/מחלה/כוננויות רק אם זה יום קבוע לגמרי (אין משמרות עבודה)
        if is_fully_fixed:
            # עיבוד סגמנטי חופשה
            for s, e, actual_date in vacation_segments:
                duration = e - s
                pay = (duration / 60) * minimum_wage  # חופשה = 100% שכר מינימום
                d_calc100 += duration
                d_payment += pay

                start_str = f"{s // 60 % 24:02d}:{s % 60:02d}"
                end_str = f"{e // 60 % 24:02d}:{e % 60:02d}"

                chains.append({
                    "start_time": start_str,
                    "end_time": end_str,
                    "total_minutes": duration,
                    "payment": pay,
                    "calc100": duration,
                    "calc125": 0, "calc150": 0, "calc175": 0, "calc200": 0,
                    "type": "vacation",
                    "apartment_name": "",
                    "shift_name": "חופשה",
                    "shift_type": "חופשה",
                    "segments": [(start_str, end_str, "חופשה")],
                    "break_reason": "",
                    "from_prev_day": False,
                    "effective_rate": minimum_wage,
                })

            # עיבוד סגמנטי מחלה - עם אחוזי תשלום מדורגים לפי חוק דמי מחלה
            for s, e, actual_date in sick_segments:
                duration = e - s

                # קביעת מספר יום המחלה ברצף ואחוז התשלום
                sick_date = actual_date.date() if isinstance(actual_date, datetime) else actual_date
                sick_day_num = sick_day_sequence.get(sick_date, 1)
                sick_rate = get_sick_payment_rate(sick_day_num)

                # חישוב תשלום לפי האחוז המדורג
                pay = (duration / 60) * minimum_wage * sick_rate
                d_calc100 += duration
                d_payment += pay

                start_str = f"{s // 60 % 24:02d}:{s % 60:02d}"
                end_str = f"{e // 60 % 24:02d}:{e % 60:02d}"

                chains.append({
                    "start_time": start_str,
                    "end_time": end_str,
                    "total_minutes": duration,
                    "payment": pay,
                    "calc100": duration,
                    "calc125": 0, "calc150": 0, "calc175": 0, "calc200": 0,
                    "type": "sick",
                    "apartment_name": "",
                    "shift_name": "מחלה",
                    "shift_type": "מחלה",
                    "segments": [(start_str, end_str, "מחלה")],
                    "break_reason": "",
                    "from_prev_day": False,
                    "effective_rate": minimum_wage,
                    "sick_day_number": sick_day_num,
                    "sick_rate_percent": int(sick_rate * 100),
                })

            # עיבוד כוננויות רק למשמרות תגבור (לא לחופשה/מחלה)
            is_tagbur = bool(day_shift_ids & TAGBUR_SHIFT_IDS)  # בדיקה לפי ID
            if is_tagbur and standby_segments:
                for sb_start, sb_end, seg_id, apt_type, married, actual_date, _shift_type_id, actual_apt_type, _standby_defined_end in standby_segments:
                    duration = sb_end - sb_start
                    if duration <= 0:
                        continue

                    # בדיקה אם כבר שילמנו על כוננות ביום הזה
                    # כוננות משולמת פעם אחת ליום לכל סוג דירה
                    standby_key = ("apt", apt_type)
                    if standby_key in paid_standby_ids:
                        continue  # כבר שולם, דלג

                    # חישוב תשלום כוננות (עם תמיכה בתעריפים היסטוריים)
                    standby_rate = get_standby_rate(conn, seg_id or 0, apt_type, bool(married), year, month) if seg_id else DEFAULT_STANDBY_RATE
                    d_standby_pay += standby_rate
                    paid_standby_ids.add(standby_key)

                    start_str = f"{sb_start // 60 % 24:02d}:{sb_start % 60:02d}"
                    end_str = f"{sb_end // 60 % 24:02d}:{sb_end % 60:02d}"

                    chains.append({
                        "start_time": start_str,
                        "end_time": end_str,
                        "total_minutes": duration,
                        "payment": standby_rate,
                        "calc100": 0, "calc125": 0, "calc150": 0, "calc175": 0, "calc200": 0,
                        "type": "standby",
                        "apartment_name": "",
                        "apartment_type_id": actual_apt_type,  # Use actual type for visual indicator
                        "shift_name": "כוננות",
                        "shift_type": "כוננות",
                        "segments": [(start_str, end_str, "כוננות")],
                        "break_reason": "",
                        "from_prev_day": False,
                        "effective_rate": 0,
                        "standby_rate": standby_rate,
                    })

            total_minutes = sum(w[1]-w[0] for w in work_segments) + sum(v[1]-v[0] for v in vacation_segments) + sum(s[1]-s[0] for s in sick_segments)

            # Add escort bonus payment (does NOT add to chain/carryover - bonus is separate from work hours)
            bonus_mins = entry.get("escort_bonus_minutes", 0)
            if bonus_mins > 0:
                # מציאת ה-chain של הליווי הרפואי לקבלת התעריף
                for chain in chains:
                    if chain.get("type") == "work" and chain.get("total_minutes", 0) < 60:
                        effective_rate = chain.get("effective_rate", minimum_wage)
                        bonus_pay = (bonus_mins / 60) * effective_rate

                        # תשלום בלבד - לא מוסיפים לדקות הרצף
                        d_payment += bonus_pay

                        # עדכון תשלום ה-chain והערה על הבונוס (לתצוגה בלבד)
                        chain["payment"] += bonus_pay
                        chain["escort_bonus_pay"] = bonus_pay  # שמירת הבונוס לצבירה חודשית
                        if chain.get("segments"):
                            old_seg = chain["segments"][0]
                            start_time = old_seg[0]
                            end_time = old_seg[1]
                            chain["segments"] = [(start_time, end_time, f"100% (+ בונוס {bonus_mins} דק')")]
                        break

            # Add partial payments from cancelled standbys (when standby > 70₪)
            cancelled_partial_pay = sum(c.get("partial_pay", 0) for c in cancelled_standbys)
            d_standby_pay += cancelled_partial_pay

            # מיון chains לפי זמן התחלה ביום עבודה (08:00-08:00)
            def fixed_chain_sort_key(c):
                t = c.get("start_time", "00:00")
                h, m = map(int, t.split(":"))
                minutes = h * 60 + m
                # יום עבודה מתחיל ב-08:00 (480 דקות)
                # זמנים 00:00-07:59 הם בעצם 24:00-31:59 ביום העבודה
                if minutes < 480:  # לפני 08:00
                    minutes += MINUTES_PER_DAY
                return minutes

            chains.sort(key=fixed_chain_sort_key)

            daily_segments.append({
                "day": day,
                "day_name": day_name_he,
                "hebrew_date": hebrew_date_str,
                "date_obj": day_date,
                "payment": d_payment,
                "standby_payment": d_standby_pay,
                "calc100": d_calc100, "calc125": d_calc125, "calc150": d_calc150, "calc175": d_calc175, "calc200": d_calc200,
                "shift_names": shift_names,
                "has_work": len(work_segments) > 0,
                "total_minutes": total_minutes,
                "total_minutes_no_standby": total_minutes,
                "buckets": buckets,
                "chains": chains,
                "cancelled_standbys": cancelled_standbys,
            })
            continue  # דלג לסיבוב הבא - כבר סיימנו את היום הזה

        # Merge all events for processing - כל המשמרות כולל תגבור
        all_events = []
        for s, e, l, sid, apt_name, actual_date, apt_type, actual_apt_type in work_segments:
            all_events.append({"start": s, "end": e, "type": "work", "label": l, "shift_id": sid, "apartment_name": apt_name or "", "apartment_type_id": actual_apt_type, "rate_apt_type": apt_type, "actual_date": actual_date or day_date})
        for s, e, seg_id, apt, married, actual_date, _shift_type_id, actual_apt_type, _standby_defined_end in standby_segments:
            all_events.append({"start": s, "end": e, "type": "standby", "label": "כוננות", "seg_id": seg_id, "apt": apt, "actual_apt_type": actual_apt_type, "married": married, "actual_date": actual_date or day_date})
        for s, e, actual_date in vacation_segments:
            all_events.append({"start": s, "end": e, "type": "vacation", "label": "חופשה", "actual_date": actual_date or day_date})
        for s, e, actual_date in sick_segments:
            all_events.append({"start": s, "end": e, "type": "sick", "label": "מחלה", "actual_date": actual_date or day_date})

        all_events.sort(key=lambda x: x["start"])

        # Build a set of work segment boundaries for quick lookup
        # This helps determine if standby truly breaks the chain or if work continues through it
        work_starts = {ws[0] for ws in work_segments}  # All work start times
        work_ends = {ws[1] for ws in work_segments}    # All work end times

        # Process chains logic - כל המשמרות כולל תגבור מחושבות ברצף אחד

        current_chain_segments = []
        last_end = None
        last_etype = None

        def calculate_chain_pay(segments, minutes_offset=0, carryover_night_minutes=0):
            # segments is list of (start, end, label, shift_id, apartment_name, actual_date, apt_type, actual_apt_type, rate_apt_type)
            # Convert to format expected by _calculate_chain_wages: (start, end, shift_id, actual_date)
            # Include actual_date for each segment for correct Shabbat calculation
            chain_segs = [(s, e, sid, adate) for s, e, l, sid, apt, adate, apt_type, actual_apt_type, rate_apt_type in segments]

            # Calculate night hours in current chain segments
            # Times are in extended 00:00-32:00 axis (0-1920 minutes)
            # where 1440+ represents next day (00:00-08:00 after midnight)
            current_chain_night_minutes = 0
            for s, e, l, sid, apt, adate, apt_type, actual_apt_type, rate_apt_type in segments:
                # Convert from extended 00:00-32:00 axis to 00:00-24:00 axis
                real_start = s % 1440
                real_end = e % 1440
                # Handle overnight segments (when end wraps around to next day)
                if real_end <= real_start and e > s:
                    real_end += 1440
                current_chain_night_minutes += calculate_night_hours_in_segment(real_start, real_end)

            # Total night minutes in chain = carryover + current
            total_chain_night_minutes = carryover_night_minutes + current_chain_night_minutes

            # A chain is a "night chain" if it has 2+ hours (120 min) in 22:00-06:00 range
            chain_is_night = total_chain_night_minutes >= NIGHT_HOURS_THRESHOLD

            # Use optimized block calculation with carryover offset
            # Pass night chain flag for 7-hour workday threshold
            # Each segment includes its actual_date for correct Shabbat boundary calculation
            result = _calculate_chain_wages(chain_segs, shabbat_cache, minutes_offset, chain_is_night)

            c_100 = result["calc100"]
            c_125 = result["calc125"]
            c_150 = result["calc150"]
            c_175 = result["calc175"]
            c_200 = result["calc200"]
            seg_detail = result.get("segments_detail", [])

            # Get effective rate from first segment's shift_id (all segments in chain should have same rate)
            first_shift_id = segments[0][3] if segments else None
            effective_rate = shift_rates.get(first_shift_id, minimum_wage)

            # משמרת ליווי בי"ח (120): בשבת הלכתית משתמשים בשכר מינימום
            # seg_detail = [(start_min, end_min, label, is_shabbat), ...]
            if is_hospital_escort_shift(first_shift_id) and seg_detail:
                c_pay = 0
                for seg_start, seg_end, seg_label, is_shabbat in seg_detail:
                    seg_minutes = seg_end - seg_start
                    # קביעת תעריף: שבת = מינימום, חול = תעריף מיוחד
                    seg_rate = minimum_wage if is_shabbat else effective_rate
                    # קביעת מכפיל לפי אחוז
                    if "200%" in seg_label:
                        multiplier = 2.0
                    elif "175%" in seg_label:
                        multiplier = 1.75
                    elif "150%" in seg_label:
                        multiplier = 1.5
                    elif "125%" in seg_label:
                        multiplier = 1.25
                    else:
                        multiplier = 1.0
                    c_pay += (seg_minutes / 60) * multiplier * seg_rate
            else:
                c_pay = (c_100/60*1.0 + c_125/60*1.25 + c_150/60*1.5 + c_175/60*1.75 + c_200/60*2.0) * effective_rate

            return c_pay, c_100, c_125, c_150, c_175, c_200, seg_detail, effective_rate

        def close_chain_and_record(segments, break_reason="", minutes_offset=0, carryover_night_minutes=0):
            """Close current chain and add to chains list.
            Each rate segment becomes a separate row in chains.
            Returns (pay, c100, c125, c150, c175, c200, chain_total_minutes, chain_ends_at_0800, chain_night_minutes)"""
            if not segments:
                return 0, 0, 0, 0, 0, 0, 0, False, 0

            pay, c100, c125, c150, c175, c200, seg_detail, effective_rate = calculate_chain_pay(segments, minutes_offset, carryover_night_minutes)

            # Calculate total chain duration (including offset from previous day)
            chain_duration = sum(e - s for s, e, l, sid, apt, adate, apt_type, actual_apt_type, rate_apt_type in segments)
            chain_total_minutes = minutes_offset + chain_duration

            # Get apartment names and types from segments - segments is (start, end, label, sid, apt_name, actual_date, apt_type, actual_apt_type, rate_apt_type)
            chain_apartments = set()
            chain_shift_names = set()
            chain_apt_types = set()
            chain_shift_ids = set()
            chain_actual_apt_types = set()
            chain_rate_apt_types = set()
            for s, e, l, sid, apt, adate, apt_type, actual_apt_type, rate_apt_type in segments:
                if apt:
                    chain_apartments.add(apt)
                if apt_type:
                    chain_apt_types.add(apt_type)
                if actual_apt_type:
                    chain_actual_apt_types.add(actual_apt_type)
                if rate_apt_type:
                    chain_rate_apt_types.add(rate_apt_type)
                if sid:
                    chain_shift_ids.add(sid)
                    shift_name = shift_names_map.get(sid, "")
                    if shift_name:
                        chain_shift_names.add(shift_name)
            apt_name = ", ".join(sorted(chain_apartments)) if chain_apartments else ""
            # Use the first (or only) apartment type for the chain
            chain_apt_type = list(chain_apt_types)[0] if chain_apt_types else None
            chain_actual_apt = list(chain_actual_apt_types)[0] if chain_actual_apt_types else None
            chain_rate_apt = list(chain_rate_apt_types)[0] if chain_rate_apt_types else None
            chain_shift_id = list(chain_shift_ids)[0] if chain_shift_ids else None
            # שם המשמרת הספציפי של ה-chain (לא כל המשמרות של היום)
            shift_name_str = shift_names_map.get(chain_shift_id, "") if chain_shift_id else ""

            # Helper function: Split a rate segment by apartment boundaries
            def split_segment_by_apartments(seg_start, seg_end, seg_label, is_shabbat, segs):
                """
                פיצול סגמנט לפי גבולות דירות.
                אם יש כמה דירות באותו טווח זמן, מחזיר רשימת תת-סגמנטים.
                """
                result_segments = []
                current_start = seg_start

                # מיון הסגמנטים המקוריים לפי זמן התחלה
                sorted_segs = sorted(segs, key=lambda x: x[0])

                for s, e, l, sid, apt, adate, apt_type, actual_apt_type, rate_apt_type in sorted_segs:
                    # בדיקה אם יש חפיפה עם הטווח הנוכחי
                    if s < seg_end and e > current_start:
                        # זמן התחלה של החפיפה
                        overlap_start = max(current_start, s)
                        # זמן סיום של החפיפה
                        overlap_end = min(seg_end, e)

                        if overlap_end > overlap_start:
                            result_segments.append({
                                "start": overlap_start,
                                "end": overlap_end,
                                "label": seg_label,
                                "is_shabbat": is_shabbat,
                                "apt_name": apt,
                                "apt_type": apt_type,
                                "actual_apt_type": actual_apt_type,
                                "rate_apt_type": rate_apt_type,
                                "shift_id": sid
                            })
                            current_start = overlap_end

                    if current_start >= seg_end:
                        break

                # אם לא נמצאו חפיפות, החזר סגמנט בודד עם ברירת מחדל
                if not result_segments:
                    if segs:
                        s, e, l, sid, apt, adate, apt_type, actual_apt_type, rate_apt_type = segs[0]
                        result_segments.append({
                            "start": seg_start,
                            "end": seg_end,
                            "label": seg_label,
                            "is_shabbat": is_shabbat,
                            "apt_name": apt,
                            "apt_type": apt_type,
                            "actual_apt_type": actual_apt_type,
                            "rate_apt_type": rate_apt_type,
                            "shift_id": sid
                        })
                    else:
                        result_segments.append({
                            "start": seg_start,
                            "end": seg_end,
                            "label": seg_label,
                            "is_shabbat": is_shabbat,
                            "apt_name": "",
                            "apt_type": None,
                            "actual_apt_type": None,
                            "rate_apt_type": None,
                            "shift_id": None
                        })

                return result_segments

            # Create a separate chain row for each rate segment, split by apartment
            # First, expand all segments by apartment boundaries
            expanded_segments = []
            for seg_start, seg_end, seg_label, is_shabbat in seg_detail:
                sub_segments = split_segment_by_apartments(seg_start, seg_end, seg_label, is_shabbat, segments)
                expanded_segments.extend(sub_segments)

            # Now create chain rows from expanded segments
            prev_seg_label = None
            prev_seg_apt_name = None
            prev_seg_shift_id = None

            for i, sub_seg in enumerate(expanded_segments):
                is_first = (i == 0)
                is_last = (i == len(expanded_segments) - 1)

                seg_start = sub_seg["start"]
                seg_end = sub_seg["end"]
                seg_label = sub_seg["label"]
                is_shabbat = sub_seg["is_shabbat"]
                seg_apt_name = sub_seg["apt_name"]
                seg_apt_type = sub_seg["apt_type"]
                seg_actual_apt = sub_seg["actual_apt_type"]
                seg_rate_apt = sub_seg["rate_apt_type"]
                seg_shift_id = sub_seg["shift_id"]

                seg_duration = seg_end - seg_start

                # Calculate payment and counts for this segment based on its label
                seg_c100, seg_c125, seg_c150, seg_c175, seg_c200 = 0, 0, 0, 0, 0
                seg_c150_shabbat, seg_c150_overtime = 0, 0
                if "100%" in seg_label:
                    seg_c100 = seg_duration
                elif "125%" in seg_label:
                    seg_c125 = seg_duration
                elif "150%" in seg_label:
                    seg_c150 = seg_duration
                    # Check if Shabbat or overtime
                    if is_shabbat:
                        seg_c150_shabbat = seg_duration
                    else:
                        seg_c150_overtime = seg_duration
                elif "175%" in seg_label:
                    seg_c175 = seg_duration
                elif "200%" in seg_label:
                    seg_c200 = seg_duration

                # משמרת ליווי בי"ח (120): בשבת הלכתית משתמשים בשכר מינימום
                seg_rate = minimum_wage if (is_hospital_escort_shift(seg_shift_id) and is_shabbat) else effective_rate
                seg_pay = (seg_c100/60*1.0 + seg_c125/60*1.25 + seg_c150/60*1.5 + seg_c175/60*1.75 + seg_c200/60*2.0) * seg_rate

                start_str = f"{seg_start // 60 % 24:02d}:{seg_start % 60:02d}"
                end_str = f"{seg_end // 60 % 24:02d}:{seg_end % 60:02d}"

                # Determine shift type label (לפי ID, לא לפי שם)
                # השתמש ב-shift_id של הסגמנט הספציפי, לא הרצף כולו
                current_shift_id = seg_shift_id or chain_shift_id
                current_actual_apt = seg_actual_apt if seg_actual_apt is not None else chain_actual_apt
                current_rate_apt = seg_rate_apt if seg_rate_apt is not None else chain_rate_apt

                # קביעת תווית סוג המשמרת
                # is_shabbat מציין אם הזמן בפועל הוא בשבת (לפי כניסה/יציאה)
                # זה חשוב יותר מסוג המשמרת כי משמרת שבת יכולה להמשיך אחרי צאת שבת
                shift_type_label = ""
                if is_tagbur_shift(current_shift_id):
                    shift_type_label = "תגבור"
                elif is_implicit_tagbur(current_shift_id, current_actual_apt, current_rate_apt):
                    # משמרת שישי/שבת בדירה טיפולית עם תעריף דירה רגילה = תגבור
                    shift_type_label = "תגבור"
                elif is_night_shift(current_shift_id):
                    shift_type_label = "לילה"
                elif is_shabbat:
                    # הזמן בפועל הוא בתוך שבת (לפי שעות כניסה/יציאה)
                    shift_type_label = "שבת"
                else:
                    # אחרי צאת שבת או יום חול רגיל
                    shift_type_label = "חול"

                # השתמש בדירה הספציפית של הסגמנט, לא של הרצף כולו
                display_apt_name = seg_apt_name if seg_apt_name else apt_name
                display_apt_type = seg_apt_type if seg_apt_type is not None else chain_apt_type

                # קביעת סיבת מעבר שורה (אם לא השורה הראשונה)
                # מציג את הסיבה בשורה הנוכחית (למה התחלנו שורה חדשה)
                row_split_reason = ""
                if not is_first:
                    # בדיקת סיבות למעבר שורה
                    if prev_seg_label != seg_label:
                        # שינוי אחוז
                        row_split_reason = f"מעבר ל-{seg_label}"
                    elif prev_seg_apt_name and seg_apt_name and prev_seg_apt_name != seg_apt_name:
                        # שינוי דירה
                        row_split_reason = "דירה אחרת"
                    elif prev_seg_shift_id and seg_shift_id and prev_seg_shift_id != seg_shift_id:
                        # שינוי משמרת
                        row_split_reason = "משמרת אחרת"

                # שמירת ערכים לסגמנט הבא
                prev_seg_label = seg_label
                prev_seg_apt_name = seg_apt_name
                prev_seg_shift_id = seg_shift_id

                # קביעת הערה סופית - סיבת מעבר או סיבת שבירה (בשורה האחרונה)
                final_reason = row_split_reason
                if is_last and break_reason:
                    # בשורה האחרונה, אם יש סיבת שבירה, היא עדיפה
                    final_reason = break_reason

                # שם המשמרת הספציפי של הסגמנט (לא כל המשמרות של היום)
                seg_shift_name = shift_names_map.get(current_shift_id, "") if current_shift_id else shift_name_str

                chains.append({
                    "start_time": start_str,
                    "end_time": end_str,
                    "total_minutes": seg_duration,
                    "payment": seg_pay,
                    "calc100": seg_c100,
                    "calc125": seg_c125,
                    "calc150": seg_c150,
                    "calc150_shabbat": seg_c150_shabbat,
                    "calc150_overtime": seg_c150_overtime,
                    "calc175": seg_c175,
                    "calc200": seg_c200,
                    "type": "work",
                    "apartment_name": display_apt_name,
                    "apartment_type_id": display_apt_type,
                    "shift_name": seg_shift_name,
                    "shift_type": shift_type_label,
                    "shift_id": current_shift_id,  # For identifying special shifts like medical escort
                    "is_special_hourly": shift_is_special_hourly.get(chain_shift_id, False),  # For variable rate tracking
                    "segments": [(start_str, end_str, seg_label)],
                    "break_reason": final_reason,
                    "from_prev_day": (seg_start >= MINUTES_PER_DAY) if is_first else False,
                    "effective_rate": effective_rate,
                })

            # Check if chain ends at 08:00 boundary (1920 = 08:00 + 1440)
            # This indicates the chain continues to the next workday
            chain_ends_at_0800 = (segments[-1][1] == 1920) if segments else False

            # Calculate night minutes in this chain (for carryover to next day)
            # Times are in extended 00:00-32:00 axis (0-1920 minutes)
            chain_night_minutes = 0
            for s, e, l, sid, apt, adate, apt_type, actual_apt_type, rate_apt_type in segments:
                # Convert from extended 00:00-32:00 axis to 00:00-24:00 axis
                real_start = s % 1440
                real_end = e % 1440
                # Handle overnight segments (when end wraps around to next day)
                if real_end <= real_start and e > s:
                    real_end += 1440
                chain_night_minutes += calculate_night_hours_in_segment(real_start, real_end)
            # Include carryover night minutes in the total
            chain_night_minutes += carryover_night_minutes

            return pay, c100, c125, c150, c175, c200, chain_total_minutes, chain_ends_at_0800, chain_night_minutes

        # Determine if we should use carryover from previous day
        # Carryover applies if the gap between previous chain end and first work start is <= 60 minutes
        first_work_start = None
        first_work_shift_id = None
        for evt in all_events:
            if evt["type"] == "work":
                first_work_start = evt["start"]
                first_work_shift_id = evt.get("shift_id")
                break

        use_carryover = False
        rate_changed_from_prev_day = False
        if first_work_start is not None and prev_day_carryover_minutes > 0:
            # בדיקת הפסקה בין סוף הרצף הקודם לתחילת העבודה היום
            # prev_day_chain_end_time הוא בציר מנורמל (08:00 = 480, אחרי חצות +1440)
            # first_work_start הוא גם בציר מנורמל
            # אם הרצף הקודם הסתיים ב-1920 (08:00) והיום מתחיל ב-480 (08:00), ההפסקה היא 0
            # אם הרצף הקודם הסתיים ב-1890 (07:30) והיום מתחיל ב-480 (08:00), ההפסקה היא 30 דקות

            # המרה לציר אחיד: סוף יום קודם הוא ביחס ל-1440 (תחילת יום חדש)
            # first_work_start הוא בציר של היום הנוכחי (מתחיל מ-480)
            # צריך להשוות: (first_work_start + 1440) - prev_day_chain_end_time
            # או: first_work_start - (prev_day_chain_end_time - 1440)

            prev_end_in_new_day = prev_day_chain_end_time - 1440  # המרה לציר היום הבא
            gap_minutes = first_work_start - prev_end_in_new_day

            # אם ההפסקה היא 60 דקות או פחות, הרצף נמשך
            use_carryover = (gap_minutes <= BREAK_THRESHOLD_MINUTES)

            # בדיקה אם התעריף השתנה בין הרצף הקודם לרצף הנוכחי
            # שינוי תעריף לא שובר את הרצף לגמרי - הוא מעביר את ה-offset
            if use_carryover and prev_day_chain_shift_id is not None and first_work_shift_id is not None:
                prev_rate = shift_rates.get(prev_day_chain_shift_id, minimum_wage)
                first_rate = shift_rates.get(first_work_shift_id, minimum_wage)
                if prev_rate != first_rate:
                    rate_changed_from_prev_day = True

        current_offset = prev_day_carryover_minutes if use_carryover else 0
        current_night_minutes = prev_day_night_minutes if use_carryover else 0  # Night minutes from carryover

        # Reset carryover tracking for this day
        day_carryover_for_next = 0
        last_chain_ended_at_0800 = False
        last_chain_total = 0
        last_chain_night_minutes = 0  # Track night minutes for carryover to next day

        # Re-process chains with proper carryover
        # We need to re-process since the first chain might need offset
        current_chain_segments = []
        last_end = None
        last_etype = None
        d_calc100 = 0; d_calc125 = 0; d_calc150 = 0; d_calc175 = 0; d_calc200 = 0
        d_payment = 0
        chains = []  # Reset chains list
        first_chain_of_day = True

        for event in all_events:
            start, end, etype = event["start"], event["end"], event["type"]
            is_special = etype in ("standby", "vacation", "sick")

            should_break = False
            break_reason = ""
            if current_chain_segments:
                if is_special:
                    # כוננות שוברת רצף רק אם אין עבודה שחופפת לה או ממשיכה אחריה
                    # בדיקה: האם יש עבודה שמסתיימת אחרי תחילת הכוננות או מתחילה לפני סוף הכוננות?
                    standby_overlaps_work = any(
                        ws[0] < end and ws[1] > start  # עבודה חופפת לכוננות
                        for ws in work_segments
                    )
                    if standby_overlaps_work:
                        # יש עבודה שחופפת לכוננות - לא לשבור רצף
                        should_break = False
                    else:
                        should_break = True
                        break_reason = etype
                elif last_end is not None and (start - last_end) > BREAK_THRESHOLD_MINUTES:
                    should_break = True
                    break_reason = f"הפסקה ({start - last_end} דקות)"

            # בדיקה נוספת: האם התעריף משתנה?
            # אם הסגמנט החדש הוא עם תעריף שונה מהסגמנטים הקודמים ב-chain, צריך לסגור את ה-chain
            if not is_special and current_chain_segments and not should_break:
                new_shift_id = event.get("shift_id")
                new_rate = shift_rates.get(new_shift_id, minimum_wage)
                # בדיקת התעריף של ה-chain הנוכחי
                current_shift_id = current_chain_segments[0][3] if current_chain_segments else None
                current_rate = shift_rates.get(current_shift_id, minimum_wage)
                if new_rate != current_rate:
                    should_break = True
                    break_reason = "שינוי תעריף"

            if should_break:
                chain_offset = current_offset
                chain_night_offset = current_night_minutes
                pay, c100, c125, c150, c175, c200, chain_total, ends_at_0800, chain_night = close_chain_and_record(
                    current_chain_segments, break_reason, chain_offset, chain_night_offset)
                d_payment += pay
                d_calc100 += c100; d_calc125 += c125; d_calc150 += c150; d_calc175 += c175; d_calc200 += c200

                # Track last chain info for potential carryover to next day
                last_chain_total = chain_total
                last_chain_ended_at_0800 = ends_at_0800
                last_chain_night_minutes = chain_night

                # אם נשבר בגלל שינוי תעריף, צריך להעביר את ה-minutes offset ל-chain הבא
                # כי ה-overtime נמשך על פני כל יום העבודה
                # גם שעות הלילה מועברות כי הרצף ממשיך
                if break_reason == "שינוי תעריף":
                    current_offset = chain_total  # ה-offset לchain הבא כולל את כל הדקות עד עכשיו
                    current_night_minutes = chain_night  # גם שעות הלילה מועברות
                else:
                    current_offset = 0  # הפסקה/כוננות מאפסת את ה-offset
                    current_night_minutes = 0  # גם שעות הלילה מתאפסות

                current_chain_segments = []
                first_chain_of_day = False

            if is_special:
                if etype == "standby":
                    is_cont = (last_etype == "standby" and last_end == start)

                    # בדיקה אם כבר שילמנו על כוננות ביום הזה
                    # כוננות משולמת פעם אחת ליום לכל סוג דירה
                    apt_type = event.get("apt")
                    standby_key = ("apt", apt_type)
                    already_paid = standby_key in paid_standby_ids

                    if not is_cont and not already_paid:
                        rate = get_standby_rate(conn, event.get("seg_id") or 0, apt_type, bool(event.get("married")), year, month)
                        d_standby_pay += rate
                        paid_standby_ids.add(standby_key)

                    chains.append({
                        "start_time": f"{start // 60 % 24:02d}:{start % 60:02d}",
                        "end_time": f"{end // 60 % 24:02d}:{end % 60:02d}",
                        "total_minutes": end - start,
                        "payment": 0,
                        "calc100": 0, "calc125": 0, "calc150": 0, "calc175": 0, "calc200": 0,
                        "type": "standby",
                        "apartment_name": event.get("apartment_name", ""),
                        "apartment_type_id": event.get("actual_apt_type"),  # Use actual type for visual indicator
                        "shift_name": "כוננות",
                        "shift_type": "כוננות",
                        "segments": [],
                        "break_reason": "",
                        "from_prev_day": start >= MINUTES_PER_DAY,
                        "effective_rate": minimum_wage,
                    })
                elif etype == "vacation" or etype == "sick":
                    duration = end - start
                    hrs = duration / 60
                    pay = hrs * minimum_wage
                    d_payment += pay
                    d_calc100 += duration  # מחלה/חופשה = 100%

                    label = "חופשה" if etype == "vacation" else "מחלה"
                    chains.append({
                        "start_time": f"{start // 60 % 24:02d}:{start % 60:02d}",
                        "end_time": f"{end // 60 % 24:02d}:{end % 60:02d}",
                        "total_minutes": duration,
                        "payment": pay,
                        "calc100": duration, "calc125": 0, "calc150": 0, "calc175": 0, "calc200": 0,
                        "type": etype,  # "vacation" או "sick"
                        "apartment_name": "",
                        "apartment_type_id": None,
                        "shift_name": label,
                        "shift_type": label,
                        "segments": [(f"{start // 60 % 24:02d}:{start % 60:02d}", f"{end // 60 % 24:02d}:{end % 60:02d}", label)],
                        "break_reason": "",
                        "from_prev_day": start >= MINUTES_PER_DAY,
                        "effective_rate": minimum_wage,
                    })

                last_end = end
                last_etype = etype
            else:
                # segments: (start, end, label, shift_id, apt_name, actual_date, apt_type, actual_apt_type, rate_apt_type)
                # apt_type = rate_apt_type (לחישוב), actual_apt_type = apartment_type_id (להצגה)
                current_chain_segments.append((start, end, event["label"], event["shift_id"], event.get("apartment_name", ""), event.get("actual_date"), event.get("rate_apt_type"), event.get("apartment_type_id"), event.get("rate_apt_type")))
                last_end = end
                last_etype = etype

        # Close last chain
        if current_chain_segments:
            chain_offset = current_offset
            chain_night_offset = current_night_minutes
            pay, c100, c125, c150, c175, c200, chain_total, ends_at_0800, chain_night = close_chain_and_record(
                current_chain_segments, "", chain_offset, chain_night_offset)
            d_payment += pay
            d_calc100 += c100; d_calc125 += c125; d_calc150 += c150; d_calc175 += c175; d_calc200 += c200

            # Track for potential carryover
            last_chain_total = chain_total
            last_chain_ended_at_0800 = ends_at_0800
            last_chain_night_minutes = chain_night

        # Update carryover for next day
        # If the last chain ended at 08:00 (1920 normalized), save its total for next day
        if last_chain_ended_at_0800:
            prev_day_carryover_minutes = last_chain_total
            prev_day_chain_end_time = 1920  # 08:00 normalized
            prev_day_night_minutes = last_chain_night_minutes  # Night minutes for next day's chain
        else:
            prev_day_carryover_minutes = 0
            prev_day_chain_end_time = 0
            prev_day_night_minutes = 0
            
        # Calculate total_minutes
        total_minutes = sum(w[1]-w[0] for w in work_segments)
        for sb in standby_segments:
            total_minutes += sb[1] - sb[0]

        # מיון chains לפי זמן התחלה ביום עבודה (08:00-08:00)
        # זמנים לפני 08:00 שייכים לסוף יום העבודה ולכן ממוינים אחרי זמנים מ-08:00+
        def chain_sort_key(c):
            t = c.get("start_time", "00:00")
            h, m = map(int, t.split(":"))
            minutes = h * 60 + m
            # יום עבודה מתחיל ב-08:00 (480 דקות)
            # זמנים 00:00-07:59 הם בעצם 24:00-31:59 ביום העבודה
            if minutes < 480:  # לפני 08:00
                minutes += MINUTES_PER_DAY
            return minutes

        chains.sort(key=chain_sort_key)

        # Add escort bonus payment (does NOT add to chain/carryover - bonus is separate from work hours)
        bonus_mins = entry.get("escort_bonus_minutes", 0)
        if bonus_mins > 0:
            # מציאת ה-chain של הליווי הרפואי לקבלת התעריף
            for chain in chains:
                if chain.get("type") == "work" and chain.get("total_minutes", 0) < 60:
                    effective_rate = chain.get("effective_rate", minimum_wage)
                    bonus_pay = (bonus_mins / 60) * effective_rate

                    # תשלום בלבד - לא מוסיפים לדקות הרצף
                    d_payment += bonus_pay

                    # עדכון תשלום ה-chain והערה על הבונוס (לתצוגה בלבד)
                    chain["payment"] += bonus_pay
                    chain["escort_bonus_pay"] = bonus_pay  # שמירת הבונוס לצבירה חודשית
                    if chain.get("segments"):
                        old_seg = chain["segments"][0]
                        start_time = old_seg[0]
                        end_time = old_seg[1]
                        chain["segments"] = [(start_time, end_time, f"100% (+ בונוס {bonus_mins} דק')")]
                    break

        # Add partial payments from cancelled standbys (when standby > 70₪)
        cancelled_partial_pay = sum(c.get("partial_pay", 0) for c in cancelled_standbys)
        d_standby_pay += cancelled_partial_pay

        daily_segments.append({
            "day": day,
            "day_name": day_name_he,
            "hebrew_date": hebrew_date_str,
            "date_obj": day_date,
            "payment": d_payment,
            "standby_payment": d_standby_pay,
            "calc100": d_calc100, "calc125": d_calc125, "calc150": d_calc150, "calc175": d_calc175, "calc200": d_calc200,
            "shift_names": shift_names,
            "has_work": len(work_segments) > 0,
            "total_minutes": total_minutes,
            "total_minutes_no_standby": sum(w[1]-w[0] for w in work_segments),
            "buckets": buckets,
            "chains": chains,
            "cancelled_standbys": cancelled_standbys,
        })

        # עדכון התאריך הקודם לסיבוב הבא
        prev_day_date = day_date

    return daily_segments, reports[0]["person_name"] if reports else ""


def aggregate_daily_segments_to_monthly(
    conn,
    daily_segments: List[Dict],
    person_id: int,
    year: int,
    month: int,
    minimum_wage: float
) -> Dict[str, Any]:
    """
    מאחד את כל הנתונים מ-daily_segments למילון monthly_totals.
    זהו מקור האמת היחיד לחישוב שכר - מחליף את calculate_person_monthly_totals.

    Args:
        conn: חיבור לדאטבייס
        daily_segments: רשימת ימים עם פירוט הרצפים (מ-get_daily_segments_data)
        person_id: מזהה העובד
        year: שנה
        month: חודש
        minimum_wage: שכר מינימום לחודש

    Returns:
        מילון monthly_totals עם כל השדות הנדרשים לכל הטאבים
    """
    from utils.utils import calculate_accruals
    from datetime import datetime
    from zoneinfo import ZoneInfo

    LOCAL_TZ = ZoneInfo("Asia/Jerusalem")

    # אתחול סיכומים
    monthly_totals = {
        # שעות לפי אחוזים (בדקות)
        "calc100": 0,
        "calc125": 0,
        "calc150": 0,
        "calc150_shabbat": 0,
        "calc150_shabbat_100": 0,
        "calc150_shabbat_50": 0,
        "calc150_overtime": 0,
        "calc175": 0,
        "calc200": 0,
        "calc_variable": 0,

        # תשלומים לפי אחוזים
        "payment_calc100": 0.0,
        "payment_calc125": 0.0,
        "payment_calc150": 0.0,
        "payment_calc150_overtime": 0.0,
        "payment_calc150_shabbat": 0.0,
        "payment_calc175": 0.0,
        "payment_calc200": 0.0,
        "payment_calc_variable": 0.0,

        # סיכומים
        "total_hours": 0,
        "payment": 0.0,
        "standby": 0,
        "standby_payment": 0.0,

        # חופשה ומחלה
        "vacation_minutes": 0,
        "vacation_payment": 0.0,
        "vacation": 0,
        "vacation_days_taken": 0,
        "sick_minutes": 0,
        "sick_payment": 0.0,
        "sick_days_taken": 0,
        "sick_days_accrued": 0.0,
        "vacation_days_accrued": 0.0,

        # נסיעות ותוספות
        "travel": 0.0,
        "extras": 0.0,

        # ימי עבודה
        "actual_work_days": 0,

        # תעריף משתנה - מבנה חדש לתמיכה במספר תעריפים
        "variable_rate_value": minimum_wage,
        "variable_rate_extra_payment": 0.0,
        "variable_rates": {},  # {rate_value: {calc100, calc125, calc150, calc175, calc200, payment}}
    }

    # ספירת ימי עבודה, חופשה ומחלה
    work_days_set = set()
    vacation_days_set = set()
    sick_days_set = set()
    standby_days_set = set()

    # עיבוד כל הימים
    for day in daily_segments:
        day_date = day.get("date_obj")

        # ספירת ימי עבודה
        if day.get("has_work"):
            work_days_set.add(day_date)

        # צבירת סיכומים יומיים
        monthly_totals["payment"] += day.get("payment", 0) or 0
        monthly_totals["standby_payment"] += day.get("standby_payment", 0) or 0

        # עיבוד רצפים (chains) לחישוב מדויק של שעות ותשלומים
        for chain in day.get("chains", []):
            chain_type = chain.get("type", "work")
            effective_rate = chain.get("effective_rate", minimum_wage)

            # תעריף משתנה = משמרת עם תעריף שעתי מיוחד (is_special_hourly),
            # או תעריף שונה משכר מינימום
            is_special_hourly = chain.get("is_special_hourly", False)
            is_variable_rate = is_special_hourly or abs(effective_rate - minimum_wage) > 0.01

            if chain_type == "work":
                # אתחול מילון לתעריף משתנה אם צריך
                if is_variable_rate:
                    rate_key = round(effective_rate, 2)
                    if rate_key not in monthly_totals["variable_rates"]:
                        monthly_totals["variable_rates"][rate_key] = {
                            "calc100": 0, "calc125": 0, "calc150": 0,
                            "calc175": 0, "calc200": 0, "payment": 0.0
                        }

                # שעות רגילות (100%)
                c100 = chain.get("calc100", 0) or 0
                if c100 > 0:
                    if is_variable_rate:
                        monthly_totals["calc_variable"] += c100
                        monthly_totals["payment_calc_variable"] += (c100 / 60) * 1.0 * effective_rate
                        monthly_totals["variable_rate_value"] = effective_rate
                        # שמירה גם במבנה החדש
                        monthly_totals["variable_rates"][rate_key]["calc100"] += c100
                        monthly_totals["variable_rates"][rate_key]["payment"] += (c100 / 60) * 1.0 * effective_rate
                    else:
                        monthly_totals["calc100"] += c100
                        monthly_totals["payment_calc100"] += (c100 / 60) * 1.0 * effective_rate

                # שעות נוספות 125%
                c125 = chain.get("calc125", 0) or 0
                if c125 > 0:
                    if is_variable_rate:
                        monthly_totals["calc_variable"] += c125
                        monthly_totals["payment_calc_variable"] += (c125 / 60) * 1.25 * effective_rate
                        monthly_totals["variable_rate_value"] = effective_rate
                        # שמירה גם במבנה החדש
                        monthly_totals["variable_rates"][rate_key]["calc125"] += c125
                        monthly_totals["variable_rates"][rate_key]["payment"] += (c125 / 60) * 1.25 * effective_rate
                    else:
                        monthly_totals["calc125"] += c125
                        monthly_totals["payment_calc125"] += (c125 / 60) * 1.25 * effective_rate

                # שעות נוספות 150% (כולל הפרדה בין חול לשבת)
                c150 = chain.get("calc150", 0) or 0
                c150_shabbat = chain.get("calc150_shabbat", 0) or 0
                c150_overtime = chain.get("calc150_overtime", 0) or 0

                if c150 > 0:
                    if is_variable_rate:
                        monthly_totals["calc_variable"] += c150
                        monthly_totals["payment_calc_variable"] += (c150 / 60) * 1.5 * effective_rate
                        monthly_totals["variable_rate_value"] = effective_rate
                        # שמירה גם במבנה החדש
                        monthly_totals["variable_rates"][rate_key]["calc150"] += c150
                        monthly_totals["variable_rates"][rate_key]["payment"] += (c150 / 60) * 1.5 * effective_rate
                    else:
                        monthly_totals["calc150"] += c150
                        monthly_totals["payment_calc150"] += (c150 / 60) * 1.5 * effective_rate

                        # הפרדה בין שבת לחול
                        if c150_shabbat > 0:
                            monthly_totals["calc150_shabbat"] += c150_shabbat
                            monthly_totals["calc150_shabbat_100"] += c150_shabbat
                            monthly_totals["calc150_shabbat_50"] += c150_shabbat
                            monthly_totals["payment_calc150_shabbat"] += (c150_shabbat / 60) * 1.5 * effective_rate
                        if c150_overtime > 0:
                            monthly_totals["calc150_overtime"] += c150_overtime
                            monthly_totals["payment_calc150_overtime"] += (c150_overtime / 60) * 1.5 * effective_rate

                # שעות שבת 175%
                c175 = chain.get("calc175", 0) or 0
                if c175 > 0:
                    if is_variable_rate:
                        monthly_totals["calc_variable"] += c175
                        monthly_totals["payment_calc_variable"] += (c175 / 60) * 1.75 * effective_rate
                        monthly_totals["variable_rate_value"] = effective_rate
                        # שמירה גם במבנה החדש
                        monthly_totals["variable_rates"][rate_key]["calc175"] += c175
                        monthly_totals["variable_rates"][rate_key]["payment"] += (c175 / 60) * 1.75 * effective_rate
                    else:
                        monthly_totals["calc175"] += c175
                        monthly_totals["payment_calc175"] += (c175 / 60) * 1.75 * effective_rate

                # שעות שבת 200%
                c200 = chain.get("calc200", 0) or 0
                if c200 > 0:
                    if is_variable_rate:
                        monthly_totals["calc_variable"] += c200
                        monthly_totals["payment_calc_variable"] += (c200 / 60) * 2.0 * effective_rate
                        monthly_totals["variable_rate_value"] = effective_rate
                        # שמירה גם במבנה החדש
                        monthly_totals["variable_rates"][rate_key]["calc200"] += c200
                        monthly_totals["variable_rates"][rate_key]["payment"] += (c200 / 60) * 2.0 * effective_rate
                    else:
                        monthly_totals["calc200"] += c200
                        monthly_totals["payment_calc200"] += (c200 / 60) * 2.0 * effective_rate

                # בונוס ליווי רפואי (תשלום בלבד, לא נספר בשעות)
                escort_bonus = chain.get("escort_bonus_pay", 0) or 0
                if escort_bonus > 0:
                    if is_variable_rate:
                        monthly_totals["payment_calc_variable"] += escort_bonus
                        monthly_totals["variable_rates"][rate_key]["payment"] += escort_bonus
                    else:
                        monthly_totals["payment_calc100"] += escort_bonus

            elif chain_type == "standby":
                standby_days_set.add(day_date)

            elif chain_type == "vacation":
                vacation_days_set.add(day_date)
                vacation_mins = chain.get("total_minutes", 0) or 0
                monthly_totals["vacation_minutes"] += vacation_mins

            elif chain_type == "sick":
                sick_days_set.add(day_date)
                sick_mins = chain.get("total_minutes", 0) or 0
                sick_pay = chain.get("payment", 0) or 0
                monthly_totals["sick_minutes"] += sick_mins
                monthly_totals["sick_payment"] += sick_pay

    # חישוב סך שעות עבודה (ללא כוננויות)
    monthly_totals["total_hours"] = sum(
        day.get("total_minutes_no_standby", 0) or 0
        for day in daily_segments
    )

    # ספירת כוננויות
    monthly_totals["standby"] = len(standby_days_set)

    # ימי עבודה בפועל (כולל חופשה ומחלה)
    monthly_totals["actual_work_days"] = len(work_days_set | vacation_days_set | sick_days_set)

    # ימי חופשה שנוצלו
    monthly_totals["vacation_days_taken"] = len(vacation_days_set)

    # תשלום חופשה
    monthly_totals["vacation_payment"] = (monthly_totals["vacation_minutes"] / 60) * minimum_wage
    monthly_totals["vacation"] = monthly_totals["vacation_minutes"]

    # ימי מחלה שנוצלו (התשלום כבר חושב בלולאה עם האחוזים המדורגים)
    monthly_totals["sick_days_taken"] = len(sick_days_set)

    # שליפת נסיעות ותוספות מהדאטבייס
    month_start = datetime(year, month, 1, tzinfo=LOCAL_TZ)
    if month == 12:
        month_end = datetime(year + 1, 1, 1, tzinfo=LOCAL_TZ)
    else:
        month_end = datetime(year, month + 1, 1, tzinfo=LOCAL_TZ)

    payment_comps = conn.execute("""
        SELECT (quantity * rate) as total_amount, component_type_id
        FROM payment_components
        WHERE person_id = %s AND date >= %s AND date < %s
    """, (person_id, month_start, month_end)).fetchall()

    for pc in payment_comps:
        amount = (pc["total_amount"] or 0) / 100
        if pc["component_type_id"] == 2 or pc["component_type_id"] == 7:
            monthly_totals["travel"] += amount
        else:
            monthly_totals["extras"] += amount

    # שליפת פרטי העובד לחישוב צבירות
    person = conn.execute(
        "SELECT start_date FROM people WHERE id = %s", (person_id,)
    ).fetchone()

    # חישוב צבירות (מחלה וחופשה)
    if person:
        accruals = calculate_accruals(
            actual_work_days=monthly_totals["actual_work_days"],
            start_date_ts=person["start_date"],
            report_year=year,
            report_month=month
        )
        monthly_totals["sick_days_accrued"] = accruals.get("sick_days_accrued", 0)
        monthly_totals["vacation_days_accrued"] = accruals.get("vacation_days_accrued", 0)
        monthly_totals["vacation_details"] = accruals.get("vacation_details", {
            "seniority": 1,
            "annual_quota": 12,
            "job_scope_pct": 100
        })
    else:
        monthly_totals["vacation_details"] = {
            "seniority": 1,
            "annual_quota": 12,
            "job_scope_pct": 100
        }

    # תשלום סופי כולל - מחושב מהרכיבים המפורטים (לא מ-payment שכולל כפילויות)
    # payment_calc100/125/150/175/200 = עבודה בשכר מינימום
    # variable_rates = עבודה בתעריפים מיוחדים
    # vacation_payment = ימי חופשה
    # sick_payment = ימי מחלה
    variable_rates_total = sum(
        data.get("payment", 0) for data in monthly_totals["variable_rates"].values()
    )
    monthly_totals["total_payment"] = (
        monthly_totals["payment_calc100"] +
        monthly_totals["payment_calc125"] +
        monthly_totals["payment_calc150"] +
        monthly_totals["payment_calc175"] +
        monthly_totals["payment_calc200"] +
        variable_rates_total +
        monthly_totals["vacation_payment"] +
        monthly_totals["sick_payment"] +
        monthly_totals["standby_payment"] +
        monthly_totals["travel"] +
        monthly_totals["extras"]
    )

    # שמירת שכר אפקטיבי
    monthly_totals["effective_hourly_rate"] = minimum_wage

    # חישוב סה"כ מעוגל - שעות מעוגלות × תעריף = סכום
    # זה מבטיח שסכום השורות = סה"כ לתשלום
    rounded_total = 0.0

    # calc100: שעות מעוגלות × תעריף
    calc100_hours = round(monthly_totals["calc100"] / 60, 2)
    rounded_total += calc100_hours * minimum_wage

    # calc125: שעות מעוגלות × תעריף × 1.25
    calc125_hours = round(monthly_totals["calc125"] / 60, 2)
    rounded_total += calc125_hours * minimum_wage * 1.25

    # calc150_overtime: שעות מעוגלות × תעריף × 1.5
    calc150_overtime_hours = round(monthly_totals.get("calc150_overtime", 0) / 60, 2)
    rounded_total += calc150_overtime_hours * minimum_wage * 1.5

    # calc150_shabbat: שעות מעוגלות × תעריף × 1.5
    calc150_shabbat_hours = round(monthly_totals.get("calc150_shabbat", 0) / 60, 2)
    rounded_total += calc150_shabbat_hours * minimum_wage * 1.5

    # calc175: שעות מעוגלות × תעריף × 1.75
    calc175_hours = round(monthly_totals["calc175"] / 60, 2)
    rounded_total += calc175_hours * minimum_wage * 1.75

    # calc200: שעות מעוגלות × תעריף × 2.0
    calc200_hours = round(monthly_totals["calc200"] / 60, 2)
    rounded_total += calc200_hours * minimum_wage * 2.0

    # calc_variable: שעות מעוגלות × תעריף אפקטיבי מעוגל
    calc_variable_hours = round(monthly_totals["calc_variable"] / 60, 2)
    if calc_variable_hours > 0:
        effective_var_rate = round(monthly_totals["payment_calc_variable"] / calc_variable_hours, 2)
        rounded_total += calc_variable_hours * effective_var_rate

    # סכומים ישירים (לא צריכים עיגול של שעות)
    rounded_total += monthly_totals["vacation_payment"]
    rounded_total += monthly_totals["sick_payment"]
    rounded_total += monthly_totals["standby_payment"]
    rounded_total += monthly_totals["travel"]
    rounded_total += monthly_totals["extras"]

    monthly_totals["rounded_total"] = round(rounded_total, 2)

    return monthly_totals
