
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
    is_implicit_tagbur,
    qualifies_as_night_shift,
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
    chain_segments: List[Tuple[int, int, int]],
    day_date: date,
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
        chain_segments: List of (start_min, end_min, shift_id) tuples
        day_date: The date for Shabbat calculation
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

    weekday = day_date.weekday()
    is_fri_or_sat = weekday in (FRIDAY, SATURDAY)

    # Get Shabbat boundaries if relevant
    shabbat_enter, shabbat_exit = (-1, -1)
    if is_fri_or_sat:
        shabbat_enter, shabbat_exit = _get_shabbat_boundaries(day_date, shabbat_cache)

    # Flatten all segments into a list of (abs_start, abs_end) in continuous minutes
    # and calculate total chain minutes
    total_chain_minutes = 0
    flat_segments = []

    for seg_start, seg_end, seg_shift_id in chain_segments:
        flat_segments.append((seg_start, seg_end))
        total_chain_minutes += (seg_end - seg_start)

    # Process in blocks based on overtime thresholds
    # Use night shift thresholds if applicable (7 hours instead of 8)
    regular_limit = NIGHT_REGULAR_HOURS_LIMIT if is_night_shift else REGULAR_HOURS_LIMIT
    overtime_limit = NIGHT_OVERTIME_125_LIMIT if is_night_shift else OVERTIME_125_LIMIT
    # Start from offset if this chain continues from previous day
    minutes_processed = minutes_offset

    for seg_start, seg_end in flat_segments:
        seg_duration = seg_end - seg_start
        seg_offset = 0

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

            # Now check Shabbat boundaries within this block
            if is_fri_or_sat:
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
                # - יום שישי: offset = 0 (או 1440 אם חצה חצות = שבת בבוקר)
                # - יום שבת: offset = 1440
                # - יום ראשון בוקר (זמנים >= 1440 מנורמלים ליום שבת): offset = 2880
                day_offset_start = 0
                day_offset_end = 0
                if weekday == FRIDAY:
                    # אם הזמן מתחיל אחרי חצות ביום שישי, זה בעצם שבת בבוקר
                    if block_abs_start >= MINUTES_PER_DAY:
                        day_offset_start = MINUTES_PER_DAY
                    # אם הזמן נגמר אחרי חצות (ומתחיל לפני), זה עובר לשבת בבוקר
                    # שימוש ב-> ולא >= כי 1440 בדיוק (חצות) הוא עדיין סוף יום שישי
                    if block_abs_end > MINUTES_PER_DAY:
                        day_offset_end = MINUTES_PER_DAY
                elif weekday == SATURDAY:
                    day_offset_start = MINUTES_PER_DAY
                    day_offset_end = MINUTES_PER_DAY
                    # אם הזמן המקורי חצה חצות (>=1440), זה בעצם יום ראשון בבוקר
                    if block_abs_start >= MINUTES_PER_DAY:
                        day_offset_start = 2 * MINUTES_PER_DAY
                    if block_abs_end >= MINUTES_PER_DAY:
                        day_offset_end = 2 * MINUTES_PER_DAY

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

    # Find standby segment_id for each Shabbat shift (for rate priority)
    shabbat_standby_seg_ids = {}  # shift_type_id -> standby segment_id
    for shift_id in shabbat_shifts:
        if shift_id in segments_by_shift:
            for seg in segments_by_shift[shift_id]:
                if seg.get("segment_type") == "standby":
                    shabbat_standby_seg_ids[shift_id] = seg.get("id")
                    break
        
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
            seg_list = [{
                "start_time": r["start_time"],
                "end_time": r["end_time"],
                "wage_percent": 100,
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
                    "id": None
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

            for seg in seg_list:
                seg_start, seg_end = span_minutes(seg["start_time"], seg["end_time"])
                duration = seg_end - seg_start

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

                entry["segments"].append((seg_start, seg_end, effective_seg_type, label, r["shift_type_id"], segment_id, apartment_type_id, is_married, apartment_name, r_date, actual_apartment_type_id))

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
                # אבל רק אם זה המשך של משמרת (לא דיווח עצמאי שמתחיל בחצות)
                # דיווח עצמאי = הדיווח המקורי התחיל בחצות (00:00) ביום הנוכחי
                is_standalone_midnight_shift = (s_start == 0 and p_date == r_date and rep_start_orig == 0)
                if s_end <= CUTOFF and not is_standalone_midnight_shift:
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
                    entry["segments"].append((eff_start, eff_end, effective_seg_type, label, r["shift_type_id"], segment_id, apartment_type_id, is_married, apartment_name, p_date, actual_apartment_type_id))
                    
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
                            apartment_name, p_date, actual_apartment_type_id
                        ))

    # Process Daily Segments
    daily_segments = []

    # We need access to is_shabbat_time and calculate_wage_rate which are in logic.py
    # They are imported.

    # Track carryover minutes from previous day's chain ending at 08:00
    # This is used when a work chain continues from 06:30-08:00 to 08:00-...
    prev_day_carryover_minutes = 0
    prev_day_date = None  # לעקוב אחרי התאריך הקודם

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
            # Normalize length to 11 (now includes actual_apartment_type_id for visual indicator)
            if len(seg_entry) < 11:
                # Pad with None
                seg_entry = seg_entry + (None,) * (11 - len(seg_entry))

            s_start, s_end, s_type, label, sid, seg_id, apt_type, married, apt_name, actual_date, actual_apt_type = seg_entry

            if s_type == "standby":
                # Include shift_type_id (sid) for priority selection when merging
                standby_segments.append((s_start, s_end, seg_id, apt_type, married, actual_date, sid, actual_apt_type))
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

        # Check if this day qualifies as night shift (2+ hours in 22:00-06:00)
        # Night shifts use 7-hour workday instead of 8-hour for overtime calculation
        dayis_night_shift = qualifies_as_night_shift([(w[0], w[1]) for w in work_segments])

        # Dedup standby - now includes shift_type_id (7 elements)
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
        # When merging, prefer Shabbat/holiday shift's seg_id for rate calculation
        standby_segments.sort(key=lambda x: x[0])
        merged_standbys = []

        # Check if this day has any Shabbat shift - if so, use Shabbat standby rate for all standbys
        day_shift_types = entry.get("day_shift_types", set())
        day_has_shabbat = bool(day_shift_types & shabbat_shifts)
        shabbat_standby_seg_id = None
        if day_has_shabbat:
            # Find the standby seg_id from a Shabbat shift
            for st_id in (day_shift_types & shabbat_shifts):
                if st_id in shabbat_standby_seg_ids:
                    shabbat_standby_seg_id = shabbat_standby_seg_ids[st_id]
                    break

        for sb in standby_segments:
            sb_start, sb_end, seg_id, apt_type, married, actual_date, shift_type_id, actual_apt_type = sb

            # If day has Shabbat and we found a Shabbat standby seg_id, use it
            if shabbat_standby_seg_id is not None:
                seg_id = shabbat_standby_seg_id

            if merged_standbys and sb_start <= merged_standbys[-1][1]:  # Overlapping or adjacent
                # Extend the previous merged standby (seg_id already corrected above)
                prev = merged_standbys[-1]
                merged_standbys[-1] = (prev[0], max(prev[1], sb_end), seg_id, apt_type, married, actual_date, shift_type_id, actual_apt_type)
            else:
                merged_standbys.append((sb_start, sb_end, seg_id, apt_type, married, actual_date, shift_type_id, actual_apt_type))

        # Standby Trim Logic - subtract work time from standby instead of cancelling
        cancelled_standbys = []
        trimmed_standbys = []
        for sb in merged_standbys:
            sb_start, sb_end, seg_id, apt_type, married, actual_date, shift_type_id, actual_apt_type = sb
            duration = sb_end - sb_start
            if duration <= 0: continue

            # Calculate total overlap with work
            total_overlap = 0
            for w in work_segments:
                total_overlap += overlap_minutes(sb_start, sb_end, w[0], w[1])

            ratio = total_overlap / duration if duration > 0 else 0

            if ratio >= STANDBY_CANCEL_OVERLAP_THRESHOLD:
                # כוננות מתבטלת - מורידים עד 70₪, משלמים את ההפרש
                standby_rate = get_standby_rate(conn, seg_id or 0, apt_type, bool(married), year, month) if seg_id else DEFAULT_STANDBY_RATE
                partial_pay = max(0, standby_rate - MAX_CANCELLED_STANDBY_DEDUCTION)

                if sb_start % MINUTES_PER_DAY > 0:
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

                # Add trimmed parts (keep shift_type_id and actual_apt_type)
                for r_start, r_end in remaining_parts:
                    if r_end > r_start:
                        trimmed_standbys.append((r_start, r_end, seg_id, apt_type, married, actual_date, shift_type_id, actual_apt_type))

        standby_segments = trimmed_standbys
        
        # Calculate Chains
        chains_detail = []

        # משמרת קבועה (ערב שבת/חג) - לא מחשבים רצף, משתמשים באחוזים הקבועים מהסגמנטים
        if is_fixed_segments:
            d_calc100 = 0; d_calc125 = 0; d_calc150 = 0; d_calc175 = 0; d_calc200 = 0
            d_payment = 0; d_standby_pay = 0
            chains = []
            cancelled_standbys = []
            paid_standby_ids = set()  # Track paid standbys to avoid double payment

            for s, e, label, sid, apt_name, actual_date, apt_type, actual_apt_type in work_segments:
                duration = e - s
                # Get effective hourly rate for this shift (uses custom rate if defined)
                effective_rate = shift_rates.get(sid, minimum_wage)
                shift_name_str = shift_names_map.get(sid, "") if sid else ""

                # Determine shift type label (לפי ID, לא לפי שם)
                shift_type_label = ""
                if is_tagbur_shift(sid):
                    shift_type_label = "תגבור"
                elif is_implicit_tagbur(sid, actual_apt_type, apt_type):
                    # משמרת שישי/שבת בדירה טיפולית עם תעריף דירה רגילה = תגבור
                    shift_type_label = "תגבור"
                elif is_night_shift(sid):
                    shift_type_label = "לילה"
                elif is_shabbat_shift(sid):
                    shift_type_label = "שבת"
                else:
                    shift_type_label = "חול"

                # חישוב לפי האחוז הקבוע
                if "100%" in label:
                    d_calc100 += duration
                    pay = (duration / 60) * 1.0 * effective_rate
                elif "125%" in label:
                    d_calc125 += duration
                    pay = (duration / 60) * 1.25 * effective_rate
                elif "150%" in label:
                    d_calc150 += duration
                    pay = (duration / 60) * 1.5 * effective_rate
                elif "175%" in label:
                    d_calc175 += duration
                    pay = (duration / 60) * 1.75 * effective_rate
                elif "200%" in label:
                    d_calc200 += duration
                    pay = (duration / 60) * 2.0 * effective_rate
                else:
                    d_calc100 += duration
                    pay = (duration / 60) * 1.0 * effective_rate

                d_payment += pay

                start_str = f"{s // 60 % 24:02d}:{s % 60:02d}"
                end_str = f"{e // 60 % 24:02d}:{e % 60:02d}"

                chains.append({
                    "start_time": start_str,
                    "end_time": end_str,
                    "total_minutes": duration,
                    "payment": pay,
                    "calc100": duration if "100%" in label else 0,
                    "calc125": duration if "125%" in label else 0,
                    "calc150": duration if "150%" in label else 0,
                    "calc175": duration if "175%" in label else 0,
                    "calc200": duration if "200%" in label else 0,
                    "type": "work",
                    "apartment_name": apt_name or "",
                    "apartment_type_id": actual_apt_type,  # Use actual type for visual indicator
                    "shift_name": shift_name_str,
                    "shift_type": shift_type_label,
                    "shift_id": sid,  # For identifying special shifts like medical escort
                    "is_special_hourly": shift_is_special_hourly.get(sid, False),  # For variable rate tracking
                    "segments": [(start_str, end_str, label)],
                    "break_reason": "",
                    "from_prev_day": False,
                    "effective_rate": effective_rate,
                })

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
                for sb_start, sb_end, seg_id, apt_type, married, actual_date, _shift_type_id, actual_apt_type in standby_segments:
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

            # Add escort bonus minutes to calc100, d_payment, total_minutes, and the relevant chain
            bonus_mins = entry.get("escort_bonus_minutes", 0)
            if bonus_mins > 0:
                # מציאת ה-chain של הליווי הרפואי ועדכון שלו
                for chain in chains:
                    if chain.get("type") == "work" and chain.get("total_minutes", 0) < 60:
                        # משתמשים בתעריף האפקטיבי של ה-chain (לא בהכרח שכר מינימום)
                        effective_rate = chain.get("effective_rate", minimum_wage)
                        bonus_pay = (bonus_mins / 60) * effective_rate

                        d_calc100 += bonus_mins
                        d_payment += bonus_pay
                        total_minutes += bonus_mins

                        chain["total_minutes"] += bonus_mins
                        chain["calc100"] += bonus_mins
                        chain["payment"] += bonus_pay
                        # עדכון פירוט המקטעים - שומרים על השעות המקוריות, רק מוסיפים הערה על הבונוס
                        if chain.get("segments"):
                            old_seg = chain["segments"][0]
                            start_time = old_seg[0]
                            end_time = old_seg[1]
                            chain["segments"] = [(start_time, end_time, f"100% (כולל בונוס {bonus_mins} דק')")]
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
                "total_minutes_no_standby": total_minutes,
                "buckets": buckets,
                "chains": chains,
                "cancelled_standbys": cancelled_standbys,
            })
            continue  # דלג לסיבוב הבא - כבר סיימנו את היום הזה

        # Merge all events for processing
        all_events = []
        for s, e, l, sid, apt_name, actual_date, apt_type, actual_apt_type in work_segments:
            all_events.append({"start": s, "end": e, "type": "work", "label": l, "shift_id": sid, "apartment_name": apt_name or "", "apartment_type_id": actual_apt_type, "rate_apt_type": apt_type, "actual_date": actual_date or day_date})
        for s, e, seg_id, apt, married, actual_date, _shift_type_id, actual_apt_type in standby_segments:
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

        # Process chains logic (Simplified version of guide_view logic for brevity, 
        # but needs to match calculations)
        # ... copying the chain processing logic is complex.
        # Can we simplify? The request is for "Simple View".
        # We need "Payment" per day to be accurate.
        
        # To reuse the exact logic, we should probably COPY the logic from guide_view exactly.
        # Since I'm creating a new file `app_utils.py`, I can put the full logic here.
        
        # ... (Include full chain processing logic here) ...
        # For the sake of the tool call size, I will abbreviate the chain logic construction
        # but ensure payment calculation is done.
        
        current_chain_segments = []
        last_end = None
        last_etype = None
        
        # Accumulators
        d_calc100 = 0; d_calc125 = 0; d_calc150 = 0; d_calc175 = 0; d_calc200 = 0
        d_payment = 0; d_standby_pay = 0
        chains = []  # List of chain objects for display
        paid_standby_ids = set()  # Track paid standbys to avoid double payment

        def calculate_chain_pay(segments, minutes_offset=0):
            # segments is list of (start, end, label, shift_id, apartment_name, actual_date, apt_type, actual_apt_type, rate_apt_type)
            # Convert to format expected by _calculate_chain_wages: (start, end, shift_id)
            chain_segs = [(s, e, sid) for s, e, l, sid, apt, adate, apt_type, actual_apt_type, rate_apt_type in segments]

            # Use display day_date for Shabbat calculation
            # The display date is the actual calendar date when work was performed
            # (e.g., Saturday 08/11 even if the report started on Friday 07/11)
            calc_date = day_date

            # Use optimized block calculation with carryover offset
            # Pass night shift flag for 7-hour workday threshold
            result = _calculate_chain_wages(chain_segs, calc_date, shabbat_cache, minutes_offset, dayis_night_shift)

            c_100 = result["calc100"]
            c_125 = result["calc125"]
            c_150 = result["calc150"]
            c_175 = result["calc175"]
            c_200 = result["calc200"]
            seg_detail = result.get("segments_detail", [])

            # Get effective rate from first segment's shift_id (all segments in chain should have same rate)
            first_shift_id = segments[0][3] if segments else None
            effective_rate = shift_rates.get(first_shift_id, minimum_wage)
            
            c_pay = (c_100/60*1.0 + c_125/60*1.25 + c_150/60*1.5 + c_175/60*1.75 + c_200/60*2.0) * effective_rate
            return c_pay, c_100, c_125, c_150, c_175, c_200, seg_detail, effective_rate

        def close_chain_and_record(segments, break_reason="", minutes_offset=0):
            """Close current chain and add to chains list.
            Each rate segment becomes a separate row in chains.
            Returns (pay, c100, c125, c150, c175, c200, chain_total_minutes, chain_ends_at_0800)"""
            if not segments:
                return 0, 0, 0, 0, 0, 0, 0, False

            pay, c100, c125, c150, c175, c200, seg_detail, effective_rate = calculate_chain_pay(segments, minutes_offset)

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
            shift_name_str = ", ".join(sorted(chain_shift_names)) if chain_shift_names else ""

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

                seg_pay = (seg_c100/60*1.0 + seg_c125/60*1.25 + seg_c150/60*1.5 + seg_c175/60*1.75 + seg_c200/60*2.0) * effective_rate

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
                    "shift_name": shift_name_str,
                    "shift_type": shift_type_label,
                    "shift_id": chain_shift_id,  # For identifying special shifts like medical escort
                    "is_special_hourly": shift_is_special_hourly.get(chain_shift_id, False),  # For variable rate tracking
                    "segments": [(start_str, end_str, seg_label)],
                    "break_reason": break_reason if is_last else "",
                    "from_prev_day": (seg_start >= MINUTES_PER_DAY) if is_first else False,
                    "effective_rate": effective_rate,
                })

            # Check if chain ends at 08:00 boundary (1920 = 08:00 + 1440)
            # This indicates the chain continues to the next workday
            chain_ends_at_0800 = (segments[-1][1] == 1920) if segments else False

            return pay, c100, c125, c150, c175, c200, chain_total_minutes, chain_ends_at_0800

        # Determine if we should use carryover from previous day
        # Carryover applies if first work event starts at 08:00 (480 minutes)
        first_work_start = None
        for evt in all_events:
            if evt["type"] == "work":
                first_work_start = evt["start"]
                break

        use_carryover = (first_work_start == 480 and prev_day_carryover_minutes > 0)
        current_offset = prev_day_carryover_minutes if use_carryover else 0

        # Reset carryover tracking for this day
        day_carryover_for_next = 0
        last_chain_ended_at_0800 = False
        last_chain_total = 0

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
                pay, c100, c125, c150, c175, c200, chain_total, ends_at_0800 = close_chain_and_record(
                    current_chain_segments, break_reason, chain_offset)
                d_payment += pay
                d_calc100 += c100; d_calc125 += c125; d_calc150 += c150; d_calc175 += c175; d_calc200 += c200

                # Track last chain info for potential carryover to next day
                last_chain_total = chain_total
                last_chain_ended_at_0800 = ends_at_0800

                # אם נשבר בגלל שינוי תעריף, צריך להעביר את ה-minutes offset ל-chain הבא
                # כי ה-overtime נמשך על פני כל יום העבודה
                if break_reason == "שינוי תעריף":
                    current_offset = chain_total  # ה-offset לchain הבא כולל את כל הדקות עד עכשיו
                else:
                    current_offset = 0  # הפסקה/כוננות מאפסת את ה-offset

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
            pay, c100, c125, c150, c175, c200, chain_total, ends_at_0800 = close_chain_and_record(
                current_chain_segments, "", chain_offset)
            d_payment += pay
            d_calc100 += c100; d_calc125 += c125; d_calc150 += c150; d_calc175 += c175; d_calc200 += c200

            # Track for potential carryover
            last_chain_total = chain_total
            last_chain_ended_at_0800 = ends_at_0800

        # Update carryover for next day
        # If the last chain ended at 08:00 (1920 normalized), save its total for next day
        if last_chain_ended_at_0800:
            prev_day_carryover_minutes = last_chain_total
        else:
            prev_day_carryover_minutes = 0
            
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

        # Add escort bonus minutes to calc100, d_payment, total_minutes, and the relevant chain
        bonus_mins = entry.get("escort_bonus_minutes", 0)
        if bonus_mins > 0:
            # מציאת ה-chain של הליווי הרפואי ועדכון שלו
            # ה-chain הראשון של היום שהוא מסוג work עם פחות משעה הוא הליווי הרפואי
            for chain in chains:
                if chain.get("type") == "work" and chain.get("total_minutes", 0) < 60:
                    # משתמשים בתעריף האפקטיבי של ה-chain (לא בהכרח שכר מינימום)
                    effective_rate = chain.get("effective_rate", minimum_wage)
                    bonus_pay = (bonus_mins / 60) * effective_rate

                    d_calc100 += bonus_mins
                    d_payment += bonus_pay
                    total_minutes += bonus_mins

                    chain["total_minutes"] += bonus_mins
                    chain["calc100"] += bonus_mins
                    chain["payment"] += bonus_pay
                    # עדכון פירוט המקטעים - שומרים על השעות המקוריות, רק מוסיפים הערה על הבונוס
                    if chain.get("segments"):
                        old_seg = chain["segments"][0]
                        start_time = old_seg[0]
                        end_time = old_seg[1]
                        chain["segments"] = [(start_time, end_time, f"100% (כולל בונוס {bonus_mins} דק')")]
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
            "total_minutes_no_standby": sum(w[1]-w[0] for w in work_segments) + bonus_mins,
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

    # ימי עבודה בפועל
    monthly_totals["actual_work_days"] = len(work_days_set)

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

    # תשלום סופי כולל
    monthly_totals["total_payment"] = (
        monthly_totals["payment"] +
        monthly_totals["standby_payment"] +
        monthly_totals["travel"] +
        monthly_totals["extras"]
    )

    # שמירת שכר אפקטיבי
    monthly_totals["effective_hourly_rate"] = minimum_wage

    return monthly_totals
