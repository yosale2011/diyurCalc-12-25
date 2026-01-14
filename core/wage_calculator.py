"""
Wage calculation engine for DiyurCalc application.
Contains functions for calculating wages, processing daily maps, and generating totals.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import List, Tuple, Dict, Any, Optional, Callable

from core.time_utils import (
    MINUTES_PER_HOUR, MINUTES_PER_DAY,
    REGULAR_HOURS_LIMIT, OVERTIME_125_LIMIT,
    FRIDAY, SATURDAY,
    _get_shabbat_boundaries
)
from core.segments import BREAK_THRESHOLD_MINUTES

logger = logging.getLogger(__name__)

# =============================================================================
# Constants
# =============================================================================

# Standby cancellation threshold
# If work overlaps with standby by more than this percentage, standby is cancelled
STANDBY_CANCEL_OVERLAP_THRESHOLD = 0.70  # 70%

# Default standby rate
DEFAULT_STANDBY_RATE = 70.0


# =============================================================================
# Wage Rate Calculation
# =============================================================================

def calculate_wage_rate(
    minutes_in_chain: int,
    is_shabbat: bool
) -> str:
    """
    Determine the wage rate label based on hours worked in chain and Shabbat status.

    Args:
        minutes_in_chain: Total minutes worked so far in the current chain
        is_shabbat: Whether this minute falls within Shabbat hours

    Returns:
        Rate label: "100%", "125%", "150%", "175%", or "200%"
    """
    if minutes_in_chain <= REGULAR_HOURS_LIMIT:
        return "150%" if is_shabbat else "100%"
    elif minutes_in_chain <= OVERTIME_125_LIMIT:
        return "175%" if is_shabbat else "125%"
    else:
        return "200%" if is_shabbat else "150%"


# =============================================================================
# Chain Wage Calculation
# =============================================================================

def _calculate_chain_wages(
    chain_segments: List[Tuple[int, int, int]],
    day_date: date,
    shabbat_cache: Dict[str, Dict[str, str]],
    minutes_offset: int = 0
) -> Dict[str, Any]:
    """
    חישוב שכר לרצף עבודה (chain) בשיטת בלוקים.

    במקום לעבור דקה-דקה, מחשב בלוקים לפי גבולות:
    - 480 דקות (מעבר 100% -> 125%)
    - 600 דקות (מעבר 125% -> 150%)
    - גבולות שבת (כניסה/יציאה)

    Args:
        chain_segments: List of (start_min, end_min, shift_id) tuples
        day_date: The date for Shabbat calculation
        shabbat_cache: Cache of Shabbat times
        minutes_offset: Minutes already worked in this chain (from previous day's carryover)

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
    # Thresholds: 0-480 = tier1, 480-600 = tier2, 600+ = tier3
    # Start from offset if this chain continues from previous day
    minutes_processed = minutes_offset

    for seg_start, seg_end in flat_segments:
        seg_duration = seg_end - seg_start
        seg_offset = 0

        while seg_offset < seg_duration:
            current_abs_minute = seg_start + seg_offset
            current_chain_minute = minutes_processed + 1  # 1-based for wage calculation

            # Determine which overtime tier we're in
            if current_chain_minute <= REGULAR_HOURS_LIMIT:
                tier_end = REGULAR_HOURS_LIMIT
                base_rate = "100%"
                shabbat_rate = "150%"
            elif current_chain_minute <= OVERTIME_125_LIMIT:
                tier_end = OVERTIME_125_LIMIT
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
                    if block_abs_end >= MINUTES_PER_DAY:
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
# Daily Map Processing
# =============================================================================

def _process_daily_map(
    daily_map: Dict[str, Dict],
    shabbat_cache: Dict[str, Dict[str, str]],
    get_standby_rate_fn: Callable[[int, Optional[int], bool], float],
    year: int,
    month: int
) -> Tuple[Dict[str, int], set, set]:
    """
    עיבוד מפת ימים וחישוב סיכומים.

    Args:
        daily_map: מפת הימים שנבנתה ע"י _build_daily_map
        shabbat_cache: זמני שבת
        get_standby_rate_fn: פונקציה לקבלת תעריף כוננות (מאפשרת DB או cache)
        year, month: שנה וחודש לסינון

    Returns:
        (day_totals, work_days_set, vacation_days_set)
    """
    from utils.utils import calculate_accruals
    WORK_DAY_CUTOFF = 480  # 08:00

    totals = {
        "calc100": 0, "calc125": 0, "calc150": 0, "calc175": 0, "calc200": 0,
        "calc150_shabbat": 0, "calc150_overtime": 0,
        "calc150_shabbat_100": 0, "calc150_shabbat_50": 0,
        "total_hours": 0, "standby_payment": 0, "vacation_minutes": 0
    }
    work_days_set = set()
    vacation_days_set = set()


    # Track carryover minutes from previous day's chain ending at 08:00
    prev_day_carryover_minutes = 0
    prev_day_ended_at_midnight = False
    prev_day_date = None  # לעקוב אחרי התאריך הקודם

    for day_key, entry in sorted(daily_map.items()):
        day_date = entry["date"]

        if entry.get("escort_bonus_minutes", 0) > 0:
            totals["calc100"] += entry["escort_bonus_minutes"]

        # בדיקה אם הימים רציפים - אם לא, לאפס carryover
        if prev_day_date is not None:
            days_diff = (day_date - prev_day_date).days
            if days_diff != 1:
                # הימים לא רציפים - אין carryover
                prev_day_carryover_minutes = 0
                prev_day_ended_at_midnight = False

        # משמרת תגבור - משתמשים באחוזים הקבועים מהסגמנטים, לא מחשבים רצף
        if entry.get("is_tagbur") and entry.get("tagbur_wages"):
            tagbur = entry["tagbur_wages"]
            totals["calc100"] += tagbur.get("calc100", 0)
            totals["calc125"] += tagbur.get("calc125", 0)
            totals["calc175"] += tagbur.get("calc175", 0)
            totals["calc200"] += tagbur.get("calc200", 0)

            # חישוב calc150 עם הפרדה בין שבת לחול
            tagbur_calc150 = tagbur.get("calc150", 0)
            if tagbur_calc150 > 0:
                # בדיקה אם יש פרטי סגמנטים לחישוב שבת/חול
                tagbur_segments_detail = entry.get("tagbur_segments_detail", [])
                if tagbur_segments_detail:
                    # חישוב לפי סגמנטים - בדיקה אם כל סגמנט הוא שבת או חול
                    calc150_shabbat_minutes = 0
                    calc150_overtime_minutes = 0

                    weekday = day_date.weekday()
                    is_fri_or_sat = weekday in (FRIDAY, SATURDAY)

                    # קבלת גבולות שבת אם רלוונטי
                    shabbat_enter, shabbat_exit = (-1, -1)
                    if is_fri_or_sat:
                        shabbat_enter, shabbat_exit = _get_shabbat_boundaries(day_date, shabbat_cache)

                    for seg_detail in tagbur_segments_detail:
                        if seg_detail["wage_percent"] == 150:
                            seg_start = seg_detail["start"] % MINUTES_PER_DAY  # נרמול ל-0-1439
                            seg_end = seg_detail["end"] % MINUTES_PER_DAY
                            seg_date = seg_detail["date"]
                            seg_weekday = seg_date.weekday()
                            seg_duration = seg_end - seg_start

                            # בדיקה אם הסגמנט נופל בשבת
                            # חשוב: לבדוק את seg_weekday של הסגמנט עצמו, לא רק את day_date
                            is_seg_shabbat = False

                            # קבלת גבולות שבת לפי התאריך של הסגמנט
                            seg_shabbat_enter, seg_shabbat_exit = (-1, -1)
                            if seg_weekday in (FRIDAY, SATURDAY):
                                seg_shabbat_enter, seg_shabbat_exit = _get_shabbat_boundaries(seg_date, shabbat_cache)

                            if seg_weekday == FRIDAY and seg_shabbat_enter >= 0:
                                # יום שישי - בדיקה אם הסגמנט מתחיל אחרי כניסת שבת
                                if seg_start >= seg_shabbat_enter:
                                    # כל הסגמנט הוא שבת
                                    is_seg_shabbat = True
                                elif seg_end > seg_shabbat_enter:
                                    # הסגמנט חוצה את כניסת שבת - נחלק אותו
                                    shabbat_part = seg_end - seg_shabbat_enter
                                    weekday_part = seg_shabbat_enter - seg_start
                                    calc150_shabbat_minutes += shabbat_part
                                    calc150_overtime_minutes += weekday_part
                                    continue  # כבר עדכנו, עוברים לסגמנט הבא
                            elif seg_weekday == SATURDAY and seg_shabbat_exit >= 0:
                                # יום שבת - בדיקה אם הסגמנט מסתיים לפני יציאת שבת
                                # shabbat_exit הוא יחסית לחצות יום שישי, אז בשבת זה shabbat_exit - 1440
                                shabbat_exit_saturday = seg_shabbat_exit - MINUTES_PER_DAY
                                if seg_end <= shabbat_exit_saturday:
                                    # כל הסגמנט הוא שבת
                                    is_seg_shabbat = True
                                elif seg_start < shabbat_exit_saturday:
                                    # הסגמנט חוצה את יציאת שבת - נחלק אותו
                                    shabbat_part = shabbat_exit_saturday - seg_start
                                    weekday_part = seg_end - shabbat_exit_saturday
                                    calc150_shabbat_minutes += shabbat_part
                                    calc150_overtime_minutes += weekday_part
                                    continue  # כבר עדכנו, עוברים לסגמנט הבא
                            elif seg_weekday == SATURDAY:
                                # יום שבת ללא גבולות שבת - כל הסגמנט הוא שבת
                                is_seg_shabbat = True

                            # אם לא חילקנו את הסגמנט, נבדוק אם הוא שבת או חול
                            if is_seg_shabbat:
                                calc150_shabbat_minutes += seg_duration
                            else:
                                calc150_overtime_minutes += seg_duration

                    # אם יש חלוקה, עדכן את הסכומים
                    if calc150_shabbat_minutes > 0 or calc150_overtime_minutes > 0:
                        totals["calc150"] += tagbur_calc150
                        totals["calc150_shabbat"] += calc150_shabbat_minutes
                        totals["calc150_overtime"] += calc150_overtime_minutes
                        # עדכון גם של השעות המפוצלות לפנסיה (100% + 50%)
                        totals["calc150_shabbat_100"] += calc150_shabbat_minutes
                        totals["calc150_shabbat_50"] += calc150_shabbat_minutes
                    else:
                        # אם לא הצלחנו לחלק, נחשוב לפי יום השבוע
                        if weekday == SATURDAY:
                            totals["calc150"] += tagbur_calc150
                            totals["calc150_shabbat"] += tagbur_calc150
                            # עדכון גם של השעות המפוצלות לפנסיה (100% + 50%)
                            totals["calc150_shabbat_100"] += tagbur_calc150
                            totals["calc150_shabbat_50"] += tagbur_calc150
                        elif weekday == FRIDAY:
                            # יום שישי - נבדוק אם יש חלק בשבת
                            # נניח שכל התגבור הוא חול (כי בדרך כלל תגבור ביום שישי הוא לפני שבת)
                            totals["calc150"] += tagbur_calc150
                            totals["calc150_overtime"] += tagbur_calc150
                        else:
                            # חול
                            totals["calc150"] += tagbur_calc150
                            totals["calc150_overtime"] += tagbur_calc150
                else:
                    # אין calc150 בתגבור
                    totals["calc150"] += tagbur_calc150

            total_day_minutes = sum(tagbur.values())
            totals["total_hours"] += total_day_minutes

            if total_day_minutes > 0:
                work_days_set.add(day_date)

            # אפס carryover כי משמרת תגבור היא עצמאית
            prev_day_carryover_minutes = 0
            prev_day_ended_at_midnight = False
            prev_day_date = day_date
            continue

        # הפרדת מקטעים לסוגים
        work_segments = []
        standby_segments = []
        vacation_segments = []

        for seg in entry["segments"]:
            s_start, s_end, s_type, shift_id, seg_id, apt_type, is_married = seg
            if s_type == "standby":
                standby_segments.append((s_start, s_end, seg_id, apt_type, is_married))
            elif s_type == "vacation":
                vacation_segments.append((s_start, s_end))
            else:
                work_segments.append((s_start, s_end, shift_id))

        work_segments.sort(key=lambda x: x[0])
        standby_segments.sort(key=lambda x: x[0])

        # הסרת כפילויות
        seen = set()
        deduped = []
        for ws in work_segments:
            key = (ws[0], ws[1])
            if key not in seen:
                deduped.append(ws)
                seen.add(key)
        work_segments = deduped

        # הסרת כפילויות מקטעי כוננות
        seen_standby = set()
        deduped_standby = []
        for sb in standby_segments:
            key = (sb[0], sb[1], sb[2])  # start_time, end_time, segment_id
            if key not in seen_standby:
                deduped_standby.append(sb)
                seen_standby.add(key)
        standby_segments = deduped_standby

        # איחוד מקטעי כוננות רציפים לפני בדיקת ביטול
        # כדי להבטיח שבודקים את כל תקופת הכוננות המלאה, לא כל חלק בנפרד
        standby_segments.sort(key=lambda x: x[0])
        merged_standbys = []
        for sb in standby_segments:
            sb_start, sb_end, sb_seg_id, sb_apt, sb_married = sb
            if merged_standbys and sb_start <= merged_standbys[-1][1]:  # חופפים או רציפים
                # הרחבת הכוננות הקודמת
                prev = merged_standbys[-1]
                merged_standbys[-1] = (prev[0], max(prev[1], sb_end), prev[2], prev[3], prev[4])
            else:
                merged_standbys.append(sb)

        # ביטול כוננות אם יש חפיפה מעל 70% - הוחלף בלוגיקת קיזוז (Trim)
        # במקום לבטל את הכוננות כליל, אנו מקזזים את זמני העבודה מזמן הכוננות
        final_standby_segments = []
        for sb in merged_standbys:
            sb_start, sb_end, sb_seg_id, sb_apt, sb_married = sb

            # Start with the full standby segment
            remaining_parts = [(sb_start, sb_end)]

            # Subtract each work segment
            for w_start, w_end, _ in work_segments:
                new_parts = []
                for r_start, r_end in remaining_parts:
                    # Calculate intersection
                    inter_start = max(r_start, w_start)
                    inter_end = min(r_end, w_end)

                    if inter_start < inter_end:
                        # There is overlap, subtract it
                        # Part before overlap
                        if r_start < inter_start:
                            new_parts.append((r_start, inter_start))
                        # Part after overlap
                        if inter_end < r_end:
                            new_parts.append((inter_end, r_end))
                    else:
                        # No overlap, keep original
                        new_parts.append((r_start, r_end))
                remaining_parts = new_parts

            # Add resulting parts to final list
            for r_start, r_end in remaining_parts:
                if r_end > r_start:
                    final_standby_segments.append((r_start, r_end, sb_seg_id, sb_apt, sb_married))

        standby_segments = final_standby_segments
        standby_segments.sort(key=lambda x: x[0])

        # איחוד אירועים
        all_events = []
        for s, e, sid in work_segments:
            all_events.append({"start": s, "end": e, "type": "work", "shift_id": sid})
        for s, e, seg_id, apt_type, is_married_val in standby_segments:
            all_events.append({"start": s, "end": e, "type": "standby", "segment_id": seg_id,
                              "apartment_type_id": apt_type, "is_married": is_married_val})
        for s, e in vacation_segments:
            all_events.append({"start": s, "end": e, "type": "vacation"})

        all_events.sort(key=lambda x: x["start"])

        # Build a set of work segment boundaries for quick lookup
        # This helps determine if standby truly breaks the chain or if work continues through it
        work_starts = {ws[0] for ws in work_segments}  # All work start times
        work_ends = {ws[1] for ws in work_segments}    # All work end times

        # Determine if we should use carryover from previous day
        # Carryover applies if first work event starts at 08:00 (480 minutes)
        first_work_start = None
        for evt in all_events:
            if evt["type"] == "work":
                first_work_start = evt["start"]
                break

        use_carryover = (first_work_start == WORK_DAY_CUTOFF or prev_day_ended_at_midnight) and prev_day_carryover_minutes > 0
        current_offset = prev_day_carryover_minutes if use_carryover else 0

        # משתני רצף
        current_chain_segments = []
        last_end = None
        last_etype = None
        day_standby_payment = 0
        day_vacation_minutes = 0
        day_wages = {
            "calc100": 0, "calc125": 0, "calc150": 0, "calc175": 0, "calc200": 0,
            "calc150_shabbat": 0, "calc150_overtime": 0,
            "calc150_shabbat_100": 0, "calc150_shabbat_50": 0
        }

        # Track chain info for carryover
        first_chain_of_day = True
        last_chain_total = 0
        last_chain_ended_at_0800 = False

        # Track paid standby segments to avoid double payment on split segments
        paid_standby_ids = set()

        def close_chain(minutes_offset=0):
            nonlocal current_chain_segments, day_wages, last_chain_total, last_chain_ended_at_0800, prev_day_ended_at_midnight
            if not current_chain_segments:
                return

            chain_wages = _calculate_chain_wages(current_chain_segments, day_date, shabbat_cache, minutes_offset)
            for key in day_wages:
                day_wages[key] += chain_wages[key]

            # Calculate chain duration for potential carryover
            chain_duration = sum(e - s for s, e, _ in current_chain_segments)
            last_chain_total = minutes_offset + chain_duration

            # Check if chain ends at 08:00 boundary (1920 = 08:00 + 1440)
            last_chain_ended_at_0800 = (current_chain_segments[-1][1] == 1920) if current_chain_segments else False

            # Check if chain ends at midnight (1440)
            last_segment_end = current_chain_segments[-1][1] if current_chain_segments else 0
            prev_day_ended_at_midnight = (last_segment_end == 1440)

            current_chain_segments = []

        for event in all_events:
            seg_start = event["start"]
            seg_end = event["end"]
            seg_type = event["type"]

            is_special = seg_type in ("standby", "vacation")

            # בדיקת שבירת רצף
            should_break = False
            if current_chain_segments:
                if is_special:
                    should_break = True
                elif last_end is not None:
                    # Calculate gap considering normalized times
                    # Normalized times (after midnight, before 08:00) have 1440 added
                    # So they are >= 1440, not < 480
                    gap = seg_start - last_end
                    if gap > BREAK_THRESHOLD_MINUTES:
                        should_break = True

            if should_break:
                chain_offset = current_offset if first_chain_of_day else 0
                close_chain(chain_offset)
                first_chain_of_day = False

            if is_special:
                if seg_type == "standby":
                    # בדיקה האם זו המשכיות של כוננות קודמת
                    is_continuation = (last_etype == "standby" and last_end == seg_start)

                    # בדיקה אם כבר שילמנו על כוננות ביום הזה
                    # כוננות משולמת פעם אחת ליום לכל סוג דירה, לא משנה מאיזו משמרת
                    seg_id = event.get("segment_id")
                    apt_type = event.get("apartment_type_id")

                    # מפתח ייחודי לפי סוג דירה בלבד - כוננות אחת ליום לכל סוג דירה
                    standby_key = ("apt", apt_type)

                    already_paid = standby_key in paid_standby_ids

                    if not is_continuation and not already_paid:
                        is_married_val = event.get("is_married")
                        is_married_bool = bool(is_married_val) if is_married_val is not None else False
                        rate = get_standby_rate_fn(seg_id or 0, apt_type, is_married_bool)
                        day_standby_payment += rate

                        paid_standby_ids.add(standby_key)
                elif seg_type == "vacation":
                    day_vacation_minutes += (seg_end - seg_start)

                last_end = seg_end
                last_etype = seg_type
            else:
                shift_id = event.get("shift_id", 0)
                current_chain_segments.append((seg_start, seg_end, shift_id))
                last_end = seg_end
                last_etype = seg_type

        # Close last chain with proper offset
        chain_offset = current_offset if first_chain_of_day else 0
        close_chain(chain_offset)

        # Update carryover for next day
        if last_chain_ended_at_0800:
            prev_day_carryover_minutes = last_chain_total
        else:
            prev_day_carryover_minutes = 0

        # עדכון סיכומים
        for key in day_wages:
            totals[key] += day_wages[key]
        totals["total_hours"] += sum(day_wages[k] for k in ["calc100", "calc125", "calc150", "calc175", "calc200"])
        totals["standby_payment"] += day_standby_payment
        totals["vacation_minutes"] += day_vacation_minutes

        # ספירת ימי עבודה - בגרסה המנורמלת, המפתח הוא יום העבודה
        if work_segments:
            work_days_set.add(day_date)

        if vacation_segments:
            vacation_days_set.add(day_date)

        # עדכון התאריך הקודם לסיבוב הבא
        prev_day_date = day_date

    return totals, work_days_set, vacation_days_set
