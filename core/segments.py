"""
Shift segment processing for DiyurCalc application.
Contains functions for building and processing shift segments and daily maps.
"""
from __future__ import annotations

import logging
from datetime import timedelta, date
from typing import List, Dict, Any, Optional

from core.time_utils import (
    MINUTES_PER_HOUR, MINUTES_PER_DAY,
    WORK_DAY_START_MINUTES,
    span_minutes, to_local_date, minutes_to_time_str
)
from utils.utils import overlap_minutes

logger = logging.getLogger(__name__)

# =============================================================================
# Constants
# =============================================================================

# Break threshold (in minutes) - breaks longer than this split work chains
BREAK_THRESHOLD_MINUTES = 60

# Night shift constants
NIGHT_SHIFT_WORK_FIRST_MINUTES = 2 * MINUTES_PER_HOUR  # 120 = first 2 hours are work
NIGHT_SHIFT_STANDBY_END = 390  # 06:30 in minutes
NIGHT_SHIFT_MORNING_END = WORK_DAY_START_MINUTES  # 480 = 08:00
NOON_MINUTES = 12 * MINUTES_PER_HOUR  # 720 = 12:00

# Medical escort shift
MEDICAL_ESCORT_SHIFT_ID = 148


# =============================================================================
# Segment Helper Functions
# =============================================================================

def _create_segment_dict(
    start_minutes: int,
    end_minutes: int,
    wage_percent: int,
    segment_type: str
) -> Dict:
    """
    יצירת מילון סגמנט עם מחרוזות זמן מפורמטות.

    פרמטרים:
        start_minutes: זמן התחלה בדקות מחצות
        end_minutes: זמן סיום בדקות מחצות
        wage_percent: אחוז שכר (0 = חשב לפי רצף)
        segment_type: סוג הסגמנט (work, standby, vacation)

    מחזיר:
        מילון עם start_time, end_time, wage_percent, segment_type, id
    """
    return {
        "start_time": minutes_to_time_str(start_minutes),
        "end_time": minutes_to_time_str(end_minutes),
        "wage_percent": wage_percent,
        "segment_type": segment_type,
        "id": None
    }


def _build_night_shift_segments(entry_time: int, exit_time: int) -> List[Dict]:
    """
    בניית סגמנטים דינמיים למשמרת לילה לפי זמן הכניסה.

    חוקי משמרת לילה:
    - 2 שעות ראשונות: עבודה (אחוז לפי רצף)
    - עד 06:30: כוננות (24%)
    - 06:30-08:00: עבודה (אחוז לפי רצף)

    פרמטרים:
        entry_time: זמן כניסה בדקות מחצות
        exit_time: זמן יציאה בדקות מחצות (יכול לעבור 1440 אם למחרת)

    מחזיר:
        רשימת מילוני סגמנטים
    """
    dynamic_segments = []

    # סגמנט 1: 2 שעות ראשונות - עבודה
    work1_start = entry_time
    work1_end = min(entry_time + NIGHT_SHIFT_WORK_FIRST_MINUTES, exit_time)
    if work1_end > work1_start:
        dynamic_segments.append(_create_segment_dict(
            work1_start, work1_end, 0, "work"  # 0 = חשב לפי רצף
        ))

    # סגמנט 2: כוננות מסוף 2 שעות עבודה עד 06:30
    standby_start = work1_end
    # 06:30 - אם הכניסה אחרי 12:00, 06:30 הוא למחרת
    standby_end_time = NIGHT_SHIFT_STANDBY_END if entry_time < NOON_MINUTES else NIGHT_SHIFT_STANDBY_END + MINUTES_PER_DAY
    standby_end = min(standby_end_time, exit_time)
    if standby_end > standby_start:
        dynamic_segments.append(_create_segment_dict(
            standby_start, standby_end, 24, "standby"  # 24% = כוננות
        ))

    # סגמנט 3: עבודה 06:30-08:00
    morning_start = standby_end_time
    morning_end_time = NIGHT_SHIFT_MORNING_END if entry_time < NOON_MINUTES else NIGHT_SHIFT_MORNING_END + MINUTES_PER_DAY
    morning_end = min(morning_end_time, exit_time)
    if morning_end > morning_start and morning_start < exit_time:
        dynamic_segments.append(_create_segment_dict(
            morning_start, morning_end, 0, "work"  # 0 = חשב לפי רצף
        ))

    return dynamic_segments


# =============================================================================
# Shift Type Processing Functions
# =============================================================================

def _process_tagbur_shift(
    daily_map: Dict[str, Dict],
    r: Dict,
    seg_list: List[Dict],
    r_date: date,
    is_vacation_report: bool
) -> None:
    """
    עיבוד משמרת תגבור - מוסיף סגמנטים ישירות עם אחוזים קבועים.

    משמרות תגבור משתמשות בסגמנטים מוגדרים מראש ללא חישוב חפיפה עם שעות הדיווח.

    פרמטרים:
        daily_map: מפת הימים לעדכון
        r: מילון הדיווח
        seg_list: רשימת מילוני סגמנטים למשמרת
        r_date: תאריך הדיווח
        is_vacation_report: האם זה דיווח חופשה/מחלה
    """
    display_date = r_date
    day_key = display_date.strftime("%d/%m/%Y")
    entry = daily_map.setdefault(day_key, {"segments": [], "date": display_date, "escort_bonus_minutes": 0})
    entry["is_tagbur"] = True
    if "tagbur_wages" not in entry or not entry["tagbur_wages"]:
        entry["tagbur_wages"] = {"calc100": 0, "calc125": 0, "calc150": 0, "calc175": 0, "calc200": 0}

    if "tagbur_segments_detail" not in entry:
        entry["tagbur_segments_detail"] = []

    for seg in seg_list:
        seg_start, seg_end = span_minutes(seg["start_time"], seg["end_time"])
        duration = seg_end - seg_start

        effective_seg_type = seg["segment_type"]
        if is_vacation_report:
            effective_seg_type = "vacation"

        segment_id = seg.get("id")
        apartment_type_id = r.get("apartment_type_id")
        is_married = r.get("is_married")
        wage_percent = seg.get("wage_percent", 100)

        # שמירת פרטי הסגמנט לחישוב שבת/חול
        entry["tagbur_segments_detail"].append({
            "start": seg_start,
            "end": seg_end,
            "wage_percent": wage_percent,
            "date": display_date
        })

        # חישוב לפי אחוז קבוע
        if wage_percent == 100:
            entry["tagbur_wages"]["calc100"] += duration
        elif wage_percent == 125:
            entry["tagbur_wages"]["calc125"] += duration
        elif wage_percent == 150:
            entry["tagbur_wages"]["calc150"] += duration
        elif wage_percent == 175:
            entry["tagbur_wages"]["calc175"] += duration
        elif wage_percent == 200:
            entry["tagbur_wages"]["calc200"] += duration
        else:
            entry["tagbur_wages"]["calc100"] += duration

        # שמירה באותו מבנה כמו סגמנטים רגילים: (start, end, type, shift_id, seg_id, apt_type, married)
        entry["segments"].append((
            seg_start, seg_end, effective_seg_type,
            r["shift_type_id"], segment_id, apartment_type_id, is_married
        ))


def _process_fixed_vacation_shift(
    daily_map: Dict[str, Dict],
    r: Dict,
    seg_list: List[Dict],
    r_date: date
) -> None:
    """
    עיבוד משמרת חופשה/מחלה קבועה - מסמן סגמנטים כסוג חופשה.

    משמרות חופשה משתמשות בסגמנטים מוגדרים מראש אבל מסומנות כחופשה.

    פרמטרים:
        daily_map: מפת הימים לעדכון
        r: מילון הדיווח
        seg_list: רשימת מילוני סגמנטים למשמרת
        r_date: תאריך הדיווח
    """
    display_date = r_date
    day_key = display_date.strftime("%d/%m/%Y")
    entry = daily_map.setdefault(day_key, {"segments": [], "date": display_date, "escort_bonus_minutes": 0})

    for seg in seg_list:
        seg_start, seg_end = span_minutes(seg["start_time"], seg["end_time"])

        segment_id = seg.get("id")
        apartment_type_id = r.get("apartment_type_id")
        is_married = r.get("is_married")

        # סימון כחופשה - יטופל בנפרד ב-_process_daily_map
        entry["segments"].append((
            seg_start, seg_end, "vacation",
            r["shift_type_id"], segment_id, apartment_type_id, is_married
        ))


# =============================================================================
# Daily Map Building
# =============================================================================

def _build_daily_map(
    reports: List[Any],
    segments_by_shift: Dict[int, List[Any]],
    year: int,
    month: int
) -> Dict[str, Dict]:
    """
    בניית מפת ימים מדיווחים.
    מחלצת את הלוגיקה המשותפת של בניית daily_map משתי הפונקציות.
    """
    daily_map = {}

    for r in reports:
        if not r["start_time"] or not r["end_time"] or not r["shift_type_id"]:
            continue

        r_start, r_end = span_minutes(r["start_time"], r["end_time"])
        r_date = to_local_date(r["date"])

        # משמרת לווי רפואי - לפחות שעה עבודה
        is_medical_escort = (r["shift_type_id"] == MEDICAL_ESCORT_SHIFT_ID)
        escort_bonus_minutes = 0
        if is_medical_escort:
            duration = r_end - r_start
            if duration < MINUTES_PER_HOUR:
                escort_bonus_minutes = MINUTES_PER_HOUR - duration

        # פיצול משמרות חוצות חצות
        parts = []
        if r_end <= MINUTES_PER_DAY:
            parts.append((r_date, r_start, r_end, escort_bonus_minutes))
        else:
            # בפיצול חצות, הבונוס בדרך כלל שייך ליום ההתחלה, אבל נצמיד אותו לחלק הראשון
            parts.append((r_date, r_start, MINUTES_PER_DAY, escort_bonus_minutes))
            parts.append((r_date + timedelta(days=1), 0, r_end - MINUTES_PER_DAY, 0))

        seg_list = segments_by_shift.get(r["shift_type_id"], [])
        if not seg_list:
            # משמרת ללא סגמנטים מוגדרים - wage_percent=0 מסמן "חשב לפי רצף"
            seg_list = [{"start_time": r["start_time"], "end_time": r["end_time"],
                        "wage_percent": 0, "segment_type": "work", "id": None}]

        work_type = r.get("work_type")
        shift_name = r.get("shift_name") or ""
        is_vacation_report = (work_type == "sick_vacation" or
                             "חופשה" in shift_name or
                             "מחלה" in shift_name)

        # משמרות תגבור - משתמשים בסגמנטים המוגדרים ישירות (לא לפי שעות דיווח)
        # הערה: חופשה/מחלה מטופלות בנפרד - לא כתגבור
        is_tagbur_shift = "תגבור" in shift_name

        # משמרות חופשה/מחלה - סגמנטים קבועים אבל נספרות כחופשה
        is_fixed_vacation_shift = is_vacation_report and not is_tagbur_shift

        # משמרת לילה - סגמנטים דינמיים לפי זמן הכניסה בפועל
        is_night_shift = (shift_name == "משמרת לילה")
        if is_night_shift:
            entry_time = r_start
            exit_time = r_end if r_end > entry_time else r_end + MINUTES_PER_DAY
            seg_list = _build_night_shift_segments(entry_time, exit_time)

        # אם זו משמרת תגבור - מוסיפים את הסגמנטים ישירות בלי לחשב חפיפה עם שעות הדיווח
        if is_tagbur_shift and seg_list:
            _process_tagbur_shift(daily_map, r, seg_list, r_date, is_vacation_report)
            continue

        # משמרת חופשה/מחלה קבועה - מוסיפים את הסגמנטים ישירות כחופשה
        if is_fixed_vacation_shift and seg_list:
            _process_fixed_vacation_shift(daily_map, r, seg_list, r_date)
            continue

        for p_date, p_start, p_end, p_escort_bonus in parts:
            # פיצול מקטעים שחוצים את גבול 08:00
            CUTOFF = WORK_DAY_START_MINUTES  # 480
            sub_parts = []
            if p_start < CUTOFF < p_end:
                sub_parts.append((p_start, CUTOFF))
                sub_parts.append((CUTOFF, p_end))
            else:
                sub_parts.append((p_start, p_end))

            for s_start, s_end in sub_parts:
                # שיוך ליום עבודה ונרמול זמנים
                # דיווח ששעת הסיום שלו לפני 08:00 שייך ליום העבודה הקודם
                # אבל רק אם זה המשך של משמרת (לא דיווח עצמאי שמתחיל בחצות)
                # דיווח עצמאי = הדיווח המקורי התחיל בחצות (00:00) ביום הנוכחי
                is_standalone_midnight_shift = (s_start < CUTOFF and p_date == r_date and r_start < CUTOFF)
                if s_end <= CUTOFF and not is_standalone_midnight_shift:
                    # שייך ליום העבודה הקודם (המשך משמרת)
                    display_date = p_date - timedelta(days=1)
                    norm_start = s_start + MINUTES_PER_DAY
                    norm_end = s_end + MINUTES_PER_DAY
                else:
                    # שייך ליום העבודה הנוכחי
                    display_date = p_date
                    norm_start = s_start
                    norm_end = s_end

                if display_date.year != year or display_date.month != month:
                    continue

                day_key = display_date.strftime("%d/%m/%Y")
                if day_key not in daily_map:
                    daily_map[day_key] = {
                        "segments": [],
                        "date": display_date,
                        "escort_bonus_minutes": 0
                    }
                entry = daily_map[day_key]

                # Ensure escort_bonus_minutes exists (in case entry was created by tagbur/vacation processing)
                if "escort_bonus_minutes" not in entry:
                    entry["escort_bonus_minutes"] = 0

                # Add bonus only once per part
                if s_start == p_start:  # Only add to the first sub-part to avoid double counting
                    entry["escort_bonus_minutes"] += p_escort_bonus

                is_second_day = (p_date > r_date)

                # Sort segments chronologically by start time before normalizing
                seg_list_sorted = sorted(seg_list, key=lambda s: span_minutes(s["start_time"], s["end_time"])[0])

                # Rotate the list so that the segment corresponding to the report start time comes first
                # This ensures that normalization flows correctly (e.g. 06:30-08:00 is end of shift, not start)
                rotate_idx = 0
                rep_start_min = r_start % MINUTES_PER_DAY

                # Find the segment that starts closest to (and before/at) the report start time
                best_start_diff = -1
                for i, seg in enumerate(seg_list_sorted):
                    seg_start_min, _ = span_minutes(seg["start_time"], seg["end_time"])
                    if seg_start_min <= rep_start_min:
                        if seg_start_min > best_start_diff:
                            best_start_diff = seg_start_min
                            rotate_idx = i

                # If no segment starts before report (e.g. report 05:00, first seg 06:00),
                # then it belongs to the LAST segment (from yesterday)
                if best_start_diff == -1 and seg_list_sorted:
                    rotate_idx = len(seg_list_sorted) - 1

                seg_list_ordered = seg_list_sorted[rotate_idx:] + seg_list_sorted[:rotate_idx]

                last_s_end_norm = -1
                minutes_covered = 0
                covered_intervals = []

                for seg in seg_list_ordered:
                    # שימוש במשתנים ייחודיים למניעת דריסת משתני הלופ החיצוני
                    orig_s_start, orig_s_end = span_minutes(seg["start_time"], seg["end_time"])

                    while orig_s_start < last_s_end_norm:
                        orig_s_start += MINUTES_PER_DAY
                        orig_s_end += MINUTES_PER_DAY
                    last_s_end_norm = orig_s_end

                    if is_second_day:
                        current_seg_start = orig_s_start - MINUTES_PER_DAY
                        current_seg_end = orig_s_end - MINUTES_PER_DAY
                    else:
                        current_seg_start = orig_s_start
                        current_seg_end = orig_s_end

                    overlap_val = overlap_minutes(s_start, s_end, current_seg_start, current_seg_end)
                    if overlap_val <= 0:
                        continue

                    minutes_covered += overlap_val

                    # שמירת אינטרוול מכוסה לחישוב "חורים" בהמשך
                    inter_start = max(s_start, current_seg_start)
                    inter_end = min(s_end, current_seg_end)
                    if inter_start < inter_end:
                        covered_intervals.append((inter_start, inter_end))

                    # נרמול גבולות המקטע לפי workday
                    eff_start_in_part = max(current_seg_start, s_start)
                    eff_end_in_part = min(current_seg_end, s_end)

                    if s_end <= CUTOFF:
                        eff_start = eff_start_in_part + MINUTES_PER_DAY
                        eff_end = eff_end_in_part + MINUTES_PER_DAY
                    else:
                        eff_start = eff_start_in_part
                        eff_end = eff_end_in_part

                    eff_type = seg["segment_type"]
                    # אם זה דיווח חופשה/מחלה - סמן כחופשה
                    if is_vacation_report:
                        eff_type = "vacation"

                    segment_id = seg.get("id")
                    apartment_type_id = r.get("apartment_type_id")
                    is_married = r.get("is_married")

                    entry["segments"].append((
                        eff_start, eff_end, eff_type,
                        r["shift_type_id"], segment_id, apartment_type_id, is_married
                    ))

                # טיפול בשעות עבודה שלא מכוסות ע"י סגמנטים מוגדרים
                total_part_minutes = s_end - s_start
                remaining = total_part_minutes - minutes_covered

                if remaining > 0:
                    # מיון ומיזוג אינטרוולים חופפים
                    covered_intervals.sort()
                    merged_covered = []
                    for interval in covered_intervals:
                        if merged_covered and interval[0] <= merged_covered[-1][1]:
                            merged_covered[-1] = (merged_covered[-1][0], max(merged_covered[-1][1], interval[1]))
                        else:
                            merged_covered.append(interval)

                    # מציאת ה"חורים" - זמנים לא מכוסים
                    uncovered_intervals = []
                    current_pos = s_start
                    for cov_start, cov_end in merged_covered:
                        if current_pos < cov_start:
                            uncovered_intervals.append((current_pos, cov_start))
                        current_pos = max(current_pos, cov_end)
                    if current_pos < s_end:
                        uncovered_intervals.append((current_pos, s_end))

                    # יצירת סגמנטי עבודה לכל זמן לא מכוסה
                    segment_id = None
                    apartment_type_id = r.get("apartment_type_id")
                    is_married = r.get("is_married")

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
                            eff_uncov_start, eff_uncov_end, "work",
                            r["shift_type_id"], segment_id, apartment_type_id, is_married
                        ))

    return daily_map
