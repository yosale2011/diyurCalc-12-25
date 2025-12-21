
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime, timedelta, date
from logic import (
    MINUTES_PER_HOUR, MINUTES_PER_DAY, BREAK_THRESHOLD_MINUTES,
    STANDBY_CANCEL_OVERLAP_THRESHOLD, LOCAL_TZ,
    span_minutes, to_local_date, is_shabbat_time, calculate_wage_rate,
    get_standby_rate, _calculate_chain_wages
)
from utils import overlap_minutes, minutes_to_hours_str, to_gematria, month_range_ts
from convertdate import hebrew
import logging

logger = logging.getLogger(__name__)

def get_effective_hourly_rate(report, minimum_wage: float) -> float:
    """
    Get the effective hourly rate for a shift.
    If the shift has a custom rate defined (is_minimum_wage=False and rate is set),
    use that rate. Otherwise, use the minimum wage.
    
    Args:
        report: The report dict containing shift_rate and shift_is_minimum_wage
        minimum_wage: The default minimum wage rate
        
    Returns:
        The effective hourly rate to use for payment calculation
    """
    shift_rate = report.get('shift_rate')
    is_minimum_wage = report.get('shift_is_minimum_wage', True)
    
    # If shift has a custom rate and is NOT using minimum wage
    if shift_rate and not is_minimum_wage:
        # shift_rate is stored in agorot (cents), convert to shekels
        return float(shift_rate) / 100
    
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
    for r in reports:
        shift_id = r.get("shift_type_id")
        if shift_id and shift_id not in shift_rates:
            shift_rates[shift_id] = get_effective_hourly_rate(r, minimum_wage)
        
    daily_map = {}
    
    for r in reports:
        if not r["start_time"] or not r["end_time"] or not r["shift_type_id"]:
            continue
        
        # Split shifts across midnight
        rep_start_orig, rep_end_orig = span_minutes(r["start_time"], r["end_time"])
        r_date = to_local_date(r["date"])
        
        parts = []
        if rep_end_orig <= MINUTES_PER_DAY:
            parts.append((r_date, rep_start_orig, rep_end_orig))
        else:
            parts.append((r_date, rep_start_orig, MINUTES_PER_DAY))
            next_day = r_date + timedelta(days=1)
            parts.append((next_day, 0, rep_end_orig - MINUTES_PER_DAY))
            
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
        is_fixed_segments_shift = "תגבור" in shift_name_str or is_vacation_report or is_sick_report

        # משמרת לילה - סגמנטים דינמיים לפי זמן הכניסה בפועל
        # החוק: 2 שעות ראשונות עבודה, עד 06:30 כוננות, 06:30-08:00 עבודה
        is_night_shift = (shift_name_str == "משמרת לילה")
        if is_night_shift:
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
            entry = daily_map.setdefault(day_key, {"buckets": {}, "shifts": set(), "segments": [], "is_fixed_segments": False})
            entry["is_fixed_segments"] = True  # סימון שזו משמרת קבועה
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
                apartment_type_id = r.get("apartment_type_id")
                is_married = r.get("is_married")
                apartment_name = r.get("apartment_name", "")

                entry["segments"].append((seg_start, seg_end, effective_seg_type, label, r["shift_type_id"], segment_id, apartment_type_id, is_married, apartment_name, r_date))

            continue  # דלג על העיבוד הרגיל עבור משמרת זו

        for p_date, p_start, p_end in parts:
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
                    continue

                day_key = display_date.strftime("%d/%m/%Y")
                entry = daily_map.setdefault(day_key, {"buckets": {}, "shifts": set(), "segments": [], "is_fixed_segments": False})
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
                    is_married = r.get("is_married")
                    apartment_name = r.get("apartment_name", "")

                    # Store actual_date (p_date) for correct Shabbat calculation even when displayed under different day
                    entry["segments"].append((eff_start, eff_end, effective_seg_type, label, r["shift_type_id"], segment_id, apartment_type_id, is_married, apartment_name, p_date))
                    
                # Uncovered minutes -> work
                # חישוב שעות עבודה שלא מכוסות ע"י סגמנטים מוגדרים
                total_part_minutes = s_end - s_start
                remaining = total_part_minutes - minutes_covered

                if remaining > 0:
                    # מיון ומיזוג אינטרוולים חופפים (covered_intervals נאסף בלולאה הראשית)
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
                            apartment_name, p_date
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
        
        # Sort and Dedup Segments
        # entry["segments"]: (start, end, type, label, shift_id, seg_id, apt_type, married, apt_name, actual_date)
        raw_segments = entry["segments"]

        work_segments = []
        standby_segments = []
        vacation_segments = []
        sick_segments = []

        for seg_entry in raw_segments:
            # Normalize length to 10 (now includes apartment_name and actual_date)
            if len(seg_entry) < 10:
                # Pad with None
                seg_entry = seg_entry + (None,) * (10 - len(seg_entry))

            s_start, s_end, s_type, label, sid, seg_id, apt_type, married, apt_name, actual_date = seg_entry

            if s_type == "standby":
                standby_segments.append((s_start, s_end, seg_id, apt_type, married, actual_date))
            elif s_type == "vacation":
                vacation_segments.append((s_start, s_end, actual_date))
            elif s_type == "sick":
                sick_segments.append((s_start, s_end, actual_date))
            else:
                work_segments.append((s_start, s_end, label, sid, apt_name, actual_date))
                
        work_segments.sort(key=lambda x: x[0])
        standby_segments.sort(key=lambda x: x[0])
        vacation_segments.sort(key=lambda x: x[0])
        sick_segments.sort(key=lambda x: x[0])
        
        # Dedup work
        deduped = []
        seen = set()
        for w in work_segments:
            k = (w[0], w[1])  # (start, end)
            if k not in seen:
                deduped.append(w)
                seen.add(k)
        work_segments = deduped  # Each is (start, end, label, sid, apt_name, actual_date)
        
        # Dedup standby
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
        standby_segments.sort(key=lambda x: x[0])
        merged_standbys = []
        for sb in standby_segments:
            sb_start, sb_end, seg_id, apt_type, married, actual_date = sb
            if merged_standbys and sb_start <= merged_standbys[-1][1]:  # Overlapping or adjacent
                # Extend the previous merged standby
                prev = merged_standbys[-1]
                merged_standbys[-1] = (prev[0], max(prev[1], sb_end), prev[2], prev[3], prev[4], prev[5])
            else:
                merged_standbys.append(sb)

        # Standby Trim Logic - subtract work time from standby instead of cancelling
        cancelled_standbys = []
        trimmed_standbys = []
        for sb in merged_standbys:
            sb_start, sb_end, seg_id, apt_type, married, actual_date = sb
            duration = sb_end - sb_start
            if duration <= 0: continue

            # Calculate total overlap with work
            total_overlap = 0
            for w in work_segments:
                total_overlap += overlap_minutes(sb_start, sb_end, w[0], w[1])

            ratio = total_overlap / duration if duration > 0 else 0

            if ratio >= STANDBY_CANCEL_OVERLAP_THRESHOLD:
                # Cancel standby completely if >70% overlap
                if sb_start % MINUTES_PER_DAY > 0:
                    cancelled_standbys.append({
                        "start": sb_start % MINUTES_PER_DAY,
                        "end": sb_end % MINUTES_PER_DAY,
                        "reason": f"חפיפה ({int(ratio*100)}%)"
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

                # Add trimmed parts
                for r_start, r_end in remaining_parts:
                    if r_end > r_start:
                        trimmed_standbys.append((r_start, r_end, seg_id, apt_type, married, actual_date))

        standby_segments = trimmed_standbys
        
        # Calculate Chains
        chains_detail = []

        # משמרת קבועה (ערב שבת/חג) - לא מחשבים רצף, משתמשים באחוזים הקבועים מהסגמנטים
        if is_fixed_segments:
            d_calc100 = 0; d_calc125 = 0; d_calc150 = 0; d_calc175 = 0; d_calc200 = 0
            d_payment = 0; d_standby_pay = 0
            chains = []
            cancelled_standbys = []

            for s, e, label, sid, apt_name, actual_date in work_segments:
                duration = e - s
                # Get effective hourly rate for this shift (uses custom rate if defined)
                effective_rate = shift_rates.get(sid, minimum_wage)
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
                    "segments": [(start_str, end_str, "חופשה")],
                    "break_reason": "",
                    "from_prev_day": False,
                    "effective_rate": minimum_wage,
                })

            # עיבוד סגמנטי מחלה
            for s, e, actual_date in sick_segments:
                duration = e - s
                pay = (duration / 60) * minimum_wage  # מחלה = 100% שכר מינימום
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
                    "segments": [(start_str, end_str, "מחלה")],
                    "break_reason": "",
                    "from_prev_day": False,
                    "effective_rate": minimum_wage,
                })

            total_minutes = sum(w[1]-w[0] for w in work_segments) + sum(v[1]-v[0] for v in vacation_segments) + sum(s[1]-s[0] for s in sick_segments)

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
        for s, e, l, sid, apt_name, actual_date in work_segments:
            all_events.append({"start": s, "end": e, "type": "work", "label": l, "shift_id": sid, "apartment_name": apt_name or "", "actual_date": actual_date or day_date})
        for s, e, seg_id, apt, married, actual_date in standby_segments:
            all_events.append({"start": s, "end": e, "type": "standby", "label": "כוננות", "seg_id": seg_id, "apt": apt, "married": married, "actual_date": actual_date or day_date})
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

        def calculate_chain_pay(segments, minutes_offset=0):
            # segments is list of (start, end, label, shift_id, apartment_name, actual_date)
            # Convert to format expected by _calculate_chain_wages: (start, end, shift_id)
            chain_segs = [(s, e, sid) for s, e, l, sid, apt, adate in segments]

            # Use display day_date for Shabbat calculation
            # The display date is the actual calendar date when work was performed
            # (e.g., Saturday 08/11 even if the report started on Friday 07/11)
            calc_date = day_date

            # Use optimized block calculation with carryover offset
            result = _calculate_chain_wages(chain_segs, calc_date, shabbat_cache, minutes_offset)

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
            chain_duration = sum(e - s for s, e, l, sid, apt, adate in segments)
            chain_total_minutes = minutes_offset + chain_duration

            # Get apartment names from segments - segments is (start, end, label, sid, apt_name, actual_date)
            chain_apartments = set()
            for s, e, l, sid, apt, adate in segments:
                if apt:
                    chain_apartments.add(apt)
            apt_name = ", ".join(sorted(chain_apartments)) if chain_apartments else ""

            # Create a separate chain row for each rate segment (like the old code)
            for i, (seg_start, seg_end, seg_label, is_shabbat) in enumerate(seg_detail):
                is_first = (i == 0)
                is_last = (i == len(seg_detail) - 1)

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
                    "apartment_name": apt_name,
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

            if should_break:
                chain_offset = current_offset if first_chain_of_day else 0
                pay, c100, c125, c150, c175, c200, chain_total, ends_at_0800 = close_chain_and_record(
                    current_chain_segments, break_reason, chain_offset)
                d_payment += pay
                d_calc100 += c100; d_calc125 += c125; d_calc150 += c150; d_calc175 += c175; d_calc200 += c200

                # Track last chain info for potential carryover to next day
                last_chain_total = chain_total
                last_chain_ended_at_0800 = ends_at_0800

                current_chain_segments = []
                first_chain_of_day = False

            if is_special:
                if etype == "standby":
                    is_cont = (last_etype == "standby" and last_end == start)
                    if not is_cont:
                        rate = get_standby_rate(conn, event.get("seg_id") or 0, event.get("apt"), bool(event.get("married")))
                        d_standby_pay += rate

                    chains.append({
                        "start_time": f"{start // 60 % 24:02d}:{start % 60:02d}",
                        "end_time": f"{end // 60 % 24:02d}:{end % 60:02d}",
                        "total_minutes": end - start,
                        "payment": 0,
                        "calc100": 0, "calc125": 0, "calc150": 0, "calc175": 0, "calc200": 0,
                        "type": "standby",
                        "apartment_name": event.get("apartment_name", ""),
                        "segments": [],
                        "break_reason": "",
                        "from_prev_day": start >= MINUTES_PER_DAY,
                        "effective_rate": minimum_wage,
                    })
                elif etype == "vacation" or etype == "sick":
                    hrs = (end - start) / 60
                    d_payment += hrs * minimum_wage

                    chains.append({
                        "start_time": f"{start // 60 % 24:02d}:{start % 60:02d}",
                        "end_time": f"{end // 60 % 24:02d}:{end % 60:02d}",
                        "total_minutes": end - start,
                        "payment": hrs * minimum_wage,
                        "calc100": 0, "calc125": 0, "calc150": 0, "calc175": 0, "calc200": 0,
                        "type": "vacation",
                        "apartment_name": "",
                        "segments": [],
                        "break_reason": "",
                        "from_prev_day": start >= MINUTES_PER_DAY,
                        "effective_rate": minimum_wage,
                    })

                last_end = end
                last_etype = etype
            else:
                current_chain_segments.append((start, end, event["label"], event["shift_id"], event.get("apartment_name", ""), event.get("actual_date")))
                last_end = end
                last_etype = etype

        # Close last chain
        if current_chain_segments:
            chain_offset = current_offset if first_chain_of_day else 0
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

