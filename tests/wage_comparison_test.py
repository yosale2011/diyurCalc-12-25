"""
סקריפט בדיקת השוואת חישוב שכר
בודק את ההתאמה בין טאב רצפים (app_utils) לטאב ייצוא שכר (logic.py)

הבדיקה:
1. בוחר 5 עובדים מורכבים עם מקרי קצה
2. משווה את החישובים בין שתי השיטות
3. מדווח על פערים
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from core.database import get_conn
from core.logic import (
    get_shabbat_times_cache,
    calculate_person_monthly_totals,
    calculate_monthly_summary
)
from core.history import get_minimum_wage_for_month
from app_utils import get_daily_segments_data, aggregate_daily_segments_to_monthly


def find_complex_employees(conn, year: int, month: int, limit: int = 5):
    """
    מוצא עובדים מורכבים לבדיקה - כאלה שיש להם:
    1. משמרות לילה
    2. משמרות שבת/חג
    3. משמרות תגבור
    4. כוננויות
    5. שעות נוספות (יותר מ-8 שעות ביום)
    6. חופשות/מחלות
    """
    from datetime import datetime

    month_start = datetime(year, month, 1)
    if month == 12:
        month_end = datetime(year + 1, 1, 1)
    else:
        month_end = datetime(year, month + 1, 1)

    # מצא עובדים עם הכי הרבה מגוון במשמרות
    query = """
    WITH employee_stats AS (
        SELECT
            tr.person_id,
            p.name,
            COUNT(DISTINCT tr.id) as total_reports,
            COUNT(DISTINCT tr.shift_type_id) as shift_variety,
            COUNT(DISTINCT CASE WHEN st.name LIKE '%%לילה%%' THEN tr.id END) as night_shifts,
            COUNT(DISTINCT CASE WHEN st.name LIKE '%%שבת%%' OR st.name LIKE '%%חג%%' THEN tr.id END) as shabbat_shifts,
            COUNT(DISTINCT CASE WHEN st.name LIKE '%%תגבור%%' THEN tr.id END) as tagbur_shifts,
            COUNT(DISTINCT CASE WHEN st.name LIKE '%%חופשה%%' OR st.name LIKE '%%מחלה%%' THEN tr.id END) as vacation_sick,
            COUNT(DISTINCT tr.apartment_id) as apartment_variety,
            -- בדיקה אם יש דיווחים ארוכים (יותר מ-8 שעות) - בלי EXTRACT כי השדות הם TIME
            COUNT(DISTINCT CASE
                WHEN tr.end_time IS NOT NULL AND tr.start_time IS NOT NULL AND
                     tr.end_time < tr.start_time THEN tr.id
            END) as long_shifts
        FROM time_reports tr
        JOIN people p ON tr.person_id = p.id
        LEFT JOIN shift_types st ON tr.shift_type_id = st.id
        WHERE tr.date >= %s AND tr.date < %s
            AND p.is_active::integer = 1
        GROUP BY tr.person_id, p.name
    )
    SELECT
        person_id,
        name,
        total_reports,
        shift_variety,
        night_shifts,
        shabbat_shifts,
        tagbur_shifts,
        vacation_sick,
        apartment_variety,
        long_shifts,
        -- ניקוד מורכבות
        (shift_variety * 2 + night_shifts * 3 + shabbat_shifts * 3 +
         tagbur_shifts * 4 + vacation_sick * 2 + long_shifts * 2 +
         apartment_variety) as complexity_score
    FROM employee_stats
    WHERE total_reports >= 3  -- לפחות 3 דיווחים בחודש
    ORDER BY complexity_score DESC
    LIMIT %s
    """

    cursor = conn.execute(query, (month_start.date(), month_end.date(), limit))
    results = cursor.fetchall()

    return results


def compare_wage_calculations(conn, person_id: int, person_name: str, year: int, month: int):
    """
    משווה את חישובי השכר בין שתי השיטות
    """
    # קבל shabbat cache ושכר מינימום
    shabbat_cache = get_shabbat_times_cache(conn.conn)
    minimum_wage = get_minimum_wage_for_month(conn.conn, year, month)

    print(f"\n{'='*80}")
    print(f"בדיקת עובד: {person_name} (ID: {person_id})")
    print(f"חודש: {month:02d}/{year}, שכר מינימום: {minimum_wage:.2f} ש\"ח")
    print(f"{'='*80}")

    # שיטה 1: חישוב דרך רצפים (app_utils)
    try:
        daily_segments, _ = get_daily_segments_data(conn, person_id, year, month, shabbat_cache, minimum_wage)
        segments_totals = aggregate_daily_segments_to_monthly(conn, daily_segments, person_id, year, month, minimum_wage)
    except Exception as e:
        print(f"  [שגיאה] בחישוב רצפים: {e}")
        segments_totals = None

    # שיטה 2: חישוב דרך ייצוא שכר (logic.py)
    try:
        export_totals = calculate_person_monthly_totals(conn.conn, person_id, year, month, shabbat_cache, minimum_wage)
    except Exception as e:
        print(f"  [שגיאה] בחישוב ייצוא: {e}")
        export_totals = None

    if segments_totals is None or export_totals is None:
        print("  [!] לא ניתן להשוות - אחד מהחישובים נכשל")
        return None

    # השוואת שדות מרכזיים
    fields_to_compare = [
        ("calc100", "שעות 100%"),
        ("calc125", "שעות 125%"),
        ("calc150", "שעות 150%"),
        ("calc150_shabbat", "150% שבת"),
        ("calc150_shabbat_100", "150% שבת (100%)"),
        ("calc150_shabbat_50", "150% שבת (50%)"),
        ("calc150_overtime", "150% שעות נוספות"),
        ("calc175", "שעות 175%"),
        ("calc200", "שעות 200%"),
        ("standby_payment", "תשלום כוננות"),
        ("vacation_minutes", "דקות חופשה"),
        ("payment", "תשלום בסיס"),
        ("total_payment", "תשלום כולל"),
    ]

    discrepancies = []
    results = {
        "person_id": person_id,
        "person_name": person_name,
        "segments_totals": {},
        "export_totals": {},
        "discrepancies": []
    }

    print("\n  השוואת שדות:")
    print(f"  {'שדה':<25} {'רצפים':<15} {'ייצוא':<15} {'הפרש':<15} {'סטטוס':<10}")
    print(f"  {'-'*80}")

    for field, label in fields_to_compare:
        seg_val = segments_totals.get(field, 0) or 0
        exp_val = export_totals.get(field, 0) or 0

        # המר דקות לשעות עבור שדות שעות
        if "calc" in field or "minutes" in field:
            seg_display = f"{seg_val/60:.2f} שע'"
            exp_display = f"{exp_val/60:.2f} שע'"
            diff = seg_val - exp_val
            diff_display = f"{diff/60:.2f} שע'" if diff != 0 else "0"
        else:
            seg_display = f"{seg_val:.2f}"
            exp_display = f"{exp_val:.2f}"
            diff = seg_val - exp_val
            diff_display = f"{diff:.2f}" if diff != 0 else "0"

        # קבע סטטוס
        tolerance = 0.01  # סבילות של אגורה
        if abs(diff) <= tolerance:
            status = "[תקין]"
        else:
            status = "[פער!]"
            discrepancies.append({
                "field": field,
                "label": label,
                "segments": seg_val,
                "export": exp_val,
                "diff": diff
            })

        results["segments_totals"][field] = seg_val
        results["export_totals"][field] = exp_val

        print(f"  {label:<25} {seg_display:<15} {exp_display:<15} {diff_display:<15} {status:<10}")

    results["discrepancies"] = discrepancies

    # סיכום
    if discrepancies:
        print(f"\n  [!] נמצאו {len(discrepancies)} פערים!")
        for disc in discrepancies:
            print(f"      - {disc['label']}: הפרש של {disc['diff']:.2f}")
    else:
        print(f"\n  [V] כל השדות תואמים!")

    return results


def get_employee_reports_detail(conn, person_id: int, year: int, month: int):
    """
    מציג פירוט דיווחים לעובד
    """
    from datetime import datetime

    month_start = datetime(year, month, 1)
    if month == 12:
        month_end = datetime(year + 1, 1, 1)
    else:
        month_end = datetime(year, month + 1, 1)

    query = """
    SELECT
        tr.date,
        tr.start_time,
        tr.end_time,
        st.name as shift_name,
        a.name as apartment_name,
        a.apartment_type_id,
        tr.rate_apartment_type_id
    FROM time_reports tr
    LEFT JOIN shift_types st ON tr.shift_type_id = st.id
    LEFT JOIN apartments a ON tr.apartment_id = a.id
    WHERE tr.person_id = %s AND tr.date >= %s AND tr.date < %s
    ORDER BY tr.date, tr.start_time
    """

    cursor = conn.execute(query, (person_id, month_start.date(), month_end.date()))
    reports = cursor.fetchall()

    print(f"\n  פירוט דיווחים ({len(reports)} דיווחים):")
    print(f"  {'תאריך':<12} {'התחלה':<8} {'סיום':<8} {'משמרת':<25} {'דירה':<20} {'סוג':<5}")
    print(f"  {'-'*90}")

    for r in reports:
        date_str = r['date'].strftime('%d/%m/%Y') if r['date'] else ''
        start_str = str(r['start_time'])[:5] if r['start_time'] else ''
        end_str = str(r['end_time'])[:5] if r['end_time'] else ''
        shift_name = (r['shift_name'] or '')[:24]
        apt_name = (r['apartment_name'] or '')[:19]
        apt_type = r['rate_apartment_type_id'] or r['apartment_type_id'] or ''

        print(f"  {date_str:<12} {start_str:<8} {end_str:<8} {shift_name:<25} {apt_name:<20} {apt_type}")


def run_full_test(year: int = 2025, month: int = 12):
    """
    מריץ בדיקה מלאה
    """
    print("\n" + "="*80)
    print("   בדיקת השוואת חישוב שכר - רצפים מול ייצוא שכר")
    print("="*80)
    print(f"\nחודש נבדק: {month:02d}/{year}")

    with get_conn() as conn:
        # מצא עובדים מורכבים
        print("\n[1] מחפש עובדים מורכבים לבדיקה...")
        complex_employees = find_complex_employees(conn, year, month, limit=5)

        if not complex_employees:
            print("  [!] לא נמצאו עובדים עם דיווחים בחודש זה")
            return

        print(f"\nנמצאו {len(complex_employees)} עובדים מורכבים:")
        for emp in complex_employees:
            print(f"  - {emp['name']} (ID: {emp['person_id']}) - ניקוד מורכבות: {emp['complexity_score']}")
            print(f"    דיווחים: {emp['total_reports']}, לילות: {emp['night_shifts']}, שבתות: {emp['shabbat_shifts']}, תגבור: {emp['tagbur_shifts']}")

        # בדיקת כל עובד
        print("\n[2] מריץ בדיקות השוואה...")
        all_results = []
        total_discrepancies = 0

        for emp in complex_employees:
            person_id = emp['person_id']
            person_name = emp['name']

            # הצג פירוט דיווחים
            get_employee_reports_detail(conn, person_id, year, month)

            # השוואת חישובים
            result = compare_wage_calculations(conn, person_id, person_name, year, month)

            if result:
                all_results.append(result)
                total_discrepancies += len(result['discrepancies'])

        # סיכום סופי
        print("\n" + "="*80)
        print("   סיכום בדיקה")
        print("="*80)
        print(f"\nעובדים שנבדקו: {len(all_results)}")
        print(f"סה\"כ פערים שנמצאו: {total_discrepancies}")

        if total_discrepancies == 0:
            print("\n[V] כל החישובים תואמים בין שתי השיטות!")
        else:
            print("\n[!] נמצאו פערים בחישובים - יש לבדוק:")
            for result in all_results:
                if result['discrepancies']:
                    print(f"\n  עובד: {result['person_name']}")
                    for disc in result['discrepancies']:
                        print(f"    - {disc['label']}: רצפים={disc['segments']:.2f}, ייצוא={disc['export']:.2f}, הפרש={disc['diff']:.2f}")

        return all_results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='בדיקת השוואת חישוב שכר')
    parser.add_argument('--year', type=int, default=2025, help='שנה לבדיקה')
    parser.add_argument('--month', type=int, default=12, help='חודש לבדיקה')

    args = parser.parse_args()

    results = run_full_test(args.year, args.month)
