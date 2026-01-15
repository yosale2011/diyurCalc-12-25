"""
בדיקה מקיפה - השוואת רצפים לייצוא על כל העובדים הפעילים
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from core.database import get_conn
from core.logic import get_shabbat_times_cache, calculate_person_monthly_totals
from core.history import get_minimum_wage_for_month
from app_utils import get_daily_segments_data, aggregate_daily_segments_to_monthly


def full_comparison():
    print("\n" + "="*70)
    print("   בדיקה מקיפה: השוואת רצפים לייצוא - כל העובדים")
    print("="*70)

    year, month = 2025, 12  # דצמבר 2025

    with get_conn() as conn:
        shabbat_cache = get_shabbat_times_cache(conn.conn)
        minimum_wage = get_minimum_wage_for_month(conn.conn, year, month)

        # Get all active employees
        cursor = conn.conn.cursor()
        cursor.execute("SELECT id, name FROM people WHERE is_active::integer = 1 ORDER BY name")
        people = cursor.fetchall()
        cursor.close()

        print(f"\nחודש: {month:02d}/{year}")
        print(f"שכר מינימום: {minimum_wage:.2f} ש\"ח")
        print(f"עובדים פעילים: {len(people)}")

        fields = ["calc100", "calc125", "calc150", "calc150_shabbat", "calc175", "calc200",
                  "standby_payment", "payment", "total_payment"]

        passed = 0
        failed = 0
        failed_employees = []

        for person in people:
            pid = person[0]
            pname = person[1]

            # חישוב רצפים
            daily_segments, _ = get_daily_segments_data(conn, pid, year, month, shabbat_cache, minimum_wage)
            segments_totals = aggregate_daily_segments_to_monthly(conn, daily_segments, pid, year, month, minimum_wage)

            # חישוב ייצוא
            export_totals = calculate_person_monthly_totals(conn.conn, pid, year, month, shabbat_cache, minimum_wage)

            # השוואה
            discrepancies = []
            for field in fields:
                seg_val = segments_totals.get(field, 0) or 0
                exp_val = export_totals.get(field, 0) or 0
                if abs(seg_val - exp_val) > 0.01:
                    discrepancies.append((field, seg_val, exp_val))

            if discrepancies:
                failed += 1
                failed_employees.append({
                    "id": pid,
                    "name": pname,
                    "discrepancies": discrepancies
                })
            else:
                passed += 1

        # סיכום
        print("\n" + "="*70)
        print("   תוצאות")
        print("="*70)
        print(f"\nעברו: {passed}")
        print(f"נכשלו: {failed}")

        if failed_employees:
            print("\n" + "-"*70)
            print("עובדים עם פערים:")
            print("-"*70)
            for emp in failed_employees:
                print(f"\n  {emp['name']} (ID {emp['id']}):")
                for field, seg, exp in emp['discrepancies']:
                    print(f"    {field}: רצפים={seg:.2f}, ייצוא={exp:.2f}, הפרש={seg-exp:.2f}")
        else:
            print("\n[V] כל העובדים עברו בהצלחה!")
            print("    החישוב ברצפים זהה לחישוב בייצוא - 100% התאמה")


if __name__ == "__main__":
    full_comparison()
