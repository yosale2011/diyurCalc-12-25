"""
בדיקה על מספר חודשים - וידוא שהחישוב זהה לכל תקופה
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


def multi_month_test():
    print("\n" + "="*70)
    print("   בדיקת התאמה על פני מספר חודשים")
    print("="*70)

    # בדיקה על 6 חודשים אחרונים
    months_to_test = [
        (2025, 12),
        (2025, 11),
        (2025, 10),
        (2025, 9),
        (2025, 8),
        (2025, 7),
    ]

    fields = ["calc100", "calc125", "calc150", "calc150_shabbat", "calc175", "calc200",
              "standby_payment", "total_payment"]

    with get_conn() as conn:
        shabbat_cache = get_shabbat_times_cache(conn.conn)

        for year, month in months_to_test:
            minimum_wage = get_minimum_wage_for_month(conn.conn, year, month)

            # Get all active employees
            cursor = conn.conn.cursor()
            cursor.execute("SELECT id, name FROM people WHERE is_active::integer = 1")
            people = cursor.fetchall()
            cursor.close()

            passed = 0
            failed = 0
            total_tested = 0

            for person in people:
                pid = person[0]
                pname = person[1]

                # חישוב רצפים
                daily_segments, _ = get_daily_segments_data(conn, pid, year, month, shabbat_cache, minimum_wage)
                if not daily_segments:
                    continue

                total_tested += 1
                segments_totals = aggregate_daily_segments_to_monthly(conn, daily_segments, pid, year, month, minimum_wage)

                # חישוב ייצוא
                export_totals = calculate_person_monthly_totals(conn.conn, pid, year, month, shabbat_cache, minimum_wage)

                # השוואה
                has_discrepancy = False
                for field in fields:
                    seg_val = segments_totals.get(field, 0) or 0
                    exp_val = export_totals.get(field, 0) or 0
                    if abs(seg_val - exp_val) > 0.01:
                        has_discrepancy = True
                        break

                if has_discrepancy:
                    failed += 1
                else:
                    passed += 1

            status = "V" if failed == 0 else "X"
            print(f"  [{status}] {month:02d}/{year}: {passed}/{total_tested} עברו ({failed} נכשלו)")

    print("\n" + "="*70)


if __name__ == "__main__":
    multi_month_test()
