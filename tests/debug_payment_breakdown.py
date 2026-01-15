"""
Debug script to compare payment breakdown between old and new methods
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from datetime import datetime
from zoneinfo import ZoneInfo
from core.database import get_conn
from core.logic import get_shabbat_times_cache
from core.history import get_minimum_wage_for_month

LOCAL_TZ = ZoneInfo("Asia/Jerusalem")


def compare_methods():
    print("\n" + "="*70)
    print("   Comparing Payment Breakdown: Old vs New Method")
    print("="*70)

    year, month = 2025, 11

    with get_conn() as conn:
        minimum_wage = get_minimum_wage_for_month(conn.conn, year, month)
        shabbat_cache = get_shabbat_times_cache(conn.conn)

        from app_utils import get_daily_segments_data, aggregate_daily_segments_to_monthly
        from core.database import PostgresConnection

        conn_wrapper = PostgresConnection(conn.conn, use_pool=False)

        # Pick a few employees to compare
        test_employees = [
            (91, "אורן סויסה"),  # High earner
            (158, "בתאל שמואל קניג"),  # Has extras
            (101, "אביתר צעירי"),  # From top list
        ]

        for pid, pname in test_employees:
            print(f"\n{'='*70}")
            print(f"Employee: {pname} (ID: {pid})")
            print("="*70)

            # New method calculation
            daily_segments, _ = get_daily_segments_data(conn_wrapper, pid, year, month, shabbat_cache, minimum_wage)
            if not daily_segments:
                print("  No data")
                continue

            monthly = aggregate_daily_segments_to_monthly(conn_wrapper, daily_segments, pid, year, month, minimum_wage)

            # Extract values
            calc100 = monthly.get("calc100", 0)
            calc125 = monthly.get("calc125", 0)
            calc150 = monthly.get("calc150", 0)
            calc175 = monthly.get("calc175", 0)
            calc200 = monthly.get("calc200", 0)
            calc_variable = monthly.get("calc_variable", 0)
            variable_rate_extra = monthly.get("variable_rate_extra_payment", 0)
            vacation_mins = monthly.get("vacation_minutes", 0)
            vacation_payment = monthly.get("vacation_payment", 0)
            standby_payment = monthly.get("standby_payment", 0)
            travel = monthly.get("travel", 0)
            extras = monthly.get("extras", 0)
            payment = monthly.get("payment", 0)
            total_payment = monthly.get("total_payment", 0)

            # Manual calculation like old method
            calc_from_hours = (
                (calc100 / 60) * minimum_wage * 1.0 +
                (calc125 / 60) * minimum_wage * 1.25 +
                (calc150 / 60) * minimum_wage * 1.5 +
                (calc175 / 60) * minimum_wage * 1.75 +
                (calc200 / 60) * minimum_wage * 2.0
            )

            old_style_payment = calc_from_hours + variable_rate_extra + standby_payment + vacation_payment
            old_style_total = old_style_payment + travel + extras

            new_style_total = payment + standby_payment + travel + extras

            print(f"\n  Hours breakdown:")
            print(f"    calc100:  {calc100/60:>8.2f} hrs = {(calc100/60)*1.0*minimum_wage:>10.2f} NIS")
            print(f"    calc125:  {calc125/60:>8.2f} hrs = {(calc125/60)*1.25*minimum_wage:>10.2f} NIS")
            print(f"    calc150:  {calc150/60:>8.2f} hrs = {(calc150/60)*1.5*minimum_wage:>10.2f} NIS")
            print(f"    calc175:  {calc175/60:>8.2f} hrs = {(calc175/60)*1.75*minimum_wage:>10.2f} NIS")
            print(f"    calc200:  {calc200/60:>8.2f} hrs = {(calc200/60)*2.0*minimum_wage:>10.2f} NIS")
            print(f"    calc_var: {calc_variable/60:>8.2f} hrs (extra={variable_rate_extra:.2f})")
            print(f"    -----------------------------------------")
            print(f"    Calculated from hours:        {calc_from_hours:>10.2f} NIS")

            print(f"\n  Additional components:")
            print(f"    Vacation:  {vacation_mins/60:>6.2f} hrs = {vacation_payment:>10.2f} NIS")
            print(f"    Standby:                      {standby_payment:>10.2f} NIS")
            print(f"    Travel:                       {travel:>10.2f} NIS")
            print(f"    Extras:                       {extras:>10.2f} NIS")

            print(f"\n  Comparison:")
            print(f"    NEW: payment = {payment:.2f} (sum of daily payments)")
            print(f"    OLD: payment = {old_style_payment:.2f} (hours + standby + vacation)")
            print(f"    Difference in payment: {payment - old_style_payment:.2f}")

            print(f"\n    NEW: total_payment = {total_payment:.2f}")
            print(f"    OLD style total:     {old_style_total:.2f}")
            print(f"    NEW style total:     {new_style_total:.2f}")

            # Check if payment includes vacation
            sum_daily_payment = sum(ds.get("payment", 0) for ds in daily_segments)
            sum_daily_standby = sum(ds.get("standby_payment", 0) for ds in daily_segments)
            print(f"\n  Daily sums:")
            print(f"    Sum of daily 'payment':       {sum_daily_payment:.2f}")
            print(f"    Sum of daily 'standby':       {sum_daily_standby:.2f}")
            print(f"    monthly['payment']:           {payment:.2f}")
            print(f"    monthly['standby_payment']:   {standby_payment:.2f}")

        # Now calculate grand totals both ways
        print("\n" + "="*70)
        print("Grand Total Comparison (All Employees)")
        print("="*70)

        cursor = conn.conn.cursor()
        cursor.execute("SELECT id, name FROM people WHERE is_active::integer = 1")
        people = cursor.fetchall()
        cursor.close()

        grand_new_total = 0
        grand_old_total = 0

        for person in people:
            pid = person[0]

            daily_segments, _ = get_daily_segments_data(conn_wrapper, pid, year, month, shabbat_cache, minimum_wage)
            if not daily_segments:
                continue

            monthly = aggregate_daily_segments_to_monthly(conn_wrapper, daily_segments, pid, year, month, minimum_wage)

            # New method total
            new_total = monthly.get("total_payment", 0)

            # Old method calculation
            calc100 = monthly.get("calc100", 0)
            calc125 = monthly.get("calc125", 0)
            calc150 = monthly.get("calc150", 0)
            calc175 = monthly.get("calc175", 0)
            calc200 = monthly.get("calc200", 0)
            variable_rate_extra = monthly.get("variable_rate_extra_payment", 0)
            vacation_payment = monthly.get("vacation_payment", 0)
            standby_payment = monthly.get("standby_payment", 0)
            travel = monthly.get("travel", 0)
            extras = monthly.get("extras", 0)

            calc_from_hours = (
                (calc100 / 60) * minimum_wage * 1.0 +
                (calc125 / 60) * minimum_wage * 1.25 +
                (calc150 / 60) * minimum_wage * 1.5 +
                (calc175 / 60) * minimum_wage * 1.75 +
                (calc200 / 60) * minimum_wage * 2.0
            )

            old_payment = calc_from_hours + variable_rate_extra + standby_payment + vacation_payment
            old_total = old_payment + travel + extras

            grand_new_total += new_total
            grand_old_total += old_total

        print(f"\n  Grand Total (New Method): {grand_new_total:,.2f} NIS")
        print(f"  Grand Total (Old Method): {grand_old_total:,.2f} NIS")
        print(f"  Difference:               {grand_new_total - grand_old_total:,.2f} NIS")


if __name__ == "__main__":
    compare_methods()
