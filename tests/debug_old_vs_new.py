"""
Debug script to compare old vs new calculation
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


def debug_calculations():
    print("\n" + "="*60)
    print("   Debug: Comparing Calculation Methods")
    print("="*60)

    year, month = 2025, 11

    with get_conn() as conn:
        minimum_wage = get_minimum_wage_for_month(conn.conn, year, month)
        shabbat_cache = get_shabbat_times_cache(conn.conn)
        print(f"Minimum wage: {minimum_wage:.2f} NIS")

        # Take a sample employee
        person_id = 158  # Has payment components

        print(f"\n[1] Testing employee {person_id}:")

        # New method - aggregate_daily_segments_to_monthly
        from app_utils import get_daily_segments_data, aggregate_daily_segments_to_monthly
        from core.database import PostgresConnection

        conn_wrapper = PostgresConnection(conn.conn, use_pool=False)
        daily_segments, person_name = get_daily_segments_data(conn_wrapper, person_id, year, month, shabbat_cache, minimum_wage)

        print(f"\n    Daily segments for {person_name}:")
        total_payment_from_days = 0
        total_standby_from_days = 0
        for ds in daily_segments:
            day_payment = ds.get("payment", 0)
            day_standby = ds.get("standby_payment", 0)
            total_payment_from_days += day_payment
            total_standby_from_days += day_standby
            print(f"    {ds['day']}: payment={day_payment:.2f}, standby={day_standby:.2f}")

        print(f"\n    Sum from daily segments: payment={total_payment_from_days:.2f}, standby={total_standby_from_days:.2f}")

        monthly_totals = aggregate_daily_segments_to_monthly(conn_wrapper, daily_segments, person_id, year, month, minimum_wage)

        print(f"\n    monthly_totals['payment'] = {monthly_totals.get('payment', 0):.2f}")
        print(f"    monthly_totals['standby_payment'] = {monthly_totals.get('standby_payment', 0):.2f}")
        print(f"    monthly_totals['travel'] = {monthly_totals.get('travel', 0):.2f}")
        print(f"    monthly_totals['extras'] = {monthly_totals.get('extras', 0):.2f}")
        print(f"    monthly_totals['total_payment'] = {monthly_totals.get('total_payment', 0):.2f}")

        # Let's check what payment should be vs what daily segments give us
        print(f"\n[2] Analyzing payment calculation:")
        print(f"    Sum of daily payment: {total_payment_from_days:.2f}")
        print(f"    monthly_totals['payment']: {monthly_totals.get('payment', 0):.2f}")

        # The 'payment' in monthly_totals should equal sum of daily payments
        # Let's check the actual calculation

        # Calculate what payment should be from hours
        calc100 = monthly_totals.get("calc100", 0)
        calc125 = monthly_totals.get("calc125", 0)
        calc150 = monthly_totals.get("calc150", 0)
        calc175 = monthly_totals.get("calc175", 0)
        calc200 = monthly_totals.get("calc200", 0)

        calculated_payment = (
            (calc100 / 60) * 1.0 * minimum_wage +
            (calc125 / 60) * 1.25 * minimum_wage +
            (calc150 / 60) * 1.5 * minimum_wage +
            (calc175 / 60) * 1.75 * minimum_wage +
            (calc200 / 60) * 2.0 * minimum_wage
        )

        standby_payment = monthly_totals.get("standby_payment", 0)

        print(f"\n[3] Payment breakdown:")
        print(f"    calc100: {calc100/60:.2f} hrs = {(calc100/60)*1.0*minimum_wage:.2f} NIS")
        print(f"    calc125: {calc125/60:.2f} hrs = {(calc125/60)*1.25*minimum_wage:.2f} NIS")
        print(f"    calc150: {calc150/60:.2f} hrs = {(calc150/60)*1.5*minimum_wage:.2f} NIS")
        print(f"    calc175: {calc175/60:.2f} hrs = {(calc175/60)*1.75*minimum_wage:.2f} NIS")
        print(f"    calc200: {calc200/60:.2f} hrs = {(calc200/60)*2.0*minimum_wage:.2f} NIS")
        print(f"    Total from hours: {calculated_payment:.2f}")
        print(f"    Standby: {standby_payment:.2f}")
        print(f"    Total (hours + standby): {calculated_payment + standby_payment:.2f}")

        # Now check ALL employees
        print("\n" + "="*60)
        print("[4] Checking ALL employees sum:")

        # Get all active people
        cursor = conn.conn.cursor()
        cursor.execute("SELECT id, name FROM people WHERE is_active::integer = 1")
        people = cursor.fetchall()
        cursor.close()

        grand_payment = 0
        grand_standby = 0
        grand_travel = 0
        grand_extras = 0
        grand_total = 0

        for person in people:
            pid = person[0]
            pname = person[1]

            daily_segments, _ = get_daily_segments_data(conn_wrapper, pid, year, month, shabbat_cache, minimum_wage)
            if not daily_segments:
                continue

            monthly = aggregate_daily_segments_to_monthly(conn_wrapper, daily_segments, pid, year, month, minimum_wage)

            payment = monthly.get("payment", 0)
            standby = monthly.get("standby_payment", 0)
            travel = monthly.get("travel", 0)
            extras = monthly.get("extras", 0)
            total = monthly.get("total_payment", 0)

            grand_payment += payment
            grand_standby += standby
            grand_travel += travel
            grand_extras += extras
            grand_total += total

        print(f"\n    Grand payment (work): {grand_payment:,.2f} NIS")
        print(f"    Grand standby: {grand_standby:,.2f} NIS")
        print(f"    Grand travel: {grand_travel:,.2f} NIS")
        print(f"    Grand extras: {grand_extras:,.2f} NIS")
        print(f"    Grand total: {grand_total:,.2f} NIS")

        expected_total = grand_payment + grand_travel + grand_extras
        print(f"\n    Expected total (payment + travel + extras): {expected_total:,.2f} NIS")
        print(f"    Does grand_total include standby? {grand_payment} includes standby or not?")

        # Check if payment includes standby
        print(f"\n    payment = {grand_payment:,.2f}")
        print(f"    standby = {grand_standby:,.2f}")
        print(f"    payment - standby = {grand_payment - grand_standby:,.2f}")


if __name__ == "__main__":
    debug_calculations()
