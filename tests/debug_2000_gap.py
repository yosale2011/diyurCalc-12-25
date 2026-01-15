"""
Debug script to investigate the ~2,000 NIS difference
between old calculation (399,876) and new calculation (401,837)
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


def investigate_gap():
    print("\n" + "="*70)
    print("   Investigating ~2,000 NIS Gap (401,837 vs 399,876)")
    print("="*70)

    year, month = 2025, 11

    with get_conn() as conn:
        minimum_wage = get_minimum_wage_for_month(conn.conn, year, month)
        shabbat_cache = get_shabbat_times_cache(conn.conn)

        from app_utils import get_daily_segments_data, aggregate_daily_segments_to_monthly
        from core.database import PostgresConnection

        conn_wrapper = PostgresConnection(conn.conn, use_pool=False)

        # Get all active people
        cursor = conn.conn.cursor()
        cursor.execute("SELECT id, name FROM people WHERE is_active::integer = 1 ORDER BY name")
        people = cursor.fetchall()
        cursor.close()

        print(f"\nActive employees: {len(people)}")
        print(f"Minimum wage: {minimum_wage:.2f} NIS")

        # Calculate totals for each employee
        employee_totals = []

        for person in people:
            pid = person[0]
            pname = person[1]

            daily_segments, _ = get_daily_segments_data(conn_wrapper, pid, year, month, shabbat_cache, minimum_wage)
            if not daily_segments:
                continue

            monthly = aggregate_daily_segments_to_monthly(conn_wrapper, daily_segments, pid, year, month, minimum_wage)

            total_payment = monthly.get("total_payment", 0)
            if total_payment > 0:
                employee_totals.append({
                    "id": pid,
                    "name": pname,
                    "payment": monthly.get("payment", 0),
                    "standby": monthly.get("standby_payment", 0),
                    "travel": monthly.get("travel", 0),
                    "extras": monthly.get("extras", 0),
                    "total": total_payment,
                    "calc100": monthly.get("calc100", 0),
                    "calc125": monthly.get("calc125", 0),
                    "calc150": monthly.get("calc150", 0),
                    "calc175": monthly.get("calc175", 0),
                    "calc200": monthly.get("calc200", 0),
                })

        # Sort by total payment descending
        employee_totals.sort(key=lambda x: x["total"], reverse=True)

        # Print top 10 employees
        print("\n" + "-"*70)
        print("Top 10 employees by total payment:")
        print("-"*70)
        print(f"{'Name':<25} {'Payment':>10} {'Standby':>10} {'Travel':>8} {'Extras':>8} {'Total':>12}")
        print("-"*70)

        for emp in employee_totals[:10]:
            print(f"{emp['name'][:24]:<25} {emp['payment']:>10.2f} {emp['standby']:>10.2f} {emp['travel']:>8.2f} {emp['extras']:>8.2f} {emp['total']:>12.2f}")

        # Calculate grand totals
        grand_payment = sum(e["payment"] for e in employee_totals)
        grand_standby = sum(e["standby"] for e in employee_totals)
        grand_travel = sum(e["travel"] for e in employee_totals)
        grand_extras = sum(e["extras"] for e in employee_totals)
        grand_total = sum(e["total"] for e in employee_totals)

        print("-"*70)
        print(f"{'TOTAL':<25} {grand_payment:>10.2f} {grand_standby:>10.2f} {grand_travel:>8.2f} {grand_extras:>8.2f} {grand_total:>12.2f}")
        print("-"*70)

        # Check the math
        print("\n" + "="*70)
        print("Verification:")
        print("="*70)
        print(f"  Payment (work hours):     {grand_payment:>12.2f}")
        print(f"  Standby payment:          {grand_standby:>12.2f}")
        print(f"  Travel:                   {grand_travel:>12.2f}")
        print(f"  Extras:                   {grand_extras:>12.2f}")
        print(f"  -----------------------------------------")
        calculated_total = grand_payment + grand_standby + grand_travel + grand_extras
        print(f"  Calculated Total:         {calculated_total:>12.2f}")
        print(f"  Reported Total:           {grand_total:>12.2f}")
        print(f"  Difference:               {grand_total - calculated_total:>12.2f}")

        # Compare with expected
        expected_old = 399876
        print(f"\n  Expected (old system):    {expected_old:>12.2f}")
        print(f"  New calculation:          {grand_total:>12.2f}")
        print(f"  Difference:               {grand_total - expected_old:>12.2f}")

        # Try to find where the difference comes from
        print("\n" + "="*70)
        print("Looking for unusual values:")
        print("="*70)

        # Check for employees with very high standby
        high_standby = [e for e in employee_totals if e["standby"] > 3000]
        if high_standby:
            print(f"\nEmployees with standby > 3000 NIS:")
            for emp in high_standby:
                print(f"  {emp['name']}: {emp['standby']:.2f} NIS")

        # Check total standby count
        month_start = datetime(year, month, 1, tzinfo=LOCAL_TZ)
        month_end = datetime(year, month + 1, 1, tzinfo=LOCAL_TZ)

        # Get raw standby payments from database if available
        print("\n" + "="*70)
        print("Checking standby calculation consistency:")
        print("="*70)

        # Let's check a specific employee's standby calculation
        # Pick one with moderate standby
        test_emp = next((e for e in employee_totals if 500 < e["standby"] < 1500), None)
        if test_emp:
            print(f"\nDetailed check for: {test_emp['name']} (ID: {test_emp['id']})")
            print(f"  Calculated standby: {test_emp['standby']:.2f} NIS")

            daily_segments, _ = get_daily_segments_data(conn_wrapper, test_emp['id'], year, month, shabbat_cache, minimum_wage)

            print(f"\n  Daily breakdown:")
            total_daily_standby = 0
            for ds in daily_segments:
                day_standby = ds.get("standby_payment", 0)
                if day_standby > 0:
                    print(f"    {ds['day']}: {day_standby:.2f} NIS")
                    total_daily_standby += day_standby
            print(f"  Sum of daily standby: {total_daily_standby:.2f} NIS")


if __name__ == "__main__":
    investigate_gap()
