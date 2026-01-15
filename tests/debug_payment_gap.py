"""
Debug script to investigate the ~60,000 NIS gap in November 2025 totals
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from datetime import datetime
from zoneinfo import ZoneInfo
from core.database import get_conn

LOCAL_TZ = ZoneInfo("Asia/Jerusalem")


def debug_payment_gap():
    print("\n" + "="*60)
    print("   Debug: Investigating Payment Gap in November 2025")
    print("="*60)

    year, month = 2025, 11

    month_start = datetime(year, month, 1, tzinfo=LOCAL_TZ)
    month_end = datetime(year, month + 1, 1, tzinfo=LOCAL_TZ)

    with get_conn() as conn:
        # 1. Check payment_components total for November
        print("\n[1] Payment Components for November 2025:")

        # Total amount
        result = conn.execute("""
            SELECT COUNT(*) as count, SUM(quantity * rate) as total_amount
            FROM payment_components
            WHERE date >= %s AND date < %s
        """, (month_start, month_end)).fetchone()

        count = result["count"] or 0
        total_amount = (result["total_amount"] or 0) / 100
        print(f"    Records: {count}, Total Amount: {total_amount:,.2f} NIS")

        # By type
        print("\n    By component type:")
        by_type = conn.execute("""
            SELECT component_type_id, COUNT(*) as count, SUM(quantity * rate) as total_amount
            FROM payment_components
            WHERE date >= %s AND date < %s
            GROUP BY component_type_id
            ORDER BY component_type_id
        """, (month_start, month_end)).fetchall()

        for row in by_type:
            type_id = row["component_type_id"]
            cnt = row["count"]
            amt = (row["total_amount"] or 0) / 100
            type_name = "Travel" if type_id in (2, 7) else "Extras"
            print(f"    Type {type_id} ({type_name}): {cnt} records, {amt:,.2f} NIS")

        # 2. Check date format in database
        print("\n[2] Sample payment_components dates:")
        sample = conn.execute("""
            SELECT id, person_id, date, component_type_id, quantity, rate
            FROM payment_components
            WHERE date >= %s AND date < %s
            LIMIT 5
        """, (month_start, month_end)).fetchall()

        for row in sample:
            print(f"    ID={row['id']}, person={row['person_id']}, date={row['date']}, type={row['component_type_id']}, qty={row['quantity']}, rate={row['rate']}")

        # 3. Check if dates are being compared correctly
        print("\n[3] Date comparison test:")
        print(f"    month_start = {month_start} (type: {type(month_start)})")
        print(f"    month_end = {month_end} (type: {type(month_end)})")

        # Check what the date column type is
        first_row = conn.execute("SELECT date FROM payment_components LIMIT 1").fetchone()
        if first_row:
            print(f"    DB date value = {first_row['date']} (type: {type(first_row['date'])})")

        # 4. Compare totals using old vs new calculation
        print("\n[4] Calculating totals using unified logic:")

        from core.logic import get_shabbat_times_cache, calculate_monthly_summary
        from core.history import get_minimum_wage_for_month

        # Get minimum wage
        minimum_wage = get_minimum_wage_for_month(conn.conn, year, month)
        print(f"    Minimum wage: {minimum_wage:.2f} NIS")

        # Calculate using unified function
        summary_data, grand_totals = calculate_monthly_summary(conn.conn, year, month)

        print(f"\n    Results from calculate_monthly_summary:")
        print(f"    - Employees with data: {len(summary_data)}")
        print(f"    - Payment (work): {grand_totals.get('payment', 0):,.2f} NIS")
        print(f"    - Standby: {grand_totals.get('standby_payment', 0):,.2f} NIS")
        print(f"    - Travel: {grand_totals.get('travel', 0):,.2f} NIS")
        print(f"    - Extras: {grand_totals.get('extras', 0):,.2f} NIS")
        print(f"    - Total Payment: {grand_totals.get('total_payment', 0):,.2f} NIS")

        # 5. Check a specific employee to see if travel/extras are being added
        print("\n[5] Checking specific employee with payment_components:")

        # Find an employee with payment_components in November
        emp_with_comps = conn.execute("""
            SELECT DISTINCT p.id, p.name, COUNT(*) as comp_count, SUM(pc.quantity * pc.rate) as comp_total
            FROM people p
            JOIN payment_components pc ON pc.person_id = p.id
            WHERE pc.date >= %s AND pc.date < %s
            GROUP BY p.id, p.name
            ORDER BY comp_total DESC
            LIMIT 3
        """, (month_start, month_end)).fetchall()

        if emp_with_comps:
            from app_utils import get_daily_segments_data, aggregate_daily_segments_to_monthly
            from core.database import PostgresConnection

            shabbat_cache = get_shabbat_times_cache(conn.conn)
            conn_wrapper = PostgresConnection(conn.conn, use_pool=False)

            for emp in emp_with_comps:
                print(f"\n    Employee: {emp['name']} (ID: {emp['id']})")
                print(f"    Payment components: {emp['comp_count']} records, {(emp['comp_total'] or 0)/100:,.2f} NIS")

                # Calculate using aggregate_daily_segments_to_monthly
                daily_segments, _ = get_daily_segments_data(conn_wrapper, emp['id'], year, month, shabbat_cache, minimum_wage)
                monthly_totals = aggregate_daily_segments_to_monthly(conn_wrapper, daily_segments, emp['id'], year, month, minimum_wage)

                print(f"    Calculated travel: {monthly_totals.get('travel', 0):,.2f} NIS")
                print(f"    Calculated extras: {monthly_totals.get('extras', 0):,.2f} NIS")
                print(f"    Calculated total_payment: {monthly_totals.get('total_payment', 0):,.2f} NIS")


if __name__ == "__main__":
    debug_payment_gap()
