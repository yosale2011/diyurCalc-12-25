"""
Debug script to check Gesher export vs HTML display for a specific person
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from core.database import get_conn
from core.logic import get_shabbat_times_cache
from core.history import get_minimum_wage_for_month
from services.gesher_exporter import (
    load_export_config_from_db,
    calculate_value,
)
from app_utils import get_daily_segments_data, aggregate_daily_segments_to_monthly
from core.database import PostgresConnection


def check_person(name_or_id, year: int = 2025, month: int = 1):
    """Check Gesher export vs HTML display for a specific person"""

    print(f"\n{'='*70}")
    print(f"   Checking Gesher Export vs HTML Display")
    print(f"   Filter: {name_or_id}, Period: {year}/{month:02d}")
    print(f"{'='*70}")

    with get_conn() as conn:
        cursor = conn.conn.cursor()
        if isinstance(name_or_id, int):
            cursor.execute("""
                SELECT id, name, meirav_code
                FROM people
                WHERE id = %s
            """, (name_or_id,))
        else:
            cursor.execute("""
                SELECT id, name, meirav_code
                FROM people
                WHERE name LIKE %s AND is_active::integer = 1
            """, (f"%{name_or_id}%",))
        person = cursor.fetchone()

        if not person:
            print(f"Person not found: {name_or_id}")
            return

        person_id, person_name, meirav_code = person
        print(f"\nFound: {person_name} (ID: {person_id}, Meirav code: {meirav_code})")

        minimum_wage = get_minimum_wage_for_month(conn.conn, year, month)
        shabbat_cache = get_shabbat_times_cache(conn.conn)
        print(f"Minimum wage: {minimum_wage} NIS")

        # חישוב סיכומים חודשיים
        conn_wrapper = PostgresConnection(conn.conn, use_pool=False)
        daily_segments, _ = get_daily_segments_data(conn_wrapper, person_id, year, month, shabbat_cache, minimum_wage)

        if not daily_segments:
            print("No data for this month")
            return

        monthly_totals = aggregate_daily_segments_to_monthly(conn_wrapper, daily_segments, person_id, year, month, minimum_wage)

        export_codes = load_export_config_from_db(conn)

        print(f"\n{'='*70}")
        print("   Payment Codes Configuration from DB")
        print(f"{'='*70}")

        for symbol, (internal_key, value_type, display_name) in export_codes.items():
            print(f"  {symbol}: {internal_key} -> {value_type}")

        print(f"\n{'='*70}")
        print("   Monthly Totals (from app_utils)")
        print(f"{'='*70}")

        relevant_keys = [
            'calc100', 'calc125', 'calc150', 'calc150_overtime', 'calc150_shabbat',
            'calc150_shabbat_100', 'calc150_shabbat_50',
            'calc175', 'calc200', 'calc_variable',
            'payment_calc100', 'payment_calc125', 'payment_calc150',
            'payment_calc150_overtime', 'payment_calc150_shabbat',
            'payment_calc175', 'payment_calc200', 'payment_calc_variable',
            'variable_rate_value', 'standby_payment', 'vacation_minutes',
            'vacation_payment', 'travel', 'sick_payment', 'total_payment'
        ]

        for key in relevant_keys:
            val = monthly_totals.get(key, 0)
            if val and val != 0:
                if 'payment' in key or key in ['travel', 'variable_rate_value']:
                    print(f"  {key}: {val:,.2f} NIS")
                elif 'minutes' in key or key.startswith('calc'):
                    hours = val / 60
                    print(f"  {key}: {val:,.0f} min = {hours:.2f} hrs")
                else:
                    print(f"  {key}: {val}")

        print(f"\n{'='*70}")
        print("   Gesher Export Comparison")
        print(f"{'='*70}")
        print(f"{'Code':<6} {'Key':<25} {'Qty':>10} {'Rate':>10} {'Qty*Rate':>12} {'Correct':>12} {'Diff':>10}")
        print("-" * 95)

        total_gesher = 0
        total_correct = 0

        for symbol, (internal_key, value_type, display_name) in export_codes.items():
            quantity, rate = calculate_value(monthly_totals, internal_key, value_type, minimum_wage)

            if quantity < 0.01 and rate < 0.01:
                continue

            gesher_total = quantity * rate
            total_gesher += gesher_total

            # מה הערך הנכון?
            correct_total = gesher_total  # ברירת מחדל

            # בדיקה אם יש payment מחושב מראש
            payment_key = f"payment_{internal_key}"
            if payment_key in monthly_totals and monthly_totals[payment_key] > 0:
                correct_total = monthly_totals[payment_key]
            elif internal_key == 'standby':
                correct_total = monthly_totals.get('standby_payment', 0) or 0
            elif internal_key == 'vacation':
                correct_total = monthly_totals.get('vacation_payment', 0) or 0
            elif internal_key == 'travel':
                correct_total = monthly_totals.get('travel', 0) or 0
            elif internal_key == 'sick_payment':
                correct_total = monthly_totals.get('sick_payment', 0) or 0

            total_correct += correct_total

            diff = gesher_total - correct_total
            diff_str = f"{diff:+.2f}" if abs(diff) > 0.01 else "-"

            print(f"{symbol:<6} {display_name:<25} {quantity:>10.2f} {rate:>10.2f} {gesher_total:>12.2f} {correct_total:>12.2f} {diff_str:>10}")

        print("-" * 95)
        print(f"{'סה״כ':<6} {'':<25} {'':<10} {'':<10} {total_gesher:>12.2f} {total_correct:>12.2f} {total_gesher - total_correct:>+10.2f}")

        print(f"\n{'='*70}")
        print("   Variable Rate Details")
        print(f"{'='*70}")

        calc_var = monthly_totals.get('calc_variable', 0)
        var_rate = monthly_totals.get('variable_rate_value', 0)
        payment_var = monthly_totals.get('payment_calc_variable', 0)

        if calc_var > 0:
            hours = calc_var / 60
            simple_calc = hours * var_rate
            print(f"  calc_variable: {calc_var:.0f} min = {hours:.2f} hrs")
            print(f"  variable_rate_value: {var_rate:.2f} NIS")
            print(f"  Simple calc (hrs * rate): {hours:.2f} * {var_rate:.2f} = {simple_calc:.2f} NIS")
            print(f"  payment_calc_variable (with multipliers): {payment_var:.2f} NIS")
            print(f"  Difference (overtime multipliers): {payment_var - simple_calc:.2f} NIS")

            var_rates = monthly_totals.get('variable_rates', {})
            if var_rates:
                print(f"\n  Breakdown by rate:")
                for rate_key, rate_data in var_rates.items():
                    print(f"    Rate {rate_key} NIS:")
                    for calc_type in ['calc100', 'calc125', 'calc150', 'calc175', 'calc200']:
                        mins = rate_data.get(calc_type, 0)
                        if mins > 0:
                            print(f"      {calc_type}: {mins/60:.2f} hrs")
                    print(f"      payment: {rate_data.get('payment', 0):.2f} NIS")


def find_person_with_variable_rate(year: int, month: int):
    """Find people with variable rate hours"""
    print(f"\n{'='*70}")
    print(f"   Finding people with variable rate hours ({year}/{month:02d})")
    print(f"{'='*70}")

    with get_conn() as conn:
        minimum_wage = get_minimum_wage_for_month(conn.conn, year, month)
        shabbat_cache = get_shabbat_times_cache(conn.conn)

        cursor = conn.conn.cursor()
        cursor.execute("SELECT id, name FROM people WHERE is_active::integer = 1")
        people = cursor.fetchall()
        cursor.close()

        conn_wrapper = PostgresConnection(conn.conn, use_pool=False)

        found = []
        for person_id, person_name in people:
            daily_segments, _ = get_daily_segments_data(conn_wrapper, person_id, year, month, shabbat_cache, minimum_wage)
            if not daily_segments:
                continue

            monthly_totals = aggregate_daily_segments_to_monthly(conn_wrapper, daily_segments, person_id, year, month, minimum_wage)
            calc_var = monthly_totals.get('calc_variable', 0)
            if calc_var > 0:
                payment_var = monthly_totals.get('payment_calc_variable', 0)
                var_rate = monthly_totals.get('variable_rate_value', 0)
                hours = calc_var / 60
                found.append((person_name, person_id, hours, var_rate, payment_var))

        print(f"\nFound {len(found)} people with variable rate:")
        for name, pid, hours, rate, payment in found:
            simple = hours * rate
            print(f"  {name} (ID:{pid}): {hours:.2f} hrs * {rate:.2f} = {simple:.2f}, actual payment = {payment:.2f}, diff = {payment - simple:.2f}")


if __name__ == "__main__":
    # Check person ID 192 (has rate 42)
    check_person(192, 2025, 11)
