"""
Debug: בדיקת extras בדצמבר 2025
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


def check_extras():
    print("\n" + "="*70)
    print("   בדיקת extras (תוספות) בדצמבר 2025")
    print("="*70)

    year, month = 2025, 12

    month_start = datetime(year, month, 1, tzinfo=LOCAL_TZ)
    month_end = datetime(year + 1, 1, 1, tzinfo=LOCAL_TZ)

    with get_conn() as conn:
        # בדיקת payment_components
        result = conn.execute("""
            SELECT
                component_type_id,
                COUNT(*) as count,
                SUM(quantity * rate) as total_amount
            FROM payment_components
            WHERE date >= %s AND date < %s
            GROUP BY component_type_id
            ORDER BY component_type_id
        """, (month_start, month_end)).fetchall()

        print(f"\n[1] payment_components לפי סוג:")
        total_travel = 0
        total_extras = 0

        for row in result:
            type_id = row["component_type_id"]
            count = row["count"]
            amount = (row["total_amount"] or 0) / 100

            # סוגים 2, 7 = נסיעות, השאר = תוספות
            if type_id in (2, 7):
                category = "נסיעות"
                total_travel += amount
            else:
                category = "תוספות"
                total_extras += amount

            print(f"    סוג {type_id} ({category}): {count} רשומות, {amount:,.2f} ש\"ח")

        print(f"\n    סה\"כ נסיעות:  {total_travel:>12,.2f} ש\"ח")
        print(f"    סה\"כ תוספות:  {total_extras:>12,.2f} ש\"ח")
        print(f"    סה\"כ הכל:     {total_travel + total_extras:>12,.2f} ש\"ח")

        # בדיקה לפי עובדים
        print(f"\n[2] עובדים עם תוספות גבוהות (מעל 500 ש\"ח):")

        high_extras = conn.execute("""
            SELECT
                p.name,
                pc.person_id,
                SUM(pc.quantity * pc.rate) as total_amount
            FROM payment_components pc
            JOIN people p ON p.id = pc.person_id
            WHERE pc.date >= %s AND pc.date < %s
                AND pc.component_type_id NOT IN (2, 7)
            GROUP BY pc.person_id, p.name
            HAVING SUM(pc.quantity * pc.rate) > 50000
            ORDER BY total_amount DESC
            LIMIT 10
        """, (month_start, month_end)).fetchall()

        for row in high_extras:
            amount = (row["total_amount"] or 0) / 100
            print(f"    {row['name']}: {amount:,.2f} ש\"ח")

        # סיכום
        print(f"\n[3] סיכום:")
        print(f"    הפער הכולל:     ~20,343 ש\"ח")
        print(f"    תוספות בפועל:  {total_extras:,.2f} ש\"ח")

        if abs(total_extras - 20343) < 1000:
            print(f"\n    [!] הפער קרוב לסכום התוספות!")
            print(f"        ייתכן שהתוספות לא נכללו בחישוב הישן")


if __name__ == "__main__":
    check_extras()
