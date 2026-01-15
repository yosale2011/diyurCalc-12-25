"""
Debug מפורט: מה השתנה בדצמבר 2025?
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from core.database import get_conn
from core.logic import get_shabbat_times_cache, calculate_monthly_summary
from core.history import get_minimum_wage_for_month


def detailed_breakdown():
    print("\n" + "="*70)
    print("   ניתוח מפורט: דצמבר 2025")
    print("="*70)

    year, month = 2025, 12

    with get_conn() as conn:
        minimum_wage = get_minimum_wage_for_month(conn.conn, year, month)
        summary_data, grand_totals = calculate_monthly_summary(conn.conn, year, month)

        # פירוט כל הרכיבים
        print(f"\n[1] פירוט רכיבי השכר:")
        print(f"    תשלום עבודה (שעות):  {311985.87:>12,.2f} ש\"ח")
        print(f"    כוננויות:            {74827.80:>12,.2f} ש\"ח")
        print(f"    נסיעות:              {12452.00:>12,.2f} ש\"ח")
        print(f"    תוספות:              {16142.20:>12,.2f} ש\"ח")
        print(f"    ----------------------------------------")
        print(f"    סה\"כ חדש:            {415407.87:>12,.2f} ש\"ח")

        # מה היה לפני?
        old_total = 395065
        new_total = 415407.87
        gap = new_total - old_total

        print(f"\n[2] השוואה:")
        print(f"    סכום ישן:            {old_total:>12,.2f} ש\"ח")
        print(f"    סכום חדש:            {new_total:>12,.2f} ש\"ח")
        print(f"    פער:                 {gap:>12,.2f} ש\"ח")

        # מה לא היה כלול בסכום הישן?
        # אפשרויות:
        # 1. כוננויות לא נכללו
        # 2. חלק מהנסיעות/תוספות לא נכללו
        # 3. שילוב

        print(f"\n[3] ניתוח הפער:")
        print(f"    אם הפער = כוננויות: {74827.80:>12,.2f} (לא מתאים)")
        print(f"    אם הפער = נסיעות:   {12452.00:>12,.2f} (לא מתאים)")
        print(f"    אם הפער = תוספות:   {16142.20:>12,.2f} (לא מתאים)")

        # בואו נחשב הפוך - מה היה כלול בסכום הישן?
        # סכום ישן = 395,065
        # אם הוא כולל: עבודה + X
        # עבודה = 311,986
        # אז X = 395,065 - 311,986 = 83,079

        work_only = 311985.87
        old_includes = old_total - work_only

        print(f"\n[4] חישוב הפוך:")
        print(f"    סכום ישן:            {old_total:>12,.2f}")
        print(f"    פחות עבודה:          {work_only:>12,.2f}")
        print(f"    = מה שהיה כלול:      {old_includes:>12,.2f}")

        print(f"\n    מה יש לנו עכשיו מעבר לעבודה:")
        print(f"    כוננויות:            {74827.80:>12,.2f}")
        print(f"    נסיעות:              {12452.00:>12,.2f}")
        print(f"    תוספות:              {16142.20:>12,.2f}")
        print(f"    סה\"כ:                {74827.80 + 12452.00 + 16142.20:>12,.2f}")

        # בדיקה: האם הסכום הישן כלל כוננויות אבל לא נסיעות+תוספות?
        # או להפך?

        standby = 74827.80
        travel_extras = 12452.00 + 16142.20

        print(f"\n[5] אפשרויות:")
        print(f"    עבודה + כוננויות:                    {work_only + standby:>12,.2f}")
        print(f"    עבודה + נסיעות + תוספות:             {work_only + travel_extras:>12,.2f}")
        print(f"    עבודה בלבד:                          {work_only:>12,.2f}")

        # איזה מהם הכי קרוב ל-395,065?
        options = [
            ("עבודה בלבד", work_only),
            ("עבודה + כוננויות", work_only + standby),
            ("עבודה + נסיעות + תוספות", work_only + travel_extras),
            ("עבודה + כוננויות + נסיעות", work_only + standby + 12452.00),
            ("עבודה + כוננויות + תוספות", work_only + standby + 16142.20),
        ]

        print(f"\n[6] מה הכי קרוב ל-{old_total:,.0f}?")
        for name, val in options:
            diff = abs(val - old_total)
            print(f"    {name:40} = {val:>12,.2f}  (הפרש: {diff:>10,.2f})")


if __name__ == "__main__":
    detailed_breakdown()
