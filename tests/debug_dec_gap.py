"""
Debug: מאיפה מגיע הפער בדצמבר 2025?
לפני: 395,065 ש"ח
עכשיו: 415,408 ש"ח
פער: ~20,343 ש"ח
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from core.database import get_conn
from core.logic import get_shabbat_times_cache, calculate_monthly_summary
from core.history import get_minimum_wage_for_month


def debug_december_gap():
    print("\n" + "="*70)
    print("   Debug: פער בדצמבר 2025")
    print("   לפני: 395,065 | עכשיו: 415,408 | פער: ~20,343")
    print("="*70)

    year, month = 2025, 12

    with get_conn() as conn:
        minimum_wage = get_minimum_wage_for_month(conn.conn, year, month)
        print(f"\nשכר מינימום: {minimum_wage:.2f} ש\"ח")

        summary_data, grand_totals = calculate_monthly_summary(conn.conn, year, month)

        print(f"\n[1] פירוט grand_totals:")
        print(f"    payment:         {grand_totals.get('payment', 0):>12,.2f}")
        print(f"    total_payment:   {grand_totals.get('total_payment', 0):>12,.2f}")
        print(f"    standby_payment: {grand_totals.get('standby_payment', 0):>12,.2f}")
        print(f"    travel:          {grand_totals.get('travel', 0):>12,.2f}")
        print(f"    extras:          {grand_totals.get('extras', 0):>12,.2f}")

        # חישוב מפורט
        total_work_payment = 0
        total_standby = 0
        total_travel = 0
        total_extras = 0
        total_total = 0

        print(f"\n[2] סיכום לפי עובדים ({len(summary_data)} עובדים):")

        for row in summary_data:
            totals = row["totals"]
            total_work_payment += totals.get("payment", 0)
            total_standby += totals.get("standby_payment", 0)
            total_travel += totals.get("travel", 0)
            total_extras += totals.get("extras", 0)
            total_total += totals.get("total_payment", 0)

        print(f"    סה\"כ payment (עבודה):    {total_work_payment:>12,.2f}")
        print(f"    סה\"כ standby:            {total_standby:>12,.2f}")
        print(f"    סה\"כ travel:             {total_travel:>12,.2f}")
        print(f"    סה\"כ extras:             {total_extras:>12,.2f}")
        print(f"    סה\"כ total_payment:      {total_total:>12,.2f}")

        # חישוב מה צריך להיות
        expected_total = total_work_payment + total_standby + total_travel + total_extras
        print(f"\n[3] בדיקת נוסחה:")
        print(f"    payment + standby + travel + extras = {expected_total:,.2f}")
        print(f"    total_payment בפועל = {total_total:,.2f}")
        print(f"    הפרש: {total_total - expected_total:,.2f}")

        # השוואה לסכום הישן
        old_total = 395065
        new_total = total_total
        gap = new_total - old_total

        print(f"\n[4] השוואה לסכום הישן:")
        print(f"    סכום ישן:  {old_total:>12,.2f}")
        print(f"    סכום חדש:  {new_total:>12,.2f}")
        print(f"    פער:       {gap:>12,.2f}")

        # האם הפער שווה ל-standby?
        print(f"\n[5] האם הפער = כוננויות?")
        print(f"    standby_payment: {total_standby:>12,.2f}")
        print(f"    פער:             {gap:>12,.2f}")
        print(f"    הפרש:            {gap - total_standby:>12,.2f}")

        if abs(gap - total_standby) < 100:
            print(f"\n    [!] כן! הפער כמעט זהה לסכום הכוננויות")
            print(f"    המסקנה: לפני התיקון, הכוננויות לא נכללו ב-total_payment")


if __name__ == "__main__":
    debug_december_gap()
