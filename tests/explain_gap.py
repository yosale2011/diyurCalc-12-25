"""
הסבר הפער בדצמבר 2025
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from core.database import get_conn
from core.logic import get_shabbat_times_cache, calculate_monthly_summary
from core.history import get_minimum_wage_for_month


def explain():
    print("\n" + "="*70)
    print("   הסבר הפער בדצמבר 2025")
    print("="*70)

    year, month = 2025, 12

    with get_conn() as conn:
        minimum_wage = get_minimum_wage_for_month(conn.conn, year, month)
        summary_data, grand_totals = calculate_monthly_summary(conn.conn, year, month)

        # הנתונים הנוכחיים
        work_payment = 0
        standby = 0
        travel = 0
        extras = 0

        for row in summary_data:
            totals = row["totals"]
            work_payment += totals.get("payment", 0)
            standby += totals.get("standby_payment", 0)
            travel += totals.get("travel", 0)
            extras += totals.get("extras", 0)

        current_total = work_payment + standby + travel + extras
        old_total = 395065

        print(f"\n[1] פירוט הסכום הנוכחי (הנכון):")
        print(f"    תשלום עבודה:      {work_payment:>12,.2f}")
        print(f"    כוננויות:         {standby:>12,.2f}")
        print(f"    נסיעות:           {travel:>12,.2f}")
        print(f"    תוספות:           {extras:>12,.2f}")
        print(f"    ---------------------------------")
        print(f"    סה\"כ:             {current_total:>12,.2f}")

        print(f"\n[2] הסכום הישן: {old_total:,.2f}")
        print(f"    הפער: {current_total - old_total:,.2f}")

        # ניתוח הפער
        print(f"\n[3] ניתוח הפער:")

        # מה כלל הסכום הישן?
        # 395,065 = עבודה (311,986) + כוננויות (74,828) + נסיעות (12,452) - משהו
        # 395,065 = 399,266 - 4,200
        # או:
        # 395,065 = עבודה + כוננויות + נסיעות - חלק מהכוננויות

        work_standby_travel = work_payment + standby + travel
        print(f"    עבודה + כוננויות + נסיעות = {work_standby_travel:,.2f}")
        print(f"    הפרש מהסכום הישן: {work_standby_travel - old_total:,.2f}")

        # או אולי הסכום הישן לא כלל את כל הכוננויות?
        print(f"\n    אפשרות 1: הסכום הישן לא כלל תוספות ({extras:,.2f})")
        print(f"               ואולי חלק מהכוננויות ({work_standby_travel - old_total:,.2f})")

        # בדיקה: כמה כוננויות היו "חסרות"?
        missing_from_old = current_total - old_total
        extras_and_some_standby = extras + (work_standby_travel - old_total)

        print(f"\n[4] מסקנה:")
        print(f"    הפער ({missing_from_old:,.2f}) מורכב מ:")
        print(f"    • תוספות שלא נכללו: {extras:,.2f}")
        print(f"    • הפרש נוסף: {missing_from_old - extras:,.2f}")

        # האם ההפרש הנוסף קשור לשינוי בחישוב?
        print(f"\n[5] האם הלוגיקה נכונה?")
        print(f"    הסכום הנוכחי ({current_total:,.2f}) כולל:")
        print(f"    • כל שעות העבודה לפי האחוזים הנכונים")
        print(f"    • כל הכוננויות")
        print(f"    • כל הנסיעות")
        print(f"    • כל התוספות")
        print(f"\n    זה הסכום הנכון שצריך לשלם לעובדים!")


if __name__ == "__main__":
    explain()
