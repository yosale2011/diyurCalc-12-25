"""
Check what the original total was before unification
by using the route/template directly
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from core.database import get_conn
from core.logic import calculate_monthly_summary


def check_totals():
    print("\n" + "="*70)
    print("   Checking Monthly Summary Totals")
    print("="*70)

    year, month = 2025, 11

    with get_conn() as conn:
        summary_data, grand_totals = calculate_monthly_summary(conn.conn, year, month)

        print(f"\nNovember 2025 Summary:")
        print(f"  Employees: {len(summary_data)}")
        print(f"\n  Grand Totals from calculate_monthly_summary:")
        for key in sorted(grand_totals.keys()):
            val = grand_totals[key]
            if isinstance(val, (int, float)) and val != 0:
                if "calc" in key or "minutes" in key:
                    print(f"    {key}: {val/60:.2f} hrs ({val:.0f} mins)")
                else:
                    print(f"    {key}: {val:,.2f}")

        # Calculate what total_payment should be
        payment = grand_totals.get("payment", 0)
        total_payment = grand_totals.get("total_payment", 0)
        standby = grand_totals.get("standby_payment", 0)
        travel = grand_totals.get("travel", 0)
        extras = grand_totals.get("extras", 0)

        print(f"\n  Breakdown:")
        print(f"    payment (from grand_totals):       {payment:>12,.2f}")
        print(f"    total_payment (from grand_totals): {total_payment:>12,.2f}")
        print(f"    standby_payment:                   {standby:>12,.2f}")
        print(f"    travel:                            {travel:>12,.2f}")
        print(f"    extras:                            {extras:>12,.2f}")

        # The actual total should be:
        # payment (work) + standby + travel + extras
        # But 'payment' in grand_totals is being set to total_payment in the loop

        # Let's recalculate
        work_payment = 0
        for row in summary_data:
            totals = row["totals"]
            work_payment += totals.get("payment", 0)

        print(f"\n  Recalculated:")
        print(f"    Sum of employee payments: {work_payment:>12,.2f}")
        print(f"    + standby:                {standby:>12,.2f}")
        print(f"    + travel:                 {travel:>12,.2f}")
        print(f"    + extras:                 {extras:>12,.2f}")
        print(f"    = Expected total:         {work_payment + standby + travel + extras:>12,.2f}")


if __name__ == "__main__":
    check_totals()
