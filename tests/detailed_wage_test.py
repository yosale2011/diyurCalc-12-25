"""
בדיקה מפורטת של פערי חישוב שכר
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from core.database import get_conn
from core.logic import get_shabbat_times_cache
from core.history import get_minimum_wage_for_month
from app_utils import get_daily_segments_data


def analyze_employee_detailed(person_id: int, year: int = 2025, month: int = 12):
    """
    מנתח בפירוט את חישובי השכר לעובד
    """
    with get_conn() as conn:
        shabbat_cache = get_shabbat_times_cache(conn.conn)
        minimum_wage = get_minimum_wage_for_month(conn.conn, year, month)

        print(f"\n{'='*80}")
        print(f"ניתוח מפורט לעובד {person_id}")
        print(f"שכר מינימום: {minimum_wage:.2f}")
        print(f"{'='*80}")

        # קבל את הרצפים
        daily_segments, person_name = get_daily_segments_data(
            conn, person_id, year, month, shabbat_cache, minimum_wage
        )

        print(f"שם העובד: {person_name}")
        print(f"מספר ימים: {len(daily_segments)}")

        # סיכומים
        total_payment = 0
        total_standby_payment = 0
        total_calc100 = 0
        total_calc125 = 0
        total_calc150 = 0
        total_calc150_shabbat = 0
        total_calc150_overtime = 0
        total_calc175 = 0
        total_calc200 = 0
        standby_count = 0

        for day in daily_segments:
            day_str = day["day"]
            day_name = day["day_name"]
            payment = day.get("payment", 0) or 0
            standby_payment = day.get("standby_payment", 0) or 0

            total_payment += payment
            total_standby_payment += standby_payment

            print(f"\n{'-'*60}")
            print(f"יום: {day_str} ({day_name})")
            print(f"תשלום: {payment:.2f}, כוננות: {standby_payment:.2f}")

            for chain in day.get("chains", []):
                chain_type = chain.get("type", "work")
                start = chain.get("start_time", "")
                end = chain.get("end_time", "")
                shift_type = chain.get("shift_type", "")
                shift_name = chain.get("shift_name", "")

                c100 = chain.get("calc100", 0) or 0
                c125 = chain.get("calc125", 0) or 0
                c150 = chain.get("calc150", 0) or 0
                c150_shabbat = chain.get("calc150_shabbat", 0) or 0
                c150_overtime = chain.get("calc150_overtime", 0) or 0
                c175 = chain.get("calc175", 0) or 0
                c200 = chain.get("calc200", 0) or 0
                chain_payment = chain.get("payment", 0) or 0

                if chain_type == "standby":
                    standby_count += 1
                    print(f"  [כוננות] {start}-{end} | תשלום: {chain.get('standby_rate', 0):.2f}")
                elif chain_type in ("vacation", "sick"):
                    print(f"  [{chain_type}] {start}-{end} | {c100/60:.2f} שעות")
                    total_calc100 += c100
                else:
                    total_calc100 += c100
                    total_calc125 += c125
                    total_calc150 += c150
                    total_calc150_shabbat += c150_shabbat
                    total_calc150_overtime += c150_overtime
                    total_calc175 += c175
                    total_calc200 += c200

                    parts = []
                    if c100 > 0: parts.append(f"100%:{c100/60:.2f}שע'")
                    if c125 > 0: parts.append(f"125%:{c125/60:.2f}שע'")
                    if c150 > 0:
                        detail = f"150%:{c150/60:.2f}שע'"
                        if c150_shabbat > 0:
                            detail += f"(שבת:{c150_shabbat/60:.2f})"
                        if c150_overtime > 0:
                            detail += f"(נוספות:{c150_overtime/60:.2f})"
                        parts.append(detail)
                    if c175 > 0: parts.append(f"175%:{c175/60:.2f}שע'")
                    if c200 > 0: parts.append(f"200%:{c200/60:.2f}שע'")

                    print(f"  [{shift_type}] {start}-{end} | {shift_name}")
                    print(f"    {' | '.join(parts) if parts else 'ללא שעות'}")
                    print(f"    תשלום: {chain_payment:.2f}")

            # כוננויות מבוטלות
            for cancelled in day.get("cancelled_standbys", []):
                print(f"  [כוננות מבוטלת] {cancelled.get('start', 0)//60:02d}:{cancelled.get('start', 0)%60:02d}-{cancelled.get('end', 0)//60:02d}:{cancelled.get('end', 0)%60:02d} | {cancelled.get('reason', '')}")

        print(f"\n{'='*80}")
        print(f"סיכום חישוב רצפים:")
        print(f"{'='*80}")
        print(f"שעות 100%: {total_calc100/60:.2f}")
        print(f"שעות 125%: {total_calc125/60:.2f}")
        print(f"שעות 150%: {total_calc150/60:.2f}")
        print(f"  - 150% שבת: {total_calc150_shabbat/60:.2f}")
        print(f"  - 150% נוספות: {total_calc150_overtime/60:.2f}")
        print(f"שעות 175%: {total_calc175/60:.2f}")
        print(f"שעות 200%: {total_calc200/60:.2f}")
        print(f"תשלום עבודה: {total_payment:.2f}")
        print(f"תשלום כוננות: {total_standby_payment:.2f}")
        print(f"מספר כוננויות: {standby_count}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--person', type=int, default=101)
    parser.add_argument('--year', type=int, default=2025)
    parser.add_argument('--month', type=int, default=12)
    args = parser.parse_args()

    analyze_employee_detailed(args.person, args.year, args.month)
