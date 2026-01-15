"""
בדיקת מקרי קצה נוספים
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from core.database import get_conn
from core.logic import get_shabbat_times_cache, calculate_person_monthly_totals
from core.history import get_minimum_wage_for_month
from app_utils import get_daily_segments_data, aggregate_daily_segments_to_monthly


def compare_employee(conn, person_id: int, person_name: str, year: int, month: int):
    """השוואת חישובים לעובד"""
    shabbat_cache = get_shabbat_times_cache(conn.conn)
    minimum_wage = get_minimum_wage_for_month(conn.conn, year, month)

    # רצפים
    daily_segments, _ = get_daily_segments_data(conn, person_id, year, month, shabbat_cache, minimum_wage)
    segments_totals = aggregate_daily_segments_to_monthly(conn, daily_segments, person_id, year, month, minimum_wage)

    # ייצוא
    export_totals = calculate_person_monthly_totals(conn.conn, person_id, year, month, shabbat_cache, minimum_wage)

    # השוואה
    fields = ["calc100", "calc125", "calc150", "calc150_shabbat", "calc175", "calc200",
              "standby_payment", "payment", "total_payment"]

    discrepancies = []
    for field in fields:
        seg_val = segments_totals.get(field, 0) or 0
        exp_val = export_totals.get(field, 0) or 0
        if abs(seg_val - exp_val) > 0.01:
            discrepancies.append((field, seg_val, exp_val))

    status = "PASS" if not discrepancies else "FAIL"
    print(f"  [{status}] {person_name} (ID {person_id})")

    if discrepancies:
        for field, seg, exp in discrepancies:
            print(f"       {field}: רצפים={seg:.2f}, ייצוא={exp:.2f}")

    return len(discrepancies) == 0


def run_edge_case_tests():
    """הרצת בדיקות מקרי קצה"""
    print("\n" + "="*60)
    print("   בדיקת מקרי קצה נוספים")
    print("="*60)

    edge_cases = [
        # (person_id, name, description)
        (82, "דפנה בנימין", "ליווי רפואי קצר"),
        (138, "אפרת דהן", "מספר ליווים רפואיים"),
        (162, "קיילי קליינשטיין קלין", "לווי בית חולים"),
        (207, "יוסי אוחנה", "לווי בית חולים + rate override"),
        (182, "יונתן בוטבול", "4 דירות שונות"),
        (84, "שגיא חמני", "3 דירות שונות"),
        (181, "לי ארום גולד", "נשוי עם לילות"),
        (259, "ניר צלח", "נשוי עם לילות"),
        # מקרי קצה נוספים
        (199, "אביטבול בוסקילה שני", "הכי הרבה דיווחים (46)"),
        (75, "מאיה בן דוד", "דירה טיפולית"),
        (92, "עדינדיי הגוסה", "דירה טיפולית"),
        (116, "יובל חזות", "rate override (apt=2, rate=1) - תגבור מרומז"),
        (217, "אוריאן גלבע", "rate override"),
    ]

    with get_conn() as conn:
        passed = 0
        failed = 0

        for person_id, name, desc in edge_cases:
            print(f"\n{desc}:")
            if compare_employee(conn, person_id, name, 2025, 12):
                passed += 1
            else:
                failed += 1

        print("\n" + "="*60)
        print(f"סיכום: {passed} עברו, {failed} נכשלו")
        print("="*60)


if __name__ == "__main__":
    run_edge_case_tests()
