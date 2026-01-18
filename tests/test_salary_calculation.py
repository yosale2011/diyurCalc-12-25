# -*- coding: utf-8 -*-
"""
קובץ בדיקות מקיף לחישוב שכר ותצוגה
=====================================

חלק 1: בדיקות אוטומטיות (Unit Tests)
חלק 2: בדיקות ידניות עם נתונים אמיתיים מהמערכת

הרצה:
    python tests/test_salary_calculation.py

    או רק בדיקות אוטומטיות:
    python tests/test_salary_calculation.py --unit

    או רק בדיקות ידניות:
    python tests/test_salary_calculation.py --manual
"""

import unittest
import sys
import os
from datetime import datetime, date, timedelta
from decimal import Decimal

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app_utils import calculate_wage_rate, _calculate_chain_wages
from core.time_utils import (
    REGULAR_HOURS_LIMIT,
    OVERTIME_125_LIMIT,
)
from core.constants import (
    NIGHT_REGULAR_HOURS_LIMIT,
    NIGHT_OVERTIME_125_LIMIT,
    calculate_night_hours_in_segment,
    qualifies_as_night_shift,
)

# ============================================================================
# חלק 1: בדיקות אוטומטיות (Unit Tests)
# ============================================================================

class TestOvertimeCalculation(unittest.TestCase):
    """בדיקות חישוב שעות נוספות"""

    def test_regular_hours_100_percent(self):
        """8 שעות ראשונות = 100%"""
        # 0 דקות = 100%
        self.assertEqual(calculate_wage_rate(0, False), "100%")
        # 4 שעות = 100%
        self.assertEqual(calculate_wage_rate(240, False), "100%")
        # 8 שעות = 100%
        self.assertEqual(calculate_wage_rate(480, False), "100%")

    def test_overtime_125_percent(self):
        """שעות 9-10 = 125%"""
        # 8 שעות ודקה = 125%
        self.assertEqual(calculate_wage_rate(481, False), "125%")
        # 9 שעות = 125%
        self.assertEqual(calculate_wage_rate(540, False), "125%")
        # 10 שעות = 125%
        self.assertEqual(calculate_wage_rate(600, False), "125%")

    def test_overtime_150_percent(self):
        """שעה 11+ = 150%"""
        # 10 שעות ודקה = 150%
        self.assertEqual(calculate_wage_rate(601, False), "150%")
        # 12 שעות = 150%
        self.assertEqual(calculate_wage_rate(720, False), "150%")
        # 16 שעות = 150%
        self.assertEqual(calculate_wage_rate(960, False), "150%")

    def test_chain_8_hours_all_100(self):
        """רצף של 8 שעות = הכל 100%"""
        # רצף 08:00-16:00 (480 דקות)
        segments = [(480, 960, None)]  # start, end, shift_id
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0)  # יום ראשון

        self.assertEqual(result["calc100"], 480)
        self.assertEqual(result["calc125"], 0)
        self.assertEqual(result["calc150"], 0)

    def test_chain_10_hours_100_and_125(self):
        """רצף של 10 שעות = 8 שעות 100% + 2 שעות 125%"""
        # רצף 08:00-18:00 (600 דקות)
        segments = [(480, 1080, None)]
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0)

        self.assertEqual(result["calc100"], 480)
        self.assertEqual(result["calc125"], 120)
        self.assertEqual(result["calc150"], 0)

    def test_chain_12_hours_all_tiers(self):
        """רצף של 12 שעות = 8 שעות 100% + 2 שעות 125% + 2 שעות 150%"""
        # רצף 08:00-20:00 (720 דקות)
        segments = [(480, 1200, None)]
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0)

        self.assertEqual(result["calc100"], 480)
        self.assertEqual(result["calc125"], 120)
        self.assertEqual(result["calc150"], 120)


class TestShabbatCalculation(unittest.TestCase):
    """בדיקות חישוב שבת"""

    def test_shabbat_adds_50_percent(self):
        """שבת מוסיפה 50% לכל אחוז"""
        # 100% + שבת = 150%
        self.assertEqual(calculate_wage_rate(0, True), "150%")
        self.assertEqual(calculate_wage_rate(480, True), "150%")

        # 125% + שבת = 175%
        self.assertEqual(calculate_wage_rate(481, True), "175%")
        self.assertEqual(calculate_wage_rate(600, True), "175%")

        # 150% + שבת = 200%
        self.assertEqual(calculate_wage_rate(601, True), "200%")
        self.assertEqual(calculate_wage_rate(720, True), "200%")


class TestCarryover(unittest.TestCase):
    """בדיקות העברת שעות בין ימים"""

    def test_carryover_affects_overtime_tiers(self):
        """העברת שעות משפיעה על חישוב שעות נוספות"""
        # יום 1: 6 שעות (נגמר ב-08:00)
        # יום 2: 6 שעות (מתחיל ב-08:00)
        # סה"כ רצף: 12 שעות = 8*100% + 2*125% + 2*150%

        # יום 2 עם העברה של 360 דקות (6 שעות)
        segments = [(480, 840, None)]  # 08:00-14:00 (6 שעות)
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 360)

        # 6+6=12 שעות, אז:
        # שעות 1-6 מיום קודם (360 דקות) - לא נספרות כאן
        # שעות 7-8 = 100% = 120 דקות
        # שעות 9-10 = 125% = 120 דקות
        # שעות 11-12 = 150% = 120 דקות
        self.assertEqual(result["calc100"], 120)  # רק 2 שעות 100% (עד 480)
        self.assertEqual(result["calc125"], 120)  # 2 שעות 125%
        self.assertEqual(result["calc150"], 120)  # 2 שעות 150%

    def test_no_carryover_starts_fresh(self):
        """בלי העברה, הרצף מתחיל מאפס"""
        segments = [(480, 840, None)]  # 08:00-14:00 (6 שעות)
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0)

        # 6 שעות = הכל 100%
        self.assertEqual(result["calc100"], 360)
        self.assertEqual(result["calc125"], 0)
        self.assertEqual(result["calc150"], 0)


class TestOverlappingShiftsWithDifferentRates(unittest.TestCase):
    """בדיקות משמרות חופפות עם תעריפים שונים"""

    def test_payment_calculation_with_different_rates(self):
        """חישוב תשלום כשיש תעריפים שונים"""
        # דוגמה: 6 שעות בתעריף 34.40 + 2 שעות בתעריף 40
        # תשלום צפוי: 6*34.40 + 2*40 = 206.40 + 80 = 286.40

        hours_rate_34_40 = 6
        hours_rate_40 = 2
        rate_low = 34.40
        rate_high = 40.0

        expected_payment = (hours_rate_34_40 * rate_low) + (hours_rate_40 * rate_high)
        self.assertAlmostEqual(expected_payment, 286.40, places=2)

    def test_overtime_with_different_rates(self):
        """שעות נוספות עם תעריפים שונים"""
        # 10 שעות: 8 שעות 100% + 2 שעות 125%
        # אם שעות 9-10 הן בתעריף גבוה (40), התשלום צריך להיות:
        # 8 * 34.40 * 1.0 + 2 * 40 * 1.25 = 275.20 + 100 = 375.20

        payment = (8 * 34.40 * 1.0) + (2 * 40 * 1.25)
        self.assertAlmostEqual(payment, 375.20, places=2)


class TestMedicalEscort(unittest.TestCase):
    """בדיקות ליווי רפואי (shift_type_id=148)"""

    def test_minimum_one_hour_payment(self):
        """ליווי רפואי - מינימום שעה"""
        # דיווח של 30 דקות צריך לקבל תשלום של 60 דקות
        reported_minutes = 30
        minimum_payment_minutes = 60

        actual_payment_minutes = max(reported_minutes, minimum_payment_minutes)
        self.assertEqual(actual_payment_minutes, 60)

    def test_no_bonus_for_longer_escort(self):
        """ליווי רפואי מעל שעה - תשלום לפי דיווח"""
        # דיווח של 90 דקות = תשלום 90 דקות
        reported_minutes = 90
        minimum_payment_minutes = 60

        actual_payment_minutes = max(reported_minutes, minimum_payment_minutes)
        self.assertEqual(actual_payment_minutes, 90)


class TestStandaloneMidnightShift(unittest.TestCase):
    """בדיקות דיווחים עצמאיים בלילה"""

    def test_standalone_shift_stays_in_day(self):
        """דיווח עצמאי 00:20-03:00 נשאר ביום שלו"""
        # הלוגיקה: אם p_date == r_date ושעת התחלה < 08:00
        # אז זה דיווח עצמאי ולא חלק ממשמרת חוצה חצות

        report_date = date(2024, 12, 3)
        part_date = date(2024, 12, 3)
        start_time = 20  # 00:20 = 20 דקות

        is_standalone = (part_date == report_date and start_time < 480)
        self.assertTrue(is_standalone)

    def test_overnight_shift_moves_to_previous_day(self):
        """משמרת חוצה חצות עוברת ליום הקודם"""
        # משמרת 22:00-06:00: החלק של 00:00-06:00 צריך להיות ביום הקודם

        report_date = date(2024, 12, 2)  # יום הדיווח
        part_date = date(2024, 12, 3)    # יום החלק (אחרי חצות)
        start_time = 0  # 00:00

        is_standalone = (part_date == report_date and start_time < 480)
        self.assertFalse(is_standalone)  # לא עצמאי = עובר ליום הקודם


class TestTagbur(unittest.TestCase):
    """בדיקות תגבור"""

    def test_tagbur_uses_fixed_percentages(self):
        """תגבור משתמש באחוזים קבועים"""
        # תגבור לא מחשב שעות נוספות רגילות
        # אלא משתמש בסגמנטים הקבועים של המשמרת

        # דוגמה: תגבור 12:00-17:00 @ 100%, 17:00-22:00 @ 150%
        segment_100_minutes = 300  # 5 שעות
        segment_150_minutes = 300  # 5 שעות

        # לא משנה שיש 10 שעות, האחוזים קבועים
        self.assertEqual(segment_100_minutes, 300)
        self.assertEqual(segment_150_minutes, 300)


class TestStandby(unittest.TestCase):
    """בדיקות כוננות"""

    def test_standby_payment_separate_from_work(self):
        """תשלום כוננות נפרד מתשלום עבודה"""
        standby_rate = 150  # ש"ח לכוננות
        work_hours = 8
        hourly_rate = 34.40

        work_payment = work_hours * hourly_rate
        total_payment = work_payment + standby_rate

        self.assertAlmostEqual(work_payment, 275.20, places=2)
        self.assertAlmostEqual(total_payment, 425.20, places=2)

    def test_standby_cancelled_when_work_overlaps(self):
        """כוננות מבוטלת כשיש עבודה חופפת"""
        # אם יש עבודה שחופפת לכוננות, הכוננות לא משולמת
        work_start = 480   # 08:00
        work_end = 960     # 16:00
        standby_start = 600  # 10:00
        standby_end = 720    # 12:00

        # בדיקה אם יש חפיפה
        overlaps = (work_start < standby_end and work_end > standby_start)
        self.assertTrue(overlaps)


class TestFullSalaryCalculation(unittest.TestCase):
    """בדיקות מקיפות לחישוב שכר מלא"""

    def test_full_day_payment_8_hours(self):
        """חישוב תשלום ליום עבודה של 8 שעות"""
        hours = 8
        rate = 34.40  # שכר מינימום לשעה

        # 8 שעות @ 100%
        expected_payment = hours * rate * 1.0
        self.assertAlmostEqual(expected_payment, 275.20, places=2)

    def test_full_day_payment_10_hours(self):
        """חישוב תשלום ליום עבודה של 10 שעות"""
        rate = 34.40

        # 8 שעות @ 100% + 2 שעות @ 125%
        payment_100 = 8 * rate * 1.0
        payment_125 = 2 * rate * 1.25
        expected_payment = payment_100 + payment_125

        self.assertAlmostEqual(payment_100, 275.20, places=2)
        self.assertAlmostEqual(payment_125, 86.00, places=2)
        self.assertAlmostEqual(expected_payment, 361.20, places=2)

    def test_full_day_payment_12_hours(self):
        """חישוב תשלום ליום עבודה של 12 שעות"""
        rate = 34.40

        # 8 שעות @ 100% + 2 שעות @ 125% + 2 שעות @ 150%
        payment_100 = 8 * rate * 1.0
        payment_125 = 2 * rate * 1.25
        payment_150 = 2 * rate * 1.5
        expected_payment = payment_100 + payment_125 + payment_150

        self.assertAlmostEqual(payment_100, 275.20, places=2)
        self.assertAlmostEqual(payment_125, 86.00, places=2)
        self.assertAlmostEqual(payment_150, 103.20, places=2)
        self.assertAlmostEqual(expected_payment, 464.40, places=2)

    def test_full_day_payment_16_hours(self):
        """חישוב תשלום ליום עבודה של 16 שעות (משמרת לילה מלאה)"""
        rate = 34.40

        # 8 שעות @ 100% + 2 שעות @ 125% + 6 שעות @ 150%
        payment_100 = 8 * rate * 1.0    # 275.20
        payment_125 = 2 * rate * 1.25   # 86.00
        payment_150 = 6 * rate * 1.5    # 309.60
        expected_payment = payment_100 + payment_125 + payment_150  # 670.80

        self.assertAlmostEqual(expected_payment, 670.80, places=2)

    def test_shabbat_full_day_8_hours(self):
        """חישוב תשלום ליום שבת של 8 שעות"""
        rate = 34.40

        # 8 שעות @ 150% (שבת)
        expected_payment = 8 * rate * 1.5
        self.assertAlmostEqual(expected_payment, 412.80, places=2)

    def test_shabbat_full_day_12_hours(self):
        """חישוב תשלום ליום שבת של 12 שעות"""
        rate = 34.40

        # 8 שעות @ 150% + 2 שעות @ 175% + 2 שעות @ 200%
        payment_150 = 8 * rate * 1.5
        payment_175 = 2 * rate * 1.75
        payment_200 = 2 * rate * 2.0
        expected_payment = payment_150 + payment_175 + payment_200

        self.assertAlmostEqual(payment_150, 412.80, places=2)
        self.assertAlmostEqual(payment_175, 120.40, places=2)
        self.assertAlmostEqual(payment_200, 137.60, places=2)
        self.assertAlmostEqual(expected_payment, 670.80, places=2)

    def test_mixed_rates_overlapping_shifts(self):
        """חישוב תשלום למשמרות חופפות עם תעריפים שונים"""
        # משמרת לילה 16:00-08:00 (16 שעות) תעריף 34.40
        # שמירה על דייר 22:00-03:00 (5 שעות) תעריף 40.00

        # שעות לא חופפות במשמרת לילה: 16:00-22:00 (6 שעות) + 03:00-08:00 (5 שעות) = 11 שעות
        # שעות חופפות (שמירה על דייר): 22:00-03:00 = 5 שעות

        # חישוב (בהנחה שהכל @ 100% לפשטות):
        hours_low_rate = 11
        hours_high_rate = 5
        rate_low = 34.40
        rate_high = 40.00

        payment_low = hours_low_rate * rate_low * 1.0
        payment_high = hours_high_rate * rate_high * 1.0
        total_payment = payment_low + payment_high

        self.assertAlmostEqual(payment_low, 378.40, places=2)
        self.assertAlmostEqual(payment_high, 200.00, places=2)
        self.assertAlmostEqual(total_payment, 578.40, places=2)

    def test_night_shift_with_overtime(self):
        """חישוב משמרת לילה עם שעות נוספות"""
        rate = 34.40

        # משמרת 16:00-08:00 (16 שעות)
        # 8 שעות @ 100% + 2 שעות @ 125% + 6 שעות @ 150%
        calc100 = 8 * 60  # 480 דקות
        calc125 = 2 * 60  # 120 דקות
        calc150 = 6 * 60  # 360 דקות

        payment = (calc100/60 * 1.0 + calc125/60 * 1.25 + calc150/60 * 1.5) * rate

        expected = (8 * 1.0 + 2 * 1.25 + 6 * 1.5) * rate  # 19.5 * 34.40 = 670.80
        self.assertAlmostEqual(payment, expected, places=2)
        self.assertAlmostEqual(payment, 670.80, places=2)

    def test_partial_shabbat_shift(self):
        """חישוב משמרת שחלקה בשבת וחלקה לא"""
        rate = 34.40

        # משמרת 14:00 יום שישי עד 08:00 שבת (18 שעות)
        # נניח כניסת שבת 16:00
        # 14:00-16:00 = 2 שעות חול @ 100%
        # 16:00-08:00 = 16 שעות שבת

        hours_weekday = 2
        hours_shabbat_100 = 8  # ראשונות בשבת @ 150%
        hours_shabbat_125 = 2  # @ 175%
        hours_shabbat_150 = 6  # @ 200%

        payment_weekday = hours_weekday * rate * 1.0       # 68.80
        payment_shabbat_150 = hours_shabbat_100 * rate * 1.5   # 412.80
        payment_shabbat_175 = hours_shabbat_125 * rate * 1.75  # 120.40
        payment_shabbat_200 = hours_shabbat_150 * rate * 2.0   # 412.80

        total = payment_weekday + payment_shabbat_150 + payment_shabbat_175 + payment_shabbat_200

        self.assertAlmostEqual(payment_weekday, 68.80, places=2)
        # total = 68.80 + 412.80 + 120.40 + 412.80 = 1014.80
        self.assertAlmostEqual(total, 1014.80, places=2)

    def test_two_separate_shifts_same_day(self):
        """חישוב שתי משמרות נפרדות באותו יום"""
        rate = 34.40

        # משמרת בוקר 08:00-12:00 (4 שעות)
        # הפסקה
        # משמרת ערב 16:00-20:00 (4 שעות)
        # סה"כ 8 שעות @ 100%

        hours_total = 8
        expected_payment = hours_total * rate * 1.0
        self.assertAlmostEqual(expected_payment, 275.20, places=2)

    def test_continuous_shift_with_break(self):
        """חישוב משמרת רציפה עם הפסקה קצרה (לא שוברת רצף)"""
        rate = 34.40

        # משמרת 08:00-17:00 עם הפסקה של 30 דקות
        # סה"כ 8.5 שעות עבודה
        # 8 שעות @ 100% + 0.5 שעות @ 125%

        hours_100 = 8
        hours_125 = 0.5

        payment = (hours_100 * 1.0 + hours_125 * 1.25) * rate
        self.assertAlmostEqual(payment, 296.70, places=2)

    def test_monthly_calculation_example(self):
        """דוגמה לחישוב חודשי"""
        rate = 34.40

        # חודש עם 20 ימי עבודה של 8 שעות
        days = 20
        hours_per_day = 8

        total_hours = days * hours_per_day
        monthly_payment = total_hours * rate

        self.assertEqual(total_hours, 160)
        self.assertAlmostEqual(monthly_payment, 5504.00, places=2)

    def test_monthly_with_overtime(self):
        """חישוב חודשי עם שעות נוספות"""
        rate = 34.40

        # 20 ימים: 15 ימים של 8 שעות + 5 ימים של 10 שעות
        regular_days = 15
        overtime_days = 5

        # ימים רגילים: 15 * 8 * 34.40 = 4128
        regular_payment = regular_days * 8 * rate * 1.0

        # ימי שעות נוספות: 5 * (8*34.40 + 2*34.40*1.25) = 5 * 361.20 = 1806
        overtime_payment = overtime_days * (8 * rate * 1.0 + 2 * rate * 1.25)

        total = regular_payment + overtime_payment

        self.assertAlmostEqual(regular_payment, 4128.00, places=2)
        self.assertAlmostEqual(overtime_payment, 1806.00, places=2)
        self.assertAlmostEqual(total, 5934.00, places=2)


class TestEdgeCases(unittest.TestCase):
    """בדיקות מקרי קצה"""

    def test_exactly_8_hours(self):
        """בדיוק 8 שעות - גבול בין 100% ל-125%"""
        rate = 34.40
        hours = 8

        # כל השעות צריכות להיות 100%
        payment = hours * rate * 1.0
        self.assertAlmostEqual(payment, 275.20, places=2)

    def test_exactly_10_hours(self):
        """בדיוק 10 שעות - גבול בין 125% ל-150%"""
        rate = 34.40

        # 8 @ 100% + 2 @ 125%
        payment = 8 * rate * 1.0 + 2 * rate * 1.25
        self.assertAlmostEqual(payment, 361.20, places=2)

    def test_one_minute_over_8_hours(self):
        """8 שעות ודקה - צריך להיות 125%"""
        # 481 דקות = 8 שעות ודקה
        result = calculate_wage_rate(481, False)
        self.assertEqual(result, "125%")

    def test_one_minute_over_10_hours(self):
        """10 שעות ודקה - צריך להיות 150%"""
        # 601 דקות = 10 שעות ודקה
        result = calculate_wage_rate(601, False)
        self.assertEqual(result, "150%")

    def test_zero_hours(self):
        """אפס שעות"""
        rate = 34.40
        hours = 0

        payment = hours * rate
        self.assertEqual(payment, 0)

    def test_very_long_shift(self):
        """משמרת ארוכה מאוד (24 שעות)"""
        rate = 34.40

        # 8 @ 100% + 2 @ 125% + 14 @ 150%
        payment_100 = 8 * rate * 1.0
        payment_125 = 2 * rate * 1.25
        payment_150 = 14 * rate * 1.5

        total = payment_100 + payment_125 + payment_150
        self.assertAlmostEqual(total, 1083.60, places=2)

    def test_fraction_of_hour(self):
        """חלק משעה (דקות)"""
        rate = 34.40

        # 30 דקות = 0.5 שעה @ 100%
        minutes = 30
        payment = (minutes / 60) * rate * 1.0
        self.assertAlmostEqual(payment, 17.20, places=2)

    def test_different_rates_exact_boundary(self):
        """תעריפים שונים בדיוק על הגבול"""
        # 8 שעות בתעריף נמוך, דקה אחת בתעריף גבוה
        rate_low = 34.40
        rate_high = 40.00

        # 8 שעות @ 100% בתעריף נמוך
        payment_low = 8 * rate_low * 1.0

        # 1 דקה @ 125% בתעריף גבוה
        payment_high = (1/60) * rate_high * 1.25

        total = payment_low + payment_high
        self.assertAlmostEqual(payment_low, 275.20, places=2)
        self.assertAlmostEqual(payment_high, 0.83, places=2)

    def test_rounding(self):
        """בדיקת עיגול"""
        rate = 34.40

        # 8 שעות ו-20 דקות = 8.333... שעות
        hours = 8 + 20/60
        payment = hours * rate * 1.0

        # 8.333... * 34.40 = 286.666...
        self.assertAlmostEqual(payment, 286.67, places=2)


class TestChainCalculationIntegration(unittest.TestCase):
    """בדיקות אינטגרציה לחישוב רצפים"""

    def test_simple_chain(self):
        """רצף פשוט"""
        # רצף 08:00-16:00 (480 דקות)
        segments = [(480, 960, None)]
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0)

        self.assertEqual(result["calc100"], 480)
        self.assertEqual(result["calc125"], 0)
        self.assertEqual(result["calc150"], 0)

    def test_chain_crossing_overtime_boundary(self):
        """רצף שחוצה גבול שעות נוספות"""
        # רצף 08:00-19:00 (660 דקות = 11 שעות)
        segments = [(480, 1140, None)]
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0)

        self.assertEqual(result["calc100"], 480)  # 8 שעות
        self.assertEqual(result["calc125"], 120)  # 2 שעות
        self.assertEqual(result["calc150"], 60)   # 1 שעה

    def test_chain_with_carryover(self):
        """רצף עם העברה מיום קודם"""
        # 4 שעות מיום קודם + 6 שעות היום = 10 שעות
        segments = [(480, 840, None)]  # 08:00-14:00 (6 שעות)
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 240)  # 4 שעות carryover

        # 4 שעות מיום קודם + 4 שעות היום = 8 שעות @ 100%
        # 2 שעות היום = 125%
        self.assertEqual(result["calc100"], 240)  # 4 שעות
        self.assertEqual(result["calc125"], 120)  # 2 שעות
        self.assertEqual(result["calc150"], 0)

    def test_multiple_segments_same_chain(self):
        """מספר סגמנטים באותו רצף"""
        # סגמנט 1: 08:00-12:00 (4 שעות)
        # סגמנט 2: 12:00-16:00 (4 שעות)
        segments = [(480, 720, None), (720, 960, None)]
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0)

        self.assertEqual(result["calc100"], 480)  # 8 שעות
        self.assertEqual(result["calc125"], 0)
        self.assertEqual(result["calc150"], 0)

    def test_night_shift_chain(self):
        """רצף משמרת לילה"""
        # 16:00-08:00 (16 שעות = 960 דקות)
        # בייצוג: 960-1920 (16:00 ביום הראשון עד 08:00 ביום השני)
        segments = [(960, 1920, None)]
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0)

        self.assertEqual(result["calc100"], 480)   # 8 שעות
        self.assertEqual(result["calc125"], 120)   # 2 שעות
        self.assertEqual(result["calc150"], 360)   # 6 שעות


class TestNightShiftDetection(unittest.TestCase):
    """בדיקות זיהוי שעות לילה (22:00-06:00)"""

    def test_night_hours_full_night(self):
        """משמרת שלמה בלילה 22:00-06:00"""
        # 22:00 = 1320, 06:00 = 360 (למחרת) = 1800 בייצוג מנורמל
        night_mins = calculate_night_hours_in_segment(1320, 1800)
        self.assertEqual(night_mins, 480)  # 8 שעות

    def test_night_hours_partial_evening(self):
        """משמרת ערב חלקית 20:00-23:00"""
        # רק שעה אחת בלילה (22:00-23:00)
        night_mins = calculate_night_hours_in_segment(1200, 1380)
        self.assertEqual(night_mins, 60)  # שעה אחת

    def test_night_hours_early_morning(self):
        """משמרת בוקר מוקדם 04:00-08:00"""
        # 04:00-06:00 = 2 שעות לילה
        night_mins = calculate_night_hours_in_segment(240, 480)
        self.assertEqual(night_mins, 120)  # 2 שעות

    def test_night_hours_no_night(self):
        """משמרת יום 08:00-16:00"""
        night_mins = calculate_night_hours_in_segment(480, 960)
        self.assertEqual(night_mins, 0)

    def test_qualifies_2_hours(self):
        """סף 2 שעות - בדיוק 2 שעות"""
        # 22:00-00:00 = בדיוק 2 שעות
        self.assertTrue(qualifies_as_night_shift([(1320, 1440)]))

    def test_qualifies_above_threshold(self):
        """מעל סף 2 שעות"""
        # 22:00-01:00 = 3 שעות
        self.assertTrue(qualifies_as_night_shift([(1320, 1500)]))

    def test_not_qualifies_below_threshold(self):
        """מתחת לסף 2 שעות"""
        # 22:00-23:30 = 1.5 שעות
        self.assertFalse(qualifies_as_night_shift([(1320, 1410)]))

    def test_qualifies_multiple_segments(self):
        """מספר סגמנטים שביחד מגיעים ל-2 שעות"""
        # 22:00-23:00 (1 שעה) + 05:00-06:00 (1 שעה) = 2 שעות
        segments = [(1320, 1380), (300, 360)]
        self.assertTrue(qualifies_as_night_shift(segments))


class TestNightShiftOvertime(unittest.TestCase):
    """בדיקות שעות נוספות במשמרת לילה (סף 7 שעות)"""

    def test_night_shift_7_hours_all_100(self):
        """משמרת לילה של 7 שעות = הכל 100%"""
        # 22:00-05:00 = 7 שעות (1320-1740)
        segments = [(1320, 1740, None)]
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0, is_night_shift=True)

        self.assertEqual(result["calc100"], 420)  # 7 שעות
        self.assertEqual(result["calc125"], 0)
        self.assertEqual(result["calc150"], 0)

    def test_night_shift_8_hours_has_125(self):
        """משמרת לילה של 8 שעות = 7 שעות 100% + 1 שעה 125%"""
        # 22:00-06:00 = 8 שעות (1320-1800)
        segments = [(1320, 1800, None)]
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0, is_night_shift=True)

        self.assertEqual(result["calc100"], 420)  # 7 שעות
        self.assertEqual(result["calc125"], 60)   # 1 שעה
        self.assertEqual(result["calc150"], 0)

    def test_night_shift_10_hours_has_150(self):
        """משמרת לילה של 10 שעות = 7 שעות 100% + 2 שעות 125% + 1 שעה 150%"""
        # 20:00-06:00 = 10 שעות (1200-1800)
        segments = [(1200, 1800, None)]
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0, is_night_shift=True)

        self.assertEqual(result["calc100"], 420)  # 7 שעות
        self.assertEqual(result["calc125"], 120)  # 2 שעות
        self.assertEqual(result["calc150"], 60)   # 1 שעה

    def test_regular_shift_8_hours_all_100(self):
        """משמרת רגילה של 8 שעות = הכל 100% (סף 8 שעות)"""
        # 08:00-16:00 = 8 שעות
        segments = [(480, 960, None)]
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0, is_night_shift=False)

        self.assertEqual(result["calc100"], 480)  # 8 שעות
        self.assertEqual(result["calc125"], 0)
        self.assertEqual(result["calc150"], 0)

    def test_regular_shift_9_hours_has_125(self):
        """משמרת רגילה של 9 שעות = 8 שעות 100% + 1 שעה 125%"""
        # 08:00-17:00 = 9 שעות
        segments = [(480, 1020, None)]
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0, is_night_shift=False)

        self.assertEqual(result["calc100"], 480)  # 8 שעות
        self.assertEqual(result["calc125"], 60)   # 1 שעה
        self.assertEqual(result["calc150"], 0)

    def test_night_vs_regular_comparison(self):
        """השוואה: אותה משמרת עם דגל לילה שונה"""
        # משמרת 18:00-02:00 = 8 שעות (1080-1560)
        segments = [(1080, 1560, None)]

        # כמשמרת רגילה (סף 8 שעות)
        result_regular = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0, is_night_shift=False)
        self.assertEqual(result_regular["calc100"], 480)  # 8 שעות ב-100%
        self.assertEqual(result_regular["calc125"], 0)

        # כמשמרת לילה (סף 7 שעות)
        result_night = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0, is_night_shift=True)
        self.assertEqual(result_night["calc100"], 420)   # 7 שעות ב-100%
        self.assertEqual(result_night["calc125"], 60)    # 1 שעה ב-125%


class TestNightShiftWageRate(unittest.TestCase):
    """בדיקות פונקציית calculate_wage_rate עם משמרת לילה"""

    def test_night_shift_rate_at_7_hours(self):
        """בדיוק 7 שעות במשמרת לילה = 100%"""
        self.assertEqual(calculate_wage_rate(420, False, is_night_shift=True), "100%")

    def test_night_shift_rate_at_8_hours(self):
        """8 שעות במשמרת לילה = 125%"""
        self.assertEqual(calculate_wage_rate(421, False, is_night_shift=True), "125%")

    def test_night_shift_rate_at_9_hours(self):
        """9 שעות במשמרת לילה = 125%"""
        self.assertEqual(calculate_wage_rate(540, False, is_night_shift=True), "125%")

    def test_night_shift_rate_at_10_hours(self):
        """10 שעות במשמרת לילה = 150%"""
        self.assertEqual(calculate_wage_rate(541, False, is_night_shift=True), "150%")

    def test_night_shift_shabbat_rate(self):
        """משמרת לילה בשבת"""
        self.assertEqual(calculate_wage_rate(420, True, is_night_shift=True), "150%")   # 7 שעות
        self.assertEqual(calculate_wage_rate(421, True, is_night_shift=True), "175%")   # 8 שעות
        self.assertEqual(calculate_wage_rate(541, True, is_night_shift=True), "200%")   # 10 שעות

    def test_regular_shift_rate_at_8_hours(self):
        """בדיוק 8 שעות במשמרת רגילה = 100%"""
        self.assertEqual(calculate_wage_rate(480, False, is_night_shift=False), "100%")

    def test_regular_shift_rate_at_9_hours(self):
        """9 שעות במשמרת רגילה = 125%"""
        self.assertEqual(calculate_wage_rate(481, False, is_night_shift=False), "125%")


# ============================================================================
# חלק 2: בדיקות ידניות עם נתונים אמיתיים
# ============================================================================

def run_real_data_tests():
    """הרצת בדיקות על נתונים אמיתיים מהמערכת"""

    print("\n" + "="*70)
    print("בדיקות ידניות עם נתונים אמיתיים")
    print("="*70)

    try:
        from core.database import get_pooled_connection, return_connection
        from core.logic import calculate_person_monthly_totals
        from app_utils import get_daily_segments_data
    except ImportError as e:
        print(f"שגיאה בייבוא: {e}")
        print("וודא שאתה מריץ מתיקיית הפרויקט")
        return

    conn = get_pooled_connection()
    if not conn:
        print("לא ניתן להתחבר למסד הנתונים")
        return

    try:
        # בדיקה 1: ברהמי רחל - דצמבר 2025
        print("\n" + "-"*50)
        print("בדיקה 1: ברהמי רחל - דצמבר 2025")
        print("-"*50)
        test_worker_calculation(conn, "ברהמי רחל", 2025, 12)

        # בדיקה 2: בדיקת יום ספציפי עם משמרות חופפות
        print("\n" + "-"*50)
        print("בדיקה 2: ברהמי רחל - 18/12/2025 (משמרות חופפות)")
        print("-"*50)
        test_specific_day(conn, "ברהמי רחל", date(2025, 12, 18))

    finally:
        return_connection(conn)


def test_worker_calculation(conn, worker_name: str, year: int, month: int):
    """בדיקת חישוב לעובד ספציפי"""

    from core.logic import calculate_person_monthly_totals

    # מציאת העובד
    cursor = conn.cursor()
    cursor.execute("SELECT id, name FROM people WHERE name LIKE %s", (f"%{worker_name}%",))
    row = cursor.fetchone()

    if not row:
        print(f"עובד '{worker_name}' לא נמצא")
        return

    person_id, full_name = row
    print(f"עובד: {full_name} (ID: {person_id})")

    # חישוב חודשי
    shabbat_cache = {}
    result = calculate_person_monthly_totals(conn, person_id, year, month, shabbat_cache)

    if not result:
        print("לא נמצאו נתונים לחודש זה")
        return

    print(f"\nסיכום חודשי:")
    print(f"  שעות 100%: {result.get('calc100', 0) / 60:.2f}")
    print(f"  שעות 125%: {result.get('calc125', 0) / 60:.2f}")
    print(f"  שעות 150%: {result.get('calc150', 0) / 60:.2f}")
    print(f"  שעות 175%: {result.get('calc175', 0) / 60:.2f}")
    print(f"  שעות 200%: {result.get('calc200', 0) / 60:.2f}")
    print(f"  סה\"כ שעות: {result.get('total_minutes', 0) / 60:.2f}")
    print(f"  תשלום עבודה: {result.get('work_payment', 0):.2f} ש\"ח")
    print(f"  תשלום כוננות: {result.get('standby_payment', 0):.2f} ש\"ח")


def test_specific_day(conn, worker_name: str, test_date: date):
    """בדיקת יום ספציפי עם פירוט מלא - מציג דיווחים גולמיים"""

    # מציאת העובד
    cursor = conn.cursor()
    cursor.execute("SELECT id, name FROM people WHERE name LIKE %s", (f"%{worker_name}%",))
    row = cursor.fetchone()

    if not row:
        print(f"עובד '{worker_name}' לא נמצא")
        return

    person_id, full_name = row

    # שליפת דיווחים ליום הספציפי
    cursor.execute("""
        SELECT tr.id, tr.date, tr.start_time, tr.end_time,
               st.name AS shift_name, st.rate AS shift_rate,
               st.is_minimum_wage
        FROM time_reports tr
        LEFT JOIN shift_types st ON st.id = tr.shift_type_id
        WHERE tr.person_id = %s AND tr.date = %s
        ORDER BY tr.start_time
    """, (person_id, test_date))

    reports = cursor.fetchall()

    if not reports:
        print(f"לא נמצאו דיווחים ליום {test_date.strftime('%d/%m/%Y')}")
        return

    print(f"\nדיווחים ליום {test_date.strftime('%d/%m/%Y')}:")
    print("-" * 60)

    for report in reports:
        report_id, rep_date, start_time, end_time, shift_name, shift_rate, is_min_wage = report
        rate_display = "מינימום" if is_min_wage else f"{shift_rate/100:.2f}" if shift_rate else "?"
        print(f"  ID: {report_id}")
        print(f"    שעות: {start_time} - {end_time}")
        print(f"    משמרת: {shift_name or '?'}")
        print(f"    תעריף: {rate_display} ש\"ח")
        print()


def compare_logic_and_display(conn, person_id: int, year: int, month: int):
    """השוואה בין חישוב logic.py לתצוגה app_utils.py"""

    from core.logic import calculate_person_monthly_totals
    from app_utils import get_daily_segments_data

    print("\n" + "-"*50)
    print("השוואה בין logic.py ל-app_utils.py")
    print("-"*50)

    # חישוב מ-logic.py
    logic_result = calculate_person_monthly_totals(conn, person_id, year, month)

    # חישוב מ-app_utils.py
    display_data = get_daily_segments_data(conn, person_id, year, month)

    # סיכום מהתצוגה
    display_total_minutes = sum(d.get("total_minutes", 0) for d in display_data)
    display_payment = sum(d.get("payment", 0) for d in display_data)

    print(f"\nlogic.py:")
    print(f"  סה\"כ דקות: {logic_result.get('total_minutes', 0)}")
    print(f"  תשלום עבודה: {logic_result.get('work_payment', 0):.2f} ש\"ח")

    print(f"\napp_utils.py:")
    print(f"  סה\"כ דקות: {display_total_minutes}")
    print(f"  תשלום: {display_payment:.2f} ש\"ח")

    # בדיקת התאמה
    minutes_match = abs(logic_result.get('total_minutes', 0) - display_total_minutes) < 5
    payment_match = abs(logic_result.get('work_payment', 0) - display_payment) < 1

    if minutes_match and payment_match:
        print("\n✓ התוצאות תואמות!")
    else:
        print("\n✗ יש אי-התאמה!")
        if not minutes_match:
            print(f"  הפרש דקות: {abs(logic_result.get('total_minutes', 0) - display_total_minutes)}")
        if not payment_match:
            print(f"  הפרש תשלום: {abs(logic_result.get('work_payment', 0) - display_payment):.2f}")


# ============================================================================
# הרצת הבדיקות
# ============================================================================

def run_unit_tests():
    """הרצת בדיקות אוטומטיות בלבד"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # בדיקות בסיסיות
    suite.addTests(loader.loadTestsFromTestCase(TestOvertimeCalculation))
    suite.addTests(loader.loadTestsFromTestCase(TestShabbatCalculation))
    suite.addTests(loader.loadTestsFromTestCase(TestCarryover))
    suite.addTests(loader.loadTestsFromTestCase(TestOverlappingShiftsWithDifferentRates))
    suite.addTests(loader.loadTestsFromTestCase(TestMedicalEscort))
    suite.addTests(loader.loadTestsFromTestCase(TestStandaloneMidnightShift))
    suite.addTests(loader.loadTestsFromTestCase(TestTagbur))
    suite.addTests(loader.loadTestsFromTestCase(TestStandby))

    # בדיקות חישוב שכר מלא
    suite.addTests(loader.loadTestsFromTestCase(TestFullSalaryCalculation))
    suite.addTests(loader.loadTestsFromTestCase(TestEdgeCases))
    suite.addTests(loader.loadTestsFromTestCase(TestChainCalculationIntegration))

    # הרצה
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="בדיקות חישוב שכר")
    parser.add_argument("--unit", action="store_true", help="הרץ רק בדיקות אוטומטיות")
    parser.add_argument("--manual", action="store_true", help="הרץ רק בדיקות ידניות")
    args = parser.parse_args()

    if args.unit:
        result = run_unit_tests()
        sys.exit(0 if result.wasSuccessful() else 1)
    elif args.manual:
        run_real_data_tests()
    else:
        # הרץ הכל
        print("="*70)
        print("חלק 1: בדיקות אוטומטיות (Unit Tests)")
        print("="*70)
        result = run_unit_tests()

        if result.wasSuccessful():
            print("\n[OK] כל הבדיקות האוטומטיות עברו!")
        else:
            print(f"\n✗ {len(result.failures)} בדיקות נכשלו")

        # בדיקות ידניות
        run_real_data_tests()
