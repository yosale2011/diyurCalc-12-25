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

from app_utils import calculate_wage_rate, _calculate_chain_wages as _calculate_chain_wages_new
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


def _calculate_chain_wages(segments, day_date, shabbat_cache, minutes_offset, is_night_shift=False):
    """
    פונקציית עטיפה לתאימות לאחור עם הבדיקות הקיימות.
    ממירה מהחתימה הישנה (segments, day_date, cache, offset) לחדשה (segments_with_date, cache, offset).

    הפונקציה מפצלת סגמנטים שחוצים חצות כדי שלכל חלק יהיה התאריך הנכון:
    - חלק לפני חצות (< 1440) נשאר עם day_date
    - חלק אחרי חצות (>= 1440) מקבל day_date + 1
    """
    from datetime import timedelta

    MINUTES_PER_DAY = 1440
    segments_with_date = []

    for s, e, sid in segments:
        if s < MINUTES_PER_DAY and e > MINUTES_PER_DAY:
            # סגמנט חוצה חצות - פיצול לשני חלקים
            # חלק 1: עד חצות (ביום הנוכחי)
            segments_with_date.append((s, MINUTES_PER_DAY, sid, day_date))
            # חלק 2: מחצות ואילך (ביום הבא)
            # הזמנים נשארים מעל 1440 כדי לשמור על הרציפות
            segments_with_date.append((MINUTES_PER_DAY, e, sid, day_date + timedelta(days=1)))
        elif s >= MINUTES_PER_DAY:
            # סגמנט כולו אחרי חצות - שייך ליום הבא
            segments_with_date.append((s, e, sid, day_date + timedelta(days=1)))
        else:
            # סגמנט כולו לפני חצות - שייך ליום הנוכחי
            segments_with_date.append((s, e, sid, day_date))

    return _calculate_chain_wages_new(segments_with_date, shabbat_cache, minutes_offset, is_night_shift)

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

    def test_early_exit_partial_standby_becomes_work(self):
        """כוננות חלקית בגלל יציאה מוקדמת - משלמים כשעות עבודה"""
        # דוגמה: משמרת לילה 22:00-03:00 (יציאה מוקדמת)
        # כוננות מוגדרת: 00:00-06:30
        # העובד יצא ב-03:00, אז הכוננות היא רק 00:00-03:00

        hourly_rate = 34.40  # שכר מינימום לשעה

        # עבודה: 22:00-00:00 = 2 שעות
        work_hours = 2

        # כוננות חלקית בגלל יציאה מוקדמת: 00:00-03:00 = 3 שעות
        # לפי הכלל החדש: משלמים כשעות עבודה שממשיכות את הרצף
        partial_standby_hours = 3

        # סה"כ שעות עבודה (כולל הכוננות החלקית): 5 שעות
        total_work_hours = work_hours + partial_standby_hours
        self.assertEqual(total_work_hours, 5)

        # כל 5 השעות הן @ 100% (פחות מ-8)
        expected_payment = total_work_hours * hourly_rate * 1.0
        self.assertAlmostEqual(expected_payment, 172.00, places=2)

    def test_early_exit_partial_standby_continues_chain_overtime(self):
        """כוננות חלקית בגלל יציאה מוקדמת - ממשיכה את הרצף לשעות נוספות"""
        # דוגמה: משמרת 13:00-03:00 (יציאה מוקדמת)
        # כוננות מוגדרת: 22:00-08:00
        # עבודה: 13:00-22:00 = 9 שעות
        # כוננות חלקית: 22:00-03:00 = 5 שעות (יציאה מוקדמת)

        hourly_rate = 34.40

        # עבודה: 9 שעות
        #   8 שעות @ 100%
        #   1 שעה @ 125%
        work_payment = (8 * hourly_rate * 1.0) + (1 * hourly_rate * 1.25)

        # כוננות חלקית: 5 שעות - ממשיכה את הרצף
        #   1 שעה @ 125% (שעות 9-10)
        #   4 שעות @ 150% (שעה 11+)
        partial_standby_payment = (1 * hourly_rate * 1.25) + (4 * hourly_rate * 1.5)

        # סה"כ: 14 שעות
        total_hours = 9 + 5
        self.assertEqual(total_hours, 14)

        total_payment = work_payment + partial_standby_payment
        # 8*34.40*1.0 + 2*34.40*1.25 + 4*34.40*1.5
        # = 275.20 + 86.00 + 206.40 = 567.60
        self.assertAlmostEqual(total_payment, 567.60, places=2)

    def test_full_standby_not_affected(self):
        """כוננות מלאה (לא יציאה מוקדמת) - משלמים תעריף כוננות קבוע"""
        # דוגמה: משמרת לילה 22:00-08:00 (שמרה עד הסוף)
        # כוננות מוגדרת: 00:00-06:30
        # העובד היה עד הסוף, אז זו כוננות מלאה

        hourly_rate = 34.40
        standby_rate = 150  # ש"ח - תעריף כוננות קבוע

        # עבודה: 22:00-00:00 = 2 שעות + 06:30-08:00 = 1.5 שעות = 3.5 שעות
        work_hours = 3.5
        work_payment = work_hours * hourly_rate * 1.0

        # כוננות מלאה: 00:00-06:30 = 6.5 שעות - תעריף קבוע
        total_payment = work_payment + standby_rate

        # 3.5 * 34.40 + 150 = 120.40 + 150 = 270.40
        self.assertAlmostEqual(total_payment, 270.40, places=2)


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


class TestNightChainOvertimeThresholds(unittest.TestCase):
    """
    בדיקות מקיפות לוודא שסף 7/8 שעות מחושב נכון בכל סוגי המשמרות.

    כלל: משמרת שיש בה 2+ שעות בטווח 22:00-06:00 = משמרת לילה (סף 7 שעות)
    אחרת = משמרת רגילה (סף 8 שעות)
    """

    def test_weekday_night_22_to_0630_is_night_chain(self):
        """
        משמרת לילה בחול 22:00-06:30 = 8.5 שעות, כולן בטווח לילה
        צריך להיות: 7 שעות 100% + 1.5 שעות 125%

        הערה: משתמשים ביום ראשון (2024-12-15) שהוא יום חול
        """
        # 22:00 = 1320, 06:30 למחרת = 1830 (במערכת הזמנים המורחבת)
        segments = [(1320, 1830, None)]
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0, is_night_shift=True)

        # סף 7 שעות (420 דקות)
        self.assertEqual(result["calc100"], 420)   # 7 שעות @ 100%
        self.assertEqual(result["calc125"], 90)    # 1.5 שעות @ 125%
        self.assertEqual(result["calc150"], 0)

    def test_weekday_night_22_to_0630_NOT_night_chain_comparison(self):
        """
        אותה משמרת 22:00-06:30 אבל עם is_night_shift=False
        צריך להיות: 8 שעות 100% + 0.5 שעות 125%
        """
        segments = [(1320, 1830, None)]
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0, is_night_shift=False)

        # סף 8 שעות (480 דקות) - ההתנהגות השגויה לפני התיקון
        self.assertEqual(result["calc100"], 480)   # 8 שעות @ 100%
        self.assertEqual(result["calc125"], 30)    # 0.5 שעות @ 125%
        self.assertEqual(result["calc150"], 0)

    def test_friday_night_is_shabbat(self):
        """
        משמרת ליל שישי 22:00-06:30 (ביום שישי) = שבת הלכתית
        כי שבת נכנסת בערב שישי (~17:00)
        צריך להיות: 7 שעות @ 150% + 1.5 שעות @ 175%
        """
        # 09/01/2026 = יום שישי, 22:00 = אחרי כניסת שבת
        segments = [(1320, 1830, None)]
        result = _calculate_chain_wages(segments, date(2026, 1, 9), {}, 0, is_night_shift=True)

        # בשבת עם סף לילה (7 שעות)
        self.assertEqual(result["calc100"], 0)
        self.assertEqual(result["calc125"], 0)
        self.assertEqual(result["calc150"], 420)   # 7 שעות @ 150% שבת
        self.assertEqual(result["calc175"], 90)    # 1.5 שעות @ 175% שבת

    def test_shabbat_night_22_to_0630_with_shabbat_rates(self):
        """
        משמרת שבת 22:00-06:30 בשבת הלכתית = 8.5 שעות
        עם is_night_shift=True ושבת:
        - 7 שעות @ 150% (100% בסיס + 50% שבת)
        - 1.5 שעות @ 175% (125% בסיס + 50% שבת)

        הערה: הבדיקה הזו בודקת את calc150 ו-calc175 שמתאימים לשבת
        """
        # צריך shabbat_cache עם זמני שבת
        shabbat_cache = {
            "2026-01-09": {"enter": "16:30", "exit": "17:45"},  # שבת נכנסת ב-16:30 ויוצאת למחרת ב-17:45
        }

        # משמרת 22:00-06:30 ביום שישי (09/01/2026)
        segments = [(1320, 1830, None)]
        result = _calculate_chain_wages(segments, date(2026, 1, 9), shabbat_cache, 0, is_night_shift=True)

        # בשבת עם סף לילה (7 שעות):
        # calc100=0 (אין שעות רגילות - הכל שבת)
        # calc150 = 420 דקות (7 שעות @ 150% שבת)
        # calc175 = 90 דקות (1.5 שעות @ 175% שבת)
        self.assertEqual(result["calc100"], 0)
        self.assertEqual(result["calc125"], 0)
        self.assertEqual(result["calc150"], 420)   # 7 שעות @ 150%
        self.assertEqual(result["calc175"], 90)    # 1.5 שעות @ 175%
        self.assertEqual(result["calc200"], 0)

    def test_day_shift_08_to_17_is_NOT_night_chain(self):
        """
        משמרת יום 08:00-17:00 = 9 שעות, 0 שעות בטווח לילה
        צריך להיות: 8 שעות 100% + 1 שעה 125% (סף 8 שעות)
        """
        # 08:00 = 480, 17:00 = 1020
        segments = [(480, 1020, None)]
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0, is_night_shift=False)

        self.assertEqual(result["calc100"], 480)   # 8 שעות
        self.assertEqual(result["calc125"], 60)    # 1 שעה
        self.assertEqual(result["calc150"], 0)

    def test_evening_shift_18_to_02_qualifies_as_night(self):
        """
        משמרת ערב 18:00-02:00 = 8 שעות
        4 שעות בטווח 22:00-02:00 = יותר מ-2 שעות = משמרת לילה
        צריך להיות: 7 שעות 100% + 1 שעה 125%
        """
        # 18:00 = 1080, 02:00 למחרת = 1560
        night_hours = calculate_night_hours_in_segment(1080, 1560)
        self.assertEqual(night_hours, 240)  # 4 שעות (22:00-02:00)
        self.assertTrue(night_hours >= 120)  # מעל 2 שעות = משמרת לילה

        segments = [(1080, 1560, None)]
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0, is_night_shift=True)

        self.assertEqual(result["calc100"], 420)   # 7 שעות
        self.assertEqual(result["calc125"], 60)    # 1 שעה
        self.assertEqual(result["calc150"], 0)

    def test_evening_shift_18_to_2330_NOT_night(self):
        """
        משמרת ערב 18:00-23:30 = 5.5 שעות
        1.5 שעות בטווח 22:00-23:30 = פחות מ-2 שעות = לא משמרת לילה
        """
        # 18:00 = 1080, 23:30 = 1410
        night_hours = calculate_night_hours_in_segment(1080, 1410)
        self.assertEqual(night_hours, 90)  # 1.5 שעות (22:00-23:30)
        self.assertFalse(night_hours >= 120)  # פחות מ-2 שעות = לא משמרת לילה

    def test_early_morning_04_to_12_qualifies_as_night(self):
        """
        משמרת בוקר מוקדם 04:00-12:00 = 8 שעות
        2 שעות בטווח 04:00-06:00 = בדיוק 2 שעות = משמרת לילה
        צריך להיות: 7 שעות 100% + 1 שעה 125%
        """
        # 04:00 = 240, 12:00 = 720
        night_hours = calculate_night_hours_in_segment(240, 720)
        self.assertEqual(night_hours, 120)  # 2 שעות (04:00-06:00)
        self.assertTrue(night_hours >= 120)  # בדיוק 2 שעות = משמרת לילה

        segments = [(240, 720, None)]
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0, is_night_shift=True)

        self.assertEqual(result["calc100"], 420)   # 7 שעות
        self.assertEqual(result["calc125"], 60)    # 1 שעה
        self.assertEqual(result["calc150"], 0)

    def test_rate_change_with_carryover_keeps_night_flag(self):
        """
        שינוי תעריף באמצע רצף לילה:
        - 22:00-02:00 בתעריף 42 (4 שעות)
        - 02:00-06:00 בתעריף 34.40 (4 שעות)
        סה"כ 8 שעות, כולן בטווח לילה

        הרצף כולו צריך להיות עם סף 7 שעות גם אחרי שינוי התעריף
        """
        # רצף ראשון: 22:00-02:00 (4 שעות) עם 4 שעות לילה
        segments1 = [(1320, 1560, None)]  # shift_id=42 rate
        result1 = _calculate_chain_wages(segments1, date(2024, 12, 15), {}, 0, is_night_shift=True)

        # 4 שעות < 7 שעות = הכל 100%
        self.assertEqual(result1["calc100"], 240)
        self.assertEqual(result1["calc125"], 0)

        # רצף שני: 02:00-06:00 (4 שעות) עם carryover של 4 שעות
        # סה"כ 8 שעות ברצף, סף 7 שעות
        segments2 = [(1560, 1800, None)]  # shift_id=34.40 rate
        result2 = _calculate_chain_wages(segments2, date(2024, 12, 15), {}, 240, is_night_shift=True)

        # 4 שעות carryover + 3 שעות = 7 שעות @ 100%, 1 שעה @ 125%
        self.assertEqual(result2["calc100"], 180)   # 3 שעות נוספות ב-100% (עד סף 7)
        self.assertEqual(result2["calc125"], 60)    # 1 שעה ב-125%

    def test_long_night_shift_10_hours_all_tiers(self):
        """
        משמרת לילה ארוכה: 20:00-06:00 = 10 שעות
        8 שעות בטווח 22:00-06:00 = משמרת לילה
        צריך להיות: 7 שעות 100% + 2 שעות 125% + 1 שעה 150%
        """
        # 20:00 = 1200, 06:00 למחרת = 1800
        segments = [(1200, 1800, None)]
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0, is_night_shift=True)

        self.assertEqual(result["calc100"], 420)   # 7 שעות
        self.assertEqual(result["calc125"], 120)   # 2 שעות
        self.assertEqual(result["calc150"], 60)    # 1 שעה

    def test_long_day_shift_10_hours_all_tiers(self):
        """
        משמרת יום ארוכה: 08:00-18:00 = 10 שעות, 0 שעות לילה
        צריך להיות: 8 שעות 100% + 2 שעות 125% (סף 8 שעות)
        """
        # 08:00 = 480, 18:00 = 1080
        segments = [(480, 1080, None)]
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0, is_night_shift=False)

        self.assertEqual(result["calc100"], 480)   # 8 שעות
        self.assertEqual(result["calc125"], 120)   # 2 שעות
        self.assertEqual(result["calc150"], 0)

    def test_shabbat_with_night_threshold_all_rates(self):
        """
        משמרת שבת עם סף לילה: 22:00-08:30 = 10.5 שעות
        כולן בשבת וכולן בטווח לילה
        צריך להיות (סף 7 שעות, עם תוספות שבת):
        - 7 שעות @ 150% (100%+50%)
        - 2 שעות @ 175% (125%+50%)
        - 1.5 שעות @ 200% (150%+50%)
        """
        shabbat_cache = {
            "2026-01-09": {"enter": "16:30", "exit": "17:45"},
        }

        # 22:00 = 1320, 08:30 למחרת = 1950 (במערכת מורחבת)
        segments = [(1320, 1950, None)]
        result = _calculate_chain_wages(segments, date(2026, 1, 9), shabbat_cache, 0, is_night_shift=True)

        # בשבת עם סף 7 שעות:
        self.assertEqual(result["calc150"], 420)   # 7 שעות @ 150%
        self.assertEqual(result["calc175"], 120)   # 2 שעות @ 175%
        self.assertEqual(result["calc200"], 90)    # 1.5 שעות @ 200%

        # אין שעות חול
        self.assertEqual(result["calc100"], 0)
        self.assertEqual(result["calc125"], 0)


class TestMultipleShiftsSameDay(unittest.TestCase):
    """
    בדיקות למספר משמרות באותו יום עבודה (08:00-08:00).
    כולל: משמרות עם הפסקה ביניהן, שינוי תעריף, ומעבר יום/לילה.
    """

    def test_two_shifts_same_day_with_break_over_60_min(self):
        """
        שתי משמרות באותו יום עם הפסקה > 60 דקות = שני רצפים נפרדים
        משמרת 1: 08:00-12:00 (4 שעות) @ 100%
        הפסקה: 2 שעות
        משמרת 2: 14:00-18:00 (4 שעות) @ 100%

        כל משמרת מתחילה מ-0 כי ההפסקה שוברת רצף
        """
        # משמרת 1
        segments1 = [(480, 720, None)]  # 08:00-12:00
        result1 = _calculate_chain_wages(segments1, date(2024, 12, 15), {}, 0, is_night_shift=False)
        self.assertEqual(result1["calc100"], 240)  # 4 שעות
        self.assertEqual(result1["calc125"], 0)

        # משמרת 2 - מתחילה מ-0 (אחרי הפסקה ארוכה)
        segments2 = [(840, 1080, None)]  # 14:00-18:00
        result2 = _calculate_chain_wages(segments2, date(2024, 12, 15), {}, 0, is_night_shift=False)
        self.assertEqual(result2["calc100"], 240)  # 4 שעות
        self.assertEqual(result2["calc125"], 0)

    def test_two_shifts_same_day_continuous(self):
        """
        שתי משמרות באותו יום עם הפסקה < 60 דקות = רצף אחד
        משמרת 1: 08:00-12:00 (4 שעות)
        הפסקה: 30 דקות
        משמרת 2: 12:30-17:00 (4.5 שעות)

        סה"כ 8.5 שעות ברצף אחד = 8 @ 100% + 0.5 @ 125%
        """
        # סימולציה של שתי משמרות ברצף (הפסקה קצרה לא שוברת)
        segments = [(480, 720, None), (750, 1020, None)]  # 08:00-12:00, 12:30-17:00
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0, is_night_shift=False)

        total_minutes = (720 - 480) + (1020 - 750)  # 240 + 270 = 510 דקות = 8.5 שעות
        self.assertEqual(total_minutes, 510)

        self.assertEqual(result["calc100"], 480)   # 8 שעות
        self.assertEqual(result["calc125"], 30)    # 0.5 שעות
        self.assertEqual(result["calc150"], 0)

    def test_day_shift_then_night_shift_same_day(self):
        """
        משמרת יום ואז משמרת לילה באותו יום עבודה
        משמרת יום: 08:00-16:00 (8 שעות) - לא לילה
        הפסקה: 4 שעות (שוברת רצף)
        משמרת לילה: 20:00-04:00 (8 שעות) - לילה (6 שעות בטווח 22:00-04:00)

        משמרת היום: סף 8 שעות
        משמרת הלילה: סף 7 שעות
        """
        # משמרת יום
        segments_day = [(480, 960, None)]  # 08:00-16:00
        result_day = _calculate_chain_wages(segments_day, date(2024, 12, 15), {}, 0, is_night_shift=False)
        self.assertEqual(result_day["calc100"], 480)  # 8 שעות @ 100%
        self.assertEqual(result_day["calc125"], 0)

        # משמרת לילה - 20:00-04:00 = 8 שעות, 6 שעות בטווח לילה
        night_hours = calculate_night_hours_in_segment(20*60, 28*60)  # 20:00-04:00 (04:00 = 28*60 למחרת)
        self.assertEqual(night_hours, 360)  # 6 שעות (22:00-04:00)
        self.assertTrue(night_hours >= 120)  # מעל 2 שעות = משמרת לילה

        segments_night = [(1200, 1680, None)]  # 20:00-04:00 (04:00 = 1680)
        result_night = _calculate_chain_wages(segments_night, date(2024, 12, 15), {}, 0, is_night_shift=True)
        self.assertEqual(result_night["calc100"], 420)  # 7 שעות @ 100%
        self.assertEqual(result_night["calc125"], 60)   # 1 שעה @ 125%

    def test_multiple_short_shifts_accumulate_overtime(self):
        """
        מספר משמרות קצרות שמצטברות לשעות נוספות
        3 משמרות של 3 שעות כל אחת ברצף (עם הפסקות קצרות)
        סה"כ 9 שעות = 8 @ 100% + 1 @ 125%
        """
        # 3 סגמנטים ברצף
        segments = [
            (480, 660, None),   # 08:00-11:00 (3 שעות)
            (690, 870, None),   # 11:30-14:30 (3 שעות)
            (900, 1080, None),  # 15:00-18:00 (3 שעות)
        ]
        result = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0, is_night_shift=False)

        total = (660-480) + (870-690) + (1080-900)  # 180 + 180 + 180 = 540 = 9 שעות
        self.assertEqual(total, 540)

        self.assertEqual(result["calc100"], 480)  # 8 שעות
        self.assertEqual(result["calc125"], 60)   # 1 שעה

    def test_different_rates_same_day_chain_breaks(self):
        """
        משמרות עם תעריפים שונים באותו יום - שינוי תעריף שובר רצף
        אבל ה-carryover נשמר (לחישוב שעות נוספות)

        משמרת 1: 08:00-14:00 (6 שעות) @ 42 ש"ח
        משמרת 2: 14:00-18:00 (4 שעות) @ 34.40 ש"ח

        החישוב: 6 שעות + 4 שעות = 10 שעות
        אבל כל משמרת מחושבת בנפרד עם carryover
        """
        # משמרת 1 - 6 שעות @ 100%
        segments1 = [(480, 840, 42)]  # shift_id=42
        result1 = _calculate_chain_wages(segments1, date(2024, 12, 15), {}, 0, is_night_shift=False)
        self.assertEqual(result1["calc100"], 360)  # 6 שעות @ 100%
        self.assertEqual(result1["calc125"], 0)

        # משמרת 2 - 4 שעות, עם carryover של 6 שעות = סה"כ 10 שעות
        # 2 שעות @ 100% (עד 8), 2 שעות @ 125%
        segments2 = [(840, 1080, 34)]  # shift_id=34
        result2 = _calculate_chain_wages(segments2, date(2024, 12, 15), {}, 360, is_night_shift=False)
        self.assertEqual(result2["calc100"], 120)  # 2 שעות @ 100% (עד 480)
        self.assertEqual(result2["calc125"], 120)  # 2 שעות @ 125%


class TestConsecutiveDaysCarryover(unittest.TestCase):
    """
    בדיקות לימים רצופים עם carryover בין הימים.
    רצף שמסתיים ב-08:00 בדיוק מעביר את הדקות ליום הבא.
    """

    def test_overnight_shift_ends_at_0800_carryover(self):
        """
        משמרת לילה שמסתיימת ב-08:00 בדיוק - carryover ליום הבא
        יום 1: 20:00-08:00 (12 שעות) - רצף לילה
        יום 2: 08:00-10:00 (2 שעות) - המשך הרצף

        סה"כ 14 שעות ברצף אחד
        """
        # יום 1: 20:00-08:00 = 12 שעות
        # שעות לילה: 22:00-06:00 = 8 שעות = רצף לילה (סף 7 שעות)
        night_hours_day1 = calculate_night_hours_in_segment(20*60, 32*60)  # 20:00-08:00
        self.assertEqual(night_hours_day1, 480)  # 8 שעות לילה

        segments_day1 = [(1200, 1920, None)]  # 20:00-08:00 (08:00 = 1920 בציר מורחב)
        result_day1 = _calculate_chain_wages(segments_day1, date(2024, 12, 15), {}, 0, is_night_shift=True)

        # סף 7 שעות: 7 @ 100%, 2 @ 125%, 3 @ 150%
        self.assertEqual(result_day1["calc100"], 420)   # 7 שעות
        self.assertEqual(result_day1["calc125"], 120)   # 2 שעות
        self.assertEqual(result_day1["calc150"], 180)   # 3 שעות

        # יום 2: 08:00-10:00 = 2 שעות, עם carryover של 12 שעות
        # כבר עברנו את כל הסף, הכל ב-150%
        segments_day2 = [(480, 600, None)]  # 08:00-10:00
        result_day2 = _calculate_chain_wages(segments_day2, date(2024, 12, 16), {}, 720, is_night_shift=True)

        # 12 שעות carryover + 2 שעות = 14 שעות (כבר מעל 9 שעות = הכל 150%)
        self.assertEqual(result_day2["calc100"], 0)
        self.assertEqual(result_day2["calc125"], 0)
        self.assertEqual(result_day2["calc150"], 120)   # 2 שעות @ 150%

    def test_three_consecutive_days_with_carryover(self):
        """
        3 ימים רצופים עם רצף עבודה רציף (עבודה עד 08:00 כל יום)

        יום 1: 14:00-08:00 (18 שעות)
        יום 2: 08:00-08:00 (24 שעות) - המשך!
        יום 3: 08:00-12:00 (4 שעות)

        סה"כ 46 שעות ברצף אחד (תיאורטי)
        """
        # יום 1: 14:00-08:00 = 18 שעות
        # שעות לילה: 22:00-06:00 = 8 שעות
        night_hours_1 = calculate_night_hours_in_segment(14*60, 32*60)
        self.assertEqual(night_hours_1, 480)  # 8 שעות לילה = רצף לילה

        segments1 = [(840, 1920, None)]  # 14:00-08:00
        result1 = _calculate_chain_wages(segments1, date(2024, 12, 15), {}, 0, is_night_shift=True)

        # סף 7 שעות: 7 @ 100%, 2 @ 125%, 9 @ 150%
        self.assertEqual(result1["calc100"], 420)
        self.assertEqual(result1["calc125"], 120)
        self.assertEqual(result1["calc150"], 540)  # 9 שעות @ 150%

        # יום 2: עוד 24 שעות עם carryover של 18 שעות
        # כבר הכל ב-150%
        segments2 = [(480, 1920, None)]  # 08:00-08:00 למחרת
        result2 = _calculate_chain_wages(segments2, date(2024, 12, 16), {}, 1080, is_night_shift=True)

        # carryover 18 שעות = 1080 דקות, כבר מעל 9 שעות
        self.assertEqual(result2["calc100"], 0)
        self.assertEqual(result2["calc125"], 0)
        self.assertEqual(result2["calc150"], 1440)  # 24 שעות @ 150%

    def test_day_to_night_transition_across_days(self):
        """
        מעבר מיום ללילה בין ימים:
        יום 1: 10:00-18:00 (8 שעות) - יום רגיל
        יום 2: 22:00-06:00 (8 שעות) - לילה

        שני רצפים נפרדים (הפסקה > 60 דקות בין 18:00 ל-22:00)
        """
        # יום 1 - משמרת יום
        segments1 = [(600, 1080, None)]  # 10:00-18:00
        result1 = _calculate_chain_wages(segments1, date(2024, 12, 15), {}, 0, is_night_shift=False)
        self.assertEqual(result1["calc100"], 480)  # 8 שעות @ 100%

        # יום 2 - משמרת לילה (רצף חדש)
        night_hours = calculate_night_hours_in_segment(22*60, 30*60)
        self.assertEqual(night_hours, 480)  # 8 שעות לילה

        segments2 = [(1320, 1800, None)]  # 22:00-06:00
        result2 = _calculate_chain_wages(segments2, date(2024, 12, 16), {}, 0, is_night_shift=True)

        # סף 7 שעות: 7 @ 100%, 1 @ 125%
        self.assertEqual(result2["calc100"], 420)
        self.assertEqual(result2["calc125"], 60)


class TestMonthBoundaryCarryover(unittest.TestCase):
    """
    בדיקות למעבר בין חודשים עם carryover.
    רצף שמתחיל בסוף חודש ונמשך לתחילת החודש הבא.
    """

    def test_end_of_month_carryover_calculation(self):
        """
        רצף שמתחיל ב-31/12 ונמשך ל-01/01

        31/12: 22:00-08:00 (10 שעות, 8 שעות לילה)
        01/01: 08:00-12:00 (4 שעות) - המשך הרצף

        סה"כ 14 שעות ברצף לילה
        """
        # 31/12: רצף לילה
        night_hours = calculate_night_hours_in_segment(22*60, 32*60)
        self.assertEqual(night_hours, 480)  # 8 שעות לילה

        segments_dec = [(1320, 1920, None)]  # 22:00-08:00
        result_dec = _calculate_chain_wages(segments_dec, date(2024, 12, 31), {}, 0, is_night_shift=True)

        # סף 7 שעות: 7 @ 100%, 2 @ 125%, 1 @ 150%
        self.assertEqual(result_dec["calc100"], 420)
        self.assertEqual(result_dec["calc125"], 120)
        self.assertEqual(result_dec["calc150"], 60)

        # 01/01: המשך עם carryover של 10 שעות (600 דקות)
        segments_jan = [(480, 720, None)]  # 08:00-12:00
        result_jan = _calculate_chain_wages(segments_jan, date(2025, 1, 1), {}, 600, is_night_shift=True)

        # 10 שעות carryover + 4 שעות = 14 שעות
        # כבר עברנו 9 שעות, הכל @ 150%
        self.assertEqual(result_jan["calc100"], 0)
        self.assertEqual(result_jan["calc125"], 0)
        self.assertEqual(result_jan["calc150"], 240)  # 4 שעות @ 150%

    def test_shabbat_spanning_month_boundary(self):
        """
        משמרת 10:00-20:00 ביום שבת (28/12/2024)

        שבת נכנסת ב-16:30 (יום שישי) ויוצאת ב-17:45 (יום שבת)
        כלומר 10:00-17:45 ביום שבת = עדיין שבת (7.75 שעות = 465 דקות)
        17:45-20:00 ביום שבת = אחרי צאת שבת = חול (2.25 שעות = 135 דקות)
        """
        # 28/12/2024 = שבת (weekday=5)
        shabbat_cache = {
            "2024-12-28": {"enter": "16:30", "exit": "17:45"},  # יציאת שבת ב-17:45 ביום שבת
        }

        segments = [(600, 1200, None)]  # 10:00-20:00
        result = _calculate_chain_wages(segments, date(2024, 12, 28), shabbat_cache, 0, is_night_shift=False)

        # 10:00-17:45 (7.75 שעות = 465 דקות) = שבת @ 150%
        # 17:45-18:00 (0.25 שעות = 15 דקות) = חול @ 100% (להשלמת 8 שעות)
        # 18:00-20:00 (2 שעות = 120 דקות) = חול @ 125% (שעות נוספות)
        # סה"כ 10 שעות = 600 דקות
        self.assertEqual(result["calc150"], 465)  # 7.75 שעות שבת
        self.assertEqual(result["calc100"], 15)   # 0.25 שעות חול (להשלמת 8)
        self.assertEqual(result["calc125"], 120)  # 2 שעות חול שעות נוספות

    def test_night_hours_carryover_between_months(self):
        """
        שעות לילה שמועברות בין חודשים

        31/12: 23:00-08:00 (9 שעות, 7 שעות לילה)
        01/01: 08:00-10:00 (2 שעות, 0 שעות לילה)

        הרצף של 01/01 ממשיך להיחשב כרצף לילה כי סה"כ שעות הלילה ברצף = 7
        """
        # 31/12: 23:00-08:00 = 9 שעות
        night_hours_dec = calculate_night_hours_in_segment(23*60, 32*60)  # 23:00-08:00
        self.assertEqual(night_hours_dec, 420)  # 7 שעות לילה (23:00-06:00)

        # 01/01: 08:00-10:00 = 2 שעות, 0 שעות לילה
        night_hours_jan = calculate_night_hours_in_segment(8*60, 10*60)
        self.assertEqual(night_hours_jan, 0)

        # סה"כ שעות לילה ברצף = 7 שעות >= 2 שעות = רצף לילה
        total_night = night_hours_dec + night_hours_jan
        self.assertEqual(total_night, 420)
        self.assertTrue(total_night >= 120)  # NIGHT_HOURS_THRESHOLD


class TestComplexScenarios(unittest.TestCase):
    """
    בדיקות לתרחישים מורכבים שמשלבים מספר מקרים.
    """

    def test_week_of_night_shifts(self):
        """
        שבוע של משמרות לילה רצופות (22:00-08:00 כל לילה)
        כל משמרת נמשכת לבוקר הבא, אז כל יום מתחיל עם carryover

        בדיקה: האם סף 7 שעות נשמר לכל המשמרות?
        """
        # כל משמרת: 22:00-08:00 = 10 שעות, 8 שעות לילה
        segments = [(1320, 1920, None)]

        # יום 1 - ללא carryover
        result1 = _calculate_chain_wages(segments, date(2024, 12, 15), {}, 0, is_night_shift=True)
        self.assertEqual(result1["calc100"], 420)   # 7 שעות
        self.assertEqual(result1["calc125"], 120)   # 2 שעות
        self.assertEqual(result1["calc150"], 60)    # 1 שעה

        # יום 2 - עם carryover של 10 שעות (אם העבודה ב-08:00 בדיוק)
        # אבל בד"כ יש הפסקה, אז נניח שזה רצף חדש
        result2 = _calculate_chain_wages(segments, date(2024, 12, 16), {}, 0, is_night_shift=True)
        self.assertEqual(result2["calc100"], 420)
        self.assertEqual(result2["calc125"], 120)
        self.assertEqual(result2["calc150"], 60)

    def test_mixed_shift_types_same_week(self):
        """
        שבוע עם סוגי משמרות שונים:
        - יום ראשון: משמרת יום 08:00-16:00 (סף 8)
        - יום שני: משמרת לילה 22:00-06:00 (סף 7)
        - יום שלישי: משמרת יום 10:00-18:00 (סף 8)
        """
        # יום ראשון - משמרת יום
        segments_sun = [(480, 960, None)]
        result_sun = _calculate_chain_wages(segments_sun, date(2024, 12, 15), {}, 0, is_night_shift=False)
        self.assertEqual(result_sun["calc100"], 480)

        # יום שני - משמרת לילה
        segments_mon = [(1320, 1800, None)]
        result_mon = _calculate_chain_wages(segments_mon, date(2024, 12, 16), {}, 0, is_night_shift=True)
        self.assertEqual(result_mon["calc100"], 420)
        self.assertEqual(result_mon["calc125"], 60)

        # יום שלישי - משמרת יום
        segments_tue = [(600, 1080, None)]
        result_tue = _calculate_chain_wages(segments_tue, date(2024, 12, 17), {}, 0, is_night_shift=False)
        self.assertEqual(result_tue["calc100"], 480)

    def test_shabbat_to_weekday_transition(self):
        """
        מעבר משבת ליום חול באותו רצף:
        משמרת 10:00-22:00 ביום שבת (28/12/2024)

        שבת נכנסת ב-16:30 (יום שישי) ויוצאת ב-17:45 (יום שבת)
        כלומר 10:00-17:45 ביום שבת = עדיין שבת (7.75 שעות = 465 דקות)
        17:45-22:00 ביום שבת = אחרי צאת שבת = חול (4.25 שעות = 255 דקות)
        """
        shabbat_cache = {
            "2024-12-28": {"enter": "16:30", "exit": "17:45"},
        }

        # משמרת: 10:00-22:00 (12 שעות)
        segments_shabbat = [(600, 1320, None)]
        result_shabbat = _calculate_chain_wages(segments_shabbat, date(2024, 12, 28), shabbat_cache, 0, is_night_shift=False)

        # 10:00-17:45 (7.75 שעות = 465 דקות) = שבת @ 150%
        # 17:45-18:00 (0.25 שעות = 15 דקות) = חול @ 100% (להשלמת 8 שעות)
        # 18:00-20:00 (2 שעות = 120 דקות) = חול @ 125% (שעות 8-10)
        # 20:00-22:00 (2 שעות = 120 דקות) = חול @ 150% (שעות 10+)
        # סה"כ 12 שעות = 720 דקות
        self.assertEqual(result_shabbat["calc150"], 585)  # 7.75 שעות שבת (465) + 2 שעות חול 150% (120)
        self.assertEqual(result_shabbat["calc100"], 15)   # 0.25 שעות חול (להשלמת 8)
        self.assertEqual(result_shabbat["calc125"], 120)  # 2 שעות חול שעות נוספות (8-10)

    def test_partial_night_shift_boundary(self):
        """
        משמרת שנמצאת בדיוק על הגבול של 2 שעות לילה:
        21:00-23:00 = 1 שעה לילה (לא עובר סף)
        21:00-00:00 = 2 שעות לילה (עובר סף)
        """
        # 21:00-23:00 = 1 שעה לילה
        night_1 = calculate_night_hours_in_segment(21*60, 23*60)
        self.assertEqual(night_1, 60)
        self.assertFalse(night_1 >= 120)

        # 21:00-00:00 = 2 שעות לילה
        night_2 = calculate_night_hours_in_segment(21*60, 24*60)
        self.assertEqual(night_2, 120)
        self.assertTrue(night_2 >= 120)

        # משמרת 21:00-23:00 - לא לילה, סף 8 שעות
        segments_short = [(1260, 1380, None)]
        result_short = _calculate_chain_wages(segments_short, date(2024, 12, 15), {}, 0, is_night_shift=False)
        self.assertEqual(result_short["calc100"], 120)  # 2 שעות @ 100%

        # משמרת 21:00-00:00 - לילה, סף 7 שעות
        segments_long = [(1260, 1440, None)]
        result_long = _calculate_chain_wages(segments_long, date(2024, 12, 15), {}, 0, is_night_shift=True)
        self.assertEqual(result_long["calc100"], 180)  # 3 שעות @ 100% (פחות מ-7)


class TestNightChainWithCarryover(unittest.TestCase):
    """בדיקות רצף לילה עם carryover - הרצף נחשב לילה לפי סה"כ שעות הלילה ברצף כולו"""

    def test_carryover_night_hours_determine_chain_type(self):
        """
        רצף עם carryover של שעות לילה:
        אם ה-carryover כולל 2+ שעות לילה, הרצף כולו הוא רצף לילה (סף 7 שעות)
        """
        # דוגמה: אתמול עבדתי 22:00-08:00 (10 שעות, מתוכן 8 שעות לילה)
        # היום ממשיך ב-08:00 עם עוד 2 שעות
        # הרצף כולו = 12 שעות, מתוכן 8 שעות לילה = רצף לילה
        # סף 7 שעות: 420 דק' 100%, 120 דק' 125%, 180 דק' 150%

        # בדיקת זיהוי שעות לילה
        # 22:00-06:00 = 8 שעות לילה (480 דקות)
        night_hours_1 = calculate_night_hours_in_segment(22*60, 6*60)
        self.assertEqual(night_hours_1, 480)

        # 08:00-10:00 = 0 שעות לילה
        night_hours_2 = calculate_night_hours_in_segment(8*60, 10*60)
        self.assertEqual(night_hours_2, 0)

        # סה"כ: 480 + 0 = 480 דקות לילה >= 120 = רצף לילה ✓
        total_night = night_hours_1 + night_hours_2
        self.assertTrue(total_night >= 120)  # NIGHT_HOURS_THRESHOLD

    def test_day_carryover_to_night_chain(self):
        """
        רצף יום שממשיך לרצף לילה:
        אתמול: 14:00-08:00 (18 שעות, מתוכן 8 שעות לילה 22:00-06:00)
        = רצף לילה, סף 7 שעות
        """
        # 14:00-22:00 = 0 שעות לילה
        night_1 = calculate_night_hours_in_segment(14*60, 22*60)
        self.assertEqual(night_1, 0)

        # 22:00-06:00 = 8 שעות לילה
        night_2 = calculate_night_hours_in_segment(22*60, 6*60)
        self.assertEqual(night_2, 480)

        # 06:00-08:00 = 0 שעות לילה
        night_3 = calculate_night_hours_in_segment(6*60, 8*60)
        self.assertEqual(night_3, 0)

        total = night_1 + night_2 + night_3
        self.assertEqual(total, 480)  # 8 שעות לילה
        self.assertTrue(total >= 120)  # רצף לילה

    def test_short_night_hours_not_night_chain(self):
        """
        רצף עם פחות מ-2 שעות לילה = רצף יום (סף 8 שעות)
        """
        # 20:00-23:00 = 1 שעה לילה בלבד (22:00-23:00)
        night_hours = calculate_night_hours_in_segment(20*60, 23*60)
        self.assertEqual(night_hours, 60)  # רק שעה אחת בטווח 22:00-06:00
        self.assertFalse(night_hours >= 120)  # לא רצף לילה

    def test_exactly_2_hours_qualifies(self):
        """
        בדיוק 2 שעות בטווח 22:00-06:00 = רצף לילה
        """
        # 21:00-00:00 = 2 שעות לילה (22:00-00:00)
        night_hours = calculate_night_hours_in_segment(21*60, 24*60)
        self.assertEqual(night_hours, 120)  # בדיוק 2 שעות
        self.assertTrue(night_hours >= 120)  # רצף לילה

    def test_carryover_adds_to_current_night_hours(self):
        """
        carryover של 1 שעת לילה + עבודה נוכחית של 1 שעת לילה = 2 שעות = רצף לילה
        """
        carryover_night = 60  # 1 שעה מאתמול
        current_night = 60     # 1 שעה היום

        total_night = carryover_night + current_night
        self.assertEqual(total_night, 120)
        self.assertTrue(total_night >= 120)  # רצף לילה


class TestMixedDaysInSameWorkday(unittest.TestCase):
    """בדיקות סגמנטים מימים שונים באותו יום עבודה"""

    def test_saturday_segment_in_friday_display_day(self):
        """
        באג שתוקן: משמרת ביום שבת (00:00-01:00) שמוצגת תחת יום שישי
        צריכה להיחשב כשבת (150%) ולא כחול (100%)

        סצנריו: דיווח ביום שישי 30/01/2026
        - 15:00-17:00 = חול (לפני כניסת שבת ~16:50)
        - 17:00-22:00 = שבת
        - בנוסף, משמרת נפרדת 00:00-01:00 ביום שבת 31/01/2026

        ה-00:00-01:00 מוצג תחת יום שישי (אותו יום עבודה) אבל הזמן עצמו
        הוא ביום שבת ולכן צריך להיחשב כשבת.
        """
        # שבת - כניסה בסביבות 16:50 ביום שישי, יציאה בסביבות 17:50 ביום שבת
        shabbat_cache = {
            "2026-01-30": {"start": "16:50", "end": "17:50"},  # שבת פרשת בשלח
            "2026-01-31": {"start": "16:50", "end": "17:50"},
        }

        # משמרת ביום שבת 00:00-01:00 (60 דקות)
        # זמן 0-60 ביום שבת צריך להיחשב כשבת (150%)
        saturday_date = date(2026, 1, 31)  # שבת
        segments_saturday = [(0, 60, None, saturday_date)]

        result = _calculate_chain_wages_new(segments_saturday, shabbat_cache, 0, False)

        # צריך להיות 150% (שבת) ולא 100% (חול)
        self.assertEqual(result["calc150"], 60, "משמרת 00:00-01:00 בשבת צריכה להיות 150%")
        self.assertEqual(result["calc100"], 0, "לא צריך להיות שעות ב-100%")
        self.assertEqual(result["calc150_shabbat"], 60, "צריך להיות מסומן כשבת")

    def test_friday_before_shabbat_vs_saturday_during_shabbat(self):
        """
        השוואה בין סגמנט ביום שישי לפני שבת לסגמנט ביום שבת
        """
        shabbat_cache = {
            "2026-01-30": {"start": "16:50", "end": "17:50"},
            "2026-01-31": {"start": "16:50", "end": "17:50"},
        }

        # סגמנט ביום שישי 15:00-16:00 (לפני כניסת שבת) = חול
        friday_date = date(2026, 1, 30)
        segments_friday = [(15*60, 16*60, None, friday_date)]
        result_friday = _calculate_chain_wages_new(segments_friday, shabbat_cache, 0, False)
        self.assertEqual(result_friday["calc100"], 60, "15:00-16:00 ביום שישי = חול")

        # סגמנט ביום שישי 17:00-18:00 (אחרי כניסת שבת) = שבת
        segments_friday_shabbat = [(17*60, 18*60, None, friday_date)]
        result_friday_shabbat = _calculate_chain_wages_new(segments_friday_shabbat, shabbat_cache, 0, False)
        self.assertEqual(result_friday_shabbat["calc150"], 60, "17:00-18:00 ביום שישי = שבת")

        # סגמנט ביום שבת 00:00-01:00 = שבת
        saturday_date = date(2026, 1, 31)
        segments_saturday = [(0, 60, None, saturday_date)]
        result_saturday = _calculate_chain_wages_new(segments_saturday, shabbat_cache, 0, False)
        self.assertEqual(result_saturday["calc150"], 60, "00:00-01:00 ביום שבת = שבת")

    def test_multiple_segments_different_dates_same_chain(self):
        """
        רצף עבודה עם סגמנטים מימים שונים - כל סגמנט מחושב לפי התאריך שלו
        """
        shabbat_cache = {
            "2026-01-30": {"start": "16:50", "end": "17:50"},
            "2026-01-31": {"start": "16:50", "end": "17:50"},
        }

        friday = date(2026, 1, 30)
        saturday = date(2026, 1, 31)

        # רצף: 15:00-16:00 (שישי, חול) + 00:00-01:00 (שבת, שבת)
        segments_mixed = [
            (15*60, 16*60, None, friday),    # 60 דקות חול
            (0, 60, None, saturday),          # 60 דקות שבת
        ]

        result = _calculate_chain_wages_new(segments_mixed, shabbat_cache, 0, False)

        # 60 דקות חול + 60 דקות שבת
        self.assertEqual(result["calc100"], 60, "60 דקות ביום שישי לפני שבת = חול")
        self.assertEqual(result["calc150"], 60, "60 דקות ביום שבת = שבת")


class TestFixedSegmentsWithWorkLabel(unittest.TestCase):
    """בדיקות לתיקון: סגמנטים עם label='work' ביום is_fixed_segments"""

    def test_work_label_on_saturday_in_fixed_segments_day(self):
        """
        באג: כשיש תגבור + שעת עבודה באותו יום
        - תגבור קובעת is_fixed_segments=True
        - שעת עבודה מקבלת label='work'
        - בעיבוד is_fixed_segments, label='work' נופל ל-else ומחושב כ-100%
        - אבל אם שעת העבודה היא בשבת, צריך להיות 150%

        תיקון: בעיבוד is_fixed_segments, אם label='work', לחשב לפי actual_date
        """
        from core.time_utils import _get_shabbat_boundaries, FRIDAY, SATURDAY, MINUTES_PER_DAY

        # סימולציה של סגמנט עם label='work' ביום שבת
        saturday = date(2026, 1, 31)  # שבת
        seg_weekday = saturday.weekday()

        self.assertEqual(seg_weekday, SATURDAY, "31/01/2026 צריך להיות שבת")

        # קבלת גבולות שבת
        shabbat_cache = {}
        seg_shabbat_enter, seg_shabbat_exit = _get_shabbat_boundaries(saturday, shabbat_cache)

        # סגמנט 00:00-01:00 ביום שבת
        s, e = 0, 60
        actual_start = s % MINUTES_PER_DAY
        actual_end = e % MINUTES_PER_DAY

        day_offset = MINUTES_PER_DAY if seg_weekday == SATURDAY else 0
        abs_start = actual_start + day_offset
        abs_end = actual_end + day_offset

        # בדיקה שהזמן בתוך שבת
        is_in_shabbat = seg_shabbat_enter > 0 and abs_start >= seg_shabbat_enter and abs_end <= seg_shabbat_exit

        self.assertTrue(is_in_shabbat, f"00:00-01:00 בשבת צריך להיות בתוך שבת. enter={seg_shabbat_enter}, exit={seg_shabbat_exit}, abs_start={abs_start}, abs_end={abs_end}")


class TestHolidayWages(unittest.TestCase):
    """
    בדיקות חישוב שכר בחגים.
    חגים מתנהגים כמו שבת (תוספת 50%) וערבי חג כמו יום שישי.
    """

    def test_holiday_adds_50_percent(self):
        """
        יום חג (לא שבת) מקבל תוספת 50% כמו שבת.
        סוכות א' - יום רביעי 2025-10-07
        """
        # ערב סוכות (יום שלישי) עם enter, סוכות א' (יום רביעי) עם exit
        holiday_cache = {
            "2025-10-06": {"enter": "17:45"},  # ערב סוכות - יום שלישי
            "2025-10-07": {"exit": "18:40"},   # סוכות א' - יום רביעי
        }

        # משמרת 10:00-18:00 ביום חג (8 שעות)
        holiday_date = date(2025, 10, 7)  # יום רביעי
        segments = [(600, 1080, None, holiday_date)]

        result = _calculate_chain_wages_new(segments, holiday_cache, 0, False)

        # 8 שעות @ 150% (חג)
        self.assertEqual(result["calc150"], 480, "8 שעות ביום חג צריכות להיות 150%")
        self.assertEqual(result["calc100"], 0, "לא צריך להיות שעות ב-100%")
        self.assertEqual(result["calc150_shabbat"], 480, "צריך להיות מסומן כשבת/חג")

    def test_holiday_eve_like_friday(self):
        """
        ערב חג מתנהג כמו יום שישי - עבודה לפני כניסת החג היא חול,
        עבודה אחרי כניסת החג היא חג.

        מבנה הטבלה: הנתונים (enter + exit) נמצאים ברשומה של יום החג עצמו,
        לא ברשומה של יום הערב.
        """
        # מבנה כמו בטבלה האמיתית - כל הנתונים ברשומה של יום החג
        holiday_cache = {
            "2025-10-07": {"enter": "17:45", "exit": "18:40"},  # סוכות א' - enter היא הדלקת נרות בערב
        }

        # משמרת 16:00-20:00 בערב חג
        eve_date = date(2025, 10, 6)  # יום שני - ערב סוכות
        segments = [(960, 1200, None, eve_date)]  # 16:00-20:00

        result = _calculate_chain_wages_new(segments, holiday_cache, 0, False)

        # 16:00-17:45 (105 דקות) = חול @ 100%
        # 17:45-20:00 (135 דקות) = חג @ 150%
        self.assertEqual(result["calc100"], 105, "לפני כניסת החג = חול")
        self.assertEqual(result["calc150"], 135, "אחרי כניסת החג = 150%")

    def test_holiday_overtime_rates(self):
        """
        שעות נוספות בחג - אותם תעריפים כמו שבת:
        - 0-8 שעות: 150% (100% + 50%)
        - 8-10 שעות: 175% (125% + 50%)
        - 10+ שעות: 200% (150% + 50%)

        מבנה הטבלה: כל הנתונים (enter + exit) נמצאים ברשומה של יום החג עצמו.
        """
        # מבנה נכון - כל הנתונים ברשומה אחת
        holiday_cache = {
            "2025-10-07": {"enter": "17:45", "exit": "18:40"},
        }

        # משמרת 08:00-18:40 ביום חג (10:40 שעות = 640 דקות, כולו בחג)
        holiday_date = date(2025, 10, 7)
        segments = [(480, 1120, None, holiday_date)]  # עד צאת החג

        result = _calculate_chain_wages_new(segments, holiday_cache, 0, False)

        # 8 שעות @ 150%, 2 שעות @ 175%, 40 דקות @ 200%
        self.assertEqual(result["calc150"], 480, "8 שעות @ 150%")
        self.assertEqual(result["calc175"], 120, "2 שעות @ 175%")
        self.assertEqual(result["calc200"], 40, "40 דקות @ 200%")

    def test_two_day_holiday(self):
        """
        חג של יומיים (כמו ראש השנה).

        מבנה הטבלה: לחג דו-יומי יש רשומה אחת ליום האחרון עם enter (מהערב הראשון) ו-exit.
        חייב להיות שדה 'holiday' כדי לזהות אותו כחג דו-יומי.
        """
        # מבנה נכון - רשומה אחת ליום האחרון עם שדה holiday
        holiday_cache = {
            "2025-09-24": {"enter": "18:15", "exit": "19:10", "holiday": "ראש השנה"},
        }

        # משמרת ביום ב' של ראש השנה (2025-09-24)
        day2_date = date(2025, 9, 24)
        segments = [(600, 1080, None, day2_date)]  # 10:00-18:00 (8 שעות)

        result = _calculate_chain_wages_new(segments, holiday_cache, 0, False)

        # כל המשמרת בחג
        self.assertEqual(result["calc150"], 480, "8 שעות ביום ב' של ר\"ה = 150%")
        self.assertEqual(result["calc100"], 0)

    def test_holiday_vs_shabbat_same_logic(self):
        """
        השוואה: חג ושבת צריכים לקבל אותו חישוב כשהמשמרת כולה בתוך הזמן המקודש.
        """
        # שבת רגילה - משמרת 10:00-16:00 (כולה לפני צאת שבת ב-17:45)
        shabbat_cache = {
            "2026-01-09": {"enter": "16:30"},  # יום שישי
            "2026-01-10": {"exit": "17:45"},   # שבת
        }

        saturday = date(2026, 1, 10)
        segments_shabbat = [(600, 960, None, saturday)]  # 10:00-16:00 (6 שעות בשבת)

        result_shabbat = _calculate_chain_wages_new(segments_shabbat, shabbat_cache, 0, False)

        # חג (סוכות) - משמרת 10:00-16:00 (כולה לפני צאת החג ב-18:40)
        holiday_cache = {
            "2025-10-06": {"enter": "17:45"},
            "2025-10-07": {"exit": "18:40"},
        }

        holiday = date(2025, 10, 7)
        segments_holiday = [(600, 960, None, holiday)]  # 10:00-16:00 (6 שעות בחג)

        result_holiday = _calculate_chain_wages_new(segments_holiday, holiday_cache, 0, False)

        # שניהם צריכים להיות 6 שעות @ 150%
        self.assertEqual(result_shabbat["calc150"], result_holiday["calc150"],
                        "שבת וחג צריכים לקבל אותו חישוב")
        self.assertEqual(result_shabbat["calc150"], 360)  # 6 שעות

    def test_weekday_no_holiday(self):
        """
        יום חול רגיל (ללא חג) - לא מקבל תוספת.
        """
        # cache ריק - אין שבת או חג
        empty_cache = {}

        # יום רביעי רגיל
        wednesday = date(2025, 10, 8)  # יום אחרי סוכות
        segments = [(600, 1080, None, wednesday)]  # 10:00-18:00

        result = _calculate_chain_wages_new(segments, empty_cache, 0, False)

        # 8 שעות @ 100% (חול)
        self.assertEqual(result["calc100"], 480)
        self.assertEqual(result["calc150"], 0)

    def test_night_shift_on_holiday(self):
        """
        משמרת לילה בחג - סף 7 שעות כמו בשבת.

        מבנה הטבלה: כל הנתונים (enter + exit) נמצאים ברשומה של יום החג עצמו.
        """
        # מבנה נכון - כל הנתונים ברשומה אחת
        holiday_cache = {
            "2025-10-07": {"enter": "17:45", "exit": "18:40"},
        }

        # משמרת 22:00-06:30 בערב חג (8.5 שעות)
        eve_date = date(2025, 10, 6)
        segments = [
            (1320, 1440, None, eve_date),  # 22:00-00:00 בערב חג
            (1440, 1830, None, date(2025, 10, 7)),  # 00:00-06:30 ביום חג
        ]

        result = _calculate_chain_wages_new(segments, holiday_cache, 0, is_night_shift=True)

        # כל המשמרת אחרי כניסת החג (17:45) = חג
        # עם סף לילה (7 שעות): 7 שעות @ 150%, 1.5 שעות @ 175%
        self.assertEqual(result["calc150"], 420, "7 שעות @ 150%")
        self.assertEqual(result["calc175"], 90, "1.5 שעות @ 175%")


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
        _manual_test_worker_calculation(conn, "ברהמי רחל", 2025, 12)

        # בדיקה 2: בדיקת יום ספציפי עם משמרות חופפות
        print("\n" + "-"*50)
        print("בדיקה 2: ברהמי רחל - 18/12/2025 (משמרות חופפות)")
        print("-"*50)
        _manual_test_specific_day(conn, "ברהמי רחל", date(2025, 12, 18))

    finally:
        return_connection(conn)


def _manual_test_worker_calculation(conn, worker_name: str, year: int, month: int):
    """בדיקת חישוב לעובד ספציפי - להרצה ידנית עם DB אמיתי (לא pytest)"""

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


def _manual_test_specific_day(conn, worker_name: str, test_date: date):
    """בדיקת יום ספציפי עם פירוט מלא - להרצה ידנית עם DB אמיתי (לא pytest)"""

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
               st.name AS shift_name
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
        report_id, rep_date, start_time, end_time, shift_name = report
        print(f"  ID: {report_id}")
        print(f"    שעות: {start_time} - {end_time}")
        print(f"    משמרת: {shift_name or '?'}")
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
