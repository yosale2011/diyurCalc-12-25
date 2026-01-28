"""
Unit tests for logic module - testing critical calculation functions.
"""

import unittest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app_utils import calculate_wage_rate, get_effective_hourly_rate
from core.sick_days import get_sick_payment_rate
from core.time_utils import (
    minutes_to_time_str,
    span_minutes,
    REGULAR_HOURS_LIMIT,
    OVERTIME_125_LIMIT,
    parse_hhmm,
)
from utils.utils import calculate_annual_vacation_quota, overlap_minutes

# from logic_enhanced import (
#     calculate_wage_rate_enhanced,
#     validate_time_string,
#     validate_date_range,
#     format_hours_minutes,
#     parse_time_to_minutes,
#     calculate_overlap_percentage,
#     ValidationError
# )


class TestWageCalculations(unittest.TestCase):
    """Test wage rate calculations."""

    def test_regular_hours_rate(self):
        """Test wage rate for regular hours (first 8 hours)."""
        # Regular hours, not Shabbat
        self.assertEqual(calculate_wage_rate(0, False), "100%")
        self.assertEqual(calculate_wage_rate(240, False), "100%")  # 4 hours
        self.assertEqual(calculate_wage_rate(480, False), "100%")  # 8 hours

        # Regular hours during Shabbat
        self.assertEqual(calculate_wage_rate(0, True), "150%")
        self.assertEqual(calculate_wage_rate(240, True), "150%")
        self.assertEqual(calculate_wage_rate(480, True), "150%")

    def test_overtime_125_rate(self):
        """Test wage rate for hours 9-10 (125% overtime)."""
        # Hours 9-10, not Shabbat
        self.assertEqual(calculate_wage_rate(481, False), "125%")  # Just over 8 hours
        self.assertEqual(calculate_wage_rate(540, False), "125%")  # 9 hours
        self.assertEqual(calculate_wage_rate(600, False), "125%")  # 10 hours

        # Hours 9-10 during Shabbat
        self.assertEqual(calculate_wage_rate(481, True), "175%")
        self.assertEqual(calculate_wage_rate(540, True), "175%")
        self.assertEqual(calculate_wage_rate(600, True), "175%")

    def test_overtime_150_rate(self):
        """Test wage rate for hours 11+ (150% overtime)."""
        # Hours 11+, not Shabbat
        self.assertEqual(calculate_wage_rate(601, False), "150%")  # Just over 10 hours
        self.assertEqual(calculate_wage_rate(720, False), "150%")  # 12 hours
        self.assertEqual(calculate_wage_rate(900, False), "150%")  # 15 hours

        # Hours 11+ during Shabbat
        self.assertEqual(calculate_wage_rate(601, True), "200%")
        self.assertEqual(calculate_wage_rate(720, True), "200%")
        self.assertEqual(calculate_wage_rate(900, True), "200%")

    # def test_enhanced_wage_rate(self):
    #     """Test enhanced wage rate function with type safety."""
    #     rate_type, percentage = calculate_wage_rate_enhanced(240, False)
    #     self.assertEqual(percentage, 1.0)
    #
    #     rate_type, percentage = calculate_wage_rate_enhanced(540, False)
    #     self.assertEqual(percentage, 1.25)
    #
    #     rate_type, percentage = calculate_wage_rate_enhanced(700, True)
    #     self.assertEqual(percentage, 2.0)
    #
    #     # Test negative minutes validation
    #     with self.assertRaises(ValidationError):
    #         calculate_wage_rate_enhanced(-10, False)


class TestVacationCalculations(unittest.TestCase):
    """Test vacation quota calculations."""

    def test_vacation_quota_5_day_week(self):
        """Test vacation quota for 5-day work week."""
        # First 5 years
        for year in range(1, 6):
            self.assertEqual(calculate_annual_vacation_quota(year, False), 12)

        # Year 6
        self.assertEqual(calculate_annual_vacation_quota(6, False), 14)

        # Year 10
        self.assertEqual(calculate_annual_vacation_quota(10, False), 18)

        # Year 12+
        self.assertEqual(calculate_annual_vacation_quota(12, False), 20)
        self.assertEqual(calculate_annual_vacation_quota(15, False), 20)

    def test_vacation_quota_6_day_week(self):
        """Test vacation quota for 6-day work week."""
        # First 4 years
        for year in range(1, 5):
            self.assertEqual(calculate_annual_vacation_quota(year, True), 14)

        # Year 5
        self.assertEqual(calculate_annual_vacation_quota(5, True), 16)

        # Year 7
        self.assertEqual(calculate_annual_vacation_quota(7, True), 21)

        # Year 10+
        self.assertEqual(calculate_annual_vacation_quota(10, True), 24)
        self.assertEqual(calculate_annual_vacation_quota(15, True), 24)


class TestTimeUtilities(unittest.TestCase):
    """Test time utility functions."""

    def test_minutes_to_time_str(self):
        """Test conversion from minutes to HH:MM string."""
        self.assertEqual(minutes_to_time_str(0), "00:00")
        self.assertEqual(minutes_to_time_str(60), "01:00")
        self.assertEqual(minutes_to_time_str(90), "01:30")
        self.assertEqual(minutes_to_time_str(480), "08:00")
        self.assertEqual(minutes_to_time_str(1439), "23:59")

    def test_parse_hhmm(self):
        """Test parsing HH:MM to (hours, minutes) tuple."""
        # parse_hhmm returns (hours, minutes) tuple, not total minutes
        self.assertEqual(parse_hhmm("00:00"), (0, 0))
        self.assertEqual(parse_hhmm("01:00"), (1, 0))
        self.assertEqual(parse_hhmm("08:30"), (8, 30))
        self.assertEqual(parse_hhmm("23:59"), (23, 59))

    def test_span_minutes(self):
        """Test calculating span between times - returns (start_min, end_min) tuple."""
        # span_minutes returns (start_minutes, end_minutes) tuple, not duration
        # Same day: 08:00 = 480, 16:00 = 960
        start, end = span_minutes("08:00", "16:00")
        self.assertEqual(start, 480)
        self.assertEqual(end, 960)
        self.assertEqual(end - start, 480)  # Duration is 8 hours

        # 09:30 = 570, 17:45 = 1065
        start, end = span_minutes("09:30", "17:45")
        self.assertEqual(start, 570)
        self.assertEqual(end, 1065)
        self.assertEqual(end - start, 495)  # Duration is 8:15

        # Cross midnight: 22:00 = 1320, 06:00 = 360 -> becomes 1800 (360 + 1440)
        start, end = span_minutes("22:00", "06:00")
        self.assertEqual(start, 1320)
        self.assertEqual(end, 1800)  # 360 + 1440 for overnight
        self.assertEqual(end - start, 480)  # Duration is 8 hours

        # 23:00 = 1380, 01:00 = 60 -> becomes 1500 (60 + 1440)
        start, end = span_minutes("23:00", "01:00")
        self.assertEqual(start, 1380)
        self.assertEqual(end, 1500)  # 60 + 1440 for overnight
        self.assertEqual(end - start, 120)  # Duration is 2 hours

    # def test_format_hours_minutes(self):
    #     """Test formatting minutes to HH:MM."""
    #     self.assertEqual(format_hours_minutes(0), "00:00")
    #     self.assertEqual(format_hours_minutes(90), "01:30")
    #     self.assertEqual(format_hours_minutes(615), "10:15")

    # def test_parse_time_to_minutes(self):
    #     """Test parsing time string to minutes with validation."""
    #     self.assertEqual(parse_time_to_minutes("08:30"), 510)
    #     self.assertEqual(parse_time_to_minutes("00:00"), 0)
    #     self.assertEqual(parse_time_to_minutes("23:59"), 1439)

        # Test invalid format
        # with self.assertRaises(ValidationError):
        #     parse_time_to_minutes("25:00")

        # with self.assertRaises(ValidationError):
        #     parse_time_to_minutes("12:70")


class TestOverlapCalculations(unittest.TestCase):
    """Test time overlap calculations."""

    def test_overlap_minutes(self):
        """Test calculating overlap between time ranges."""
        # Full overlap
        self.assertEqual(overlap_minutes(480, 600, 480, 600), 120)

        # Partial overlap
        self.assertEqual(overlap_minutes(480, 600, 540, 660), 60)

        # No overlap
        self.assertEqual(overlap_minutes(480, 540, 600, 660), 0)

        # One range contains the other
        self.assertEqual(overlap_minutes(480, 720, 540, 600), 60)


class TestSickPaymentRate(unittest.TestCase):
    """Test sick day payment rate calculations."""

    def test_first_day_zero_percent(self):
        """Test that first sick day pays 0%."""
        self.assertEqual(get_sick_payment_rate(1), 0.0)

    def test_days_2_3_fifty_percent(self):
        """Test that days 2-3 pay 50%."""
        self.assertEqual(get_sick_payment_rate(2), 0.5)
        self.assertEqual(get_sick_payment_rate(3), 0.5)

    def test_day_4_plus_full_payment(self):
        """Test that day 4 and beyond pay 100%."""
        self.assertEqual(get_sick_payment_rate(4), 1.0)
        self.assertEqual(get_sick_payment_rate(5), 1.0)
        self.assertEqual(get_sick_payment_rate(10), 1.0)
        self.assertEqual(get_sick_payment_rate(30), 1.0)


class TestEffectiveHourlyRate(unittest.TestCase):
    """Test effective hourly rate calculation with housing rates."""

    def test_custom_rate_used(self):
        """Test that custom rate from housing_rates_cache is used."""
        report = {'shift_type_id': 101, 'housing_array_id': 1, 'is_married': False}
        minimum_wage = 32.30
        # תעריף קבוע 50 ש"ח (5000 אגורות)
        housing_rates_cache = {
            (101, 1): {'weekday_single_rate': 5000, 'weekday_single_wage_percentage': None,
                       'weekday_married_rate': None, 'weekday_married_wage_percentage': None,
                       'shabbat_rate': None, 'shabbat_wage_percentage': None}
        }
        self.assertEqual(get_effective_hourly_rate(report, minimum_wage, False, housing_rates_cache), 50.0)

    def test_minimum_wage_when_no_rate(self):
        """Test that minimum wage is used when no housing rate defined."""
        report = {}
        minimum_wage = 32.30
        self.assertEqual(get_effective_hourly_rate(report, minimum_wage), 32.30)

    def test_minimum_wage_when_rate_none(self):
        """Test that minimum wage is used when housing_rates_cache is None."""
        report = {'shift_type_id': 101, 'housing_array_id': 1}
        minimum_wage = 32.30
        self.assertEqual(get_effective_hourly_rate(report, minimum_wage, False, None), 32.30)

    def test_minimum_wage_when_rate_zero(self):
        """Test that minimum wage is used when all rate fields are empty."""
        report = {'shift_type_id': 101, 'housing_array_id': 1, 'is_married': False}
        minimum_wage = 32.30
        housing_rates_cache = {
            (101, 1): {'weekday_single_rate': None, 'weekday_single_wage_percentage': None,
                       'weekday_married_rate': None, 'weekday_married_wage_percentage': None,
                       'shabbat_rate': None, 'shabbat_wage_percentage': None}
        }
        self.assertEqual(get_effective_hourly_rate(report, minimum_wage, False, housing_rates_cache), 32.30)

    def test_rate_conversion_from_agorot(self):
        """Test that rate is correctly converted from agorot to shekels."""
        # 3230 agorot = 32.30 shekels
        report = {'shift_type_id': 101, 'housing_array_id': 1, 'is_married': True}
        minimum_wage = 30.0
        housing_rates_cache = {
            (101, 1): {'weekday_single_rate': None, 'weekday_single_wage_percentage': None,
                       'weekday_married_rate': 3230, 'weekday_married_wage_percentage': None,
                       'shabbat_rate': None, 'shabbat_wage_percentage': None}
        }
        self.assertEqual(get_effective_hourly_rate(report, minimum_wage, False, housing_rates_cache), 32.30)


    # def test_overlap_percentage(self):
    #     """Test calculating overlap percentage."""
    #     # Full overlap
    #     self.assertAlmostEqual(calculate_overlap_percentage(480, 600, 480, 600), 1.0)
    #
    #     # 50% overlap
    #     self.assertAlmostEqual(calculate_overlap_percentage(480, 600, 540, 660), 0.5)
    #
    #     # No overlap
    #     self.assertAlmostEqual(calculate_overlap_percentage(480, 540, 600, 660), 0.0)
    #
    #     # Invalid ranges
    #     self.assertAlmostEqual(calculate_overlap_percentage(600, 480, 540, 660), 0.0)


# class TestValidation(unittest.TestCase):
#     """Test validation functions."""

#     def test_validate_time_string(self):
#         """Test time string validation."""
#         # Valid times
#         self.assertTrue(validate_time_string("00:00"))
#         self.assertTrue(validate_time_string("12:30"))
#         self.assertTrue(validate_time_string("23:59"))

#         # Invalid times
#         self.assertFalse(validate_time_string("24:00"))
#         self.assertFalse(validate_time_string("12:60"))
#         self.assertFalse(validate_time_string("invalid"))
#         self.assertFalse(validate_time_string("12"))

#     def test_validate_date_range(self):
#         """Test date range validation."""
#         today = datetime.now()
#         yesterday = today - timedelta(days=1)
#         tomorrow = today + timedelta(days=1)
#         next_year = today + timedelta(days=365)
#         far_future = today + timedelta(days=500)
#         far_past = today - timedelta(days=4000)

#         # Valid ranges
#         self.assertTrue(validate_date_range(yesterday, today))
#         self.assertTrue(validate_date_range(today, tomorrow))
#         self.assertTrue(validate_date_range(today, next_year))

#         # Invalid ranges
#         self.assertFalse(validate_date_range(tomorrow, yesterday))  # End before start
#         self.assertFalse(validate_date_range(today, far_future))     # Too far future
#         self.assertFalse(validate_date_range(far_past, today))       # Too far past


def run_tests():
    """Run all tests and return results."""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestWageCalculations))
    suite.addTests(loader.loadTestsFromTestCase(TestVacationCalculations))
    suite.addTests(loader.loadTestsFromTestCase(TestTimeUtilities))
    suite.addTests(loader.loadTestsFromTestCase(TestOverlapCalculations))
    suite.addTests(loader.loadTestsFromTestCase(TestSickPaymentRate))
    suite.addTests(loader.loadTestsFromTestCase(TestEffectiveHourlyRate))
    # suite.addTests(loader.loadTestsFromTestCase(TestValidation))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result


if __name__ == "__main__":
    result = run_tests()
    if result.wasSuccessful():
        print("\n✅ All tests passed successfully!")
    else:
        print(f"\n❌ {len(result.failures)} tests failed, {len(result.errors)} errors")
        sys.exit(1)