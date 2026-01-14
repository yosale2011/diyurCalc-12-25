"""
Utility functions for DiyurCalc application.
Contains helper functions for calculations, formatting, and general utilities.
"""
from __future__ import annotations

from datetime import datetime, date
from typing import Dict, Optional

from core.config import config

# Import LOCAL_TZ from config for accruals calculation
LOCAL_TZ = config.LOCAL_TZ


def calculate_annual_vacation_quota(work_year: int, is_6_day_week: bool) -> int:
    """
    Calculate annual vacation quota based on Israeli law.

    Args:
        work_year: The employee's current work year (1st year, 2nd year, etc.)
        is_6_day_week: True if employee works 6-day weeks, False for 5-day weeks

    Returns:
        Annual vacation days quota
    """
    if is_6_day_week:
        # Table for 6-day work week
        if work_year <= 4:
            return 14
        elif work_year == 5:
            return 16
        elif work_year == 6:
            return 18
        elif work_year == 7:
            return 21
        elif work_year == 8:
            return 22
        elif work_year == 9:
            return 23
        else:  # 10+
            return 24
    else:
        # Table for 5-day work week
        if work_year <= 5:
            return 12
        elif work_year == 6:
            return 14
        elif work_year == 7:
            return 15
        elif work_year == 8:
            return 16
        elif work_year == 9:
            return 17
        elif work_year == 10:
            return 18
        elif work_year == 11:
            return 19
        else:  # 12+
            return 20


def calculate_accruals(
    actual_work_days: int,
    start_date_ts,  # Can be epoch timestamp (int), datetime, or date
    report_year: int,
    report_month: int
) -> Dict:
    """
    Calculate sick leave and vacation accruals for a month.

    Args:
        actual_work_days: Number of actual work days in the month
        start_date_ts: Employee start date (can be epoch timestamp, datetime, or None)
        report_year: The year being reported
        report_month: The month being reported

    Returns:
        Dict with sick_days_accrued, vacation_days_accrued, and vacation_details
    """
    # Calculate job scope (proportion of full-time)
    job_scope = min(actual_work_days / config.STANDARD_WORK_DAYS_PER_MONTH, 1.0)

    # Sick leave accrual (1.5 days per month at full-time)
    sick_days_accrued = job_scope * config.MAX_SICK_DAYS_PER_MONTH

    # Calculate seniority for vacation
    current_work_year = 1
    if start_date_ts:
        try:
            # Handle different input types for start_date
            if isinstance(start_date_ts, datetime):
                start_dt = start_date_ts.date()
            elif isinstance(start_date_ts, date):
                start_dt = start_date_ts
            elif isinstance(start_date_ts, (int, float)):
                start_dt = datetime.fromtimestamp(start_date_ts, LOCAL_TZ).date()
            else:
                start_dt = None

            if start_dt:
                report_dt = datetime(report_year, report_month, 1, tzinfo=LOCAL_TZ).date()
                diff = report_dt - start_dt
                seniority_years = diff.days / 365.25
                current_work_year = max(1, int(seniority_years) + 1)
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.debug(f"Error calculating seniority: {e}")

    # Determine if 6-day or 5-day week (based on > 20 work days)
    is_6_day_week = actual_work_days > 20

    # Get annual vacation quota
    annual_quota = calculate_annual_vacation_quota(current_work_year, is_6_day_week)

    # Monthly vacation accrual
    vacation_days_accrued = (annual_quota / 12) * job_scope

    return {
        "sick_days_accrued": sick_days_accrued,
        "vacation_days_accrued": vacation_days_accrued,
        "vacation_details": {
            "seniority": current_work_year,
            "annual_quota": annual_quota,
            "job_scope_pct": int(job_scope * 100)
        }
    }


def overlap_minutes(a_start: int, a_end: int, b_start: int, b_end: int) -> int:
    """Calculate overlapping minutes between two time ranges."""
    return max(0, min(a_end, b_end) - max(a_start, b_start))


def minutes_to_hours_str(minutes: int) -> str:
    """Convert minutes to hours string with 2 decimal places."""
    hours = minutes / 60
    return f"{hours:.2f}".rstrip("0").rstrip(".")


def to_gematria(num: int) -> str:
    """Simple gematria converter for numbers 1-31 and years."""
    if num <= 0:
        return str(num)

    # מיפוי פשוט לימים (1-31)
    gematria_map = {
        1: "א'", 2: "ב'", 3: "ג'", 4: "ד'", 5: "ה'", 6: "ו'", 7: "ז'", 8: "ח'", 9: "ט'",
        10: "י'", 11: "י\"א", 12: "י\"ב", 13: "י\"ג", 14: "י\"ד", 15: "ט\"ו", 16: "ט\"ז",
        17: "י\"ז", 18: "י\"ח", 19: "י\"ט", 20: "כ'", 21: "כ\"א", 22: "כ\"ב", 23: "כ\"ג",
        24: "כ\"ד", 25: "כ\"ה", 26: "כ\"ו", 27: "כ\"ז", 28: "כ\"ח", 29: "כ\"ט", 30: "ל'"
    }
    if num in gematria_map:
        return gematria_map[num]

    # עבור שנים (למשל 5786 -> תשפ"ו)
    # זה מימוש פשוט מאוד שיכסה את השנים הקרובות
    if num == 5785: return "תשפ\"ה"
    if num == 5786: return "תשפ\"ו"
    if num == 5787: return "תשפ\"ז"

    return str(num)


def format_currency(value: float | int | None) -> str:
    """Format number as currency with thousand separators (e.g., 11403.00 -> 11,403.00)."""
    if value is None:
        value = 0
    return f"{float(value):,.2f}"


def human_date(ts: int | datetime | date | None) -> str:
    """Format epoch seconds, datetime, or date to dd/mm/yyyy in local timezone."""
    if ts is None:
        return "-"
    try:
        if isinstance(ts, date) and not isinstance(ts, datetime):
            # PostgreSQL can return date objects directly
            return ts.strftime("%d/%m/%Y")
        if isinstance(ts, datetime):
            # PostgreSQL returns datetime objects
            dt = ts if ts.tzinfo else ts.replace(tzinfo=config.LOCAL_TZ)
        else:
            # SQLite returns epoch timestamps
            dt = datetime.fromtimestamp(ts, config.LOCAL_TZ)
        return dt.strftime("%d/%m/%Y")
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"Failed to format date: {e}")
        return "-"


def month_range_ts(year: int, month: int) -> tuple[datetime, datetime]:
    """Return datetime range [start, end) for the given month in local TZ."""
    start_dt = datetime(year, month, 1, tzinfo=config.LOCAL_TZ)
    if month == 12:
        end_dt = datetime(year + 1, 1, 1, tzinfo=config.LOCAL_TZ)
    else:
        end_dt = datetime(year, month + 1, 1, tzinfo=config.LOCAL_TZ)
    # Return datetime objects directly (PostgreSQL prefers this)
    return start_dt, end_dt
