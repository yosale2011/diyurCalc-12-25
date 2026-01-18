"""
Time utilities and Shabbat logic for DiyurCalc application.
Contains time conversion functions, Shabbat time detection, and related constants.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, date
from typing import Tuple, Dict, Any

import psycopg2.extras

from core.config import config
from utils.cache_manager import cache

logger = logging.getLogger(__name__)

# =============================================================================
# Constants
# =============================================================================

# Time constants (in minutes)
MINUTES_PER_HOUR = 60
MINUTES_PER_DAY = 24 * MINUTES_PER_HOUR  # 1440

# Work hour thresholds (in minutes)
REGULAR_HOURS_LIMIT = 8 * MINUTES_PER_HOUR   # 480 - First 8 hours at 100%
OVERTIME_125_LIMIT = 10 * MINUTES_PER_HOUR   # 600 - Hours 9-10 at 125%
# Beyond 600 minutes = 150%

# Work day boundaries
WORK_DAY_START_MINUTES = 8 * MINUTES_PER_HOUR  # 480 = 08:00

# Shabbat defaults (when not found in DB)
SHABBAT_ENTER_DEFAULT = 16 * MINUTES_PER_HOUR  # 960 = 16:00 on Friday
SHABBAT_EXIT_DEFAULT = 22 * MINUTES_PER_HOUR   # 1320 = 22:00 on Saturday

# Weekday indices (Python's weekday())
FRIDAY = 4
SATURDAY = 5

# Use LOCAL_TZ from config
LOCAL_TZ = config.LOCAL_TZ


# =============================================================================
# Date/Time Conversion Functions
# =============================================================================

def to_local_date(ts: int | datetime | date) -> date:
    """Convert epoch timestamp, datetime, or date object to local date."""
    from zoneinfo import ZoneInfo

    if isinstance(ts, date) and not isinstance(ts, datetime):
        # Already a date object (PostgreSQL can return date directly)
        return ts
    if isinstance(ts, datetime):
        # PostgreSQL returns datetime objects directly
        if ts.tzinfo is None:
            # Assume UTC if no timezone
            return ts.replace(tzinfo=ZoneInfo("UTC")).astimezone(LOCAL_TZ).date()
        return ts.astimezone(LOCAL_TZ).date()
    # SQLite returns epoch timestamps
    return datetime.fromtimestamp(ts, LOCAL_TZ).date()


def parse_hhmm(value: str) -> Tuple[int, int]:
    """Return (hours, minutes) integers from 'HH:MM'."""
    h, m = value.split(":")
    return int(h), int(m)


def span_minutes(start_str: str, end_str: str) -> Tuple[int, int]:
    """Return start/end minutes-from-midnight, handling overnight end < start."""
    sh, sm = parse_hhmm(start_str)
    eh, em = parse_hhmm(end_str)
    start = sh * MINUTES_PER_HOUR + sm
    end = eh * MINUTES_PER_HOUR + em
    if end <= start:
        end += MINUTES_PER_DAY
    return start, end


def minutes_to_time_str(minutes: int) -> str:
    """Convert minutes from midnight to HH:MM format (handles >24h wrapping)."""
    day_minutes = minutes % MINUTES_PER_DAY
    h = day_minutes // MINUTES_PER_HOUR
    m = day_minutes % MINUTES_PER_HOUR
    return f"{h:02d}:{m:02d}"


# =============================================================================
# Shabbat Cache and Detection
# =============================================================================

SHABBAT_CACHE_KEY = "shabbat_times_cache"
SHABBAT_CACHE_TTL = 86400  # 24 hours


def get_shabbat_times_cache(conn) -> Dict[str, Dict[str, Any]]:
    """
    Load Shabbat times from DB into a dictionary with 24-hour caching.
    Key: Date string (YYYY-MM-DD) representing the day.
    Value: {'enter': HH:MM, 'exit': HH:MM, 'parsha': str, 'holiday': str}
    """
    # Check cache first
    cached_result = cache.get(SHABBAT_CACHE_KEY)
    if cached_result is not None:
        return cached_result

    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("SELECT shabbat_date, candle_lighting, havdalah, parsha, holiday_name FROM shabbat_times")
        rows = cursor.fetchall()
        result = {}
        for r in rows:
            if r["shabbat_date"]:
                result[r["shabbat_date"]] = {
                    "enter": r["candle_lighting"],
                    "exit": r["havdalah"],
                    "parsha": r["parsha"],
                    "holiday": r["holiday_name"]
                }
        cursor.close()

        # Store in cache
        cache.set(SHABBAT_CACHE_KEY, result, SHABBAT_CACHE_TTL)
        return result
    except Exception as e:
        logger.warning(f"Failed to load shabbat times cache: {e}")
        return {}


def _get_shabbat_boundaries(day_date: date, shabbat_cache: Dict[str, Dict[str, str]]) -> Tuple[int, int]:
    """
    Get Shabbat enter/exit times in minutes from Friday midnight.
    Returns (enter_minute, exit_minute) where exit is relative to Friday midnight (can be >1440).
    """
    weekday = day_date.weekday()

    # Find the relevant Saturday
    if weekday == FRIDAY:
        target_saturday = day_date + timedelta(days=1)
    elif weekday == SATURDAY:
        target_saturday = day_date
    else:
        # Not Friday or Saturday - no Shabbat
        return (-1, -1)

    saturday_str = target_saturday.strftime("%Y-%m-%d")
    shabbat_data = shabbat_cache.get(saturday_str)

    if shabbat_data:
        try:
            eh, em = map(int, shabbat_data["enter"].split(":"))
            enter_minutes = eh * MINUTES_PER_HOUR + em

            xh, xm = map(int, shabbat_data["exit"].split(":"))
            exit_minutes = xh * MINUTES_PER_HOUR + xm + MINUTES_PER_DAY  # Add 1440 for Saturday

            return (enter_minutes, exit_minutes)
        except (ValueError, KeyError, AttributeError):
            pass

    # Default times
    return (SHABBAT_ENTER_DEFAULT, SHABBAT_EXIT_DEFAULT + MINUTES_PER_DAY)
