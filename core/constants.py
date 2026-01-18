"""
Central constants for DiyurCalc application.
All shift IDs, apartment types, and other shared constants should be defined here.

This module serves as the single source of truth for constants used across:
- core/segments.py
- core/wage_calculator.py
- core/logic.py
- app_utils.py
- routes/*.py
"""
from typing import Set

# =============================================================================
# Shift Type IDs
# =============================================================================

# Regular shifts
FRIDAY_SHIFT_ID = 105           # משמרת שישי/ערב חג
SHABBAT_SHIFT_ID = 106          # משמרת שבת/חג
NIGHT_SHIFT_ID = 107            # משמרת לילה

# Tagbur (backup) shifts
TAGBUR_FRIDAY_SHIFT_ID = 108    # משמרת תגבור שישי/ערב חג
TAGBUR_SHABBAT_SHIFT_ID = 109   # משמרת תגבור שבת/חג

# Special shifts
HOSPITAL_ESCORT_SHIFT_ID = 120  # משמרת לווי בי"ח
MEDICAL_ESCORT_SHIFT_ID = 148   # משמרת ליווי רפואי

# =============================================================================
# Shift ID Groups
# =============================================================================

# משמרות שישי/שבת רגילות (לא תגבור)
SHABBAT_SHIFT_IDS: Set[int] = {FRIDAY_SHIFT_ID, SHABBAT_SHIFT_ID}

# משמרות תגבור
TAGBUR_SHIFT_IDS: Set[int] = {TAGBUR_FRIDAY_SHIFT_ID, TAGBUR_SHABBAT_SHIFT_ID}

# כל משמרות השבת (כולל תגבור)
ALL_SHABBAT_SHIFT_IDS: Set[int] = SHABBAT_SHIFT_IDS | TAGBUR_SHIFT_IDS

# =============================================================================
# Apartment Types
# =============================================================================

REGULAR_APT_TYPE = 1        # דירה רגילה
THERAPEUTIC_APT_TYPE = 2    # דירה טיפולית

# =============================================================================
# Night Shift Constants (in minutes)
# =============================================================================

NIGHT_SHIFT_WORK_FIRST_MINUTES = 120    # 2 שעות ראשונות = עבודה
NIGHT_SHIFT_STANDBY_END = 390           # 06:30 = סוף כוננות
NIGHT_SHIFT_MORNING_END = 480           # 08:00 = סוף עבודת בוקר
NOON_MINUTES = 720                      # 12:00 = חצות היום

# =============================================================================
# Night Shift Overtime Thresholds (in minutes)
# A shift qualifies as "night shift" if 2+ hours are between 22:00-06:00
# Night shifts use 7-hour workday instead of 8-hour
# =============================================================================

NIGHT_REGULAR_HOURS_LIMIT = 420         # 7 hours = 100% (for night shifts)
NIGHT_OVERTIME_125_LIMIT = 540          # 9 hours = 125% (for night shifts)

# Night hours definition (22:00-06:00)
NIGHT_HOURS_START = 22 * 60             # 1320 = 22:00
NIGHT_HOURS_END = 6 * 60                # 360 = 06:00
NIGHT_HOURS_THRESHOLD = 120             # 2 hours required to qualify as night shift

# =============================================================================
# Standby Constants
# =============================================================================

# Threshold for cancelling standby due to work overlap
# If work overlaps >= 70% of standby duration, standby is cancelled
STANDBY_CANCEL_OVERLAP_THRESHOLD = 0.70

# Default standby rate (in shekels)
DEFAULT_STANDBY_RATE = 70.0

# Maximum deduction from cancelled standby
# If standby rate > 70, pay the difference (rate - 70)
MAX_CANCELLED_STANDBY_DEDUCTION = 70.0

# =============================================================================
# Break/Chain Constants
# =============================================================================

# Breaks longer than this split work chains (in minutes)
BREAK_THRESHOLD_MINUTES = 60

# =============================================================================
# Medical Escort Constants
# =============================================================================

# Minimum billable time for medical escort (in minutes)
MINIMUM_ESCORT_MINUTES = 60

# =============================================================================
# Helper Functions for Shift Type Identification
# =============================================================================

def is_tagbur_shift(shift_id: int | None) -> bool:
    """Check if shift is a tagbur (backup) shift by ID."""
    return shift_id in TAGBUR_SHIFT_IDS


def is_night_shift(shift_id: int | None) -> bool:
    """Check if shift is a night shift by ID."""
    return shift_id == NIGHT_SHIFT_ID


def is_shabbat_shift(shift_id: int | None) -> bool:
    """Check if shift is a Friday/Shabbat shift (not tagbur) by ID."""
    return shift_id in SHABBAT_SHIFT_IDS


def is_hospital_escort_shift(shift_id: int | None) -> bool:
    """Check if shift is a hospital escort shift by ID."""
    return shift_id == HOSPITAL_ESCORT_SHIFT_ID


def is_medical_escort_shift(shift_id: int | None) -> bool:
    """Check if shift is a medical escort shift by ID."""
    return shift_id == MEDICAL_ESCORT_SHIFT_ID


def is_implicit_tagbur(
    shift_id: int | None,
    actual_apt_type: int | None,
    rate_apt_type: int | None
) -> bool:
    """
    Check if shift is an implicit tagbur (backup) shift.

    Condition: Friday (105) or Shabbat (106) shift in therapeutic apartment (2)
    with regular apartment rate (1).

    Args:
        shift_id: Shift type ID
        actual_apt_type: Actual apartment type (from apartments table)
        rate_apt_type: Apartment type for rate calculation (rate_apartment_type_id or historical)

    Returns:
        True if this is an implicit tagbur shift
    """
    is_friday_or_shabbat = is_shabbat_shift(shift_id)
    is_therapeutic_apt = (actual_apt_type == THERAPEUTIC_APT_TYPE)
    is_regular_rate = (rate_apt_type == REGULAR_APT_TYPE)
    return is_friday_or_shabbat and is_therapeutic_apt and is_regular_rate


# =============================================================================
# Night Hours Detection Functions
# =============================================================================

def calculate_night_hours_in_segment(start_min: int, end_min: int) -> int:
    """
    Calculate how many minutes of a segment fall within night hours (22:00-06:00).

    Args:
        start_min: Start time in minutes from midnight
        end_min: End time in minutes from midnight (can be >1440 for overnight)

    Returns:
        Minutes of work within 22:00-06:00 range
    """
    total_night_minutes = 0

    # Normalize overnight shifts
    if end_min <= start_min:
        end_min += 1440

    # Range 1: 22:00-24:00 (1320-1440)
    overlap_start = max(start_min, NIGHT_HOURS_START)
    overlap_end = min(end_min, 1440)
    if overlap_end > overlap_start:
        total_night_minutes += overlap_end - overlap_start

    # Range 2: 00:00-06:00 (check both original and shifted for overnight)
    for offset in [0, 1440]:
        overlap_start = max(start_min, offset)
        overlap_end = min(end_min, NIGHT_HOURS_END + offset)
        if overlap_end > overlap_start:
            total_night_minutes += overlap_end - overlap_start

    return total_night_minutes


def qualifies_as_night_shift(work_segments: list) -> bool:
    """
    Check if work segments qualify as night shift (2+ hours in 22:00-06:00).

    Args:
        work_segments: List of (start_min, end_min) tuples

    Returns:
        True if total night hours >= 120 minutes (2 hours)
    """
    total_night = sum(
        calculate_night_hours_in_segment(start, end)
        for start, end in work_segments
    )
    return total_night >= NIGHT_HOURS_THRESHOLD
