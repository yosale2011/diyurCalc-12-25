"""
History module for DiyurCalc.
Handles historical data lookups for person status, apartment types, and standby rates.
Uses "save on change" approach - current data is used unless there's a historical record.
"""
from __future__ import annotations

import logging
from typing import Optional, Any, List, Dict
from datetime import datetime

import psycopg2.extras

logger = logging.getLogger(__name__)


def get_person_status_for_month(conn, person_id: int, year: int, month: int) -> dict:
    """
    Get person status (married, employer, type) for a specific month.
    First checks history table using "valid until" logic, falls back to current data.
    
    History records store (year, month) as "valid until" - meaning the old value
    was valid up to but NOT including that month.

    Returns:
        dict with keys: is_married, employer_id, employee_type
    """
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        # Find a historical record where the requested month is covered
        # Logic: requested (year, month) < historical (year, month)
        # Order by year/month ASC to get the first (earliest) matching record
        cursor.execute("""
            SELECT is_married, employer_id, employee_type
            FROM person_status_history
            WHERE person_id = %s
              AND (year > %s OR (year = %s AND month > %s))
            ORDER BY year ASC, month ASC
            LIMIT 1
        """, (person_id, year, year, month))

        history = cursor.fetchone()

        if history:
            logger.debug(f"Using historical data for person {person_id} ({year}/{month})")
            return {
                "is_married": history["is_married"],
                "employer_id": history["employer_id"],
                "employee_type": history["employee_type"]
            }

        # No history covers this month - use current data from people table
        cursor.execute("""
            SELECT is_married, employer_id, type as employee_type
            FROM people
            WHERE id = %s
        """, (person_id,))

        person = cursor.fetchone()

        if person:
            return {
                "is_married": person["is_married"],
                "employer_id": person["employer_id"],
                "employee_type": person["employee_type"]
            }

        # Person not found
        return {
            "is_married": None,
            "employer_id": None,
            "employee_type": None
        }
    finally:
        cursor.close()


def get_apartment_type_for_month(conn, apartment_id: int, year: int, month: int) -> Optional[int]:
    """
    Get apartment type ID for a specific month.
    First checks history table using "valid until" logic, falls back to current data.
    
    History records store (year, month) as "valid until" - meaning the old value
    was valid up to but NOT including that month.

    Returns:
        apartment_type_id or None
    """
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        # Find a historical record where the requested month is covered
        # Logic: requested (year, month) < historical (year, month)
        # Order by year/month ASC to get the first (earliest) matching record
        cursor.execute("""
            SELECT apartment_type_id
            FROM apartment_status_history
            WHERE apartment_id = %s
              AND (year > %s OR (year = %s AND month > %s))
            ORDER BY year ASC, month ASC
            LIMIT 1
        """, (apartment_id, year, year, month))

        history = cursor.fetchone()

        if history:
            logger.debug(f"Using historical data for apartment {apartment_id} ({year}/{month})")
            return history["apartment_type_id"]

        # No history covers this month - use current data from apartments table
        cursor.execute("""
            SELECT apartment_type_id
            FROM apartments
            WHERE id = %s
        """, (apartment_id,))

        apartment = cursor.fetchone()

        if apartment:
            return apartment["apartment_type_id"]

        return None
    finally:
        cursor.close()


def get_standby_rate_for_month(
    conn,
    segment_id: int,
    apartment_type_id: int,
    marital_status: str,
    year: int,
    month: int
) -> Optional[int]:
   
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        # 1. Try historical record for specific apartment type
        # Logic: requested (year, month) < historical (year, month)
        if apartment_type_id is not None:
            cursor.execute("""
                SELECT amount
                FROM standby_rates_history
                WHERE segment_id = %s
                  AND apartment_type_id = %s
                  AND marital_status = %s
                  AND (year > %s OR (year = %s AND month > %s))
                ORDER BY year ASC, month ASC
                LIMIT 1
            """, (segment_id, apartment_type_id, marital_status, year, year, month))

            history = cursor.fetchone()
            if history:
                logger.debug(f"Using historical standby rate for segment {segment_id}, apt_type {apartment_type_id} ({year}/{month})")
                return history["amount"]

        # 2. Try historical record for general (apt_type=NULL)
        cursor.execute("""
            SELECT amount
            FROM standby_rates_history
            WHERE segment_id = %s
              AND apartment_type_id IS NULL
              AND marital_status = %s
              AND (year > %s OR (year = %s AND month > %s))
            ORDER BY year ASC, month ASC
            LIMIT 1
        """, (segment_id, marital_status, year, year, month))

        history = cursor.fetchone()
        if history:
            logger.debug(f"Using historical standby rate (general) for segment {segment_id} ({year}/{month})")
            return history["amount"]

        # 3. No history - try current data for specific apartment type
        if apartment_type_id is not None:
            cursor.execute("""
                SELECT amount
                FROM standby_rates
                WHERE segment_id = %s
                AND apartment_type_id = %s
                AND marital_status = %s
            """, (segment_id, apartment_type_id, marital_status))

            rate = cursor.fetchone()
            if rate:
                return rate["amount"]

        # 4. Fallback to current general rate (apt_type=NULL)
        cursor.execute("""
            SELECT amount
            FROM standby_rates
            WHERE segment_id = %s
            AND apartment_type_id IS NULL
            AND marital_status = %s
        """, (segment_id, marital_status))

        rate = cursor.fetchone()
        if rate:
            return rate["amount"]

        return None
    finally:
        cursor.close()


def is_month_locked(conn, year: int, month: int) -> bool:
    """
    Check if a month is locked for changes.

    Returns:
        True if locked, False otherwise
    """
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cursor.execute("""
            SELECT id, unlocked_at
            FROM month_locks
            WHERE year = %s AND month = %s
        """, (year, month))

        lock = cursor.fetchone()

        if lock is None:
            return False

        # If unlocked_at is set, the month was unlocked
        if lock["unlocked_at"] is not None:
            return False

        return True
    finally:
        cursor.close()


def get_month_lock_info(conn, year: int, month: int) -> Optional[dict]:
    """
    Get detailed lock information for a month.

    Returns:
        dict with lock info or None if not locked
    """
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cursor.execute("""
            SELECT ml.*, p.name as locked_by_name
            FROM month_locks ml
            LEFT JOIN people p ON ml.locked_by = p.id
            WHERE ml.year = %s AND ml.month = %s
        """, (year, month))

        lock = cursor.fetchone()

        if lock is None:
            return None

        return dict(lock)
    finally:
        cursor.close()


def lock_month(conn, year: int, month: int, locked_by: int, notes: str = None) -> bool:
    """
    Lock a month to prevent changes.

    Returns:
        True if locked successfully, False if already locked
    """
    if is_month_locked(conn, year, month):
        return False

    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO month_locks (year, month, locked_by, notes, locked_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (year, month)
            DO UPDATE SET
                locked_at = NOW(),
                locked_by = EXCLUDED.locked_by,
                notes = EXCLUDED.notes,
                unlocked_at = NULL,
                unlocked_by = NULL
        """, (year, month, locked_by, notes))

        conn.commit()
        logger.info(f"Month {year}/{month} locked by user {locked_by}")
        return True
    finally:
        cursor.close()


def unlock_month(conn, year: int, month: int, unlocked_by: int) -> bool:
    """
    Unlock a month to allow changes.

    Returns:
        True if unlocked successfully, False if not locked
    """
    if not is_month_locked(conn, year, month):
        return False

    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE month_locks
            SET unlocked_at = NOW(), unlocked_by = %s
            WHERE year = %s AND month = %s
        """, (unlocked_by, year, month))

        conn.commit()
        logger.info(f"Month {year}/{month} unlocked by user {unlocked_by}")
        return True
    finally:
        cursor.close()


def get_historical_months(conn, person_id: int = None) -> list:
    """
    Get list of months that have historical data.
    Useful for debugging or viewing history.

    Returns:
        List of (year, month) tuples
    """
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        if person_id:
            cursor.execute("""
                SELECT DISTINCT year, month
                FROM person_status_history
                WHERE person_id = %s
                ORDER BY year DESC, month DESC
            """, (person_id,))
        else:
            cursor.execute("""
                SELECT DISTINCT year, month
                FROM person_status_history
                ORDER BY year DESC, month DESC
            """)

        return [(row["year"], row["month"]) for row in cursor.fetchall()]
    finally:
        cursor.close()


# ============================================================================
# Shift Types History Functions
# ============================================================================

def get_shift_rate_for_month(
    conn,
    shift_type_id: int,
    year: int,
    month: int
) -> Optional[dict]:
    """
    Get shift rate for a specific month.
    First checks history table using "valid until" logic, falls back to current data.
    
    History records store (year, month) as "valid until" - meaning the old value
    was valid up to but NOT including that month.

    Returns:
        dict with keys: rate, is_minimum_wage, wage_percentage, or None if not found
    """
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        # Find a historical record where the requested month is covered
        cursor.execute("""
            SELECT rate, is_minimum_wage, wage_percentage
            FROM shift_types_history
            WHERE shift_type_id = %s
              AND (year > %s OR (year = %s AND month > %s))
            ORDER BY year ASC, month ASC
            LIMIT 1
        """, (shift_type_id, year, year, month))

        history = cursor.fetchone()

        if history:
            logger.debug(f"Using historical rate for shift_type {shift_type_id} ({year}/{month})")
            return {
                "rate": history["rate"],
                "is_minimum_wage": history["is_minimum_wage"],
                "wage_percentage": history["wage_percentage"]
            }

        # No history covers this month - use current data from shift_types table
        cursor.execute("""
            SELECT rate, is_minimum_wage, wage_percentage
            FROM shift_types
            WHERE id = %s
        """, (shift_type_id,))

        shift_type = cursor.fetchone()

        if shift_type:
            return {
                "rate": shift_type["rate"],
                "is_minimum_wage": shift_type["is_minimum_wage"],
                "wage_percentage": shift_type["wage_percentage"]
            }

        return None
    finally:
        cursor.close()


def save_shift_rate_to_history(
    conn,
    shift_type_id: int,
    year: int,
    month: int,
    rate: int,
    is_minimum_wage: bool,
    created_by: int = None,
    wage_percentage: int = 100
) -> bool:
    """
    Save shift rate to history before a change.
    Called before updating shift_types.rate.

    Returns:
        True if saved successfully
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO shift_types_history
            (shift_type_id, year, month, rate, is_minimum_wage, wage_percentage, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (shift_type_id, year, month)
            DO UPDATE SET
                rate = EXCLUDED.rate,
                is_minimum_wage = EXCLUDED.is_minimum_wage,
                wage_percentage = EXCLUDED.wage_percentage,
                created_by = EXCLUDED.created_by,
                created_at = NOW()
        """, (shift_type_id, year, month, rate, is_minimum_wage, wage_percentage, created_by))

        conn.commit()
        logger.info(f"Saved shift_type {shift_type_id} rate history for {year}/{month}")
        return True
    except Exception as e:
        logger.error(f"Error saving shift rate history: {e}")
        return False
    finally:
        cursor.close()


def get_all_shift_rates_for_month(conn, year: int, month: int) -> dict:
    """
    Get all shift rates for a specific month as a cache dictionary.
    First checks history table using "valid until" logic, falls back to current data.

    History records store (year, month) as "valid until" - meaning the old value
    was valid up to but NOT including that month.

    Returns:
        dict mapping shift_type_id to {rate, is_minimum_wage, wage_percentage}
    """
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    result = {}
    try:
        # Get all shift types first
        cursor.execute("SELECT id FROM shift_types")
        all_shift_ids = [row["id"] for row in cursor.fetchall()]

        # For each shift type, find the appropriate rate
        for shift_type_id in all_shift_ids:
            # Check for historical rate using "valid until" logic
            cursor.execute("""
                SELECT rate, is_minimum_wage, wage_percentage
                FROM shift_types_history
                WHERE shift_type_id = %s
                  AND (year > %s OR (year = %s AND month > %s))
                ORDER BY year ASC, month ASC
                LIMIT 1
            """, (shift_type_id, year, year, month))

            history = cursor.fetchone()

            if history:
                result[shift_type_id] = {
                    "rate": history["rate"],
                    "is_minimum_wage": history["is_minimum_wage"],
                    "wage_percentage": history["wage_percentage"]
                }
            else:
                # No history - use current rate
                cursor.execute("""
                    SELECT rate, is_minimum_wage, wage_percentage
                    FROM shift_types
                    WHERE id = %s
                """, (shift_type_id,))
                current = cursor.fetchone()
                if current:
                    result[shift_type_id] = {
                        "rate": current["rate"],
                        "is_minimum_wage": current["is_minimum_wage"],
                        "wage_percentage": current["wage_percentage"]
                    }

        return result
    finally:
        cursor.close()


# ============================================================================
# Shift Time Segments History Functions
# ============================================================================

def get_segments_for_shift_month(
    conn,
    shift_type_id: int,
    year: int,
    month: int
) -> list:
    """
    Get shift time segments for a specific month.
    First checks history table using "valid until" logic, falls back to current data.
    
    History records store (year, month) as "valid until" - meaning the old value
    was valid up to but NOT including that month.

    Returns:
        list of dicts with keys: id, shift_type_id, start_time, end_time,
                                 wage_percent, segment_type, order_index
    """
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        # Find historical records where the requested month is covered
        # We need to find the earliest "valid until" that covers our month
        cursor.execute("""
            SELECT DISTINCT year, month
            FROM shift_time_segments_history
            WHERE shift_type_id = %s
              AND (year > %s OR (year = %s AND month > %s))
            ORDER BY year ASC, month ASC
            LIMIT 1
        """, (shift_type_id, year, year, month))
        
        valid_until = cursor.fetchone()
        
        if valid_until:
            # Get segments for this valid_until period
            cursor.execute("""
                SELECT segment_id as id, shift_type_id, start_time, end_time,
                       wage_percent, segment_type, order_index
                FROM shift_time_segments_history
                WHERE shift_type_id = %s AND year = %s AND month = %s
                ORDER BY order_index
            """, (shift_type_id, valid_until["year"], valid_until["month"]))
            
            history = cursor.fetchall()
            if history:
                logger.debug(f"Using historical segments for shift_type {shift_type_id} ({year}/{month})")
                return [dict(row) for row in history]

        # No history covers this month - use current data from shift_time_segments table
        cursor.execute("""
            SELECT id, shift_type_id, start_time, end_time,
                   wage_percent, segment_type, order_index
            FROM shift_time_segments
            WHERE shift_type_id = %s
            ORDER BY order_index
        """, (shift_type_id,))

        return [dict(row) for row in cursor.fetchall()]
    finally:
        cursor.close()


def get_all_segments_for_month(conn, shift_type_ids: list, year: int, month: int) -> dict:
    """
    Get all shift time segments for multiple shift types for a specific month.
    Uses "valid until" logic: for each shift type, finds historical data that covers
    the requested month, falls back to current data.

    Args:
        shift_type_ids: List of shift type IDs to get segments for

    Returns:
        dict mapping shift_type_id to list of segment dicts
    """
    if not shift_type_ids:
        return {}

    result = {}
    
    # For each shift type, get segments using "valid until" logic
    for shift_type_id in shift_type_ids:
        segments = get_segments_for_shift_month(conn, shift_type_id, year, month)
        if segments:
            result[shift_type_id] = segments

    return result


def save_segment_to_history(
    conn,
    segment_id: int,
    shift_type_id: int,
    year: int,
    month: int,
    wage_percent: int,
    segment_type: str,
    start_time: str,
    end_time: str,
    order_index: int,
    created_by: int = None
) -> bool:
    """
    Save a shift time segment to history before a change.
    Called before updating shift_time_segments.

    Returns:
        True if saved successfully
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO shift_time_segments_history
            (segment_id, shift_type_id, year, month, wage_percent, segment_type,
             start_time, end_time, order_index, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (segment_id, year, month)
            DO UPDATE SET
                shift_type_id = EXCLUDED.shift_type_id,
                wage_percent = EXCLUDED.wage_percent,
                segment_type = EXCLUDED.segment_type,
                start_time = EXCLUDED.start_time,
                end_time = EXCLUDED.end_time,
                order_index = EXCLUDED.order_index,
                created_by = EXCLUDED.created_by,
                created_at = NOW()
        """, (segment_id, shift_type_id, year, month, wage_percent, segment_type,
              start_time, end_time, order_index, created_by))

        conn.commit()
        logger.info(f"Saved segment {segment_id} history for {year}/{month}")
        return True
    except Exception as e:
        logger.error(f"Error saving segment history: {e}")
        return False
    finally:
        cursor.close()


def save_all_segments_to_history(conn, year: int, month: int, created_by: int = None) -> bool:
    """
    Save all current shift time segments to history for a specific month.
    Called before making segment changes or when locking a month.

    Returns:
        True if saved successfully
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO shift_time_segments_history
            (year, month, segment_id, shift_type_id, wage_percent, segment_type,
             start_time, end_time, order_index, created_by)
            SELECT %s, %s, id, shift_type_id, wage_percent, segment_type,
                   start_time, end_time, order_index, %s
            FROM shift_time_segments
            ON CONFLICT (segment_id, year, month) DO NOTHING
        """, (year, month, created_by))

        conn.commit()
        logger.info(f"Saved all segments history for {year}/{month}")
        return True
    except Exception as e:
        logger.error(f"Error saving segments history: {e}")
        return False
    finally:
        cursor.close()


# ============================================================================
# Minimum Wage History Functions
# ============================================================================

def get_minimum_wage_for_month(conn, year: int, month: int) -> float:
   
    # Validate month
    if not (1 <= month <= 12):
        raise ValueError(f"Invalid month: {month}. Must be 1-12.")

    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        # Get the rate that was effective at the start of the month
        month_start = f"{year}-{month:02d}-01"

        cursor.execute("""
            SELECT hourly_rate
            FROM minimum_wage_rates
            WHERE effective_from <= %s
            ORDER BY effective_from DESC
            LIMIT 1
        """, (month_start,))

        row = cursor.fetchone()

        if row and row["hourly_rate"]:
            rate = float(row["hourly_rate"]) / 100  # Convert from agorot to shekels
            if rate > 0:
                return rate

        raise ValueError(
            f"No minimum wage found in DB for {year}/{month}. "
            f"Please add the rate to minimum_wage_rates table with effective_from <= {year}-{month:02d}-01"
        )
    finally:
        cursor.close()


def apply_historical_overrides(
    conn,
    reports: List[Dict],
    person_id: int,
    year: int,
    month: int
) -> List[Dict]:
    """
    החלת נתונים היסטוריים על דיווחים.

    מעדכן את הדיווחים עם:
    - סטטוס נישואין היסטורי
    - סוג דירה היסטורי
    - תעריפי משמרת היסטוריים

    פרמטרים:
        conn: חיבור לבסיס הנתונים
        reports: רשימת דיווחים (כמילונים)
        person_id: מזהה העובד
        year: שנה
        month: חודש

    מחזיר:
        רשימת דיווחים מעודכנת עם הנתונים ההיסטוריים
    """
    if not reports:
        return []

    # שליפת סטטוס נישואין היסטורי
    historical_person = get_person_status_for_month(conn, person_id, year, month)
    historical_is_married = historical_person.get("is_married")

    # בניית מטמון סוגי דירות היסטוריים
    apartment_ids = {r.get("apartment_id") for r in reports if r.get("apartment_id")}
    apartment_type_cache = {}
    for apt_id in apartment_ids:
        hist_type = get_apartment_type_for_month(conn, apt_id, year, month)
        if hist_type is not None:
            apartment_type_cache[apt_id] = hist_type

    # בניית מטמון תעריפי משמרות היסטוריים
    shift_rates_cache = get_all_shift_rates_for_month(conn, year, month)

    # החלת הנתונים ההיסטוריים על כל דיווח
    processed_reports = []
    for r in reports:
        r_dict = dict(r) if not isinstance(r, dict) else r.copy()

        # עדכון סטטוס נישואין
        if historical_is_married is not None:
            r_dict["is_married"] = historical_is_married

        # עדכון סוג דירה - עדיפות: rate_apartment_type_id > היסטורי > נוכחי
        rate_apt_type = r_dict.get("rate_apartment_type_id")
        if rate_apt_type:
            r_dict["apartment_type_id"] = rate_apt_type
        else:
            apt_id = r_dict.get("apartment_id")
            if apt_id and apt_id in apartment_type_cache:
                old_val = r_dict.get("apartment_type_id")
                r_dict["apartment_type_id"] = apartment_type_cache[apt_id]
                if old_val != apartment_type_cache[apt_id]:
                    logger.debug(f"Historical override for apt {apt_id}: {old_val} -> {apartment_type_cache[apt_id]} ({year}/{month})")

        # עדכון תעריפי משמרת
        shift_type_id = r_dict.get("shift_type_id")
        if shift_type_id and shift_type_id in shift_rates_cache:
            rate_info = shift_rates_cache[shift_type_id]
            r_dict["shift_rate"] = rate_info.get("rate")
            r_dict["shift_is_minimum_wage"] = rate_info.get("is_minimum_wage")
            r_dict["shift_wage_percentage"] = rate_info.get("wage_percentage")

        processed_reports.append(r_dict)

    return processed_reports
