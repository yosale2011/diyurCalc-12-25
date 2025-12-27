"""
History module for DiyurCalc.
Handles historical data lookups for person status, apartment types, and standby rates.
Uses "save on change" approach - current data is used unless there's a historical record.
"""
from __future__ import annotations

import logging
from typing import Optional, Any
from datetime import datetime

import psycopg2.extras

logger = logging.getLogger(__name__)


def get_person_status_for_month(conn, person_id: int, year: int, month: int) -> dict:
    """
    Get person status (married, employer, type) for a specific month.
    First checks history table, falls back to current data in people table.

    Returns:
        dict with keys: is_married, employer_id, employee_type
    """
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        # First, check if there's a historical record for this month
        cursor.execute("""
            SELECT is_married, employer_id, employee_type
            FROM person_status_history
            WHERE person_id = %s AND year = %s AND month = %s
        """, (person_id, year, month))

        history = cursor.fetchone()

        if history:
            logger.debug(f"Using historical data for person {person_id} ({year}/{month})")
            return {
                "is_married": history["is_married"],
                "employer_id": history["employer_id"],
                "employee_type": history["employee_type"]
            }

        # No history - use current data from people table
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
    First checks history table, falls back to current data in apartments table.

    Returns:
        apartment_type_id or None
    """
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        # First, check if there's a historical record for this month
        cursor.execute("""
            SELECT apartment_type_id
            FROM apartment_status_history
            WHERE apartment_id = %s AND year = %s AND month = %s
        """, (apartment_id, year, month))

        history = cursor.fetchone()

        if history:
            logger.debug(f"Using historical data for apartment {apartment_id} ({year}/{month})")
            return history["apartment_type_id"]

        # No history - use current data from apartments table
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
    """
    Get standby rate amount for a specific month.
    First checks history table, falls back to current data in standby_rates table.

    Args:
        segment_id: The shift segment ID
        apartment_type_id: The apartment type ID
        marital_status: 'married' or 'single'
        year: Year
        month: Month

    Returns:
        amount in agorot or None
    """
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        # First, check if there's a historical record for this month
        cursor.execute("""
            SELECT amount
            FROM standby_rates_history
            WHERE year = %s AND month = %s
            AND segment_id = %s
            AND apartment_type_id = %s
            AND marital_status = %s
        """, (year, month, segment_id, apartment_type_id, marital_status))

        history = cursor.fetchone()

        if history:
            logger.debug(f"Using historical standby rate for segment {segment_id} ({year}/{month})")
            return history["amount"]

        # No history - use current data from standby_rates table
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


def save_person_status_to_history(
    conn,
    person_id: int,
    year: int,
    month: int,
    is_married: bool,
    employer_id: int,
    employee_type: str,
    created_by: int = None
) -> bool:
    """
    Save person status to history before a change.
    Called from the forms system when a change is made.

    Returns:
        True if saved successfully
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO person_status_history
            (person_id, year, month, is_married, employer_id, employee_type, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (person_id, year, month)
            DO UPDATE SET
                is_married = EXCLUDED.is_married,
                employer_id = EXCLUDED.employer_id,
                employee_type = EXCLUDED.employee_type,
                created_by = EXCLUDED.created_by,
                created_at = NOW()
        """, (person_id, year, month, is_married, employer_id, employee_type, created_by))

        conn.commit()
        logger.info(f"Saved person {person_id} status history for {year}/{month}")
        return True
    except Exception as e:
        logger.error(f"Error saving person status history: {e}")
        return False
    finally:
        cursor.close()


def save_apartment_status_to_history(
    conn,
    apartment_id: int,
    year: int,
    month: int,
    apartment_type_id: int,
    created_by: int = None
) -> bool:
    """
    Save apartment status to history before a change.

    Returns:
        True if saved successfully
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO apartment_status_history
            (apartment_id, year, month, apartment_type_id, created_by)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (apartment_id, year, month)
            DO UPDATE SET
                apartment_type_id = EXCLUDED.apartment_type_id,
                created_by = EXCLUDED.created_by,
                created_at = NOW()
        """, (apartment_id, year, month, apartment_type_id, created_by))

        conn.commit()
        logger.info(f"Saved apartment {apartment_id} status history for {year}/{month}")
        return True
    except Exception as e:
        logger.error(f"Error saving apartment status history: {e}")
        return False
    finally:
        cursor.close()


def save_standby_rates_to_history(conn, year: int, month: int, created_by: int = None) -> bool:
    """
    Save all current standby rates to history for a specific month.
    Usually called before making rate changes.

    Returns:
        True if saved successfully
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO standby_rates_history
            (year, month, original_rate_id, segment_id, apartment_type_id, marital_status, amount, created_by)
            SELECT %s, %s, id, segment_id, apartment_type_id, marital_status, amount, %s
            FROM standby_rates
            ON CONFLICT DO NOTHING
        """, (year, month, created_by))

        conn.commit()
        logger.info(f"Saved standby rates history for {year}/{month}")
        return True
    except Exception as e:
        logger.error(f"Error saving standby rates history: {e}")
        return False
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
