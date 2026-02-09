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


def get_all_person_statuses_for_month(
    conn, person_ids: List[int], year: int, month: int
) -> Dict[int, dict]:
    """
    טעינת סטטוס כל העובדים בשאילתה אחת.

    Returns:
        dict mapping person_id to {is_married, employer_id, employee_type}
    """
    if not person_ids:
        return {}

    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    result = {}
    try:
        # Single query with DISTINCT ON to get historical records
        placeholders = ",".join(["%s"] * len(person_ids))
        cursor.execute(f"""
            WITH historical AS (
                SELECT DISTINCT ON (person_id)
                    person_id, is_married, employer_id, employee_type
                FROM person_status_history
                WHERE person_id IN ({placeholders})
                  AND (year > %s OR (year = %s AND month > %s))
                ORDER BY person_id, year ASC, month ASC
            )
            SELECT
                p.id as person_id,
                COALESCE(h.is_married, p.is_married) as is_married,
                COALESCE(h.employer_id, p.employer_id) as employer_id,
                COALESCE(h.employee_type, p.type) as employee_type
            FROM people p
            LEFT JOIN historical h ON h.person_id = p.id
            WHERE p.id IN ({placeholders})
        """, (*person_ids, year, year, month, *person_ids))

        for row in cursor.fetchall():
            result[row["person_id"]] = {
                "is_married": row["is_married"],
                "employer_id": row["employer_id"],
                "employee_type": row["employee_type"]
            }

        return result
    finally:
        cursor.close()


def get_all_apartment_types_for_month(
    conn, apartment_ids: List[int], year: int, month: int
) -> Dict[int, Optional[int]]:
    """
    טעינת סוג כל הדירות בשאילתה אחת.

    Returns:
        dict mapping apartment_id to apartment_type_id
    """
    if not apartment_ids:
        return {}

    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    result = {}
    try:
        placeholders = ",".join(["%s"] * len(apartment_ids))
        cursor.execute(f"""
            WITH historical AS (
                SELECT DISTINCT ON (apartment_id)
                    apartment_id, apartment_type_id
                FROM apartment_status_history
                WHERE apartment_id IN ({placeholders})
                  AND (year > %s OR (year = %s AND month > %s))
                ORDER BY apartment_id, year ASC, month ASC
            )
            SELECT
                a.id as apartment_id,
                COALESCE(h.apartment_type_id, a.apartment_type_id) as apartment_type_id
            FROM apartments a
            LEFT JOIN historical h ON h.apartment_id = a.id
            WHERE a.id IN ({placeholders})
        """, (*apartment_ids, year, year, month, *apartment_ids))

        for row in cursor.fetchall():
            result[row["apartment_id"]] = row["apartment_type_id"]

        return result
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


# ============================================================================
# Shift Type Housing Rates History Functions
# ============================================================================

def get_all_housing_rates_for_month(conn, year: int = None, month: int = None) -> dict:
    """
    טעינת כל תעריפי מערכי הדיור בשאילתה אחת.

    אם year ו-month מסופקים, בודק קודם בטבלת ההיסטוריה.
    רשומות היסטוריה שומרות (year, month) כ-"valid until" - כלומר הערך הישן
    היה תקף עד (לא כולל) החודש הזה.

    Returns:
        dict: מיפוי (shift_type_id, housing_array_id) -> rate_info
    """
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    result = {}
    try:
        if year is not None and month is not None:
            # שאילתה עם תמיכה בהיסטוריה
            # שימוש ב-CASE WHEN במקום COALESCE כי NULL בהיסטוריה = שכר מינימום
            # COALESCE(NULL, value) מחזיר value, אבל אנחנו רוצים NULL אם יש רשומת היסטוריה
            cursor.execute("""
                WITH historical AS (
                    SELECT DISTINCT ON (shift_type_id, housing_array_id)
                        shift_type_id, housing_array_id,
                        weekday_single_rate, weekday_single_wage_percentage,
                        weekday_married_rate, weekday_married_wage_percentage,
                        shabbat_rate, shabbat_wage_percentage
                    FROM shift_type_housing_rates_history
                    WHERE (year > %s OR (year = %s AND month > %s))
                    ORDER BY shift_type_id, housing_array_id, year ASC, month ASC
                )
                SELECT
                    sthr.shift_type_id,
                    sthr.housing_array_id,
                    CASE WHEN h.shift_type_id IS NOT NULL THEN h.weekday_single_rate ELSE sthr.weekday_single_rate END as weekday_single_rate,
                    CASE WHEN h.shift_type_id IS NOT NULL THEN h.weekday_single_wage_percentage ELSE sthr.weekday_single_wage_percentage END as weekday_single_wage_percentage,
                    CASE WHEN h.shift_type_id IS NOT NULL THEN h.weekday_married_rate ELSE sthr.weekday_married_rate END as weekday_married_rate,
                    CASE WHEN h.shift_type_id IS NOT NULL THEN h.weekday_married_wage_percentage ELSE sthr.weekday_married_wage_percentage END as weekday_married_wage_percentage,
                    CASE WHEN h.shift_type_id IS NOT NULL THEN h.shabbat_rate ELSE sthr.shabbat_rate END as shabbat_rate,
                    CASE WHEN h.shift_type_id IS NOT NULL THEN h.shabbat_wage_percentage ELSE sthr.shabbat_wage_percentage END as shabbat_wage_percentage
                FROM shift_type_housing_rates sthr
                LEFT JOIN historical h ON h.shift_type_id = sthr.shift_type_id
                    AND h.housing_array_id = sthr.housing_array_id
                WHERE sthr.is_active = true
            """, (year, year, month))
        else:
            # שאילתה פשוטה ללא היסטוריה
            cursor.execute("""
                SELECT
                    shift_type_id,
                    housing_array_id,
                    weekday_single_rate,
                    weekday_single_wage_percentage,
                    weekday_married_rate,
                    weekday_married_wage_percentage,
                    shabbat_rate,
                    shabbat_wage_percentage
                FROM shift_type_housing_rates
                WHERE is_active = true
            """)

        for row in cursor.fetchall():
            key = (row["shift_type_id"], row["housing_array_id"])
            result[key] = {
                "weekday_single_rate": row["weekday_single_rate"],
                "weekday_single_wage_percentage": row["weekday_single_wage_percentage"],
                "weekday_married_rate": row["weekday_married_rate"],
                "weekday_married_wage_percentage": row["weekday_married_wage_percentage"],
                "shabbat_rate": row["shabbat_rate"],
                "shabbat_wage_percentage": row["shabbat_wage_percentage"],
            }

        return result
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


def get_all_apartment_type_change_dates(
    conn, apartment_ids: List[int]
) -> Dict[int, Optional[str]]:
    """
    טוען תאריכי שינוי סוג דירה לכל הדירות בשאילתה אחת.

    מחזיר את התאריך האחרון שבו סוג הדירה השתנה (הרשומה האחרונה בהיסטוריה).
    אם אין רשומה - הדירה לא השתנתה מעולם.

    Returns:
        dict mapping apartment_id to date string "MM/YYYY" or None
    """
    if not apartment_ids:
        return {}

    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cursor.execute("""
            SELECT DISTINCT ON (apartment_id)
                apartment_id, year, month
            FROM apartment_status_history
            WHERE apartment_id = ANY(%s)
            ORDER BY apartment_id, year DESC, month DESC
        """, (list(apartment_ids),))

        result: Dict[int, Optional[str]] = {apt_id: None for apt_id in apartment_ids}
        for row in cursor.fetchall():
            result[row['apartment_id']] = f"{row['month']:02d}/{row['year']}"
        return result
    finally:
        cursor.close()

