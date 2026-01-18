"""
Core business logic for DiyurCalc application.
Contains public API functions for calculating monthly totals and summaries.

This module re-exports functions from submodules for backwards compatibility.
New code should import directly from the submodules:
- core.time_utils: Time conversion and Shabbat detection
- core.segments: Shift segment processing
- core.wage_calculator: Wage calculation and daily processing
"""
import logging
import psycopg2
import psycopg2.extras
from typing import List, Tuple, Dict, Any

from utils.cache_manager import cached, cache

# =============================================================================
# Re-exports from submodules for backwards compatibility
# =============================================================================

# Time utilities and Shabbat
from core.time_utils import (
    MINUTES_PER_HOUR,
    MINUTES_PER_DAY,
    REGULAR_HOURS_LIMIT,
    OVERTIME_125_LIMIT,
    WORK_DAY_START_MINUTES,
    SHABBAT_ENTER_DEFAULT,
    SHABBAT_EXIT_DEFAULT,
    LOCAL_TZ,
    to_local_date,
    span_minutes,
    minutes_to_time_str,
    is_shabbat_time,
    get_shabbat_times_cache,
)

# Segment processing
from core.segments import (
    BREAK_THRESHOLD_MINUTES,
    NIGHT_SHIFT_WORK_FIRST_MINUTES,
    NIGHT_SHIFT_STANDBY_END,
    NIGHT_SHIFT_MORNING_END,
    NOON_MINUTES,
    MEDICAL_ESCORT_SHIFT_ID,
)

# Wage calculation
from core.wage_calculator import (
    STANDBY_CANCEL_OVERLAP_THRESHOLD,
    DEFAULT_STANDBY_RATE,
    calculate_wage_rate,
    _calculate_chain_wages,
)

# Month availability functions - now in utils
from utils.utils import available_months, available_months_from_db

# =============================================================================
# Configure logging
# =============================================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# Data Access Functions (with caching)
# =============================================================================

MINIMUM_WAGE_CACHE_KEY = "minimum_wage_cache"
MINIMUM_WAGE_CACHE_TTL = 86400  # 24 hours


def get_minimum_wage(conn) -> float:
    """
    Get current minimum wage rate from DB with 24-hour caching.
    Returns hourly rate in shekels.

    Raises:
        ValueError: if no valid minimum wage found in DB
    """
    cached_result = cache.get(MINIMUM_WAGE_CACHE_KEY)
    if cached_result is not None:
        return cached_result

    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute("SELECT hourly_rate FROM minimum_wage_rates ORDER BY effective_from DESC LIMIT 1")
    row = cursor.fetchone()
    cursor.close()

    if row and row["hourly_rate"]:
        result = float(row["hourly_rate"]) / 100  # Convert from agorot to shekels
        if result <= 0:
            raise ValueError("Invalid minimum wage rate in DB (must be positive)")
    else:
        raise ValueError(
            "No minimum wage found in DB. "
            "Please add a rate to minimum_wage_rates table."
        )

    cache.set(MINIMUM_WAGE_CACHE_KEY, result, MINIMUM_WAGE_CACHE_TTL)
    return result


def get_standby_rate(conn, segment_id: int, apartment_type_id: int | None, is_married: bool, year: int = None, month: int = None) -> float:
    """
    Get standby rate from standby_rates table.
    Priority: specific apartment_type (priority=10) > general (priority=0)
    If year/month provided, checks historical rates first.
    """
    marital_status = "married" if is_married else "single"

    # If year/month provided, try historical rates first
    if year is not None and month is not None:
        from core.history import get_standby_rate_for_month
        historical_amount = get_standby_rate_for_month(
            conn, segment_id, apartment_type_id, marital_status, year, month
        )
        if historical_amount is not None:
            return float(historical_amount) / 100

    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # First try specific rate for apartment type (priority=10)
    if apartment_type_id is not None:
        cursor.execute("""
            SELECT amount FROM standby_rates
            WHERE segment_id = %s AND apartment_type_id = %s AND marital_status = %s AND priority = 10
            LIMIT 1
        """, (segment_id, apartment_type_id, marital_status))
        row = cursor.fetchone()
        if row:
            cursor.close()
            return float(row["amount"]) / 100

    # Fallback to general rate (priority=0)
    cursor.execute("""
        SELECT amount FROM standby_rates
        WHERE segment_id = %s AND apartment_type_id IS NULL AND marital_status = %s AND priority = 0
        LIMIT 1
    """, (segment_id, marital_status))
    row = cursor.fetchone()
    cursor.close()

    if row:
        return float(row["amount"]) / 100

    return DEFAULT_STANDBY_RATE


@cached(ttl=1800)  # Cache for 30 minutes
def get_active_guides() -> List[Dict[str, Any]]:
    """Fetch active guides from people table."""
    from core.database import get_pooled_connection, return_connection
    conn = get_pooled_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(
            """
            SELECT id, name, type, is_active, start_date
            FROM people
            WHERE is_active::integer = 1
            ORDER BY name
            """
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()
        return_connection(conn)

    return [dict(row) for row in rows]


def get_available_months_for_person(conn, person_id: int) -> List[Tuple[int, int]]:
    """Fetch distinct months for a specific person efficiently using SQL."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT DISTINCT
                CAST(EXTRACT(YEAR FROM date) AS INTEGER) as year,
                CAST(EXTRACT(MONTH FROM date) AS INTEGER) as month
            FROM time_reports
            WHERE person_id = %s
            ORDER BY year DESC, month DESC
        """, (person_id,))
        rows = cursor.fetchall()
        return [(r[0], r[1]) for r in rows]
    except Exception as e:
        logger.warning(f"Error fetching months for person {person_id}: {e}")
        return []
    finally:
        cursor.close()


def get_payment_codes(conn):
    """Fetch payment codes sorted by display_order."""
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("""
            SELECT * FROM payment_codes
            ORDER BY display_order ASC NULLS LAST
        """)
        result = cursor.fetchall()
        cursor.close()
        return result
    except Exception as e:
        logger.error(f"Error fetching payment codes: {e}")
        return []


def ensure_sick_payment_code(conn):
    """
    מוודא שקוד מירב 319 לתשלום מחלה קיים בטבלת payment_codes.
    אם לא קיים, מוסיף אותו.
    """
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # בדיקה אם הקוד כבר קיים
        cursor.execute("""
            SELECT id FROM payment_codes WHERE internal_key = 'sick_payment'
        """)
        existing = cursor.fetchone()

        if not existing:
            # הוספת קוד מחלה חדש
            cursor.execute("""
                INSERT INTO payment_codes (internal_key, display_name, merav_code, display_order)
                VALUES ('sick_payment', 'תשלום מחלה', '319', 175)
            """)
            conn.commit()
            logger.info("Added sick_payment code (319) to payment_codes table")

        cursor.close()
    except Exception as e:
        logger.error(f"Error ensuring sick payment code: {e}")


# =============================================================================
# Main Calculation Functions
# =============================================================================

def calculate_person_monthly_totals(
    conn,
    person_id: int,
    year: int,
    month: int,
    shabbat_cache: Dict[str, Dict[str, str]],
    minimum_wage: float = None
) -> Dict:
    """
    חישוב מדויק של סיכומים חודשיים לעובד.

    Uses the unified calculation logic from app_utils (get_daily_segments_data +
    aggregate_daily_segments_to_monthly) which is the source of truth for wage calculation.
    """
    from core.history import get_minimum_wage_for_month
    from core.database import PostgresConnection
    from app_utils import get_daily_segments_data, aggregate_daily_segments_to_monthly

    # Get minimum wage for the specific month (historical)
    if minimum_wage is None:
        minimum_wage = get_minimum_wage_for_month(conn, year, month)

    # Wrap the raw psycopg2 connection in PostgresConnection for app_utils compatibility
    conn_wrapper = PostgresConnection(conn, use_pool=False)

    # Use the unified calculation from app_utils (source of truth)
    daily_segments, _ = get_daily_segments_data(
        conn_wrapper, person_id, year, month, shabbat_cache, minimum_wage
    )

    monthly_totals = aggregate_daily_segments_to_monthly(
        conn_wrapper, daily_segments, person_id, year, month, minimum_wage
    )

    return monthly_totals


# NOTE: _calculate_totals_from_data was removed as dead code.
# The calculation is now done exclusively through app_utils.get_daily_segments_data
# and app_utils.aggregate_daily_segments_to_monthly (source of truth).


def calculate_monthly_summary(conn, year: int, month: int) -> Tuple[List[Dict], Dict]:
    """
    Calculate monthly summary for all active people.

    Uses the unified calculation logic from app_utils (get_daily_segments_data +
    aggregate_daily_segments_to_monthly) which is the source of truth for wage calculation.
    """
    from core.history import get_minimum_wage_for_month
    from core.database import PostgresConnection
    from app_utils import get_daily_segments_data, aggregate_daily_segments_to_monthly

    payment_codes = get_payment_codes(conn)

    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute("SELECT id, name, start_date, is_married, meirav_code FROM people WHERE is_active::integer = 1 ORDER BY name")
    people = cursor.fetchall()
    cursor.close()

    shabbat_cache = get_shabbat_times_cache(conn)
    minimum_wage = get_minimum_wage_for_month(conn, year, month)

    # Wrap the raw psycopg2 connection in PostgresConnection for app_utils compatibility
    conn_wrapper = PostgresConnection(conn, use_pool=False)

    summary_data = []
    grand_totals = {code["internal_key"]: 0 for code in payment_codes}
    grand_totals.update({
        "payment": 0, "standby_payment": 0, "travel": 0, "extras": 0, "total_payment": 0,
        "calc150_shabbat_100": 0, "calc150_shabbat_50": 0,
        "vacation_payment": 0, "vacation_minutes": 0
    })

    for p in people:
        pid = p["id"]

        # Use the unified calculation from app_utils (source of truth)
        daily_segments, _ = get_daily_segments_data(
            conn_wrapper, pid, year, month, shabbat_cache, minimum_wage
        )

        monthly_totals = aggregate_daily_segments_to_monthly(
            conn_wrapper, daily_segments, pid, year, month, minimum_wage
        )

        if monthly_totals.get("total_payment", 0) > 0 or monthly_totals.get("total_hours", 0) > 0:
            summary_data.append({"name": p["name"], "person_id": p["id"], "merav_code": p["meirav_code"], "totals": monthly_totals})

            grand_totals["payment"] += monthly_totals.get("total_payment", 0)
            grand_totals["total_payment"] += monthly_totals.get("total_payment", 0)

            for k, v in monthly_totals.items():
                if k in grand_totals and isinstance(v, (int, float)) and k not in ("payment", "total_payment"):
                    grand_totals[k] += v

    return summary_data, grand_totals
