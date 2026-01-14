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
from datetime import datetime, timedelta, date
from typing import List, Tuple, Dict, Optional, Any
from zoneinfo import ZoneInfo

from convertdate import hebrew

# Import utilities and config
from core.config import config
from utils.cache_manager import cached, cache
from utils.utils import overlap_minutes

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
    FRIDAY,
    SATURDAY,
    LOCAL_TZ,
    to_local_date,
    parse_hhmm,
    span_minutes,
    minutes_to_time_str,
    is_shabbat_time,
    get_shabbat_times_cache,
    _get_shabbat_boundaries,
    SHABBAT_CACHE_KEY,
    SHABBAT_CACHE_TTL,
)

# Segment processing
from core.segments import (
    BREAK_THRESHOLD_MINUTES,
    NIGHT_SHIFT_WORK_FIRST_MINUTES,
    NIGHT_SHIFT_STANDBY_END,
    NIGHT_SHIFT_MORNING_END,
    NOON_MINUTES,
    MEDICAL_ESCORT_SHIFT_ID,
    _create_segment_dict,
    _build_night_shift_segments,
    _process_tagbur_shift,
    _process_fixed_vacation_shift,
    _build_daily_map,
)

# Wage calculation
from core.wage_calculator import (
    STANDBY_CANCEL_OVERLAP_THRESHOLD,
    DEFAULT_STANDBY_RATE,
    calculate_wage_rate,
    _calculate_chain_wages,
    _process_daily_map,
)

# Month availability functions - now in utils
from utils.utils import available_months, available_months_from_db

# =============================================================================
# Configure logging
# =============================================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# Database connection (legacy - prefer core.database.get_conn())
# =============================================================================
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONNECTION_STRING = os.getenv("DATABASE_URL")
if not DB_CONNECTION_STRING:
    raise RuntimeError("DATABASE_URL environment variable is required. Please set it in .env file.")


def get_db_connection():
    """Create and return a PostgreSQL database connection.
    Note: Prefer using core.database.get_conn() for connection pooling."""
    try:
        conn = psycopg2.connect(DB_CONNECTION_STRING)
        return conn
    except psycopg2.OperationalError as e:
        error_msg = str(e)
        if "could not translate host name" in error_msg or "Name or service not known" in error_msg:
            logger.error(
                f"Database DNS resolution failed. Hostname cannot be resolved.\n"
                f"Error: {error_msg}\n"
                f"Please check:\n"
                f"1. Your internet connection\n"
                f"2. DNS settings\n"
                f"3. VPN/firewall configuration\n"
                f"4. Database hostname in DATABASE_URL is correct"
            )
        elif "connection refused" in error_msg.lower():
            logger.error(
                f"Database connection refused.\n"
                f"Error: {error_msg}\n"
                f"Please check:\n"
                f"1. Database server is running\n"
                f"2. Port number is correct\n"
                f"3. Firewall allows connections"
            )
        else:
            logger.error(f"Database connection error: {error_msg}")
        raise
    except Exception as e:
        logger.error(f"Unexpected database connection error: {e}")
        raise


def dict_cursor(conn):
    """Create a cursor that returns rows as dicts."""
    return conn.cursor(cursor_factory=psycopg2.extras.DictCursor)


# =============================================================================
# Local Constants (kept for compatibility)
# =============================================================================

# Wage/Accrual constants
STANDARD_WORK_DAYS_PER_MONTH = config.STANDARD_WORK_DAYS_PER_MONTH
MAX_SICK_DAYS_PER_MONTH = config.MAX_SICK_DAYS_PER_MONTH

# Wage multipliers (overtime percentages)
WAGE_MULTIPLIER_100 = 1.0
WAGE_MULTIPLIER_125 = 1.25
WAGE_MULTIPLIER_150 = 1.5
WAGE_MULTIPLIER_175 = 1.75
WAGE_MULTIPLIER_200 = 2.0

# Additional time constants for app_utils compatibility
MORNING_STANDBY_END_MINUTES = 390   # 06:30
WORK_DAY_END_NORMALIZED = 1920      # 08:00 next day (480 + 1440)

# Medical escort constants
MINIMUM_ESCORT_MINUTES = 60         # שעה מינימלית לליווי רפואי

# Night shift standby wage percent
NIGHT_STANDBY_WAGE_PERCENT = 24     # אחוז שכר כוננות בלילה


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
            logger.debug(f"Using historical standby rate for seg={segment_id}, type={apartment_type_id}: {historical_amount/100}")
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


# =============================================================================
# Main Calculation Functions
# =============================================================================

def _create_empty_monthly_totals() -> Dict:
    """
    יצירת מילון סיכומים חודשיים ריק עם כל השדות הנדרשים.

    מחזיר:
        מילון עם כל שדות הסיכום מאותחלים לאפס
    """
    return {
        "total_hours": 0,
        "payment": 0,
        "standby": 0,
        "standby_payment": 0,
        "actual_work_days": 0,
        "vacation_days_taken": 0,
        "calc100": 0,
        "calc125": 0,
        "calc150": 0,
        "calc150_shabbat": 0,
        "calc150_shabbat_100": 0,
        "calc150_shabbat_50": 0,
        "calc150_overtime": 0,
        "calc175": 0,
        "calc200": 0,
        "calc_variable": 0,
        "vacation_minutes": 0,
        "vacation_payment": 0,
        "travel": 0,
        "extras": 0,
        "sick_days_accrued": 0,
        "vacation_days_accrued": 0
    }


def _build_variable_rate_map(reports: List[Dict]) -> Dict[int, float]:
    """
    בניית מפת תעריפים משתנים לפי סוג משמרת.

    פרמטרים:
        reports: רשימת דיווחים

    מחזיר:
        מילון {shift_type_id: rate} עם תעריפים מותאמים
    """
    variable_rate_by_shift = {}
    for r in reports:
        shift_rate = r.get("shift_rate")
        shift_type_id = r.get("shift_type_id")
        if shift_rate:
            variable_rate_by_shift[shift_type_id] = float(shift_rate) / 100
    return variable_rate_by_shift


def _calculate_variable_rate_payment(
    daily_map: Dict,
    variable_rate_by_shift: Dict[int, float],
    minimum_wage: float
) -> Tuple[int, float]:
    """
    חישוב דקות ותשלום נוסף עבור תעריפים משתנים.

    פרמטרים:
        daily_map: מפת הימים עם הסגמנטים
        variable_rate_by_shift: מפת תעריפים לפי משמרת
        minimum_wage: שכר מינימום

    מחזיר:
        tuple של (דקות בתעריף משתנה, תשלום נוסף)
    """
    variable_rate_minutes = 0
    variable_rate_extra_payment = 0.0

    for day_key, entry in daily_map.items():
        for seg in entry.get("segments", []):
            s_start, s_end, s_type, shift_id, seg_id, apt_type, is_married = seg
            duration = s_end - s_start
            if s_type == "work":
                if shift_id in variable_rate_by_shift:
                    variable_rate_minutes += duration
                    actual_rate = variable_rate_by_shift[shift_id]
                    rate_diff = actual_rate - minimum_wage
                    if rate_diff > 0:
                        variable_rate_extra_payment += (duration / 60) * rate_diff

    return variable_rate_minutes, variable_rate_extra_payment


def _process_payment_components(payment_comps: List[Dict], monthly_totals: Dict) -> None:
    """
    עיבוד רכיבי תשלום נוספים (נסיעות ותוספות).

    פרמטרים:
        payment_comps: רשימת רכיבי תשלום
        monthly_totals: מילון הסיכומים לעדכון
    """
    for pc in payment_comps:
        amount = (pc["total_amount"] or 0) / 100
        if pc["component_type_id"] == 2 or pc["component_type_id"] == 7:
            monthly_totals["travel"] += amount
        else:
            monthly_totals["extras"] += amount


def _get_default_vacation_details() -> Dict:
    """
    קבלת ערכי ברירת מחדל לפרטי חופשה.

    מחזיר:
        מילון עם ערכי ברירת מחדל
    """
    return {
        "seniority": 1,
        "annual_quota": 12,
        "job_scope_pct": 100
    }


def _calculate_final_payment(monthly_totals: Dict, minimum_wage: float) -> None:
    """
    חישוב התשלום הסופי על בסיס כל הרכיבים.

    פרמטרים:
        monthly_totals: מילון הסיכומים
        minimum_wage: שכר מינימום
    """
    pay = 0
    pay += (monthly_totals["calc100"] / 60) * minimum_wage * 1.0
    pay += (monthly_totals["calc125"] / 60) * minimum_wage * 1.25
    pay += (monthly_totals["calc150"] / 60) * minimum_wage * 1.5
    pay += (monthly_totals["calc175"] / 60) * minimum_wage * 1.75
    pay += (monthly_totals["calc200"] / 60) * minimum_wage * 2.0
    pay += monthly_totals.get("variable_rate_extra_payment", 0)
    pay += monthly_totals["standby_payment"]
    pay += monthly_totals["vacation_payment"]
    monthly_totals["payment"] = pay
    monthly_totals["total_payment"] = pay + monthly_totals["travel"] + monthly_totals["extras"]


def _count_standby_dates(reports: List[Dict], shift_has_standby: Dict[int, bool]) -> int:
    """
    ספירת ימים עם כוננות.

    פרמטרים:
        reports: רשימת דיווחים
        shift_has_standby: מילון המציין אילו משמרות כוללות כוננות

    מחזיר:
        מספר הימים עם כוננות
    """
    dates_with_standby = set()
    for r in reports:
        if r["shift_type_id"] and shift_has_standby.get(r["shift_type_id"], False):
            dates_with_standby.add(r["date"])
    return len(dates_with_standby)


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
    """
    from utils.utils import month_range_ts, calculate_accruals
    from core.history import get_minimum_wage_for_month, apply_historical_overrides

    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Get minimum wage for the specific month (historical)
    if minimum_wage is None:
        minimum_wage = get_minimum_wage_for_month(conn, year, month)

    # שליפת פרטי העובד
    cursor.execute("""
        SELECT id, name, phone, email, is_active, start_date, is_married, type
        FROM people WHERE id = %s
    """, (person_id,))
    person = cursor.fetchone()
    if not person:
        cursor.close()
        return {}

    # שליפת דיווחים לחודש
    start_ts, end_ts = month_range_ts(year, month)
    cursor.execute("""
        SELECT tr.*, st.name as shift_name,
               a.apartment_type_id,
               p.is_married,
               st.rate as shift_rate,
               st.is_minimum_wage as shift_is_minimum_wage,
               st.wage_percentage as shift_wage_percentage
        FROM time_reports tr
        LEFT JOIN shift_types st ON st.id = tr.shift_type_id
        LEFT JOIN apartments a ON tr.apartment_id = a.id
        LEFT JOIN people p ON tr.person_id = p.id
        WHERE tr.person_id = %s AND tr.date >= %s AND tr.date < %s
        ORDER BY tr.date, tr.start_time
    """, (person_id, start_ts, end_ts))
    reports_raw = cursor.fetchall()

    # החלת נתונים היסטוריים על הדיווחים
    reports = apply_historical_overrides(conn, reports_raw, person_id, year, month)

    # אתחול סיכומים
    monthly_totals = _create_empty_monthly_totals()

    if reports:
        shift_ids = list({r["shift_type_id"] for r in reports if r["shift_type_id"]})
        segments_by_shift = {}
        if shift_ids:
            placeholders = ",".join(["%s"] * len(shift_ids))
            cursor.execute(f"""
                SELECT id, shift_type_id, start_time, end_time, wage_percent, segment_type, order_index
                FROM shift_time_segments
                WHERE shift_type_id IN ({placeholders})
                ORDER BY order_index
            """, tuple(shift_ids))
            for s in cursor.fetchall():
                segments_by_shift.setdefault(s["shift_type_id"], []).append(dict(s))

        shift_has_standby = {sid: any(s["segment_type"] == "standby" for s in segs)
                             for sid, segs in segments_by_shift.items()}

        daily_map = _build_daily_map(reports, segments_by_shift, year, month)

        monthly_totals["standby"] = _count_standby_dates(reports, shift_has_standby)

        def get_standby_rate_from_db(seg_id: int, apt_type: Optional[int], is_married: bool) -> float:
            return get_standby_rate(conn, seg_id, apt_type, is_married, year, month)

        totals, work_days_set, vacation_days_set = _process_daily_map(
            daily_map, shabbat_cache, get_standby_rate_from_db, year, month
        )

        for key in ["calc100", "calc125", "calc150", "calc175", "calc200",
                    "calc150_shabbat", "calc150_overtime", "calc150_shabbat_100",
                    "calc150_shabbat_50", "total_hours", "standby_payment", "vacation_minutes"]:
            monthly_totals[key] = totals[key]

        monthly_totals["actual_work_days"] = len(work_days_set)
        monthly_totals["vacation_days_taken"] = len(vacation_days_set)
        monthly_totals["vacation_payment"] = (monthly_totals.get("vacation_minutes", 0) / 60) * minimum_wage

        variable_rate_by_shift = _build_variable_rate_map(reports)
        variable_rate_minutes, variable_rate_extra_payment = _calculate_variable_rate_payment(
            daily_map, variable_rate_by_shift, minimum_wage
        )
        monthly_totals["calc_variable"] = variable_rate_minutes
        monthly_totals["variable_rate_extra_payment"] = variable_rate_extra_payment

    # שליפת רכיבי תשלום נוספים
    month_start = datetime(year, month, 1, tzinfo=LOCAL_TZ)
    month_end = datetime(year + 1, 1, 1, tzinfo=LOCAL_TZ) if month == 12 else datetime(year, month + 1, 1, tzinfo=LOCAL_TZ)

    cursor.execute("""
        SELECT (quantity * rate) as total_amount, component_type_id FROM payment_components
        WHERE person_id = %s AND date >= %s AND date < %s
    """, (person_id, month_start, month_end))
    payment_comps = cursor.fetchall()

    _process_payment_components(payment_comps, monthly_totals)

    cursor.close()

    # חישוב צבירות
    accruals = calculate_accruals(
        actual_work_days=monthly_totals["actual_work_days"],
        start_date_ts=person["start_date"],
        report_year=year,
        report_month=month
    )
    monthly_totals["sick_days_accrued"] = accruals["sick_days_accrued"]
    monthly_totals["vacation_days_accrued"] = accruals["vacation_days_accrued"]
    monthly_totals["vacation_details"] = accruals.get("vacation_details", _get_default_vacation_details())

    # חישוב תשלום סופי
    _calculate_final_payment(monthly_totals, minimum_wage)

    monthly_totals["vacation"] = monthly_totals["vacation_minutes"]

    return monthly_totals


def _calculate_totals_from_data(
    person,
    reports,
    segments_by_shift,
    shift_has_standby,
    payment_comps,
    standby_rates_cache,
    shabbat_cache,
    minimum_wage,
    year,
    month
) -> Dict:
    """
    Helper for calculating totals from pre-fetched data.
    Uses shared helper functions to avoid code duplication.
    """
    from utils.utils import calculate_accruals

    monthly_totals = _create_empty_monthly_totals()

    if reports:
        monthly_totals["standby"] = _count_standby_dates(reports, shift_has_standby)

        daily_map = _build_daily_map(reports, segments_by_shift, year, month)

        def get_standby_rate_from_cache(seg_id: int, apt_type: Optional[int], is_married: bool) -> float:
            marital_status = "married" if is_married else "single"
            rate = DEFAULT_STANDBY_RATE

            if apt_type is not None:
                val = standby_rates_cache.get((seg_id, apt_type, marital_status, 10))
                if val is not None:
                    return val

            val = standby_rates_cache.get((seg_id, None, marital_status, 0))
            if val is not None:
                return val

            return rate

        totals, work_days_set, vacation_days_set = _process_daily_map(
            daily_map, shabbat_cache, get_standby_rate_from_cache, year, month
        )

        for key in ["calc100", "calc125", "calc150", "calc175", "calc200",
                    "calc150_shabbat", "calc150_overtime", "calc150_shabbat_100",
                    "calc150_shabbat_50", "total_hours", "standby_payment", "vacation_minutes"]:
            monthly_totals[key] = totals[key]

        variable_rate_by_shift = _build_variable_rate_map(reports)
        variable_rate_minutes, variable_rate_extra_payment = _calculate_variable_rate_payment(
            daily_map, variable_rate_by_shift, minimum_wage
        )
        monthly_totals["calc_variable"] = variable_rate_minutes
        monthly_totals["variable_rate_extra_payment"] = variable_rate_extra_payment

        monthly_totals["actual_work_days"] = len(work_days_set)
        monthly_totals["vacation_days_taken"] = len(vacation_days_set)
        monthly_totals["vacation_payment"] = (monthly_totals.get("vacation_minutes", 0) / 60) * minimum_wage

    _process_payment_components(payment_comps, monthly_totals)

    accruals = calculate_accruals(
        actual_work_days=monthly_totals["actual_work_days"],
        start_date_ts=person["start_date"],
        report_year=year,
        report_month=month
    )
    monthly_totals["sick_days_accrued"] = accruals["sick_days_accrued"]
    monthly_totals["vacation_days_accrued"] = accruals["vacation_days_accrued"]
    monthly_totals["vacation_details"] = accruals.get("vacation_details", _get_default_vacation_details())

    _calculate_final_payment(monthly_totals, minimum_wage)

    monthly_totals["vacation"] = monthly_totals["vacation_minutes"]

    return monthly_totals


def calculate_monthly_summary(conn, year: int, month: int) -> Tuple[List[Dict], Dict]:
    """Calculate monthly summary for all active people."""
    from utils.utils import month_range_ts
    from core.history import (get_person_status_for_month, get_apartment_type_for_month,
                         get_all_shift_rates_for_month, get_minimum_wage_for_month)

    payment_codes = get_payment_codes(conn)

    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute("SELECT id, name, start_date, is_married, meirav_code FROM people WHERE is_active::integer = 1 ORDER BY name")
    people = cursor.fetchall()

    start_ts, end_ts = month_range_ts(year, month)
    cursor.execute("""
        SELECT tr.*, st.name as shift_name,
               st.rate AS shift_rate,
               st.is_minimum_wage AS shift_is_minimum_wage,
               st.wage_percentage AS shift_wage_percentage,
               a.apartment_type_id,
               p.is_married
        FROM time_reports tr
        LEFT JOIN shift_types st ON st.id = tr.shift_type_id
        LEFT JOIN apartments a ON tr.apartment_id = a.id
        LEFT JOIN people p ON tr.person_id = p.id
        WHERE tr.date >= %s AND tr.date < %s
        ORDER BY tr.person_id, tr.date, tr.start_time
    """, (start_ts, end_ts))
    all_reports_raw = cursor.fetchall()

    # Load historical data for overrides
    person_ids = {r["person_id"] for r in all_reports_raw if r["person_id"]}
    person_status_cache = {}
    for pid in person_ids:
        hist = get_person_status_for_month(conn, pid, year, month)
        if hist.get("is_married") is not None:
            person_status_cache[pid] = hist

    apartment_ids = {r["apartment_id"] for r in all_reports_raw if r["apartment_id"]}
    apartment_type_cache = {}
    for apt_id in apartment_ids:
        hist_type = get_apartment_type_for_month(conn, apt_id, year, month)
        if hist_type is not None:
            apartment_type_cache[apt_id] = hist_type

    shift_rates_cache = get_all_shift_rates_for_month(conn, year, month)

    # Apply historical overrides
    reports_by_person = {}
    shift_type_ids = set()
    for r in all_reports_raw:
        r_dict = dict(r)
        pid = r_dict.get("person_id")
        apt_id = r_dict.get("apartment_id")
        shift_type_id = r_dict.get("shift_type_id")

        if pid and pid in person_status_cache:
            hist_married = person_status_cache[pid].get("is_married")
            if hist_married is not None:
                r_dict["is_married"] = hist_married

        rate_apt_type = r_dict.get("rate_apartment_type_id")
        if rate_apt_type:
            r_dict["apartment_type_id"] = rate_apt_type
        elif apt_id and apt_id in apartment_type_cache:
            r_dict["apartment_type_id"] = apartment_type_cache[apt_id]

        if shift_type_id and shift_type_id in shift_rates_cache:
            rate_info = shift_rates_cache[shift_type_id]
            r_dict["shift_rate"] = rate_info.get("rate")
            r_dict["shift_is_minimum_wage"] = rate_info.get("is_minimum_wage")
            r_dict["shift_wage_percentage"] = rate_info.get("wage_percentage")

        reports_by_person.setdefault(pid, []).append(r_dict)
        if shift_type_id:
            shift_type_ids.add(shift_type_id)

    # Shift Segments
    segments_by_shift = {}
    shift_has_standby = {}
    if shift_type_ids:
        placeholders = ",".join(["%s"] * len(shift_type_ids))
        cursor.execute(f"""
            SELECT id, shift_type_id, start_time, end_time, wage_percent, segment_type, order_index
            FROM shift_time_segments
            WHERE shift_type_id IN ({placeholders})
            ORDER BY order_index
        """, tuple(shift_type_ids))
        all_segs = cursor.fetchall()
        for s in all_segs:
            segments_by_shift.setdefault(s["shift_type_id"], []).append(dict(s))

        for sid, segs in segments_by_shift.items():
            shift_has_standby[sid] = any(s["segment_type"] == "standby" for s in segs)

    # Payment Components
    month_start = datetime(year, month, 1, tzinfo=LOCAL_TZ)
    if month == 12:
        month_end = datetime(year + 1, 1, 1, tzinfo=LOCAL_TZ)
    else:
        month_end = datetime(year, month + 1, 1, tzinfo=LOCAL_TZ)

    cursor.execute("""
        SELECT person_id, (quantity * rate) as total_amount, component_type_id
        FROM payment_components
        WHERE date >= %s AND date < %s
    """, (month_start, month_end))
    all_payment_comps = cursor.fetchall()
    payment_comps_by_person = {}
    for pc in all_payment_comps:
        payment_comps_by_person.setdefault(pc["person_id"], []).append(pc)

    # Standby Rates - first check historical, then fallback to current
    standby_rates_cache_local = {}

    cursor.execute("""
        SELECT segment_id, apartment_type_id, marital_status, amount
        FROM standby_rates_history
        WHERE year = %s AND month = %s
    """, (year, month))
    historical_rates = cursor.fetchall()

    if historical_rates:
        for row in historical_rates:
            priority = 10 if row["apartment_type_id"] is not None else 0
            key = (row["segment_id"], row["apartment_type_id"], row["marital_status"], priority)
            standby_rates_cache_local[key] = float(row["amount"]) / 100
    else:
        cursor.execute("SELECT * FROM standby_rates")
        all_standby_rates = cursor.fetchall()
        for row in all_standby_rates:
            key = (row["segment_id"], row["apartment_type_id"], row["marital_status"], row["priority"])
            standby_rates_cache_local[key] = float(row["amount"]) / 100

    shabbat_cache = get_shabbat_times_cache(conn)
    minimum_wage = get_minimum_wage_for_month(conn, year, month)

    cursor.close()

    summary_data = []
    grand_totals = {code["internal_key"]: 0 for code in payment_codes}
    grand_totals.update({
        "payment": 0, "standby_payment": 0, "travel": 0, "extras": 0, "total_payment": 0,
        "calc150_shabbat_100": 0, "calc150_shabbat_50": 0,
        "vacation_payment": 0, "vacation_minutes": 0
    })

    for p in people:
        pid = p["id"]
        monthly_totals = _calculate_totals_from_data(
            person=p,
            reports=reports_by_person.get(pid, []),
            segments_by_shift=segments_by_shift,
            shift_has_standby=shift_has_standby,
            payment_comps=payment_comps_by_person.get(pid, []),
            standby_rates_cache=standby_rates_cache_local,
            shabbat_cache=shabbat_cache,
            minimum_wage=minimum_wage,
            year=year,
            month=month
        )

        if monthly_totals.get("total_payment", 0) > 0 or monthly_totals.get("total_hours", 0) > 0:
            summary_data.append({"name": p["name"], "person_id": p["id"], "merav_code": p["meirav_code"], "totals": monthly_totals})

            grand_totals["payment"] += monthly_totals.get("total_payment", 0)
            grand_totals["total_payment"] += monthly_totals.get("total_payment", 0)

            for k, v in monthly_totals.items():
                if k in grand_totals and isinstance(v, (int, float)) and k not in ("payment", "total_payment"):
                    grand_totals[k] += v

    return summary_data, grand_totals
