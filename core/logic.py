"""
Core business logic for DiyurCalc application.
Contains public API functions for calculating monthly totals and summaries.

Import directly from submodules for specific functionality:
- core.time_utils: Time conversion and Shabbat detection
- app_utils: Wage calculation (single source of truth)
- core.constants: Shift IDs and constants
"""
import logging
import psycopg2
import psycopg2.extras
from typing import List, Tuple, Dict, Any, Optional

from utils.cache_manager import cached
from core.time_utils import get_shabbat_times_cache
from core.database import get_housing_array_filter

# =============================================================================
# Configure logging
# =============================================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# Data Access Functions (with caching)
# =============================================================================


@cached(ttl=1800)  # Cache for 30 minutes
def get_active_guides(housing_array_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    שליפת מדריכים פעילים.

    Args:
        housing_array_id: מזהה מערך דיור לסינון. אם None - מחזיר את כל המדריכים.

    Returns:
        רשימת מדריכים פעילים.
    """
    from core.database import get_pooled_connection, return_connection
    conn = get_pooled_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        if housing_array_id is not None:
            # סינון מדריכים לפי מערך דיור שלהם
            cursor.execute(
                """
                SELECT id, name, type, is_active, start_date
                FROM people
                WHERE is_active::integer = 1
                  AND housing_array_id = %s
                ORDER BY name
                """,
                (housing_array_id,)
            )
        else:
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
    """Fetch distinct months for a specific person efficiently using SQL.

    כולל חודשים עם משמרות (time_reports) או רכיבי תשלום (payment_components).
    מסנן לפי מערך דיור אם הוגדר פילטר.
    """
    cursor = conn.cursor()
    housing_filter = get_housing_array_filter()

    try:
        if housing_filter is not None:
            # סינון לפי מערך דיור
            cursor.execute("""
                SELECT DISTINCT year, month FROM (
                    SELECT
                        CAST(EXTRACT(YEAR FROM tr.date) AS INTEGER) as year,
                        CAST(EXTRACT(MONTH FROM tr.date) AS INTEGER) as month
                    FROM time_reports tr
                    JOIN apartments ap ON ap.id = tr.apartment_id
                    WHERE tr.person_id = %s AND ap.housing_array_id = %s
                    UNION
                    SELECT
                        CAST(EXTRACT(YEAR FROM pc.date) AS INTEGER) as year,
                        CAST(EXTRACT(MONTH FROM pc.date) AS INTEGER) as month
                    FROM payment_components pc
                    JOIN apartments ap ON ap.id = pc.apartment_id
                    WHERE pc.person_id = %s AND ap.housing_array_id = %s
                ) combined
                ORDER BY year DESC, month DESC
            """, (person_id, housing_filter, person_id, housing_filter))
        else:
            # ללא סינון
            cursor.execute("""
                SELECT DISTINCT year, month FROM (
                    SELECT
                        CAST(EXTRACT(YEAR FROM date) AS INTEGER) as year,
                        CAST(EXTRACT(MONTH FROM date) AS INTEGER) as month
                    FROM time_reports
                    WHERE person_id = %s
                    UNION
                    SELECT
                        CAST(EXTRACT(YEAR FROM date) AS INTEGER) as year,
                        CAST(EXTRACT(MONTH FROM date) AS INTEGER) as month
                    FROM payment_components
                    WHERE person_id = %s
                ) combined
                ORDER BY year DESC, month DESC
            """, (person_id, person_id))
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

    Optimized with bulk loading: all data is loaded in a few queries instead of per-person.
    """
    from core.history import (
        get_minimum_wage_for_month,
        get_all_person_statuses_for_month,
        get_all_apartment_types_for_month,
        get_all_housing_rates_for_month,
    )
    from core.database import PostgresConnection
    from app_utils import get_daily_segments_data, aggregate_daily_segments_to_monthly
    from utils.utils import month_range_ts

    payment_codes = get_payment_codes(conn)

    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute("SELECT id, name, start_date, is_married, meirav_code FROM people WHERE is_active::integer = 1 ORDER BY name")
    people = cursor.fetchall()
    cursor.close()

    shabbat_cache = get_shabbat_times_cache(conn)
    minimum_wage = get_minimum_wage_for_month(conn, year, month)

    # Wrap the raw psycopg2 connection in PostgresConnection for app_utils compatibility
    conn_wrapper = PostgresConnection(conn, use_pool=False)

    # Pre-load all caches ONCE for the entire month (optimization)
    person_ids = [p["id"] for p in people]
    start_dt, end_dt = month_range_ts(year, month)
    start_date = start_dt.date()
    end_date = end_dt.date()
    housing_filter = get_housing_array_filter()

    person_status_cache = get_all_person_statuses_for_month(conn, person_ids, year, month)

    # ============================================================
    # BULK LOADING OPTIMIZATION - Load all data in single queries
    # ============================================================

    # 1. Load ALL time_reports for all people at once
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    if housing_filter is not None:
        cursor.execute("""
            SELECT tr.*,
                   st.name AS shift_name,
                   st.color AS shift_color,
                   st.for_friday_eve,
                   st.for_shabbat_holiday,
                   st.is_special_hourly AS shift_is_special_hourly,
                   ap.name AS apartment_name,
                   ap.apartment_type_id,
                   ap.housing_array_id,
                   at.hourly_wage_supplement,
                   at.name AS apartment_type_name,
                   ha.name AS housing_array_name,
                   p.is_married,
                   p.name as person_name
            FROM time_reports tr
            LEFT JOIN shift_types st ON st.id = tr.shift_type_id
            JOIN apartments ap ON ap.id = tr.apartment_id
            LEFT JOIN apartment_types at ON at.id = ap.apartment_type_id
            LEFT JOIN housing_arrays ha ON ha.id = ap.housing_array_id
            LEFT JOIN people p ON p.id = tr.person_id
            WHERE tr.person_id = ANY(%s) AND tr.date >= %s AND tr.date < %s
              AND ap.housing_array_id = %s
            ORDER BY tr.person_id, tr.date, tr.start_time
        """, (person_ids, start_date, end_date, housing_filter))
    else:
        cursor.execute("""
            SELECT tr.*,
                   st.name AS shift_name,
                   st.color AS shift_color,
                   st.for_friday_eve,
                   st.for_shabbat_holiday,
                   st.is_special_hourly AS shift_is_special_hourly,
                   ap.name AS apartment_name,
                   ap.apartment_type_id,
                   ap.housing_array_id,
                   at.hourly_wage_supplement,
                   at.name AS apartment_type_name,
                   ha.name AS housing_array_name,
                   p.is_married,
                   p.name as person_name
            FROM time_reports tr
            LEFT JOIN shift_types st ON st.id = tr.shift_type_id
            LEFT JOIN apartments ap ON ap.id = tr.apartment_id
            LEFT JOIN apartment_types at ON at.id = ap.apartment_type_id
            LEFT JOIN housing_arrays ha ON ha.id = ap.housing_array_id
            LEFT JOIN people p ON p.id = tr.person_id
            WHERE tr.person_id = ANY(%s) AND tr.date >= %s AND tr.date < %s
            ORDER BY tr.person_id, tr.date, tr.start_time
        """, (person_ids, start_date, end_date))
    all_reports = cursor.fetchall()

    # Group reports by person_id
    reports_by_person = {}
    all_shift_ids = set()
    all_apartment_ids = set()
    for r in all_reports:
        reports_by_person.setdefault(r["person_id"], []).append(r)
        if r["shift_type_id"]:
            all_shift_ids.add(r["shift_type_id"])
        if r["apartment_id"]:
            all_apartment_ids.add(r["apartment_id"])

    # 2. Load ALL shift_time_segments for all used shifts
    segments_by_shift = {}
    if all_shift_ids:
        placeholders = ",".join(["%s"] * len(all_shift_ids))
        cursor.execute(f"""
            SELECT seg.*, st.name AS shift_name
            FROM shift_time_segments seg
            JOIN shift_types st ON st.id = seg.shift_type_id
            WHERE seg.shift_type_id IN ({placeholders})
            ORDER BY seg.shift_type_id, seg.order_index, seg.id
        """, tuple(all_shift_ids))
        for seg in cursor.fetchall():
            segments_by_shift.setdefault(seg["shift_type_id"], []).append(seg)

    # 3. Load ALL payment_components for all people at once
    month_start = start_dt
    month_end = end_dt
    if housing_filter is not None:
        cursor.execute("""
            SELECT pc.person_id, (pc.quantity * pc.rate) as total_amount, pc.component_type_id
            FROM payment_components pc
            JOIN apartments ap ON ap.id = pc.apartment_id
            WHERE pc.person_id = ANY(%s) AND pc.date >= %s AND pc.date < %s
              AND ap.housing_array_id = %s
        """, (person_ids, month_start, month_end, housing_filter))
    else:
        cursor.execute("""
            SELECT person_id, (quantity * rate) as total_amount, component_type_id
            FROM payment_components
            WHERE person_id = ANY(%s) AND date >= %s AND date < %s
        """, (person_ids, month_start, month_end))
    all_payment_comps = cursor.fetchall()

    # Group payment_components by person_id
    payment_comps_by_person = {}
    for pc in all_payment_comps:
        payment_comps_by_person.setdefault(pc["person_id"], []).append(pc)

    cursor.close()

    # 4. Load apartment type cache and housing rates cache
    apartment_type_cache = get_all_apartment_types_for_month(conn, list(all_apartment_ids), year, month)
    housing_rates_cache = get_all_housing_rates_for_month(conn, year, month)

    # Build person start_date map (already have this data from people query)
    person_start_dates = {p["id"]: p["start_date"] for p in people}

    # ============================================================
    # END BULK LOADING - Now process each person with cached data
    # ============================================================

    summary_data = []
    grand_totals = {code["internal_key"]: 0 for code in payment_codes}
    grand_totals.update({
        "payment": 0, "standby_payment": 0, "travel": 0, "extras": 0, "total_payment": 0,
        "calc150_shabbat_100": 0, "calc150_shabbat_50": 0,
        "vacation_payment": 0, "vacation_minutes": 0,
        "sick_payment": 0, "sick_minutes": 0,  # מחלה
        "rounded_total": 0  # סה"כ מעוגל - סכום השורות עם עיגול
    })

    for p in people:
        pid = p["id"]

        # Use the unified calculation from app_utils with ALL pre-loaded data
        daily_segments, _ = get_daily_segments_data(
            conn_wrapper, pid, year, month, shabbat_cache, minimum_wage,
            person_status_cache=person_status_cache,
            apartment_type_cache=apartment_type_cache,
            housing_rates_cache=housing_rates_cache,
            preloaded_reports=reports_by_person.get(pid, []),
            preloaded_segments=segments_by_shift
        )

        monthly_totals = aggregate_daily_segments_to_monthly(
            conn_wrapper, daily_segments, pid, year, month, minimum_wage,
            preloaded_payment_comps=payment_comps_by_person.get(pid, []),
            person_start_date=person_start_dates.get(pid)
        )

        # הצג מדריכים עם שעות עבודה או תשלום כלשהו
        # (כשיש סינון לפי מערך דיור, גם השעות וגם רכיבי התשלום כבר מסוננים)
        should_include = monthly_totals.get("total_payment", 0) > 0 or monthly_totals.get("total_hours", 0) > 0

        if should_include:
            summary_data.append({"name": p["name"], "person_id": p["id"], "merav_code": p["meirav_code"], "totals": monthly_totals})

            grand_totals["payment"] += monthly_totals.get("payment", 0)
            grand_totals["total_payment"] += monthly_totals.get("total_payment", 0)
            grand_totals["rounded_total"] += monthly_totals.get("rounded_total", 0)

            for k, v in monthly_totals.items():
                if k in grand_totals and isinstance(v, (int, float)) and k not in ("payment", "total_payment", "rounded_total"):
                    grand_totals[k] += v

    return summary_data, grand_totals
