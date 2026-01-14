"""
Guide routes for DiyurCalc application.
Contains routes for viewing guide details and summaries.
"""
from __future__ import annotations

import time
import logging
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any

from fastapi import Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from core.config import config
from core.database import get_conn
from core.logic import (
    get_shabbat_times_cache,
    get_payment_codes,
    get_available_months_for_person,
    calculate_person_monthly_totals,
)
from core.history import get_minimum_wage_for_month
from core.app_utils import get_daily_segments_data
from utils.utils import human_date, format_currency, month_range_ts
import psycopg2.extras

logger = logging.getLogger(__name__)
templates = Jinja2Templates(directory=str(config.TEMPLATES_DIR))
templates.env.filters["human_date"] = human_date
templates.env.filters["format_currency"] = format_currency
templates.env.globals["app_version"] = config.VERSION


def simple_summary_view(
    request: Request,
    person_id: int,
    month: Optional[int] = None,
    year: Optional[int] = None
) -> HTMLResponse:
    """Simple summary view for a guide."""
    start_time = time.time()
    logger.info(f"Starting simple_summary_view for person_id={person_id}, {month}/{year}")

    conn_start = time.time()
    with get_conn() as conn:
        conn_time = time.time() - conn_start
        logger.info(f"Database connection took: {conn_time:.4f}s")
        # Defaults
        if month is None or year is None:
            now = datetime.now(config.LOCAL_TZ)
            year, month = now.year, now.month

        # Minimum Wage (historical - for the selected month)
        wage_start = time.time()
        minimum_wage = get_minimum_wage_for_month(conn.conn, year, month)
        logger.info(f"get_minimum_wage_for_month took: {time.time() - wage_start:.4f}s, value={minimum_wage} for {year}/{month}")

        shabbat_start = time.time()
        shabbat_cache = get_shabbat_times_cache(conn.conn)
        logger.info(f"get_shabbat_times_cache took: {time.time() - shabbat_start:.4f}s")

        # Get data
        segments_start = time.time()
        daily_segments, person_name = get_daily_segments_data(conn, person_id, year, month, shabbat_cache, minimum_wage)
        logger.info(f"get_daily_segments_data took: {time.time() - segments_start:.4f}s")

        person = conn.execute("SELECT * FROM people WHERE id = %s", (person_id,)).fetchone()

        # Aggregate
        summary = {
            "weekday": {"count": 0, "payment": 0},
            "friday": {"count": 0, "payment": 0},
            "saturday": {"count": 0, "payment": 0},
            "overtime": {"hours": 0, "payment": 0},
            "total_payment": 0
        }

        for day in daily_segments:
            # Skip if no work/vacation/sick (just empty day)
            if not day.get("payment") and not day.get("has_work"):
                continue

            # Determine type
            # weekday() 0-3=Sun-Wed, 4=Thu(Wait.. Mon=0..Sun=6)
            # Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6
            wd = day["date_obj"].weekday()

            # Sun(6), Mon(0)-Thu(3) -> Weekday
            is_weekday = (wd == 6 or wd <= 3)
            is_friday = (wd == 4)
            is_saturday = (wd == 5)

            day_payment = day["payment"] or 0

            # Calculate Overtime part (125% + 150% non-shabbat)
            overtime_hours = 0
            overtime_payment = 0

            for seg in day["segments"]:
                rate = seg.get("rate", 100)
                if rate > 100 and not seg.get("is_shabbat", False):
                    overtime_hours += seg["hours"]
                    overtime_payment += seg["payment"]

            # Accumulate
            if is_weekday:
                summary["weekday"]["count"] += 1
                summary["weekday"]["payment"] += day_payment
            elif is_friday:
                summary["friday"]["count"] += 1
                summary["friday"]["payment"] += day_payment
            elif is_saturday:
                summary["saturday"]["count"] += 1
                summary["saturday"]["payment"] += day_payment

            summary["overtime"]["hours"] += overtime_hours
            summary["overtime"]["payment"] += overtime_payment
            summary["total_payment"] += day_payment

    render_start = time.time()
    response = templates.TemplateResponse(
        "simple_summary.html",
        {
            "request": request,
            "person": person,
            "summary": summary,
            "year": year,
            "month": month,
            "person_name": person_name,
        },
    )
    render_time = time.time() - render_start
    logger.info(f"Template rendering took: {render_time:.4f}s")

    total_time = time.time() - start_time
    logger.info(f"Total simple_summary_view execution time: {total_time:.4f}s")

    return response


def guide_view(
    request: Request,
    person_id: int,
    month: Optional[int] = None,
    year: Optional[int] = None
) -> HTMLResponse:
    """Detailed guide view with full monthly report."""
    func_start_time = time.time()
    logger.info(f"Starting guide_view for person_id={person_id}, {month}/{year}")

    conn_start = time.time()
    with get_conn() as conn:
        conn_time = time.time() - conn_start
        logger.info(f"Database connection took: {conn_time:.4f}s")

        # שכר מינימום יישלף בהמשך לפי החודש הנבחר

        person = conn.execute(
            """
            SELECT p.id, p.name, p.phone, p.email, p.type, p.is_active, p.start_date, p.meirav_code, 
                   e.code as employer_code, e.name as employer_name
            FROM people p
            LEFT JOIN employers e ON p.employer_id = e.id
            WHERE p.id = %s
            """,
            (person_id,),
        ).fetchone()
        if not person:
            raise HTTPException(status_code=404, detail="מדריך לא נמצא")

        # Fetch payment codes early to avoid connection issues later
        payment_start = time.time()
        payment_codes = get_payment_codes(conn.conn)
        logger.info(f"get_payment_codes took: {time.time() - payment_start:.4f}s")
        if not payment_codes:
            # Try once more with a fresh connection if first fetch failed
            try:
                with get_conn() as temp_conn:
                    payment_codes = get_payment_codes(temp_conn.conn)
            except Exception as e:
                logger.warning(f"Secondary fetch of payment codes failed: {e}")

        # Optimized: Fetch available months
        months_start = time.time()
        months = get_available_months_for_person(conn.conn, person_id)
        logger.info(f"get_available_months_for_person took: {time.time() - months_start:.4f}s")

        # Prepare months options for template
        months_options = [{"year": y, "month": m, "label": f"{m:02d}/{y}"} for y, m in months]

        if not months:
            selected_year, selected_month = year or datetime.now().year, month or datetime.now().month
            # שליפת שכר מינימום לפי החודש הנבחר
            MINIMUM_WAGE = get_minimum_wage_for_month(conn.conn, selected_year, selected_month)
            month_reports = []
            shift_segments = []
            daily_segments = []
            monthly_totals = {
                "total_hours": 0.0,
                "calc100": 0.0,
                "calc125": 0.0,
                "calc150": 0.0,
                "calc150_shabbat": 0.0,
                "calc150_shabbat_100": 0.0,
                "calc150_shabbat_50": 0.0,
                "calc150_overtime": 0.0,
                "calc175": 0.0,
                "calc200": 0.0,
                "vacation_minutes": 0.0,
                "vacation_payment": 0.0,
                "travel": 0.0,
                "extras": 0.0,
                "sick_days_accrued": 0.0,
                "vacation_days_accrued": 0.0,
                "payment": 0.0,
                "actual_work_days": 0.0,
                "vacation_days_taken": 0.0,
                "standby": 0.0,
                "standby_payment": 0.0,
            }
        else:
            # Select month/year
            if month is None or year is None:
                selected_year, selected_month = months[-1]
            else:
                selected_year, selected_month = year, month

            # שליפת שכר מינימום לפי החודש הנבחר
            wage_start = time.time()
            MINIMUM_WAGE = get_minimum_wage_for_month(conn.conn, selected_year, selected_month)
            logger.info(f"get_minimum_wage_for_month took: {time.time() - wage_start:.4f}s, value={MINIMUM_WAGE} for {selected_year}/{selected_month}")

            # Get monthly data
            shabbat_start = time.time()
            shabbat_cache = get_shabbat_times_cache(conn.conn)
            logger.info(f"get_shabbat_times_cache took: {time.time() - shabbat_start:.4f}s")

            segments_calc_start = time.time()
            daily_segments, person_name = get_daily_segments_data(
                conn, person_id, selected_year, selected_month, shabbat_cache, MINIMUM_WAGE
            )
            logger.info(f"get_daily_segments_data took: {time.time() - segments_calc_start:.4f}s")

            totals_start = time.time()
            monthly_totals = calculate_person_monthly_totals(
                conn.conn, person_id, selected_year, selected_month, shabbat_cache, MINIMUM_WAGE
            )
            logger.info(f"calculate_person_monthly_totals took: {time.time() - totals_start:.4f}s")

            # Recalculate payment from daily_segments to use custom shift rates
            # (daily_segments uses get_effective_hourly_rate which handles custom shift rates)
            total_payment_from_segments = sum(day.get("payment", 0) for day in daily_segments)
            total_standby_from_segments = sum(day.get("standby_payment", 0) for day in daily_segments)

            # Calculate payment breakdown by rate category from chains
            # Sum minutes and payment for each category separately
            # Also track variable rate hours (non-minimum wage) separately
            minutes_by_rate = {"calc100": 0, "calc125": 0, "calc150": 0, "calc175": 0, "calc200": 0}
            payment_by_rate = {"calc100": 0.0, "calc125": 0.0, "calc150": 0.0, "calc175": 0.0, "calc200": 0.0}

            # Track variable rate hours separately (shifts with custom rate != minimum wage)
            variable_rate_minutes = 0
            variable_rate_payment = 0.0
            variable_rate_value = None  # The actual rate used

            for day in daily_segments:
                for chain in day.get("chains", []):
                    if chain.get("type") == "work":
                        effective_rate = chain.get("effective_rate", MINIMUM_WAGE)
                        is_variable_rate = abs(effective_rate - MINIMUM_WAGE) > 0.01  # Not minimum wage

                        # Sum minutes and calculate payment for each rate category
                        for rate_key in ["calc100", "calc125", "calc150", "calc175", "calc200"]:
                            mins = chain.get(rate_key, 0)
                            if mins > 0:
                                multiplier = {"calc100": 1.0, "calc125": 1.25, "calc150": 1.5, "calc175": 1.75, "calc200": 2.0}[rate_key]
                                payment = (mins / 60) * multiplier * effective_rate

                                if is_variable_rate:
                                    # Track separately for variable rate
                                    variable_rate_minutes += mins
                                    variable_rate_payment += payment
                                    variable_rate_value = effective_rate
                                else:
                                    # Regular minimum wage hours
                                    minutes_by_rate[rate_key] += mins
                                    payment_by_rate[rate_key] += payment

            # Store calculated payments in monthly_totals for template use
            monthly_totals["payment_calc100"] = payment_by_rate["calc100"]
            monthly_totals["payment_calc125"] = payment_by_rate["calc125"]
            monthly_totals["payment_calc175"] = payment_by_rate["calc175"]
            monthly_totals["payment_calc200"] = payment_by_rate["calc200"]

            # For calc150, we need to recalculate payment including tagbur
            # monthly_totals["calc150_overtime"] and ["calc150_shabbat"] include tagbur from logic.py
            # So we calculate payment from these values directly
            calc150_overtime_minutes = monthly_totals.get("calc150_overtime", 0)
            calc150_shabbat_minutes = monthly_totals.get("calc150_shabbat", 0)
            
            # Calculate payment for overtime (150% at minimum wage) - includes tagbur
            payment_calc150_overtime = (calc150_overtime_minutes / 60) * MINIMUM_WAGE * 1.5
            
            # Calculate payment for shabbat (150% at minimum wage) - includes tagbur
            payment_calc150_shabbat = (calc150_shabbat_minutes / 60) * MINIMUM_WAGE * 1.5
            
            # Total calc150 payment = overtime + shabbat (both include tagbur)
            monthly_totals["payment_calc150"] = payment_calc150_overtime + payment_calc150_shabbat
            
            # Store split payments for template
            monthly_totals["payment_calc150_overtime"] = payment_calc150_overtime
            monthly_totals["payment_calc150_shabbat"] = payment_calc150_shabbat

            # Override the minutes values to exclude variable rate hours
            # This prevents double-counting in the payment table
            # BUT: Keep calc150, calc150_overtime, calc150_shabbat from monthly_totals (they include tagbur)
            monthly_totals["calc100"] = minutes_by_rate["calc100"]
            monthly_totals["calc125"] = minutes_by_rate["calc125"]
            # Don't override calc150, calc150_overtime, calc150_shabbat - they include tagbur from monthly_totals
            monthly_totals["calc175"] = minutes_by_rate["calc175"]
            monthly_totals["calc200"] = minutes_by_rate["calc200"]

            # Calculate total_hours from daily_segments (sum of all work minutes, excluding standby)
            # This ensures the display matches the actual calculated segments
            total_work_minutes = sum(
                day["total_minutes_no_standby"] for day in daily_segments
            )
            monthly_totals["total_hours"] = total_work_minutes

            # Store variable rate hours data
            monthly_totals["calc_variable"] = variable_rate_minutes  # Minutes at variable rate
            monthly_totals["payment_calc_variable"] = variable_rate_payment
            monthly_totals["variable_rate_value"] = variable_rate_value or MINIMUM_WAGE

            # Calculate effective hourly rate for regular hours (minimum wage)
            effective_hourly_rate = MINIMUM_WAGE

            # Override monthly_totals payment with correctly calculated values
            # Note: Don't include travel/extras here - the template adds them where needed
            monthly_totals["payment"] = total_payment_from_segments + total_standby_from_segments
            monthly_totals["standby_payment"] = total_standby_from_segments
            monthly_totals["effective_hourly_rate"] = effective_hourly_rate

            # Get raw reports for the template
            start_dt, end_dt = month_range_ts(selected_year, selected_month)
            # Convert datetime to date for PostgreSQL date column
            start_date = start_dt.date()
            end_date = end_dt.date()
            month_reports = conn.execute("""
                SELECT tr.*, st.name as shift_name,
                       a.apartment_type_id, a.name as apartment_name,
                       p.is_married
                FROM time_reports tr
                LEFT JOIN shift_types st ON st.id = tr.shift_type_id
                LEFT JOIN apartments a ON tr.apartment_id = a.id
                LEFT JOIN people p ON tr.person_id = p.id
                WHERE tr.person_id = %s AND tr.date >= %s AND tr.date < %s
                ORDER BY tr.date, tr.start_time
            """, (person_id, start_date, end_date)).fetchall()

            # Pre-load all shift segments in one query (avoid N+1)
            shift_type_ids = {r['shift_type_id'] for r in month_reports if r['shift_type_id']}
            segments_by_shift = {}
            if shift_type_ids:
                placeholders = ",".join(["%s"] * len(shift_type_ids))
                cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                cursor.execute(f"""
                    SELECT shift_type_id, start_time, end_time, wage_percent, segment_type
                    FROM shift_time_segments
                    WHERE shift_type_id IN ({placeholders})
                    ORDER BY order_index
                """, tuple(shift_type_ids))
                for seg in cursor.fetchall():
                    segments_by_shift.setdefault(seg['shift_type_id'], []).append(seg)
                cursor.close()

            # Get shift segments - just the raw reports without payment calculation
            shift_segments = [{"report": report} for report in month_reports]

    # Calculate total standby count
    total_standby_count = monthly_totals.get("standby", 0)

    # Get unique years for dropdown
    years = sorted(set(m["year"] for m in months_options), reverse=True) if months_options else [selected_year]

    # Build simple_summary for "old calculation" tab - based on shift_name from reports
    standby_payment_total = monthly_totals.get('standby_payment', 0) or 0
    travel_payment = monthly_totals.get('travel', 0) or 0
    extras_payment = monthly_totals.get('extras', 0) or 0
    simple_summary = {
        "night": {"count": 0, "payment": 0},      # משמרת לילה
        "weekday": {"count": 0, "payment": 0},    # משמרת חול
        "friday": {"count": 0, "payment": 0},     # משמרת שישי/ערב חג
        "saturday": {"count": 0, "payment": 0},   # משמרת שבת/חג
        "hours": {"count": 0, "payment": 0},      # שעת עבודה
        "standby": {
            "count": total_standby_count,
            "payment_per": standby_payment_total / total_standby_count if total_standby_count > 0 else 0,
            "payment_total": standby_payment_total
        },
        "travel": travel_payment,
        "extras": extras_payment
    }

    # Sum by shift_name from shift_segments (which has the calculated payments)
    for seg in shift_segments:
        report = seg.get('report', {})
        shift_name = report.get('shift_name', '') or ''
        payment = seg.get('payment', 0) or 0  # Use calculated payment from shift_segments

        if 'לילה' in shift_name:
            simple_summary["night"]["count"] += 1
            simple_summary["night"]["payment"] += payment
        elif 'שישי' in shift_name or 'ערב חג' in shift_name:
            simple_summary["friday"]["count"] += 1
            simple_summary["friday"]["payment"] += payment
        elif ('שבת' in shift_name or 'חג' in shift_name) and 'שישי' not in shift_name and 'ערב' not in shift_name:
            simple_summary["saturday"]["count"] += 1
            simple_summary["saturday"]["payment"] += payment
        elif 'שעת עבודה' in shift_name or 'שעה' in shift_name:
            simple_summary["hours"]["count"] += 1
            simple_summary["hours"]["payment"] += payment
        elif 'חול' in shift_name:
            simple_summary["weekday"]["count"] += 1
            simple_summary["weekday"]["payment"] += payment

    render_start = time.time()
    response = templates.TemplateResponse(
        "guide.html",
        {
            "request": request,
            "person": person,
            "months": months_options,
            "years": years,
            "selected_year": selected_year,
            "selected_month": selected_month,
            "reports": month_reports,
            "month_reports": month_reports,
            "shift_segments": shift_segments,
            "daily_segments": daily_segments,
            "monthly_totals": monthly_totals,
            "payment_codes": payment_codes or {},
            "minimum_wage": MINIMUM_WAGE,
            "total_standby_count": total_standby_count,
            "simple_summary": simple_summary,
        },
    )
    render_time = time.time() - render_start
    logger.info(f"Template rendering took: {render_time:.4f}s")

    total_time = time.time() - func_start_time
    logger.info(f"Total guide_view execution time: {total_time:.4f}s")

    return response