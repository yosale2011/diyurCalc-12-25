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
from core.time_utils import get_shabbat_times_cache
from core.logic import (
    get_payment_codes,
    get_available_months_for_person,
)
from core.history import get_minimum_wage_for_month
from app_utils import get_daily_segments_data, aggregate_daily_segments_to_monthly
from core.constants import is_implicit_tagbur, FRIDAY_SHIFT_ID, SHABBAT_SHIFT_ID
from utils.utils import month_range_ts, format_currency, human_date

logger = logging.getLogger(__name__)
templates = Jinja2Templates(directory=str(config.TEMPLATES_DIR))
templates.env.filters["format_currency"] = format_currency
templates.env.filters["human_date"] = human_date
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

            # חישוב monthly_totals ממקור אחד - daily_segments
            # זה מחליף את calculate_person_monthly_totals והדריסות הידניות
            totals_start = time.time()
            monthly_totals = aggregate_daily_segments_to_monthly(
                conn, daily_segments, person_id, selected_year, selected_month, MINIMUM_WAGE
            )
            logger.info(f"aggregate_daily_segments_to_monthly took: {time.time() - totals_start:.4f}s")

            # Get raw reports for the template
            start_dt, end_dt = month_range_ts(selected_year, selected_month)
            # Convert datetime to date for PostgreSQL date column
            start_date = start_dt.date()
            end_date = end_dt.date()
            month_reports = conn.execute("""
                SELECT tr.*, st.name as shift_name,
                       a.apartment_type_id, a.name as apartment_name,
                       tr.rate_apartment_type_id,
                       p.is_married
                FROM time_reports tr
                LEFT JOIN shift_types st ON st.id = tr.shift_type_id
                LEFT JOIN apartments a ON tr.apartment_id = a.id
                LEFT JOIN people p ON tr.person_id = p.id
                WHERE tr.person_id = %s AND tr.date >= %s AND tr.date < %s
                ORDER BY tr.date, tr.start_time
            """, (person_id, start_date, end_date)).fetchall()

            # Build shift_segments list for display
            shift_segments = []
            for report in month_reports:

                # בדיקת תגבור משתמע להצגה בטאב משמרות
                shift_id = report.get('shift_type_id')
                actual_apt_type = report.get('apartment_type_id')
                rate_apt_type = report.get('rate_apartment_type_id') or actual_apt_type
                display_shift_name = report.get('shift_name', '')

                if is_implicit_tagbur(shift_id, actual_apt_type, rate_apt_type):
                    if shift_id == FRIDAY_SHIFT_ID:
                        display_shift_name = "משמרת תגבור שישי/ערב חג"
                    elif shift_id == SHABBAT_SHIFT_ID:
                        display_shift_name = "משמרת תגבור שבת/חג"

                shift_segments.append({
                    "report": report,
                    "display_shift_name": display_shift_name,
                })

    # Calculate total standby count
    total_standby_count = monthly_totals.get("standby", 0)

    # Get unique years for dropdown
    years = sorted(set(m["year"] for m in months_options), reverse=True) if months_options else [selected_year]

    # Build simple_summary from daily_segments chains (correct calculation)
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

    # Aggregate payments from chains (correctly calculated values)
    for day in daily_segments:
        day_payment = day.get("payment", 0) or 0
        chains = day.get("chains", [])

        # Determine shift type from chains
        shift_names_in_day = set()
        for chain in chains:
            chain_shift_name = chain.get("shift_name", "") or ""
            if chain_shift_name:
                shift_names_in_day.add(chain_shift_name)

        # Classify by shift name pattern
        shift_name_str = " ".join(shift_names_in_day)
        if 'לילה' in shift_name_str:
            simple_summary["night"]["count"] += 1
            simple_summary["night"]["payment"] += day_payment
        elif 'שישי' in shift_name_str or 'ערב חג' in shift_name_str:
            simple_summary["friday"]["count"] += 1
            simple_summary["friday"]["payment"] += day_payment
        elif ('שבת' in shift_name_str or 'חג' in shift_name_str) and 'שישי' not in shift_name_str and 'ערב' not in shift_name_str:
            simple_summary["saturday"]["count"] += 1
            simple_summary["saturday"]["payment"] += day_payment
        elif 'שעת עבודה' in shift_name_str or 'שעה' in shift_name_str:
            simple_summary["hours"]["count"] += 1
            simple_summary["hours"]["payment"] += day_payment
        elif 'חול' in shift_name_str or day_payment > 0:
            # Default to weekday if has payment but no specific type
            simple_summary["weekday"]["count"] += 1
            simple_summary["weekday"]["payment"] += day_payment

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