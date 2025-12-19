"""
Guide routes for DiyurCalc application.
Contains routes for viewing guide details and summaries.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any

from fastapi import Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from config import config
from database import get_conn
from logic import (
    DEFAULT_MINIMUM_WAGE,
    get_shabbat_times_cache,
    get_payment_codes,
    get_available_months_for_person,
    calculate_person_monthly_totals,
)
from app_utils import get_daily_segments_data
from utils import human_date, format_currency, month_range_ts
import psycopg2.extras

templates = Jinja2Templates(directory=str(config.TEMPLATES_DIR))
templates.env.filters["human_date"] = human_date
templates.env.filters["format_currency"] = format_currency


def simple_summary_view(
    request: Request,
    person_id: int,
    month: Optional[int] = None,
    year: Optional[int] = None
) -> HTMLResponse:
    """Simple summary view for a guide."""
    with get_conn() as conn:
        # Defaults
        if month is None or year is None:
            now = datetime.now(config.LOCAL_TZ)
            year, month = now.year, now.month

        # Minimum Wage
        try:
            row = conn.execute("SELECT hourly_rate FROM minimum_wage_rates ORDER BY effective_from DESC LIMIT 1").fetchone()
            minimum_wage = (float(row["hourly_rate"]) / 100) if row else DEFAULT_MINIMUM_WAGE
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to get minimum wage from DB, using default: {e}")
            minimum_wage = DEFAULT_MINIMUM_WAGE

        shabbat_cache = get_shabbat_times_cache(conn.conn)

        # Get data
        daily_segments, person_name = get_daily_segments_data(conn, person_id, year, month, shabbat_cache, minimum_wage)

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

    return templates.TemplateResponse(
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


def guide_view(
    request: Request,
    person_id: int,
    month: Optional[int] = None,
    year: Optional[int] = None
) -> HTMLResponse:
    """Detailed guide view with full monthly report."""
    with get_conn() as conn:
        # שליפת שכר מינימום מה-DB
        MINIMUM_WAGE = 34.40
        try:
            row = conn.execute("SELECT hourly_rate FROM minimum_wage_rates ORDER BY effective_from DESC LIMIT 1").fetchone()
            if row and row["hourly_rate"]:
                MINIMUM_WAGE = float(row["hourly_rate"]) / 100
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Error fetching minimum wage: {e}")

        person = conn.execute(
            "SELECT id, name, phone, email, type, is_active, start_date FROM people WHERE id = %s",
            (person_id,),
        ).fetchone()
        if not person:
            raise HTTPException(status_code=404, detail="מדריך לא נמצא")

        # Fetch payment codes early to avoid connection issues later
        payment_codes = get_payment_codes(conn.conn)
        if not payment_codes:
            # Try once more with a fresh connection if first fetch failed
            try:
                with get_conn() as temp_conn:
                    payment_codes = get_payment_codes(temp_conn.conn)
            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"Secondary fetch of payment codes failed: {e}")

        # Optimized: Fetch available months
        months = get_available_months_for_person(conn.conn, person_id)

        # Prepare months options for template
        months_options = [{"year": y, "month": m, "label": f"{m:02d}/{y}"} for y, m in months]

        if not months:
            selected_year, selected_month = year or datetime.now().year, month or datetime.now().month
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

            # Get monthly data
            shabbat_cache = get_shabbat_times_cache(conn.conn)
            daily_segments, person_name = get_daily_segments_data(
                conn, person_id, selected_year, selected_month, shabbat_cache, MINIMUM_WAGE
            )
            monthly_totals = calculate_person_monthly_totals(
                conn.conn, person_id, selected_year, selected_month, shabbat_cache, MINIMUM_WAGE
            )

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

            # Get shift segments with payment calculation
            shift_segments = []
            for report in month_reports:
                # Calculate payment for this specific shift
                shift_payment = 0.0
                shift_standby_payment = 0.0
                
                if report['shift_type_id']:
                    # Get segments for this shift type
                    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                    cursor.execute("""
                        SELECT start_time, end_time, wage_percent, segment_type
                        FROM shift_time_segments
                        WHERE shift_type_id = %s
                        ORDER BY order_index
                    """, (report['shift_type_id'],))
                    segments = cursor.fetchall()
                    cursor.close()
                    
                    # Calculate payment based on segments
                    for seg in segments:
                        # Convert time strings to minutes
                        if isinstance(seg['start_time'], str):
                            hours, minutes = map(int, seg['start_time'].split(':'))
                            start_time = hours * 60 + minutes
                        else:
                            start_time = seg['start_time']
                            
                        if isinstance(seg['end_time'], str):
                            hours, minutes = map(int, seg['end_time'].split(':'))
                            end_time = hours * 60 + minutes
                        else:
                            end_time = seg['end_time']
                            
                        duration = (end_time - start_time) / 60  # Convert minutes to hours
                        
                        # Check if this is actual work reported during standby hours
                        work_type = report.get('work_type')
                        shift_name = report.get('shift_name') or ''
                        is_vacation_report = (work_type == "sick_vacation" or
                                             "חופשה" in shift_name or
                                             "מחלה" in shift_name)
                        
                        segment_type = seg['segment_type']

                        if segment_type == 'standby':
                            # Standby payment logic
                            apt_type = report.get('apartment_type_id')
                            is_married = report.get('is_married', False)
                            # Use default standby rate or calculate based on apartment type
                            standby_rate = 70.0  # Default rate
                            shift_standby_payment += standby_rate
                        else:
                            # Work payment based on wage percent
                            hourly_rate = MINIMUM_WAGE
                            if seg['wage_percent'] == 100:
                                shift_payment += duration * hourly_rate * 1.0
                            elif seg['wage_percent'] == 125:
                                shift_payment += duration * hourly_rate * 1.25
                            elif seg['wage_percent'] == 150:
                                shift_payment += duration * hourly_rate * 1.5
                            elif seg['wage_percent'] == 175:
                                shift_payment += duration * hourly_rate * 1.75
                            elif seg['wage_percent'] == 200:
                                shift_payment += duration * hourly_rate * 2.0
                
                total_shift_payment = shift_payment + shift_standby_payment
                
                shift_segments.append({
                    "report": report,
                    "payment": total_shift_payment,
                    "work_payment": shift_payment,
                    "standby_payment": shift_standby_payment
                })

    # Calculate total standby count
    total_standby_count = monthly_totals.get("standby", 0)

    # Get unique years for dropdown
    years = sorted(set(m["year"] for m in months_options), reverse=True) if months_options else [selected_year]

    # Build simple_summary for "old calculation" tab
    simple_summary = {
        "weekday": {"count": 0, "payment": 0},
        "friday": {"count": 0, "payment": 0},
        "saturday": {"count": 0, "payment": 0},
        "overtime": {"hours": 0, "payment": 0}
    }
    for day in daily_segments:
        wd = day.get("date_obj").weekday() if day.get("date_obj") else 0
        day_payment = day.get("payment", 0) or 0

        # Mon(0)-Thu(3), Sun(6) = weekday
        if wd == 6 or wd <= 3:
            simple_summary["weekday"]["count"] += 1
            simple_summary["weekday"]["payment"] += day_payment
        elif wd == 4:  # Friday
            simple_summary["friday"]["count"] += 1
            simple_summary["friday"]["payment"] += day_payment
        elif wd == 5:  # Saturday
            simple_summary["saturday"]["count"] += 1
            simple_summary["saturday"]["payment"] += day_payment

    return templates.TemplateResponse(
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