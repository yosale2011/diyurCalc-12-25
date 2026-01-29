"""
Home page routes for DiyurCalc application.
"""
from __future__ import annotations

import time
import logging
from datetime import datetime, date
from typing import Optional

from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from core.config import config
from core.database import get_conn
from core.logic import get_active_guides
from utils.utils import month_range_ts, available_months_from_db, format_currency, human_date

logger = logging.getLogger(__name__)
templates = Jinja2Templates(directory=str(config.TEMPLATES_DIR))
templates.env.filters["format_currency"] = format_currency
templates.env.filters["human_date"] = human_date
templates.env.globals["app_version"] = config.VERSION


def home(
    request: Request,
    month: Optional[int] = None,
    year: Optional[int] = None,
    q: Optional[str] = None
) -> HTMLResponse:
    """Home page route showing guides and monthly overview."""
    func_start_time = time.time()
    logger.info(f"Starting home for month={month}, year={year}, q={q}")

    guides_start = time.time()
    guides = get_active_guides()
    logger.info(f"get_active_guides took: {time.time() - guides_start:.4f}s")

    months_start = time.time()
    months_all = available_months_from_db()
    logger.info(f"available_months_from_db took: {time.time() - months_start:.4f}s")

    if months_all:
        if month is None or year is None:
            selected_year, selected_month = months_all[-1]
        else:
            selected_year, selected_month = year, month
    else:
        selected_year = selected_month = None

    months_options = [{"year": y, "month": m, "label": f"{m:02d}/{y}"} for y, m in months_all]
    years_options = sorted({y for y, _ in months_all}, reverse=True)

    counts: dict[int, int] = {}
    has_payment_components: set[int] = set()
    if selected_year and selected_month:
        start_dt, end_dt = month_range_ts(selected_year, selected_month)
        # Convert datetime to date for PostgreSQL date column
        start_date = start_dt.date()
        end_date = end_dt.date()
        counts_start = time.time()
        with get_conn() as conn:
            for row in conn.execute(
                """
                SELECT person_id, COUNT(*) AS cnt
                FROM time_reports
                WHERE date >= %s AND date < %s
                GROUP BY person_id
                """,
                (start_date, end_date),
            ):
                counts[row["person_id"]] = row["cnt"]
            # גם מדריכים עם רכיבי תשלום צריכים להופיע
            for row in conn.execute(
                """
                SELECT DISTINCT person_id
                FROM payment_components
                WHERE date >= %s AND date < %s
                """,
                (start_date, end_date),
            ):
                has_payment_components.add(row["person_id"])
        logger.info(f"Counts query took: {time.time() - counts_start:.4f}s")

    # Calculate seniority years for each guide
    reference_date = datetime.now(config.LOCAL_TZ).date()
    if selected_year and selected_month:
        reference_date = datetime(selected_year, selected_month, 1, tzinfo=config.LOCAL_TZ).date()

    allowed_types = {"permanent", "substitute"}
    guides_filtered = []
    q_norm = q.lower().strip() if q else None
    for g in guides:
        if g["type"] not in allowed_types:
            continue
        if q_norm and q_norm not in (g["name"] or "").lower():
            continue

        if selected_year and selected_month:
            # הצג מדריכים עם לפחות משמרת אחת או רכיבי תשלום בחודש
            if counts.get(g["id"], 0) < 1 and g["id"] not in has_payment_components:
                continue

        # Calculate seniority years
        seniority_years = None
        if g.get("start_date"):
            try:
                # Handle datetime, date objects (from psycopg2) and timestamp (int/float)
                if isinstance(g["start_date"], datetime):
                    start_dt = g["start_date"].date()
                elif isinstance(g["start_date"], date):
                    start_dt = g["start_date"]
                else:
                    # Assume it's a timestamp
                    start_dt = datetime.fromtimestamp(g["start_date"], config.LOCAL_TZ).date()
                diff = reference_date - start_dt
                seniority_years = diff.days / 365.25
                if seniority_years < 0:
                    seniority_years = 0
            except Exception as e:
                logger.warning(f"Error calculating seniority for guide {g.get('id')} ({g.get('name')}): {e}, start_date type: {type(g.get('start_date'))}, value: {g.get('start_date')}")
                seniority_years = None

        guide_dict = dict(g)
        guide_dict["seniority_years"] = seniority_years
        guides_filtered.append(guide_dict)

    render_start = time.time()
    response = templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "guides": guides_filtered,
            "months": months_options,
            "years": years_options,
            "selected_year": selected_year,
            "selected_month": selected_month,
            "counts": counts,
            "q": q or "",
        },
    )
    render_time = time.time() - render_start
    logger.info(f"Template rendering took: {render_time:.4f}s")

    total_time = time.time() - func_start_time
    logger.info(f"Total home execution time: {total_time:.4f}s")

    return response