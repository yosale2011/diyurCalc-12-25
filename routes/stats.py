"""
Statistics routes for DiyurCalc application.
Contains routes for visual analytics dashboard with Chart.js.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional, List

from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from core.config import config
from core.database import get_conn, get_housing_array_filter
from core.logic import calculate_monthly_summary
from utils.utils import format_currency, human_date, available_months_from_db

logger = logging.getLogger(__name__)
templates = Jinja2Templates(directory=str(config.TEMPLATES_DIR))
templates.env.filters["format_currency"] = format_currency
templates.env.filters["human_date"] = human_date
templates.env.globals["app_version"] = config.VERSION

# Cache לנתונים - מונע חישוב חוזר
_stats_cache = {}


def _get_cached_summary(year: int, month: int):
    """מחזיר נתוני סיכום מה-cache או מחשב אותם."""
    cache_key = f"{year}-{month}"
    if cache_key not in _stats_cache:
        with get_conn() as conn:
            summary_data, grand_totals = calculate_monthly_summary(conn.conn, year, month)
            _stats_cache[cache_key] = (summary_data, grand_totals)
    return _stats_cache[cache_key]


def clear_stats_cache():
    """מנקה את ה-cache (לקריאה אחרי עדכון נתונים)."""
    global _stats_cache
    _stats_cache = {}


# פלטת צבעים לגרפים
CHART_COLORS = [
    "#1f6feb", "#10B981", "#F59E0B", "#EF4444", "#8B5CF6",
    "#EC4899", "#06B6D4", "#84CC16", "#F97316", "#6366F1"
]


def _generate_colors(count: int) -> List[str]:
    """יוצר פלטת צבעים לגרפים."""
    return (CHART_COLORS * ((count // len(CHART_COLORS)) + 1))[:count]


def stats_page(
    request: Request,
    year: Optional[int] = None,
    month: Optional[int] = None
) -> HTMLResponse:
    """
    דף סטטיסטיקות ראשי עם גרפים אינטראקטיביים.

    Args:
        request: בקשת FastAPI
        year: שנה לתצוגה
        month: חודש לתצוגה
    """
    now = datetime.now(config.LOCAL_TZ)
    if year is None:
        year = now.year
    if month is None:
        month = now.month

    housing_filter = get_housing_array_filter()
    months_all = available_months_from_db(housing_filter)
    months_options = [{"year": y, "month": m, "label": f"{m:02d}/{y}"} for y, m in months_all]
    years_options = sorted({y for y, _ in months_all}, reverse=True)

    return templates.TemplateResponse(
        "stats.html",
        {
            "request": request,
            "selected_year": year,
            "selected_month": month,
            "months": months_options,
            "years": years_options,
        },
    )


def get_salary_by_housing_array(year: int, month: int) -> JSONResponse:
    """
    שכר לפי מערך דיור - API לגרף.
    מחשב את סה"כ השכר (עבודה + כוננויות + תוספות) לפי מערך דיור.
    """
    from collections import defaultdict

    with get_conn() as conn:
        # שליפת שמות מערכי דיור
        housing_arrays = {}
        rows = conn.execute("SELECT id, name FROM housing_arrays").fetchall()
        for r in rows:
            housing_arrays[r["id"]] = r["name"]

        # שליפת קישור מדריך -> מערך דיור (לפי הדירות שלו)
        person_to_housing = {}
        rows = conn.execute("""
            SELECT DISTINCT tr.person_id, ap.housing_array_id
            FROM time_reports tr
            JOIN apartments ap ON ap.id = tr.apartment_id
            WHERE EXTRACT(YEAR FROM tr.date) = %s
              AND EXTRACT(MONTH FROM tr.date) = %s
        """, (year, month)).fetchall()
        for r in rows:
            person_to_housing[r["person_id"]] = r["housing_array_id"]

    # שימוש ב-cache
    summary_data, _ = _get_cached_summary(year, month)

    # איחוד לפי מערך דיור
    totals_by_housing = defaultdict(float)
    for person in summary_data:
        person_id = person["person_id"]
        total = person["totals"].get("total_payment", 0)
        housing_id = person_to_housing.get(person_id)
        if housing_id:
            totals_by_housing[housing_id] += total

    # בניית הנתונים לגרף
    labels = []
    data = []
    for housing_id, total in sorted(totals_by_housing.items(), key=lambda x: x[1], reverse=True):
        name = housing_arrays.get(housing_id, f"מערך {housing_id}")
        labels.append(name)
        data.append(total)

    return JSONResponse({
        "labels": labels,
        "datasets": [{
            "label": "שכר כולל (ש\"ח)",
            "data": data,
            "backgroundColor": _generate_colors(len(data))
        }]
    })


def get_salary_by_guide(year: int, month: int, limit: int = 20) -> JSONResponse:
    """שכר לפי מדריך - Top N מדריכים."""
    summary_data, _ = _get_cached_summary(year, month)

    sorted_data = sorted(
        summary_data,
        key=lambda x: x["totals"].get("total_payment", 0),
        reverse=True
    )[:limit]

    labels = [d["name"] for d in sorted_data]
    data = [d["totals"].get("total_payment", 0) for d in sorted_data]

    return JSONResponse({
        "labels": labels,
        "datasets": [{
            "label": "שכר כולל (ש\"ח)",
            "data": data,
            "backgroundColor": "#1f6feb"
        }]
    })


def get_hours_distribution(year: int, month: int) -> JSONResponse:
    """התפלגות שעות לפי אחוזים (100%, 125%, 150%, 175%, 200%)."""
    summary_data, _ = _get_cached_summary(year, month)

    # סיכום כל השעות מכל המדריכים
    calc100 = sum(p["totals"].get("calc100", 0) for p in summary_data)
    calc125 = sum(p["totals"].get("calc125", 0) for p in summary_data)
    calc150 = sum(p["totals"].get("calc150", 0) for p in summary_data)
    calc175 = sum(p["totals"].get("calc175", 0) for p in summary_data)
    calc200 = sum(p["totals"].get("calc200", 0) for p in summary_data)

    labels = ["100%", "125%", "150%", "175%", "200%"]
    data = [
        calc100 / 60,  # המרה מדקות לשעות
        calc125 / 60,
        calc150 / 60,
        calc175 / 60,
        calc200 / 60,
    ]

    colors = ["#4CAF50", "#8BC34A", "#FFC107", "#FF5722", "#E91E63"]

    # סינון ערכים אפס מהגרף
    filtered_labels = []
    filtered_data = []
    filtered_colors = []
    for i, val in enumerate(data):
        if val > 0:
            filtered_labels.append(labels[i])
            filtered_data.append(round(val, 1))
            filtered_colors.append(colors[i])

    return JSONResponse({
        "labels": filtered_labels if filtered_labels else labels,
        "datasets": [{
            "label": "שעות",
            "data": filtered_data if filtered_data else [0] * len(labels),
            "backgroundColor": filtered_colors if filtered_colors else colors
        }]
    })


def get_extras_distribution(year: int, month: int) -> JSONResponse:
    """התפלגות כוננויות, חופשות, מחלות."""
    summary_data, _ = _get_cached_summary(year, month)

    # סיכום מכל המדריכים
    standby = sum(p["totals"].get("standby_payment", 0) for p in summary_data)
    vacation = sum(p["totals"].get("vacation_payment", 0) for p in summary_data)
    sick = sum(p["totals"].get("sick_payment", 0) for p in summary_data)
    travel = sum(p["totals"].get("travel", 0) for p in summary_data)
    extras = sum(p["totals"].get("extras", 0) for p in summary_data)

    labels = ["כוננויות", "חופשות", "מחלות", "נסיעות", "תוספות"]
    data = [standby, vacation, sick, travel, extras]
    colors = ["#3B82F6", "#10B981", "#EF4444", "#F59E0B", "#8B5CF6"]

    # סינון ערכים אפס
    filtered_labels = []
    filtered_data = []
    filtered_colors = []
    for i, val in enumerate(data):
        if val > 0:
            filtered_labels.append(labels[i])
            filtered_data.append(round(val, 2))
            filtered_colors.append(colors[i])

    return JSONResponse({
        "labels": filtered_labels if filtered_labels else labels,
        "datasets": [{
            "label": "סכום (ש\"ח)",
            "data": filtered_data if filtered_data else [0] * len(labels),
            "backgroundColor": filtered_colors if filtered_colors else colors
        }]
    })


def get_monthly_trends(year: int, months_back: int = 6) -> JSONResponse:
    """מגמות חודשיות - השוואה בין חודשים."""
    trends_total = []
    trends_hours = []
    labels = []

    current_month = datetime.now(config.LOCAL_TZ).month
    current_year = year

    for i in range(months_back - 1, -1, -1):
        target_month = current_month - i
        target_year = current_year

        while target_month <= 0:
            target_month += 12
            target_year -= 1

        _, grand_totals = _get_cached_summary(target_year, target_month)

        labels.append(f"{target_month:02d}/{target_year}")
        trends_total.append(grand_totals.get("total_payment", 0))
        trends_hours.append(grand_totals.get("total_hours", 0) / 60 if grand_totals.get("total_hours") else 0)

    return JSONResponse({
        "labels": labels,
        "datasets": [
            {
                "label": "שכר כולל (ש\"ח)",
                "data": trends_total,
                "borderColor": "#1f6feb",
                "backgroundColor": "rgba(31, 111, 235, 0.1)",
                "yAxisID": "y",
                "fill": True
            },
            {
                "label": "שעות עבודה",
                "data": trends_hours,
                "borderColor": "#10B981",
                "backgroundColor": "rgba(16, 185, 129, 0.1)",
                "yAxisID": "y1",
                "fill": True
            }
        ]
    })


def get_comparison_data(
    year1: int, month1: int,
    year2: int, month2: int
) -> JSONResponse:
    """השוואה בין שני חודשים."""
    _, totals1 = _get_cached_summary(year1, month1)
    _, totals2 = _get_cached_summary(year2, month2)

    categories = ["שכר כולל", "שעות רגילות", "שעות נוספות", "שבת", "כוננויות", "חופשות"]

    data1 = [
        totals1.get("total_payment", 0),
        totals1.get("calc100", 0) / 60,
        (totals1.get("calc125", 0) + totals1.get("calc150", 0)) / 60,
        (totals1.get("calc175", 0) + totals1.get("calc200", 0)) / 60,
        totals1.get("standby_payment", 0),
        totals1.get("vacation_payment", 0),
    ]

    data2 = [
        totals2.get("total_payment", 0),
        totals2.get("calc100", 0) / 60,
        (totals2.get("calc125", 0) + totals2.get("calc150", 0)) / 60,
        (totals2.get("calc175", 0) + totals2.get("calc200", 0)) / 60,
        totals2.get("standby_payment", 0),
        totals2.get("vacation_payment", 0),
    ]

    return JSONResponse({
        "labels": categories,
        "datasets": [
            {
                "label": f"{month1:02d}/{year1}",
                "data": data1,
                "backgroundColor": "#1f6feb"
            },
            {
                "label": f"{month2:02d}/{year2}",
                "data": data2,
                "backgroundColor": "#10B981"
            }
        ]
    })


def get_all_stats(year: int, month: int) -> JSONResponse:
    """
    מחזיר את כל הנתונים לגרפים בקריאה אחת.
    זה מונע קריאות רשת מרובות ומאיץ את הטעינה.
    """
    from collections import defaultdict

    # שליפת נתוני בסיס
    summary_data, grand_totals = _get_cached_summary(year, month)

    with get_conn() as conn:
        # מערכי דיור
        housing_arrays = {}
        rows = conn.execute("SELECT id, name FROM housing_arrays").fetchall()
        for r in rows:
            housing_arrays[r["id"]] = r["name"]

        # קישור מדריך -> מערך
        person_to_housing = {}
        rows = conn.execute("""
            SELECT DISTINCT tr.person_id, ap.housing_array_id
            FROM time_reports tr
            JOIN apartments ap ON ap.id = tr.apartment_id
            WHERE EXTRACT(YEAR FROM tr.date) = %s AND EXTRACT(MONTH FROM tr.date) = %s
        """, (year, month)).fetchall()
        for r in rows:
            person_to_housing[r["person_id"]] = r["housing_array_id"]

        # סוגי משמרות
        shift_rows = conn.execute("""
            SELECT st.name, COUNT(*) as count
            FROM time_reports tr
            JOIN shift_types st ON st.id = tr.shift_type_id
            WHERE EXTRACT(YEAR FROM tr.date) = %s AND EXTRACT(MONTH FROM tr.date) = %s
            GROUP BY st.id, st.name ORDER BY count DESC
        """, (year, month)).fetchall()

    # === חישוב שכר לפי מערך ===
    totals_by_housing = defaultdict(float)
    for person in summary_data:
        housing_id = person_to_housing.get(person["person_id"])
        if housing_id:
            totals_by_housing[housing_id] += person["totals"].get("total_payment", 0)

    housing_labels = []
    housing_data = []
    for hid, total in sorted(totals_by_housing.items(), key=lambda x: x[1], reverse=True):
        housing_labels.append(housing_arrays.get(hid, f"מערך {hid}"))
        housing_data.append(total)

    # === שכר לפי מדריך ===
    sorted_guides = sorted(summary_data, key=lambda x: x["totals"].get("total_payment", 0), reverse=True)[:20]
    guides_labels = [g["name"] for g in sorted_guides]
    guides_data = [g["totals"].get("total_payment", 0) for g in sorted_guides]

    # === התפלגות שעות ===
    calc100 = sum(p["totals"].get("calc100", 0) for p in summary_data) / 60
    calc125 = sum(p["totals"].get("calc125", 0) for p in summary_data) / 60
    calc150 = sum(p["totals"].get("calc150", 0) for p in summary_data) / 60
    calc175 = sum(p["totals"].get("calc175", 0) for p in summary_data) / 60
    calc200 = sum(p["totals"].get("calc200", 0) for p in summary_data) / 60
    hours_data = [calc100, calc125, calc150, calc175, calc200]

    # === כוננויות ותוספות ===
    extras_data = [
        sum(p["totals"].get("standby_payment", 0) for p in summary_data),
        sum(p["totals"].get("vacation_payment", 0) for p in summary_data),
        sum(p["totals"].get("sick_payment", 0) for p in summary_data),
        sum(p["totals"].get("travel", 0) for p in summary_data),
        sum(p["totals"].get("extras", 0) for p in summary_data),
    ]

    return JSONResponse({
        "summary": {
            "total_salary": sum(guides_data),
            "total_hours": sum(hours_data),
            "total_guides": len(summary_data),
            "total_standby": extras_data[0]
        },
        "by_housing": {
            "labels": housing_labels,
            "data": housing_data
        },
        "by_guide": {
            "labels": guides_labels,
            "data": guides_data
        },
        "hours": {
            "labels": ["100%", "125%", "150%", "175%", "200%"],
            "data": [round(h, 1) for h in hours_data]
        },
        "extras": {
            "labels": ["כוננויות", "חופשות", "מחלות", "נסיעות", "תוספות"],
            "data": [round(e, 2) for e in extras_data]
        },
        "shift_types": {
            "labels": [r["name"] for r in shift_rows],
            "data": [r["count"] for r in shift_rows]
        }
    })


def get_shift_types_distribution(year: int, month: int) -> JSONResponse:
    """התפלגות סוגי משמרות."""
    with get_conn() as conn:
        housing_filter = get_housing_array_filter()

        if housing_filter is not None:
            rows = conn.execute("""
                SELECT st.name, COUNT(*) as count
                FROM time_reports tr
                JOIN shift_types st ON st.id = tr.shift_type_id
                JOIN apartments ap ON ap.id = tr.apartment_id
                WHERE EXTRACT(YEAR FROM tr.date) = %s
                  AND EXTRACT(MONTH FROM tr.date) = %s
                  AND ap.housing_array_id = %s
                GROUP BY st.id, st.name
                ORDER BY count DESC
            """, (year, month, housing_filter)).fetchall()
        else:
            rows = conn.execute("""
                SELECT st.name, COUNT(*) as count
                FROM time_reports tr
                JOIN shift_types st ON st.id = tr.shift_type_id
                WHERE EXTRACT(YEAR FROM tr.date) = %s
                  AND EXTRACT(MONTH FROM tr.date) = %s
                GROUP BY st.id, st.name
                ORDER BY count DESC
            """, (year, month)).fetchall()

    labels = [r["name"] for r in rows]
    data = [r["count"] for r in rows]

    return JSONResponse({
        "labels": labels,
        "datasets": [{
            "label": "מספר משמרות",
            "data": data,
            "backgroundColor": _generate_colors(len(data))
        }]
    })
