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

    גישה פשוטה: מדריך שעבד בשני מערכים יופיע בשניהם עם כל השכר שלו.
    הסכום הכולל של הגרף עשוי להיות גבוה יותר מייצוא שכר (אם יש מדריכים ביותר ממערך אחד).
    """
    from collections import defaultdict

    # שימוש ב-cache - אותו חישוב כמו ייצוא שכר
    summary_data, grand_totals = _get_cached_summary(year, month)

    with get_conn() as conn:
        # שליפת מערכי דיור לכל מדריך לפי הדירות שעבד בהן
        totals_by_housing = defaultdict(float)

        for person in summary_data:
            person_id = person["person_id"]
            total_payment = person["totals"].get("total_payment", 0)

            if total_payment <= 0:
                continue

            # מציאת כל מערכי הדיור שהמדריך עבד בהם
            housing_arrays = conn.execute("""
                SELECT DISTINCT ha.name
                FROM time_reports tr
                JOIN apartments ap ON ap.id = tr.apartment_id
                JOIN housing_arrays ha ON ha.id = ap.housing_array_id
                WHERE tr.person_id = %s
                  AND EXTRACT(YEAR FROM tr.date) = %s
                  AND EXTRACT(MONTH FROM tr.date) = %s
            """, (person_id, year, month)).fetchall()

            # הוספת כל השכר של המדריך לכל מערך שעבד בו
            for row in housing_arrays:
                totals_by_housing[row["name"]] += total_payment

    # בניית הנתונים לגרף - מיון לפי סכום
    sorted_housing = sorted(totals_by_housing.items(), key=lambda x: x[1], reverse=True)

    labels = [name for name, _ in sorted_housing]
    data = [round(total, 2) for _, total in sorted_housing]

    # הסכום הכולל מה-cache - זהה לייצוא שכר
    grand_total = grand_totals.get("total_payment", 0)

    return JSONResponse({
        "labels": labels,
        "datasets": [{
            "label": "שכר כולל (ש\"ח)",
            "data": data,
            "backgroundColor": _generate_colors(len(data))
        }],
        "total": round(grand_total, 2)
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

    # שליפת נתוני בסיס - אותו חישוב כמו ייצוא שכר
    summary_data, grand_totals = _get_cached_summary(year, month)

    with get_conn() as conn:
        # סוגי משמרות
        shift_rows = conn.execute("""
            SELECT st.name, COUNT(*) as count
            FROM time_reports tr
            JOIN shift_types st ON st.id = tr.shift_type_id
            WHERE EXTRACT(YEAR FROM tr.date) = %s AND EXTRACT(MONTH FROM tr.date) = %s
            GROUP BY st.id, st.name ORDER BY count DESC
        """, (year, month)).fetchall()

        # === חישוב שכר לפי מערך - מדריך מופיע בכל מערך שעבד בו ===
        totals_by_housing = defaultdict(float)

        for person in summary_data:
            person_id = person["person_id"]
            total_payment = person["totals"].get("total_payment", 0)

            if total_payment <= 0:
                continue

            # מציאת כל מערכי הדיור שהמדריך עבד בהם
            housing_arrays = conn.execute("""
                SELECT DISTINCT ha.name
                FROM time_reports tr
                JOIN apartments ap ON ap.id = tr.apartment_id
                JOIN housing_arrays ha ON ha.id = ap.housing_array_id
                WHERE tr.person_id = %s
                  AND EXTRACT(YEAR FROM tr.date) = %s
                  AND EXTRACT(MONTH FROM tr.date) = %s
            """, (person_id, year, month)).fetchall()

            # הוספת כל השכר לכל מערך
            for row in housing_arrays:
                totals_by_housing[row["name"]] += total_payment

    # מיון לפי סכום
    sorted_housing = sorted(totals_by_housing.items(), key=lambda x: x[1], reverse=True)
    housing_labels = [name for name, _ in sorted_housing]
    housing_data = [round(total, 2) for _, total in sorted_housing]

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


# =============================================================================
# APIs חדשים לדשבורד מורחב
# =============================================================================


def _aggregate_by_apartment(summary_data: List, year: int, month: int) -> dict:
    """
    אגרגציה של נתוני סיכום לפי דירה.

    Returns:
        dict: {apartment_id: {name, housing_array_id, housing_array_name, totals}}
    """
    from collections import defaultdict

    with get_conn() as conn:
        # שליפת כל הדירות עם מערך הדיור שלהן
        apartments = {}
        rows = conn.execute("""
            SELECT ap.id, ap.name, ap.housing_array_id, ha.name as housing_array_name
            FROM apartments ap
            LEFT JOIN housing_arrays ha ON ha.id = ap.housing_array_id
        """).fetchall()
        for r in rows:
            apartments[r["id"]] = {
                "name": r["name"],
                "housing_array_id": r["housing_array_id"],
                "housing_array_name": r["housing_array_name"] or "ללא מערך"
            }

        # שליפת קישור מדריך+דירה -> תשלומים מפורטים
        # צריך לשלוף את הדיווחים עצמם כדי לדעת איזה תשלום שייך לאיזו דירה
        reports = conn.execute("""
            SELECT tr.person_id, tr.apartment_id
            FROM time_reports tr
            WHERE EXTRACT(YEAR FROM tr.date) = %s
              AND EXTRACT(MONTH FROM tr.date) = %s
        """, (year, month)).fetchall()

    # מיפוי מדריך -> דירות
    person_apartments = defaultdict(set)
    for r in reports:
        person_apartments[r["person_id"]].add(r["apartment_id"])

    # אגרגציה לפי דירה
    apartment_totals = defaultdict(lambda: {
        "total_payment": 0,
        "total_hours": 0,
        "calc100": 0, "calc125": 0, "calc150": 0, "calc175": 0, "calc200": 0,
        "standby_payment": 0,
        "guides_count": 0
    })

    for person in summary_data:
        person_id = person["person_id"]
        totals = person["totals"]
        person_apts = person_apartments.get(person_id, set())

        if not person_apts:
            continue

        # חלוקה שווה בין הדירות של המדריך (אם עבד ביותר מדירה אחת)
        apt_count = len(person_apts)
        for apt_id in person_apts:
            apartment_totals[apt_id]["total_payment"] += totals.get("total_payment", 0) / apt_count
            apartment_totals[apt_id]["total_hours"] += totals.get("total_hours", 0) / apt_count
            apartment_totals[apt_id]["calc100"] += totals.get("calc100", 0) / apt_count
            apartment_totals[apt_id]["calc125"] += totals.get("calc125", 0) / apt_count
            apartment_totals[apt_id]["calc150"] += totals.get("calc150", 0) / apt_count
            apartment_totals[apt_id]["calc175"] += totals.get("calc175", 0) / apt_count
            apartment_totals[apt_id]["calc200"] += totals.get("calc200", 0) / apt_count
            apartment_totals[apt_id]["standby_payment"] += totals.get("standby_payment", 0) / apt_count
            apartment_totals[apt_id]["guides_count"] += 1

    # בניית התוצאה הסופית
    result = {}
    for apt_id, totals in apartment_totals.items():
        if apt_id in apartments:
            result[apt_id] = {
                **apartments[apt_id],
                "totals": totals
            }

    return result


def get_compare_housing_arrays(
    year: int,
    month: int,
    array_ids: List[int]
) -> JSONResponse:
    """
    השוואת 2-5 מערכי דיור - שכר ב-2 החודשים האחרונים.

    Args:
        year: שנה נבחרת
        month: חודש נבחר
        array_ids: רשימת מזהי מערכי דיור להשוואה (2-5)
    """
    from collections import defaultdict

    if not array_ids or len(array_ids) < 2:
        return JSONResponse({"error": "יש לבחור לפחות 2 מערכי דיור"}, status_code=400)
    if len(array_ids) > 5:
        array_ids = array_ids[:5]

    # חישוב החודש הקודם
    prev_month = month - 1
    prev_year = year
    if prev_month <= 0:
        prev_month = 12
        prev_year -= 1

    with get_conn() as conn:
        # שליפת שמות המערכים
        array_names = {}
        placeholders = ",".join(["%s"] * len(array_ids))
        rows = conn.execute(f"""
            SELECT id, name FROM housing_arrays WHERE id IN ({placeholders})
        """, tuple(array_ids)).fetchall()
        for r in rows:
            array_names[r["id"]] = r["name"]

        # שליפת קישור מדריך -> מערך לשני החודשים
        def get_person_to_housing(y: int, m: int) -> dict:
            rows = conn.execute("""
                SELECT DISTINCT tr.person_id, ap.housing_array_id
                FROM time_reports tr
                JOIN apartments ap ON ap.id = tr.apartment_id
                WHERE EXTRACT(YEAR FROM tr.date) = %s
                  AND EXTRACT(MONTH FROM tr.date) = %s
                  AND ap.housing_array_id = ANY(%s)
            """, (y, m, array_ids)).fetchall()
            return {r["person_id"]: r["housing_array_id"] for r in rows}

    # סיכומים לכל חודש
    summary_curr, _ = _get_cached_summary(year, month)
    summary_prev, _ = _get_cached_summary(prev_year, prev_month)

    person_to_housing_curr = get_person_to_housing(year, month)
    person_to_housing_prev = get_person_to_housing(prev_year, prev_month)

    # אגרגציה לפי מערך
    def aggregate_by_array(summary_data: List, person_to_housing: dict) -> dict:
        totals = defaultdict(float)
        for person in summary_data:
            pid = person["person_id"]
            if pid in person_to_housing:
                hid = person_to_housing[pid]
                totals[hid] += person["totals"].get("total_payment", 0)
        return totals

    totals_curr = aggregate_by_array(summary_curr, person_to_housing_curr)
    totals_prev = aggregate_by_array(summary_prev, person_to_housing_prev)

    # בניית הנתונים לגרף
    labels = [array_names.get(aid, f"מערך {aid}") for aid in array_ids]
    data_curr = [totals_curr.get(aid, 0) for aid in array_ids]
    data_prev = [totals_prev.get(aid, 0) for aid in array_ids]

    return JSONResponse({
        "labels": labels,
        "datasets": [
            {
                "label": f"{month:02d}/{year}",
                "data": data_curr,
                "backgroundColor": "#1f6feb"
            },
            {
                "label": f"{prev_month:02d}/{prev_year}",
                "data": data_prev,
                "backgroundColor": "#10B981"
            }
        ]
    })


def get_top_apartments_by_percent(
    year: int,
    month: int,
    percent: int = 100,
    limit: int = 10
) -> JSONResponse:
    """
    Top 10 דירות עם הכי הרבה שכר באחוז מסוים.

    Args:
        year: שנה
        month: חודש
        percent: אחוז לסינון (100/125/150/175/200)
        limit: מספר דירות להציג
    """
    summary_data, _ = _get_cached_summary(year, month)
    apartment_data = _aggregate_by_apartment(summary_data, year, month)

    # מיפוי אחוז לשדה
    percent_field = f"calc{percent}"
    if percent_field not in ["calc100", "calc125", "calc150", "calc175", "calc200"]:
        percent_field = "calc100"

    # מיון לפי השדה הנבחר
    sorted_apartments = sorted(
        apartment_data.items(),
        key=lambda x: x[1]["totals"].get(percent_field, 0),
        reverse=True
    )[:limit]

    labels = [apt["name"] for _, apt in sorted_apartments]
    data = [apt["totals"].get(percent_field, 0) / 60 for _, apt in sorted_apartments]  # המרה לשעות

    return JSONResponse({
        "labels": labels,
        "datasets": [{
            "label": f"שעות {percent}%",
            "data": [round(d, 1) for d in data],
            "backgroundColor": "#8B5CF6"
        }]
    })


def get_apartments_in_array(
    year: int,
    month: int,
    housing_array_id: int
) -> JSONResponse:
    """
    כל הדירות במערך דיור מסוים - סך השכר.

    Args:
        year: שנה
        month: חודש
        housing_array_id: מזהה מערך דיור
    """
    summary_data, _ = _get_cached_summary(year, month)
    apartment_data = _aggregate_by_apartment(summary_data, year, month)

    # סינון לפי מערך דיור
    filtered = {
        apt_id: apt
        for apt_id, apt in apartment_data.items()
        if apt["housing_array_id"] == housing_array_id
    }

    # מיון לפי שכר
    sorted_apartments = sorted(
        filtered.items(),
        key=lambda x: x[1]["totals"].get("total_payment", 0),
        reverse=True
    )

    labels = [apt["name"] for _, apt in sorted_apartments]
    data = [apt["totals"].get("total_payment", 0) for _, apt in sorted_apartments]

    return JSONResponse({
        "labels": labels,
        "datasets": [{
            "label": "שכר כולל (ש\"ח)",
            "data": [round(d, 2) for d in data],
            "backgroundColor": _generate_colors(len(data))
        }]
    })


def get_apartments_in_array_by_percent(
    year: int,
    month: int,
    housing_array_id: int
) -> JSONResponse:
    """
    כל הדירות במערך דיור - פילוח לפי אחוזים.

    Args:
        year: שנה
        month: חודש
        housing_array_id: מזהה מערך דיור
    """
    summary_data, _ = _get_cached_summary(year, month)
    apartment_data = _aggregate_by_apartment(summary_data, year, month)

    # סינון לפי מערך דיור
    filtered = {
        apt_id: apt
        for apt_id, apt in apartment_data.items()
        if apt["housing_array_id"] == housing_array_id
    }

    # מיון לפי שכר כולל
    sorted_apartments = sorted(
        filtered.items(),
        key=lambda x: x[1]["totals"].get("total_payment", 0),
        reverse=True
    )

    labels = [apt["name"] for _, apt in sorted_apartments]

    # בניית datasets לכל אחוז
    datasets = []
    percent_colors = {
        100: "#4CAF50",
        125: "#8BC34A",
        150: "#FFC107",
        175: "#FF5722",
        200: "#E91E63"
    }

    for percent in [100, 125, 150, 175, 200]:
        field = f"calc{percent}"
        data = [apt["totals"].get(field, 0) / 60 for _, apt in sorted_apartments]
        # רק אם יש נתונים
        if sum(data) > 0:
            datasets.append({
                "label": f"{percent}%",
                "data": [round(d, 1) for d in data],
                "backgroundColor": percent_colors[percent]
            })

    return JSONResponse({
        "labels": labels,
        "datasets": datasets
    })


def get_apartment_details(
    year: int,
    month: int,
    apartment_id: int
) -> JSONResponse:
    """
    פרטי דירה - שעות ושכר לפי סוג משמרת + מדריכים.

    Args:
        year: שנה
        month: חודש
        apartment_id: מזהה דירה
    """
    with get_conn() as conn:
        # שליפת שם הדירה
        apt_row = conn.execute(
            "SELECT name FROM apartments WHERE id = %s", (apartment_id,)
        ).fetchone()
        apartment_name = apt_row["name"] if apt_row else f"דירה {apartment_id}"

        # שליפת כל המדריכים שעבדו בדירה בחודש
        guides = conn.execute("""
            SELECT DISTINCT p.id, p.name
            FROM time_reports tr
            JOIN people p ON p.id = tr.person_id
            WHERE tr.apartment_id = %s
              AND EXTRACT(YEAR FROM tr.date) = %s
              AND EXTRACT(MONTH FROM tr.date) = %s
            ORDER BY p.name
        """, (apartment_id, year, month)).fetchall()

        # שליפת סוגי משמרות בדירה
        shift_types = conn.execute("""
            SELECT st.id, st.name, COUNT(*) as count,
                   SUM(EXTRACT(EPOCH FROM (tr.end_time - tr.start_time))/60) as total_minutes
            FROM time_reports tr
            JOIN shift_types st ON st.id = tr.shift_type_id
            WHERE tr.apartment_id = %s
              AND EXTRACT(YEAR FROM tr.date) = %s
              AND EXTRACT(MONTH FROM tr.date) = %s
            GROUP BY st.id, st.name
            ORDER BY count DESC
        """, (apartment_id, year, month)).fetchall()

    # נתוני משמרות
    shift_labels = [s["name"] for s in shift_types]
    shift_hours = [round((s["total_minutes"] or 0) / 60, 1) for s in shift_types]
    shift_counts = [s["count"] for s in shift_types]

    # נתוני מדריכים - שימוש ב-summary הקיים
    guide_labels = [g["name"] for g in guides]
    guide_salaries = []

    summary_data, _ = _get_cached_summary(year, month)

    for guide in guides:
        for person in summary_data:
            if person["person_id"] == guide["id"]:
                # הערכה - חלק יחסי מהשכר (אם עבד ביותר מדירה אחת)
                guide_salaries.append(person["totals"].get("total_payment", 0))
                break
        else:
            guide_salaries.append(0)

    return JSONResponse({
        "apartment_name": apartment_name,
        "shifts": {
            "labels": shift_labels,
            "datasets": [
                {
                    "label": "שעות",
                    "data": shift_hours,
                    "backgroundColor": "#1f6feb",
                    "yAxisID": "y"
                },
                {
                    "label": "מספר משמרות",
                    "data": shift_counts,
                    "backgroundColor": "#10B981",
                    "yAxisID": "y1"
                }
            ]
        },
        "guides": {
            "labels": guide_labels,
            "datasets": [{
                "label": "שכר כולל (ש\"ח)",
                "data": [round(s, 2) for s in guide_salaries],
                "backgroundColor": _generate_colors(len(guide_salaries))
            }]
        }
    })


def get_guide_yearly(person_id: int, year: int) -> JSONResponse:
    """
    מגמת שכר של מדריך ב-12 החודשים האחרונים.

    Args:
        person_id: מזהה מדריך
        year: שנה (נקודת התחלה)
    """
    from app_utils import get_daily_segments_data, aggregate_daily_segments_to_monthly
    from core.time_utils import get_shabbat_times_cache
    from core.history import get_minimum_wage_for_month
    from core.database import PostgresConnection

    labels = []
    salary_data = []
    hours_data = []

    # 12 חודשים אחורה מהחודש הנוכחי
    current_month = datetime.now(config.LOCAL_TZ).month
    current_year = year

    with get_conn() as conn:
        # שליפת שם המדריך
        guide_row = conn.execute(
            "SELECT name FROM people WHERE id = %s", (person_id,)
        ).fetchone()
        guide_name = guide_row["name"] if guide_row else f"מדריך {person_id}"

        shabbat_cache = get_shabbat_times_cache(conn.conn)
        conn_wrapper = PostgresConnection(conn.conn, use_pool=False)

        for i in range(11, -1, -1):
            target_month = current_month - i
            target_year = current_year

            while target_month <= 0:
                target_month += 12
                target_year -= 1

            minimum_wage = get_minimum_wage_for_month(conn.conn, target_year, target_month)

            try:
                daily_segments, _ = get_daily_segments_data(
                    conn_wrapper, person_id, target_year, target_month,
                    shabbat_cache, minimum_wage
                )
                monthly_totals = aggregate_daily_segments_to_monthly(
                    conn_wrapper, daily_segments, person_id,
                    target_year, target_month, minimum_wage
                )

                salary = monthly_totals.get("total_payment", 0)
                hours = monthly_totals.get("total_hours", 0) / 60
            except Exception:
                salary = 0
                hours = 0

            labels.append(f"{target_month:02d}/{target_year}")
            salary_data.append(round(salary, 2))
            hours_data.append(round(hours, 1))

    return JSONResponse({
        "guide_name": guide_name,
        "labels": labels,
        "datasets": [
            {
                "label": "שכר (ש\"ח)",
                "data": salary_data,
                "borderColor": "#1f6feb",
                "backgroundColor": "rgba(31, 111, 235, 0.1)",
                "fill": True,
                "yAxisID": "y"
            },
            {
                "label": "שעות",
                "data": hours_data,
                "borderColor": "#10B981",
                "backgroundColor": "rgba(16, 185, 129, 0.1)",
                "fill": True,
                "yAxisID": "y1"
            }
        ]
    })


def get_housing_arrays_list() -> JSONResponse:
    """רשימת כל מערכי הדיור."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, name FROM housing_arrays ORDER BY name"
        ).fetchall()

    return JSONResponse({
        "arrays": [{"id": r["id"], "name": r["name"]} for r in rows]
    })


def get_apartments_list(housing_array_id: Optional[int] = None) -> JSONResponse:
    """רשימת דירות, אופציונלי לפי מערך דיור."""
    with get_conn() as conn:
        if housing_array_id:
            rows = conn.execute("""
                SELECT id, name FROM apartments
                WHERE housing_array_id = %s
                ORDER BY name
            """, (housing_array_id,)).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, name FROM apartments ORDER BY name"
            ).fetchall()

    return JSONResponse({
        "apartments": [{"id": r["id"], "name": r["name"]} for r in rows]
    })


def get_guides_list() -> JSONResponse:
    """רשימת כל המדריכים הפעילים."""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT id, name FROM people
            WHERE is_active::integer = 1
            ORDER BY name
        """).fetchall()

    return JSONResponse({
        "guides": [{"id": r["id"], "name": r["name"]} for r in rows]
    })
