"""
Export routes for DiyurCalc application.
Contains file export functionality for various formats.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional, List
from urllib.parse import quote

from fastapi import Request, HTTPException
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates
from config import config
from database import get_conn
import gesher_exporter
from utils import human_date, format_currency

templates = Jinja2Templates(directory=str(config.TEMPLATES_DIR))
templates.env.filters["human_date"] = human_date
templates.env.filters["format_currency"] = format_currency
templates.env.globals["app_version"] = config.VERSION


def export_gesher(
    year: int,
    month: int,
    company: Optional[str] = None,
    filter_name: Optional[str] = None,
    encoding: str = "ascii"
) -> Response:
    """
    ייצוא קובץ גשר למירב - לפי מפעל
    company: קוד מפעל (001 או 400)
    encoding: קידוד הקובץ (ascii / windows-1255 / utf-8)
    """
    if not company:
        raise HTTPException(status_code=400, detail="חובה לבחור מפעל")

    with get_conn() as conn:
        content = gesher_exporter.generate_gesher_file(conn, year, month, filter_name, company)

    # קידוד הקובץ
    encoded_content = content.encode(encoding, errors='replace')

    # שם קובץ עם קוד מפעל
    filename = f"gesher_{company}_{year}_{month:02d}.mrv"
    return Response(
        content=encoded_content,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "Content-Type": f"text/plain; charset={encoding}"
        }
    )


def export_gesher_person(
    person_id: int,
    year: int,
    month: int,
    encoding: str = "ascii"
) -> Response:
    """
    ייצוא קובץ גשר לעובד בודד
    """
    with get_conn() as conn:
        # שליפת שם העובד לשם הקובץ
        person = conn.execute("SELECT name, meirav_code FROM people WHERE id = %s", (person_id,)).fetchone()
        if not person:
            raise HTTPException(status_code=404, detail="עובד לא נמצא")

        content, company = gesher_exporter.generate_gesher_file_for_person(conn, person_id, year, month)

    if not content:
        raise HTTPException(status_code=400, detail="לא ניתן לייצר קובץ - אין קוד מירב לעובד")

    encoded_content = content.encode(encoding, errors='replace')

    # שם קובץ - שימוש בקוד מירב במקום שם (כי זה תמיד ASCII)
    meirav_code = person['meirav_code'] or person_id
    filename = f"gesher_{meirav_code}_{year}_{month:02d}.mrv"

    # לשם התצוגה בדפדפן - שם מקודד ב-URL encoding
    display_name = quote(person['name'], safe='')

    return Response(
        content=encoded_content,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f"attachment; filename={filename}; filename*=UTF-8''{display_name}_{year}_{month:02d}.mrv",
            "Content-Type": f"text/plain; charset={encoding}"
        }
    )


def export_gesher_multiple(
    person_ids: List[int],
    year: int,
    month: int,
    encoding: str = "ascii"
) -> Response:
    """
    ייצוא קובץ גשר ממוזג למספר עובדים נבחרים
    """
    with get_conn() as conn:
        content, company = gesher_exporter.generate_gesher_file_for_multiple(conn, person_ids, year, month)

    if not content:
        raise HTTPException(status_code=400, detail="לא נוצרו נתונים - אין קוד מירב לעובדים שנבחרו")

    # קידוד הקובץ
    encoded_content = content.encode(encoding, errors='replace')

    # שם קובץ עם קוד מפעל
    filename = f"gesher_{company}_{year}_{month:02d}.mrv"

    return Response(
        content=encoded_content,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "Content-Type": f"text/plain; charset={encoding}"
        }
    )


def export_gesher_preview(
    request: Request,
    year: Optional[int] = None,
    month: Optional[int] = None,
    show_zero: Optional[str] = None
) -> HTMLResponse:
    """תצוגה מקדימה של ייצוא גשר"""
    now = datetime.now()
    if year is None:
        year = now.year
    if month is None:
        month = now.month

    show_zero_flag = show_zero == "1"

    from logic import calculate_monthly_summary

    with get_conn() as conn:
        preview = gesher_exporter.get_export_preview(conn, year, month, limit=100)
        export_codes = gesher_exporter.load_export_config_from_db(conn)
        if not export_codes:
            export_codes = gesher_exporter.load_export_config()
        # שליפת מפעלים מהטבלה
        employers = conn.execute("SELECT code, name FROM employers WHERE is_active::integer = 1 ORDER BY code").fetchall()

        # מציאת מדריכים ללא קוד מירב - רק אלו שיש להם נתונים בחודש הנבחר
        raw_conn = conn.conn if hasattr(conn, 'conn') else conn
        summary_data, _ = calculate_monthly_summary(raw_conn, year, month)

    missing_merav_list = []
    for person_data in summary_data:
        meirav_code = person_data.get('merav_code') or person_data.get('meirav_code')
        if not meirav_code:
            missing_merav_list.append({
                'id': person_data.get('person_id') or person_data.get('id'),
                'name': person_data.get('name', '')
            })
    missing_merav_count = len(missing_merav_list)

    # אם לא מבקשים להציג ערכים 0, מסננים שורות ועובדים ללא נתונים
    if not show_zero_flag:
        filtered_preview = []
        for person in preview:
            # סינון שורות: לכסף - בודקים payment, לשאר - בודקים quantity
            non_zero_lines = [
                line for line in person['lines']
                if (line['type'] == 'money' and line['payment'] > 0) or
                   (line['type'] != 'money' and line['quantity'] > 0)
            ]
            if non_zero_lines:
                filtered_preview.append({
                    'person_id': person['person_id'],
                    'name': person['name'],
                    'meirav_code': person['meirav_code'],
                    'lines': non_zero_lines
                })
        preview = filtered_preview

    return templates.TemplateResponse("gesher_preview.html", {
        "request": request,
        "preview": preview,
        "export_codes": export_codes,
        "employers": employers,
        "selected_year": year,
        "selected_month": month,
        "show_zero": show_zero_flag,
        "years": list(range(2023, 2027)),
        "missing_merav_count": missing_merav_count,
        "missing_merav_list": missing_merav_list
    })


def export_excel(year: Optional[int] = None, month: Optional[int] = None) -> Response:
    """ייצוא סיכום חודשי לאקסל"""
    now = datetime.now(config.LOCAL_TZ)
    if year is None:
        year = now.year
    if month is None:
        month = now.month

    from logic import calculate_monthly_summary
    import pandas as pd
    from io import BytesIO

    with get_conn() as conn:
        summary_data, grand_totals = calculate_monthly_summary(conn.conn, year, month)

    # Create Excel file
    output = BytesIO()

    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Main summary sheet
        summary_rows = []
        for person_data in summary_data:
            totals = person_data.get('totals', {})
            row = {
                'שם': person_data.get('name', ''),
                'קוד מירב': person_data.get('merav_code', ''),
                'שעות עבודה': round(totals.get('total_hours', 0) / 60, 2),
                'תשלום': round(totals.get('total_payment', 0), 2),
                'כוננויות': totals.get('standby', 0),
                'תשלום כוננויות': round(totals.get('standby_payment', 0), 2),
                'ימי עבודה': totals.get('actual_work_days', 0),
                'חופשה נוצלה': totals.get('vacation_days_taken', 0),
                'שעות 100%': round(totals.get('calc100', 0) / 60, 2),
                'שעות 125%': round(totals.get('calc125', 0) / 60, 2),
                'שעות 150%': round(totals.get('calc150', 0) / 60, 2),
                'שעות 175%': round(totals.get('calc175', 0) / 60, 2),
                'שעות 200%': round(totals.get('calc200', 0) / 60, 2),
                'נסיעות': round(totals.get('travel', 0), 2),
                'תוספות': round(totals.get('extras', 0), 2),
            }
            summary_rows.append(row)

        if summary_rows:
            df_summary = pd.DataFrame(summary_rows)
            df_summary.to_excel(writer, sheet_name='סיכום חודשי', index=False)
        else:
            # Create empty sheet with headers if no data
            df_empty = pd.DataFrame(columns=['שם', 'קוד מירב', 'שעות עבודה', 'תשלום'])
            df_empty.to_excel(writer, sheet_name='סיכום חודשי', index=False)

        # Grand totals sheet
        grand_totals_data = [{
            'סה"כ שעות עבודה': round(grand_totals.get('total_hours', 0) / 60, 2),
            'סה"כ לתשלום': round(grand_totals.get('payment', 0), 2),
            'סה"כ כוננויות': grand_totals.get('standby', 0),
            'תשלום כוננויות': round(grand_totals.get('standby_payment', 0), 2),
            'ימי עבודה': grand_totals.get('actual_work_days', 0),
            'חופשה נוצלה': grand_totals.get('vacation_days_taken', 0),
        }]
        df_totals = pd.DataFrame(grand_totals_data)
        df_totals.to_excel(writer, sheet_name='סיכום כללי', index=False)

    output.seek(0)

    filename = f"summary_{year}_{month:02d}.xlsx"
    return Response(
        content=output.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )