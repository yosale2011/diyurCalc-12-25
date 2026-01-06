"""
Admin routes for DiyurCalc application.
Contains administrative functionality like payment codes management.
"""
from __future__ import annotations

import logging

from fastapi import Request
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from config import config
from database import get_conn
from logic import get_payment_codes
from utils import human_date, format_currency
from db_sync import sync_database, check_demo_database_status

logger = logging.getLogger(__name__)

templates = Jinja2Templates(directory=str(config.TEMPLATES_DIR))
templates.env.filters["human_date"] = human_date
templates.env.filters["format_currency"] = format_currency
templates.env.globals["app_version"] = config.VERSION


def manage_payment_codes(request: Request) -> HTMLResponse:
    """Display payment codes management page."""
    with get_conn() as conn:
        codes = get_payment_codes(conn.conn)
    return templates.TemplateResponse("payment_codes.html", {"request": request, "codes": codes})


async def update_payment_codes(request: Request) -> RedirectResponse:
    """Update payment codes from form submission."""
    try:
        form_data = await request.form()

        # Parse form data manually to gather updates by ID
        ids = set()
        for key in form_data:
            if key.startswith("display_name_"):
                ids.add(key.split("_")[-1])

        with get_conn() as conn:
            for code_id in ids:
                # Get form values, handling None/empty cases
                display_name = form_data.get(f"display_name_{code_id}")
                merav_code = form_data.get(f"merav_code_{code_id}")
                display_order_raw = form_data.get(f"display_order_{code_id}")

                # Convert display_order to integer or None
                display_order = None
                if display_order_raw:
                    try:
                        display_order = int(display_order_raw)
                    except (ValueError, TypeError):
                        display_order = None

                # Ensure string values are not None
                display_name = display_name or ""
                merav_code = merav_code or ""

                # Only update if we have a display_name
                if display_name:
                    conn.execute("""
                        UPDATE payment_codes
                        SET display_name = %s, merav_code = %s, display_order = %s
                        WHERE id = %s
                    """, (display_name, merav_code, display_order, code_id))
            conn.commit()

        return RedirectResponse(url="/admin/payment-codes", status_code=303)
    except Exception as e:
        # Log the error and re-raise for FastAPI to handle
        logger.error(f"Error updating payment codes: {e}", exc_info=True)
        raise


def demo_sync_page(request: Request) -> HTMLResponse:
    """Display demo database sync page."""
    status = check_demo_database_status()
    return templates.TemplateResponse("demo_sync.html", {
        "request": request,
        "demo_status": status
    })


async def sync_demo_database(request: Request):
    """Sync demo database with production data using Server-Sent Events for progress."""
    from fastapi.responses import StreamingResponse
    import json

    async def generate_progress():
        progress_data = {"current": 0, "total": 0, "message": ""}

        def progress_callback(step, total, message):
            progress_data["current"] = step
            progress_data["total"] = total
            progress_data["message"] = message

        # Send initial message
        yield f"data: {json.dumps({'type': 'start', 'message': 'מתחיל סנכרון...'})}\n\n"

        try:
            # Run sync with progress callback
            import threading
            result_holder = [None]
            error_holder = [None]

            def run_sync():
                try:
                    result_holder[0] = sync_database(progress_callback)
                except Exception as e:
                    error_holder[0] = e

            sync_thread = threading.Thread(target=run_sync)
            sync_thread.start()

            import asyncio
            last_step = -1
            while sync_thread.is_alive():
                if progress_data["current"] != last_step:
                    last_step = progress_data["current"]
                    yield f"data: {json.dumps({'type': 'progress', 'current': progress_data['current'], 'total': progress_data['total'], 'message': progress_data['message']})}\n\n"
                await asyncio.sleep(0.1)

            sync_thread.join()

            if error_holder[0]:
                raise error_holder[0]

            result = result_holder[0]

            if result["success"]:
                tables = result["tables_synced"]
                rows = result["total_rows"]
                msg = f"הסנכרון הושלם בהצלחה! {tables} טבלאות, {rows} שורות"
                data = {"type": "complete", "success": True, "message": msg, "details": result}
                yield f"data: {json.dumps(data)}\n\n"
            else:
                data = {"type": "complete", "success": False, "message": "הסנכרון הושלם עם שגיאות", "details": result}
                yield f"data: {json.dumps(data)}\n\n"

        except Exception as e:
            logger.error(f"Error syncing demo database: {e}", exc_info=True)
            yield f"data: {json.dumps({'type': 'error', 'message': f'שגיאה בסנכרון: {str(e)}'})}\n\n"

    return StreamingResponse(
        generate_progress(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


def demo_sync_status(request: Request) -> JSONResponse:
    """Get demo database status."""
    status = check_demo_database_status()
    return JSONResponse(status)


# Month Lock APIs
def get_month_lock_status(request: Request, year: int, month: int) -> JSONResponse:
    """Get lock status for a specific month."""
    from history import get_month_lock_info, is_month_locked
    with get_conn() as conn:
        locked = is_month_locked(conn.conn, year, month)
        lock_info = get_month_lock_info(conn.conn, year, month) if locked else None
    return JSONResponse({
        "year": year,
        "month": month,
        "locked": locked,
        "lock_info": lock_info
    })


async def lock_month_api(request: Request) -> JSONResponse:
    """Lock a month to prevent changes."""
    from history import lock_month
    try:
        data = await request.json()
        year = data.get("year")
        month = data.get("month")
        locked_by = data.get("locked_by", 1)  # Default to admin user
        notes = data.get("notes", "")

        if not year or not month:
            return JSONResponse({"success": False, "error": "year and month are required"}, status_code=400)

        with get_conn() as conn:
            success = lock_month(conn.conn, year, month, locked_by, notes)

        if success:
            return JSONResponse({"success": True, "message": f"חודש {month}/{year} ננעל בהצלחה"})
        else:
            return JSONResponse({"success": False, "error": "החודש כבר נעול"}, status_code=400)

    except Exception as e:
        logger.error(f"Error locking month: {e}", exc_info=True)
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)


async def unlock_month_api(request: Request) -> JSONResponse:
    """Unlock a month to allow changes."""
    from history import unlock_month
    try:
        data = await request.json()
        year = data.get("year")
        month = data.get("month")
        unlocked_by = data.get("unlocked_by", 1)  # Default to admin user

        if not year or not month:
            return JSONResponse({"success": False, "error": "year and month are required"}, status_code=400)

        with get_conn() as conn:
            success = unlock_month(conn.conn, year, month, unlocked_by)

        if success:
            return JSONResponse({"success": True, "message": f"חודש {month}/{year} נפתח בהצלחה"})
        else:
            return JSONResponse({"success": False, "error": "החודש לא נעול"}, status_code=400)

    except Exception as e:
        logger.error(f"Error unlocking month: {e}", exc_info=True)
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)


# Shift Types Rate History API
async def update_shift_type_rate(request: Request, shift_type_id: int) -> JSONResponse:
    """Update shift type rate with history support."""
    from history import save_shift_rate_to_history, is_month_locked
    from datetime import datetime

    try:
        data = await request.json()
        new_rate = data.get("rate")  # in agorot
        new_is_minimum_wage = data.get("is_minimum_wage", True)

        current_month = datetime.now().month
        current_year = datetime.now().year

        with get_conn() as conn:
            # Check if month is locked
            if is_month_locked(conn.conn, current_year, current_month):
                return JSONResponse(
                    {"success": False, "error": "החודש נעול לעריכה"},
                    status_code=400
                )

            # Get current values
            cursor = conn.execute(
                "SELECT rate, is_minimum_wage FROM shift_types WHERE id = %s",
                (shift_type_id,)
            )
            current = cursor.fetchone()

            if not current:
                return JSONResponse(
                    {"success": False, "error": "סוג משמרת לא נמצא"},
                    status_code=404
                )

            current_rate = current["rate"]
            current_is_minimum_wage = current["is_minimum_wage"]

            # Check if there's actually a change
            if current_rate != new_rate or current_is_minimum_wage != new_is_minimum_wage:
                # Save current value to history BEFORE updating
                save_shift_rate_to_history(
                    conn.conn,
                    shift_type_id,
                    current_year,
                    current_month,
                    current_rate,
                    current_is_minimum_wage
                )

            # Update the shift type
            conn.execute("""
                UPDATE shift_types
                SET rate = %s, is_minimum_wage = %s
                WHERE id = %s
            """, (new_rate, new_is_minimum_wage, shift_type_id))
            conn.commit()

        return JSONResponse({
            "success": True,
            "message": "התעריף עודכן בהצלחה"
        })

    except Exception as e:
        logger.error(f"Error updating shift type rate: {e}", exc_info=True)
        return JSONResponse(
            {"success": False, "error": str(e)},
            status_code=500
        )


# Shift Time Segments History API
async def update_shift_segment(request: Request, segment_id: int) -> JSONResponse:
    """Update shift time segment with history support."""
    from history import save_segment_to_history, is_month_locked
    from datetime import datetime

    try:
        data = await request.json()
        new_wage_percent = data.get("wage_percent")
        new_segment_type = data.get("segment_type")
        new_start_time = data.get("start_time")
        new_end_time = data.get("end_time")
        new_order_index = data.get("order_index")

        current_month = datetime.now().month
        current_year = datetime.now().year

        with get_conn() as conn:
            # Check if month is locked
            if is_month_locked(conn.conn, current_year, current_month):
                return JSONResponse(
                    {"success": False, "error": "החודש נעול לעריכה"},
                    status_code=400
                )

            # Get current values
            cursor = conn.execute(
                """SELECT id, shift_type_id, wage_percent, segment_type, 
                          start_time, end_time, order_index 
                   FROM shift_time_segments WHERE id = %s""",
                (segment_id,)
            )
            current = cursor.fetchone()

            if not current:
                return JSONResponse(
                    {"success": False, "error": "מקטע משמרת לא נמצא"},
                    status_code=404
                )

            # Use current values for any field not provided in the request
            if new_wage_percent is None:
                new_wage_percent = current["wage_percent"]
            if new_segment_type is None:
                new_segment_type = current["segment_type"]
            if new_start_time is None:
                new_start_time = current["start_time"]
            if new_end_time is None:
                new_end_time = current["end_time"]
            if new_order_index is None:
                new_order_index = current["order_index"]

            # Check if there's actually a change
            has_change = (
                current["wage_percent"] != new_wage_percent or
                current["segment_type"] != new_segment_type or
                current["start_time"] != new_start_time or
                current["end_time"] != new_end_time or
                current["order_index"] != new_order_index
            )

            if has_change:
                # Save current value to history BEFORE updating
                save_segment_to_history(
                    conn.conn,
                    segment_id=segment_id,
                    shift_type_id=current["shift_type_id"],
                    year=current_year,
                    month=current_month,
                    wage_percent=current["wage_percent"],
                    segment_type=current["segment_type"],
                    start_time=current["start_time"],
                    end_time=current["end_time"],
                    order_index=current["order_index"]
                )

                # Update the segment
                conn.execute("""
                    UPDATE shift_time_segments
                    SET wage_percent = %s, segment_type = %s, 
                        start_time = %s, end_time = %s, order_index = %s
                    WHERE id = %s
                """, (new_wage_percent, new_segment_type, new_start_time, 
                      new_end_time, new_order_index, segment_id))
                conn.commit()

        return JSONResponse({
            "success": True,
            "message": "המקטע עודכן בהצלחה"
        })

    except Exception as e:
        logger.error(f"Error updating shift segment: {e}", exc_info=True)
        return JSONResponse(
            {"success": False, "error": str(e)},
            status_code=500
        )


async def save_all_segments_history_for_month(request: Request) -> JSONResponse:
    """Save all current shift time segments to history for a specific month.
    Useful when locking a month or before making bulk changes."""
    from history import save_all_segments_to_history, is_month_locked

    try:
        data = await request.json()
        year = data.get("year")
        month = data.get("month")
        created_by = data.get("created_by", 1)  # Default to admin user

        if not year or not month:
            return JSONResponse(
                {"success": False, "error": "year and month are required"},
                status_code=400
            )

        with get_conn() as conn:
            # Check if month is already locked - if so, warn but still allow saving history
            locked = is_month_locked(conn.conn, year, month)
            
            success = save_all_segments_to_history(conn.conn, year, month, created_by)

        if success:
            msg = f"היסטוריית מקטעים נשמרה לחודש {month}/{year}"
            if locked:
                msg += " (החודש נעול)"
            return JSONResponse({"success": True, "message": msg})
        else:
            return JSONResponse(
                {"success": False, "error": "שגיאה בשמירת היסטוריה"},
                status_code=500
            )

    except Exception as e:
        logger.error(f"Error saving segments history: {e}", exc_info=True)
        return JSONResponse(
            {"success": False, "error": str(e)},
            status_code=500
        )