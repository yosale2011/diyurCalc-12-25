"""
Email routes for DiyurCalc application.
Contains routes for email settings management and sending guide reports.
פונקציות הגדרות מייל דורשות הרשאת מנהל על (super_admin).
"""
from __future__ import annotations

import logging
from datetime import datetime

from fastapi import Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from core.config import config
from core.database import get_conn
from core.auth import is_super_admin
from services.email_service import (
    get_email_settings,
    save_email_settings,
    test_email_connection,
    send_test_email,
    send_guide_email,
    send_all_guides_email,
)

from utils.utils import format_currency, human_date

logger = logging.getLogger(__name__)


def _require_super_admin(request: Request) -> None:
    """בודק שהמשתמש הוא מנהל על, אחרת זורק שגיאה 403."""
    if not is_super_admin(request):
        raise HTTPException(status_code=403, detail="אין הרשאה - נדרש מנהל על")

templates = Jinja2Templates(directory=str(config.TEMPLATES_DIR))
templates.env.filters["format_currency"] = format_currency
templates.env.filters["human_date"] = human_date
templates.env.globals["app_version"] = config.VERSION


def email_settings_page(request: Request) -> HTMLResponse:
    """Display email settings management page. רק למנהל על."""
    _require_super_admin(request)
    with get_conn() as conn:
        settings = get_email_settings(conn)

    return templates.TemplateResponse(
        "email_settings.html",
        {
            "request": request,
            "settings": settings or {},
        }
    )


async def update_email_settings(request: Request) -> RedirectResponse:
    """Update email settings from form submission. רק למנהל על."""
    _require_super_admin(request)
    try:
        form_data = await request.form()

        settings = {
            "smtp_host": form_data.get("smtp_host", ""),
            "smtp_port": int(form_data.get("smtp_port", 587)),
            "smtp_user": form_data.get("smtp_user", ""),
            "smtp_password": form_data.get("smtp_password", ""),
            "from_email": form_data.get("from_email", ""),
            "from_name": form_data.get("from_name", "דיור003"),
            "smtp_secure": form_data.get("smtp_secure") == "on",
        }

        with get_conn() as conn:
            # If password is empty, keep the existing one
            if not settings["smtp_password"]:
                existing = get_email_settings(conn)
                if existing:
                    settings["smtp_password"] = existing.get("smtp_password", "")

            success = save_email_settings(conn, settings)

        if success:
            return RedirectResponse(
                url="/admin/email-settings?saved=1",
                status_code=303
            )
        else:
            return RedirectResponse(
                url="/admin/email-settings?error=1",
                status_code=303
            )

    except Exception as e:
        logger.error(f"Error updating email settings: {e}", exc_info=True)
        return RedirectResponse(
            url="/admin/email-settings?error=1",
            status_code=303
        )


async def test_email_settings(request: Request) -> JSONResponse:
    """Test email connection with current settings. רק למנהל על."""
    _require_super_admin(request)
    try:
        form_data = await request.json()

        settings = {
            "smtp_host": form_data.get("smtp_host", ""),
            "smtp_port": int(form_data.get("smtp_port", 587)),
            "smtp_user": form_data.get("smtp_user", ""),
            "smtp_password": form_data.get("smtp_password", ""),
            "smtp_secure": form_data.get("smtp_secure", False),
        }

        # If password is empty, try to get from DB
        if not settings["smtp_password"]:
            with get_conn() as conn:
                existing = get_email_settings(conn)
                if existing:
                    settings["smtp_password"] = existing.get("smtp_password", "")

        result = test_email_connection(settings)
        return JSONResponse(result)

    except Exception as e:
        logger.error(f"Error testing email: {e}", exc_info=True)
        return JSONResponse({"success": False, "error": str(e)})


async def send_guide_email_route(request: Request, person_id: int, year: int, month: int) -> JSONResponse:
    """Send guide report email to a specific person or custom email."""
    try:
        # Try to get custom email from request body
        custom_email = None
        try:
            body = await request.json()
            custom_email = body.get('email')
        except:
            pass

        import asyncio
        
        # Wrapper function to run in separate thread with its own DB connection
        def send_email_task_wrapper(pid, y, m, email):
            try:
                # Create a fresh connection for this thread to avoid any sharing issues
                with get_conn() as new_conn:
                    return send_guide_email(new_conn, pid, y, m, email)
            except Exception as task_error:
                logger.error(f"Error in threaded email task: {task_error}")
                return {"success": False, "error": str(task_error)}

        # Run blocking PDF generation in a thread to prevent event loop deadlock
        result = await asyncio.to_thread(send_email_task_wrapper, person_id, year, month, custom_email)
            
        return JSONResponse(result)
    except Exception as e:
        logger.error(f"Error in send_guide_email_route: {e}", exc_info=True)
        return JSONResponse({"success": False, "error": str(e)})


def send_all_guides_email_route(request: Request, year: int, month: int) -> JSONResponse:
    """Send guide report emails to all active guides."""
    try:
        with get_conn() as conn:
            result = send_all_guides_email(conn, year, month)
        return JSONResponse(result)
    except Exception as e:
        logger.error(f"Error in send_all_guides_email_route: {e}", exc_info=True)
        return JSONResponse({"success": False, "error": str(e)})


async def send_test_email_route(request: Request) -> JSONResponse:
    """Send a test email to verify email settings. רק למנהל על."""
    _require_super_admin(request)
    try:
        form_data = await request.json()
        to_email = form_data.get("to_email", "")

        if not to_email:
            return JSONResponse({"success": False, "error": "יש להזין כתובת מייל"})

        with get_conn() as conn:
            result = send_test_email(conn, to_email)
        return JSONResponse(result)
    except Exception as e:
        logger.error(f"Error in send_test_email_route: {e}", exc_info=True)
        return JSONResponse({"success": False, "error": str(e)})
