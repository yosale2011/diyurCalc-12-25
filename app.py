"""
Refactored main application file for DiyurCalc.
Uses modular structure with separate route handlers.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware
import psycopg2

from config import config
from database import set_demo_mode, get_demo_mode_from_cookie, is_demo_mode, get_current_db_name
from logic import (
    calculate_person_monthly_totals,
)
from utils import human_date
from utils import calculate_accruals, format_currency
from routes.home import home
from routes.guide import simple_summary_view, guide_view
from routes.admin import (
    manage_payment_codes, update_payment_codes,
    demo_sync_page, sync_demo_database, demo_sync_status,
    get_month_lock_status, lock_month_api, unlock_month_api
)
from routes.summary import general_summary
from routes.export import (
    export_gesher,
    export_gesher_person,
    export_gesher_multiple,
    export_gesher_preview,
    export_excel,
)
from routes.email import (
    email_settings_page,
    update_email_settings,
    test_email_settings,
    send_test_email_route,
    send_guide_email_route,
    send_all_guides_email_route,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI app setup
app = FastAPI(title="ניהול משמרות בענן")
templates = Jinja2Templates(directory=str(config.TEMPLATES_DIR))
templates.env.filters["human_date"] = human_date
templates.env.filters["format_currency"] = format_currency
templates.env.globals["app_version"] = config.VERSION


# Middleware to set demo mode from cookie
class DemoModeMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Set demo mode based on cookie
        demo_mode = get_demo_mode_from_cookie(request)
        set_demo_mode(demo_mode)
        response = await call_next(request)
        return response


app.add_middleware(DemoModeMiddleware)

# Mount static files
if config.STATIC_DIR:
    app.mount("/static", StaticFiles(directory=str(config.STATIC_DIR)), name="static")

# Global exception handler for database connection errors
@app.exception_handler(psycopg2.OperationalError)
async def database_connection_error_handler(request: Request, exc: psycopg2.OperationalError):
    """Handle database connection errors with helpful messages."""
    error_msg = str(exc)
    
    if "could not translate host name" in error_msg or "Name or service not known" in error_msg:
        user_message = (
            "שגיאת חיבור לבסיס הנתונים: לא ניתן לפתור את שם השרת.\n\n"
            "אפשרויות לפתרון:\n"
            "1. בדוק את חיבור האינטרנט\n"
            "2. ודא שהחיבור ל-VPN פעיל (אם נדרש)\n"
            "3. בדוק את הגדרות ה-DNS\n"
            "4. ודא שה-DATABASE_URL נכון בקובץ .env"
        )
    elif "connection refused" in error_msg.lower():
        user_message = (
            "שגיאת חיבור לבסיס הנתונים: השרת דחה את החיבור.\n\n"
            "אפשרויות לפתרון:\n"
            "1. ודא ששרת בסיס הנתונים פועל\n"
            "2. בדוק את מספר הפורט\n"
            "3. ודא שהחומת אש מאפשרת חיבורים"
        )
    else:
        user_message = f"שגיאת חיבור לבסיס הנתונים: {error_msg}"
    
    logger.error(f"Database connection error: {error_msg}")
    
    # Return HTML error page for web requests
    if request.url.path.startswith('/api/'):
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=503,
            content={
                "error": user_message,
                "error_type": "database_connection_error"
            }
        )
    
    return templates.TemplateResponse(
        "error.html",
        {
            "request": request,
            "error_message": user_message,
            "error_id": None,
            "back_url": "/"
        },
        status_code=503
    )

@app.get("/debug/filters")
def debug_filters():
    """Debug endpoint to check if filters are registered."""
    return {
        "format_currency_registered": "format_currency" in templates.env.filters,
        "human_date_registered": "human_date" in templates.env.filters,
        "available_filters": list(templates.env.filters.keys())
    }

# Route registrations
@app.get("/health")
def health_check():
    """Health check endpoint that tests database connectivity."""
    try:
        from database import get_conn
        with get_conn() as conn:
            # Simple query to test connection
            conn.execute("SELECT 1").fetchone()
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "error",
            "database": "disconnected",
            "error": str(e)
        }, 503


@app.get("/", response_class=HTMLResponse)
def home_route(request: Request, month: int | None = None, year: int | None = None, q: str | None = None):
    """Home page route."""
    return home(request, month, year, q)


@app.get("/guide/{person_id}/simple", response_class=HTMLResponse)
def simple_summary_route(request: Request, person_id: int, month: int | None = None, year: int | None = None):
    """Simple summary view for a guide."""
    return simple_summary_view(request, person_id, month, year)


@app.get("/guide/{person_id}", response_class=HTMLResponse)
def guide_route(request: Request, person_id: int, month: int | None = None, year: int | None = None):
    """Detailed guide view."""
    return guide_view(request, person_id, month, year)


@app.get("/admin/payment-codes", response_class=HTMLResponse)
def manage_payment_codes_route(request: Request):
    """Payment codes management page."""
    return manage_payment_codes(request)


@app.post("/admin/payment-codes/update")
async def update_payment_codes_route(request: Request):
    """Update payment codes."""
    return await update_payment_codes(request)


@app.get("/admin/demo-sync", response_class=HTMLResponse)
def demo_sync_route(request: Request):
    """Demo database sync page."""
    return demo_sync_page(request)


@app.get("/admin/demo-sync/run")
async def sync_demo_route(request: Request):
    """Run demo database sync with SSE progress."""
    return await sync_demo_database(request)


@app.get("/admin/demo-sync/status")
def demo_sync_status_route(request: Request):
    """Get demo database status."""
    return demo_sync_status(request)


# Month Lock APIs
@app.get("/api/month-lock/{year}/{month}")
def get_month_lock_route(request: Request, year: int, month: int):
    """Get month lock status."""
    return get_month_lock_status(request, year, month)


@app.post("/api/month-lock")
async def lock_month_route(request: Request):
    """Lock a month."""
    return await lock_month_api(request)


@app.post("/api/month-unlock")
async def unlock_month_route(request: Request):
    """Unlock a month."""
    return await unlock_month_api(request)


@app.get("/summary", response_class=HTMLResponse)
def general_summary_route(request: Request, year: int = None, month: int = None):
    """General monthly summary."""
    return general_summary(request, year, month)


@app.get("/export/gesher")
def export_gesher_route(year: int, month: int, company: str = None, filter_name: str = None, encoding: str = "ascii"):
    """Export Gesher file by company."""
    return export_gesher(year, month, company, filter_name, encoding)


@app.get("/export/gesher/person/{person_id}")
def export_gesher_person_route(person_id: int, year: int, month: int, encoding: str = "ascii"):
    """Export Gesher file for individual person."""
    return export_gesher_person(person_id, year, month, encoding)


@app.post("/export/gesher/multiple")
def export_gesher_multiple_route(year: int, month: int, person_ids: str, encoding: str = "ascii"):
    """Export Gesher files for multiple people as ZIP."""
    # person_ids מגיע כמחרוזת מופרדת בפסיקים
    ids = [int(x.strip()) for x in person_ids.split(",") if x.strip().isdigit()]
    if not ids:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail="לא נבחרו עובדים")
    return export_gesher_multiple(ids, year, month, encoding)


@app.get("/export/gesher/preview")
def export_gesher_preview_route(request: Request, year: int = None, month: int = None, show_zero: str = None):
    """Gesher export preview."""
    return export_gesher_preview(request, year, month, show_zero)


@app.get("/export/excel")
def export_excel_route(year: int = None, month: int = None):
    """Export monthly summary to Excel."""
    return export_excel(year, month)


# Email routes
@app.get("/admin/email-settings", response_class=HTMLResponse)
def email_settings_route(request: Request):
    """Email settings page."""
    return email_settings_page(request)


@app.post("/admin/email-settings/update")
async def update_email_settings_route(request: Request):
    """Update email settings."""
    return await update_email_settings(request)


@app.post("/admin/email-settings/test")
async def test_email_settings_route(request: Request):
    """Test email connection."""
    return await test_email_settings(request)


@app.post("/admin/email-settings/send-test")
async def send_test_email_api(request: Request):
    """Send a test email."""
    return await send_test_email_route(request)


@app.post("/api/send-guide-email/{person_id}")
async def send_guide_email_api(request: Request, person_id: int, year: int, month: int):
    """Send guide report email to a specific person."""
    return await send_guide_email_route(request, person_id, year, month)


@app.post("/api/send-all-guides-email")
def send_all_guides_email_api(request: Request, year: int, month: int):
    """Send guide report emails to all active guides."""
    return send_all_guides_email_route(request, year, month)


@app.post("/api/toggle-demo-mode")
async def toggle_demo_mode(request: Request):
    """Toggle between demo and production database."""
    # Verify password
    try:
        body = await request.json()
        password = body.get("password", "")
    except:
        password = ""

    if password != "8942798":
        return JSONResponse({"success": False, "error": "סיסמה שגויה"}, status_code=401)

    current_demo = get_demo_mode_from_cookie(request)
    new_demo = not current_demo

    response = JSONResponse({
        "success": True,
        "demo_mode": new_demo,
        "db_name": "פיתוח" if new_demo else "עבודה"
    })

    # Set cookie (expires in 24 hours)
    response.set_cookie(
        key="demo_mode",
        value="true" if new_demo else "false",
        max_age=86400,
        httponly=False,
        samesite="lax"
    )

    return response


@app.get("/api/demo-mode-status")
def demo_mode_status(request: Request):
    """Get current demo mode status."""
    demo = get_demo_mode_from_cookie(request)
    return {
        "demo_mode": demo,
        "db_name": "פיתוח" if demo else "עבודה"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host=config.HOST,
        port=config.PORT,
        reload=config.DEBUG
    )
