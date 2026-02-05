"""
Refactored main application file for DiyurCalc.
Uses modular structure with separate route handlers.
"""
from __future__ import annotations

import logging
import time
import signal
import sys
import atexit
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware
import psycopg2

from core.config import config
from core.database import (
    set_demo_mode, get_demo_mode_from_cookie, is_demo_mode, get_current_db_name, close_all_pools,
    get_housing_array_from_cookie, set_housing_array_filter, get_housing_array_filter, get_conn
)
from core.logic import (
    calculate_person_monthly_totals,
)
from utils.utils import human_date, format_currency
from routes.home import home
from routes.guide import simple_summary_view, guide_view
from routes.admin import (
    manage_payment_codes, update_payment_codes,
    demo_sync_page, sync_demo_database, demo_sync_status,
    get_month_lock_status, lock_month_api, unlock_month_api,
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
from routes.stats import (
    stats_page,
    get_salary_by_housing_array,
    get_salary_by_guide,
    get_hours_distribution,
    get_extras_distribution,
    get_monthly_trends,
    get_comparison_data,
    get_shift_types_distribution,
    get_all_stats,
    get_compare_housing_arrays,
    get_top_apartments_by_percent,
    get_apartments_in_array,
    get_apartments_in_array_by_percent,
    get_apartment_details,
    get_guide_yearly,
    get_housing_arrays_list,
    get_apartments_list,
    get_guides_list,
)
from routes.auth import login_page, login_submit, logout
from core.auth import validate_session_token, SESSION_COOKIE_NAME

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global flag to track shutdown
_shutting_down = False


def cleanup_resources():
    """Clean up resources before shutdown."""
    global _shutting_down
    if _shutting_down:
        return
    
    _shutting_down = True
    logger.info("Cleaning up resources...")
    
    try:
        # Close database connection pools
        close_all_pools()
        logger.info("Resource cleanup completed")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    cleanup_resources()
    sys.exit(0)


# Register signal handlers for graceful shutdown
if hasattr(signal, 'SIGTERM'):
    signal.signal(signal.SIGTERM, signal_handler)
if hasattr(signal, 'SIGINT'):
    signal.signal(signal.SIGINT, signal_handler)

# Register cleanup on exit
atexit.register(cleanup_resources)

# FastAPI app setup
app = FastAPI(title="ניהול משמרות בענן")
templates = Jinja2Templates(directory=str(config.TEMPLATES_DIR))
templates.env.filters["human_date"] = human_date
templates.env.filters["format_currency"] = format_currency
templates.env.globals["app_version"] = config.VERSION


# Middleware to set demo mode and housing array filter from cookies
class DemoModeMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Set demo mode based on cookie
        demo_mode = get_demo_mode_from_cookie(request)
        set_demo_mode(demo_mode)
        # Set housing array filter based on cookie
        housing_array_id = get_housing_array_from_cookie(request)
        set_housing_array_filter(housing_array_id)
        response = await call_next(request)
        return response


app.add_middleware(DemoModeMiddleware)


# Middleware לאימות משתמשים
class AuthMiddleware(BaseHTTPMiddleware):
    """Middleware לבדיקת התחברות בכל בקשה."""

    # נתיבים שלא דורשים התחברות
    PUBLIC_ROUTES = {"/login", "/static", "/health"}

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # בדיקת session cookie
        token = request.cookies.get(SESSION_COOKIE_NAME)
        user = validate_session_token(token) if token else None

        # שמירת פרטי המשתמש ב-request state (גם אם None)
        request.state.current_user = user

        # נתיבים ציבוריים - לא צריך בדיקה
        if any(path.startswith(route) for route in self.PUBLIC_ROUTES):
            return await call_next(request)

        if not user:
            # הפניה לעמוד התחברות עבור בקשות HTML
            if "text/html" in request.headers.get("accept", ""):
                return RedirectResponse(url="/login", status_code=303)
            # שגיאה 401 עבור בקשות API
            return JSONResponse(
                {"error": "לא מחובר למערכת"},
                status_code=401
            )

        # עבור מנהל מסגרת - כפה סינון לפי המערך שלו
        if user.get("role") == "framework_manager" and user.get("housing_array_id"):
            set_housing_array_filter(user["housing_array_id"])

        response = await call_next(request)
        return response


app.add_middleware(AuthMiddleware)

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
        from core.database import get_conn
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


# Auth routes
@app.get("/login", response_class=HTMLResponse)
def login_route(request: Request, error: str = None):
    """עמוד התחברות."""
    return login_page(request, error)


@app.post("/login")
async def login_submit_route(request: Request):
    """עיבוד טופס התחברות."""
    return await login_submit(request)


@app.get("/logout")
def logout_route(request: Request):
    """התנתקות מהמערכת."""
    return logout(request)


@app.get("/", response_class=HTMLResponse)
def home_route(request: Request, month: int | None = None, year: int | None = None, q: str | None = None):
    """Home page route."""
    return home(request, month, year, q)


@app.get("/guide", include_in_schema=False)
@app.get("/guide/", include_in_schema=False)
def redirect_to_home():
    """Redirect /guide to home page."""
    return RedirectResponse(url="/")


@app.get("/guide/{person_id}/simple", response_class=HTMLResponse)
def simple_summary_route(request: Request, person_id: int, month: int | None = None, year: int | None = None):
    """Simple summary view for a guide."""
    return simple_summary_view(request, person_id, month, year)


@app.get("/guide/{person_id}", response_class=HTMLResponse)
def guide_route(request: Request, person_id: int, month: int | None = None, year: int | None = None):
    """Detailed guide view."""
    return guide_view(request, person_id, month, year)


@app.get("/admin", include_in_schema=False)
@app.get("/admin/", include_in_schema=False)
def redirect_admin_to_home():
    """Redirect /admin to home page."""
    return RedirectResponse(url="/")


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


# Statistics routes
@app.get("/stats", response_class=HTMLResponse)
def stats_route(request: Request, year: int = None, month: int = None):
    """Statistics dashboard page."""
    return stats_page(request, year, month)


@app.get("/api/stats/by-housing")
def stats_by_housing_route(year: int, month: int):
    """Get salary by housing array."""
    return get_salary_by_housing_array(year, month)


@app.get("/api/stats/by-guide")
def stats_by_guide_route(year: int, month: int, limit: int = 20):
    """Get salary by guide."""
    return get_salary_by_guide(year, month, limit)


@app.get("/api/stats/hours-distribution")
def stats_hours_distribution_route(year: int, month: int):
    """Get hours distribution."""
    return get_hours_distribution(year, month)


@app.get("/api/stats/extras-distribution")
def stats_extras_distribution_route(year: int, month: int):
    """Get extras distribution (standby, vacation, sick)."""
    return get_extras_distribution(year, month)


@app.get("/api/stats/monthly-trends")
def stats_monthly_trends_route(year: int, months_back: int = 6):
    """Get monthly trends."""
    return get_monthly_trends(year, months_back)


@app.get("/api/stats/comparison")
def stats_comparison_route(year1: int, month1: int, year2: int, month2: int):
    """Get comparison data between two months."""
    return get_comparison_data(year1, month1, year2, month2)


@app.get("/api/stats/shift-types")
def stats_shift_types_route(year: int, month: int):
    """Get shift types distribution."""
    return get_shift_types_distribution(year, month)


@app.get("/api/stats/all")
def stats_all_route(year: int, month: int):
    """Get all stats data in one call - faster loading."""
    return get_all_stats(year, month)


@app.get("/api/stats/compare-arrays")
def stats_compare_arrays_route(year: int, month: int, array_ids: str):
    """Compare 2-5 housing arrays."""
    ids = [int(x) for x in array_ids.split(",") if x.strip()]
    return get_compare_housing_arrays(year, month, ids)


@app.get("/api/stats/top-apartments")
def stats_top_apartments_route(year: int, month: int, percent: int = 100, limit: int = 10):
    """Get top apartments by percent."""
    return get_top_apartments_by_percent(year, month, percent, limit)


@app.get("/api/stats/apartments-in-array")
def stats_apartments_in_array_route(year: int, month: int, housing_array_id: int):
    """Get all apartments in a housing array."""
    return get_apartments_in_array(year, month, housing_array_id)


@app.get("/api/stats/apartments-in-array-by-percent")
def stats_apartments_in_array_by_percent_route(year: int, month: int, housing_array_id: int):
    """Get apartments in array with percent breakdown."""
    return get_apartments_in_array_by_percent(year, month, housing_array_id)


@app.get("/api/stats/apartment-details")
def stats_apartment_details_route(year: int, month: int, apartment_id: int):
    """Get apartment details - shifts and guides."""
    return get_apartment_details(year, month, apartment_id)


@app.get("/api/stats/guide-yearly")
def stats_guide_yearly_route(person_id: int, year: int):
    """Get guide yearly trend - 12 months."""
    return get_guide_yearly(person_id, year)


@app.get("/api/stats/housing-arrays")
def stats_housing_arrays_route():
    """Get list of housing arrays."""
    return get_housing_arrays_list()


@app.get("/api/stats/apartments")
def stats_apartments_route(housing_array_id: int = None):
    """Get list of apartments."""
    return get_apartments_list(housing_array_id)


@app.get("/api/stats/guides")
def stats_guides_route():
    """Get list of active guides."""
    return get_guides_list()


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
    # Verify password (from environment variable)
    try:
        body = await request.json()
        password = body.get("password", "")
    except:
        password = ""

    # Password must be configured in environment variable DEMO_MODE_PASSWORD
    if not config.DEMO_MODE_PASSWORD:
        logger.error("DEMO_MODE_PASSWORD not configured in environment")
        return JSONResponse({"success": False, "error": "סיסמה לא מוגדרת במערכת"}, status_code=500)

    if password != config.DEMO_MODE_PASSWORD:
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


@app.get("/api/housing-arrays")
def get_housing_arrays():
    """מחזיר רשימת כל מערכי הדיור."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, name FROM housing_arrays ORDER BY name"
        ).fetchall()
    return [{"id": r["id"], "name": r["name"]} for r in rows]


@app.post("/api/set-housing-array-filter")
async def set_housing_array_filter_api(request: Request):
    """מגדיר את מערך הדיור לסינון (שומר בעוגייה)."""
    try:
        body = await request.json()
        housing_array_id = body.get("housing_array_id")
    except Exception:
        housing_array_id = None

    response = JSONResponse({
        "success": True,
        "housing_array_id": housing_array_id
    })

    if housing_array_id is not None:
        response.set_cookie(
            key="housing_array_id",
            value=str(housing_array_id),
            max_age=86400 * 30,  # 30 days
            httponly=False,
            samesite="lax"
        )
    else:
        response.delete_cookie("housing_array_id")

    return response


@app.get("/api/housing-array-status")
def housing_array_status(request: Request):
    """מחזיר את מצב הסינון הנוכחי לפי מערך דיור."""
    current_id = get_housing_array_from_cookie(request)
    current_name = None
    if current_id:
        with get_conn() as conn:
            row = conn.execute(
                "SELECT name FROM housing_arrays WHERE id = %s",
                (current_id,)
            ).fetchone()
            if row:
                current_name = row["name"]
    return {
        "housing_array_id": current_id,
        "housing_array_name": current_name
    }


@app.on_event("startup")
async def startup_event():
    """Handle application startup - ensure database has required codes."""
    from core.logic import ensure_sick_payment_code
    from core.database import get_conn
    try:
        with get_conn() as conn:
            ensure_sick_payment_code(conn.conn)
    except Exception as e:
        logger.warning(f"Could not ensure sick payment code on startup: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    """Handle application shutdown."""
    logger.info("Application shutting down...")
    cleanup_resources()


if __name__ == "__main__":
    import uvicorn
    try:
        uvicorn.run(
            "app:app",
            host=config.HOST,
            port=config.PORT,
            reload=config.DEBUG
        )
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
        cleanup_resources()
    except Exception as e:
        logger.error(f"Error running application: {e}")
        cleanup_resources()
        raise
