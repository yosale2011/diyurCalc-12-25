"""
Refactored main application file for DiyurCalc.
Uses modular structure with separate route handlers.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
import psycopg2

from config import config
from logic import (
    calculate_person_monthly_totals,
)
from utils import human_date
from utils import calculate_accruals, format_currency
from routes.home import home
from routes.guide import simple_summary_view, guide_view
from routes.admin import manage_payment_codes, update_payment_codes
from routes.summary import general_summary
from routes.export import (
    export_gesher,
    export_gesher_person,
    export_gesher_multiple,
    export_gesher_preview,
    export_excel,
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host=config.HOST,
        port=config.PORT,
        reload=config.DEBUG
    )
