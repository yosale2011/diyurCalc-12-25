"""
Routes להתחברות והתנתקות מהמערכת.
"""
from __future__ import annotations

import logging

from fastapi import Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from core.config import config
from core.auth import (
    authenticate_user,
    create_session_token,
    SESSION_COOKIE_NAME,
)

logger = logging.getLogger(__name__)

templates = Jinja2Templates(directory=str(config.TEMPLATES_DIR))
templates.env.globals["app_version"] = config.VERSION


def login_page(request: Request, error: str = None) -> HTMLResponse:
    """הצגת עמוד ההתחברות."""
    return templates.TemplateResponse("login.html", {
        "request": request,
        "error": error
    })


async def login_submit(request: Request) -> HTMLResponse | RedirectResponse:
    """עיבוד טופס התחברות."""
    try:
        form_data = await request.form()
        id_number = form_data.get("id_number", "").strip()
        password = form_data.get("password", "")

        success, user_data, error_msg = authenticate_user(id_number, password)

        if not success:
            return templates.TemplateResponse("login.html", {
                "request": request,
                "error": error_msg,
                "id_number": id_number
            })

        # יצירת session token
        token = create_session_token(
            user_data["person_id"],
            user_data["name"],
            user_data["role"],
            user_data.get("housing_array_id")
        )

        # הפניה לדף הבית עם cookie
        response = RedirectResponse(url="/", status_code=303)
        response.set_cookie(
            key=SESSION_COOKIE_NAME,
            value=token,
            max_age=86400,  # 24 שעות
            httponly=True,
            samesite="lax",
            secure=config.is_production()
        )

        return response

    except Exception as e:
        logger.error(f"שגיאה בהתחברות: {e}")
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": f"שגיאת מערכת: {e}",
            "id_number": form_data.get("id_number", "") if 'form_data' in dir() else ""
        })


def logout(request: Request) -> RedirectResponse:
    """התנתקות מהמערכת."""
    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie(SESSION_COOKIE_NAME)
    return response
