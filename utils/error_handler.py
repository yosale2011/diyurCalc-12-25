"""
Error handling module for DiyurCalc application.
Provides centralized error handling, logging, and user-friendly error messages.
"""

from __future__ import annotations
import logging
import traceback
from typing import Any, Optional
from datetime import datetime
from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

# Configure logging with more detail
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('diyur_calc.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Templates for error pages
templates = Jinja2Templates(directory="templates")


class DiyurCalcError(Exception):
    """Base exception for all DiyurCalc errors"""
    def __init__(self, message: str, details: Optional[dict] = None, user_message: Optional[str] = None):
        self.message = message
        self.details = details or {}
        self.user_message = user_message or message
        super().__init__(self.message)


class DatabaseError(DiyurCalcError):
    """Database-related errors"""
    pass


class CalculationError(DiyurCalcError):
    """Calculation-related errors"""
    pass


class ValidationError(DiyurCalcError):
    """Input validation errors"""
    pass


class DataIntegrityError(DiyurCalcError):
    """Data integrity and consistency errors"""
    pass


class ExportError(DiyurCalcError):
    """Export-related errors"""
    pass


def log_error(error: Exception, context: Optional[dict] = None) -> str:
    """
    Log an error with full context and return error ID.

    Args:
        error: The exception that occurred
        context: Additional context (user, operation, parameters)

    Returns:
        Error ID for tracking
    """
    error_id = f"{datetime.now().timestamp():.0f}"

    error_details = {
        'error_id': error_id,
        'error_type': type(error).__name__,
        'error_message': str(error),
        'timestamp': datetime.now().isoformat(),
        'context': context or {}
    }

    # Log full traceback for debugging
    if isinstance(error, DiyurCalcError):
        logger.error(f"Application error {error_id}: {error_details}")
    else:
        logger.error(f"Unexpected error {error_id}: {error_details}", exc_info=True)

    return error_id


def safe_database_operation(operation_name: str):
    """
    Decorator for safe database operations with automatic rollback.

    Usage:
        @safe_database_operation("fetch_employee_data")
        def get_employee(conn, employee_id):
            ...
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            conn = None
            try:
                # Find connection in args or kwargs
                if args and hasattr(args[0], 'execute'):
                    conn = args[0]
                elif 'conn' in kwargs:
                    conn = kwargs['conn']

                result = func(*args, **kwargs)
                return result

            except Exception as e:
                if conn:
                    try:
                        conn.rollback()
                        logger.info(f"Rolled back transaction for {operation_name}")
                    except Exception as rollback_error:
                        logger.error(f"Rollback failed for {operation_name}: {rollback_error}")

                # Convert to appropriate error type
                if "database" in str(e).lower() or "postgres" in str(e).lower() or "psycopg" in str(e).lower():
                    raise DatabaseError(
                        f"Database operation failed: {operation_name}",
                        details={'original_error': str(e)},
                        user_message="אירעה שגיאה בגישה לבסיס הנתונים. נסה שנית."
                    )
                raise

        wrapper.__name__ = func.__name__
        wrapper.__doc__ = func.__doc__
        return wrapper
    return decorator


def validate_input(validation_rules: dict):
    """
    Decorator for input validation.

    Usage:
        @validate_input({
            'employee_id': {'type': int, 'min': 1},
            'month': {'type': int, 'min': 1, 'max': 12},
            'year': {'type': int, 'min': 2020, 'max': 2030}
        })
        def process_report(employee_id, month, year):
            ...
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Validate each parameter
            for param_name, rules in validation_rules.items():
                if param_name in kwargs:
                    value = kwargs[param_name]

                    # Type validation
                    if 'type' in rules and not isinstance(value, rules['type']):
                        raise ValidationError(
                            f"Invalid type for {param_name}",
                            details={'expected': rules['type'].__name__, 'got': type(value).__name__},
                            user_message=f"ערך לא תקין עבור {param_name}"
                        )

                    # Range validation
                    if 'min' in rules and value < rules['min']:
                        raise ValidationError(
                            f"{param_name} is below minimum",
                            details={'min': rules['min'], 'got': value},
                            user_message=f"{param_name} חייב להיות לפחות {rules['min']}"
                        )

                    if 'max' in rules and value > rules['max']:
                        raise ValidationError(
                            f"{param_name} exceeds maximum",
                            details={'max': rules['max'], 'got': value},
                            user_message=f"{param_name} חייב להיות לכל היותר {rules['max']}"
                        )

            return func(*args, **kwargs)

        wrapper.__name__ = func.__name__
        wrapper.__doc__ = func.__doc__
        return wrapper
    return decorator


async def handle_application_error(request: Request, exc: DiyurCalcError) -> HTMLResponse:
    """
    Handle application-specific errors with user-friendly messages.
    """
    error_id = log_error(exc, context={'path': request.url.path, 'method': request.method})

    # For API endpoints, return JSON
    if request.url.path.startswith('/api/'):
        return JSONResponse(
            status_code=400,
            content={
                'error': exc.user_message,
                'error_id': error_id,
                'details': exc.details if hasattr(exc, 'details') else {}
            }
        )

    # For web pages, return HTML
    return templates.TemplateResponse(
        "error.html",
        {
            "request": request,
            "error_message": exc.user_message,
            "error_id": error_id,
            "back_url": request.headers.get('referer', '/')
        },
        status_code=400
    )


async def handle_unexpected_error(request: Request, exc: Exception) -> HTMLResponse:
    """
    Handle unexpected errors with generic message (no sensitive info).
    """
    error_id = log_error(exc, context={'path': request.url.path, 'method': request.method})

    # For API endpoints
    if request.url.path.startswith('/api/'):
        return JSONResponse(
            status_code=500,
            content={
                'error': 'אירעה שגיאה בלתי צפויה',
                'error_id': error_id
            }
        )

    # For web pages
    return templates.TemplateResponse(
        "error.html",
        {
            "request": request,
            "error_message": "אירעה שגיאה בלתי צפויה. הבעיה נרשמה ותטופל בהקדם.",
            "error_id": error_id,
            "back_url": "/"
        },
        status_code=500
    )


def sanitize_error_message(message: str) -> str:
    """
    Remove sensitive information from error messages before showing to users.
    """
    # Remove file paths
    import re
    message = re.sub(r'[A-Z]:[\\\/][\w\\\/\-\.]+', '[PATH]', message)

    # Remove SQL queries
    message = re.sub(r'(SELECT|INSERT|UPDATE|DELETE|FROM|WHERE).*', '[QUERY]', message, flags=re.IGNORECASE)

    # Remove stack traces
    message = re.sub(r'File ".*", line \d+.*', '[TRACE]', message)

    return message