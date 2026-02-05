"""
מודול אימות למערכת DiyurCalc.
מטפל באימות סיסמאות, ניהול sessions והרשאות.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from passlib.hash import bcrypt
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

from core.config import config
from core.database import get_conn

logger = logging.getLogger(__name__)

# תפקידים מורשים להתחברות (role_id 1 ו-2)
AUTHORIZED_ROLES = {"super_admin", "framework_manager"}

# משך תוקף session (24 שעות)
SESSION_MAX_AGE = 86400

# שם ה-cookie של ה-session
SESSION_COOKIE_NAME = "session"


def verify_password(password: str, stored_password: str) -> bool:
    """
    אימות סיסמה מול הסיסמה השמורה.
    תומך גם ב-bcrypt hash וגם בסיסמאות טקסט רגיל (לתאימות לאחור).
    """
    try:
        # בדיקה אם זה bcrypt hash (מתחיל ב-$2a$, $2b$, או $2y$)
        if stored_password.startswith("$2"):
            return bcrypt.verify(password, stored_password)
        else:
            # סיסמה בטקסט רגיל - השוואה ישירה
            return password == stored_password
    except Exception as e:
        logger.warning(f"שגיאה באימות סיסמה: {e}")
        return False


def _get_session_serializer() -> URLSafeTimedSerializer:
    """יצירת serializer עבור session tokens."""
    return URLSafeTimedSerializer(config.SECRET_KEY)


def create_session_token(person_id: int, name: str, role: str, housing_array_id: int = None) -> str:
    """יצירת token חתום עבור session."""
    serializer = _get_session_serializer()
    data = {
        "person_id": person_id,
        "name": name,
        "role": role,
        "housing_array_id": housing_array_id,
        "created": datetime.now().isoformat()
    }
    return serializer.dumps(data)


def validate_session_token(token: str) -> Optional[dict]:
    """אימות ופענוח session token."""
    if not token:
        return None
    serializer = _get_session_serializer()
    try:
        data = serializer.loads(token, max_age=SESSION_MAX_AGE)
        return data
    except (BadSignature, SignatureExpired):
        return None


def can_login(role_name: str) -> bool:
    """בדיקה האם לתפקיד יש הרשאה להתחבר."""
    return role_name in AUTHORIZED_ROLES


def authenticate_user(id_number: str, password: str) -> tuple[bool, Optional[dict], str]:
    """
    אימות משתמש לפי תעודת זהות וסיסמה.

    Returns:
        (success, user_data, error_message)
    """
    if not id_number or not password:
        return False, None, "יש למלא את כל השדות"

    try:
        with get_conn() as conn:
            # שליפת המשתמש עם התפקיד שלו
            cursor = conn.execute("""
                SELECT p.id, p.name, p.password, p.is_active, p.housing_array_id, r.name as role_name
                FROM people p
                LEFT JOIN roles r ON r.id = p.role_id
                WHERE p.id_number = %s
            """, (id_number,))

            row = cursor.fetchone()

            if not row:
                logger.info(f"ניסיון התחברות נכשל - לא נמצא משתמש עם ת.ז.: {id_number[:4]}***")
                return False, None, "תעודת זהות או סיסמה שגויים"

            # בדיקה האם המשתמש פעיל
            if not row["is_active"]:
                return False, None, "החשבון אינו פעיל"

            # בדיקת סיסמה
            if not row["password"]:
                return False, None, "לא הוגדרה סיסמה למשתמש זה"

            if not verify_password(password, row["password"]):
                logger.info(f"ניסיון התחברות נכשל - סיסמה שגויה עבור: {row['name']}")
                return False, None, "תעודת זהות או סיסמה שגויים"

            # בדיקת הרשאת תפקיד
            role_name = row["role_name"] or ""
            if not can_login(role_name):
                logger.info(f"ניסיון התחברות נכשל - תפקיד לא מורשה: {role_name} עבור: {row['name']}")
                return False, None, "אין לך הרשאה להתחבר למערכת"

            # תיעוד כניסה מוצלחת
            _log_login(conn, row["id"], True)

            logger.info(f"התחברות מוצלחת: {row['name']} ({role_name})")

            user_data = {
                "person_id": row["id"],
                "name": row["name"],
                "role": role_name,
                "housing_array_id": row["housing_array_id"]
            }

            return True, user_data, ""

    except Exception as e:
        logger.error(f"שגיאה באימות משתמש: {e}")
        return False, None, f"שגיאת מערכת: {e}"


def _log_login(conn, person_id: int, success: bool, ip_address: str = None) -> None:
    """תיעוד ניסיון התחברות."""
    try:
        conn.execute("""
            INSERT INTO login_logs (person_id, login_time, success, ip_address)
            VALUES (%s, %s, %s, %s)
        """, (person_id, datetime.now(), success, ip_address))
    except Exception as e:
        logger.warning(f"שגיאה בתיעוד התחברות: {e}")


# =============================================================================
# פונקציות בדיקת הרשאות
# =============================================================================


def is_super_admin(request) -> bool:
    """בודק אם המשתמש המחובר הוא מנהל על."""
    user = getattr(request.state, 'current_user', None)
    return user is not None and user.get('role') == 'super_admin'


def is_framework_manager(request) -> bool:
    """בודק אם המשתמש המחובר הוא מנהל מסגרת."""
    user = getattr(request.state, 'current_user', None)
    return user is not None and user.get('role') == 'framework_manager'


def get_user_housing_array(request) -> Optional[int]:
    """
    מחזיר את מערך הדיור של המשתמש המחובר.

    Returns:
        מזהה מערך הדיור אם מנהל מסגרת, None אם מנהל על.
    """
    user = getattr(request.state, 'current_user', None)
    if user and user.get('role') == 'framework_manager':
        return user.get('housing_array_id')
    return None
