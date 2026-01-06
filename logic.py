import logging
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta, date
from typing import Iterable, List, Tuple, Dict, Optional, Any, Callable
from zoneinfo import ZoneInfo

from convertdate import hebrew

# Import utilities and config
from config import config
from cache_manager import cached, cache

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection string
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_CONNECTION_STRING = os.getenv("DATABASE_URL")
if not DB_CONNECTION_STRING:
    raise RuntimeError("DATABASE_URL environment variable is required. Please set it in .env file.")

def get_db_connection():
    """Create and return a PostgreSQL database connection."""
    try:
        conn = psycopg2.connect(DB_CONNECTION_STRING)
        # Don't set cursor_factory at connection level - let each cursor decide
        return conn
    except psycopg2.OperationalError as e:
        error_msg = str(e)
        if "could not translate host name" in error_msg or "Name or service not known" in error_msg:
            logger.error(
                f"Database DNS resolution failed. Hostname cannot be resolved.\n"
                f"Error: {error_msg}\n"
                f"Please check:\n"
                f"1. Your internet connection\n"
                f"2. DNS settings\n"
                f"3. VPN/firewall configuration\n"
                f"4. Database hostname in DATABASE_URL is correct"
            )
        elif "connection refused" in error_msg.lower():
            logger.error(
                f"Database connection refused.\n"
                f"Error: {error_msg}\n"
                f"Please check:\n"
                f"1. Database server is running\n"
                f"2. Port number is correct\n"
                f"3. Firewall allows connections"
            )
        else:
            logger.error(f"Database connection error: {error_msg}")
        raise
    except Exception as e:
        logger.error(f"Unexpected database connection error: {e}")
        raise


def dict_cursor(conn):
    """Create a cursor that returns rows as dicts, avoiding psycopg2.extras.RealDictCursor bugs."""
    return conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

# =============================================================================
# Constants
# =============================================================================

# Time constants (in minutes)
MINUTES_PER_HOUR = 60
MINUTES_PER_DAY = 24 * MINUTES_PER_HOUR  # 1440

# Work hour thresholds (in minutes)
REGULAR_HOURS_LIMIT = 8 * MINUTES_PER_HOUR   # 480 - First 8 hours at 100%
OVERTIME_125_LIMIT = 10 * MINUTES_PER_HOUR   # 600 - Hours 9-10 at 125%
# Beyond 600 minutes = 150%

# Work day boundaries
WORK_DAY_START_MINUTES = 8 * MINUTES_PER_HOUR  # 480 = 08:00

# Shabbat defaults (when not found in DB)
SHABBAT_ENTER_DEFAULT = 16 * MINUTES_PER_HOUR  # 960 = 16:00 on Friday
SHABBAT_EXIT_DEFAULT = 22 * MINUTES_PER_HOUR   # 1320 = 22:00 on Saturday

# Break threshold (in minutes) - breaks longer than this split work chains
BREAK_THRESHOLD_MINUTES = 60

# Standby cancellation threshold
# If work overlaps with standby by more than this percentage, standby is cancelled
STANDBY_CANCEL_OVERLAP_THRESHOLD = 0.70  # 70%

# Wage/Accrual constants (some imported from config)
DEFAULT_MINIMUM_WAGE = 34.40
DEFAULT_STANDBY_RATE = 70.0
STANDARD_WORK_DAYS_PER_MONTH = config.STANDARD_WORK_DAYS_PER_MONTH
MAX_SICK_DAYS_PER_MONTH = config.MAX_SICK_DAYS_PER_MONTH

# Weekday indices (Python's weekday())
FRIDAY = 4
SATURDAY = 5

LOCAL_TZ = ZoneInfo("Asia/Jerusalem")




def to_local_date(ts: int | datetime | date) -> date:
    """Convert epoch timestamp, datetime, or date object to local date."""
    if isinstance(ts, date) and not isinstance(ts, datetime):
        # Already a date object (PostgreSQL can return date directly)
        return ts
    if isinstance(ts, datetime):
        # PostgreSQL returns datetime objects directly
        if ts.tzinfo is None:
            # Assume UTC if no timezone
            return ts.replace(tzinfo=ZoneInfo("UTC")).astimezone(LOCAL_TZ).date()
        return ts.astimezone(LOCAL_TZ).date()
    # SQLite returns epoch timestamps
    return datetime.fromtimestamp(ts, LOCAL_TZ).date()


SHABBAT_CACHE_KEY = "shabbat_times_cache"
SHABBAT_CACHE_TTL = 86400  # 24 hours

def get_shabbat_times_cache(conn) -> Dict[str, Dict[str, Any]]:
    """
    Load Shabbat times from DB into a dictionary with 24-hour caching.
    Key: Date string (YYYY-MM-DD) representing the day.
    Value: {'enter': HH:MM, 'exit': HH:MM, 'parsha': str, 'holiday': str}
    """
    # Check cache first
    cached_result = cache.get(SHABBAT_CACHE_KEY)
    if cached_result is not None:
        return cached_result

    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("SELECT shabbat_date, candle_lighting, havdalah, parsha, holiday_name FROM shabbat_times")
        rows = cursor.fetchall()
        result = {}
        for r in rows:
            if r["shabbat_date"]:
                result[r["shabbat_date"]] = {
                    "enter": r["candle_lighting"], 
                    "exit": r["havdalah"],
                    "parsha": r["parsha"],
                    "holiday": r["holiday_name"]
                }
        cursor.close()

        # Store in cache
        cache.set(SHABBAT_CACHE_KEY, result, SHABBAT_CACHE_TTL)
        return result
    except Exception as e:
        logger.warning(f"Failed to load shabbat times cache: {e}")
        return {}


MINIMUM_WAGE_CACHE_KEY = "minimum_wage_cache"
MINIMUM_WAGE_CACHE_TTL = 86400  # 24 hours
DEFAULT_MINIMUM_WAGE = 34.40

def get_minimum_wage(conn) -> float:
    """
    Get current minimum wage rate from DB with 24-hour caching.
    Returns hourly rate in shekels.
    """
    # Check cache first
    cached_result = cache.get(MINIMUM_WAGE_CACHE_KEY)
    if cached_result is not None:
        return cached_result

    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("SELECT hourly_rate FROM minimum_wage_rates ORDER BY effective_from DESC LIMIT 1")
        row = cursor.fetchone()
        cursor.close()

        if row and row["hourly_rate"]:
            result = float(row["hourly_rate"]) / 100  # Convert from agorot to shekels
        else:
            result = DEFAULT_MINIMUM_WAGE

        # Store in cache
        cache.set(MINIMUM_WAGE_CACHE_KEY, result, MINIMUM_WAGE_CACHE_TTL)
        return result
    except Exception as e:
        logger.warning(f"Failed to get minimum wage, using default: {e}")
        return DEFAULT_MINIMUM_WAGE


def get_standby_rate(conn, segment_id: int, apartment_type_id: int | None, is_married: bool, year: int = None, month: int = None) -> float:
    """
    Get standby rate from standby_rates table.
    Priority: specific apartment_type (priority=10) > general (priority=0)
    If year/month provided, checks historical rates first.

    Args:
        conn: Database connection
        segment_id: ID of the standby segment from shift_time_segments
        apartment_type_id: Type of apartment (None for general)
        is_married: True if person is married, False if single
        year: Optional year for historical lookup
        month: Optional month for historical lookup

    Returns:
        Standby rate in shekels (amount / 100)
    """
    marital_status = "married" if is_married else "single"

    # If year/month provided, try historical rates first
    if year is not None and month is not None:
        from history import get_standby_rate_for_month
        historical_amount = get_standby_rate_for_month(
            conn, segment_id, apartment_type_id, marital_status, year, month
        )
        if historical_amount is not None:
            return float(historical_amount) / 100

    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # First try to find specific rate for apartment type (priority=10)
    if apartment_type_id is not None:
        cursor.execute("""
            SELECT amount FROM standby_rates
            WHERE segment_id = %s AND apartment_type_id = %s AND marital_status = %s AND priority = 10
            LIMIT 1
        """, (segment_id, apartment_type_id, marital_status))
        row = cursor.fetchone()
        if row:
            cursor.close()
            return float(row["amount"]) / 100

    # Fallback to general rate (priority=0, apartment_type_id IS NULL)
    cursor.execute("""
        SELECT amount FROM standby_rates
        WHERE segment_id = %s AND apartment_type_id IS NULL AND marital_status = %s AND priority = 0
        LIMIT 1
    """, (segment_id, marital_status))
    row = cursor.fetchone()
    cursor.close()

    if row:
        return float(row["amount"]) / 100

    # Default fallback if nothing found
    return DEFAULT_STANDBY_RATE


def available_months(rows: Iterable[Dict]) -> List[Tuple[int, int]]:
    months: set[Tuple[int, int]] = set()
    for r in rows:
        ts = r["date"]
        if not ts:
            continue
        d = to_local_date(ts)
        months.add((d.year, d.month))
    return sorted(months)


@cached(ttl=300)  # Cache for 5 minutes
def available_months_from_db() -> List[Tuple[int, int]]:
    """Fetch distinct months from time_reports table."""
    from database import get_pooled_connection, return_connection
    conn = get_pooled_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # Optimized: extract year/month directly in SQL instead of fetching all dates
        cursor.execute("""
            SELECT DISTINCT
                EXTRACT(YEAR FROM date)::integer AS year,
                EXTRACT(MONTH FROM date)::integer AS month
            FROM time_reports
            WHERE date IS NOT NULL
            ORDER BY year, month
        """)
        rows = cursor.fetchall()
    finally:
        cursor.close()
        return_connection(conn)

    return [(r["year"], r["month"]) for r in rows]


@cached(ttl=1800)  # Cache for 30 minutes since guide data changes infrequently
def get_active_guides() -> List[Dict[str, Any]]:
    """Fetch active guides from people table."""
    from database import get_pooled_connection, return_connection
    conn = get_pooled_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(
            """
            SELECT id, name, type, is_active, start_date
            FROM people
            WHERE is_active::integer = 1
            ORDER BY name
            """
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()
        return_connection(conn)

    return [dict(row) for row in rows]


def get_available_months_for_person(conn, person_id: int) -> List[Tuple[int, int]]:
    """
    Fetch distinct months for a specific person efficiently using SQL.
    """
    cursor = conn.cursor()
    try:
        # Postgres specific optimization
        # date is timestamp without time zone, extract year/month directly
        cursor.execute("""
            SELECT DISTINCT 
                CAST(EXTRACT(YEAR FROM date) AS INTEGER) as year,
                CAST(EXTRACT(MONTH FROM date) AS INTEGER) as month
            FROM time_reports 
            WHERE person_id = %s
            ORDER BY year DESC, month DESC
        """, (person_id,))
        rows = cursor.fetchall()
        return [(r[0], r[1]) for r in rows]
    except Exception as e:
        logger.warning(f"Error fetching months for person {person_id}: {e}")
        return []
    finally:
        cursor.close()




def parse_hhmm(value: str) -> Tuple[int, int]:
    """Return (hours, minutes) integers from 'HH:MM'."""
    h, m = value.split(":")
    return int(h), int(m)


def span_minutes(start_str: str, end_str: str) -> Tuple[int, int]:
    """Return start/end minutes-from-midnight, handling overnight end < start."""
    sh, sm = parse_hhmm(start_str)
    eh, em = parse_hhmm(end_str)
    start = sh * MINUTES_PER_HOUR + sm
    end = eh * MINUTES_PER_HOUR + em
    if end <= start:
        end += MINUTES_PER_DAY
    return start, end


def minutes_to_time_str(minutes: int) -> str:
    """Convert minutes from midnight to HH:MM format (handles >24h wrapping)."""
    day_minutes = minutes % MINUTES_PER_DAY
    h = day_minutes // MINUTES_PER_HOUR
    m = day_minutes % MINUTES_PER_HOUR
    return f"{h:02d}:{m:02d}"


def is_shabbat_time(
    day_of_week: int,
    minute_in_day: int,
    shift_id: int,
    current_date: date,
    shabbat_cache: Dict[str, Dict[str, str]]
) -> bool:
    """
    Check if a specific time is within Shabbat hours.

    Args:
        day_of_week: Python weekday (0=Monday, 4=Friday, 5=Saturday)
        minute_in_day: Minutes from midnight (can be normalized >1440 for times after midnight)
        shift_id: The shift type ID (not used anymore - all shifts use DB times)
        current_date: The current date being checked
        shabbat_cache: Cache of Shabbat times from DB

    Returns:
        True if the time is within Shabbat hours
    """
    # נרמול זמן - אם מעל 1440, זה בוקר של היום הבא
    # לדוגמה: 1830 = 06:30 בבוקר (390 + 1440)
    actual_minute = minute_in_day % MINUTES_PER_DAY  # המרה ל-0-1439

    # Check if it's Friday or Saturday
    if day_of_week not in (FRIDAY, SATURDAY):
        return False

    # Find the Saturday for this shabbat (cache is keyed by Saturday date)
    if day_of_week == FRIDAY:
        target_saturday = current_date + timedelta(days=1)
    else:  # SATURDAY
        target_saturday = current_date

    saturday_str = target_saturday.strftime("%Y-%m-%d")
    shabbat_data = shabbat_cache.get(saturday_str)

    # Use DB times if available
    if shabbat_data:
        try:
            eh, em = map(int, shabbat_data["enter"].split(":"))
            enter_minutes = eh * MINUTES_PER_HOUR + em

            xh, xm = map(int, shabbat_data["exit"].split(":"))
            exit_minutes = xh * MINUTES_PER_HOUR + xm

            if day_of_week == FRIDAY and actual_minute >= enter_minutes:
                return True
            if day_of_week == SATURDAY and actual_minute < exit_minutes:
                return True
            return False
        except (ValueError, KeyError, AttributeError):
            pass

    # Fallback: use default Shabbat times
    if day_of_week == FRIDAY and actual_minute >= SHABBAT_ENTER_DEFAULT:
        return True
    if day_of_week == SATURDAY and actual_minute < SHABBAT_EXIT_DEFAULT:
        return True

    return False


def calculate_wage_rate(
    minutes_in_chain: int,
    is_shabbat: bool
) -> str:
    """
    Determine the wage rate label based on hours worked in chain and Shabbat status.
    
    Args:
        minutes_in_chain: Total minutes worked so far in the current chain
        is_shabbat: Whether this minute falls within Shabbat hours
    
    Returns:
        Rate label: "100%", "125%", "150%", "175%", or "200%"
    """
    if minutes_in_chain <= REGULAR_HOURS_LIMIT:
        return "150%" if is_shabbat else "100%"
    elif minutes_in_chain <= OVERTIME_125_LIMIT:
        return "175%" if is_shabbat else "125%"
    else:
        return "200%" if is_shabbat else "150%"






def get_payment_codes(conn):
    """Fetch payment codes sorted by display_order (symbol numbers)."""
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # Sort by display_order ascending
        cursor.execute("""
            SELECT * FROM payment_codes 
            ORDER BY display_order ASC NULLS LAST
        """)
        result = cursor.fetchall()
        cursor.close()
        return result
    except Exception as e:
        logger.error(f"Error fetching payment codes: {e}")
        return []


# =============================================================================
# פונקציות עזר לחישוב שכר - מאוחדות
# =============================================================================

def _build_daily_map(
    reports: List[Any],
    segments_by_shift: Dict[int, List[Any]],
    year: int,
    month: int
) -> Dict[str, Dict]:
    """
    בניית מפת ימים מדיווחים.
    מחלצת את הלוגיקה המשותפת של בניית daily_map משתי הפונקציות.
    """
    from utils import overlap_minutes
    daily_map = {}

    for r in reports:
        if not r["start_time"] or not r["end_time"] or not r["shift_type_id"]:
            continue

        r_start, r_end = span_minutes(r["start_time"], r["end_time"])
        r_date = to_local_date(r["date"])

        # פיצול משמרות חוצות חצות
        parts = []
        if r_end <= MINUTES_PER_DAY:
            parts.append((r_date, r_start, r_end))
        else:
            parts.append((r_date, r_start, MINUTES_PER_DAY))
            parts.append((r_date + timedelta(days=1), 0, r_end - MINUTES_PER_DAY))

        seg_list = segments_by_shift.get(r["shift_type_id"], [])
        if not seg_list:
            # משמרת ללא סגמנטים מוגדרים - wage_percent=0 מסמן "חשב לפי רצף"
            seg_list = [{"start_time": r["start_time"], "end_time": r["end_time"],
                        "wage_percent": 0, "segment_type": "work", "id": None}]

        work_type = r.get("work_type")
        shift_name = r.get("shift_name") or ""
        is_vacation_report = (work_type == "sick_vacation" or
                             "חופשה" in shift_name or
                             "מחלה" in shift_name)

        # משמרות תגבור - משתמשים בסגמנטים המוגדרים ישירות (לא לפי שעות דיווח)
        # הערה: חופשה/מחלה מטופלות בנפרד - לא כתגבור
        is_tagbur_shift = "תגבור" in shift_name

        # משמרות חופשה/מחלה - סגמנטים קבועים אבל נספרות כחופשה
        is_fixed_vacation_shift = is_vacation_report and not is_tagbur_shift

        # משמרת לילה - סגמנטים דינמיים לפי זמן הכניסה בפועל
        # החוק: 2 שעות ראשונות עבודה, עד 06:30 כוננות, 06:30-08:00 עבודה
        is_night_shift = (shift_name == "משמרת לילה")
        if is_night_shift:
            entry_time = r_start  # זמן הכניסה בדקות
            exit_time = r_end if r_end > entry_time else r_end + MINUTES_PER_DAY

            WORK_FIRST_HOURS = 120  # 2 שעות ראשונות = עבודה
            STANDBY_END = 390  # 06:30 = 390 דקות
            MORNING_WORK_START = 390  # 06:30
            MORNING_WORK_END = 480  # 08:00

            # חישוב הסגמנטים הדינמיים
            dynamic_segments = []

            # סגמנט 1: 2 שעות ראשונות עבודה
            work1_start = entry_time
            work1_end = min(entry_time + WORK_FIRST_HOURS, exit_time)
            if work1_end > work1_start:
                dynamic_segments.append({
                    "start_time": f"{(work1_start // 60) % 24:02d}:{work1_start % 60:02d}",
                    "end_time": f"{(work1_end // 60) % 24:02d}:{work1_end % 60:02d}",
                    "wage_percent": 0,  # 0 = חשב לפי רצף
                    "segment_type": "work",
                    "id": None
                })

            # סגמנט 2: כוננות מסוף 2 שעות עבודה עד 06:30
            standby_start = work1_end
            # 06:30 - אם הכניסה אחרי 12:00, 06:30 הוא למחרת
            standby_end_time = STANDBY_END if entry_time < 720 else STANDBY_END + MINUTES_PER_DAY
            standby_end = min(standby_end_time, exit_time)
            if standby_end > standby_start:
                dynamic_segments.append({
                    "start_time": f"{(standby_start // 60) % 24:02d}:{standby_start % 60:02d}",
                    "end_time": f"{(standby_end // 60) % 24:02d}:{standby_end % 60:02d}",
                    "wage_percent": 24,  # כוננות = 24%
                    "segment_type": "standby",
                    "id": None
                })

            # סגמנט 3: עבודה 06:30-08:00
            morning_start = standby_end_time
            morning_end_time = MORNING_WORK_END if entry_time < 720 else MORNING_WORK_END + MINUTES_PER_DAY
            morning_end = min(morning_end_time, exit_time)
            if morning_end > morning_start and morning_start < exit_time:
                dynamic_segments.append({
                    "start_time": f"{(morning_start // 60) % 24:02d}:{morning_start % 60:02d}",
                    "end_time": f"{(morning_end // 60) % 24:02d}:{morning_end % 60:02d}",
                    "wage_percent": 0,  # 0 = חשב לפי רצף
                    "segment_type": "work",
                    "id": None
                })

            # החלפת רשימת הסגמנטים בסגמנטים הדינמיים
            seg_list = dynamic_segments

        # אם זו משמרת תגבור - מוסיפים את הסגמנטים ישירות בלי לחשב חפיפה עם שעות הדיווח
        if is_tagbur_shift and seg_list:
            display_date = r_date  # יום הדיווח
            day_key = display_date.strftime("%d/%m/%Y")
            entry = daily_map.setdefault(day_key, {"segments": [], "date": display_date})
            entry["is_tagbur"] = True
            if "tagbur_wages" not in entry or not entry["tagbur_wages"]:
                entry["tagbur_wages"] = {"calc100": 0, "calc125": 0, "calc150": 0, "calc175": 0, "calc200": 0}

            # שמירת זמני הסגמנטים לחישוב שבת/חול
            if "tagbur_segments_detail" not in entry:
                entry["tagbur_segments_detail"] = []
            
            for seg in seg_list:
                seg_start, seg_end = span_minutes(seg["start_time"], seg["end_time"])
                duration = seg_end - seg_start

                effective_seg_type = seg["segment_type"]
                if is_vacation_report:
                    effective_seg_type = "vacation"

                segment_id = seg.get("id")
                apartment_type_id = r.get("apartment_type_id")
                is_married = r.get("is_married")
                wage_percent = seg.get("wage_percent", 100)

                # שמירת פרטי הסגמנט לחישוב שבת/חול
                entry["tagbur_segments_detail"].append({
                    "start": seg_start,
                    "end": seg_end,
                    "wage_percent": wage_percent,
                    "date": display_date
                })

                # חישוב לפי אחוז קבוע
                if wage_percent == 100:
                    entry["tagbur_wages"]["calc100"] += duration
                elif wage_percent == 125:
                    entry["tagbur_wages"]["calc125"] += duration
                elif wage_percent == 150:
                    entry["tagbur_wages"]["calc150"] += duration
                elif wage_percent == 175:
                    entry["tagbur_wages"]["calc175"] += duration
                elif wage_percent == 200:
                    entry["tagbur_wages"]["calc200"] += duration
                else:
                    entry["tagbur_wages"]["calc100"] += duration

                # שמירה באותו מבנה כמו סגמנטים רגילים: (start, end, type, shift_id, seg_id, apt_type, married)
                entry["segments"].append((
                    seg_start, seg_end, effective_seg_type,
                    r["shift_type_id"], segment_id, apartment_type_id, is_married
                ))
            continue  # דלג על העיבוד הרגיל עבור משמרת זו

        # משמרת חופשה/מחלה קבועה - מוסיפים את הסגמנטים ישירות כחופשה (לא כתגבור)
        if is_fixed_vacation_shift and seg_list:
            display_date = r_date  # יום הדיווח
            day_key = display_date.strftime("%d/%m/%Y")
            entry = daily_map.setdefault(day_key, {"segments": [], "date": display_date})

            for seg in seg_list:
                seg_start, seg_end = span_minutes(seg["start_time"], seg["end_time"])

                segment_id = seg.get("id")
                apartment_type_id = r.get("apartment_type_id")
                is_married = r.get("is_married")

                # סימון כחופשה - יטופל בנפרד ב-_process_daily_map
                entry["segments"].append((
                    seg_start, seg_end, "vacation",
                    r["shift_type_id"], segment_id, apartment_type_id, is_married
                ))
            continue  # דלג על העיבוד הרגיל עבור משמרת זו

        for p_date, p_start, p_end in parts:
            # פיצול מקטעים שחוצים את גבול 08:00
            CUTOFF = WORK_DAY_START_MINUTES  # 480
            sub_parts = []
            if p_start < CUTOFF < p_end:
                sub_parts.append((p_start, CUTOFF))
                sub_parts.append((CUTOFF, p_end))
            else:
                sub_parts.append((p_start, p_end))

            for s_start, s_end in sub_parts:
                # שיוך ליום עבודה ונרמול זמנים
                # דיווח ששעת הסיום שלו לפני 08:00 שייך ליום העבודה הקודם
                # אבל רק אם זה המשך של משמרת (לא דיווח עצמאי שמתחיל בחצות)
                # דיווח עצמאי = הדיווח המקורי התחיל בחצות (00:00) ביום הנוכחי
                is_standalone_midnight_shift = (s_start == 0 and p_date == r_date and r_start == 0)
                if s_end <= CUTOFF and not is_standalone_midnight_shift:
                    # שייך ליום העבודה הקודם (המשך משמרת)
                    display_date = p_date - timedelta(days=1)
                    norm_start = s_start + MINUTES_PER_DAY
                    norm_end = s_end + MINUTES_PER_DAY
                else:
                    # שייך ליום העבודה הנוכחי
                    display_date = p_date
                    norm_start = s_start
                    norm_end = s_end

                if display_date.year != year or display_date.month != month:
                    continue

                day_key = display_date.strftime("%d/%m/%Y")
                entry = daily_map.setdefault(day_key, {"segments": [], "date": display_date})

                is_second_day = (p_date > r_date)

                # Sort segments chronologically by start time before normalizing
                seg_list_sorted = sorted(seg_list, key=lambda s: span_minutes(s["start_time"], s["end_time"])[0])

                # Rotate the list so that the segment corresponding to the report start time comes first
                # This ensures that normalization flows correctly (e.g. 06:30-08:00 is end of shift, not start)
                rotate_idx = 0
                rep_start_min = r_start % MINUTES_PER_DAY

                # Find the segment that starts closest to (and before/at) the report start time
                best_start_diff = -1
                for i, seg in enumerate(seg_list_sorted):
                    seg_start_min, _ = span_minutes(seg["start_time"], seg["end_time"])
                    if seg_start_min <= rep_start_min:
                        if seg_start_min > best_start_diff:
                            best_start_diff = seg_start_min
                            rotate_idx = i

                # If no segment starts before report (e.g. report 05:00, first seg 06:00),
                # then it belongs to the LAST segment (from yesterday)
                if best_start_diff == -1 and seg_list_sorted:
                    rotate_idx = len(seg_list_sorted) - 1

                seg_list_ordered = seg_list_sorted[rotate_idx:] + seg_list_sorted[:rotate_idx]

                last_s_end_norm = -1
                minutes_covered = 0
                covered_intervals = []

                for seg in seg_list_ordered:
                    # שימוש במשתנים ייחודיים למניעת דריסת משתני הלופ החיצוני
                    orig_s_start, orig_s_end = span_minutes(seg["start_time"], seg["end_time"])

                    while orig_s_start < last_s_end_norm:
                        orig_s_start += MINUTES_PER_DAY
                        orig_s_end += MINUTES_PER_DAY
                    last_s_end_norm = orig_s_end

                    if is_second_day:
                        current_seg_start = orig_s_start - MINUTES_PER_DAY
                        current_seg_end = orig_s_end - MINUTES_PER_DAY
                    else:
                        current_seg_start = orig_s_start
                        current_seg_end = orig_s_end

                    overlap = overlap_minutes(s_start, s_end, current_seg_start, current_seg_end)
                    if overlap <= 0:
                        continue

                    minutes_covered += overlap

                    # שמירת אינטרוול מכוסה לחישוב "חורים" בהמשך
                    inter_start = max(s_start, current_seg_start)
                    inter_end = min(s_end, current_seg_end)
                    if inter_start < inter_end:
                        covered_intervals.append((inter_start, inter_end))

                    # נרמול גבולות המקטע לפי workday
                    eff_start_in_part = max(current_seg_start, s_start)
                    eff_end_in_part = min(current_seg_end, s_end)

                    if s_end <= CUTOFF:
                        eff_start = eff_start_in_part + MINUTES_PER_DAY
                        eff_end = eff_end_in_part + MINUTES_PER_DAY
                    else:
                        eff_start = eff_start_in_part
                        eff_end = eff_end_in_part

                    eff_type = seg["segment_type"]
                    # אם זה דיווח חופשה/מחלה - סמן כחופשה
                    if is_vacation_report:
                        eff_type = "vacation"

                    segment_id = seg.get("id")
                    apartment_type_id = r.get("apartment_type_id")
                    is_married = r.get("is_married")

                    entry["segments"].append((
                        eff_start, eff_end, eff_type,
                        r["shift_type_id"], segment_id, apartment_type_id, is_married
                    ))

                # טיפול בשעות עבודה שלא מכוסות ע"י סגמנטים מוגדרים
                total_part_minutes = s_end - s_start
                remaining = total_part_minutes - minutes_covered

                if remaining > 0:
                    # מיון ומיזוג אינטרוולים חופפים
                    covered_intervals.sort()
                    merged_covered = []
                    for interval in covered_intervals:
                        if merged_covered and interval[0] <= merged_covered[-1][1]:
                            merged_covered[-1] = (merged_covered[-1][0], max(merged_covered[-1][1], interval[1]))
                        else:
                            merged_covered.append(interval)

                    # מציאת ה"חורים" - זמנים לא מכוסים
                    uncovered_intervals = []
                    current_pos = s_start
                    for cov_start, cov_end in merged_covered:
                        if current_pos < cov_start:
                            uncovered_intervals.append((current_pos, cov_start))
                        current_pos = max(current_pos, cov_end)
                    if current_pos < s_end:
                        uncovered_intervals.append((current_pos, s_end))

                    # יצירת סגמנטי עבודה לכל זמן לא מכוסה
                    segment_id = None
                    apartment_type_id = r.get("apartment_type_id")
                    is_married = r.get("is_married")

                    for uncov_start, uncov_end in uncovered_intervals:
                        uncov_duration = uncov_end - uncov_start
                        if uncov_duration <= 0:
                            continue

                        # נרמול זמנים לפי יום עבודה
                        if s_end <= CUTOFF:
                            eff_uncov_start = uncov_start + MINUTES_PER_DAY
                            eff_uncov_end = uncov_end + MINUTES_PER_DAY
                        else:
                            eff_uncov_start = uncov_start
                            eff_uncov_end = uncov_end

                        # הוספת סגמנט עבודה - האחוז יחושב ע"י מנגנון הרצפים
                        entry["segments"].append((
                            eff_uncov_start, eff_uncov_end, "work",
                            r["shift_type_id"], segment_id, apartment_type_id, is_married
                        ))

    return daily_map


def _get_shabbat_boundaries(day_date: date, shabbat_cache: Dict[str, Dict[str, str]]) -> Tuple[int, int]:
    """
    Get Shabbat enter/exit times in minutes from Friday midnight.
    Returns (enter_minute, exit_minute) where exit is relative to Friday midnight (can be >1440).
    """
    weekday = day_date.weekday()

    # Find the relevant Saturday
    if weekday == FRIDAY:
        target_saturday = day_date + timedelta(days=1)
    elif weekday == SATURDAY:
        target_saturday = day_date
    else:
        # Not Friday or Saturday - no Shabbat
        return (-1, -1)

    saturday_str = target_saturday.strftime("%Y-%m-%d")
    shabbat_data = shabbat_cache.get(saturday_str)

    if shabbat_data:
        try:
            eh, em = map(int, shabbat_data["enter"].split(":"))
            enter_minutes = eh * MINUTES_PER_HOUR + em

            xh, xm = map(int, shabbat_data["exit"].split(":"))
            exit_minutes = xh * MINUTES_PER_HOUR + xm + MINUTES_PER_DAY  # Add 1440 for Saturday

            return (enter_minutes, exit_minutes)
        except (ValueError, KeyError, AttributeError):
            pass

    # Default times
    return (SHABBAT_ENTER_DEFAULT, SHABBAT_EXIT_DEFAULT + MINUTES_PER_DAY)


def _calculate_chain_wages(
    chain_segments: List[Tuple[int, int, int]],
    day_date: date,
    shabbat_cache: Dict[str, Dict[str, str]],
    minutes_offset: int = 0
) -> Dict[str, Any]:
    """
    חישוב שכר לרצף עבודה (chain) בשיטת בלוקים.

    במקום לעבור דקה-דקה, מחשב בלוקים לפי גבולות:
    - 480 דקות (מעבר 100% -> 125%)
    - 600 דקות (מעבר 125% -> 150%)
    - גבולות שבת (כניסה/יציאה)

    Args:
        chain_segments: List of (start_min, end_min, shift_id) tuples
        day_date: The date for Shabbat calculation
        shabbat_cache: Cache of Shabbat times
        minutes_offset: Minutes already worked in this chain (from previous day's carryover)

    Returns:
        Dict with calc100, calc125, calc150, calc175, calc200,
        calc150_shabbat, calc150_overtime, calc150_shabbat_100, calc150_shabbat_50,
        and segments_detail - list of (start_min, end_min, label, is_shabbat) for display
    """
    result = {
        "calc100": 0, "calc125": 0, "calc150": 0, "calc175": 0, "calc200": 0,
        "calc150_shabbat": 0, "calc150_overtime": 0,
        "calc150_shabbat_100": 0, "calc150_shabbat_50": 0,
        "segments_detail": []  # For display: list of (start_min, end_min, label, is_shabbat)
    }

    if not chain_segments:
        return result

    weekday = day_date.weekday()
    is_fri_or_sat = weekday in (FRIDAY, SATURDAY)

    # Get Shabbat boundaries if relevant
    shabbat_enter, shabbat_exit = (-1, -1)
    if is_fri_or_sat:
        shabbat_enter, shabbat_exit = _get_shabbat_boundaries(day_date, shabbat_cache)

    # Flatten all segments into a list of (abs_start, abs_end) in continuous minutes
    # and calculate total chain minutes
    total_chain_minutes = 0
    flat_segments = []

    for seg_start, seg_end, seg_shift_id in chain_segments:
        flat_segments.append((seg_start, seg_end))
        total_chain_minutes += (seg_end - seg_start)

    # Process in blocks based on overtime thresholds
    # Thresholds: 0-480 = tier1, 480-600 = tier2, 600+ = tier3
    # Start from offset if this chain continues from previous day
    minutes_processed = minutes_offset

    for seg_start, seg_end in flat_segments:
        seg_duration = seg_end - seg_start
        seg_offset = 0

        while seg_offset < seg_duration:
            current_abs_minute = seg_start + seg_offset
            current_chain_minute = minutes_processed + 1  # 1-based for wage calculation

            # Determine which overtime tier we're in
            if current_chain_minute <= REGULAR_HOURS_LIMIT:
                tier_end = REGULAR_HOURS_LIMIT
                base_rate = "100%"
                shabbat_rate = "150%"
            elif current_chain_minute <= OVERTIME_125_LIMIT:
                tier_end = OVERTIME_125_LIMIT
                base_rate = "125%"
                shabbat_rate = "175%"
            else:
                tier_end = float('inf')
                base_rate = "150%"
                shabbat_rate = "200%"

            # How many minutes until we hit the next tier?
            minutes_until_tier_change = tier_end - minutes_processed

            # How many minutes left in this segment?
            minutes_left_in_seg = seg_duration - seg_offset

            # Take the minimum
            block_size = min(minutes_until_tier_change, minutes_left_in_seg)

            # Now check Shabbat boundaries within this block
            if is_fri_or_sat:
                block_abs_start = current_abs_minute
                block_abs_end = current_abs_minute + block_size

                # נרמול זמנים - זמנים מעל 1440 הם בבוקר (אחרי חצות)
                # לדוגמה: 1830 = 06:30 בבוקר של היום הבא
                actual_block_start = block_abs_start % MINUTES_PER_DAY
                actual_block_end = block_abs_end % MINUTES_PER_DAY
                # אם הסגמנט חוצה חצות, end יהיה קטן מ-start
                if actual_block_end <= actual_block_start and block_abs_end > block_abs_start:
                    actual_block_end = block_abs_end % MINUTES_PER_DAY or MINUTES_PER_DAY

                # Adjust for day offset (if segment crosses midnight)
                # day_offset מייצג את המרחק מחצות יום שישי
                # - יום שישי: offset = 0
                # - יום שבת: offset = 1440
                # - יום ראשון בוקר (זמנים >= 1440 מנורמלים ליום שבת): offset = 2880
                day_offset = 0
                if weekday == SATURDAY:
                    day_offset = MINUTES_PER_DAY
                    # אם הזמן המקורי חצה חצות (>=1440), זה בעצם יום ראשון בבוקר
                    if block_abs_start >= MINUTES_PER_DAY:
                        day_offset = 2 * MINUTES_PER_DAY

                abs_start_from_fri = actual_block_start + day_offset
                abs_end_from_fri = actual_block_end + day_offset

                # Helper to add segment detail
                def add_segment_detail(start_min, end_min, rate_label, is_shabbat):
                    result["segments_detail"].append((start_min, end_min, rate_label, is_shabbat))

                # Split block at Shabbat boundaries
                # Case 1: Entirely before Shabbat
                if abs_end_from_fri <= shabbat_enter:
                    if base_rate == "100%":
                        result["calc100"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "100%", False)
                    elif base_rate == "125%":
                        result["calc125"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "125%", False)
                    else:
                        result["calc150"] += block_size
                        result["calc150_overtime"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "150%", False)

                # Case 2: Entirely during Shabbat
                elif abs_start_from_fri >= shabbat_enter and abs_end_from_fri <= shabbat_exit:
                    if shabbat_rate == "150%":
                        result["calc150"] += block_size
                        result["calc150_shabbat"] += block_size
                        result["calc150_shabbat_100"] += block_size
                        result["calc150_shabbat_50"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "150% שבת", True)
                    elif shabbat_rate == "175%":
                        result["calc175"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "175% שבת", True)
                    else:
                        result["calc200"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "200% שבת", True)

                # Case 3: Entirely after Shabbat
                elif abs_start_from_fri >= shabbat_exit:
                    if base_rate == "100%":
                        result["calc100"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "100%", False)
                    elif base_rate == "125%":
                        result["calc125"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "125%", False)
                    else:
                        result["calc150"] += block_size
                        result["calc150_overtime"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "150%", False)

                # Case 4: Block crosses Shabbat start
                elif abs_start_from_fri < shabbat_enter < abs_end_from_fri:
                    before_shabbat = shabbat_enter - abs_start_from_fri
                    during_shabbat = abs_end_from_fri - shabbat_enter

                    # Before Shabbat part
                    if base_rate == "100%":
                        result["calc100"] += before_shabbat
                        add_segment_detail(block_abs_start, block_abs_start + before_shabbat, "100%", False)
                    elif base_rate == "125%":
                        result["calc125"] += before_shabbat
                        add_segment_detail(block_abs_start, block_abs_start + before_shabbat, "125%", False)
                    else:
                        result["calc150"] += before_shabbat
                        result["calc150_overtime"] += before_shabbat
                        add_segment_detail(block_abs_start, block_abs_start + before_shabbat, "150%", False)

                    # During Shabbat part
                    shabbat_start_abs = block_abs_start + before_shabbat
                    if shabbat_rate == "150%":
                        result["calc150"] += during_shabbat
                        result["calc150_shabbat"] += during_shabbat
                        result["calc150_shabbat_100"] += during_shabbat
                        result["calc150_shabbat_50"] += during_shabbat
                        add_segment_detail(shabbat_start_abs, block_abs_end, "150% שבת", True)
                    elif shabbat_rate == "175%":
                        result["calc175"] += during_shabbat
                        add_segment_detail(shabbat_start_abs, block_abs_end, "175% שבת", True)
                    else:
                        result["calc200"] += during_shabbat
                        add_segment_detail(shabbat_start_abs, block_abs_end, "200% שבת", True)

                # Case 5: Block crosses Shabbat end
                elif abs_start_from_fri < shabbat_exit < abs_end_from_fri:
                    during_shabbat = shabbat_exit - abs_start_from_fri
                    after_shabbat = abs_end_from_fri - shabbat_exit

                    # During Shabbat part
                    if shabbat_rate == "150%":
                        result["calc150"] += during_shabbat
                        result["calc150_shabbat"] += during_shabbat
                        result["calc150_shabbat_100"] += during_shabbat
                        result["calc150_shabbat_50"] += during_shabbat
                        add_segment_detail(block_abs_start, block_abs_start + during_shabbat, "150% שבת", True)
                    elif shabbat_rate == "175%":
                        result["calc175"] += during_shabbat
                        add_segment_detail(block_abs_start, block_abs_start + during_shabbat, "175% שבת", True)
                    else:
                        result["calc200"] += during_shabbat
                        add_segment_detail(block_abs_start, block_abs_start + during_shabbat, "200% שבת", True)

                    # After Shabbat part
                    after_start_abs = block_abs_start + during_shabbat
                    if base_rate == "100%":
                        result["calc100"] += after_shabbat
                        add_segment_detail(after_start_abs, block_abs_end, "100%", False)
                    elif base_rate == "125%":
                        result["calc125"] += after_shabbat
                        add_segment_detail(after_start_abs, block_abs_end, "125%", False)
                    else:
                        result["calc150"] += after_shabbat
                        result["calc150_overtime"] += after_shabbat
                        add_segment_detail(after_start_abs, block_abs_end, "150%", False)

                else:
                    # Fallback - shouldn't happen but just in case
                    if base_rate == "100%":
                        result["calc100"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "100%", False)
                    elif base_rate == "125%":
                        result["calc125"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "125%", False)
                    else:
                        result["calc150"] += block_size
                        result["calc150_overtime"] += block_size
                        add_segment_detail(block_abs_start, block_abs_end, "150%", False)
            else:
                # Not Friday or Saturday - simple calculation
                if base_rate == "100%":
                    result["calc100"] += block_size
                    result["segments_detail"].append((current_abs_minute, current_abs_minute + block_size, "100%", False))
                elif base_rate == "125%":
                    result["calc125"] += block_size
                    result["segments_detail"].append((current_abs_minute, current_abs_minute + block_size, "125%", False))
                else:
                    result["calc150"] += block_size
                    result["calc150_overtime"] += block_size
                    result["segments_detail"].append((current_abs_minute, current_abs_minute + block_size, "150%", False))

            seg_offset += block_size
            minutes_processed += block_size

    # Merge adjacent segments with the same label for cleaner display
    merged_segments = []
    for seg in result["segments_detail"]:
        if merged_segments and merged_segments[-1][2] == seg[2] and merged_segments[-1][1] == seg[0]:
            # Merge with previous segment
            merged_segments[-1] = (merged_segments[-1][0], seg[1], seg[2], seg[3])
        else:
            merged_segments.append(seg)
    result["segments_detail"] = merged_segments

    return result


def _process_daily_map(
    daily_map: Dict[str, Dict],
    shabbat_cache: Dict[str, Dict[str, str]],
    get_standby_rate_fn: Callable[[int, Optional[int], bool], float],
    year: int,
    month: int
) -> Tuple[Dict[str, int], set, set]:
    """
    עיבוד מפת ימים וחישוב סיכומים.

    Args:
        daily_map: מפת הימים שנבנתה ע"י _build_daily_map
        shabbat_cache: זמני שבת
        get_standby_rate_fn: פונקציה לקבלת תעריף כוננות (מאפשרת DB או cache)
        year, month: שנה וחודש לסינון

    Returns:
        (day_totals, work_days_set, vacation_days_set)
    """
    from utils import calculate_accruals
    WORK_DAY_CUTOFF = 480  # 08:00

    totals = {
        "calc100": 0, "calc125": 0, "calc150": 0, "calc175": 0, "calc200": 0,
        "calc150_shabbat": 0, "calc150_overtime": 0,
        "calc150_shabbat_100": 0, "calc150_shabbat_50": 0,
        "total_hours": 0, "standby_payment": 0, "vacation_minutes": 0
    }
    work_days_set = set()
    vacation_days_set = set()

    # Track carryover minutes from previous day's chain ending at 08:00
    prev_day_carryover_minutes = 0
    prev_day_ended_at_midnight = False
    prev_day_date = None  # לעקוב אחרי התאריך הקודם

    for day_key, entry in sorted(daily_map.items()):
        day_date = entry["date"]

        # בדיקה אם הימים רציפים - אם לא, לאפס carryover
        if prev_day_date is not None:
            days_diff = (day_date - prev_day_date).days
            if days_diff != 1:
                # הימים לא רציפים - אין carryover
                prev_day_carryover_minutes = 0
                prev_day_ended_at_midnight = False

        # משמרת תגבור - משתמשים באחוזים הקבועים מהסגמנטים, לא מחשבים רצף
        if entry.get("is_tagbur") and entry.get("tagbur_wages"):
            tagbur = entry["tagbur_wages"]
            totals["calc100"] += tagbur.get("calc100", 0)
            totals["calc125"] += tagbur.get("calc125", 0)
            totals["calc175"] += tagbur.get("calc175", 0)
            totals["calc200"] += tagbur.get("calc200", 0)
            
            # חישוב calc150 עם הפרדה בין שבת לחול
            tagbur_calc150 = tagbur.get("calc150", 0)
            if tagbur_calc150 > 0:
                # בדיקה אם יש פרטי סגמנטים לחישוב שבת/חול
                tagbur_segments_detail = entry.get("tagbur_segments_detail", [])
                if tagbur_segments_detail:
                    # חישוב לפי סגמנטים - בדיקה אם כל סגמנט הוא שבת או חול
                    calc150_shabbat_minutes = 0
                    calc150_overtime_minutes = 0
                    
                    weekday = day_date.weekday()
                    is_fri_or_sat = weekday in (FRIDAY, SATURDAY)
                    
                    # קבלת גבולות שבת אם רלוונטי
                    shabbat_enter, shabbat_exit = (-1, -1)
                    if is_fri_or_sat:
                        shabbat_enter, shabbat_exit = _get_shabbat_boundaries(day_date, shabbat_cache)
                    
                    for seg_detail in tagbur_segments_detail:
                        if seg_detail["wage_percent"] == 150:
                            seg_start = seg_detail["start"] % MINUTES_PER_DAY  # נרמול ל-0-1439
                            seg_end = seg_detail["end"] % MINUTES_PER_DAY
                            seg_date = seg_detail["date"]
                            seg_weekday = seg_date.weekday()
                            seg_duration = seg_end - seg_start
                            
                            # בדיקה אם הסגמנט נופל בשבת
                            # חשוב: לבדוק את seg_weekday של הסגמנט עצמו, לא רק את day_date
                            is_seg_shabbat = False
                            
                            # קבלת גבולות שבת לפי התאריך של הסגמנט
                            seg_shabbat_enter, seg_shabbat_exit = (-1, -1)
                            if seg_weekday in (FRIDAY, SATURDAY):
                                seg_shabbat_enter, seg_shabbat_exit = _get_shabbat_boundaries(seg_date, shabbat_cache)
                            
                            if seg_weekday == FRIDAY and seg_shabbat_enter >= 0:
                                # יום שישי - בדיקה אם הסגמנט מתחיל אחרי כניסת שבת
                                if seg_start >= seg_shabbat_enter:
                                    # כל הסגמנט הוא שבת
                                    is_seg_shabbat = True
                                elif seg_end > seg_shabbat_enter:
                                    # הסגמנט חוצה את כניסת שבת - נחלק אותו
                                    shabbat_part = seg_end - seg_shabbat_enter
                                    weekday_part = seg_shabbat_enter - seg_start
                                    calc150_shabbat_minutes += shabbat_part
                                    calc150_overtime_minutes += weekday_part
                                    continue  # כבר עדכנו, עוברים לסגמנט הבא
                            elif seg_weekday == SATURDAY and seg_shabbat_exit >= 0:
                                # יום שבת - בדיקה אם הסגמנט מסתיים לפני יציאת שבת
                                # shabbat_exit הוא יחסית לחצות יום שישי, אז בשבת זה shabbat_exit - 1440
                                shabbat_exit_saturday = seg_shabbat_exit - MINUTES_PER_DAY
                                if seg_end <= shabbat_exit_saturday:
                                    # כל הסגמנט הוא שבת
                                    is_seg_shabbat = True
                                elif seg_start < shabbat_exit_saturday:
                                    # הסגמנט חוצה את יציאת שבת - נחלק אותו
                                    shabbat_part = shabbat_exit_saturday - seg_start
                                    weekday_part = seg_end - shabbat_exit_saturday
                                    calc150_shabbat_minutes += shabbat_part
                                    calc150_overtime_minutes += weekday_part
                                    continue  # כבר עדכנו, עוברים לסגמנט הבא
                            elif seg_weekday == SATURDAY:
                                # יום שבת ללא גבולות שבת - כל הסגמנט הוא שבת
                                is_seg_shabbat = True
                            
                            # אם לא חילקנו את הסגמנט, נבדוק אם הוא שבת או חול
                            if is_seg_shabbat:
                                calc150_shabbat_minutes += seg_duration
                            else:
                                calc150_overtime_minutes += seg_duration
                    
                    # אם יש חלוקה, עדכן את הסכומים
                    if calc150_shabbat_minutes > 0 or calc150_overtime_minutes > 0:
                        totals["calc150"] += tagbur_calc150
                        totals["calc150_shabbat"] += calc150_shabbat_minutes
                        totals["calc150_overtime"] += calc150_overtime_minutes
                        # עדכון גם של השעות המפוצלות לפנסיה (100% + 50%)
                        totals["calc150_shabbat_100"] += calc150_shabbat_minutes
                        totals["calc150_shabbat_50"] += calc150_shabbat_minutes
                    else:
                        # אם לא הצלחנו לחלק, נחשוב לפי יום השבוע
                        if weekday == SATURDAY:
                            totals["calc150"] += tagbur_calc150
                            totals["calc150_shabbat"] += tagbur_calc150
                            # עדכון גם של השעות המפוצלות לפנסיה (100% + 50%)
                            totals["calc150_shabbat_100"] += tagbur_calc150
                            totals["calc150_shabbat_50"] += tagbur_calc150
                        elif weekday == FRIDAY:
                            # יום שישי - נבדוק אם יש חלק בשבת
                            # נניח שכל התגבור הוא חול (כי בדרך כלל תגבור ביום שישי הוא לפני שבת)
                            totals["calc150"] += tagbur_calc150
                            totals["calc150_overtime"] += tagbur_calc150
                        else:
                            # חול
                            totals["calc150"] += tagbur_calc150
                            totals["calc150_overtime"] += tagbur_calc150
                else:
                    # אין calc150 בתגבור
                    totals["calc150"] += tagbur_calc150

            total_day_minutes = sum(tagbur.values())
            totals["total_hours"] += total_day_minutes

            if total_day_minutes > 0:
                work_days_set.add(day_date)

            # אפס carryover כי משמרת תגבור היא עצמאית
            prev_day_carryover_minutes = 0
            prev_day_ended_at_midnight = False
            prev_day_date = day_date
            continue

        # הפרדת מקטעים לסוגים
        work_segments = []
        standby_segments = []
        vacation_segments = []

        for seg in entry["segments"]:
            s_start, s_end, s_type, shift_id, seg_id, apt_type, is_married = seg
            if s_type == "standby":
                standby_segments.append((s_start, s_end, seg_id, apt_type, is_married))
            elif s_type == "vacation":
                vacation_segments.append((s_start, s_end))
            else:
                work_segments.append((s_start, s_end, shift_id))

        work_segments.sort(key=lambda x: x[0])
        standby_segments.sort(key=lambda x: x[0])

        # הסרת כפילויות
        seen = set()
        deduped = []
        for ws in work_segments:
            key = (ws[0], ws[1])
            if key not in seen:
                deduped.append(ws)
                seen.add(key)
        work_segments = deduped
        
        # הסרת כפילויות מקטעי כוננות
        seen_standby = set()
        deduped_standby = []
        for sb in standby_segments:
            key = (sb[0], sb[1], sb[2])  # start_time, end_time, segment_id
            if key not in seen_standby:
                deduped_standby.append(sb)
                seen_standby.add(key)
        standby_segments = deduped_standby
        
        # איחוד מקטעי כוננות רציפים לפני בדיקת ביטול
        # כדי להבטיח שבודקים את כל תקופת הכוננות המלאה, לא כל חלק בנפרד
        standby_segments.sort(key=lambda x: x[0])
        merged_standbys = []
        for sb in standby_segments:
            sb_start, sb_end, sb_seg_id, sb_apt, sb_married = sb
            if merged_standbys and sb_start <= merged_standbys[-1][1]:  # חופפים או רציפים
                # הרחבת הכוננות הקודמת
                prev = merged_standbys[-1]
                merged_standbys[-1] = (prev[0], max(prev[1], sb_end), prev[2], prev[3], prev[4])
            else:
                merged_standbys.append(sb)

        # ביטול כוננות אם יש חפיפה מעל 70% - הוחלף בלוגיקת קיזוז (Trim)
        # במקום לבטל את הכוננות כליל, אנו מקזזים את זמני העבודה מזמן הכוננות
        final_standby_segments = []
        for sb in merged_standbys:
            sb_start, sb_end, sb_seg_id, sb_apt, sb_married = sb
            
            # Start with the full standby segment
            remaining_parts = [(sb_start, sb_end)]
            
            # Subtract each work segment
            for w_start, w_end, _ in work_segments:
                new_parts = []
                for r_start, r_end in remaining_parts:
                    # Calculate intersection
                    inter_start = max(r_start, w_start)
                    inter_end = min(r_end, w_end)
                    
                    if inter_start < inter_end:
                        # There is overlap, subtract it
                        # Part before overlap
                        if r_start < inter_start:
                            new_parts.append((r_start, inter_start))
                        # Part after overlap
                        if inter_end < r_end:
                            new_parts.append((inter_end, r_end))
                    else:
                        # No overlap, keep original
                        new_parts.append((r_start, r_end))
                remaining_parts = new_parts
            
            # Add resulting parts to final list
            for r_start, r_end in remaining_parts:
                if r_end > r_start:
                    final_standby_segments.append((r_start, r_end, sb_seg_id, sb_apt, sb_married))
        
        standby_segments = final_standby_segments
        standby_segments.sort(key=lambda x: x[0])

        # איחוד אירועים
        all_events = []
        for s, e, sid in work_segments:
            all_events.append({"start": s, "end": e, "type": "work", "shift_id": sid})
        for s, e, seg_id, apt_type, is_married_val in standby_segments:
            all_events.append({"start": s, "end": e, "type": "standby", "segment_id": seg_id,
                              "apartment_type_id": apt_type, "is_married": is_married_val})
        for s, e in vacation_segments:
            all_events.append({"start": s, "end": e, "type": "vacation"})

        all_events.sort(key=lambda x: x["start"])

        # Build a set of work segment boundaries for quick lookup
        # This helps determine if standby truly breaks the chain or if work continues through it
        work_starts = {ws[0] for ws in work_segments}  # All work start times
        work_ends = {ws[1] for ws in work_segments}    # All work end times

        # Determine if we should use carryover from previous day
        # Carryover applies if first work event starts at 08:00 (480 minutes)
        first_work_start = None
        for evt in all_events:
            if evt["type"] == "work":
                first_work_start = evt["start"]
                break

        use_carryover = (first_work_start == WORK_DAY_CUTOFF or prev_day_ended_at_midnight) and prev_day_carryover_minutes > 0
        current_offset = prev_day_carryover_minutes if use_carryover else 0

        # משתני רצף
        current_chain_segments = []
        last_end = None
        last_etype = None
        day_standby_payment = 0
        day_vacation_minutes = 0
        day_wages = {
            "calc100": 0, "calc125": 0, "calc150": 0, "calc175": 0, "calc200": 0,
            "calc150_shabbat": 0, "calc150_overtime": 0,
            "calc150_shabbat_100": 0, "calc150_shabbat_50": 0
        }

        # Track chain info for carryover
        first_chain_of_day = True
        last_chain_total = 0
        last_chain_ended_at_0800 = False
        
        # Track paid standby segments to avoid double payment on split segments
        paid_standby_ids = set()

        def close_chain(minutes_offset=0):
            nonlocal current_chain_segments, day_wages, last_chain_total, last_chain_ended_at_0800, prev_day_ended_at_midnight
            if not current_chain_segments:
                return

            chain_wages = _calculate_chain_wages(current_chain_segments, day_date, shabbat_cache, minutes_offset)
            for key in day_wages:
                day_wages[key] += chain_wages[key]

            # Calculate chain duration for potential carryover
            chain_duration = sum(e - s for s, e, _ in current_chain_segments)
            last_chain_total = minutes_offset + chain_duration

            # Check if chain ends at 08:00 boundary (1920 = 08:00 + 1440)
            last_chain_ended_at_0800 = (current_chain_segments[-1][1] == 1920) if current_chain_segments else False

            # Check if chain ends at midnight (1440)
            last_segment_end = current_chain_segments[-1][1] if current_chain_segments else 0
            prev_day_ended_at_midnight = (last_segment_end == 1440)

            current_chain_segments = []

        for event in all_events:
            seg_start = event["start"]
            seg_end = event["end"]
            seg_type = event["type"]

            is_special = seg_type in ("standby", "vacation")

            # בדיקת שבירת רצף
            should_break = False
            if current_chain_segments:
                if is_special:
                    should_break = True
                elif last_end is not None:
                    # Calculate gap considering normalized times
                    # Normalized times (after midnight, before 08:00) have 1440 added
                    # So they are >= 1440, not < 480
                    gap = seg_start - last_end
                    if gap > BREAK_THRESHOLD_MINUTES:
                        should_break = True

            if should_break:
                chain_offset = current_offset if first_chain_of_day else 0
                close_chain(chain_offset)
                first_chain_of_day = False

            if is_special:
                if seg_type == "standby":
                    # בדיקה האם זו המשכיות של כוננות קודמת
                    is_continuation = (last_etype == "standby" and last_end == seg_start)
                    
                    # בדיקה אם כבר שילמנו על המקטע הזה (למקרה שפוצל עקב עבודה באמצע)
                    seg_id = event.get("segment_id")
                    already_paid = (seg_id in paid_standby_ids) if seg_id else False
                    
                    if not is_continuation and not already_paid:
                        seg_id = event.get("segment_id") or 0
                        apt_type = event.get("apartment_type_id")
                        is_married_val = event.get("is_married")
                        is_married_bool = bool(is_married_val) if is_married_val is not None else False
                        rate = get_standby_rate_fn(seg_id, apt_type, is_married_bool)
                        day_standby_payment += rate
                        
                        if seg_id:
                            paid_standby_ids.add(seg_id)
                elif seg_type == "vacation":
                    day_vacation_minutes += (seg_end - seg_start)

                last_end = seg_end
                last_etype = seg_type
            else:
                shift_id = event.get("shift_id", 0)
                current_chain_segments.append((seg_start, seg_end, shift_id))
                last_end = seg_end
                last_etype = seg_type

        # Close last chain with proper offset
        chain_offset = current_offset if first_chain_of_day else 0
        close_chain(chain_offset)

        # Update carryover for next day
        if last_chain_ended_at_0800:
            prev_day_carryover_minutes = last_chain_total
        else:
            prev_day_carryover_minutes = 0

        # עדכון סיכומים
        for key in day_wages:
            totals[key] += day_wages[key]
        totals["total_hours"] += sum(day_wages[k] for k in ["calc100", "calc125", "calc150", "calc175", "calc200"])
        totals["standby_payment"] += day_standby_payment
        totals["vacation_minutes"] += day_vacation_minutes

        # ספירת ימי עבודה - בגרסה המנורמלת, המפתח הוא יום העבודה
        if work_segments:
            work_days_set.add(day_date)

        if vacation_segments:
            vacation_days_set.add(day_date)

        # עדכון התאריך הקודם לסיבוב הבא
        prev_day_date = day_date

    return totals, work_days_set, vacation_days_set


def calculate_person_monthly_totals(
    conn,
    person_id: int,
    year: int,
    month: int,
    shabbat_cache: Dict[str, Dict[str, str]],
    minimum_wage: float = None  # If None, will be fetched from history
) -> Dict:
    """
    חישוב מדויק של סיכומים חודשיים לעובד.
    """
    from utils import month_range_ts, calculate_accruals
    from history import (get_person_status_for_month, get_apartment_type_for_month,
                         get_all_shift_rates_for_month, get_minimum_wage_for_month,
                         get_all_segments_for_month)
    
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    # Get minimum wage for the specific month (historical)
    if minimum_wage is None:
        minimum_wage = get_minimum_wage_for_month(conn, year, month)
    
    # שליפת פרטי העובד
    cursor.execute("""
        SELECT id, name, phone, email, is_active, start_date, is_married, type
        FROM people WHERE id = %s
    """, (person_id,))
    person = cursor.fetchone()
    if not person:
        cursor.close()
        return {}
    
    # שליפת דיווחים לחודש
    start_ts, end_ts = month_range_ts(year, month)
    cursor.execute("""
        SELECT tr.*, st.name as shift_name,
               a.apartment_type_id,
               p.is_married,
               st.rate as shift_rate,
               st.is_minimum_wage as shift_is_minimum_wage
        FROM time_reports tr
        LEFT JOIN shift_types st ON st.id = tr.shift_type_id
        LEFT JOIN apartments a ON tr.apartment_id = a.id
        LEFT JOIN people p ON tr.person_id = p.id
        WHERE tr.person_id = %s AND tr.date >= %s AND tr.date < %s
        ORDER BY tr.date, tr.start_time
    """, (person_id, start_ts, end_ts))
    reports_raw = cursor.fetchall()

    # Override with historical data if available
    historical_person = get_person_status_for_month(conn, person_id, year, month)
    historical_is_married = historical_person.get("is_married")

    # Build apartment historical cache
    apartment_ids = {r["apartment_id"] for r in reports_raw if r["apartment_id"]}
    apartment_type_cache = {}
    for apt_id in apartment_ids:
        hist_type = get_apartment_type_for_month(conn, apt_id, year, month)
        if hist_type is not None:
            apartment_type_cache[apt_id] = hist_type

    # Build shift rates historical cache
    shift_rates_cache = get_all_shift_rates_for_month(conn, year, month)

    # Apply historical overrides to reports
    reports = []
    for r in reports_raw:
        r_dict = dict(r)
        # Override is_married with historical value if available
        if historical_is_married is not None:
            r_dict["is_married"] = historical_is_married
        # Override apartment_type_id with historical value if available
        apt_id = r_dict.get("apartment_id")
        if apt_id and apt_id in apartment_type_cache:
            r_dict["apartment_type_id"] = apartment_type_cache[apt_id]
        # Override shift rate with historical value if available
        shift_type_id = r_dict.get("shift_type_id")
        if shift_type_id and shift_type_id in shift_rates_cache:
            rate_info = shift_rates_cache[shift_type_id]
            r_dict["shift_rate"] = rate_info.get("rate")
            r_dict["shift_is_minimum_wage"] = rate_info.get("is_minimum_wage")
        reports.append(r_dict)

    # אתחול סיכומים - תמיד, גם אם אין דיווחי שעות
    monthly_totals = {
        "total_hours": 0, "payment": 0, "standby": 0, "standby_payment": 0,
        "actual_work_days": 0, "vacation_days_taken": 0,
        "calc100": 0, "calc125": 0, "calc150": 0, "calc150_shabbat": 0,
        "calc150_shabbat_100": 0, "calc150_shabbat_50": 0,
        "calc150_overtime": 0, "calc175": 0, "calc200": 0,
        "vacation_minutes": 0, "vacation_payment": 0,
        "travel": 0, "extras": 0, "sick_days_accrued": 0, "vacation_days_accrued": 0
    }

    # עיבוד דיווחי שעות - רק אם יש דיווחים
    if reports:
        # שליפת מקטעי משמרות - עם תמיכה בהיסטוריה
        shift_ids = list({r["shift_type_id"] for r in reports if r["shift_type_id"]})
        
        # Use historical segments if available, otherwise current segments
        segments_by_shift = get_all_segments_for_month(conn, shift_ids, year, month)

        # זיהוי משמרות עם כוננות
        shift_has_standby = {sid: any(s["segment_type"] == "standby" for s in segs)
                             for sid, segs in segments_by_shift.items()}

        # בניית מפת ימים באמצעות הפונקציה המשותפת
        daily_map = _build_daily_map(reports, segments_by_shift, year, month)

        # ספירת כוננויות מדיווחים מקוריים - סופר תאריכים ייחודיים עם כוננות
        dates_with_standby = set()
        for r in reports:
            if r["shift_type_id"] and shift_has_standby.get(r["shift_type_id"], False):
                dates_with_standby.add(r["date"])
        monthly_totals["standby"] = len(dates_with_standby)

        # פונקציה לקבלת תעריף כוננות מDB (כולל נתונים היסטוריים)
        def get_standby_rate_from_db(seg_id: int, apt_type: Optional[int], is_married: bool) -> float:
            return get_standby_rate(conn, seg_id, apt_type, is_married, year, month)

        # עיבוד מפת הימים באמצעות הפונקציה המשותפת
        totals, work_days_set, vacation_days_set = _process_daily_map(
            daily_map, shabbat_cache, get_standby_rate_from_db, year, month
        )

        # העברת הסיכומים
        for key in ["calc100", "calc125", "calc150", "calc175", "calc200",
                    "calc150_shabbat", "calc150_overtime", "calc150_shabbat_100",
                    "calc150_shabbat_50", "total_hours", "standby_payment", "vacation_minutes"]:
            monthly_totals[key] = totals[key]

        monthly_totals["actual_work_days"] = len(work_days_set)
        monthly_totals["vacation_days_taken"] = len(vacation_days_set)

        # חישוב תשלום חופשה
        monthly_totals["vacation_payment"] = (monthly_totals.get("vacation_minutes", 0) / 60) * minimum_wage

        # חישוב שעות בתעריף משתנה
        # נבנה מפה של shift_id -> תעריף משתנה (בש"ח)
        variable_rate_by_shift = {}
        for r in reports:
            shift_rate = r.get("shift_rate")
            is_minimum_wage = r.get("shift_is_minimum_wage", True)
            if shift_rate and not is_minimum_wage:
                # shift_rate stored in agorot, convert to shekels
                variable_rate_by_shift[r.get("shift_type_id")] = float(shift_rate) / 100

        # נחשב את דקות העבודה מדיווחים עם תעריף משתנה ואת התשלום הנוסף
        variable_rate_minutes = 0
        variable_rate_extra_payment = 0.0
        for day_key, entry in daily_map.items():
            for seg in entry.get("segments", []):
                s_start, s_end, s_type, shift_id, seg_id, apt_type, is_married = seg
                if s_type == "work" and shift_id in variable_rate_by_shift:
                    duration = s_end - s_start
                    variable_rate_minutes += duration
                    # חישוב התשלום הנוסף (הפרש בין התעריף לשכר מינימום)
                    actual_rate = variable_rate_by_shift[shift_id]
                    rate_diff = actual_rate - minimum_wage
                    if rate_diff > 0:
                        variable_rate_extra_payment += (duration / 60) * rate_diff

        monthly_totals["calc_variable"] = variable_rate_minutes
        monthly_totals["variable_rate_extra_payment"] = variable_rate_extra_payment

    # שליפת רכיבי תשלום נוספים
    month_start = datetime(year, month, 1, tzinfo=LOCAL_TZ)
    month_end = datetime(year + 1, 1, 1, tzinfo=LOCAL_TZ) if month == 12 else datetime(year, month + 1, 1, tzinfo=LOCAL_TZ)
    
    cursor.execute("""
        SELECT (quantity * rate) as total_amount, component_type_id FROM payment_components 
        WHERE person_id = %s AND date >= %s AND date < %s
    """, (person_id, month_start, month_end))
    payment_comps = cursor.fetchall()
    
    for pc in payment_comps:
        amount = (pc["total_amount"] or 0) / 100
        if pc["component_type_id"] == 2 or pc["component_type_id"] == 7:
            monthly_totals["travel"] += amount
        else:
            monthly_totals["extras"] += amount
    
    cursor.close()
    
    # חישוב צבירות
    accruals = calculate_accruals(
        actual_work_days=monthly_totals["actual_work_days"],
        start_date_ts=person["start_date"],
        report_year=year,
        report_month=month
    )
    monthly_totals["sick_days_accrued"] = accruals["sick_days_accrued"]
    monthly_totals["vacation_days_accrued"] = accruals["vacation_days_accrued"]
    monthly_totals["vacation_details"] = accruals.get("vacation_details", {
        "seniority": 1,
        "annual_quota": 12,
        "job_scope_pct": 100
    })
    
    # חישוב תשלום סופי - זהה ל-guide_view
    pay = 0
    pay += (monthly_totals["calc100"] / 60) * minimum_wage * 1.0
    pay += (monthly_totals["calc125"] / 60) * minimum_wage * 1.25
    pay += (monthly_totals["calc150"] / 60) * minimum_wage * 1.5
    pay += (monthly_totals["calc175"] / 60) * minimum_wage * 1.75
    pay += (monthly_totals["calc200"] / 60) * minimum_wage * 2.0
    pay += monthly_totals.get("variable_rate_extra_payment", 0)  # תוספת עבור תעריף משתנה
    pay += monthly_totals["standby_payment"]
    pay += monthly_totals["vacation_payment"]
    monthly_totals["payment"] = pay  # תשלום בסיסי
    monthly_totals["total_payment"] = pay + monthly_totals["travel"] + monthly_totals["extras"]  # סה"כ כולל הכל

    # populate vacation display
    monthly_totals["vacation"] = monthly_totals["vacation_minutes"]

    return monthly_totals

def _calculate_totals_from_data(
    person,
    reports,
    segments_by_shift,
    shift_has_standby,
    payment_comps,
    standby_rates_cache,
    shabbat_cache,
    minimum_wage,
    year,
    month
) -> Dict:
    """
    Helper for calculating totals from pre-fetched data.
    Uses shared helper functions to avoid code duplication.
    """
    from utils import calculate_accruals
    # Initialize totals
    monthly_totals = {
        "total_hours": 0, "payment": 0, "standby": 0, "standby_payment": 0,
        "actual_work_days": 0, "vacation_days_taken": 0,
        "calc100": 0, "calc125": 0, "calc150": 0, "calc150_shabbat": 0,
        "calc150_shabbat_100": 0, "calc150_shabbat_50": 0,
        "calc150_overtime": 0, "calc175": 0, "calc200": 0,
        "calc_variable": 0,
        "vacation_minutes": 0, "vacation_payment": 0, "travel": 0, "extras": 0,
        "sick_days_accrued": 0, "vacation_days_accrued": 0
    }

    # עיבוד דיווחי שעות - רק אם יש דיווחים
    if reports:
        # Count standby from reports - count unique dates with standby
        dates_with_standby = set()
        for r in reports:
            if r["shift_type_id"] and shift_has_standby.get(r["shift_type_id"], False):
                dates_with_standby.add(r["date"])
        monthly_totals["standby"] = len(dates_with_standby)

        # בניית מפת ימים באמצעות הפונקציה המשותפת
        daily_map = _build_daily_map(reports, segments_by_shift, year, month)

        # פונקציה לקבלת תעריף כוננות מcache
        def get_standby_rate_from_cache(seg_id: int, apt_type: Optional[int], is_married: bool) -> float:
            marital_status = "married" if is_married else "single"
            rate = DEFAULT_STANDBY_RATE

            # Priority 10 - specific apartment type
            if apt_type is not None:
                val = standby_rates_cache.get((seg_id, apt_type, marital_status, 10))
                if val is not None:
                    return val

            # Priority 0 - default
            val = standby_rates_cache.get((seg_id, None, marital_status, 0))
            if val is not None:
                return val

            return rate

        # עיבוד מפת הימים באמצעות הפונקציה המשותפת
        totals, work_days_set, vacation_days_set = _process_daily_map(
            daily_map, shabbat_cache, get_standby_rate_from_cache, year, month
        )

        # העברת הסיכומים
        for key in ["calc100", "calc125", "calc150", "calc175", "calc200",
                    "calc150_shabbat", "calc150_overtime", "calc150_shabbat_100",
                    "calc150_shabbat_50", "total_hours", "standby_payment", "vacation_minutes"]:
            monthly_totals[key] = totals[key]

        # חישוב שעות בתעריף משתנה
        # נחשב את כל הדקות של עבודה מדיווחים עם תעריף שונה משכר מינימום
        # נשתמש ב-daily_map כדי לחשב רק את דקות העבודה (לא כוננות/חופשה)
        # נבנה מפה של shift_id -> תעריף משתנה (בש"ח)
        variable_rate_by_shift = {}
        for r in reports:
            shift_rate = r.get("shift_rate")
            is_minimum_wage = r.get("shift_is_minimum_wage", True)
            if shift_rate and not is_minimum_wage:
                # shift_rate stored in agorot, convert to shekels
                variable_rate_by_shift[r.get("shift_type_id")] = float(shift_rate) / 100

        # נחשב את דקות העבודה מדיווחים עם תעריף משתנה ואת התשלום הנוסף
        variable_rate_minutes = 0
        variable_rate_extra_payment = 0.0
        for day_key, entry in daily_map.items():
            for seg in entry.get("segments", []):
                s_start, s_end, s_type, shift_id, seg_id, apt_type, is_married = seg
                if s_type == "work" and shift_id in variable_rate_by_shift:
                    # זה סגמנט עבודה עם תעריף משתנה
                    duration = s_end - s_start
                    variable_rate_minutes += duration
                    # חישוב התשלום הנוסף (הפרש בין התעריף לשכר מינימום)
                    actual_rate = variable_rate_by_shift[shift_id]
                    rate_diff = actual_rate - minimum_wage
                    if rate_diff > 0:
                        # הוספת ההפרש (לפי 100% - כי ה-overtime כבר מחושב בחישוב הרגיל)
                        variable_rate_extra_payment += (duration / 60) * rate_diff

        monthly_totals["calc_variable"] = variable_rate_minutes
        monthly_totals["variable_rate_extra_payment"] = variable_rate_extra_payment

        monthly_totals["actual_work_days"] = len(work_days_set)
        monthly_totals["vacation_days_taken"] = len(vacation_days_set)
        monthly_totals["vacation_payment"] = (monthly_totals.get("vacation_minutes", 0) / 60) * minimum_wage

    # רכיבי תשלום - תמיד (לא בתוך ה-if!)
    for pc in payment_comps:
        amount = (pc["total_amount"] or 0) / 100
        if pc["component_type_id"] == 2 or pc["component_type_id"] == 7:
            monthly_totals["travel"] += amount
        else:
            monthly_totals["extras"] += amount

    # Accruals
    accruals = calculate_accruals(
        actual_work_days=monthly_totals["actual_work_days"],
        start_date_ts=person["start_date"],
        report_year=year,
        report_month=month
    )
    monthly_totals["sick_days_accrued"] = accruals["sick_days_accrued"]
    monthly_totals["vacation_days_accrued"] = accruals["vacation_days_accrued"]
    monthly_totals["vacation_details"] = accruals.get("vacation_details", {
        "seniority": 1,
        "annual_quota": 12,
        "job_scope_pct": 100
    })

    # Final Pay
    pay = 0
    pay += (monthly_totals["calc100"] / 60) * minimum_wage * 1.0
    pay += (monthly_totals["calc125"] / 60) * minimum_wage * 1.25
    pay += (monthly_totals["calc150"] / 60) * minimum_wage * 1.5
    pay += (monthly_totals["calc175"] / 60) * minimum_wage * 1.75
    pay += (monthly_totals["calc200"] / 60) * minimum_wage * 2.0
    pay += monthly_totals.get("variable_rate_extra_payment", 0)  # תוספת עבור תעריף משתנה
    pay += monthly_totals["standby_payment"]
    pay += monthly_totals["vacation_payment"]
    monthly_totals["payment"] = pay
    monthly_totals["total_payment"] = pay + monthly_totals["travel"] + monthly_totals["extras"]

    monthly_totals["vacation"] = monthly_totals["vacation_minutes"]

    return monthly_totals

def calculate_monthly_summary(conn, year: int, month: int) -> Tuple[List[Dict], Dict]:
    # Import month_range_ts locally to avoid circular imports
    from utils import month_range_ts
    from history import (get_person_status_for_month, get_apartment_type_for_month,
                         get_all_shift_rates_for_month, get_minimum_wage_for_month,
                         get_all_segments_for_month)
    
    # 1. Fetch Payment Codes
    payment_codes = get_payment_codes(conn)
    
    # 2. Fetch All Active People
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute("SELECT id, name, start_date, is_married, meirav_code FROM people WHERE is_active::integer = 1 ORDER BY name")
    people = cursor.fetchall()
    
    # 3. Time Reports (bulk)
    start_ts, end_ts = month_range_ts(year, month)
    cursor.execute("""
        SELECT tr.*, st.name as shift_name,
               st.rate AS shift_rate,
               st.is_minimum_wage AS shift_is_minimum_wage,
               a.apartment_type_id,
               p.is_married
        FROM time_reports tr
        LEFT JOIN shift_types st ON st.id = tr.shift_type_id
        LEFT JOIN apartments a ON tr.apartment_id = a.id
        LEFT JOIN people p ON tr.person_id = p.id
        WHERE tr.date >= %s AND tr.date < %s
        ORDER BY tr.person_id, tr.date, tr.start_time
    """, (start_ts, end_ts))
    all_reports_raw = cursor.fetchall()

    # Load historical data for overrides

    # Build person historical cache
    person_ids = {r["person_id"] for r in all_reports_raw if r["person_id"]}
    person_status_cache = {}
    for pid in person_ids:
        hist = get_person_status_for_month(conn, pid, year, month)
        if hist.get("is_married") is not None:
            person_status_cache[pid] = hist

    # Build apartment historical cache
    apartment_ids = {r["apartment_id"] for r in all_reports_raw if r["apartment_id"]}
    apartment_type_cache = {}
    for apt_id in apartment_ids:
        hist_type = get_apartment_type_for_month(conn, apt_id, year, month)
        if hist_type is not None:
            apartment_type_cache[apt_id] = hist_type

    # Build shift rates historical cache
    shift_rates_cache = get_all_shift_rates_for_month(conn, year, month)

    # Apply historical overrides
    reports_by_person = {}
    shift_type_ids = set()
    for r in all_reports_raw:
        r_dict = dict(r)
        pid = r_dict.get("person_id")
        apt_id = r_dict.get("apartment_id")
        shift_type_id = r_dict.get("shift_type_id")

        # Override is_married with historical value if available
        if pid and pid in person_status_cache:
            hist_married = person_status_cache[pid].get("is_married")
            if hist_married is not None:
                r_dict["is_married"] = hist_married

        # Override apartment_type_id with historical value if available
        if apt_id and apt_id in apartment_type_cache:
            r_dict["apartment_type_id"] = apartment_type_cache[apt_id]

        # Override shift rate with historical value if available
        if shift_type_id and shift_type_id in shift_rates_cache:
            rate_info = shift_rates_cache[shift_type_id]
            r_dict["shift_rate"] = rate_info.get("rate")
            r_dict["shift_is_minimum_wage"] = rate_info.get("is_minimum_wage")

        reports_by_person.setdefault(pid, []).append(r_dict)
        if shift_type_id:
            shift_type_ids.add(shift_type_id)
            
    # 4. Shift Segments - עם תמיכה בהיסטוריה
    segments_by_shift = {}
    shift_has_standby = {}
    if shift_type_ids:
        # Use historical segments if available, otherwise current segments
        segments_by_shift = get_all_segments_for_month(conn, list(shift_type_ids), year, month)
            
        for sid, segs in segments_by_shift.items():
            shift_has_standby[sid] = any(s["segment_type"] == "standby" for s in segs)

    # 5. Payment Components
    month_start = datetime(year, month, 1, tzinfo=LOCAL_TZ)
    if month == 12:
        month_end = datetime(year + 1, 1, 1, tzinfo=LOCAL_TZ)
    else:
        month_end = datetime(year, month + 1, 1, tzinfo=LOCAL_TZ)
        
    cursor.execute("""
        SELECT person_id, (quantity * rate) as total_amount, component_type_id 
        FROM payment_components 
        WHERE date >= %s AND date < %s
    """, (month_start, month_end))
    all_payment_comps = cursor.fetchall()
    payment_comps_by_person = {}
    for pc in all_payment_comps:
        payment_comps_by_person.setdefault(pc["person_id"], []).append(pc)

    # 6. Standby Rates - first check historical, then fallback to current
    standby_rates_cache = {}

    # Check for historical standby rates first
    cursor.execute("""
        SELECT segment_id, apartment_type_id, marital_status, amount
        FROM standby_rates_history
        WHERE year = %s AND month = %s
    """, (year, month))
    historical_rates = cursor.fetchall()

    if historical_rates:
        # Use historical rates
        for row in historical_rates:
            # Historical rates use priority 10 for apartment-specific, 0 for general
            priority = 10 if row["apartment_type_id"] is not None else 0
            key = (row["segment_id"], row["apartment_type_id"], row["marital_status"], priority)
            standby_rates_cache[key] = float(row["amount"]) / 100
    else:
        # Use current rates
        cursor.execute("SELECT * FROM standby_rates")
        all_standby_rates = cursor.fetchall()
        for row in all_standby_rates:
            key = (row["segment_id"], row["apartment_type_id"], row["marital_status"], row["priority"])
            standby_rates_cache[key] = float(row["amount"]) / 100

    # 7. Min Wage & Shabbat - שימוש בשכר מינימום היסטורי לפי החודש
    shabbat_cache = get_shabbat_times_cache(conn)
    minimum_wage = get_minimum_wage_for_month(conn, year, month)
        
    cursor.close()

    summary_data = []
    grand_totals = {code["internal_key"]: 0 for code in payment_codes}
    grand_totals.update({
        "payment": 0, "standby_payment": 0, "travel": 0, "extras": 0, "total_payment": 0,
        "calc150_shabbat_100": 0, "calc150_shabbat_50": 0,
        "vacation_payment": 0, "vacation_minutes": 0
    })

    # 8. Iterate and Calculate
    for p in people:
        pid = p["id"]
        monthly_totals = _calculate_totals_from_data(
            person=p,
            reports=reports_by_person.get(pid, []),
            segments_by_shift=segments_by_shift,
            shift_has_standby=shift_has_standby,
            payment_comps=payment_comps_by_person.get(pid, []),
            standby_rates_cache=standby_rates_cache,
            shabbat_cache=shabbat_cache,
            minimum_wage=minimum_wage,
            year=year,
            month=month
        )
        
        if monthly_totals.get("total_payment", 0) > 0 or monthly_totals.get("total_hours", 0) > 0:
            summary_data.append({"name": p["name"], "person_id": p["id"], "merav_code": p["meirav_code"], "totals": monthly_totals})
            
            # Add to Grand Totals
            # Note: The template uses grand_totals["payment"] for the final "Total Payment" column,
            # so we must accumulate the FULL total (including travel/extras) into "payment".
            grand_totals["payment"] += monthly_totals.get("total_payment", 0)
            grand_totals["total_payment"] += monthly_totals.get("total_payment", 0)
            
            for k, v in monthly_totals.items():
                if k in grand_totals and isinstance(v, (int, float)) and k not in ("payment", "total_payment"):
                    grand_totals[k] += v

    return summary_data, grand_totals


