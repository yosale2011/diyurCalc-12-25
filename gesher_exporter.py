"""
Gesher File Exporter
מייצא קובץ בפורמט גשר למערכת מירב
"""
import io
import configparser
from pathlib import Path
from typing import Dict, List, Tuple, Any

# נתיב לקובץ התצורה
CONFIG_PATH = Path(__file__).parent / "gesher_config.ini"

# קודים שלא לייצא לקובץ גשר בשום מצב
EXCLUDED_EXPORT_CODES = {'130', '199'}



def load_export_config_from_db(conn) -> Dict[str, Tuple[str, str, str]]:
    """
    טוען את תצורת הייצוא ממסד הנתונים (טבלת payment_codes).
    מחזיר: {symbol: (internal_key, value_type, display_name)}
    """
    try:
        rows = conn.execute("""
            SELECT internal_key, merav_code, display_name 
            FROM payment_codes 
            WHERE merav_code IS NOT NULL AND merav_code != ''
        """).fetchall()
        
        export_codes = {}
        
        # מיפוי סוגי נתונים ברירת מחדל
        # מפתח פנימי -> סוג ייצוא
        type_mapping = {
            # שעות רגילות ונוספות
            'calc100': 'hours_100',
            'calc125': 'hours_125',
            'calc150': 'hours_150',
            'calc150_overtime': 'hours_150',
            
            # שעות שבת - מפוצל לפנסיה
            'calc150_shabbat_100': 'hours_100',
            'calc150_shabbat_50': 'hours_50',
            'calc175': 'hours_175',
            'calc200': 'hours_200',
            
            # כוננויות - כמות ותעריף ממוצע
            'standby': 'standby_with_rate',
            'standby_payment': 'money',  # לא בשימוש אם משתמשים ב-standby_with_rate
            
            # חופשה (מדקות)
            'vacation': 'hours_100',
            'vacation_minutes': 'hours_100',
            
            # סכומים ישירים
            'travel': 'money',
            'extras': 'money',
            
            # נתונים אינפורמטיביים
            'actual_work_days': 'days_with_total_hours',  # ימים + סה"כ שעות
            'sick_days_accrued': 'days',      # ימים
            'vacation_days_accrued': 'days',  # ימים
            'vacation_days_taken': 'days',    # ימים
            'sick_days_taken': 'days'         # ימים
        }
        
        for row in rows:
            internal_key = row['internal_key']
            symbol = row['merav_code']
            display_name = row['display_name'] or internal_key
            
            # אם אין סמל, מדלגים
            if not symbol or not symbol.strip():
                continue
            
            # סינון קודים אסורים לייצוא
            if symbol in EXCLUDED_EXPORT_CODES:
                continue
                
            # קביעת הסוג
            # 1. בדיקה במיפוי הקשיח
            value_type = type_mapping.get(internal_key)
            
            # 2. אם לא נמצא, ננסה לנחש לפי המפתח
            if not value_type:
                if 'hours' in internal_key or 'calc' in internal_key:
                    value_type = 'hours_100'
                elif 'days' in internal_key:
                    value_type = 'days'
                elif 'payment' in internal_key or 'travel' in internal_key or 'extras' in internal_key:
                    value_type = 'money'
                else:
                    value_type = 'count'
            
            export_codes[symbol] = (internal_key, value_type, display_name)
            
        return export_codes
    except Exception as e:
        print(f"Error loading export config from DB: {e}")
        return {} # Fallback to empty or file config if needed

def load_export_config() -> Dict[str, Tuple[str, str, str]]:
    """
    Legacy: loads from INI file.
    Kept for backward compatibility if needed, but generate_gesher_file will use DB version.
    """
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH, encoding='utf-8')
    
    export_codes = {}
    if 'EXPORT_CODES' in config:
        for symbol, value in config['EXPORT_CODES'].items():
            parts = [p.strip() for p in value.split(',')]
            if len(parts) >= 2:
                internal_key = parts[0]
                value_type = parts[1]  # hours, money, days, count
                display_name = internal_key  # אין שם עברי ב-INI
                export_codes[symbol] = (internal_key, value_type, display_name)
    
    return export_codes


def get_export_options() -> Dict[str, Any]:
    """טוען אפשרויות ייצוא"""
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH, encoding='utf-8')
    
    options = {
        'export_zero_values': False,
        'min_amount': 0.01,
        'default_company': '001'
    }
    
    if 'OPTIONS' in config:
        options['export_zero_values'] = config.getboolean('OPTIONS', 'export_zero_values', fallback=False)
        options['min_amount'] = config.getfloat('OPTIONS', 'min_amount', fallback=0.01)
    
    if 'FORMAT' in config:
        options['default_company'] = config.get('FORMAT', 'default_company', fallback='001')
    
    return options


def get_companies(conn) -> Dict[str, str]:
    """טוען רשימת מפעלים מטבלת employers במסד הנתונים"""
    companies = {}
    try:
        rows = conn.execute("SELECT code, name FROM employers WHERE is_active::integer = 1").fetchall()
        for row in rows:
            companies[row['code']] = row['name']
    except Exception as e:
        print(f"Error loading companies from DB: {e}")
    
    return companies


def calculate_value(totals: Dict, internal_key: str, value_type: str, minimum_wage: float = 34.40) -> Tuple[float, float]:
    """
    מחשב את הערך לייצוא - מחזיר (כמות, תעריף)
    
    hours_XXX - שעות עם תעריף XXX% (מחזיר שעות ותעריף לשעה)
    money - סכום ישיר (מחזיר 0 וסכום)
    days - ימים (מחזיר ימים ו-0)
    count - ספירה (מחזיר כמות ו-0)
    standby_with_rate - כוננויות (כמות ותעריף ממוצע)
    """
    raw_value = totals.get(internal_key, 0) or 0
    
    if value_type == 'money':
        # סכום ישיר - אין כמות, רק סכום
        return (0.0, round(raw_value, 2))
    
    elif value_type.startswith('hours_'):
        # שעות עם תעריף - מחזיר שעות (ממירות לשעות) ותעריף לשעה
        multiplier_str = value_type.replace('hours_', '')
        try:
            multiplier = float(multiplier_str) / 100  # hours_100 -> 1.0, hours_125 -> 1.25
        except ValueError:
            multiplier = 1.0
        hours = round(raw_value / 60, 2)
        hourly_rate = round(minimum_wage * multiplier, 2)
        return (hours, hourly_rate)
    
    elif value_type == 'days':
        # ימים - לא לחלק ב-60!
        return (round(raw_value, 2), 0.0)
    
    elif value_type == 'count':
        # ספירה - לא לחלק ב-60!
        return (round(raw_value, 2), 0.0)
    
    elif value_type == 'days_with_total_hours':
        # ימי עבודה - כמות = ימים, תעריף = סה"כ שעות
        days = round(raw_value, 2)
        # נחשב סה"כ שעות מה-calc שדות
        total_hours = (
            totals.get('calc100', 0) + 
            totals.get('calc125', 0) + 
            totals.get('calc150', 0) + 
            totals.get('calc175', 0) + 
            totals.get('calc200', 0)
        ) / 60  # ממירות לשעות
        return (days, round(total_hours, 2))
    
    elif value_type == 'standby_with_rate':
        # כוננויות - כמות ותעריף ממוצע
        standby_count = totals.get('standby', 0) or 0
        standby_payment = totals.get('standby_payment', 0) or 0
        if standby_count > 0:
            avg_rate = round(standby_payment / standby_count, 2)
        else:
            avg_rate = 0.0
        return (round(standby_count, 2), avg_rate)
    
    else:
        # ברירת מחדל - כמות בלבד
        return (round(raw_value, 2), 0.0)


def get_minimum_wage(conn) -> float:
    """שליפת שכר מינימום מהדאטאבייס"""
    try:
        row = conn.execute("SELECT hourly_rate FROM minimum_wage_rates ORDER BY effective_from DESC LIMIT 1").fetchone()
        if row and row["hourly_rate"]:
            return float(row["hourly_rate"]) / 100
    except Exception as e:
        print(f"Warning: Failed to get minimum wage from DB: {e}")
    return 34.40  # ברירת מחדל


def format_gesher_header(company: str, year: int, month: int) -> str:
    """
    פורמט כותרת קובץ גשר
    מבנה: מס' מפעל(3) + רווח + שנה(2) + רווח + חודש(2) + רזרבה
    """
    yy = str(year)[-2:]
    mm = f"{month:02d}"
    return f"{company:>3s} {yy} {mm}      0"


def format_gesher_line(employee_code: int, symbol: str, quantity: float, rate: float) -> str:
    """
    פורמט שורת נתונים בקובץ גשר
    
    מבנה רשומת נתונים:
    Pos 1-6:   מספר עובד (6 תווים)
    Pos 8-10:  מספר סמל (3 תווים)
    Pos 12-18: כמות (7 תווים, XXXX.XX)
    Pos 20-27: תעריף (8 תווים, XXXXX.XX)
    Pos 29-37: רווחים
    Pos 38-40: סיומת (201)
    """
    emp = f"{employee_code:06d}"           # 6 chars
    sym = f"{symbol:>3s}"                  # 3 chars
    qty = f"{quantity:07.2f}"              # 7 chars (XXXX.XX)
    rt = f"{rate:08.2f}"                   # 8 chars (XXXXX.XX)
    
    # פורמט: 005835 360 0020.50 00034.30          201
    line = f"{emp} {sym} {qty} {rt}          201"
    return line


def generate_gesher_file_for_person(conn, person_id: int, year: int, month: int) -> Tuple[str, str]:
    """
    מייצר קובץ גשר לעובד בודד
    
    Args:
        conn: חיבור למסד הנתונים
        person_id: מזהה עובד
        year: שנה
        month: חודש
    
    Returns:
        Tuple[תוכן הקובץ, קוד מפעל]
    """
    from app import calculate_person_monthly_totals
    from logic import get_shabbat_times_cache
    
    # שליפת פרטי העובד כולל מפעל
    person = conn.execute("""
        SELECT p.id, p.name, p.meirav_code, e.code as employer_code
        FROM people p
        LEFT JOIN employers e ON p.employer_id = e.id
        WHERE p.id = ?
    """, (person_id,)).fetchone()
    
    if not person or not person['meirav_code']:
        return ("", "")
    
    company = person['employer_code'] or '001'
    
    # טעינת תצורה
    export_codes = load_export_config_from_db(conn)
    if not export_codes:
        export_codes = load_export_config()
    options = get_export_options()
    
    shabbat_cache = get_shabbat_times_cache(conn)
    minimum_wage = get_minimum_wage(conn)
    
    # וידוא קוד מירב
    try:
        meirav_code_clean = ''.join(filter(str.isdigit, str(person['meirav_code'])))
        if not meirav_code_clean:
            return ("", "")
        employee_code = int(meirav_code_clean)
    except ValueError:
        return ("", "")
    
    # חישוב סיכומים
    totals = calculate_person_monthly_totals(
        conn=conn,
        person_id=person_id,
        year=year,
        month=month,
        shabbat_cache=shabbat_cache,
        minimum_wage=minimum_wage
    )
    
    output = io.StringIO()
    
    # כותרת - CRLF
    header = format_gesher_header(company, year, month)
    output.write(header + "\r\n")
    
    line_count = 0
    
    # יצירת שורות
    for symbol, value_tuple in export_codes.items():
        # תמיכה גם בפורמט ישן (2 איברים) וגם חדש (3 איברים)
        if len(value_tuple) == 3:
            internal_key, value_type, display_name = value_tuple
        else:
            internal_key, value_type = value_tuple
        
        quantity, rate = calculate_value(totals, internal_key, value_type, minimum_wage)
        
        if not options['export_zero_values']:
            if value_type.startswith('hours_') and quantity < options['min_amount']:
                continue
            elif value_type == 'money' and rate < options['min_amount']:
                continue
            elif quantity < options['min_amount'] and rate < options['min_amount']:
                continue
        
        line = format_gesher_line(
            employee_code=employee_code,
            symbol=symbol,
            quantity=quantity,
            rate=rate
        )
        output.write(line + "\r\n")
        line_count += 1
    
    result = output.getvalue()
    print(f"Gesher export for person {person_id}: {line_count} lines")
    return (result, company)


def generate_gesher_file(conn, year: int, month: int, filter_name: str = None, company: str = None) -> str:
    """
    מייצר קובץ גשר לייצוא למירב
    משתמש ב-calculate_monthly_summary לחישוב יעיל של כל העובדים בבת אחת

    Args:
        conn: חיבור למסד הנתונים
        year: שנה
        month: חודש
        filter_name: סינון לפי שם עובד (אופציונלי, לבדיקות)
        company: קוד מפעל (001 או 400)

    Returns:
        תוכן הקובץ כמחרוזת
    """
    from logic import calculate_monthly_summary

    # טעינת תצורה מהדאטאבייס
    export_codes = load_export_config_from_db(conn)

    # אם אין הגדרות ב-DB, ננסה לטעון מהקובץ כגיבוי
    if not export_codes:
        export_codes = load_export_config()

    options = get_export_options()

    # קביעת מפעל
    if company is None:
        company = options.get('default_company', '001')

    minimum_wage = get_minimum_wage(conn)

    # שליפת מיפוי עובדים למפעלים
    cursor = conn.execute("""
        SELECT p.id, p.name, p.meirav_code, e.code as employer_code
        FROM people p
        LEFT JOIN employers e ON p.employer_id = e.id
        WHERE p.is_active::integer = 1 AND p.meirav_code IS NOT NULL AND p.meirav_code != ''
        ORDER BY p.name
    """)
    all_people = {row['id']: row for row in cursor.fetchall()}

    # חישוב יעיל - כל העובדים בבת אחת
    raw_conn = conn.conn if hasattr(conn, 'conn') else conn
    summary_data, _ = calculate_monthly_summary(raw_conn, year, month)

    # בניית מיפוי person_id -> totals
    totals_by_id = {}
    for person_data in summary_data:
        pid = person_data.get('id')
        if pid:
            totals_by_id[pid] = person_data.get('totals', {})

    output = io.StringIO()

    # כותרת - CRLF (Windows format: 0D 0A)
    header = format_gesher_header(company, year, month)
    output.write(header + "\r\n")

    line_count = 0

    for person_id, person in all_people.items():
        # סינון לפי מפעל
        if person.get('employer_code') != company:
            continue

        # סינון לפי שם (אם נדרש)
        if filter_name and filter_name.lower() not in person['name'].lower():
            continue

        meirav_code = person['meirav_code']

        # וידוא שקוד מירב תקין
        try:
            meirav_code_clean = ''.join(filter(str.isdigit, str(meirav_code)))
            if not meirav_code_clean:
                continue
            employee_code = int(meirav_code_clean)
        except ValueError:
            continue

        # קבלת הסיכומים מה-cache
        totals = totals_by_id.get(person_id, {})

        # יצירת שורה לכל סמל
        for symbol, value_tuple in export_codes.items():
            # תמיכה גם בפורמט ישן (2 איברים) וגם חדש (3 איברים)
            if len(value_tuple) == 3:
                internal_key, value_type, display_name = value_tuple
            else:
                internal_key, value_type = value_tuple

            quantity, rate = calculate_value(totals, internal_key, value_type, minimum_wage)

            # דילוג על ערכים אפסיים
            if not options['export_zero_values']:
                if value_type.startswith('hours_') and quantity < options['min_amount']:
                    continue
                elif value_type == 'money' and rate < options['min_amount']:
                    continue
                elif quantity < options['min_amount'] and rate < options['min_amount']:
                    continue

            # פורמט השורה לפי מבנה גשר - CRLF (Windows format: 0D 0A)
            line = format_gesher_line(
                employee_code=employee_code,
                symbol=symbol,
                quantity=quantity,
                rate=rate
            )
            output.write(line + "\r\n")
            line_count += 1

    result = output.getvalue()
    print(f"Gesher export: {line_count} lines for company {company}")
    return result


def get_export_preview(conn, year: int, month: int, limit: int = 50) -> List[Dict]:
    """
    מחזיר תצוגה מקדימה של הייצוא
    משתמש ב-calculate_monthly_summary לחישוב יעיל של כל העובדים בבת אחת
    """
    from logic import calculate_monthly_summary

    # טעינת תצורה מהדאטאבייס
    export_codes = load_export_config_from_db(conn)

    # אם אין הגדרות ב-DB, ננסה לטעון מהקובץ כגיבוי
    if not export_codes:
        export_codes = load_export_config()

    options = get_export_options()
    minimum_wage = get_minimum_wage(conn)

    # חישוב יעיל - כל העובדים בבת אחת
    summary_data, _ = calculate_monthly_summary(conn.conn if hasattr(conn, 'conn') else conn, year, month)

    preview = []

    for person_data in summary_data:
        meirav_code = person_data.get('merav_code') or person_data.get('meirav_code')
        if not meirav_code:
            continue

        person_id = person_data.get('person_id') or person_data.get('id')
        person_name = person_data.get('name', '')
        totals = person_data.get('totals', {})

        person_lines = []
        for symbol, (internal_key, value_type, display_name) in export_codes.items():
            quantity, payment = calculate_value(totals, internal_key, value_type, minimum_wage)
            if quantity >= options['min_amount'] or payment >= options['min_amount']:
                person_lines.append({
                    'symbol': symbol,
                    'key': internal_key,
                    'display_name': display_name,
                    'type': value_type,
                    'quantity': quantity,
                    'payment': payment
                })

        if person_lines:
            preview.append({
                'person_id': person_id,
                'name': person_name,
                'meirav_code': meirav_code,
                'lines': person_lines
            })

    return preview


