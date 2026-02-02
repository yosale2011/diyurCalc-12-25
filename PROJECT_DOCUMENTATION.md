# DiyurCalc - מערכת חישוב שכר ומשמרות

## תיאור כללי

מערכת DiyurCalc היא מערכת לחישוב שכר עבודה עבור מדריכים בדיור מוגן. המערכת מטפלת בחישובי שכר מורכבים הכוללים שעות נוספות, משמרות שבת/חג, כוננויות, וסוגי משמרות מיוחדים.

**גרסה:** 2.09
**מסגרת:** FastAPI
**בסיס נתונים:** PostgreSQL

---

## מבנה הפרויקט

```
diyur003/
├── app.py                      # נקודת כניסה ראשית - FastAPI
├── app_utils.py               # פונקציות עזר לעיבוד סגמנטים יומיים
├── requirements.txt           # תלויות Python
│
├── core/                      # לוגיקה עסקית מרכזית
│   ├── config.py             # הגדרות תצורה
│   ├── database.py           # ניהול חיבורי PostgreSQL
│   ├── logic.py              # API ראשי לחישובים חודשיים
│   ├── wage_calculator.py    # מנוע חישוב שכר
│   ├── segments.py           # עיבוד סגמנטי משמרות
│   ├── time_utils.py         # פונקציות זמן ושבת
│   └── history.py            # נתונים היסטוריים
│
├── routes/                    # נתיבי Web
│   ├── home.py               # דף הבית
│   ├── guide.py              # דפי מדריך
│   ├── summary.py            # סיכומים חודשיים
│   ├── admin.py              # ניהול
│   ├── export.py             # ייצוא (Gesher, Excel)
│   └── email.py              # שליחת מיילים
│
├── services/                  # שירותים עסקיים
│   ├── email_service.py      # שליחת מיילים ו-PDF
│   └── gesher_exporter.py    # ייצוא לפורמט גשר
│
├── utils/                     # כלי עזר
│   ├── utils.py              # פונקציות כלליות
│   ├── cache_manager.py      # ניהול מטמון
│   └── error_handler.py      # טיפול בשגיאות
│
├── templates/                # תבניות HTML (Jinja2)
├── static/                   # קבצים סטטיים
├── tests/                    # בדיקות
└── scripts/                  # סקריפטים
```

---

## לוגיקת חישוב השכר

### 1. מבנה יום עבודה

יום עבודה מוגדר **מ-08:00 עד 08:00 למחרת** (לא מחצות לחצות).

- דיווחים שמסתיימים לפני 08:00 שייכים ליום העבודה **הקודם**
- דיווחים שמתחילים אחרי 08:00 שייכים ליום העבודה **הנוכחי**

### 2. חישוב אחוזי שכר

| שעות ברצף | חול | שבת/חג |
|-----------|------|--------|
| 0-8 שעות | 100% | 150% |
| 8-10 שעות | 125% | 175% |
| 10+ שעות | 150% | 200% |

**הערה חשובה:** ה-150% בשעות נוספות (overtime) שונה מ-150% בשבת:
- `calc150_overtime` - שעות נוספות בימי חול
- `calc150_shabbat` - שעות בשבת (100% + 50% תוספת)

### 3. רצפי עבודה (Chains)

- משמרות עם הפסקה של **פחות מ-60 דקות** נחשבות כרצף אחד
- משמרות עם הפסקה של **60 דקות או יותר** מתחילות רצף חדש
- כל רצף מתחיל את ספירת השעות מחדש

```
דוגמה:
08:00-16:00 (8 שעות 100%)
הפסקה 30 דקות
16:30-18:30 (2 שעות 125%) - המשך אותו רצף
```

### 4. זיהוי שבת

שעות שבת נקבעות לפי **זמני כניסת/יציאת שבת** מטבלת `shabbat_times`:
- **כניסת שבת:** זמן הדלקת נרות ביום שישי (ברירת מחדל: 16:00)
- **יציאת שבת:** צאת הכוכבים בשבת (ברירת מחדל: 22:00)

---

## סוגי משמרות

### IDs של משמרות

| ID | סוג משמרת | תיאור |
|----|-----------|--------|
| 105 | FRIDAY_SHIFT | משמרת שישי/ערב חג |
| 106 | SHABBAT_SHIFT | משמרת שבת/חג |
| 107 | NIGHT_SHIFT | משמרת לילה |
| 108 | TAGBUR_FRIDAY | תגבור שישי/ערב חג |
| 109 | TAGBUR_SHABBAT | תגבור שבת/חג |
| 120 | HOSPITAL_ESCORT | לווי בי"ח |
| 148 | MEDICAL_ESCORT | ליווי רפואי |

### משמרת לילה (107)

מבנה דינמי לפי זמן כניסה:
1. **2 שעות ראשונות:** עבודה (לפי אחוז רצף)
2. **עד 06:30:** כוננות (24%)
3. **06:30-08:00:** עבודה (לפי אחוז רצף)

### משמרות תגבור (108, 109)

משמרות עם **אחוזים קבועים** מסגמנטים מוגדרים מראש - לא מחושבות לפי רצף.

### תגבור משתמע (Implicit Tagbur)

משמרת שישי/שבת (105/106) בדירה טיפולית (type=2) עם תעריף דירה רגילה (rate_apt=1) = תגבור.

### משמרת לווי בי"ח (120)

חוקים מיוחדים:
- **חול:** תעריף קבוע מטבלה
- **שבת הלכתית:** שכר מינימום בלבד

---

## סוגי דירות

| ID | סוג | תיאור |
|----|-----|--------|
| 1 | REGULAR | דירה רגילה |
| 2 | THERAPEUTIC | דירה טיפולית |

דירות טיפוליות מקבלות תעריפי כוננות גבוהים יותר.

---

## כוננות (Standby)

### חוקי ביטול כוננות

- **חפיפה >= 70%** עם עבודה: כוננות מתבטלת
  - מנוכה עד 70 ש"ח
  - אם תעריף > 70 ש"ח, משלמים את ההפרש
- **חפיפה < 70%:** הכוננות נשמרת, זמן העבודה מקוזז

### תעריפי כוננות

נקבעים לפי:
- סוג סגמנט (`segment_id`)
- סוג דירה (`apartment_type_id`)
- מצב משפחתי (`marital_status`: married/single)
- עדיפות (`priority`: 10=ספציפי, 0=כללי)

---

## נתונים היסטוריים

המערכת תומכת ב"שמירה בעת שינוי" (Save on Change):

### טבלאות היסטוריה

| טבלה | תיאור |
|------|--------|
| `person_status_history` | מצב אישי (נשוי, סוג עובד) |
| `apartment_status_history` | סוג דירה |
| `shift_type_housing_rates_history` | תעריפי משמרות לפי מערך דיור |
| `standby_rates_history` | תעריפי כוננות |

### לוגיקת "Valid Until"

- רשומה היסטורית מכילה `(year, month)` = "תקף עד"
- הערך הישן היה תקף **עד לפני** אותו חודש
- אם אין רשומה היסטורית - משתמשים בערך הנוכחי

---

## פונקציות מרכזיות

### core/logic.py

```python
calculate_person_monthly_totals(conn, person_id, year, month, shabbat_cache, minimum_wage)
```
**חישוב סיכומים חודשיים לעובד**

מחזיר:
- `calc100`, `calc125`, `calc150`, `calc175`, `calc200` - דקות לפי אחוז
- `calc150_shabbat`, `calc150_overtime` - פירוט 150%
- `calc150_shabbat_100`, `calc150_shabbat_50` - פיצול לפנסיה
- `standby`, `standby_payment` - כוננויות
- `vacation_minutes`, `vacation_payment` - חופשה
- `travel`, `extras` - רכיבי תשלום נוספים

### core/wage_calculator.py

```python
calculate_wage_rate(minutes_in_chain, is_shabbat) -> str
```
**קביעת אחוז שכר** לפי דקות ברצף וסטטוס שבת

```python
_calculate_chain_wages(chain_segments, day_date, shabbat_cache, minutes_offset)
```
**חישוב שכר לרצף עבודה** בשיטת בלוקים

```python
_process_daily_map(daily_map, shabbat_cache, get_standby_rate_fn, year, month)
```
**עיבוד מפת ימים** וחישוב סיכומים

### core/segments.py

```python
_build_daily_map(reports, segments_by_shift, year, month)
```
**בניית מפת ימים** מדיווחים

```python
get_hospital_escort_shabbat_ranges(r_date, start, end, shabbat_cache)
```
**זיהוי טווחי שבת** במשמרת לווי בי"ח

### app_utils.py

```python
get_daily_segments_data(conn, person_id, year, month, shabbat_cache, minimum_wage)
```
**נתוני סגמנטים יומיים מפורטים** לתצוגה

---

## זרימת החישוב

```
1. שליפת דיווחים מ-time_reports
           ↓
2. החלת נתונים היסטוריים (מצב אישי, סוג דירה, תעריפים)
           ↓
3. בניית daily_map - מיפוי דיווחים לימי עבודה
           ↓
4. לכל יום:
   a. פיצול לסגמנטים (עבודה, כוננות, חופשה)
   b. זיהוי וביטול/קיזוז כוננויות
   c. חלוקה לרצפי עבודה (chains)
           ↓
5. לכל רצף:
   a. חישוב אחוזים לפי שעות ברצף
   b. פיצול לפי גבולות שבת
   c. צבירת דקות לפי אחוז
           ↓
6. סיכום חודשי:
   - שעות לפי אחוז
   - תשלום עבודה
   - תשלום כוננות
   - רכיבי תשלום נוספים (נסיעות, תוספות)
```

---

## ייצוא גשר (Gesher)

פורמט תקני לשכר בישראל. המערכת מייצאת:
- קודי תשלום (`payment_codes`)
- המרת ערכים (שעות/דקות/כסף)
- תמיכה בקידודים: ASCII, Windows-1255, UTF-8

---

## Code Review - ממצאים

### פונקציות לא בשימוש

הפונקציות הבאות מוגדרות אך אינן נקראות מהקוד הפעיל:

| קובץ | פונקציה | הערה |
|------|---------|------|
| `core/logic.py` | `get_db_connection()` | Legacy - השתמש ב-`get_conn()` |
| `core/logic.py` | `dict_cursor()` | Legacy - לא בשימוש |
| `core/history.py` | `get_historical_months()` | לדיבוג בלבד |
| `core/history.py` | `get_segments_for_shift_month()` | נקרא רק מ-`get_all_segments_for_month` |
| `core/history.py` | `get_all_segments_for_month()` | לא בשימוש |
| `utils/utils.py` | `available_months()` | Legacy - הועבר ל-SQL |
| `utils/utils.py` | `to_local_date_for_months()` | נקרא רק מ-`available_months` |

### כפילויות קוד

1. **קבועי משמרות** מוגדרים בשני מקומות:
   - `core/segments.py`
   - `app_utils.py`

   **המלצה:** לאחד ב-`core/segments.py` ולייבא משם

2. **MAX_CANCELLED_STANDBY_DEDUCTION** מוגדר ב:
   - `core/wage_calculator.py`
   - `app_utils.py`

   **המלצה:** לייבא מ-`wage_calculator.py`

3. **לוגיקת משמרת לילה** מופיעה פעמיים:
   - `core/segments.py`: `_build_night_shift_segments()`
   - `app_utils.py`: inline בפונקציה `get_daily_segments_data()`

   **המלצה:** להשתמש בפונקציה מ-`segments.py`

### פרמטר לא בשימוש

ב-`core/time_utils.py`:
```python
def is_shabbat_time(day_of_week, minute_in_day, shift_id, current_date, shabbat_cache)
```
הפרמטר `shift_id` לא בשימוש (הערה קיימת בקוד).

---

## קבועים חשובים

```python
# זמן
MINUTES_PER_HOUR = 60
MINUTES_PER_DAY = 1440
WORK_DAY_START_MINUTES = 480  # 08:00

# שעות נוספות
REGULAR_HOURS_LIMIT = 480     # 8 שעות = 100%
OVERTIME_125_LIMIT = 600      # 10 שעות = 125%

# הפסקות
BREAK_THRESHOLD_MINUTES = 60  # הפסקה > 60 = רצף חדש

# כוננות
STANDBY_CANCEL_OVERLAP_THRESHOLD = 0.70  # 70%
DEFAULT_STANDBY_RATE = 70.0
MAX_CANCELLED_STANDBY_DEDUCTION = 70.0

# שבת (ברירות מחדל)
SHABBAT_ENTER_DEFAULT = 960   # 16:00
SHABBAT_EXIT_DEFAULT = 1320   # 22:00
```

---

## טבלאות בסיס נתונים

### טבלאות ראשיות

| טבלה | תיאור |
|------|--------|
| `people` | עובדים |
| `apartments` | דירות |
| `apartment_types` | סוגי דירות |
| `employers` | מעסיקים |
| `time_reports` | דיווחי עבודה |
| `shift_types` | סוגי משמרות |
| `shift_time_segments` | סגמנטי זמן למשמרת |
| `standby_rates` | תעריפי כוננות |
| `minimum_wage_rates` | שכר מינימום היסטורי |
| `payment_codes` | קודי תשלום |
| `payment_components` | רכיבי תשלום (נסיעות וכו') |
| `shabbat_times` | זמני שבת |
| `month_locks` | נעילת חודשים |

---

## API Endpoints

### דפים ראשיים

| נתיב | תיאור |
|------|--------|
| `GET /` | דף הבית |
| `GET /guide/{person_id}` | דף מדריך |
| `GET /summary` | סיכום חודשי |
| `GET /admin/payment-codes` | ניהול קודי תשלום |

### ייצוא

| נתיב | תיאור |
|------|--------|
| `GET /export/gesher` | ייצוא גשר |
| `GET /export/excel` | ייצוא Excel |

### API

| נתיב | תיאור |
|------|--------|
| `POST /api/lock-month` | נעילת חודש |
| `POST /api/unlock-month` | פתיחת חודש |
| `POST /api/send-guide-email/{id}` | שליחת דוח למדריך |

---

## סיכום

מערכת DiyurCalc היא מערכת מורכבת לחישוב שכר עם:
- תמיכה בסוגי משמרות מגוונים
- חישוב שעות נוספות ושבת
- ניהול כוננויות
- נתונים היסטוריים
- ייצוא לפורמטים תקניים

הקוד מאורגן היטב עם הפרדה ברורה בין שכבות (לוגיקה, נתיבים, שירותים).
