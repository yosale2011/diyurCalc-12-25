# DiyurCalc - מדריך לקלוד

## סקירה כללית
מערכת חישוב משכורות למדריכים בעמותת צהר. גרסה 2.09.

**טכנולוגיות:** FastAPI + PostgreSQL + Jinja2

## מבנה הפרויקט

```
diyur003/
├── app.py                 # נקודת כניסה FastAPI
├── app_utils.py           # מקור האמת לחישוב שכר + סגמנטים יומיים
├── core/                  # לוגיקה עסקית
│   ├── config.py          # הגדרות (VERSION, DB, timezone)
│   ├── constants.py       # קבועים (shift IDs, זמנים)
│   ├── database.py        # חיבורי PostgreSQL + pooling
│   ├── logic.py           # API ראשי לחישוב חודשי (מעטפת ל-app_utils)
│   ├── time_utils.py      # זמן, זיהוי שבת
│   ├── history.py         # נתונים היסטוריים
│   └── sick_days.py       # לוגיקת ימי מחלה
├── routes/                # נתיבי API
│   ├── home.py            # דף בית + רשימת מדריכים
│   ├── guide.py           # תצוגה מפורטת למדריך
│   ├── summary.py         # סיכום חודשי כללי
│   ├── admin.py           # ניהול, נעילת חודשים
│   ├── export.py          # ייצוא Gesher, Excel
│   └── email.py           # שליחת מיילים
├── services/              # שירותים
│   ├── email_service.py
│   └── gesher_exporter.py
├── utils/                 # כלים
│   ├── cache_manager.py
│   ├── error_handler.py
│   └── utils.py
├── templates/             # HTML (Jinja2)
└── tests/                 # בדיקות pytest
```

## מושגים עסקיים חשובים

### יום עבודה
- **08:00 עד 08:00** למחרת (לא 00:00-24:00)
- דיווח 22:00-06:30 = יום עבודה אחד

### סוגי משמרות (Shift IDs)
| קוד | שם | התנהגות |
|-----|-----|---------|
| 105 | שישי | משמרת שבת רגילה |
| 106 | שבת | משמרת שבת רגילה |
| 107 | לילה | סגמנטים דינמיים (2שעות עבודה → כוננות → עבודה) |
| 108 | תגבור שישי | סגמנטים קבועים, ללא רצף |
| 109 | תגבור שבת | סגמנטים קבועים, ללא רצף |
| 120 | ליווי בי"ח | תעריפים מיוחדים |
| 148 | ליווי רפואי | תעריף קבוע בחול, שכר מינימום בשבת |

### שעות נוספות (ימי חול)
```
0-8 שעות (0-480 דק')     → 100%
8-10 שעות (480-600 דק')  → 125%
10+ שעות (600+ דק')      → 150%
```

### תוספות שבת
```
0-8 שעות    → 150% (מתפצל: 100% בסיס + 50% תוספת)
8-10 שעות   → 175%
10+ שעות    → 200%
```

### רצפי עבודה (Chains)
- רצף = עבודה רציפה ללא הפסקה > 60 דקות
- הפסקה > 60 דק' = שבירת רצף (ספירה מחדש)
- כוננות שוברת רצף

### כוננויות (Standby)
- תעריף קבוע לפי סוג דירה + מצב משפחתי
- שוברת רצף
- מתבטלת אם עבודה חופפת ≥70%

### Carryover
העברת דקות בין ימים כשרצף מסתיים ב-08:00 בדיוק והיום הבא מתחיל ב-08:00.

## קבועים חשובים (constants.py)

```python
FRIDAY_SHIFT_ID = 105
SHABBAT_SHIFT_ID = 106
NIGHT_SHIFT_ID = 107
TAGBUR_FRIDAY_SHIFT_ID = 108
TAGBUR_SHABBAT_SHIFT_ID = 109

REGULAR_HOURS_LIMIT = 480      # 8 שעות בדקות
OVERTIME_125_LIMIT = 600       # 10 שעות בדקות
BREAK_THRESHOLD_MINUTES = 60   # הפסקה שוברת רצף

STANDBY_CANCEL_OVERLAP_THRESHOLD = 0.70  # 70%
```

## נתיבים עיקריים (Routes)

```
GET /                          → דף בית
GET /guide/{person_id}         → תצוגה מפורטת
GET /guide/{person_id}/simple  → סיכום פשוט
GET /summary                   → סיכום חודשי כללי
GET /export/gesher             → ייצוא למירב
GET /export/excel              → ייצוא Excel
```

## פונקציות מרכזיות

### core/logic.py
- `calculate_person_monthly_totals()` - חישוב סיכום חודשי (מעטפת)

### app_utils.py
- `get_daily_segments_data()` - נתוני סגמנטים לתצוגה
- `aggregate_daily_segments_to_monthly()` - צבירה לסיכום חודשי
- `calculate_wage_rate()` - קביעת אחוז שכר
- `_calculate_chain_wages()` - חישוב רצף עבודה
- `get_standby_rate()` - קבלת תעריף כוננות מה-DB

### core/sick_days.py
- `_identify_sick_day_sequences()` - זיהוי רצפי ימי מחלה
- `get_sick_payment_rate()` - אחוז תשלום לפי יום מחלה

## ארכיטקטורת החישוב

**מקור האמת היחיד: `app_utils.py`**
- `get_daily_segments_data()` - בניית סגמנטים יומיים
- `aggregate_daily_segments_to_monthly()` - צבירה לסיכום חודשי

**`logic.py` = מעטפת בלבד** - קורא ל-`app_utils.py` ומחזיר תוצאות.

**אין תלויות מעגליות** - `app_utils.py` לא מייבא מ-`logic.py`.

## ⚠️ אזהרות חשובות

1. **זמן מנורמל:** זמנים לפני 08:00 צריכים +1440 דקות לחישוב שבת

2. **Shift IDs:** קבועים - לא לשנות בלי לבדוק את כל הקוד

## הרצת הפרויקט

```bash
# פיתוח
uvicorn app:app --reload --port 8000

# בדיקות
pytest tests/ -v
```

## סטנדרטים - Clean Code

### שמות (Naming)
```python
# קבועים - UPPER_SNAKE_CASE
FRIDAY_SHIFT_ID = 105

# משתנים ופונקציות - snake_case
actual_work_days = 21
def calculate_monthly_totals():

# פונקציות פרטיות - prefix _
def _build_segment_dict():
```

### Type Hints (חובה)
```python
def get_rate(segment_id: int, is_married: bool) -> float:
def process(data: dict | None = None) -> Dict[str, Any]:
```

### Docstrings (בעברית)
```python
def calculate_wages(segments: List[Tuple]) -> Dict:
    """
    חישוב שכר לרצף עבודה.

    Args:
        segments: רשימת סגמנטים (start, end, shift_id)

    Returns:
        Dict עם calc100, calc125, calc150...
    """
```

### ארגון קוד
| תיקייה | תוכן |
|--------|------|
| `core/` | לוגיקה עסקית בלבד |
| `routes/` | endpoints בלבד (קוראים ל-core) |
| `services/` | שירותים חיצוניים (email, export) |
| `utils/` | עזרים כלליים |

### כללים
- פונקציות עד 50 שורות (אם ארוך - לפצל)
- להשתמש ב-exceptions מ-`utils/error_handler.py`
- לא להשאיר debug prints
- לא לחזור על קוד (DRY)
- קבועים ב-`constants.py` בלבד

## תיעוד נוסף
- [docs/LOGIC.md](docs/LOGIC.md) - תיעוד מפורט של הלוגיקה
- [PROJECT_DOCUMENTATION.md](PROJECT_DOCUMENTATION.md) - תיעוד כללי
