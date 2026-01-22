# clean-code

שמירה על קוד נקי - בדיקה ומחיקת קוד מיותר בכל שינוי.

## Instructions

### הפעלה אוטומטית
**חשוב:** הסקיל הזה פעיל תמיד. בכל פעם שאתה כותב או משנה קוד:

### 1. בדיקה לפני כתיבה

לפני שאתה כותב קוד חדש, שאל את עצמך:
- האם יש כבר פונקציה דומה בפרויקט?
- האם אני יכול להשתמש בקוד קיים במקום ליצור חדש?
- האם הקוד הזה באמת נחוץ?

### 2. Code Review אוטומטי

בכל שינוי, בדוק:

#### קוד מת (Dead Code)
- פונקציות שלא נקראות
- משתנים שלא בשימוש
- imports שלא בשימוש
- קוד מוסתר בתגובות (commented out)
- קבצים שלא בשימוש

#### כפילויות (DRY)
- פונקציות דומות שאפשר לאחד
- לוגיקה חוזרת שאפשר לחלץ לפונקציה
- קבועים שמופיעים יותר מפעם אחת

#### קריאות (Readability)
- שמות משמעותיים לפונקציות ומשתנים
- פונקציות קצרות (עד 50 שורות)
- תיעוד לפונקציות מורכבות

### 3. מחיקת קוד מיותר

**מחק בלי פחד:**
- קוד שלא בשימוש - מחק אותו (git שומר היסטוריה)
- תגובות ישנות - מחק אותן
- TODO ישנים - בצע או מחק
- debug prints - מחק אותם
- קבצי בדיקה זמניים - מחק אותם

**אל תשאיר:**
```python
# def old_function():  # קוד ישן בתגובה - למחוק!
#     pass

# TODO: לטפל בזה מתישהו  # אם לא רלוונטי - למחוק!

print("DEBUG:", value)  # debug print - למחוק!
```

### 4. סטנדרטים לפרויקט

#### שמות (Naming)
```python
# קבועים - UPPER_SNAKE_CASE
FRIDAY_SHIFT_ID = 105

# משתנים ופונקציות - snake_case
actual_work_days = 21
def calculate_monthly_totals():

# פונקציות פרטיות - prefix _
def _build_segment_dict():
```

#### Type Hints (חובה)
```python
def get_rate(segment_id: int, is_married: bool) -> float:
def process(data: dict | None = None) -> Dict[str, Any]:
```

#### Docstrings (בעברית, לפונקציות ציבוריות)
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

### 5. בדיקות לאחר שינוי

אחרי כל שינוי משמעותי:

```bash
# בדוק imports לא בשימוש
cd f:/DiyurClock/104/diyur003 && python -c "import py_compile; py_compile.compile('FILENAME')"

# הרץ בדיקות
python -m pytest tests/test_logic.py tests/test_salary_calculation.py -v
```

### 6. רשימת בדיקה (Checklist)

לפני שאתה מסיים עבודה על קובץ:

- [ ] אין imports לא בשימוש
- [ ] אין משתנים לא בשימוש
- [ ] אין פונקציות לא בשימוש
- [ ] אין קוד בתגובות
- [ ] אין debug prints
- [ ] שמות ברורים ומשמעותיים
- [ ] פונקציות קצרות (עד 50 שורות)
- [ ] Type hints לכל הפונקציות
- [ ] הבדיקות עוברות

### 7. כלים לבדיקה

אם צריך לבדוק קוד מת בפרויקט:

```bash
# חפש imports לא בשימוש
grep -r "^import\|^from" --include="*.py" | head -20

# חפש פונקציות שלא נקראות
grep -r "def " --include="*.py" | head -20
```

### 8. אין תאימות לאחור (No Backwards Compatibility)

**חשוב מאוד:** אלא אם המשתמש מבקש במפורש, **אין צורך בתאימות לאחור**.

**מה זה אומר:**
- אם משנים שם לפונקציה - פשוט לשנות, בלי להשאיר alias ישן
- אם מוחקים פונקציה - פשוט למחוק, בלי deprecated wrapper
- אם משנים חתימה של פונקציה - לעדכן את כל הקריאות
- אם משנים מבנה נתונים - לעדכן את כל השימושים

**לא לעשות:**
```python
# לא! אין צורך ב-alias לשם ישן
def calculate_wages():  # שם חדש
    pass
get_wages = calculate_wages  # מיותר! למחוק

# לא! אין צורך ב-deprecated wrapper
def old_function():
    """Deprecated: use new_function instead"""
    return new_function()  # מיותר! למחוק

# לא! אין צורך ב-re-export
from new_module import func  # re-export לתאימות - מיותר!
```

**כן לעשות:**
```python
# פשוט לשנות את השם ולעדכן את כל הקריאות
def calculate_wages():  # שם חדש
    pass

# ולעדכן בכל מקום שקורא לפונקציה
result = calculate_wages()  # במקום get_wages()
```

**למה?**
- הפרויקט הזה בשליטה מלאה שלנו
- אין צרכנים חיצוניים שתלויים ב-API
- git שומר היסטוריה אם צריך לחזור
- קוד פשוט יותר קל לתחזוקה

### תזכורת חשובה

**קוד נקי = קוד טוב**

- מחק קוד שלא בשימוש - git זוכר הכל
- אל תשאיר "ליתר ביטחון" - זה רק מבלבל
- פשוט יותר טוב ממורכב
- קצר יותר טוב מארוך (אם ברור)
- **אין תאימות לאחור** - אלא אם מבקשים במפורש
