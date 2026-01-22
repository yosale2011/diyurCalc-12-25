# DiyurCalc - מדריך לקלוד

## סקירה כללית
מערכת חישוב משכורות למדריכים בעמותת צהר.

**טכנולוגיות:** FastAPI + PostgreSQL + Jinja2

## הנחיות חובה - קרא לפני כל שינוי!

**לפני כל שינוי בקוד, קרא את הסקילים הבאים:**
- [.claude/skills/clean-code.md](.claude/skills/clean-code.md) - קוד נקי, מחיקת קוד מיותר, אין תאימות לאחור
- [.claude/skills/changelog.md](.claude/skills/changelog.md) - תיעוד שינויים עם מספר גרסה
- [.claude/skills/test.md](.claude/skills/test.md) - הרצת בדיקות

## למידת הפרויקט

**להבנת מבנה הפרויקט והקוד:**
- [.claude/skills/learn-project.md](.claude/skills/learn-project.md) - קרא קבצים בפועל כדי להבין את המצב הנוכחי

**תיעוד נוסף:**
- [docs/LOGIC.md](docs/LOGIC.md) - תיעוד מפורט של הלוגיקה העסקית
- [PROJECT_DOCUMENTATION.md](PROJECT_DOCUMENTATION.md) - תיעוד כללי

## כללי קוד בסיסיים

### שמות (Naming)
- קבועים: `UPPER_SNAKE_CASE`
- משתנים ופונקציות: `snake_case`
- פונקציות פרטיות: `_prefix`

### חובה
- Type Hints לכל פונקציה
- Docstrings בעברית לפונקציות ציבוריות
- פונקציות עד 50 שורות
- קבועים ב-`core/constants.py` בלבד

### הרצת הפרויקט
```bash
# פיתוח
uvicorn app:app --reload --port 8000

# בדיקות
pytest tests/ -v
```
