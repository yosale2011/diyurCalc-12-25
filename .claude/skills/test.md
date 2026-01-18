# test

הרצת בדיקות לפרויקט DiyurCalc.

## Arguments

- ללא ארגומנט: בדיקות מהירות (unit tests)
- `full`: בדיקות מלאות כולל השוואה
- `compare`: בדיקות רגרסיה - וידוא שהחישוב עקבי
- `coverage`: דוח כיסוי קוד

## Instructions

### ללא ארגומנט או עם "quick" - בדיקות מהירות
הרץ את הבדיקות המהירות (unit tests):
```bash
cd f:/DiyurClock/104/diyur003 && python -m pytest tests/test_logic.py tests/test_salary_calculation.py -v
```

אם יש failures:
1. דווח על מספר הבדיקות שעברו/נכשלו
2. הצג את השגיאות
3. הצע תיקון אם רלוונטי

### עם ארגומנט "full" - בדיקות מלאות
הרץ את כל הבדיקות (דורש חיבור DB):
```bash
cd f:/DiyurClock/104/diyur003 && python -m pytest tests/ -v --ignore=tests/debug_payment_gap.py --ignore=tests/debug_2000_gap.py --ignore=tests/debug_dec_gap.py --ignore=tests/debug_dec_detailed.py --ignore=tests/debug_old_vs_new.py --ignore=tests/debug_payment_breakdown.py --ignore=tests/debug_extras.py
```

### עם ארגומנט "compare" - בדיקות רגרסיה
הרץ בדיקות השוואה לוידוא שהחישוב עקבי:
```bash
cd f:/DiyurClock/104/diyur003 && python tests/full_comparison_test.py
```

הבדיקה מוודאת שחישוב השכר עובד נכון לכל העובדים הפעילים.

אם יש failures:
1. דווח על העובדים שנכשלו
2. הצג את השדות עם ההבדלים
3. הצע לחקור את הסיבה

### עם ארגומנט "coverage" - כיסוי קוד
הרץ בדיקות עם דוח כיסוי:
```bash
cd f:/DiyurClock/104/diyur003 && python -m pytest tests/test_logic.py tests/test_salary_calculation.py --cov=core --cov=app_utils --cov-report=term-missing
```

דווח על:
1. אחוז הכיסוי הכללי
2. קבצים עם כיסוי נמוך
3. שורות שלא נבדקו

## סיכום תוצאות

בסוף כל הרצה, תן סיכום:
- כמה בדיקות עברו / נכשלו
- אם יש בעיות - מה הן
- המלצות לפעולה הבאה
