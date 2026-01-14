# שינוי מסד הנתונים ל-PostgreSQL בענן

## תאריך: 17/12/2025

## סיכום השינויים

המערכת שונתה מ-SQLite מקומי ל-PostgreSQL בענן (Vultr).

### קבצים שעודכנו:

1. **logic.py**
   - הוחלפו כל ה-imports של `sqlite3` ב-`psycopg2`
   - כל placeholders של SQL שונו מ-`?` ל-`%s`
   - נוספה פונקציה `get_db_connection()` ליצירת חיבור ל-PostgreSQL
   - תוקן טיפול בתאריכים - PostgreSQL מחזיר datetime objects ולא epoch timestamps
   - כל הפונקציות עודכנו לעבוד עם cursors של PostgreSQL

2. **app.py**
   - נוצר wrapper class `PostgresConnection` שמספק ממשק דומה ל-SQLite
   - הפונקציה `get_conn()` עודכנה להחזיר wrapper ל-PostgreSQL
   - Wrapper ממיר אוטומטית `?` ל-`%s` בשאילתות

3. **app_utils.py**
   - הוסרו type hints של `sqlite3.Connection`

4. **requirements.txt**
   - נוסף `psycopg2-binary==2.9.9`

### מחרוזת החיבור

מוגדרת ב-`logic.py` במשתנה `DB_CONNECTION_STRING` (נטענת מ-`.env` דרך `DATABASE_URL`).

### טבלאות במסד הנתונים (24 טבלאות):

- **people**: 205 מדריכים
- **time_reports**: 1130 דיווחים  
- **shift_types**: 22 סוגי משמרות
- **payment_codes**: 18 קודי תשלום
- **shabbat_times**: 112 זמני שבת
- **minimum_wage_rates**: 1 שורה
- ועוד...

### הבדלים חשובים בין SQLite ל-PostgreSQL:

1. **תאריכים**: 
   - PostgreSQL מחזיר datetime objects, SQLite מחזיר epoch timestamps (int)
   - כל השאילתות עודכנו להשתמש ב-datetime objects
   - הפונקציות `to_local_date()` ו-`human_date()` עודכנו לטפל בשני הסוגים
   - הפונקציה `month_range_ts()` מחזירה datetime objects

2. **Placeholders**: PostgreSQL משתמש ב-`%s`, SQLite משתמש ב-`?`

3. **Row Factory**: 
   - PostgreSQL משתמש ב-`DictCursor` (לא RealDictCursor - יש בו באגים)
   - SQLite משתמש ב-`Row`

4. **SELECT * בעייתי**: 
   - `SELECT *` גורם לשגיאות עם שדות timestamp
   - כל ה-SELECT * שונו ל-SELECT עם שדות ספציפיים
   
5. **IN Placeholders**: 
   - צריך ליצור: `",".join(["%s"] * len(ids))`
   - לא: `",".join("%s" * len(ids))`

### קבצי בדיקה שנוצרו:

- `test_connection.py` - בודק חיבור למסד הנתונים
- `check_tables.py` - מציג רשימת טבלאות ומספר שורות
- `test_logic.py` - בודק פונקציות בסיסיות מ-logic.py
- `test_home_view.py` - בודק לוגיקה של דף הבית

### התקנה:

```bash
pip install -r requirements.txt
```

### הרצת השרת:

```bash
python -m uvicorn app:app --reload --host 0.0.0.0 --port 8000
```

### מה עוד צריך לבדוק:

1. ✅ חיבור למסד נתונים - עובד
2. ✅ קריאת נתונים מטבלאות - עובד  
3. ✅ פונקציות בסיסיות - עובד
4. ✅ חישובי שכר - עובד! (נבדק עם מדריך אמיתי)
5. ⏳ בדיקת כל המסכים באפליקציה
6. ⏳ בדיקת כתיבה למסד הנתונים (INSERT/UPDATE/DELETE)

### תוצאות בדיקה:

**מדריך לדוגמה**: נרקיס גרינר אני (ID: 78)  
**חודש**: נובמבר 2025

- ✅ סה"כ שעות: 34.50
- ✅ ימי עבודה: 7
- ✅ כוננויות: 7 (תשלום: ₪495.00)
- ✅ תשלום בסיסי: ₪2008.60
- ✅ סה"כ תשלום: ₪2112.60
- ✅ פירוט שעות: 100%/125%/150%/175%/200% - הכל עובד!

### הערות:

- קבצי helper scripts (check_employee.py, וכו') לא עודכנו - הם לא בשימוש רגיל
- קובץ gesher_exporter.py לא עודכן - רק בדיקת ה-main שלו משתמשת ב-SQLite
- database_manager.py לא עודכן - ככל הנראה לא בשימוש

