# פרומפט להוספת מערכת היסטוריה במערכת הטפסים

## רקע
מערכת חישוב השכר (DiyurCalc) כבר עודכנה לתמוך בנתונים היסטוריים. הפרמטרים הבאים משתנים לאורך זמן ויש לשמור היסטוריה שלהם:

1. **מצב משפחתי של מדריך** (`is_married`) - משפיע על תעריף כוננות
2. **סוג דירה** (`apartment_type_id`) - משפיע על תעריף כוננות
3. **מעסיק** (`employer_id`) - משפיע על לאיזו חברה מייצאים את הדוח
4. **סוג עובד** (`employee_type`) - קבוע/מחליף
5. **תעריפי כוננות** (`standby_rates`) - תעריפים שמשתנים בין חודשים

## טבלאות שכבר נוצרו בבסיס הנתונים

```sql
-- היסטוריית סטטוס מדריך
CREATE TABLE person_status_history (
    id SERIAL PRIMARY KEY,
    person_id INTEGER NOT NULL REFERENCES people(id),
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    is_married BOOLEAN,
    employer_id INTEGER,
    employee_type TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    created_by INTEGER,
    UNIQUE(person_id, year, month)
);

-- היסטוריית סוג דירה
CREATE TABLE apartment_status_history (
    id SERIAL PRIMARY KEY,
    apartment_id INTEGER NOT NULL REFERENCES apartments(id),
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    apartment_type_id INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    created_by INTEGER,
    UNIQUE(apartment_id, year, month)
);

-- נעילת חודשים
CREATE TABLE month_locks (
    id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    locked_at TIMESTAMP DEFAULT NOW(),
    locked_by INTEGER,
    unlocked_at TIMESTAMP,
    unlocked_by INTEGER,
    notes TEXT,
    UNIQUE(year, month)
);

-- היסטוריית תעריפי כוננות
CREATE TABLE standby_rates_history (
    id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    original_rate_id INTEGER,
    segment_id INTEGER NOT NULL,
    apartment_type_id INTEGER,
    marital_status TEXT NOT NULL,
    amount INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    created_by INTEGER
);
```

## הגישה: "שמור בשינוי"

המערכת משתמשת בגישה של "שמור בשינוי" (Save on Change):
- **אין היסטוריה?** = משתמשים בערך הנוכחי מהטבלה הראשית
- **יש היסטוריה?** = משתמשים בערך ההיסטורי

כשמשנים פרמטר, יש לשמור את הערך הישן להיסטוריה **לפני** השינוי.

## משימות לביצוע

### 1. עדכון טופס עריכת מדריך

כאשר עורכים מדריך ומשנים אחד מהשדות הבאים:
- `is_married`
- `employer_id`
- `type` (employee_type)

**לפני השמירה**, יש לבדוק:
1. האם השדה באמת השתנה?
2. האם החודש הנוכחי נעול?
3. אם לא נעול - לשמור את הערך הישן לטבלת `person_status_history`

```javascript
// דוגמה לוגיקה
async function updatePerson(personId, newData) {
    const currentMonth = new Date().getMonth() + 1;
    const currentYear = new Date().getFullYear();

    // בדיקה אם החודש נעול
    const isLocked = await checkMonthLocked(currentYear, currentMonth);
    if (isLocked) {
        throw new Error('החודש נעול לעריכה');
    }

    // שליפת הנתונים הנוכחיים
    const currentPerson = await getPersonById(personId);

    // בדיקה אם יש שינוי ברלוונטי
    const hasRelevantChange =
        currentPerson.is_married !== newData.is_married ||
        currentPerson.employer_id !== newData.employer_id ||
        currentPerson.type !== newData.type;

    if (hasRelevantChange) {
        // שמירת הערכים הנוכחיים להיסטוריה
        await savePersonStatusToHistory(
            personId,
            currentYear,
            currentMonth,
            currentPerson.is_married,
            currentPerson.employer_id,
            currentPerson.type
        );
    }

    // עדכון הנתונים
    await updatePersonInDB(personId, newData);
}
```

### 2. עדכון טופס עריכת דירה

כאשר משנים `apartment_type_id` של דירה:

```javascript
async function updateApartment(apartmentId, newData) {
    const currentMonth = new Date().getMonth() + 1;
    const currentYear = new Date().getFullYear();

    // בדיקה אם החודש נעול
    const isLocked = await checkMonthLocked(currentYear, currentMonth);
    if (isLocked) {
        throw new Error('החודש נעול לעריכה');
    }

    const currentApartment = await getApartmentById(apartmentId);

    if (currentApartment.apartment_type_id !== newData.apartment_type_id) {
        await saveApartmentStatusToHistory(
            apartmentId,
            currentYear,
            currentMonth,
            currentApartment.apartment_type_id
        );
    }

    await updateApartmentInDB(apartmentId, newData);
}
```

### 3. עדכון טופס עריכת תעריפי כוננות

כאשר משנים תעריף כוננות:

```javascript
async function updateStandbyRates(newRates) {
    const currentMonth = new Date().getMonth() + 1;
    const currentYear = new Date().getFullYear();

    // בדיקה אם החודש נעול
    const isLocked = await checkMonthLocked(currentYear, currentMonth);
    if (isLocked) {
        throw new Error('החודש נעול לעריכה');
    }

    // לפני שינוי תעריפים - שמור את כל התעריפים הנוכחיים להיסטוריה
    await saveAllStandbyRatesToHistory(currentYear, currentMonth);

    // עדכון התעריפים
    await updateStandbyRatesInDB(newRates);
}
```

### 4. הוספת ממשק נעילת חודש

הוסף לממשק הניהול:
- כפתור "נעילת חודש" / "פתיחת חודש"
- אינדיקציה ויזואלית אם החודש נעול

```javascript
// API endpoints זמינים במערכת החישוב:
// GET /api/month-lock/{year}/{month} - בדיקת סטטוס נעילה
// POST /api/month-lock - נעילת חודש
// POST /api/month-unlock - פתיחת חודש
```

### 5. חסימת עריכה בחודש נעול

בכל טופס עריכה, יש לבדוק אם החודש נעול ולהציג הודעה מתאימה:

```javascript
async function checkMonthLocked(year, month) {
    const response = await fetch(`/api/month-lock/${year}/${month}`);
    const data = await response.json();
    return data.locked;
}
```

## API לשמירת היסטוריה

```javascript
// שמירת סטטוס מדריך להיסטוריה
async function savePersonStatusToHistory(personId, year, month, isMarried, employerId, employeeType) {
    await db.query(`
        INSERT INTO person_status_history
        (person_id, year, month, is_married, employer_id, employee_type)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (person_id, year, month)
        DO UPDATE SET
            is_married = EXCLUDED.is_married,
            employer_id = EXCLUDED.employer_id,
            employee_type = EXCLUDED.employee_type,
            created_at = NOW()
    `, [personId, year, month, isMarried, employerId, employeeType]);
}

// שמירת סטטוס דירה להיסטוריה
async function saveApartmentStatusToHistory(apartmentId, year, month, apartmentTypeId) {
    await db.query(`
        INSERT INTO apartment_status_history
        (apartment_id, year, month, apartment_type_id)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (apartment_id, year, month)
        DO UPDATE SET
            apartment_type_id = EXCLUDED.apartment_type_id,
            created_at = NOW()
    `, [apartmentId, year, month, apartmentTypeId]);
}

// שמירת כל תעריפי הכוננות להיסטוריה
async function saveAllStandbyRatesToHistory(year, month) {
    await db.query(`
        INSERT INTO standby_rates_history
        (year, month, original_rate_id, segment_id, apartment_type_id, marital_status, amount)
        SELECT $1, $2, id, segment_id, apartment_type_id, marital_status, amount
        FROM standby_rates
        ON CONFLICT DO NOTHING
    `, [year, month]);
}
```

## סיכום השינויים הנדרשים

1. **טופס עריכת מדריך** - הוסף לוגיקת שמירה להיסטוריה לפני עדכון
2. **טופס עריכת דירה** - הוסף לוגיקת שמירה להיסטוריה לפני עדכון
3. **טופס עריכת תעריפי כוננות** - הוסף לוגיקת שמירה להיסטוריה לפני עדכון
4. **ממשק נעילת חודש** - הוסף ממשק לנעילה/פתיחת חודש
5. **חסימת עריכה** - בדוק נעילה לפני כל עריכה והצג הודעה מתאימה

## הערות חשובות

- השמירה להיסטוריה צריכה להתבצע **לפני** השמירה לטבלה הראשית
- יש להשתמש ב-`ON CONFLICT DO UPDATE` כדי לא ליצור כפילויות
- בדיקת נעילת חודש צריכה להיות בצד השרת, לא רק בצד הלקוח
- התאריך הרלוונטי הוא **החודש הנוכחי** (מתי מבצעים את השינוי)
