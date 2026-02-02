-- מחיקת טבלת היסטוריה ישנה של סוגי משמרות
-- הטבלה הוחלפה ע"י shift_type_housing_rates_history
-- הרץ פקודה זו בדטאבייס PostgreSQL

-- מחיקת האינדקס אם קיים
DROP INDEX IF EXISTS idx_shift_types_history_lookup;

-- מחיקת הטבלה
DROP TABLE IF EXISTS shift_types_history;

SELECT 'shift_types_history table dropped successfully' as status;
