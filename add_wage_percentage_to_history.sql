-- הוספת עמודת wage_percentage לטבלת היסטוריה של סוגי משמרות
-- הרץ פקודה זו בדטאבייס PostgreSQL

-- הוספת העמודה אם היא לא קיימת
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'shift_types_history'
        AND column_name = 'wage_percentage'
    ) THEN
        ALTER TABLE shift_types_history ADD COLUMN wage_percentage INTEGER DEFAULT 100;
    END IF;
END $$;

-- תאשר שהעמודה נוספה
SELECT 'wage_percentage column added/verified successfully' as status;
