-- הסרת עמודת wage_percent מטבלת הסגמנטים
-- העמודה לא בשימוש - אחוזי השכר מחושבים לפי שעות נוספות

-- הסרה מטבלת shift_time_segments
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'shift_time_segments'
        AND column_name = 'wage_percent'
    ) THEN
        ALTER TABLE shift_time_segments DROP COLUMN wage_percent;
        RAISE NOTICE 'Dropped wage_percent from shift_time_segments';
    ELSE
        RAISE NOTICE 'wage_percent column does not exist in shift_time_segments';
    END IF;
END $$;

SELECT 'wage_percent column removal completed' as status;
