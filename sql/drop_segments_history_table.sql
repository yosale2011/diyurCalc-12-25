-- הסרת טבלת shift_time_segments_history
-- הטבלה לא הייתה בשימוש בפועל במערכת

DROP TABLE IF EXISTS shift_time_segments_history;

SELECT 'shift_time_segments_history table dropped successfully' as status;
