-- יצירת טבלת היסטוריה למקטעי משמרות
-- הרץ פקודה זו בדטאבייס PostgreSQL

CREATE TABLE IF NOT EXISTS shift_time_segments_history (
    id SERIAL PRIMARY KEY,
    segment_id INTEGER NOT NULL,
    shift_type_id INTEGER NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    wage_percent INTEGER,
    segment_type TEXT,
    start_time TEXT,
    end_time TEXT,
    order_index INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    created_by INTEGER,
    UNIQUE(segment_id, year, month)
);

-- הוסף אינדקסים לביצועים טובים
CREATE INDEX IF NOT EXISTS idx_segments_history_shift_year_month
    ON shift_time_segments_history(shift_type_id, year, month);

CREATE INDEX IF NOT EXISTS idx_segments_history_year_month
    ON shift_time_segments_history(year, month);

-- תאשר שהטבלה נוצרה
SELECT 'shift_time_segments_history table created successfully' as status;
