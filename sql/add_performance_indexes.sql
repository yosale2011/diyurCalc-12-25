-- Performance indexes for summary page optimization
-- Run this script to add indexes that speed up monthly summary calculations

-- Index for time_reports lookups by person and date range
CREATE INDEX IF NOT EXISTS idx_time_reports_person_date
    ON time_reports(person_id, date);

-- Index for person status history lookups
CREATE INDEX IF NOT EXISTS idx_person_status_history_lookup
    ON person_status_history(person_id, year, month);

-- Index for apartment status history lookups
CREATE INDEX IF NOT EXISTS idx_apartment_status_history_lookup
    ON apartment_status_history(apartment_id, year, month);

-- Index for shift types history lookups
CREATE INDEX IF NOT EXISTS idx_shift_types_history_lookup
    ON shift_types_history(shift_type_id, year, month);
