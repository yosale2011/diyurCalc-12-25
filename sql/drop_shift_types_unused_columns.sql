-- הסרת עמודות מיותרות מטבלת shift_types
-- העמודות האלו אינן בשימוש - התעריפים עברו לטבלה shift_type_housing_rates

-- רשימת העמודות להסרה:
-- rate, is_minimum_wage, wage_percentage (תיעוד קודם בגרסה 2.0.22)
-- single_rate, single_wage_percentage, married_rate, married_wage_percentage
-- pay_calculation_type, for_regular_apartment, for_substitute, for_all_guides, for_married, for_therapeutic_apartment

DO $$
DECLARE
    columns_to_drop TEXT[] := ARRAY[
        'rate',
        'is_minimum_wage',
        'wage_percentage',
        'single_rate',
        'single_wage_percentage',
        'married_rate',
        'married_wage_percentage',
        'pay_calculation_type',
        'for_regular_apartment',
        'for_substitute',
        'for_all_guides',
        'for_married',
        'for_therapeutic_apartment'
    ];
    col_name TEXT;
BEGIN
    FOREACH col_name IN ARRAY columns_to_drop
    LOOP
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'shift_types'
            AND column_name = col_name
        ) THEN
            EXECUTE format('ALTER TABLE shift_types DROP COLUMN %I', col_name);
            RAISE NOTICE 'Dropped column % from shift_types', col_name;
        ELSE
            RAISE NOTICE 'Column % does not exist in shift_types (already removed)', col_name;
        END IF;
    END LOOP;
END $$;

SELECT 'shift_types unused columns removal completed' as status;
