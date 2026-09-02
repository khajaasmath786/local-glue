WITH column_data AS (
    SELECT distinct
        atc.column_id,
        atc.owner,
        atc.table_name,
        atc.column_name AS column_name,
        atc.data_type,
        atc.data_length,
        atc.data_precision,
        atc.data_scale,
        CASE
            WHEN acc.constraint_name IS NOT NULL THEN 'Y'
            ELSE 'N'
        END AS is_primary_key
    FROM all_tab_columns atc
    LEFT JOIN all_cons_columns acc ON atc.column_name = acc.column_name
    LEFT JOIN all_constraints ac ON acc.constraint_name = ac.constraint_name
    WHERE atc.owner = '<SCHEMA>'
        AND atc.table_name = '<TABLE>'
)
SELECT
    column_name,
    data_type,
    data_length,
    is_primary_key,
    data_precision,
    data_scale,
    column_id,
    owner,
    table_name
FROM column_data
ORDER BY
    CASE
        WHEN is_primary_key = 'Y' THEN 1
        ELSE 2
    END,
    column_id,
    column_name