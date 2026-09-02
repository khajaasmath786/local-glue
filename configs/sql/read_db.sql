WITH column_data AS (
    SELECT column_id,
    owner,
    table_name,
    column_name AS column_name,
    data_type,
    data_length,
    data_precision,
    data_scale,
    'N' AS is_primary_key
    FROM all_tab_columns
    WHERE owner = '<SCHEMA>'
        AND table_name = '<TABLE>'
    UNION ALL
    SELECT
    column_id,
    owner,
    table_name,
    'bi_' || LOWER(column_name) AS column_name,
    data_type,
    data_length,
    data_precision,
    data_scale,
    'Y' AS is_primary_key
    FROM all_tab_columns
    WHERE owner = '<SCHEMA>'
    AND table_name = '<TABLE>'
    AND column_name IN (
        SELECT column_name
        FROM all_tab_columns
        WHERE owner = '<SCHEMA>'
            AND table_name = '<TABLE>'
            AND column_name IN (
                SELECT column_name
                FROM all_constraints cons
                JOIN all_cons_columns cols
                ON cons.constraint_name = cols.constraint_name
                WHERE cons.constraint_type = 'P'
                AND cons.owner = '<SCHEMA>'
                AND cons.table_name = '<TABLE>'
            )
    )
    )
    SELECT
        column_name,
        data_type,
        data_length,
        data_precision,
        data_scale,
        column_id,owner,
        table_name
    FROM column_data
    ORDER BY
        CASE
            WHEN is_primary_key = 'Y' THEN 1
            ELSE 2
        END,column_id,
        column_name