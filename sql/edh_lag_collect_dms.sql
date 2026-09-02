SELECT 
    _hoodie_commit_time,
        -- Convert the _hoodie_commit_time from the format yyyyMMddHHmmssSSS to a timestamp
    CASE
        WHEN length(_hoodie_commit_time) = 17 THEN 
            date_format(
                date_add(
                    'hour', 
                    -6, 
                    parse_datetime(substr(_hoodie_commit_time, 1, 14), 'yyyyMMddHHmmss')
                ),
                '%Y-%m-%d %H:%i:%s'
            )
        ELSE NULL
    END AS hoodie_commit_time_cst,
    commit_timestamp,
    -- Convert the commit timestamp to CST by subtracting 6 hours
    date_format(
        date_add('hour', -6, cast(commit_timestamp AS timestamp)),
        '%Y-%m-%d %H:%i:%s'
    ) AS commit_timestamp_cst,
    op,
    sddoco,
    sddcto,
    sdlitm,
    sdlnid,





    -- Calculate the lag (in seconds) between the current UTC time and the commit timestamp
    date_diff('second', cast(commit_timestamp AS timestamp), current_timestamp) AS lag_commit_timestamp_utc,

    -- Calculate the lag (in seconds) between the current UTC time and the _hoodie_commit_time
    CASE
        WHEN length(_hoodie_commit_time) = 17 THEN 
            date_diff(
                'second', 
                parse_datetime(substr(_hoodie_commit_time, 1, 14), 'yyyyMMddHHmmss'),
                current_timestamp
            )
        ELSE NULL
    END AS lag_hoodie_commit_time_utc

FROM F4211
WHERE 
    (sddoco IN (24177598, 24177597, 24177596, 24177599, 24672379) AND sddcto = 'SK')
    OR (sddoco IN (24658784) AND sddcto = 'ST')
ORDER BY cast(_hoodie_commit_time AS bigint) DESC
LIMIT 5;
