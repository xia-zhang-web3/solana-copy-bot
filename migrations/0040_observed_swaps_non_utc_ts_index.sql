DROP INDEX IF EXISTS idx_observed_swaps_non_utc_ts;

CREATE INDEX IF NOT EXISTS idx_observed_swaps_non_utc_ts
    ON observed_swaps(ts)
    WHERE NOT (
        (
            (length(ts) = 25 AND substr(ts, 20, 6) = '+00:00')
            OR (
                length(ts) BETWEEN 27 AND 35
                AND substr(ts, 20, 1) = '.'
                AND substr(ts, -6) = '+00:00'
                AND substr(ts, 21, length(ts) - 26) GLOB '[0-9]*'
                AND substr(ts, 21, length(ts) - 26) NOT GLOB '*[^0-9]*'
            )
        )
        AND substr(ts, 5, 1) = '-'
        AND substr(ts, 8, 1) = '-'
        AND substr(ts, 11, 1) = 'T'
        AND substr(ts, 14, 1) = ':'
        AND substr(ts, 17, 1) = ':'
        AND substr(ts, 1, 4) GLOB '[0-9][0-9][0-9][0-9]'
        AND substr(ts, 6, 2) GLOB '[0-9][0-9]'
        AND CAST(substr(ts, 6, 2) AS INTEGER) BETWEEN 1 AND 12
        AND substr(ts, 9, 2) GLOB '[0-9][0-9]'
        AND CAST(substr(ts, 9, 2) AS INTEGER) BETWEEN 1 AND 31
        AND substr(ts, 12, 2) GLOB '[0-9][0-9]'
        AND CAST(substr(ts, 12, 2) AS INTEGER) BETWEEN 0 AND 23
        AND substr(ts, 15, 2) GLOB '[0-9][0-9]'
        AND CAST(substr(ts, 15, 2) AS INTEGER) BETWEEN 0 AND 59
        AND substr(ts, 18, 2) GLOB '[0-9][0-9]'
        AND CAST(substr(ts, 18, 2) AS INTEGER) BETWEEN 0 AND 59
        AND julianday(ts) IS NOT NULL
        AND strftime('%Y-%m-%dT%H:%M:%S', ts) = substr(ts, 1, 19)
    );
