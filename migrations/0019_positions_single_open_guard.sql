WITH duplicate_open_positions AS (
    SELECT token,
           MIN(rowid) AS keep_rowid,
           MIN(opened_ts) AS keep_opened_ts,
           SUM(qty) AS merged_qty,
           SUM(cost_sol) AS merged_cost_sol,
           COALESCE(SUM(pnl_sol), 0.0) AS merged_pnl_sol
    FROM positions
    WHERE state = 'open'
    GROUP BY token
    HAVING COUNT(*) > 1
)
UPDATE positions
SET qty = (
        SELECT merged_qty
        FROM duplicate_open_positions
        WHERE duplicate_open_positions.token = positions.token
    ),
    cost_sol = (
        SELECT merged_cost_sol
        FROM duplicate_open_positions
        WHERE duplicate_open_positions.token = positions.token
    ),
    pnl_sol = (
        SELECT merged_pnl_sol
        FROM duplicate_open_positions
        WHERE duplicate_open_positions.token = positions.token
    ),
    opened_ts = (
        SELECT keep_opened_ts
        FROM duplicate_open_positions
        WHERE duplicate_open_positions.token = positions.token
    ),
    closed_ts = NULL,
    state = 'open'
WHERE rowid IN (
    SELECT keep_rowid
    FROM duplicate_open_positions
);

WITH duplicate_open_positions AS (
    SELECT token,
           MIN(rowid) AS keep_rowid
    FROM positions
    WHERE state = 'open'
    GROUP BY token
    HAVING COUNT(*) > 1
)
DELETE FROM positions
WHERE state = 'open'
  AND token IN (
        SELECT token
        FROM duplicate_open_positions
  )
  AND rowid NOT IN (
        SELECT keep_rowid
        FROM duplicate_open_positions
  );

CREATE UNIQUE INDEX IF NOT EXISTS idx_positions_one_open_token
ON positions(token)
WHERE state = 'open';
