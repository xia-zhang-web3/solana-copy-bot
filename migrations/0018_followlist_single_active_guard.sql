WITH dedup AS (
    SELECT keep.wallet_id,
           keep.id AS keep_id,
           keep.added_at AS keep_added_at
    FROM followlist AS keep
    JOIN (
        SELECT wallet_id, MAX(id) AS keep_id
        FROM followlist
        WHERE active = 1
        GROUP BY wallet_id
        HAVING COUNT(*) > 1
    ) AS latest
      ON latest.keep_id = keep.id
)
UPDATE followlist
SET active = 0,
    removed_at = CASE
        WHEN removed_at IS NULL
             OR removed_at > (
                SELECT keep_added_at
                FROM dedup
                WHERE dedup.wallet_id = followlist.wallet_id
             )
        THEN (
            SELECT keep_added_at
            FROM dedup
            WHERE dedup.wallet_id = followlist.wallet_id
        )
        ELSE removed_at
    END,
    reason = COALESCE(reason, 'migration_dedup_active_followlist')
WHERE active = 1
  AND wallet_id IN (SELECT wallet_id FROM dedup)
  AND id <> (
        SELECT keep_id
        FROM dedup
        WHERE dedup.wallet_id = followlist.wallet_id
  );

CREATE UNIQUE INDEX IF NOT EXISTS idx_followlist_one_active_wallet
ON followlist(wallet_id)
WHERE active = 1;
