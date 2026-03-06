UPDATE followlist
SET active = 0,
    removed_at = COALESCE(removed_at, added_at),
    reason = COALESCE(reason, 'migration_dedup_active_followlist')
WHERE id IN (
    SELECT duplicate.id
    FROM followlist AS duplicate
    JOIN (
        SELECT wallet_id, MAX(id) AS keep_id
        FROM followlist
        WHERE active = 1
        GROUP BY wallet_id
        HAVING COUNT(*) > 1
    ) AS dedup
      ON dedup.wallet_id = duplicate.wallet_id
    WHERE duplicate.active = 1
      AND duplicate.id <> dedup.keep_id
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_followlist_one_active_wallet
ON followlist(wallet_id)
WHERE active = 1;
