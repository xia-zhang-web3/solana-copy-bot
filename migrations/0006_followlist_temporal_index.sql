CREATE INDEX IF NOT EXISTS idx_followlist_wallet_added_removed
ON followlist(wallet_id, added_at, removed_at);
