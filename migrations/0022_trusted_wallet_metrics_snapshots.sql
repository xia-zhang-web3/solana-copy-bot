CREATE TABLE IF NOT EXISTS trusted_wallet_metrics_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    source_snapshot_id TEXT,
    source_window_start TEXT,
    effective_window_start TEXT NOT NULL,
    created_at TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    trust_state TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS
    idx_trusted_wallet_metrics_snapshots_effective_window_start
ON trusted_wallet_metrics_snapshots(effective_window_start);

CREATE INDEX IF NOT EXISTS
    idx_trusted_wallet_metrics_snapshots_trust_state_effective_window_start
ON trusted_wallet_metrics_snapshots(trust_state, effective_window_start DESC);

CREATE INDEX IF NOT EXISTS
    idx_trusted_wallet_metrics_snapshots_created_at
ON trusted_wallet_metrics_snapshots(created_at DESC);
