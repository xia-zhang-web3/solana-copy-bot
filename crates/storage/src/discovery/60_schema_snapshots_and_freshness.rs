use crate::SqliteStore;
use anyhow::{Context, Result};

impl SqliteStore {
    pub(crate) fn ensure_trusted_wallet_metrics_snapshots_table(&self) -> Result<()> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS trusted_wallet_metrics_snapshots (
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
                ON trusted_wallet_metrics_snapshots(created_at DESC);",
            )
            .context("failed to ensure trusted wallet_metrics snapshots table exists")?;
        Ok(())
    }

    pub(crate) fn ensure_discovery_wallet_freshness_history_table(&self) -> Result<()> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS discovery_wallet_freshness_history (
                    capture_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    captured_at TEXT NOT NULL,
                    recent_cycles INTEGER NOT NULL,
                    verdict TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    publication_age_seconds INTEGER,
                    raw_truth_sufficient INTEGER NOT NULL,
                    raw_truth_reason TEXT NOT NULL,
                    shadow_signal_verdict TEXT NOT NULL,
                    shadow_signal_reason TEXT NOT NULL,
                    published_wallet_ids_json TEXT NOT NULL,
                    active_follow_wallet_ids_json TEXT NOT NULL,
                    current_raw_top_wallet_ids_json TEXT NOT NULL,
                    audit_json TEXT NOT NULL,
                    shadow_signal_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS
                    idx_discovery_wallet_freshness_history_captured_at
                ON discovery_wallet_freshness_history(captured_at DESC, capture_id DESC);",
            )
            .context("failed to ensure discovery_wallet_freshness_history table exists")?;
        Ok(())
    }
}
