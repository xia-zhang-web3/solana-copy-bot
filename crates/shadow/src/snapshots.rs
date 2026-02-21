use super::{ShadowService, ShadowSnapshot};
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_storage::SqliteStore;

impl ShadowService {
    pub fn snapshot_24h(&self, store: &SqliteStore, now: DateTime<Utc>) -> Result<ShadowSnapshot> {
        let since = now - Duration::hours(24);
        let (closed_trades_24h, realized_pnl_sol_24h) = store.shadow_realized_pnl_since(since)?;
        let open_lots = store.shadow_open_lots_count()?;
        Ok(ShadowSnapshot {
            closed_trades_24h,
            realized_pnl_sol_24h,
            open_lots,
        })
    }
}
