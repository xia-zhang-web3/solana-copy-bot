//! Durable delivery hints; source authority is always rechecked by staging.
use crate::{source_sell_event, source_sell_handoff_rows as rows, SqliteDiscoveryStore};
use anyhow::{ensure, Result};
use copybot_core_types::SwapEvent;
use rusqlite::Connection;

#[derive(Clone, Debug)]
pub struct SourceSellCandidate {
    event: SwapEvent,
    position_id: String,
}
impl SourceSellCandidate {
    pub fn new(event: &SwapEvent, position_id: &str) -> Self {
        Self {
            event: event.clone(),
            position_id: position_id.into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SourceSellHandoff {
    pub sequence: i64,
    pub event: SwapEvent,
    pub original_position_id: Option<String>,
    pub disposition: String,
    pub reason: String,
}

impl SqliteDiscoveryStore {
    pub fn load_source_sell_handoff(&self, signature: &str) -> Result<Option<SourceSellHandoff>> {
        crate::source_sell_handoff_schema::required(&self.conn)?;
        rows::load(&self.conn, signature)
    }
}

// Called BEFORE the canonical INSERT, inside the same IMMEDIATE transaction.
// Triggers preserve Unknown for all no-capture core/legacy/main writers.
pub(crate) fn prepare_candidate(
    conn: &Connection,
    event: &SwapEvent,
    candidate: &SourceSellCandidate,
) -> Result<bool> {
    ensure!(
        !conn.is_autocommit(),
        "handoff requires observed transaction"
    );
    ensure!(
        source_sell_event::same(event, &candidate.event),
        "source SELL candidate event_identity_conflict"
    );
    ensure!(
        !candidate.position_id.trim().is_empty(),
        "source SELL candidate position missing"
    );
    if rows::load(conn, &event.signature)?.is_some() {
        return Ok(false);
    }
    let observed: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM observed_swaps WHERE signature=?1)",
        [&event.signature],
        |r| r.get(0),
    )?;
    // A duplicate without handoff is Unknown. The BEFORE INSERT trigger copies
    // its canonical identity; do not turn current position into historical proof.
    if observed {
        return Ok(false);
    }
    ensure!(
        event.token_out == "So11111111111111111111111111111111111111112"
            && event.token_in != event.token_out,
        "candidate is not a SELL"
    );
    rows::insert_known(conn, event, &candidate.position_id)?;
    Ok(true)
}
