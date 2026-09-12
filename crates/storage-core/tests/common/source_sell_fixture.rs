#![allow(dead_code)]
#[path = "buy_attribution_review_fixture.rs"]
mod buy;
use anyhow::Result;
pub use buy::Db;
use chrono::Duration;
use copybot_core_types::{ExactSwapAmounts, SwapEvent};
use copybot_storage_core::{
    ExecutionSourceSellIntent, ExecutionSourceSellOutcome as Outcome,
    ExecutionSourceSellReject as Reject,
};
use rusqlite::Connection;
use std::{collections::BTreeMap, path::Path};

pub const TABLE: &str = "execution_source_sell_intents";
pub const MIGRATIONS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations");

impl Db {
    pub fn position(&self) -> Result<String> {
        Ok(self
            .store
            .load_execution_canary_open_position("mint")?
            .unwrap()
            .position_id)
    }
    pub fn proven(&self, id: &str, source: &str) -> Result<String> {
        let order = self.seed(id, source, "buy")?;
        self.buy(&order)?;
        Ok(order)
    }
    pub fn sell(&self, signature: &str, wallet: &str) -> SwapEvent {
        SwapEvent {
            wallet: wallet.into(),
            dex: "pumpswap".into(),
            token_in: "mint".into(),
            token_out: "So11111111111111111111111111111111111111112".into(),
            amount_in: 4.0,
            amount_out: 0.1,
            signature: signature.into(),
            slot: 123,
            ts_utc: self.now + Duration::seconds(1),
            exact_amounts: Some(ExactSwapAmounts {
                amount_in_raw: "4000".into(),
                amount_in_decimals: 3,
                amount_out_raw: "100000000".into(),
                amount_out_decimals: 9,
            }),
        }
    }
    pub fn observed(&self, signature: &str, wallet: &str) -> Result<SwapEvent> {
        let event = self.sell(signature, wallet);
        assert!(self.store.insert_observed_swap(&event)?);
        Ok(event)
    }
    pub fn rejected(&self, event: &SwapEvent, position: &str, reason: Reject) -> Result<()> {
        let before = snapshot(&self.conn()?, &[])?;
        let outcome = self
            .store
            .stage_execution_source_sell_intent(event, position)?;
        assert!(
            matches!(outcome, Outcome::Rejected(r) if r == reason),
            "{outcome:?}"
        );
        assert_eq!(snapshot(&self.conn()?, &[])?, before);
        Ok(())
    }
}

pub fn inserted(outcome: Outcome) -> ExecutionSourceSellIntent {
    let Outcome::Inserted(row) = outcome else {
        panic!("expected Inserted: {outcome:?}")
    };
    row
}
pub fn existing(outcome: Outcome) -> ExecutionSourceSellIntent {
    let Outcome::Existing(row) = outcome else {
        panic!("expected Existing: {outcome:?}")
    };
    row
}
// All tables, including observed, shadow, quotes, cursors and money/proof/status rows.
// Compare values rather than DB bytes (WAL/checkpoint metadata is not business state).
pub fn snapshot(conn: &Connection, exclude: &[&str]) -> Result<BTreeMap<String, Vec<String>>> {
    let tables = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")?
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut result = BTreeMap::new();
    for table in tables {
        if exclude.contains(&table.as_str()) {
            continue;
        }
        let mut stmt =
            conn.prepare(&format!("SELECT * FROM \"{}\"", table.replace('"', "\"\"")))?;
        let n = stmt.column_count();
        let mut rows = stmt
            .query_map([], |r| {
                let row = (0..n)
                    .map(|i| r.get::<_, rusqlite::types::Value>(i))
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(format!("{row:?}"))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows.sort();
        result.insert(table, rows);
    }
    Ok(result)
}
pub fn copy_migrations_before(destination: &Path, boundary: &str) -> Result<()> {
    std::fs::create_dir(destination)?;
    for entry in std::fs::read_dir(MIGRATIONS)? {
        let entry = entry?;
        let name = entry.file_name();
        if name.to_string_lossy().ends_with(".sql") && name.to_string_lossy().as_ref() < boundary {
            std::fs::copy(entry.path(), destination.join(name))?;
        }
    }
    Ok(())
}
