#![allow(dead_code)]
#[path = "source_write_off_fixture.rs"]
mod shared;
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::TokenQuantity;
use copybot_storage_core::*;
pub use copybot_storage_core::{
    ExecutionSourceSellWriteOffKind as Kind, ExecutionSourceSellWriteOffOutcome as Outcome,
};
use rusqlite::Connection;
pub use shared::*;

pub struct Db {
    pub dir: tempfile::TempDir,
    pub path: std::path::PathBuf,
    pub store: SqliteStore,
    pub now: DateTime<Utc>,
    pub staged: ExecutionSourceSellIntent,
    pub order: ExecutionCanaryOrder,
}
pub fn kinds() -> [Kind; 3] {
    [
        Kind::TerminalSimulation { max_attempts: 1 },
        Kind::TerminalNoRoute { max_attempts: 1 },
        Kind::DustNoRoute,
    ]
}
impl Db {
    pub fn new(kind: Kind, qty: TokenQuantity) -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("source-write-off.db");
        let mut store = SqliteStore::open(&path)?;
        store.run_migrations(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?;
        let now: DateTime<Utc> = "2026-09-07T12:00:00Z".parse()?;
        proven_buy(&store, "buy-a", "source-a", now, qty)?;
        let staged = staged(&store, "sell-a", now)?;
        let ExecutionSourceSellPromotionOutcome::Inserted(binding) =
            store.promote_execution_source_sell_intent(&staged.intent_id)?
        else {
            anyhow::bail!("fixture promotion")
        };
        let order = fail_order(
            &store,
            &binding.signal_id,
            "tiny",
            now,
            matches!(kind, Kind::TerminalSimulation { .. }),
            1,
        )?;
        Ok(Self {
            dir,
            path,
            store,
            now,
            staged,
            order,
        })
    }
    pub fn conn(&self) -> Result<Connection> {
        Ok(Connection::open(&self.path)?)
    }
    pub fn reopen(&mut self) -> Result<()> {
        self.store = SqliteStore::open(&self.path)?;
        Ok(())
    }
    pub fn run(&self, kind: Kind) -> Result<Outcome> {
        self.store.write_off_execution_source_sell(
            &self.order.order_id,
            kind,
            self.now + chrono::Duration::seconds(2),
        )
    }
}
