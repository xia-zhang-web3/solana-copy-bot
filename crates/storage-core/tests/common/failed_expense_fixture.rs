#![allow(dead_code)]
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage_core::*;
use rusqlite::{params, Connection};
use serde_json::json;
use std::path::{Path, PathBuf};
use tempfile::{tempdir, TempDir};

pub const ORDER: &str = "exec-canary:failed-a";
pub const WALLET: &str = "failed-wallet";
pub const ROUTE: &str = "failed-route";
pub struct Db {
    pub dir: TempDir,
    pub path: PathBuf,
    pub store: SqliteStore,
    pub now: DateTime<Utc>,
}
impl Db {
    pub fn new() -> Result<Self> {
        let dir = tempdir()?;
        let path = dir.path().join("failed.db");
        let mut store = SqliteStore::open(&path)?;
        store.run_migrations(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?;
        let db = Self {
            dir,
            path,
            store,
            now: Utc::now() - chrono::Duration::seconds(2),
        };
        db.add(ORDER, "failed-signature", "sell", db.now)?;
        Ok(db)
    }
    pub fn conn(&self) -> Result<Connection> {
        Ok(Connection::open(&self.path)?)
    }
    pub fn add(&self, id: &str, sig: &str, side: &str, at: DateTime<Utc>) -> Result<()> {
        let c = self.conn()?;
        c.execute("INSERT INTO copy_signals(signal_id,wallet_id,side,token,notional_sol,ts,status) VALUES(?1,'leader',?2,'mint',1,?3,'shadow_recorded')",params![id,side,at.to_rfc3339()])?;
        c.execute("INSERT INTO orders(order_id,signal_id,route,submit_ts,status,client_order_id,tx_signature,attempt) VALUES(?1,?1,?2,?3,?4,?1,?5,1)",params![id,ROUTE,at.to_rfc3339(),EXECUTION_STATUS_CANARY_SUBMITTED,sig])?;
        Ok(())
    }
    pub fn detect(&self, id: &str, source: &str) -> Result<FailedExpenseTask> {
        self.store.detect_failed_expense(
            id,
            WALLET,
            source,
            "confirmed",
            Some(42),
            &json!({"InstructionError":[0,{"Custom":7}]}),
            self.now,
        )
    }
    pub fn facts(&self, id: &str, fee: u64) -> Result<FailedTransactionFacts> {
        let t = self.store.load_failed_expense_task(id)?.unwrap();
        Ok(FailedTransactionFacts {
            tx_signature: t.tx_signature,
            wallet: WALLET.into(),
            slot: 42,
            commitment: "confirmed".into(),
            transaction_error: json!({"InstructionError":[0,{"Custom":7}]}),
            transaction_fee_lamports: Some(fee.to_string()),
            fee_coverage: FailedExpenseCoverage::Known,
            payer: Some(WALLET.into()),
            payer_coverage: FailedExpenseCoverage::Known,
            wallet_native_pre_lamports: Some(fee.to_string()),
            wallet_native_post_lamports: Some("0".into()),
            native_coverage: FailedExpenseCoverage::Known,
        })
    }
    pub fn reopen(&mut self) -> Result<()> {
        self.store = SqliteStore::open(&self.path)?;
        Ok(())
    }
    pub fn count(&self, table: &str) -> Result<u64> {
        Ok(self
            .conn()?
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))?)
    }
    pub fn report(&self, limit: u32) -> Result<FailedExpenseReport> {
        self.store.execution_failed_expense_report(
            self.now - chrono::Duration::seconds(1),
            self.now + chrono::Duration::seconds(1),
            limit,
        )
    }
}
