#![allow(dead_code)]
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::{Lamports, SignedLamports};
use copybot_storage_core::*;
use rusqlite::{params, Connection};
use std::path::{Path, PathBuf};
use tempfile::{tempdir, TempDir};

pub const ORDER: &str = "exec-canary:settlement";

pub struct Db {
    pub _dir: TempDir,
    pub path: PathBuf,
    pub store: SqliteStore,
    pub now: DateTime<Utc>,
}

impl Db {
    pub fn new(old_raw: u64, cost: i64, accumulated: i64, sold: u64, cash: i128) -> Result<Self> {
        let dir = tempdir()?;
        let path = dir.path().join("settlement.db");
        let mut store = SqliteStore::open(&path)?;
        store.run_migrations(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?;
        let db = Self {
            _dir: dir,
            path,
            store,
            now: Utc::now(),
        };
        let conn = db.conn()?;
        conn.execute(
            "INSERT INTO copy_signals(signal_id,wallet_id,token,side,notional_sol,ts,status)
            VALUES('sell-signal','leader','mint','sell',999,?1,'shadow_recorded')",
            [db.now.to_rfc3339()],
        )?;
        conn.execute(
            "INSERT INTO orders(order_id,signal_id,route,submit_ts,status,tx_signature,
            client_order_id,simulation_status,attempt,err_code)
            VALUES(?1,'sell-signal','tiny',?2,?3,'signature','client','passed',1,?4)",
            params![
                ORDER,
                db.now.to_rfc3339(),
                EXECUTION_STATUS_CANARY_SUBMITTED,
                EXECUTION_ACCOUNTING_PENDING_REASON
            ],
        )?;
        db.store.mark_execution_canary_confirmed_unreconciled(
            ORDER,
            &ExecutionCanaryReceiptProof {
                tx_signature: "signature".into(),
                wallet_pubkey: "wallet".into(),
                token: "mint".into(),
                side: "sell".into(),
                confirmation_status: "confirmed".into(),
                slot: Some(42),
                confirmed_at: db.now,
                reason: "awaiting_settlement".into(),
            },
            db.now,
        )?;
        conn.execute(
            "INSERT INTO positions(position_id,token,qty,cost_sol,opened_ts,state,
            accounting_bucket,qty_raw,qty_decimals,cost_lamports,pnl_lamports)
            VALUES('owned','mint',999,999,?1,'open',?2,?3,3,?4,?5)",
            params![
                db.now.to_rfc3339(),
                EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
                old_raw.to_string(),
                cost,
                accumulated
            ],
        )?;
        db.store
            .record_execution_canary_receipt_facts(&db.facts(sold, cash), db.now)?;
        Ok(db)
    }
    pub fn facts(&self, sold: u64, cash: i128) -> ExecutionCanaryReceiptFacts {
        let magnitude = u64::try_from(cash.unsigned_abs()).unwrap();
        ExecutionCanaryReceiptFacts {
            order_id: ORDER.into(),
            tx_signature: "signature".into(),
            wallet_pubkey: "wallet".into(),
            token: "mint".into(),
            side: "sell".into(),
            slot: 42,
            wallet_native_pre: Lamports::new(if cash < 0 { magnitude } else { 0 }),
            wallet_native_post: Lamports::new(if cash < 0 { 0 } else { magnitude }),
            wallet_native_delta: SignedLamports::new(cash),
            transaction_fee: None,
            fee_coverage: ReceiptFeeCoverage::Missing,
            fee_payer: None,
            token_delta: Some(ReceiptTokenDelta {
                raw: -i128::from(sold),
                decimals: 3,
            }),
            token_coverage: ReceiptTokenCoverage::PairedBalances,
            token_coverage_reason: None,
            wsol_coverage: ReceiptWsolCoverage::Unresolved,
            block_time: Some(self.now.timestamp()),
            decomposition: ReceiptDecomposition::Unresolved,
        }
    }
    pub fn conn(&self) -> Result<Connection> {
        Ok(Connection::open(&self.path)?)
    }
    pub fn ready(&self) -> Result<ExecutionCanarySellSettlementPlan> {
        ready(&self.store)
    }
    pub fn unsupported(&self, reason: SellSettlementUnsupported) -> Result<()> {
        let before = snapshot(&self.conn()?)?;
        assert_eq!(
            self.store.plan_execution_canary_sell_settlement(ORDER)?,
            ExecutionCanarySellSettlement::Unsupported(reason)
        );
        assert_eq!(snapshot(&self.conn()?)?, before);
        Ok(())
    }
}

pub fn ready(store: &SqliteStore) -> Result<ExecutionCanarySellSettlementPlan> {
    match store.plan_execution_canary_sell_settlement(ORDER)? {
        ExecutionCanarySellSettlement::Ready(plan) => Ok(plan),
        other => panic!("expected exact settlement plan, got {other:?}"),
    }
}

/// Full values from every table, including facts, risk, fills, cursors and schema.
/// Sorting complete rows also handles tables without a stable first-column ordering.
pub fn snapshot(conn: &Connection) -> Result<Vec<(String, Vec<String>)>> {
    let tables = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")?
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut all = Vec::new();
    for table in tables {
        let mut stmt =
            conn.prepare(&format!("SELECT * FROM \"{}\"", table.replace('"', "\"\"")))?;
        let count = stmt.column_count();
        let mut rows = stmt
            .query_map([], |r| {
                (0..count)
                    .map(|c| r.get::<_, rusqlite::types::Value>(c))
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map(|r| format!("{r:?}"))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows.sort();
        all.push((table, rows));
    }
    Ok(all)
}
