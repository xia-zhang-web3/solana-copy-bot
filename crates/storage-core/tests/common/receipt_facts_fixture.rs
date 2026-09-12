use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::{CopySignalRow, Lamports, SignedLamports, TokenQuantity};
use copybot_storage_core::*;
use rusqlite::Connection;
use std::path::{Path, PathBuf};
use tempfile::{tempdir, TempDir};

pub fn migrations() -> &'static Path {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations"))
}

pub fn seed(store: &SqliteStore, slot: Option<u64>) -> Result<(String, DateTime<Utc>)> {
    let now = Utc::now();
    store.insert_copy_signal(&CopySignalRow {
        signal_id: "cash-facts".into(),
        wallet_id: "leader".into(),
        token: "mint".into(),
        side: "buy".into(),
        notional_sol: 1.0,
        notional_lamports: Some(Lamports::new(1_000_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
        ts: now,
        status: "shadow_recorded".into(),
    })?;
    let id = store
        .reserve_execution_canary_order("cash-facts", "tiny", now)?
        .order
        .order_id;
    store.mark_execution_canary_built(&id, now)?;
    store.mark_execution_canary_simulated(&id, now, EXECUTION_SIMULATION_STATUS_PASSED, None)?;
    store.mark_execution_canary_submitted(&id, now, "signature")?;
    store.mark_execution_canary_confirmed_unreconciled(
        &id,
        &ExecutionCanaryReceiptProof {
            tx_signature: "signature".into(),
            wallet_pubkey: "wallet".into(),
            token: "mint".into(),
            side: "buy".into(),
            confirmation_status: "confirmed".into(),
            slot,
            confirmed_at: now,
            reason: "receipt_not_fetched".into(),
        },
        now,
    )?;
    Ok((id, now))
}

pub struct Db {
    pub _dir: TempDir,
    pub path: PathBuf,
    pub store: SqliteStore,
    pub id: String,
    pub now: DateTime<Utc>,
}
impl Db {
    pub fn new(slot: Option<u64>) -> Result<Self> {
        let dir = tempdir()?;
        let path = dir.path().join("facts.db");
        let mut store = SqliteStore::open(&path)?;
        store.run_migrations(migrations())?;
        let (id, now) = seed(&store, slot)?;
        Ok(Self {
            _dir: dir,
            path,
            store,
            id,
            now,
        })
    }
    pub fn facts(&self) -> ExecutionCanaryReceiptFacts {
        ExecutionCanaryReceiptFacts {
            order_id: self.id.clone(),
            tx_signature: "signature".into(),
            wallet_pubkey: "wallet".into(),
            token: "mint".into(),
            side: "buy".into(),
            slot: 42,
            wallet_native_pre: Lamports::new(2000),
            wallet_native_post: Lamports::new(1000),
            wallet_native_delta: SignedLamports::new(-1000),
            transaction_fee: Some(Lamports::new(500)),
            fee_coverage: ReceiptFeeCoverage::Known,
            fee_payer: Some("wallet".into()),
            token_delta: Some(ReceiptTokenDelta {
                raw: 7000,
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
    pub fn reopen(&mut self) -> Result<()> {
        self.store = SqliteStore::open(&self.path)?;
        Ok(())
    }
    pub fn account(&self) -> Result<()> {
        self.store.confirm_execution_canary_buy_fill(
            &self.id,
            "mint",
            7.0,
            Some(TokenQuantity::new(7000, 3)),
            0.000001,
            self.now,
            self.now,
            Some(Lamports::new(1000)),
        )?;
        Ok(())
    }
}

pub fn snapshot(conn: &Connection) -> Result<Vec<Vec<String>>> {
    let mut rows = Vec::new();
    for table in [
        "copy_signals",
        "orders",
        "positions",
        "fills",
        "execution_canary_receipt_proofs",
        "execution_owned_sell_cursor",
    ] {
        // Compare the historical columns across additive/rebuild migrations. New
        // settlement evidence has its own complete snapshot and migration tests.
        let columns = if table == "fills" {
            "id,order_id,token,qty,avg_price,fee,slippage_bps,notional_lamports,fee_lamports,qty_raw,qty_decimals"
        } else {
            "*"
        };
        let mut stmt = conn.prepare(&format!("SELECT {columns} FROM {table} ORDER BY 1"))?;
        let columns = stmt.column_count();
        rows.extend(
            stmt.query_map([], |r| {
                (0..columns)
                    .map(|c| {
                        r.get::<_, rusqlite::types::Value>(c)
                            .map(|v| format!("{v:?}"))
                    })
                    .collect()
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?,
        );
    }
    Ok(rows)
}
