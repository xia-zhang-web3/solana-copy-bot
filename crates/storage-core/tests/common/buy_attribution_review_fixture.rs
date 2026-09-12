#![allow(dead_code)]
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::{CopySignalRow, Lamports, SignedLamports, TokenQuantity};
use copybot_storage_core::*;
use rusqlite::Connection;
use std::path::{Path, PathBuf};
use tempfile::{tempdir, TempDir};

pub struct Db {
    pub dir: TempDir,
    pub path: PathBuf,
    pub store: SqliteStore,
    pub now: DateTime<Utc>,
}
impl Db {
    pub fn new() -> Result<Self> {
        let dir = tempdir()?;
        let path = dir.path().join("attribution.db");
        let mut store = SqliteStore::open(&path)?;
        store.run_migrations(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?;
        Ok(Self {
            dir,
            path,
            store,
            now: "2026-09-07T12:00:00Z".parse()?,
        })
    }
    pub fn conn(&self) -> Result<Connection> {
        Ok(Connection::open(&self.path)?)
    }
    pub fn reopen(&mut self) -> Result<()> {
        self.store = SqliteStore::open(&self.path)?;
        Ok(())
    }
    pub fn seed(&self, id: &str, wallet: &str, side: &str) -> Result<String> {
        let signature = if id.contains("dup-") {
            "shared-signature".to_owned()
        } else {
            format!("sig:exec-canary:{id}")
        };
        self.seed_claim(id, wallet, side, "mint", "execution-wallet", &signature)
    }
    pub fn seed_claim(
        &self,
        id: &str,
        wallet: &str,
        side: &str,
        token: &str,
        execution_wallet: &str,
        signature: &str,
    ) -> Result<String> {
        self.store.insert_copy_signal(&CopySignalRow {
            signal_id: id.into(),
            wallet_id: wallet.into(),
            token: token.into(),
            side: side.into(),
            notional_sol: 0.000001,
            notional_lamports: Some(Lamports::new(1000)),
            notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
            ts: self.now,
            status: "shadow_recorded".into(),
        })?;
        let id = self
            .store
            .reserve_execution_canary_order(id, "tiny", self.now)?
            .order
            .order_id;
        self.store.mark_execution_canary_built(&id, self.now)?;
        self.store.mark_execution_canary_simulated(
            &id,
            self.now,
            EXECUTION_SIMULATION_STATUS_PASSED,
            None,
        )?;
        self.store
            .mark_execution_canary_submitted(&id, self.now, &signature.to_owned())?;
        self.store.mark_execution_canary_confirmed_unreconciled(
            &id,
            &ExecutionCanaryReceiptProof {
                tx_signature: signature.to_owned(),
                wallet_pubkey: execution_wallet.into(),
                token: token.into(),
                side: side.into(),
                confirmation_status: "confirmed".into(),
                slot: Some(42),
                confirmed_at: self.now,
                reason: "receipt_not_fetched".into(),
            },
            self.now,
        )?;
        self.store.record_execution_canary_receipt_facts(
            &ExecutionCanaryReceiptFacts {
                order_id: id.clone(),
                tx_signature: signature.to_owned(),
                wallet_pubkey: execution_wallet.into(),
                token: token.into(),
                side: side.into(),
                slot: 42,
                wallet_native_pre: Lamports::new(2000),
                wallet_native_post: Lamports::new(if side == "buy" { 1000 } else { 2500 }),
                wallet_native_delta: SignedLamports::new(if side == "buy" { -1000 } else { 500 }),
                transaction_fee: Some(Lamports::new(50)),
                fee_coverage: ReceiptFeeCoverage::Known,
                fee_payer: Some(execution_wallet.into()),
                token_delta: Some(ReceiptTokenDelta {
                    raw: if side == "buy" { 7000 } else { -7000 },
                    decimals: 3,
                }),
                token_coverage: ReceiptTokenCoverage::PairedBalances,
                token_coverage_reason: None,
                wsol_coverage: ReceiptWsolCoverage::Unresolved,
                block_time: Some(self.now.timestamp()),
                decomposition: ReceiptDecomposition::Unresolved,
            },
            self.now,
        )?;
        Ok(id)
    }
    pub fn buy(&self, id: &str) -> Result<ExecutionCanaryOwnedPositionRecordResult> {
        self.buy_claim(id, "mint")
    }
    pub fn buy_claim(
        &self,
        id: &str,
        token: &str,
    ) -> Result<ExecutionCanaryOwnedPositionRecordResult> {
        Ok(self
            .store
            .confirm_execution_canary_buy_fill(
                id,
                token,
                7.0,
                Some(TokenQuantity::new(7000, 3)),
                0.000001,
                self.now,
                self.now,
                Some(Lamports::new(1000)),
            )?
            .1)
    }
    pub fn close(&self) -> Result<()> {
        self.store
            .record_execution_canary_manual_terminal_write_off(
                "mint",
                "tiny",
                "fixture_close",
                self.now,
            )?;
        Ok(())
    }
    pub fn links(&self) -> Result<Vec<(String, Option<String>)>> {
        Ok(self
            .conn()?
            .prepare("SELECT order_id, position_id FROM fills ORDER BY order_id")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?)
    }
    pub fn snapshot(&self) -> Result<Vec<Vec<String>>> {
        let conn = self.conn()?;
        let mut result = Vec::new();
        for table in [
            "copy_signals",
            "orders",
            "positions",
            "fills",
            "execution_canary_receipt_proofs",
            "execution_canary_receipt_facts",
        ] {
            let mut stmt = conn.prepare(&format!("SELECT * FROM {table} ORDER BY 1"))?;
            let n = stmt.column_count();
            result.extend(
                stmt.query_map([], |r| {
                    (0..n)
                        .map(|c| {
                            r.get::<_, rusqlite::types::Value>(c)
                                .map(|v| format!("{v:?}"))
                        })
                        .collect()
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?,
            );
        }
        Ok(result)
    }
}
