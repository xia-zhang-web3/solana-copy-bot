#![allow(dead_code)]
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::{CopySignalRow, Lamports, SignedLamports, TokenQuantity};
use copybot_storage_core::*;
use rusqlite::Connection;
use std::path::{Path, PathBuf};

pub struct Db {
    pub dir: tempfile::TempDir,
    pub path: PathBuf,
    pub store: SqliteStore,
    pub now: DateTime<Utc>,
}
pub type Candidate = (
    ExecutionCanaryOrder,
    CopySignalRow,
    ExecutionCanaryDispatch,
    TinyBudgetClaim,
);
impl Db {
    pub fn new() -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("budget.db");
        let mut store = SqliteStore::open(&path)?;
        store.run_migrations(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?;
        let now = "2026-09-12T23:50:00Z".parse()?;
        store.activate_tiny_experiment("one", "wallet", now)?;
        Ok(Self {
            dir,
            path,
            store,
            now,
        })
    }
    pub fn conn(&self) -> Result<Connection> {
        Ok(Connection::open(&self.path)?)
    }
    pub fn candidate(&self, id: &str, side: &str) -> Result<Candidate> {
        let signal = CopySignalRow {
            signal_id: id.into(),
            wallet_id: "leader".into(),
            side: side.into(),
            token: "mint".into(),
            notional_sol: 0.01,
            notional_lamports: Some(Lamports::new(10_000_000)),
            notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
            ts: self.now,
            status: "shadow_recorded".into(),
        };
        self.store.insert_copy_signal(&signal)?;
        let order = self
            .store
            .reserve_execution_canary_order(id, "tiny", self.now)?
            .order;
        self.store
            .mark_execution_canary_built(&order.order_id, self.now)?;
        let order = self.store.mark_execution_canary_simulated(
            &order.order_id,
            self.now,
            EXECUTION_SIMULATION_STATUS_PASSED,
            None,
        )?;
        let d = ExecutionCanaryDispatch {
            order_id: order.order_id.clone(),
            signal_id: id.into(),
            client_order_id: order.client_order_id.clone(),
            route: order.route.clone(),
            attempt: order.attempt,
            wallet: "wallet".into(),
            token: "mint".into(),
            side: side.into(),
            tx_signature: format!("sig-{id}"),
            message_sha256: "a".repeat(64),
            transaction_sha256: "b".repeat(64),
        };
        let budget = TinyBudgetClaim {
            experiment_id: "one".into(),
            wallet: "wallet".into(),
            tx_signature: d.tx_signature.clone(),
            message_sha256: d.message_sha256.clone(),
            transaction_sha256: d.transaction_sha256.clone(),
            buy_lamports: Some(if side == "buy" { 10_000_000 } else { 0 }),
            protected_capital: None,
            total_fee: 100_000,
            priority_fee: 50_000,
            fee_slot: 42,
        };
        Ok((order, signal, d, budget))
    }
    pub fn claim(&self, c: &Candidate, at: DateTime<Utc>) -> Result<ExecutionDispatchClaim> {
        self.store
            .claim_tiny_experiment_dispatch(&c.0, &c.1, &c.2, &c.3, at)
    }
    pub fn totals(&self) -> Result<(u64, u64, u64)> {
        Ok(self.conn()?.query_row("SELECT COUNT(*),COALESCE(SUM(CASE WHEN actual_fee IS NULL THEN fee_bound ELSE 0 END),0),COALESCE(SUM(actual_fee),0) FROM execution_tiny_reservations",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?)
    }
    pub fn successful(
        &self,
        c: &Candidate,
        fee: u64,
        at: DateTime<Utc>,
    ) -> Result<ExecutionCanaryReceiptFacts> {
        self.store.mark_execution_canary_confirmed_unreconciled(
            &c.0.order_id,
            &ExecutionCanaryReceiptProof {
                tx_signature: c.2.tx_signature.clone(),
                wallet_pubkey: "wallet".into(),
                token: "mint".into(),
                side: c.2.side.clone(),
                confirmation_status: "confirmed".into(),
                slot: Some(42),
                confirmed_at: at,
                reason: "pending".into(),
            },
            at,
        )?;
        let f = ExecutionCanaryReceiptFacts {
            order_id: c.0.order_id.clone(),
            tx_signature: c.2.tx_signature.clone(),
            wallet_pubkey: "wallet".into(),
            token: "mint".into(),
            side: c.2.side.clone(),
            slot: 42,
            wallet_native_pre: Lamports::new(100_000_000),
            wallet_native_post: Lamports::new(90_000_000 - fee),
            wallet_native_delta: SignedLamports::new(-10_000_000 - i128::from(fee)),
            transaction_fee: Some(Lamports::new(fee)),
            fee_coverage: ReceiptFeeCoverage::Known,
            fee_payer: Some("wallet".into()),
            token_delta: Some(ReceiptTokenDelta {
                raw: 7000,
                decimals: 3,
            }),
            token_coverage: ReceiptTokenCoverage::PairedBalances,
            token_coverage_reason: None,
            wsol_coverage: ReceiptWsolCoverage::Unresolved,
            block_time: Some(at.timestamp()),
            decomposition: ReceiptDecomposition::Unresolved,
        };
        self.store.record_execution_canary_receipt_facts(&f, at)?;
        Ok(f)
    }
    pub fn open_buy(&self, c: &Candidate, fee: u64) -> Result<()> {
        self.claim(c, self.now)?;
        self.successful(c, fee, self.now)?;
        self.store.confirm_execution_canary_buy_fill(
            &c.0.order_id,
            "mint",
            7.0,
            Some(TokenQuantity::new(7000, 3)),
            0.010_005,
            self.now,
            self.now,
            Some(Lamports::new(10_000_000 + fee)),
        )?;
        Ok(())
    }
    pub fn failure(
        &self,
        c: &Candidate,
        fee: u64,
        at: DateTime<Utc>,
    ) -> Result<FailedTransactionFacts> {
        self.store.detect_failed_expense(
            &c.0.order_id,
            "wallet",
            "signature_status",
            "confirmed",
            Some(42),
            &serde_json::json!({"InstructionError":[0,{"Custom":7}]}),
            at,
        )?;
        Ok(FailedTransactionFacts {
            tx_signature: c.2.tx_signature.clone(),
            wallet: "wallet".into(),
            slot: 42,
            commitment: "confirmed".into(),
            transaction_error: serde_json::json!({"InstructionError":[0,{"Custom":7}]}),
            transaction_fee_lamports: Some(fee.to_string()),
            fee_coverage: FailedExpenseCoverage::Known,
            payer: Some("wallet".into()),
            payer_coverage: FailedExpenseCoverage::Known,
            wallet_native_pre_lamports: Some("100000000".into()),
            wallet_native_post_lamports: Some((100000000 - fee).to_string()),
            native_coverage: FailedExpenseCoverage::Known,
        })
    }
    pub fn failed(&self, c: &Candidate, fee: u64, at: DateTime<Utc>) -> Result<()> {
        let f = self.failure(c, fee, at)?;
        self.store.apply_failed_expense(&c.0.order_id, &f, at)
    }
}
