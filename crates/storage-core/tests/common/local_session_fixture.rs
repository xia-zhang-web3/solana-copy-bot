#![allow(dead_code)]
//! Test-only bootstrap operands. Working LOCAL state is never seeded this way.
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{CopySignalRow, Lamports, SignedLamports};
use copybot_storage_core::local_bounded_session::*;
use copybot_storage_core::*;
use rusqlite::{params, Connection};
use std::path::{Path, PathBuf};
pub const BOT: &str = "7vJDrxN46rmZXKAuVyv2ZRCNRMyRn3d25B5P2pkFcvrn";
pub const SOURCE: &str = "7v54NWdBtkjuAFJrLGsS2SXnuk8nKam81mZJeeYxVFi9";
pub type Candidate = (
    ExecutionCanaryOrder,
    CopySignalRow,
    ExecutionCanaryDispatch,
    TinyBudgetClaim,
);
pub struct F {
    pub dir: tempfile::TempDir,
    pub path: PathBuf,
    pub store: SqliteStore,
    pub now: DateTime<Utc>,
    pub authority: Authority,
}
impl F {
    pub fn blank() -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("session.db");
        let mut store = SqliteStore::open(&path)?;
        store.run_migrations(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?;
        let now = Utc::now();
        let scope = controlled_receipt_lot::Scope {
            policy: controlled_receipt_lot::POLICY.into(),
            source_run_id: "model-source".into(),
            bot_run_id: "model-session".into(),
            source_wallet: SOURCE.into(),
            bot_wallet: BOT.into(),
            source_buy_intent_id: "cycle-1".into(),
            source_state_db_path: path.to_string_lossy().into(),
        };
        let authority = Authority {
            genesis: "11111111111111111111111111111111".into(),
            scope,
            model: Model {
                policy: "local_bounded_session_v1".into(),
                session_id: "model-session".into(),
                max_cycles: 2,
                gross_buy_cap: 20_000_000,
                fee_cap: 400_000,
                cycles: (0..2)
                    .map(|i| SourceCycle {
                        cycle_id: format!("cycle-{}", i + 1),
                        buy_signature: ((b'a' + i * 2) as char).to_string().repeat(64),
                        sell_signature: ((b'b' + i * 2) as char).to_string().repeat(64),
                        buy_message_sha256: ((b'a' + i * 2) as char).to_string().repeat(64),
                        sell_message_sha256: ((b'b' + i * 2) as char).to_string().repeat(64),
                        acquired_raw: 7000,
                        sold_raw: 7000,
                    })
                    .collect(),
            },
            cohort: native_buy::TechnicalCohortAuthority {
                run_id: "model-session".into(),
                wallet_ids: vec![SOURCE.into()],
                mint_policy: native_buy::CLASSIC_SPL_MINT_POLICY.into(),
                activated_at: now,
                deadline: now + Duration::seconds(600),
                max_buy_count: 2,
                policy_identity: "a".repeat(64),
            },
        };
        Ok(Self {
            dir,
            path,
            store,
            now,
            authority,
        })
    }
    pub fn new() -> Result<Self> {
        let f = Self::blank()?;
        f.store.register_local_bounded_session(&f.authority)?;
        // Structural FK anchor/policy are test operands, not a converted legacy grant.
        let c = f.conn()?;
        c.execute("INSERT INTO execution_tiny_experiment(singleton,experiment_id,wallet,activated_at,deadline,last_decision_at,state,policy_mode) VALUES(1,'model-session',?1,?2,?3,?2,'active','protected_native_capital')",params![BOT,f.now.to_rfc3339(),f.authority.cohort.deadline.to_rfc3339()])?;
        c.execute("INSERT INTO execution_tiny_native_policy VALUES('model-session',?1,1,'100000000','0','0',15000000,'42',?2)",params![BOT,f.now.to_rfc3339()])?;
        Ok(f)
    }
    pub fn conn(&self) -> Result<Connection> {
        Ok(Connection::open(&self.path)?)
    }
    pub fn count(&self, table: &str) -> Result<u64> {
        Ok(self
            .conn()?
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?)
    }
    pub fn candidate(&self, cycle: usize) -> Result<Candidate> {
        let signal = CopySignalRow {
            signal_id: format!(
                "native-buy-v1:{}",
                self.authority.model.cycles[cycle].buy_signature
            ),
            wallet_id: SOURCE.into(),
            side: "buy".into(),
            token: OWNER_EXIT_MINT.into(),
            notional_sol: 0.01,
            notional_lamports: Some(Lamports::new(10000000)),
            notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
            ts: self.now,
            status: "shadow_recorded".into(),
        };
        self.store.insert_copy_signal(&signal)?;
        let order = self
            .store
            .reserve_execution_canary_order(&signal.signal_id, "tiny", self.now)?
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
            signal_id: signal.signal_id.clone(),
            client_order_id: order.client_order_id.clone(),
            route: order.route.clone(),
            attempt: order.attempt,
            wallet: BOT.into(),
            token: OWNER_EXIT_MINT.into(),
            side: "buy".into(),
            tx_signature: format!("mock-bot-{cycle}"),
            message_sha256: "a".repeat(64),
            transaction_sha256: "b".repeat(64),
        };
        let policy = self
            .store
            .tiny_native_policy("model-session", BOT, self.now)?;
        let p = TinyBudgetClaim {
            experiment_id: "model-session".into(),
            wallet: BOT.into(),
            tx_signature: d.tx_signature.clone(),
            message_sha256: d.message_sha256.clone(),
            transaction_sha256: d.transaction_sha256.clone(),
            buy_lamports: None,
            protected_capital: Some(ProtectedCapitalClaim {
                policy,
                requested_lamports: 10000000,
                floor_lamports: 0,
                request_sha256: "c".repeat(64),
            }),
            total_fee: 5000,
            priority_fee: 0,
            fee_slot: 42,
        };
        Ok((order, signal, d, p))
    }
    pub fn claim(&self, c: &Candidate, at: DateTime<Utc>) -> Result<ExecutionDispatchClaim> {
        self.store
            .claim_tiny_experiment_dispatch(&c.0, &c.1, &c.2, &c.3, at)
    }
    pub fn facts(&self, c: &Candidate, fee: u64) -> Result<ExecutionCanaryReceiptFacts> {
        self.store.mark_execution_canary_confirmed_unreconciled(
            &c.0.order_id,
            &ExecutionCanaryReceiptProof {
                tx_signature: c.2.tx_signature.clone(),
                wallet_pubkey: BOT.into(),
                token: OWNER_EXIT_MINT.into(),
                side: "buy".into(),
                confirmation_status: "finalized".into(),
                slot: Some(42),
                confirmed_at: self.now,
                reason: "accounting_pending".into(),
            },
            self.now,
        )?;
        let f = ExecutionCanaryReceiptFacts {
            order_id: c.0.order_id.clone(),
            tx_signature: c.2.tx_signature.clone(),
            wallet_pubkey: BOT.into(),
            token: OWNER_EXIT_MINT.into(),
            side: "buy".into(),
            slot: 42,
            wallet_native_pre: Lamports::new(100000000),
            wallet_native_post: Lamports::new(90000000 - fee),
            wallet_native_delta: SignedLamports::new(-10000000 - i128::from(fee)),
            transaction_fee: Some(Lamports::new(fee)),
            fee_coverage: ReceiptFeeCoverage::Known,
            fee_payer: Some(BOT.into()),
            token_delta: Some(ReceiptTokenDelta {
                raw: 7000,
                decimals: 6,
            }),
            token_coverage: ReceiptTokenCoverage::PairedBalances,
            token_coverage_reason: None,
            wsol_coverage: ReceiptWsolCoverage::Unresolved,
            block_time: Some(self.now.timestamp()),
            decomposition: ReceiptDecomposition::Unresolved,
        };
        self.store
            .record_execution_canary_receipt_facts(&f, self.now)?;
        Ok(f)
    }
}
