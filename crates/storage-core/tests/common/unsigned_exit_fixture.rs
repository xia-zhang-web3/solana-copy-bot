//! Copied pinned stranded state, fake dispatch and historical LOCAL clocks only.
#[path = "owned_expiry_fixture.rs"]
mod original;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{Lamports, SignedLamports};
use copybot_storage_core::*;
use rusqlite::Connection;
use std::ops::Deref;
pub const BOT: &str = "BwVw8ncEpWU7TwMTgysvwjQ85eEhKAMVbd7WU1iTE9Mk";
pub struct Case {
    pub original: original::Fixture,
    pub intent: OwnerExitIntent,
}
impl Deref for Case {
    type Target = original::Fixture;
    fn deref(&self) -> &Self::Target {
        &self.original
    }
}
impl Case {
    pub fn new() -> Result<Self> {
        let original = original::open()?;
        let q = &original.old.handoff.snapshot.quote;
        let c = Connection::open(&original.path)?;
        let (buy, signature): (String, String) = c.query_row(
            "SELECT order_id,tx_signature FROM execution_canary_receipt_facts WHERE side='buy'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        // This is a synthetic test decision, never a future real approval.
        let start: DateTime<Utc> = "2026-11-03T20:00:00Z".parse()?;
        let intent = OwnerExitIntent {
            intent_id: format!("bot-receipt-exit:{}", q.source_signature),
            run_id: original.old.handoff.experiment_id.clone(),
            buy_order_id: buy,
            buy_receipt_signature: signature,
            position_id: q.position_id.clone(),
            wallet: BOT.into(),
            signer: BOT.into(),
            genesis_hash: "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d".into(),
            mint: OWNER_EXIT_MINT.into(),
            amount_raw: 930183,
            decimals: 6,
            route: "jupiter_swap_instructions".into(),
            activated_at: start,
            expires_at: start + Duration::seconds(300),
            authority_sha256: "c".repeat(64),
            max_priority_fee_lamports: 22000,
            min_reserve_lamports: 0,
            max_slippage_bps: 50,
            max_daily_loss_lamports: 20000000,
        };
        Ok(Self { original, intent })
    }
    pub fn conn(&self) -> Result<Connection> {
        Ok(Connection::open(&self.path)?)
    }
    pub fn parents(&self) -> Result<Vec<String>> {
        let c = self.conn()?;
        [
            "rpc_owned_sell_handoffs",
            "execution_canary_build_plan_metadata",
            "execution_tiny_experiment",
            "execution_tiny_native_policy",
            "real_session_sessions",
            "native_buy_decisions",
            "real_session_native_decisions",
            "native_buy_fence_epochs",
        ]
        .into_iter()
        .map(|table| {
            let mut q = c.prepare(&format!("SELECT * FROM {table} ORDER BY 1"))?;
            let n = q.column_count();
            let rows = q
                .query_map([], |r| {
                    (0..n)
                        .map(|i| r.get::<_, rusqlite::types::Value>(i))
                        .collect::<rusqlite::Result<Vec<_>>>()
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(format!("{table}:{rows:?}"))
        })
        .collect()
    }
    pub fn corrupt_clone(&self, sql: &str) -> Result<()> {
        let c = self.conn()?;
        c.execute_batch("PRAGMA foreign_keys=OFF")?;
        let names = c
            .prepare("SELECT name FROM sqlite_master WHERE type='trigger'")?
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        // Explicit corruption of this LOCAL copy; never production mutation APIs.
        for n in names {
            c.execute_batch(&format!("DROP TRIGGER \"{n}\""))?;
        }
        c.execute_batch(sql)?;
        Ok(())
    }
    pub fn dispatch(
        &self,
    ) -> Result<(
        ExecutionCanaryOrder,
        ExecutionCanaryDispatch,
        TinyBudgetClaim,
    )> {
        self.store.register_owner_exit_intent(&self.intent)?;
        let now = self.intent.activated_at;
        let order = self
            .store
            .reserve_owner_exit_order(&self.intent.intent_id, || Ok(now))?
            .order;
        self.store
            .mark_execution_canary_built(&order.order_id, now)?;
        let order = self.store.mark_execution_canary_simulated(
            &order.order_id,
            now,
            EXECUTION_SIMULATION_STATUS_PASSED,
            None,
        )?;
        let d = ExecutionCanaryDispatch {
            order_id: order.order_id.clone(),
            signal_id: order.signal_id.clone(),
            client_order_id: order.client_order_id.clone(),
            route: order.route.clone(),
            attempt: 1,
            wallet: BOT.into(),
            token: OWNER_EXIT_MINT.into(),
            side: "sell".into(),
            tx_signature: "EXPLICIT-LOCAL-MOCK-unsigned-hold-transfer".into(),
            transaction_sha256: "d".repeat(64),
            message_sha256: "e".repeat(64),
        };
        let budget = TinyBudgetClaim {
            experiment_id: self.intent.run_id.clone(),
            wallet: BOT.into(),
            tx_signature: d.tx_signature.clone(),
            message_sha256: d.message_sha256.clone(),
            transaction_sha256: d.transaction_sha256.clone(),
            buy_lamports: Some(0),
            protected_capital: None,
            total_fee: 27000,
            priority_fee: 22000,
            fee_slot: 465022600,
        };
        Ok((order, d, budget))
    }
    pub fn receipt(&self, d: &ExecutionCanaryDispatch) -> ExecutionCanaryReceiptFacts {
        ExecutionCanaryReceiptFacts {
            order_id: d.order_id.clone(),
            tx_signature: d.tx_signature.clone(),
            wallet_pubkey: BOT.into(),
            token: OWNER_EXIT_MINT.into(),
            side: "sell".into(),
            slot: 465022700,
            wallet_native_pre: Lamports::new(255915094),
            wallet_native_post: Lamports::new(265833094),
            wallet_native_delta: SignedLamports::new(9918000),
            transaction_fee: Some(Lamports::new(27000)),
            fee_coverage: ReceiptFeeCoverage::Known,
            fee_payer: Some(BOT.into()),
            token_delta: Some(ReceiptTokenDelta {
                raw: -(self.intent.amount_raw as i128),
                decimals: 6,
            }),
            token_coverage: ReceiptTokenCoverage::PairedBalances,
            token_coverage_reason: None,
            wsol_coverage: ReceiptWsolCoverage::Observed,
            block_time: Some(self.intent.activated_at.timestamp()),
            decomposition: ReceiptDecomposition::Unresolved,
        }
    }
    pub fn settle(&self, d: &ExecutionCanaryDispatch) -> Result<()> {
        let now = self.intent.activated_at;
        self.store.mark_execution_canary_confirmed_unreconciled(
            &d.order_id,
            &ExecutionCanaryReceiptProof {
                tx_signature: d.tx_signature.clone(),
                wallet_pubkey: BOT.into(),
                token: OWNER_EXIT_MINT.into(),
                side: "sell".into(),
                confirmation_status: "finalized".into(),
                slot: Some(465022700),
                confirmed_at: now,
                reason: "accounting_pending".into(),
            },
            now,
        )?;
        let facts = self.receipt(d);
        self.store
            .record_execution_canary_receipt_facts(&facts, now)?;
        self.store
            .apply_execution_canary_sell_settlement(&facts, now)?;
        Ok(())
    }
}
