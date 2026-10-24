//! A separate one-shot exit of an expired native BUY, never renewed BUY authority.
use crate::{OwnerExitIntent, SqliteStore};
use anyhow::{ensure, Context, Result};
use chrono::Duration;
use rusqlite::{params, Connection};

#[path = "owner_bot_receipt_exit_observations.rs"]
mod observations;

pub const MIGRATION: &str = "0093_bot_receipt_exit_zero_scope.sql";
const PREFIX: &str = "bot-receipt-exit:";

pub fn bot_receipt_exit_id(id: &str) -> bool {
    id.strip_prefix(PREFIX).is_some_and(|s| !s.is_empty())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnerBotReceiptExitProof {
    pub personal_raw: u64,
    pub expected_wallet_total_raw: u64,
    pub native_remaining: u64,
    pub token_account: String,
    pub buy_slot: u64,
}

pub(crate) fn historical(c: &Connection, i: &OwnerExitIntent) -> Result<OwnerBotReceiptExitProof> {
    let source = i.intent_id.strip_prefix(PREFIX).context("bot_receipt_exit_origin")?;
    let (wallet, run, scope) = crate::controlled_receipt_lot::load(c)?
        .context("bot_receipt_exit_scope")?;
    ensure!(wallet == i.wallet && run == i.run_id && scope.bot_wallet == wallet
        && scope.bot_run_id == run && i.signer == wallet
        && i.buy_order_id == format!("exec-canary:native-buy-v1:{source}")
        && i.position_id == format!("exec-canary-pos:{}", i.buy_order_id)
        && i.mint == crate::OWNER_EXIT_MINT && i.decimals == 6 && i.amount_raw > 0
        && i.route == "jupiter_swap_instructions" && i.min_reserve_lamports == 0
        && i.max_priority_fee_lamports <= 22_000 && i.max_slippage_bps <= 50
        && i.max_daily_loss_lamports <= 20_000_000,
        "bot_receipt_exit_identity_or_limits");
    let cohort = crate::native_buy::cohort::load(c)?.context("bot_receipt_exit_cohort")?;
    ensure!(cohort.run_id == run && cohort.max_buy_count == 1
        && cohort.wallet_ids.contains(&scope.source_wallet)
        && i.activated_at >= cohort.deadline && i.activated_at < i.expires_at
        && i.expires_at - i.activated_at <= Duration::seconds(300),
        "bot_receipt_exit_window_or_parent");
    let bound: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM orders o
         JOIN execution_order_sources s ON s.identity_id=o.signal_id AND s.copy_signal_id=o.signal_id
         JOIN copy_signals cs ON cs.signal_id=s.copy_signal_id
         JOIN native_buy_decisions n ON n.signal_id=o.signal_id
         JOIN native_buy_cohort_decisions b ON b.signature=n.signature
         JOIN native_buy_fence_epochs e ON e.epoch_id=b.fence_epoch_id
         JOIN execution_canary_dispatch d ON d.order_id=o.order_id
         JOIN execution_canary_receipt_proofs p ON p.order_id=o.order_id
         JOIN execution_canary_receipt_facts r ON r.order_id=o.order_id
         JOIN fills f ON f.order_id=o.order_id
         JOIN execution_tiny_experiment t ON t.buy_order_id=o.order_id
         JOIN execution_tiny_native_policy a ON a.experiment_id=t.experiment_id
         JOIN execution_tiny_reservations v ON v.order_id=o.order_id
         WHERE o.order_id=?1 AND o.order_id='exec-canary:'||o.signal_id
           AND o.signal_id='native-buy-v1:'||n.signature AND n.signature=?9
           AND o.status='execution_canary_confirmed' AND o.attempt=1
           AND cs.side='buy' AND cs.wallet_id=?10 AND cs.token=?5
           AND b.run_id=?11 AND b.wallet=?10 AND b.mint=?5
           AND b.policy_identity=?12 AND e.policy_identity=?12 AND e.genesis_hash=?4
           AND e.session=n.first_session AND e.processed_slot<n.source_slot
           AND b.admitted_at=n.admitted_at AND b.wallet=n.wallet AND b.mint=n.mint
           AND n.finalized_at IS NOT NULL AND n.finalized_slot=n.source_slot
           AND n.wallet=?10 AND n.mint=?5 AND n.amount_lamports=10000000
           AND d.signal_id=o.signal_id AND d.client_order_id=o.client_order_id
           AND d.side='buy' AND d.attempt=1 AND d.route=o.route AND o.route=?13
           AND o.tx_signature=?2 AND d.tx_signature=?2 AND p.tx_signature=?2 AND r.tx_signature=?2
           AND p.confirmation_status='finalized' AND p.reason='accounting_complete'
           AND p.side='buy' AND r.side='buy' AND p.slot=r.slot
           AND p.wallet_pubkey=?3 AND r.wallet_pubkey=?3 AND d.wallet=?3
           AND p.token=?5 AND r.token=?5 AND d.token=?5
           AND f.token=?5 AND f.position_id=?6 AND f.qty_raw=?7 AND f.qty_decimals=?8
           AND r.token_delta_raw=?7 AND r.token_decimals=?8 AND r.token_coverage!='unresolved'
           AND t.experiment_id=?11 AND t.wallet=?3 AND t.token=?5 AND t.position_id=?6
           AND t.deadline=?14 AND t.policy_mode='protected_native_capital'
           AND a.wallet=?3 AND a.version=1 AND a.allowance=15000000
           AND a.original_reserve='0' AND a.floor_lamports='0'
           AND v.experiment_id=t.experiment_id AND v.wallet=?3 AND v.side='buy'
           AND v.tx_signature=?2 AND v.outcome='successful'
           AND v.message_sha256=d.message_sha256 AND v.transaction_sha256=d.transaction_sha256
           AND v.actual_fee=CAST(r.transaction_fee AS INTEGER))",
        params![i.buy_order_id, i.buy_receipt_signature, i.wallet, i.genesis_hash, i.mint,
            i.position_id, i.amount_raw.to_string(), i.decimals, source, scope.source_wallet,
            i.run_id, cohort.policy_identity, i.route, cohort.deadline.to_rfc3339()], |r| r.get(0))?;
    ensure!(bound, "bot_receipt_exit_native_buy_binding");
    let facts = crate::receipt_facts_rows::load(c, &i.buy_order_id)?
        .context("bot_receipt_exit_buy_receipt")?;
    crate::receipt_facts_identity::validate_identity(c, &facts)?;
    ensure!(!crate::buy_receipt_collisions::load(c)?
        .contains(&(i.wallet.clone(), i.buy_receipt_signature.clone())),
        "bot_receipt_exit_receipt_ambiguous");
    ensure!(facts.fee_payer.as_deref() == Some(i.wallet.as_str())
        && facts.fee_coverage.as_str() == "known", "bot_receipt_exit_buy_fee");
    let fee = facts.transaction_fee.context("bot_receipt_exit_buy_fee")?.as_u64();
    let debit = u64::try_from(facts.wallet_native_delta.as_i128().checked_neg()
        .context("bot_receipt_exit_buy_debit")?)?;
    ensure!(debit >= crate::tiny_experiment::TINY_BUY_LAMPORTS.checked_add(fee)
        .context("bot_receipt_exit_buy_debit")?, "bot_receipt_exit_buy_debit");
    let native_remaining = crate::TINY_NATIVE_ALLOWANCE.checked_sub(debit)
        .context("bot_receipt_exit_native_cap")?;
    ensure!(native_remaining >= crate::tiny_experiment::TINY_TRANSACTION_FEE,
        "bot_receipt_exit_native_cap");
    observations::proof(c, i, facts.slot, native_remaining)
}

pub(crate) fn validate(c: &Connection, i: &OwnerExitIntent) -> Result<OwnerBotReceiptExitProof> {
    let migrated: bool = c.query_row("SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=?1)",
        [MIGRATION], |r| r.get(0))?;
    ensure!(migrated, "bot_receipt_exit_migration_required");
    let proof = historical(c, i)?;
    let bound: bool = c.query_row("SELECT EXISTS(SELECT 1 FROM positions x JOIN fills f
        ON f.position_id=x.position_id AND f.order_id=?1 JOIN execution_canary_receipt_facts r
        ON r.order_id=f.order_id WHERE x.position_id=?2 AND x.token=?3
        AND x.state='open' AND x.accounting_bucket='execution_canary'
        AND x.qty_raw=?4 AND x.qty_decimals=6 AND x.cost_lamports=f.notional_lamports
        AND CAST(r.wallet_native_delta AS INTEGER)=-x.cost_lamports)",
        params![i.buy_order_id, i.position_id, i.mint, i.amount_raw.to_string()], |r| r.get(0))?;
    ensure!(bound, "bot_receipt_exit_position_changed");
    let crate::ExecutionCanaryBuyAttribution::Open(attribution) =
        crate::execution_canary_buy_attribution::read_on_conn(c, &i.mint)? else {
            anyhow::bail!("bot_receipt_exit_position_missing");
        };
    ensure!(attribution.position_id == i.position_id && attribution.unproven_links.is_empty()
        && attribution.proven_contributors.len() == 1
        && attribution.proven_contributors[0].order_id == i.buy_order_id
        && attribution.proven_contributors[0].tx_signature == i.buy_receipt_signature,
        "bot_receipt_exit_contributors_changed");
    let own = crate::owner_exit_order_id(&i.intent_id);
    let clean: bool = c.query_row("SELECT
        NOT EXISTS(SELECT 1 FROM execution_canary_unresolved_dispatch WHERE order_id<>?1)
        AND NOT EXISTS(SELECT 1 FROM execution_failed_expense_tasks WHERE status!='complete' AND order_id<>?1)
        AND NOT EXISTS(SELECT 1 FROM owner_exit_fee_reservations WHERE state='pending' AND order_id<>?1)
        AND NOT EXISTS(SELECT 1 FROM rpc_owned_sell_handoffs h WHERE NOT EXISTS(
            SELECT 1 FROM rpc_owned_sell_dispatches d WHERE d.intent_id=h.intent_id AND d.handoff_owner=h.owner))",
        [&own], |r| r.get(0))?;
    ensure!(clean, "bot_receipt_exit_foreign_obligation");
    Ok(proof)
}

impl SqliteStore {
    /// One fresh snapshot; no old BUY/cohort/tiny clocks are refreshed or updated.
    pub fn owner_bot_receipt_exit_proof(&self, i: &OwnerExitIntent) -> Result<OwnerBotReceiptExitProof> {
        if !self.conn.is_autocommit() {
            return validate(&self.conn, i);
        }
        let tx = self.conn.unchecked_transaction()?;
        let proof = validate(&tx, i)?;
        tx.commit()?;
        Ok(proof)
    }
}
