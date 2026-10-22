//! Private SOURCE exit byte scope; it is never deserialized or financial authority.
use crate::execution_submit_adapter::ExecutionSubmitRequest;
use anyhow::{ensure, Context, Result};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{controlled_receipt_lot::Scope, OwnerExitIntent, SqliteStore};
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExitWire {
    scope: Scope,
    intent: OwnerExitIntent,
    order: String,
    signal: String,
    native_remaining: u64,
}
impl ExitWire {
    pub(crate) fn native_remaining(&self) -> u64 { self.native_remaining }
    pub(crate) fn matches(&self, r: &ExecutionSubmitRequest) -> bool {
        self.scope.policy == copybot_config::CONTROLLED_RECEIPT_LOT_V1
            && self.intent.wallet == self.scope.source_wallet
            && self.intent.run_id == self.scope.source_run_id
            && r.wallet_pubkey == self.intent.wallet
            && r.side == "sell"
            && r.token == self.intent.mint
            && r.order_id == self.order
            && r.signal_id == self.signal
            && r.metadata.quote_in_amount_raw.as_deref()
                == Some(self.intent.amount_raw.to_string().as_str())
    }
}
pub(crate) fn stamp(
    c: &ExecutionConfig,
    s: &SqliteStore,
    i: &OwnerExitIntent,
    r: &mut ExecutionSubmitRequest,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let Some(scope) = super::scope(c)? else {
        return Ok(());
    };
    ensure!(
        scope.source_wallet == i.wallet
            && scope.source_run_id == i.run_id
            && (i.intent_id == format!("execution-only:{}", scope.source_buy_intent_id)
                || c.owner_exit.as_ref().is_some_and(|p|p.policy == copybot_config::OWNER_SOURCE_RECOVERY_EXIT_V1)
                    && i.intent_id == format!("source-recovery:{}",scope.source_buy_intent_id))
            && s.controlled_receipt_lot_scope()?.as_ref()
                == Some(&(i.wallet.clone(), i.run_id.clone(), scope.clone())),
        "receipt_lot_exit_wire_scope"
    );
    crate::execution_owner_exit_authority::current(c, s, i, now)?;
    ensure!(
        crate::execution_owner_exit_authority::request(
            s,
            r,
            &[copybot_storage_core::EXECUTION_STATUS_CANARY_CANDIDATE]
        )?
        .as_ref()
            == Some(i),
        "receipt_lot_exit_wire_order"
    );
    let facts = s
        .load_execution_canary_receipt_facts(&i.buy_order_id)?
        .context("receipt_lot_exit_buy_receipt")?;
    ensure!(
        facts.tx_signature == i.buy_receipt_signature
            && facts.wallet_pubkey == i.wallet
            && facts
                .token_delta
                .is_some_and(|d| d.raw == i128::from(i.amount_raw) && d.decimals == 6),
        "receipt_lot_exit_wire_receipt"
    );
    r.metadata.controlled_exit_wire = Some(Box::new(ExitWire {
        native_remaining: if i.intent_id.starts_with("source-recovery:") {
            s.owner_source_recovery_native_remaining(i)?
        } else {copybot_storage_core::TINY_NATIVE_ALLOWANCE},
        scope,
        intent: i.clone(),
        order: r.order_id.clone(),
        signal: r.signal_id.clone(),
    }));
    Ok(())
}
