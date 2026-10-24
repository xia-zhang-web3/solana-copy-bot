//! Positive, read-only closure witness for the old native BOT lot before new BUY.
use crate::{
    owner_exit_identity_id, owner_exit_order_id, ExecutionOrderOrigin, SqliteDiscoveryStore,
    EXECUTION_STATUS_CANARY_CONFIRMED,
};
use anyhow::Result;
use rusqlite::{params, Connection};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriorBotReceiptExitBinding {
    pub exit_intent_id: String,
    pub buy_order_id: String,
    pub buy_receipt_signature: String,
    pub position_id: String,
    pub wallet_pubkey: String,
    pub mint: String,
    pub amount_raw: u64,
    pub decimals: u8,
}

impl SqliteDiscoveryStore {
    /// The caller opens the previous phase with open_read_only. Every observation
    /// below shares one SQLite read snapshot; absence never proves closure.
    pub fn prior_bot_receipt_exit_closed(&self, b: &PriorBotReceiptExitBinding) -> Result<bool> {
        let tx = self.conn.unchecked_transaction()?;
        let closed = closed(self, &tx, b)?;
        tx.commit()?;
        Ok(closed)
    }
}

fn closed(
    store: &SqliteDiscoveryStore,
    c: &Connection,
    b: &PriorBotReceiptExitBinding,
) -> Result<bool> {
    let Some(intent) = store.load_owner_exit_intent(&b.exit_intent_id)? else {
        return Ok(false);
    };
    if b.amount_raw == 0
        || b.decimals != 6
        || intent.buy_order_id != b.buy_order_id
        || intent.buy_receipt_signature != b.buy_receipt_signature
        || intent.position_id != b.position_id
        || intent.wallet != b.wallet_pubkey
        || intent.signer != b.wallet_pubkey
        || intent.mint != b.mint
        || intent.amount_raw != b.amount_raw
        || intent.decimals != b.decimals
    {
        return Ok(false);
    }
    // Reuse the A lane's exact native origin, expired authority, receipt/native
    // observations and personal-inventory binding without requiring an open lot.
    crate::owner_bot_receipt_exit::historical(c, &intent)?;
    let Some(buy) = store.load_execution_canary_order(&b.buy_order_id)? else {
        return Ok(false);
    };
    let native_origin = matches!(store.execution_order_origin(&b.buy_order_id)?,
        Some(ExecutionOrderOrigin::Copy { signal_id })
            if signal_id.starts_with("native-buy-v1:")
                && b.buy_order_id == format!("exec-canary:{signal_id}"));
    if !native_origin
        || buy.status != EXECUTION_STATUS_CANARY_CONFIRMED
        || buy.tx_signature.as_deref() != Some(b.buy_receipt_signature.as_str())
    {
        return Ok(false);
    }
    let Some(buy_facts) = crate::receipt_facts_rows::load(c, &b.buy_order_id)? else {
        return Ok(false);
    };
    let Some(buy_proof) = store.load_execution_canary_receipt_proof(&b.buy_order_id)? else {
        return Ok(false);
    };
    if buy_facts.wallet_pubkey != b.wallet_pubkey
        || buy_facts.token != b.mint
        || buy_facts.side != "buy"
        || buy_facts.tx_signature != b.buy_receipt_signature
        || !buy_facts
            .token_delta
            .is_some_and(|q| q.raw == i128::from(b.amount_raw) && q.decimals == b.decimals)
        || buy_proof.reason != "accounting_complete"
    {
        return Ok(false);
    }
    buy_facts.validate()?;
    crate::receipt_facts_identity::validate_identity(c, &buy_facts)?;
    let bought_into_position: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM fills WHERE order_id=?1 AND position_id=?2
          AND token=?3 AND qty_raw=?4 AND qty_decimals=?5)",
        params![
            b.buy_order_id,
            b.position_id,
            b.mint,
            b.amount_raw.to_string(),
            b.decimals
        ],
        |r| r.get(0),
    )?;
    if !bought_into_position {
        return Ok(false);
    }

    let exit = owner_exit_order_id(&b.exit_intent_id);
    let Some(order) = store.load_execution_canary_order(&exit)? else {
        return Ok(false);
    };
    if order.status != EXECUTION_STATUS_CANARY_CONFIRMED
        || order.signal_id != owner_exit_identity_id(&b.exit_intent_id)
        || !matches!(store.execution_order_origin(&exit)?,
            Some(ExecutionOrderOrigin::OwnerExit { intent_id }) if intent_id == b.exit_intent_id)
    {
        return Ok(false);
    }
    let Some(dispatch) = store.load_execution_canary_dispatch(&exit)? else {
        return Ok(false);
    };
    let Some(facts) = crate::receipt_facts_rows::load(c, &exit)? else {
        return Ok(false);
    };
    if facts.wallet_pubkey != b.wallet_pubkey
        || facts.token != b.mint
        || facts.side != "sell"
        || !facts
            .token_delta
            .is_some_and(|q| q.raw == -i128::from(b.amount_raw) && q.decimals == b.decimals)
        || dispatch.tx_signature != facts.tx_signature
        || dispatch.wallet != b.wallet_pubkey
        || dispatch.token != b.mint
        || dispatch.side != "sell"
        || dispatch.signal_id != order.signal_id
        || dispatch.client_order_id != order.client_order_id
        || dispatch.attempt != 1
        || order.attempt != 1
    {
        return Ok(false);
    }
    facts.validate()?;
    crate::receipt_facts_identity::validate_identity(c, &facts)?;
    // The ordinary replay helper intentionally permits an absent settlement.
    // This transition requires a positive cash fill before revalidating it.
    if crate::execution_cash_settlement_rows::load(c, &exit)?.is_none() {
        return Ok(false);
    }
    let cash = crate::execution_cash_settlement::validate_existing(c, &exit)?;
    let paid = buy_facts
        .wallet_native_delta
        .as_i128()
        .checked_neg()
        .and_then(|value| u64::try_from(value).ok());
    if cash.position_id != b.position_id
        || cash.token != b.mint
        || cash.sold_quantity.raw() != b.amount_raw
        || cash.sold_quantity.decimals() != b.decimals
        || cash.remaining_quantity.raw() != 0
        || cash.remaining_entry_basis.as_u64() != 0
        || paid != Some(cash.allocated_entry_basis.as_u64())
    {
        return Ok(false);
    }
    let closed_position: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM positions WHERE position_id=?1 AND token=?2
          AND state='closed' AND qty_raw='0' AND qty_decimals=?3 AND cost_lamports=0
          AND accounting_bucket=?4 AND closed_ts IS NOT NULL)",
        params![
            b.position_id,
            b.mint,
            b.decimals,
            crate::EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET
        ],
        |r| r.get(0),
    )?;
    if !closed_position || !fees_complete(c, b, &intent.run_id, &exit, &facts, &buy_facts)? {
        return Ok(false);
    }
    if store.execution_canary_accounting_pending()? || any_pending(c)? {
        return Ok(false);
    }
    Ok(true)
}

fn fees_complete(
    c: &Connection,
    b: &PriorBotReceiptExitBinding,
    run: &str,
    exit: &str,
    sell: &crate::ExecutionCanaryReceiptFacts,
    buy: &crate::ExecutionCanaryReceiptFacts,
) -> Result<bool> {
    let (Some(sell_fee), Some(buy_fee)) = (sell.transaction_fee, buy.transaction_fee) else {
        return Ok(false);
    };
    Ok(c.query_row(
        "SELECT EXISTS(SELECT 1 FROM owner_exit_fee_reservations
            WHERE order_id=?1 AND run_id=?2 AND tx_signature=?3 AND wallet=?4
              AND state='successful' AND actual_fee=?5 AND reconciled_at IS NOT NULL)
          AND EXISTS(SELECT 1 FROM execution_tiny_reservations
            WHERE order_id=?6 AND tx_signature=?7 AND wallet=?4 AND side='buy'
              AND outcome='successful' AND actual_fee=?8 AND reconciled_at IS NOT NULL)",
        params![
            exit,
            run,
            sell.tx_signature,
            b.wallet_pubkey,
            i64::try_from(sell_fee.as_u64())?,
            b.buy_order_id,
            b.buy_receipt_signature,
            i64::try_from(buy_fee.as_u64())?
        ],
        |r| r.get(0),
    )?)
}

fn any_pending(c: &Connection) -> Result<bool> {
    // unresolved_buy excludes SELL, so it cannot implement this phase boundary.
    Ok(c.query_row(
        "SELECT EXISTS(SELECT 1 FROM execution_canary_unresolved_dispatch)
          OR EXISTS(SELECT 1 FROM execution_failed_expense_tasks WHERE status!='complete')
          OR EXISTS(SELECT 1 FROM execution_tiny_reservations
                    WHERE outcome IS NULL OR actual_fee IS NULL OR reconciled_at IS NULL)
          OR EXISTS(SELECT 1 FROM owner_exit_fee_reservations
                    WHERE state='pending' OR actual_fee IS NULL OR reconciled_at IS NULL)
          OR EXISTS(SELECT 1 FROM orders WHERE status IN ('execution_canary_candidate',
              'execution_canary_built','execution_canary_simulated','execution_canary_submitted',
              'execution_canary_confirmed_unreconciled'))",
        [],
        |r| r.get(0),
    )?)
}
