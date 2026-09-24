//! Typed owner activation and exact decoded amount within protected-capital claims.
use crate::{
    ExecutionCanaryOrder, ExecutionOrderOrigin, OwnerTechnicalBuyIntent, SqliteDiscoveryStore,
    TinyBudgetClaim, EXECUTION_STATUS_CANARY_CANDIDATE,
};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension};

pub(crate) fn activation_current(
    store: &SqliteDiscoveryStore,
    intent: &OwnerTechnicalBuyIntent,
    order: &ExecutionCanaryOrder,
    run_id: &str,
    wallet: &str,
    reserve: u64,
    now: DateTime<Utc>,
) -> Result<()> {
    ensure!(
        store
            .load_owner_technical_buy_intent(&intent.intent_id)?
            .as_ref()
            == Some(intent)
            && intent.run_id == run_id
            && intent.wallet == wallet
            && intent.signer == wallet
            && reserve >= intent.min_reserve_lamports,
        "owner_buy_protected_authority_changed"
    );
    ensure!(
        now >= intent.activated_at && now < intent.expires_at,
        "owner_buy_intent_expired"
    );
    ensure!(
        order.order_id == crate::owner_technical_buy_order_id(&intent.intent_id)
            && order.signal_id == crate::owner_technical_buy_identity_id(&intent.intent_id)
            && order.client_order_id
                == crate::owner_technical_buy_client_order_id(&intent.intent_id)
            && order.route == intent.route
            && order.attempt == 1
            && order.status == EXECUTION_STATUS_CANARY_CANDIDATE
            && order.tx_signature.is_none()
            && store.load_execution_canary_order(&order.order_id)?.as_ref() == Some(order),
        "owner_buy_protected_order_changed"
    );
    ensure!(
        matches!(store.execution_order_origin(&order.order_id)?,
        Some(ExecutionOrderOrigin::OwnerTechnicalBuy { intent_id })
            if intent_id == intent.intent_id),
        "owner_buy_protected_origin_changed"
    );
    Ok(())
}

/// Old copy protected claims retain decoded=NULL. An owner protected claim must
/// carry the independently decoded signed-message amount as well as its floor.
pub(crate) fn decoded_claim(
    conn: &Connection,
    run_id: &str,
    claim: &TinyBudgetClaim,
) -> Result<()> {
    let owner = conn
        .query_row(
            "SELECT wallet,amount_lamports,min_reserve_lamports
        FROM owner_technical_buy_intents WHERE run_id=?1",
            [run_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, u64>(2)?,
                ))
            },
        )
        .optional()?
        .context("owner_buy_protected_origin_missing")?;
    let proof = claim
        .protected_capital
        .as_ref()
        .context("owner_buy_protected_claim_missing")?;
    ensure!(
        claim.wallet == owner.0
            && claim.buy_lamports == Some(owner.1)
            && proof.requested_lamports == owner.1
            && proof.floor_lamports >= owner.2
            && proof.policy.wallet == owner.0
            && proof.policy.experiment_id == run_id,
        "owner_buy_protected_decoded_amount_binding"
    );
    Ok(())
}
