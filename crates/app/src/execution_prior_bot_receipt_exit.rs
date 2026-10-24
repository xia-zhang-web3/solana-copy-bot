//! Both new-cycle BUY lanes wait for an exact, accounted prior BOT exit.
use anyhow::{ensure, Context, Result};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{PriorBotReceiptExitBinding, SqliteStore};

pub(crate) fn bind_identity(c: &ExecutionConfig, base: String) -> Result<String> {
    copybot_config::validate_prior_bot_receipt_exit(c)?;
    match &c.prior_bot_receipt_exit {
        None => Ok(base),
        Some(p) => Ok(crate::execution_owned_sell_rpc::digest(serde_json::to_vec(
            &(base, copybot_config::PRIOR_BOT_RECEIPT_EXIT_V1, p),
        )?)),
    }
}

pub(crate) fn ready(c: &ExecutionConfig) -> Result<bool> {
    copybot_config::validate_prior_bot_receipt_exit(c)?;
    let Some(p) = &c.prior_bot_receipt_exit else {
        return Ok(true);
    };
    for (key, label) in [
        (&p.wallet_pubkey, "prior_bot_exit_wallet"),
        (&p.mint, "prior_bot_exit_mint"),
    ] {
        crate::execution_pumpswap_accounts::parse_pubkey(key, label)?;
    }
    ensure!(
        bs58::decode(&p.buy_receipt_signature).into_vec()?.len() == 64,
        "prior_bot_exit_buy_signature"
    );
    let previous = SqliteStore::open_read_only(&p.state_db_path)?;
    let closed = previous.prior_bot_receipt_exit_closed(&PriorBotReceiptExitBinding {
        exit_intent_id: p.exit_intent_id.clone(),
        buy_order_id: p.buy_order_id.clone(),
        buy_receipt_signature: p.buy_receipt_signature.clone(),
        position_id: p.position_id.clone(),
        wallet_pubkey: p.wallet_pubkey.clone(),
        mint: p.mint.clone(),
        amount_raw: p.amount_raw,
        decimals: p.decimals,
    })?;
    if !closed {
        return Ok(false);
    }
    let (_, _, old) = previous
        .controlled_receipt_lot_scope()?
        .context("prior_bot_exit_old_scope")?;
    let Some(tiny) = c.tiny_experiment.id.as_deref() else {
        return Ok(false);
    };
    let Some(scope) = c.controlled_receipt_lot.as_ref() else {
        return Ok(false);
    };
    if scope.source_buy_intent_id == old.source_buy_intent_id
        || c.owner_technical_buy
            .as_ref()
            .is_some_and(|p| p.intent_id == old.source_buy_intent_id)
    {
        return Ok(false);
    }
    if c.owner_technical_buy.is_none() && c.technical_cohort.is_none() {
        return Ok(false);
    }
    let current = [
        Some(tiny),
        c.owner_technical_buy.as_ref().map(|p| p.run_id.as_str()),
        c.technical_cohort.as_ref().map(|p| p.run_id.as_str()),
        Some(scope.source_run_id.as_str()),
        Some(scope.bot_run_id.as_str()),
    ];
    Ok(current
        .into_iter()
        .flatten()
        .all(|run| !run.is_empty() && run != old.source_run_id && run != old.bot_run_id))
}
