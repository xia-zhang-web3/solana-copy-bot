//! Explicit owner intent checks. A copy signal can never authorize this lane.
use crate::execution_submit_adapter::{ExecutionSubmitRequest, ExecutionTransactionPlan};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::{ExecutionConfig, OwnerTechnicalBuyConfig};
use copybot_storage_core::{OwnerTechnicalBuyIntent, SqliteStore};

pub(crate) fn configured(config: &ExecutionConfig) -> Result<Option<OwnerTechnicalBuyIntent>> {
    let Some(p) = config.owner_technical_buy.as_ref() else { return Ok(None); };
    copybot_config::validate_owner_technical_buy(config)?;
    Ok(Some(from_config(p)?))
}

fn from_config(p: &OwnerTechnicalBuyConfig) -> Result<OwnerTechnicalBuyIntent> {
    for (key, label) in [(&p.wallet_pubkey, "owner_buy_wallet"),
        (&p.signer_pubkey, "owner_buy_signer"),
        (&p.genesis_hash, "owner_buy_genesis"), (&p.mint, "owner_buy_mint")] {
        crate::execution_pumpswap_accounts::parse_pubkey(key, label)?;
    }
    let activated_at = DateTime::parse_from_rfc3339(&p.activated_at)?.with_timezone(&Utc);
    let expires_at = DateTime::parse_from_rfc3339(&p.expires_at)?.with_timezone(&Utc);
    ensure!(activated_at < expires_at, "owner_buy_window");
    let payload = serde_json::to_vec(&serde_json::json!({
        "policy": p.policy, "run_id": p.run_id, "intent_id": p.intent_id,
        "wallet": p.wallet_pubkey, "signer": p.signer_pubkey,
        "genesis_hash": p.genesis_hash, "mint": p.mint,
        "amount_lamports": p.amount_lamports, "route": p.route,
        "activated_at": activated_at, "expires_at": expires_at,
        "max_priority_fee_lamports": p.max_priority_fee_lamports,
        "min_reserve_lamports": p.min_reserve_lamports,
        "max_slippage_bps": p.max_slippage_bps,
        "max_daily_loss_lamports": p.max_daily_loss_lamports,
        "max_open_positions": p.max_open_positions,
        "max_buy_count": p.max_buy_count,
    }))?;
    let authority_sha256 = crate::execution_owned_sell_rpc::digest(payload);
    Ok(OwnerTechnicalBuyIntent {
        intent_id: p.intent_id.clone(),
        run_id: p.run_id.clone(),
        wallet: p.wallet_pubkey.clone(),
        signer: p.signer_pubkey.clone(),
        genesis_hash: p.genesis_hash.clone(),
        mint: p.mint.clone(),
        amount_lamports: p.amount_lamports,
        route: p.route.clone(),
        activated_at,
        expires_at,
        authority_sha256,
        max_priority_fee_lamports: p.max_priority_fee_lamports,
        min_reserve_lamports: p.min_reserve_lamports,
        max_slippage_bps: u32::try_from(p.max_slippage_bps)?,
        max_daily_loss_lamports: p.max_daily_loss_lamports,
        max_open_positions: p.max_open_positions,
        max_buy_count: p.max_buy_count,
    })
}

pub(crate) fn current(
    config: &ExecutionConfig,
    store: &SqliteStore,
    expected: &OwnerTechnicalBuyIntent,
    now: DateTime<Utc>,
) -> Result<()> {
    ensure!(config.owner_technical_buy.as_ref().is_some_and(|p| p.activate),
        "owner_buy_authority_off");
    ensure!(configured(config)?.as_ref() == Some(expected), "owner_buy_config_changed");
    ensure!(store.load_owner_technical_buy_intent(&expected.intent_id)?.as_ref() == Some(expected),
        "owner_buy_intent_changed");
    ensure!(now >= expected.activated_at && now < expected.expires_at,
        "owner_buy_expired");
    ensure!(!std::path::Path::new(&config.canary_kill_switch_path).exists(),
        "owner_buy_kill_switch");
    Ok(())
}

pub(crate) fn request(
    store: &SqliteStore,
    r: &ExecutionSubmitRequest,
    statuses: &[&str],
) -> Result<Option<OwnerTechnicalBuyIntent>> {
    let Some(id) = r.signal_id.strip_prefix("owner-buy:") else { return Ok(None); };
    let intent = store.load_owner_technical_buy_intent(id)?
        .context("owner_buy_intent_missing")?;
    let order = store.load_execution_canary_order(&r.order_id)?
        .context("owner_buy_order_missing")?;
    ensure!(matches!(store.execution_order_origin(&r.order_id)?,
        Some(copybot_storage_core::ExecutionOrderOrigin::OwnerTechnicalBuy { intent_id })
            if intent_id == id), "owner_buy_origin_changed");
    ensure!(order.order_id == format!("exec-canary:owner-buy:{id}")
        && order.signal_id == r.signal_id
        && order.client_order_id == r.client_order_id
        && order.route == r.route
        && order.attempt == r.attempt
        && statuses.contains(&order.status.as_str())
        && order.tx_signature.as_deref().is_none_or(str::is_empty)
        && r.client_order_id == format!("copybot:owner-buy:{id}")
        && r.route == intent.route
        && r.wallet_pubkey == intent.wallet
        && r.wallet_id == intent.wallet
        && r.token == intent.mint
        && r.side == "buy"
        && (r.buy_size_sol * 1_000_000_000.0).round() == intent.amount_lamports as f64
        && r.slippage_tolerance_bps <= u64::from(intent.max_slippage_bps),
        "owner_buy_request_identity");
    Ok(Some(intent))
}

pub(crate) fn fresh_quote(r: &ExecutionSubmitRequest, now: DateTime<Utc>) -> Result<()> {
    let available = r.metadata.quote_response_available_ts
        .context("owner_buy_quote_time_missing")?;
    let age = now.signed_duration_since(available);
    ensure!((0..=30_000).contains(&age.num_milliseconds()),
        "owner_buy_quote_stale");
    let expected_amount = (r.buy_size_sol * 1_000_000_000.0).round().to_string();
    ensure!(r.metadata.quote_in_amount_raw.as_deref()
        == Some(expected_amount.as_str()),
        "owner_buy_quote_amount_changed");
    Ok(())
}

pub(crate) fn plan(
    store: &SqliteStore,
    r: &ExecutionSubmitRequest,
    p: &ExecutionTransactionPlan,
) -> Result<bool> {
    let Some(_) = request(store, r, &[copybot_storage_core::EXECUTION_STATUS_CANARY_SIMULATED])?
    else { return Ok(false); };
    let blueprint = p.swap_blueprint.as_ref().context("owner_buy_plan_blueprint_missing")?;
    ensure!(p.order_id == r.order_id && p.signal_id == r.signal_id
        && p.client_order_id == r.client_order_id && p.attempt == r.attempt
        && p.route == r.route && p.token == r.token && p.side == r.side
        && p.wallet_pubkey == r.wallet_pubkey && p.metadata == r.metadata
        && p.buy_size_sol == r.buy_size_sol
        && p.slippage_tolerance_bps == r.slippage_tolerance_bps
        && blueprint.wallet_pubkey.as_deref() == Some(r.wallet_pubkey.as_str())
        && blueprint.input_mint == crate::execution_quote_canary_helpers::SOL_MINT
        && blueprint.output_mint == r.token
        && Some(blueprint.input_amount_raw.as_str()) == r.metadata.quote_in_amount_raw.as_deref()
        && Some(blueprint.output_amount_raw.as_str()) == r.metadata.quote_out_amount_raw.as_deref()
        && blueprint.slippage_bps == r.slippage_tolerance_bps as f64,
        "owner_buy_plan_identity");
    Ok(true)
}
