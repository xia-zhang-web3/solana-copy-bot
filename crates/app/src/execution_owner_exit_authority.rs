//! A durable owner exit is bound to the original confirmed BUY and one position.
use crate::execution_submit_adapter::{ExecutionSubmitRequest, ExecutionTransactionPlan};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::{ExecutionConfig, OwnerExitConfig};
use copybot_storage_core::{OwnerExitIntent, SqliteStore};

pub(crate) fn configured(config: &ExecutionConfig) -> Result<Option<OwnerExitIntent>> {
    let Some(p) = config.owner_exit.as_ref() else { return Ok(None); };
    copybot_config::validate_owner_exit(config)?;
    Ok(Some(from_config(p, config)?))
}

fn from_config(p: &OwnerExitConfig, config: &ExecutionConfig) -> Result<OwnerExitIntent> {
    for (key, label) in [(&p.wallet_pubkey, "owner_exit_wallet"),
        (&p.signer_pubkey, "owner_exit_signer"),
        (&p.genesis_hash, "owner_exit_genesis"), (&p.mint, "owner_exit_mint")] {
        crate::execution_pumpswap_accounts::parse_pubkey(key, label)?;
    }
    let activated_at = DateTime::parse_from_rfc3339(&p.activated_at)?.with_timezone(&Utc);
    let expires_at = DateTime::parse_from_rfc3339(&p.expires_at)?.with_timezone(&Utc);
    ensure!(activated_at < expires_at, "owner_exit_window");
    if p.policy == copybot_config::OWNER_SOURCE_RECOVERY_EXIT_V1 {
        ensure!(expires_at.signed_duration_since(activated_at) <= chrono::Duration::seconds(300),
            "source_recovery_window_cap");
    }
    let mut payload = serde_json::json!({
        "policy": p.policy, "run_id": p.run_id, "intent_id": p.intent_id,
        "buy_order_id": p.buy_order_id, "buy_receipt_signature": p.buy_receipt_signature,
        "position_id": p.position_id, "wallet": p.wallet_pubkey,
        "signer": p.signer_pubkey, "genesis_hash": p.genesis_hash,
        "mint": p.mint, "amount_raw": p.amount_raw, "decimals": p.decimals,
        "route": p.route, "activated_at": activated_at, "expires_at": expires_at,
        "max_priority_fee_lamports": p.max_priority_fee_lamports,
        "min_reserve_lamports": p.min_reserve_lamports,
        "max_slippage_bps": p.max_slippage_bps,
        "max_daily_loss_lamports": p.max_daily_loss_lamports,
    });
    if let Some(gate) = config.owner_technical_buy.as_ref().and_then(|p|p.source_copy_exit_gate.as_ref()) {
        payload["source_copy_exit_gate"] = serde_json::to_value(gate)?;
    }
    Ok(OwnerExitIntent {
        intent_id: p.intent_id.clone(), run_id: p.run_id.clone(),
        buy_order_id: p.buy_order_id.clone(),
        buy_receipt_signature: p.buy_receipt_signature.clone(),
        position_id: p.position_id.clone(), wallet: p.wallet_pubkey.clone(),
        signer: p.signer_pubkey.clone(), genesis_hash: p.genesis_hash.clone(),
        mint: p.mint.clone(), amount_raw: p.amount_raw, decimals: p.decimals,
        route: p.route.clone(), activated_at, expires_at,
        authority_sha256: crate::execution_owned_sell_rpc::digest(serde_json::to_vec(&payload)?),
        max_priority_fee_lamports: p.max_priority_fee_lamports,
        min_reserve_lamports: p.min_reserve_lamports,
        max_slippage_bps: u32::try_from(p.max_slippage_bps)?,
        max_daily_loss_lamports: p.max_daily_loss_lamports,
    })
}

pub(crate) fn current(
    config: &ExecutionConfig, store: &SqliteStore,
    expected: &OwnerExitIntent, now: DateTime<Utc>,
) -> Result<()> {
    ensure!(config.owner_exit.as_ref().is_some_and(|p| p.activate),
        "owner_exit_authority_off");
    ensure!(configured(config)?.as_ref() == Some(expected), "owner_exit_config_changed");
    ensure!(store.load_owner_exit_intent(&expected.intent_id)?.as_ref() == Some(expected),
        "owner_exit_intent_changed");
    ensure!(now >= expected.activated_at && now < expected.expires_at,
        "owner_exit_expired");
    ensure!(!std::path::Path::new(&config.canary_kill_switch_path).exists(),
        "owner_exit_kill_switch");
    if config.owner_exit.as_ref().is_some_and(|p|p.policy == copybot_config::OWNER_SOURCE_RECOVERY_EXIT_V1) {
        let parent=crate::execution_owner_buy_authority::parent_identity(config)?;
        ensure!(store.load_owner_technical_buy_intent(&parent.intent_id)?.as_ref() == Some(&parent),
            "source_recovery_parent_changed");
        let scope=crate::execution_controlled_receipt_lot::scope(config)?.context("source_recovery_scope")?;
        ensure!(store.controlled_receipt_lot_scope()?.as_ref()
            == Some(&(expected.wallet.clone(),expected.run_id.clone(),scope)),"source_recovery_scope_changed");
        store.owner_source_recovery_native_remaining(expected)?;
    } else {
        ensure!(crate::execution_owner_source_gate::ready(config,store,now)?, "source_copy_gate_hold");
    }
    let position = store.load_execution_canary_open_position(&expected.mint)?
        .context("owner_exit_position_missing")?;
    ensure!(position.position_id == expected.position_id
        && position.qty_exact.is_some_and(|q| q.raw() == expected.amount_raw
            && q.decimals() == expected.decimals),
        "owner_exit_position_changed");
    Ok(())
}

pub(crate) fn request(
    store: &SqliteStore, r: &ExecutionSubmitRequest, statuses: &[&str],
) -> Result<Option<OwnerExitIntent>> {
    let Some(id) = r.signal_id.strip_prefix("owner-exit:") else { return Ok(None); };
    let intent = store.load_owner_exit_intent(id)?.context("owner_exit_intent_missing")?;
    let order = store.load_execution_canary_order(&r.order_id)?
        .context("owner_exit_order_missing")?;
    ensure!(matches!(store.execution_order_origin(&r.order_id)?,
        Some(copybot_storage_core::ExecutionOrderOrigin::OwnerExit { intent_id })
            if intent_id == id), "owner_exit_origin_changed");
    ensure!(order.order_id == copybot_storage_core::owner_exit_order_id(id)
        && order.signal_id == r.signal_id && order.client_order_id == r.client_order_id
        && order.route == r.route && order.attempt == r.attempt
        && statuses.contains(&order.status.as_str())
        && order.tx_signature.as_deref().is_none_or(str::is_empty)
        && r.client_order_id == copybot_storage_core::owner_exit_client_order_id(id)
        && r.route == intent.route && r.wallet_pubkey == intent.wallet
        && r.wallet_id == intent.wallet && r.token == intent.mint
        && r.side == "sell" && r.slippage_tolerance_bps <= u64::from(intent.max_slippage_bps),
        "owner_exit_request_identity");
    ensure!(r.metadata.quote_in_amount_raw.as_deref()
        == Some(intent.amount_raw.to_string().as_str()), "owner_exit_amount_changed");
    Ok(Some(intent))
}

pub(crate) fn fresh_quote(r: &ExecutionSubmitRequest, now: DateTime<Utc>) -> Result<()> {
    let available = r.metadata.quote_response_available_ts
        .context("owner_exit_quote_time_missing")?;
    let age = now.signed_duration_since(available);
    ensure!((0..=30_000).contains(&age.num_milliseconds()),
        "owner_exit_quote_stale");
    Ok(())
}

pub(crate) fn plan(store: &SqliteStore, r: &ExecutionSubmitRequest,
    p: &ExecutionTransactionPlan) -> Result<bool> {
    let Some(intent) = request(store, r,
        &[copybot_storage_core::EXECUTION_STATUS_CANARY_SIMULATED])? else { return Ok(false); };
    let blueprint = p.swap_blueprint.as_ref().context("owner_exit_blueprint_missing")?;
    ensure!(p.order_id == r.order_id && p.signal_id == r.signal_id
        && p.client_order_id == r.client_order_id && p.attempt == r.attempt
        && p.route == r.route && p.token == r.token && p.side == r.side
        && p.wallet_pubkey == r.wallet_pubkey && p.metadata == r.metadata
        && p.slippage_tolerance_bps == r.slippage_tolerance_bps
        && blueprint.wallet_pubkey.as_deref() == Some(intent.wallet.as_str())
        && blueprint.input_mint == intent.mint
        && blueprint.output_mint == crate::execution_quote_canary_helpers::SOL_MINT
        && blueprint.input_amount_raw == intent.amount_raw.to_string()
        && Some(blueprint.output_amount_raw.as_str())
            == r.metadata.quote_out_amount_raw.as_deref(),
        "owner_exit_plan_identity");
    Ok(true)
}
