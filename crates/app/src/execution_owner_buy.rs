//! Default-off one-use technical BUY on the daemon tick, independent of Discovery.
use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use crate::execution_canary_submit_contract::ExecutionTinySubmitGate;
use crate::execution_submit_adapter::{
    ExecutionBuildPlanMetadata, ExecutionSubmitAdapter, ExecutionSubmitRequest,
    JupiterMetisDryRunExecutionAdapter, RpcExecutionSubmitTransport,
};
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    owner_technical_buy_identity_id, owner_technical_buy_order_id,
    ExecutionCanaryRecordOutcome, OwnerTechnicalBuyIntent, SqliteStore,
};

pub(crate) async fn tick(
    config: &ExecutionConfig,
    store: &SqliteStore,
    now: DateTime<Utc>,
) -> Result<ExecutionCanaryStateMachineSummary> {
    let adapter = JupiterMetisDryRunExecutionAdapter::new(config.clone());
    tick_with_adapter(config, store, now, &adapter).await
}

pub(crate) async fn tick_with_adapter<A: ExecutionSubmitAdapter>(
    config: &ExecutionConfig,
    store: &SqliteStore,
    now: DateTime<Utc>,
    adapter: &A,
) -> Result<ExecutionCanaryStateMachineSummary> {
    let mut summary = ExecutionCanaryStateMachineSummary::default();
    recover(config, store, now, &mut summary).await?;
    let Some(policy) = config.owner_technical_buy.as_ref() else { return Ok(summary); };
    if !policy.activate { return Ok(summary); }
    let intent = crate::execution_owner_buy_authority::configured(config)?
        .expect("active owner policy");
    // A registered intent is immutable. A process restart can only observe the
    // canonical order or receipt obligation, never acquire a new send slot.
    store.register_owner_technical_buy_intent(&intent)?;
    if let Some(order) = store.load_execution_canary_order(&owner_technical_buy_order_id(&intent.intent_id))? {
        summary.existing = 1;
        summary.last_order_id = Some(order.order_id);
        return Ok(summary);
    }
    if let Err(error) = crate::execution_owner_buy_authority::current(config, store, &intent, now) {
        summary.skipped_reason = Some("owner_buy_authority_inactive");
        summary.last_error = Some(error.to_string());
        return Ok(summary);
    }
    summary.candidates = 1;
    let safety = crate::execution_canary_safety::live_pre_submit_safety_snapshot(config, store, now)?;
    summary.open_positions = safety.open_positions;
    summary.daily_loss_sol = safety.daily_loss_sol;
    summary.entry_cost = safety.entry_cost;
    if let Some(reason) = safety.blocked_reason {
        summary.buy_blocker = safety.buy_blocker;
        summary.safety_blocked = 1;
        summary.skipped_reason = Some(reason);
        return Ok(summary);
    }
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()?;
    let check = || crate::execution_owner_buy_authority::current(
        config, store, &intent,
        crate::execution_canary_safety::risk_clock::decision_time(now)
            .ok_or_else(|| anyhow::anyhow!("owner_buy_clock"))?,
    );
    let decimals = match crate::execution_owner_buy_rpc::verify(
        &http, &config.submit_adapter_http_url, config.submit_timeout_ms,
        &intent, &check,
    ).await {
        Ok(value) => value,
        Err(error) => {
            summary.skipped_reason = Some("owner_buy_chain_or_mint_refused");
            summary.last_error = Some(error.to_string());
            return Ok(summary);
        }
    };
    let metadata = match crate::execution_owner_buy_quote::fetch(
        &http, config, &intent, decimals,
    ).await {
        Ok(value) => value,
        Err(error) => {
            summary.skipped_reason = Some("owner_buy_quote_refused");
            summary.last_error = Some(error.to_string());
            return Ok(summary);
        }
    };
    check()?;
    execute(config, store, &intent, now, adapter, metadata, &mut summary).await?;
    Ok(summary)
}

pub(crate) async fn execute<A: ExecutionSubmitAdapter>(
    config: &ExecutionConfig,
    store: &SqliteStore,
    intent: &OwnerTechnicalBuyIntent,
    now: DateTime<Utc>,
    adapter: &A,
    metadata: ExecutionBuildPlanMetadata,
    summary: &mut ExecutionCanaryStateMachineSummary,
) -> Result<()> {
    crate::execution_owner_buy_authority::current(config, store, intent, now)?;
    if let Some(reason) = crate::execution_canary_entry_gate::validate_execution_canary_entry_metadata(config, &metadata) {
        summary.entry_gate_blocked = 1;
        summary.skipped_reason = Some(reason);
        return Ok(());
    }
    // The owner lane alone activates its pinned tiny budget after the chain,
    // mint, quote, and entry checks. The storage call is idempotent and never
    // resets a stopped or already used experiment on restart.
    let activation = crate::execution_canary_safety::risk_clock::decision_time(now)
        .ok_or_else(|| anyhow::anyhow!("owner_buy_clock"))?;
    crate::execution_owner_buy_authority::current(config, store, intent, activation)?;
    if !crate::execution_native_floor_policy::protected::enabled(config) {
        let experiment = store.activate_tiny_experiment(&intent.run_id, &intent.wallet, activation)?;
        ensure!(experiment.state == "active", "owner_buy_tiny_budget_stopped");
    }
    let reserve = store.reserve_owner_technical_buy_order(&intent.intent_id, || {
        crate::execution_canary_safety::risk_clock::decision_time(now)
            .ok_or_else(|| anyhow::anyhow!("owner_buy_clock"))
    })?;
    summary.last_order_id = Some(reserve.order.order_id.clone());
    if reserve.outcome == ExecutionCanaryRecordOutcome::Existing {
        summary.existing = 1;
        return Ok(());
    }
    summary.reserved = 1;
    let mut request = ExecutionSubmitRequest {
        order_id: reserve.order.order_id.clone(),
        signal_id: owner_technical_buy_identity_id(&intent.intent_id),
        client_order_id: reserve.order.client_order_id,
        attempt: reserve.order.attempt,
        route: intent.route.clone(),
        wallet_id: intent.wallet.clone(),
        token: intent.mint.clone(),
        side: "buy".into(),
        buy_size_sol: intent.amount_lamports as f64 / 1_000_000_000.0,
        slippage_tolerance_bps: u64::from(intent.max_slippage_bps),
        wallet_pubkey: intent.wallet.clone(),
        entry_route_plan_json: None,
        metadata,
    };
    crate::execution_native_floor_policy::protected::prepare_request(
        store, config, &mut request, now,
    ).await?;
    crate::execution_owner_buy_authority::current(config, store, intent,
        crate::execution_canary_safety::risk_clock::decision_time(now)
            .ok_or_else(|| anyhow::anyhow!("owner_buy_clock"))?)?;
    let Some(envelope) = crate::execution_canary_route::build_simulated_signed_envelope(
        store, adapter, &request, now, summary, None,
    ).await? else { return Ok(()); };
    crate::execution_owner_buy_authority::current(config, store, intent,
        crate::execution_canary_safety::risk_clock::decision_time(now)
            .ok_or_else(|| anyhow::anyhow!("owner_buy_clock"))?)?;
    let gate = ExecutionTinySubmitGate::from_config(config);
    ensure!(gate.allow_rpc_submit, "owner_buy_submit_disabled");
    let transport = RpcExecutionSubmitTransport::new(config.submit_adapter_http_url.clone());
    let outcome = crate::execution_submit_adapter::record_execution_tiny_submit_confirm_path_guarded(
        store, adapter, &request, &envelope, &gate, &transport,
        &reqwest::Client::new(), &config.submit_adapter_http_url, now,
        config.max_confirm_seconds.saturating_mul(1_000).max(1), None,
    ).await?;
    crate::execution_canary_route::apply_tiny_submit_confirm_path_outcome(summary, outcome);
    Ok(())
}

async fn recover(
    config: &ExecutionConfig,
    store: &SqliteStore,
    now: DateTime<Utc>,
    summary: &mut ExecutionCanaryStateMachineSummary,
) -> Result<()> {
    for intent in store.list_owner_technical_buy_recovery_intents()? {
        let order_id = owner_technical_buy_order_id(&intent.intent_id);
        let Some(order) = store.load_execution_canary_order(&order_id)? else { continue; };
        let Some(dispatch) = store.load_execution_canary_dispatch(&order_id)? else { continue; };
        ensure!(dispatch.wallet == intent.wallet && dispatch.token == intent.mint
            && dispatch.signal_id == owner_technical_buy_identity_id(&intent.intent_id)
            && dispatch.side == "buy" && order.signal_id == dispatch.signal_id,
            "owner_buy_recovery_identity");
        if store.load_failed_expense_task(&order_id)?
            .is_some_and(|task| task.status == "pending") {
            crate::execution_submit_adapter::recover_failed_expense_order(
                store, &reqwest::Client::new(), &config.submit_adapter_http_url,
                &order_id, &intent.wallet, now,
                config.max_confirm_seconds.saturating_mul(1_000).max(1),
            ).await?;
            summary.existing += 1;
            summary.last_order_id = Some(order_id);
            continue;
        }
        if !matches!(order.status.as_str(),
            copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMITTED
            | copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED
            | copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED) {
            continue;
        }
        if store.execution_canary_fill_exists(&order_id)? { continue; }
        let mut recovery_config = config.clone();
        recovery_config.canary_wallet_pubkey = intent.wallet.clone();
        let outcome = crate::execution_submit_adapter::reconcile_execution_tiny_submit_confirmation(
            store, &recovery_config, &order_id, &reqwest::Client::new(),
            &config.submit_adapter_http_url, now,
            config.max_confirm_seconds.saturating_mul(1_000).max(1),
        ).await?;
        summary.existing += 1;
        summary.last_order_id = Some(order_id);
        crate::execution_canary_route::apply_tiny_submit_confirm_path_outcome(summary, outcome);
    }
    Ok(())
}
