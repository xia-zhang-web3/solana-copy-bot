//! One-use owner USDC exit on the ordinary daemon execution path.
use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use crate::execution_canary_submit_contract::ExecutionTinySubmitGate;
use crate::execution_submit_adapter::{
    ExecutionBuildPlanMetadata, ExecutionSubmitAdapter, ExecutionSubmitRequest,
    JupiterMetisDryRunExecutionAdapter, RpcExecutionSubmitTransport,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    owner_exit_identity_id, owner_exit_order_id, ExecutionCanaryRecordOutcome,
    OwnerExitIntent, SqliteStore,
};

pub(crate) async fn tick(config: &ExecutionConfig, store: &SqliteStore,
    now: DateTime<Utc>) -> Result<ExecutionCanaryStateMachineSummary> {
    let adapter = JupiterMetisDryRunExecutionAdapter::new(config.clone());
    tick_with_adapter(config, store, now, &adapter).await
}

pub(crate) async fn tick_with_adapter<A: ExecutionSubmitAdapter>(
    config: &ExecutionConfig, store: &SqliteStore, now: DateTime<Utc>, adapter: &A,
) -> Result<ExecutionCanaryStateMachineSummary> {
    let mut summary = ExecutionCanaryStateMachineSummary::default();
    recover(config, store, now, &mut summary).await?;
    let Some(policy) = config.owner_exit.as_ref() else { return Ok(summary); };
    if !policy.activate { return Ok(summary); }
    let intent = crate::execution_owner_exit_authority::configured(config)?
        .expect("active owner exit");
    store.register_owner_exit_intent(&intent)?;
    // Classify the copied, confirmed BUY from its preserved receipt and native
    // observations before any SELL quote or dispatch. This only adds evidence;
    // it never rewrites the historical fill or position cost.
    let buy_cash = store.materialize_receipt_cash_components(&intent.buy_order_id, now)?;
    anyhow::ensure!(buy_cash.side == "buy"
        && buy_cash.swap_native_delta_lamports.as_deref() == Some("-10000000")
        && buy_cash.transaction_fee_lamports.as_deref() == Some("5000")
        && buy_cash.target_ata_rent_delta_lamports.as_deref() == Some("1488440")
        && buy_cash.unclassified_native_delta_lamports.as_deref() == Some("0"),
        "owner_exit_buy_cash_unresolved");
    if let Some(order) = store.load_execution_canary_order(&owner_exit_order_id(&intent.intent_id))? {
        let before_dispatch = matches!(order.status.as_str(),
            copybot_storage_core::EXECUTION_STATUS_CANARY_CANDIDATE
            | copybot_storage_core::EXECUTION_STATUS_CANARY_BUILT
            | copybot_storage_core::EXECUTION_STATUS_CANARY_SIMULATED
            | copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED)
            && store.load_execution_canary_dispatch(&order.order_id)?.is_none();
        if !before_dispatch || (order.status == copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED
            && !store.owner_exit_failed_retry_available(&intent.intent_id)?) {
            summary.existing = 1;
            summary.last_order_id = Some(order.order_id);
            return Ok(summary);
        }
    }
    if let Err(error) = crate::execution_owner_exit_authority::current(config, store, &intent, now) {
        summary.skipped_reason = Some("owner_exit_authority_inactive");
        summary.last_error = Some(error.to_string());
        return Ok(summary);
    }
    summary.candidates = 1;
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never()).build()?;
    let check = || crate::execution_owner_exit_authority::current(
        config, store, &intent,
        crate::execution_canary_safety::risk_clock::decision_time(now)
            .ok_or_else(|| anyhow::anyhow!("owner_exit_clock"))?,
    );
    if let Err(error) = crate::execution_owner_exit_quote::verify_balance(
        &http, config, &intent, &check).await {
        summary.skipped_reason = Some("owner_exit_chain_or_balance_refused");
        summary.last_error = Some(error.to_string());
        return Ok(summary);
    }
    let metadata = match crate::execution_owner_exit_quote::fetch(&http, config, &intent).await {
        Ok(value) => value,
        Err(error) => {
            summary.skipped_reason = Some("owner_exit_quote_refused");
            summary.last_error = Some(error.to_string());
            return Ok(summary);
        }
    };
    check()?;
    execute(config, store, &intent, now, adapter, metadata, &mut summary).await?;
    Ok(summary)
}

pub(crate) async fn execute<A: ExecutionSubmitAdapter>(
    config: &ExecutionConfig, store: &SqliteStore, intent: &OwnerExitIntent,
    now: DateTime<Utc>, adapter: &A, metadata: ExecutionBuildPlanMetadata,
    summary: &mut ExecutionCanaryStateMachineSummary,
) -> Result<()> {
    crate::execution_owner_exit_authority::current(config, store, intent, now)?;
    let reserve = store.reserve_owner_exit_order(&intent.intent_id, || {
        crate::execution_canary_safety::risk_clock::decision_time(now)
            .ok_or_else(|| anyhow::anyhow!("owner_exit_clock"))
    })?;
    summary.last_order_id = Some(reserve.order.order_id.clone());
    if reserve.outcome == ExecutionCanaryRecordOutcome::Existing {
        if !store.rearm_owner_exit_undispatched(&intent.intent_id)? {
            summary.existing = 1;
            return Ok(());
        }
        summary.existing = 1;
    } else {
        summary.reserved = 1;
    }
    let request = ExecutionSubmitRequest {
        order_id: reserve.order.order_id.clone(),
        signal_id: owner_exit_identity_id(&intent.intent_id),
        client_order_id: reserve.order.client_order_id,
        attempt: reserve.order.attempt, route: intent.route.clone(),
        wallet_id: intent.wallet.clone(), token: intent.mint.clone(), side: "sell".into(),
        buy_size_sol: config.canary_buy_size_sol,
        slippage_tolerance_bps: u64::from(intent.max_slippage_bps),
        wallet_pubkey: intent.wallet.clone(), entry_route_plan_json: None, metadata,
    };
    crate::execution_owner_exit_authority::current(config, store, intent,
        crate::execution_canary_safety::risk_clock::decision_time(now)
            .ok_or_else(|| anyhow::anyhow!("owner_exit_clock"))?)?;
    let Some(envelope) = crate::execution_canary_route::build_simulated_signed_envelope(
        store, adapter, &request, now, summary, None).await? else { return Ok(()); };
    crate::execution_owner_exit_authority::current(config, store, intent,
        crate::execution_canary_safety::risk_clock::decision_time(now)
            .ok_or_else(|| anyhow::anyhow!("owner_exit_clock"))?)?;
    let gate = ExecutionTinySubmitGate::from_config(config);
    anyhow::ensure!(gate.allow_rpc_submit, "owner_exit_submit_disabled");
    let transport = RpcExecutionSubmitTransport::new(config.submit_adapter_http_url.clone());
    let outcome = crate::execution_submit_adapter::record_execution_tiny_submit_confirm_path_guarded(
        store, adapter, &request, &envelope, &gate, &transport,
        &reqwest::Client::new(), &config.submit_adapter_http_url, now,
        config.max_confirm_seconds.saturating_mul(1_000).max(1), None,
    ).await?;
    crate::execution_canary_route::apply_tiny_submit_confirm_path_outcome(summary, outcome);
    Ok(())
}

async fn recover(config: &ExecutionConfig, store: &SqliteStore, now: DateTime<Utc>,
    summary: &mut ExecutionCanaryStateMachineSummary) -> Result<()> {
    for intent in store.list_owner_exit_recovery_intents()? {
        let order_id = owner_exit_order_id(&intent.intent_id);
        let Some(order) = store.load_execution_canary_order(&order_id)? else { continue; };
        let Some(dispatch) = store.load_execution_canary_dispatch(&order_id)? else { continue; };
        anyhow::ensure!(dispatch.wallet == intent.wallet && dispatch.token == intent.mint
            && dispatch.signal_id == owner_exit_identity_id(&intent.intent_id)
            && dispatch.side == "sell" && order.signal_id == dispatch.signal_id,
            "owner_exit_recovery_identity");
        if store.load_failed_expense_task(&order_id)?
            .is_some_and(|task| task.status == "pending") {
            crate::execution_submit_adapter::recover_failed_expense_order(
                store, &reqwest::Client::new(), &config.submit_adapter_http_url,
                &order_id, &intent.wallet, now,
                config.max_confirm_seconds.saturating_mul(1_000).max(1)).await?;
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
            config.max_confirm_seconds.saturating_mul(1_000).max(1)).await?;
        summary.existing += 1;
        summary.last_order_id = Some(order_id);
        crate::execution_canary_route::apply_tiny_submit_confirm_path_outcome(summary, outcome);
    }
    Ok(())
}
