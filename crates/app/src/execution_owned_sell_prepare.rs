//! Actual strict runner: unsigned ownership, then explicit guarded tiny continuation.
#[path = "execution_owned_sell_submit.rs"]
pub(crate) mod submit;
use crate::execution_owned_sell_rpc as rpc;
use crate::execution_submit_adapter::{
    ExecutionBuildPlanMetadata, ExecutionSubmitAdapter, ExecutionSubmitRequest,
    JupiterMetisDryRunExecutionAdapter,
};
use anyhow::{ensure, Context, Result};
use chrono::Utc;
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    association_inbox::InboxLimits,
    ordered_sell_quote::{QuoteObservation, QuoteOutcome},
    SqliteStore,
};
use serde_json::json;

pub(crate) async fn run(
    store: &mut SqliteStore,
    c: &ExecutionConfig,
    l: InboxLimits,
    q: QuoteObservation,
    body: String,
    live: submit::guard::Live,
) -> Result<()> {
    ensure!(
        copybot_config::owned_sell_flags(c),
        "owned_sell_unsigned_only_flags"
    );
    if q.outcome != QuoteOutcome::Current {
        return Ok(());
    }
    let b = q.binding.as_ref().context("owned_sell_quote_binding")?;
    ensure!(
        rpc::fractional::enabled(c) == b.fractional.is_some(),
        "fraction_contract_binding"
    );
    if let Some(d) = &b.fractional {
        ensure!(
            d.producer_identity == rpc::identity(c)?,
            "fraction_producer_changed"
        );
    }
    if store.has_owned_sell_handoff(&b.intent_id)? {
        return Ok(());
    }
    ensure!(
        q.response_sha256.as_deref() == Some(&rpc::digest(&body)),
        "owned_sell_quote_body_binding"
    );
    let s = store.owned_sell_snapshot(b, l)?;
    let e = c
        .tiny_experiment
        .id
        .as_deref()
        .context("tiny_budget_inactive")?;
    let protected =
        c.tiny_experiment.policy_mode == copybot_config::TinyPolicyMode::ProtectedNativeCapital;
    let selection = crate::execution_source_sell_guard::amount::Selection::new(
        &store
            .load_execution_canary_open_position(&b.mint)?
            .context("owned_sell_position_missing")?,
    )?;
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let mut check = || {
        let store = &mut *store;
        ensure!(
            live.0.load(std::sync::atomic::Ordering::SeqCst),
            "owned_sell_runner_cancelled"
        );
        ensure!(
            !std::path::Path::new(&c.canary_kill_switch_path).exists(),
            "kill_switch_active"
        );
        ensure!(
            store.owned_sell_snapshot(b, l)? == s,
            "owned_sell_snapshot_changed"
        );
        let fresh = store
            .load_strict_sell_quote(&b.intent_id, l, Utc::now())?
            .context("owned_sell_quote_missing")?;
        ensure!(
            fresh == q && fresh.outcome == QuoteOutcome::Current,
            "owned_sell_quote_stale"
        );
        selection.recheck(store)?;
        store.check_owned_sell_budget_policy(
            e,
            &c.canary_wallet_pubkey,
            &b.mint,
            &b.position_id,
            protected,
            Utc::now(),
        )?;
        Ok(())
    };
    let rpc_deadline = q.http_started.context("owned_sell_quote_clock")?
        + chrono::Duration::milliseconds(
            copybot_storage_core::ordered_sell_quote::MAX_QUOTE_AGE_MS,
        );
    let left = (rpc_deadline - Utc::now())
        .to_std()
        .context("owned_sell_deadline")?;
    let authority = tokio::time::timeout(left, rpc::collect(&http, c, &s, &mut check))
        .await
        .context("owned_sell_deadline")??;
    check()?;
    let h = store.reserve_owned_sell_handoff(
        &s,
        &q,
        l,
        &rpc::identity(c)?,
        &authority.binding(c, &s)?,
        e,
        &c.canary_wallet_pubkey,
        Utc::now,
    )?;
    let experiment = if copybot_config::owned_sell_dispatch(c) {
        store.load_tiny_experiment(Utc::now())?;
        Some(store.owned_sell_experiment_snapshot()?)
    } else {
        None
    };
    let value: serde_json::Value = serde_json::from_str(&body)?;
    let cap = c
        .pretrade_max_priority_fee_lamports
        .min(22_000)
        .min(copybot_storage_core::TINY_PRIORITY_FEE);
    ensure!(cap > 0, "owned_sell_priority_cap_required");
    let request=ExecutionSubmitRequest{
        order_id:h.order_id.clone(),signal_id:h.intent_id.clone(),client_order_id:h.owner.clone(),attempt:1,
        route:c.canary_route.clone(),wallet_id:b.source_wallet.clone(),token:b.mint.clone(),side:"sell".into(),
        buy_size_sol:0.0,slippage_tolerance_bps:crate::execution_quote_canary_helpers::quote_canary_slippage_limit_bps(c,"sell"),wallet_pubkey:c.canary_wallet_pubkey.clone(),entry_route_plan_json:None,
        metadata:ExecutionBuildPlanMetadata{quote_event_id:Some(h.intent_id.clone()),quote_source:Some(b.provider.clone()),quote_request_ts:q.http_started,http_request_started_ts:q.http_started,quote_response_available_ts:q.quote_response_available_ts,quote_status:Some("ok".into()),quote_in_amount_raw:q.response_in_raw.clone(),quote_out_amount_raw:q.response_out_raw.clone(),quote_response_json:Some(body),route_plan_json:Some(value["routePlan"].to_string()),priority_fee_status:Some("ok".into()),priority_fee_source:Some("configured_cap".into()),priority_fee_lamports:Some(cap),priority_fee_json:Some(json!({"version":1,"source":"configured_cap","unit":"total_priority_fee_lamports","value":cap}).to_string()),..Default::default()},
    };
    let plan =
        JupiterMetisDryRunExecutionAdapter::new(c.clone()).build_transaction_plan(&request)?;
    ensure!(
        !plan.submit_enabled
            && plan
                .swap_blueprint
                .as_ref()
                .is_some_and(|p| p.input_amount_raw == b.raw.to_string()),
        "owned_sell_preparation_amount"
    );
    let mut check = || {
        let store = &mut *store;
        ensure!(
            live.0.load(std::sync::atomic::Ordering::SeqCst),
            "owned_sell_runner_cancelled"
        );
        if let Some(expected) = &experiment {
            ensure!(
                store.owned_sell_experiment_snapshot()? == *expected,
                "owned_sell_experiment_changed"
            );
        }
        ensure!(
            !std::path::Path::new(&c.canary_kill_switch_path).exists(),
            "kill_switch_active"
        );
        selection.recheck(store)?;
        store.check_owned_sell_budget_policy(
            e,
            &c.canary_wallet_pubkey,
            &b.mint,
            &b.position_id,
            protected,
            Utc::now(),
        )?;
        store.recheck_owned_sell_handoff(&h, l, Utc::now())
    };
    let left = (h.deadline - Utc::now())
        .to_std()
        .context("owned_sell_deadline")?;
    let prepared = tokio::time::timeout(
        left,
        crate::execution_guarded_generic_sell::prepare_owned(
            &http, c, &plan, &authority, &s, &mut check,
        ),
    )
    .await
    .context("owned_sell_deadline")??;
    check()?;
    let (message, priority) = crate::execution_priority_fee_wire::decode_priority_fee_message(
        &prepared.serialized_transaction_base64,
    )?;
    ensure!(priority.total <= cap, "owned_sell_priority_fee_cap");
    let fee = crate::execution_native_rpc::NativeFundingRpcClient::new()?
        .collect_fee_only(
            rpc::endpoint(c)?.as_str(),
            (h.deadline - Utc::now())
                .to_std()
                .context("owned_sell_deadline")?,
            &prepared.serialized_transaction_base64,
        )
        .await?;
    check()?;
    let (total, slot) = fee.bound_fee(&message.binding)?;
    if copybot_config::owned_sell_dispatch(c) {
        ensure!(slot >= s.sell.facts.slot, "owned_sell_fee_stale");
        ensure!(
            live.0.load(std::sync::atomic::Ordering::SeqCst),
            "owned_sell_runner_cancelled"
        );
    }
    // The common native-floor policy deliberately applies only to BUY. Calling it
    // here preserves that decision rather than inventing a SELL floor/budget.
    crate::execution_native_floor_policy::verify_submit_payload(
        &request,
        &prepared.serialized_transaction_base64,
        c.pretrade_min_sol_reserve,
        &c.execution_signer_pubkey,
    )?;
    store.complete_owned_sell_handoff(
        &h,
        l,
        &prepared.serialized_transaction_base64,
        &message.binding.message_sha256,
        total,
        priority.total,
        Utc::now,
    )?;
    if copybot_config::owned_sell_dispatch(c) {
        submit::run(
            store,
            c,
            request,
            copybot_storage_core::rpc_owned_sell_handoff::dispatch::Prepared {
                experiment: experiment.context("owned_sell_experiment_missing")?,
                handoff: h,
                limits: (l.count, l.bytes, l.busy_ms),
                payload: prepared.serialized_transaction_base64,
                message_sha256: message.binding.message_sha256,
                total_fee: total,
                priority_fee: priority.total,
            },
            &authority,
            live,
        )
        .await?;
    }
    Ok(())
}
