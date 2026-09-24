//! Owner-bound reuse of the existing immutable protected native anchor.
use super::{clock, signal_binding, ContextProof, OriginBinding};
use crate::execution_instruction_bundle_binding::BundleRequest;
use crate::execution_submit_adapter::{
    ExecutionSubmitAdapter, ExecutionSubmitRequest, JupiterMetisDryRunExecutionAdapter,
};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{SqliteStore, EXECUTION_STATUS_CANARY_CANDIDATE};
use std::{sync::OnceLock, time::Duration};

pub(super) async fn prepare(
    store: &SqliteStore,
    config: &ExecutionConfig,
    request: &mut ExecutionSubmitRequest,
    tick: DateTime<Utc>,
) -> Result<()> {
    let intent = crate::execution_owner_buy_authority::request(
        store,
        request,
        &[EXECUTION_STATUS_CANARY_CANDIDATE],
    )?
    .context("owner_buy_protected_origin")?;
    let check = || {
        let now = clock(tick)?;
        crate::execution_owner_buy_authority::current(config, store, &intent, now)?;
        Ok(now)
    };
    let now = check()?;
    let plan =
        JupiterMetisDryRunExecutionAdapter::new(config.clone()).build_transaction_plan(request)?;
    let binding = BundleRequest::capture(&plan)?;
    let requested = binding.buy_requested_lamports()?;
    ensure!(
        requested == intent.amount_lamports,
        "owner_buy_protected_amount"
    );
    let reserve = super::super::reserve_lamports(config.pretrade_min_sol_reserve)?;
    ensure!(
        reserve >= intent.min_reserve_lamports,
        "owner_buy_protected_reserve"
    );
    let before = store
        .load_execution_canary_order(&request.order_id)?
        .context("owner_buy_order_missing")?;
    let policy = if store.load_tiny_experiment(now)?.is_some() {
        // A restart or an inflow must never replace the original observation.
        store.tiny_native_policy(&intent.run_id, &intent.wallet, check()?)?
    } else {
        static CLIENT: OnceLock<Result<crate::execution_native_rpc::NativeFundingRpcClient, ()>> =
            OnceLock::new();
        let client = CLIENT
            .get_or_init(|| {
                crate::execution_native_rpc::NativeFundingRpcClient::new().map_err(|_| ())
            })
            .as_ref()
            .map_err(|_| anyhow::anyhow!("owner_buy_protected_client"))?;
        let wallet = crate::execution_pumpswap_accounts::parse_pubkey(
            &intent.wallet,
            "owner_buy_protected_wallet",
        )?;
        let observed = client
            .collect_payer(
                &config.submit_adapter_http_url,
                Duration::from_millis(config.submit_timeout_ms),
                wallet,
            )
            .await;
        check()?;
        ensure!(
            store
                .load_execution_canary_order(&request.order_id)?
                .as_ref()
                == Some(&before),
            "owner_buy_protected_order_changed"
        );
        let (balance, slot, time) = observed?.bound(wallet)?;
        store.prepare_tiny_native_policy_for_owner_buy(
            &intent.run_id,
            &intent.wallet,
            balance,
            reserve,
            slot,
            time,
            &intent,
            &before,
            check,
        )?
    };
    let proof = ContextProof {
        policy,
        binding,
        requested,
        signal: OriginBinding::Owner(intent),
        decision_tick: tick,
    };
    proof.validate_config(config, clock(tick)?)?;
    ensure!(
        signal_binding(store, request)? == proof.signal,
        "owner_buy_protected_origin_changed"
    );
    request.metadata.protected_capital = Some(Box::new(proof));
    super::current(store, config, request, tick)?;
    Ok(())
}
