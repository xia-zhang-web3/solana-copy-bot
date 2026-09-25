//! Rechecks for the typed owned SELL arm; source UTC is never fabricated.
use crate::execution_owned_sell_rpc as rpc;
use crate::execution_submit_adapter::{ExecutionSubmitIntent, ExecutionSubmitRequest};
use anyhow::{ensure, Context, Result};
use chrono::Utc;
use copybot_config::ExecutionConfig;
use copybot_storage_core::{rpc_owned_sell_handoff::dispatch::Prepared, SqliteStore};

#[derive(Debug, Clone)]
pub(crate) struct Live(
    pub(crate) std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub(crate) std::sync::Arc<std::sync::atomic::AtomicI64>,
);
impl PartialEq for Live {
    fn eq(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &other.0)
    }
}
pub(crate) fn live(r: &ExecutionSubmitRequest) -> Result<()> {
    ensure!(
        r.metadata
            .rpc_owned_live
            .as_ref()
            .is_some_and(|c| c.0.load(std::sync::atomic::Ordering::SeqCst)),
        "owned_sell_runner_cancelled"
    );
    Ok(())
}
pub(crate) fn request(store: &SqliteStore, r: &ExecutionSubmitRequest) -> Result<Prepared> {
    live(r)?;
    let p = r
        .metadata
        .rpc_owned_sell
        .as_deref()
        .context("owned_sell_authority_missing")?;
    let h = &p.handoff;
    let b = &h.snapshot.quote;
    ensure!(
        r.order_id == p.order_id()
            && r.signal_id == h.intent_id
            && r.client_order_id == h.owner
            && r.attempt == 1
            && r.side == "sell"
            && r.token == b.mint
            && r.wallet_id == b.source_wallet
            && r.wallet_pubkey == h.wallet
            && r.buy_size_sol == 0.0
            && r.metadata.quote_in_amount_raw.as_deref() == Some(b.raw.to_string().as_str())
            && r.metadata.quote_out_amount_raw == h.quote.response_out_raw
            && r.metadata.http_request_started_ts == h.quote.http_started
            && r.metadata.quote_response_available_ts == h.quote.quote_response_available_ts
            && r.metadata.quote_request_ts == h.quote.http_started
            && r.metadata.quote_response_json.as_ref().map(rpc::digest) == h.quote.response_sha256,
        "owned_sell_request_changed"
    );
    let version = r
        .metadata
        .rpc_owned_live
        .as_ref()
        .context("owned_sell_runner_cancelled")?
        .1
        .load(std::sync::atomic::Ordering::SeqCst);
    if version < 0 || !store.recheck_owned_sell_prepared_at_version(p, version, Utc::now())? {
        store.recheck_owned_sell_prepared(p, Utc::now())?;
        r.metadata
            .rpc_owned_live
            .as_ref()
            .context("owned_sell_runner_cancelled")?
            .1
            .store(store.sqlite_data_version()?, std::sync::atomic::Ordering::SeqCst);
    }
    Ok(p.clone())
}
pub(crate) fn config(c: &ExecutionConfig, p: &Prepared) -> Result<()> {
    crate::execution_technical_cohort::before_deadline(c)?;
    ensure!(
        copybot_config::owned_sell_dispatch(c)
            && copybot_config::owned_sell_flags(c)
            && c.canary_enabled
            && c.quote_canary_enabled
            && c.swap_instructions_dry_run_enabled
            && c.swap_transaction_dry_run_enabled
            && (!c.tiny_experiment.activate || copybot_config::native_first_buy_activation(c)),
        "owned_sell_dispatch_mode"
    );
    ensure!(
        (c.tiny_experiment.policy_mode == copybot_config::TinyPolicyMode::ProtectedNativeCapital)
            == p.experiment.policy.is_some(),
        "owned_sell_policy_changed"
    );
    ensure!(
        rpc::identity(c)? == p.handoff.config_sha256,
        "owned_sell_config_changed"
    );
    ensure!(
        !std::path::Path::new(&c.canary_kill_switch_path).exists(),
        "kill_switch_active"
    );
    ensure!(Utc::now() < p.handoff.deadline, "owned_sell_deadline");
    Ok(())
}
pub(crate) fn payload(p: &Prepared, signed: &str) -> Result<()> {
    let before = crate::execution_transaction_wire::decode_message(&p.payload, |_| Ok(()))?;
    let after = crate::execution_transaction_wire::decode_message(signed, |_| Ok(()))?;
    ensure!(
        before.binding.message_sha256 == p.message_sha256
            && after.binding.message_bytes == before.binding.message_bytes,
        "owned_sell_message_changed"
    );
    Ok(())
}
pub(crate) async fn blockhash(
    c: &ExecutionConfig,
    store: &SqliteStore,
    r: &ExecutionSubmitRequest,
    payload: &str,
    min_slot: u64,
) -> Result<()> {
    let p = request(store, r)?;
    config(c, &p)?;
    self::payload(&p, payload)?;
    let message = crate::execution_transaction_wire::decode_message(payload, |_| Ok(()))?;
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()?;
    let mut check = || {
        config(c, &p)?;
        ensure!(request(store, r)? == p, "owned_sell_prepared_changed");
        Ok(())
    };
    let left = (p.handoff.deadline - Utc::now())
        .to_std()
        .context("owned_sell_deadline")?;
    let value=tokio::time::timeout(left,rpc::exchange(&http,c,&rpc::endpoint(c)?,
        serde_json::json!({"jsonrpc":"2.0","id":"owned-sell-blockhash","method":"isBlockhashValid","params":[bs58::encode(message.recent_blockhash).into_string(),{"commitment":"finalized","minContextSlot":min_slot}]}),&mut check)).await.context("owned_sell_deadline")??;
    ensure!(
        value.value()["result"]["value"] == true
            && value.value()["result"]["context"]["slot"]
                .as_u64()
                .is_some_and(|s| s >= min_slot),
        "owned_sell_blockhash_unknown_or_stale"
    );
    check()
}
pub(crate) async fn budget(
    store: &SqliteStore,
    r: &ExecutionSubmitRequest,
    intent: &ExecutionSubmitIntent,
    envelope: &crate::execution_signing_envelope::ExecutionSigningEnvelope,
    gate: &crate::execution_canary_submit_contract::ExecutionTinySubmitGate,
    transport: &crate::execution_submit_adapter::RpcExecutionSubmitTransport,
) -> Result<(copybot_storage_core::TinyBudgetClaim, chrono::DateTime<Utc>)> {
    let p = request(store, r)?;
    let c = gate
        .buy_safety_config
        .as_ref()
        .context("tiny_budget_config_missing")?;
    config(c, &p)?;
    ensure!(
        gate.allow_rpc_submit
            && gate.execution_wallet_pubkey == r.wallet_pubkey
            && transport.rpc_endpoint() == rpc::endpoint(c)?.as_str(),
        "tiny_budget_identity"
    );
    let fee = crate::execution_native_rpc::NativeFundingRpcClient::new()?
        .collect_fee_only(
            transport.rpc_endpoint(),
            (p.handoff.deadline - Utc::now())
                .to_std()
                .context("owned_sell_deadline")?,
            &intent.signed_transaction_base64,
        )
        .await?;
    config(c, &p)?;
    ensure!(request(store, r)? == p, "owned_sell_prepared_changed");
    let (message, priority) = crate::execution_priority_fee_wire::decode_priority_fee_message(
        &intent.signed_transaction_base64,
    )?;
    let (total_fee, fee_slot) = fee.bound_fee(&message.binding)?;
    ensure!(
        fee_slot >= p.handoff.snapshot.sell.facts.slot,
        "owned_sell_fee_stale"
    );
    blockhash(c, store, r, &intent.signed_transaction_base64, fee_slot).await?;
    config(c, &p)?;
    ensure!(request(store, r)? == p, "owned_sell_prepared_changed");
    crate::execution_priority_fee_proof::validate_submit(
        store,
        r,
        envelope,
        intent,
        c.pretrade_max_priority_fee_lamports.min(22_000),
    )?;
    crate::execution_native_floor_policy::verify_submit_payload(
        r,
        &intent.signed_transaction_base64,
        c.pretrade_min_sol_reserve,
        &r.wallet_pubkey,
    )?;
    let d = crate::execution_tiny_submit_state::dispatch::identity(r, intent)?;
    Ok((
        copybot_storage_core::TinyBudgetClaim {
            experiment_id: p.handoff.experiment_id,
            wallet: d.wallet,
            tx_signature: d.tx_signature,
            message_sha256: d.message_sha256,
            transaction_sha256: d.transaction_sha256,
            buy_lamports: Some(0),
            protected_capital: None,
            total_fee,
            priority_fee: priority.total,
            fee_slot,
        },
        Utc::now(),
    ))
}
