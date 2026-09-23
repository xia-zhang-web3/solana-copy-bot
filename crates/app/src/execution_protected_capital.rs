//! Durable protected native capital, explicitly distinct from decoded Jupiter input.
use crate::execution_instruction_bundle_binding::BundleRequest;
use crate::execution_submit_adapter::{
    ExecutionSubmitAdapter, ExecutionSubmitRequest, ExecutionTransactionPlan,
    JupiterMetisDryRunExecutionAdapter,
};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::{ExecutionConfig, TinyPolicyMode};
use copybot_storage_core::{ProtectedCapitalClaim, ProtectedNativePolicy, SqliteStore};
use std::{sync::OnceLock, time::Duration};

/// Private constructor; skipped by metadata deserialization. Only a closed collector
/// plus the durable singleton can create this preparation context.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ContextProof {
    policy: ProtectedNativePolicy,
    binding: BundleRequest,
    requested: u64,
    signal: serde_json::Value,
    decision_tick: DateTime<Utc>,
}
fn clock(tick: DateTime<Utc>) -> Result<DateTime<Utc>> {
    crate::execution_canary_safety::risk_clock::decision_time(tick).context("tiny_budget_clock")
}
pub(crate) fn enabled(config: &ExecutionConfig) -> bool {
    config.tiny_experiment.policy_mode == TinyPolicyMode::ProtectedNativeCapital
}
impl ContextProof {
    fn validate_config(&self, config: &ExecutionConfig, now: DateTime<Utc>) -> Result<()> {
        ensure!(
            enabled(config) && config.canary_tiny_submit_enabled,
            "tiny_capital_mode_conflict"
        );
        ensure!(
            config.tiny_experiment.id.as_deref() == Some(self.policy.experiment_id.as_str())
                && config.canary_wallet_pubkey == self.policy.wallet
                && config.execution_signer_pubkey == self.policy.wallet,
            "tiny_capital_identity"
        );
        ensure!(
            now >= self.policy.activated_at && now < self.policy.deadline,
            "tiny_budget_deadline"
        );
        Ok(())
    }
    pub(crate) fn for_plan(
        &self,
        config: &ExecutionConfig,
        plan: &ExecutionTransactionPlan,
    ) -> Result<u64> {
        self.validate_config(config, clock(self.decision_tick)?)?;
        self.binding.verify(plan)?;
        self.floor(&plan.wallet_pubkey, config.pretrade_min_sol_reserve)
    }
    pub(crate) fn floor(&self, wallet: &str, reserve_sol: f64) -> Result<u64> {
        ensure!(wallet == self.policy.wallet, "tiny_capital_identity");
        Ok(self
            .policy
            .floor_lamports
            .max(super::reserve_lamports(reserve_sol)?))
    }
    pub(crate) fn verify_request(&self, request: &ExecutionSubmitRequest) -> Result<()> {
        self.binding.verify_request(request)
    }
    pub(crate) fn verify_floor(
        &self,
        payload: &str,
        wallet: crate::execution_solana_tx::PubkeyBytes,
        floor: u64,
    ) -> Result<()> {
        ensure!(
            bs58::encode(wallet).into_string() == self.policy.wallet
                && floor >= self.policy.floor_lamports,
            "tiny_capital_floor_binding"
        );
        crate::execution_native_floor::verify_final_native_floor(payload, wallet, floor)?;
        Ok(())
    }
    pub(crate) fn claim(&self, floor: u64) -> Result<ProtectedCapitalClaim> {
        Ok(ProtectedCapitalClaim {
            policy: self.policy.clone(),
            requested_lamports: self.requested,
            floor_lamports: floor,
            request_sha256: self.binding.request_sha256()?,
        })
    }
}

pub(crate) fn current(
    store: &SqliteStore,
    config: &ExecutionConfig,
    request: &ExecutionSubmitRequest,
    tick: DateTime<Utc>,
) -> Result<Option<ContextProof>> {
    if !request.side.eq_ignore_ascii_case("buy") {
        return Ok(None);
    }
    let proof = request.metadata.protected_capital.as_deref();
    if !enabled(config) {
        ensure!(proof.is_none(), "tiny_capital_mode_conflict");
        return Ok(None);
    }
    let proof = proof.context("tiny_capital_context_missing")?;
    let now = clock(tick)?;
    proof.validate_config(config, now)?;
    proof.verify_request(request)?;
    ensure!(
        signal_binding(store, request)? == proof.signal,
        "tiny_capital_order_changed"
    );
    ensure!(
        store.tiny_native_policy(&proof.policy.experiment_id, &request.wallet_pubkey, now)?
            == proof.policy,
        "tiny_capital_policy_binding"
    );
    Ok(Some(proof.clone()))
}

pub(crate) async fn prepare_request(
    store: &SqliteStore,
    config: &ExecutionConfig,
    request: &mut ExecutionSubmitRequest,
    tick: DateTime<Utc>,
) -> Result<()> {
    prepare_request_guarded(store, config, request, tick, None).await
}

pub(crate) async fn prepare_request_guarded(
    store: &SqliteStore,
    config: &ExecutionConfig,
    request: &mut ExecutionSubmitRequest,
    tick: DateTime<Utc>,
    native: Option<&crate::execution_canary_route::NativeBuyGuard>,
) -> Result<()> {
    if !request.side.eq_ignore_ascii_case("buy") {
        return Ok(());
    }
    if !enabled(config) {
        ensure!(
            request.metadata.protected_capital.is_none(),
            "tiny_capital_mode_conflict"
        );
        return Ok(());
    }
    ensure!(
        config.canary_tiny_submit_enabled,
        "tiny_capital_mode_inactive"
    );
    config
        .tiny_experiment
        .validate(&config.canary_wallet_pubkey)?;
    let id = config
        .tiny_experiment
        .id
        .as_deref()
        .context("tiny_budget_inactive")?;
    ensure!(
        config.execution_signer_pubkey == request.wallet_pubkey
            && config.canary_wallet_pubkey == request.wallet_pubkey,
        "tiny_capital_identity"
    );
    let plan =
        JupiterMetisDryRunExecutionAdapter::new(config.clone()).build_transaction_plan(request)?;
    let binding = BundleRequest::capture(&plan)?;
    let requested = binding.buy_requested_lamports()?;
    let now = clock(tick)?;
    let signal = signal_binding(store, request)?;
    let policy = if store.load_tiny_experiment(now)?.is_some() {
        store.tiny_native_policy(id, &request.wallet_pubkey, now)?
    } else {
        ensure!(config.tiny_experiment.activate, "tiny_budget_inactive");
        let before = store
            .load_execution_canary_order(&request.order_id)?
            .context("tiny_submit_order_missing")?;
        ensure!(
            before.signal_id == request.signal_id
                && before.client_order_id == request.client_order_id
                && before.attempt == request.attempt
                && before.route == request.route
                && before.tx_signature.is_none(),
            "tiny_capital_order_binding"
        );
        static CLIENT: OnceLock<Result<crate::execution_native_rpc::NativeFundingRpcClient, ()>> =
            OnceLock::new();
        let client = CLIENT
            .get_or_init(|| {
                crate::execution_native_rpc::NativeFundingRpcClient::new().map_err(|_| ())
            })
            .as_ref()
            .map_err(|_| anyhow::anyhow!("tiny_capital_client"))?;
        let wallet = crate::execution_pumpswap_accounts::parse_pubkey(
            &request.wallet_pubkey,
            "tiny_capital_wallet",
        )?;
        let observation = client
            .collect_payer(
                &config.submit_adapter_http_url,
                Duration::from_millis(config.submit_timeout_ms),
                wallet,
            )
            .await;
        ensure!(
            store
                .load_execution_canary_order(&request.order_id)?
                .as_ref()
                == Some(&before),
            "tiny_capital_order_changed"
        );
        ensure!(
            signal_binding(store, request)? == signal,
            "tiny_capital_order_changed"
        );
        let (balance, slot, time) = observation?.bound(wallet)?;
        let reserve = super::reserve_lamports(config.pretrade_min_sol_reserve)?;
        if config.native_fresh_buy.is_some() {
            let native = native.context("native_buy_activation_guard_required")?;
            let binding = native.activation_binding(store, config, request, Utc::now())?;
            store.prepare_tiny_native_policy_for_native_buy(
                id, &request.wallet_pubkey, balance, reserve, slot, time,
                &binding, || clock(tick),
            )?
        } else {
            store.prepare_tiny_native_policy(
                id, &request.wallet_pubkey, balance, reserve, slot, time,
                || clock(tick),
            )?
        }
    };
    let proof = ContextProof {
        policy,
        binding,
        requested,
        signal,
        decision_tick: tick,
    };
    proof.validate_config(config, clock(tick)?)?;
    request.metadata.protected_capital = Some(Box::new(proof));
    current(store, config, request, tick)?;
    Ok(())
}

fn signal_binding(
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
) -> Result<serde_json::Value> {
    let order = store
        .load_execution_canary_order(&request.order_id)?
        .context("tiny_submit_order_missing")?;
    let s = store
        .load_copy_signal_by_signal_id(&order.signal_id)?
        .context("tiny_submit_signal_missing")?;
    ensure!(
        order.signal_id == request.signal_id
            && order.client_order_id == request.client_order_id
            && order.attempt == request.attempt
            && order.route == request.route
            && order.tx_signature.is_none()
            && s.token == request.token
            && s.wallet_id == request.wallet_id
            && s.side.eq_ignore_ascii_case(&request.side),
        "tiny_capital_order_changed"
    );
    Ok(serde_json::json!([
        s.signal_id,
        s.wallet_id,
        s.side,
        s.token,
        s.status,
        s.ts,
        s.notional_sol.to_bits(),
        s.notional_lamports.map(|n| n.as_u64()),
        s.notional_origin
    ]))
}

pub(crate) fn current_gate(
    store: &SqliteStore,
    gate: &crate::execution_canary_submit_contract::ExecutionTinySubmitGate,
    request: &ExecutionSubmitRequest,
    tick: DateTime<Utc>,
) -> Result<Option<ContextProof>> {
    let config = gate
        .buy_safety_config
        .as_ref()
        .context("tiny_budget_config_missing")?;
    let proof = current(store, config, request, tick)?;
    if let Some(p) = &proof {
        ensure!(
            gate.allow_rpc_submit
                && gate.execution_wallet_pubkey == config.execution_signer_pubkey
                && gate.pretrade_max_priority_fee_lamports
                    == config.pretrade_max_priority_fee_lamports
                && p.floor(&request.wallet_pubkey, gate.pretrade_min_sol_reserve)?
                    == p.floor(&request.wallet_pubkey, config.pretrade_min_sol_reserve)?,
            "tiny_capital_current_config"
        );
    }
    Ok(proof)
}
