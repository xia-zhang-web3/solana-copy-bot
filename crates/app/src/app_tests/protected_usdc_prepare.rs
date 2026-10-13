//! Actual native quote/hold/wire preparation; all chain/DEX operands are MOCKS.
use super::{
    association_parent_fixture as parent,
    protected_usdc_fixture::{Case, H, USDC},
};
use crate::execution_submit_adapter::*;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{Duration, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{ordered_sell_quote::*, rpc_owned_sell_handoff::dispatch::Prepared};
use serde_json::json;
pub(super) fn config(wallet: &str) -> Result<ExecutionConfig> {
    let mut c: ExecutionConfig = serde_json::from_value(
        json!({"enabled":false,"canary_tiny_submit_enabled":true,"canary_enabled":true,"quote_canary_enabled":true,"canary_wallet_pubkey":wallet,"execution_signer_pubkey":wallet,"execution_signer_keypair_path":"never-load-mock-key","canary_route":"jupiter_swap_instructions","swap_instructions_dry_run_enabled":true,"swap_transaction_dry_run_enabled":true,"pretrade_max_priority_fee_lamports":22000,"quote_canary_slippage_bps":50,"quote_canary_sell_slippage_bps":50,"submit_adapter_http_url":"http://127.0.0.1:1","tiny_experiment":{"id":"protected-usdc","activate":false},"native_fresh_buy":{"policy":"processed_slot_fence_availability_v1"},"owned_sell_preparation":{"policy":"rpc_finalized_cross_slot_owned_sell_v1","rpc_url":"http://127.0.0.1:1","genesis_hash":"11111111111111111111111111111111","identity":"protected-usdc","tiny_dispatch":true,"fractional_inventory":"whole_wallet_parent_program_fraction_v1"}}),
    )?;
    c.canary_max_signal_age_seconds = 120;
    c.canary_max_open_positions = 1;
    c.canary_buy_size_sol = 0.01;
    c.quote_canary_buy_size_sol = 0.01;
    c.canary_max_daily_loss_sol = 0.02;
    c.pretrade_min_sol_reserve = 0.160_200_031;
    c.quote_canary_buy_slippage_bps = 50;
    c.max_submit_attempts = 1;
    c.canary_entry_submit_enabled = true;
    c.tiny_experiment.activate = true;
    c.tiny_experiment.policy_mode = copybot_config::TinyPolicyMode::ProtectedNativeCapital;
    let now = Utc::now();
    c.technical_cohort = Some(copybot_config::TechnicalCohortConfig {
        policy: copybot_config::TECHNICAL_COHORT_V1.into(),
        activate: true,
        run_id: "protected-usdc".into(),
        wallet_ids: vec!["4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4P3bkLKi".into()],
        mint_policy: copybot_config::CLASSIC_SPL_MINT_V1.into(),
        route: c.canary_route.clone(),
        activated_at: (now - Duration::seconds(1)).to_rfc3339(),
        deadline: (now + Duration::seconds(599)).to_rfc3339(),
        max_wait_seconds: 600,
        max_buy_count: 1,
        max_source_sell_count: 1,
    });
    Ok(c)
}
pub(super) fn payload(c: &ExecutionConfig, amount: u64) -> Result<String> {
    let wallet =
        crate::execution_pumpswap_accounts::parse_pubkey(&c.canary_wallet_pubkey, "mock-wallet")?;
    let mut instructions = super::owner_exit_test_fixture::instructions(wallet)?;
    let route = instructions
        .iter_mut()
        .find(|i| i.data.starts_with(&[229, 23, 203, 151, 122, 227, 173, 42]))
        .unwrap();
    route.data[16..24].copy_from_slice(&amount.to_le_bytes());
    Ok(STANDARD.encode(
        crate::execution_solana_tx::serialize_unsigned_legacy_transaction(
            wallet,
            [9; 32],
            &instructions,
        )?,
    ))
}
pub(super) fn prepare(f: &Case, claim: &QuoteClaim) -> Result<(Prepared, ExecutionSubmitRequest)> {
    let c = &f.config;
    let b = &claim.binding;
    assert_eq!(b.raw, H);
    let mut body = super::owner_exit_test_fixture::quote();
    body["inAmount"] = json!(H.to_string());
    body["routePlan"][0]["swapInfo"]["inAmount"] = json!(H.to_string());
    let now = Utc::now();
    let observation = QuoteObservation {
        version: 1,
        binding: Some(b.clone()),
        outcome: QuoteOutcome::Current,
        reason: None,
        http_started: Some(now),
        http_response: Some(now),
        quote_response_available_ts: Some(now),
        http_ended: now,
        response_in_raw: Some(H.to_string()),
        response_out_raw: Some(super::owner_exit_test_fixture::OUT.to_string()),
        response_sha256: Some(crate::execution_owned_sell_rpc::digest(body.to_string())),
        event_time: None,
        event_delay_ns: None,
    };
    let q =
        f.db.store
            .complete_strict_sell_quote(claim, parent::limits(), observation, Utc::now)?;
    let snapshot = f.db.store.owned_sell_snapshot(b, parent::limits())?;
    let h = f.db.store.reserve_owned_sell_handoff(
        &snapshot,
        &q,
        parent::limits(),
        &crate::execution_owned_sell_rpc::identity(c)?,
        "explicit MOCK finalized authority; no live quote",
        "protected-usdc",
        &c.canary_wallet_pubkey,
        Utc::now,
    )?;
    let unsigned = payload(c, H)?;
    let (message, fee) =
        crate::execution_priority_fee_wire::decode_priority_fee_message(&unsigned)?;
    f.db.store.complete_owned_sell_handoff(
        &h,
        parent::limits(),
        &unsigned,
        &message.binding.message_sha256,
        5000,
        fee.total,
        Utc::now,
    )?;
    let p = Prepared {
        handoff: h,
        experiment: f.db.store.owned_sell_experiment_snapshot()?,
        limits: (
            parent::limits().count,
            parent::limits().bytes,
            parent::limits().busy_ms,
        ),
        payload: unsigned,
        message_sha256: message.binding.message_sha256,
        total_fee: 5000,
        priority_fee: fee.total,
    };
    let request = ExecutionSubmitRequest {
        order_id: p.order_id(),
        signal_id: p.handoff.intent_id.clone(),
        client_order_id: p.handoff.owner.clone(),
        attempt: 1,
        route: c.canary_route.clone(),
        wallet_id: b.source_wallet.clone(),
        token: USDC.into(),
        side: "sell".into(),
        buy_size_sol: 0.,
        slippage_tolerance_bps: 50,
        wallet_pubkey: c.canary_wallet_pubkey.clone(),
        entry_route_plan_json: None,
        metadata: ExecutionBuildPlanMetadata {
            quote_event_id: Some(p.handoff.intent_id.clone()),
            quote_source: Some(b.provider.clone()),
            quote_request_ts: Some(now),
            http_request_started_ts: Some(now),
            quote_response_available_ts: Some(now),
            quote_status: Some("ok".into()),
            quote_in_amount_raw: Some(H.to_string()),
            quote_out_amount_raw: Some(super::owner_exit_test_fixture::OUT.to_string()),
            quote_response_json: Some(body.to_string()),
            route_plan_json: Some(body["routePlan"].to_string()),
            priority_fee_status: Some("ok".into()),
            priority_fee_source: Some("configured_cap".into()),
            priority_fee_lamports: Some(0),
            rpc_owned_sell: Some(Box::new(p.clone())),
            rpc_owned_live: Some(crate::execution_owned_sell_prepare::submit::guard::Live(
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
                std::sync::Arc::new(std::sync::atomic::AtomicI64::new(-1)),
                None,
            )),
            ..Default::default()
        },
    };
    crate::execution_owned_sell_prepare::submit::guard::config(c, &p)?;
    assert_eq!(
        crate::execution_owned_sell_prepare::submit::guard::request(&f.db.store, &request)?,
        p
    );
    Ok((p, request))
}
pub(super) fn dispatch(
    p: &Prepared,
) -> (
    copybot_storage_core::ExecutionCanaryDispatch,
    copybot_storage_core::TinyBudgetClaim,
) {
    let d = copybot_storage_core::ExecutionCanaryDispatch {
        order_id: p.order_id(),
        signal_id: p.handoff.intent_id.clone(),
        client_order_id: p.handoff.owner.clone(),
        route: "jupiter_swap_instructions".into(),
        attempt: 1,
        wallet: p.handoff.wallet.clone(),
        token: USDC.into(),
        side: "sell".into(),
        tx_signature: bs58::encode([231; 64]).into_string(),
        message_sha256: p.message_sha256.clone(),
        transaction_sha256: "d".repeat(64),
    };
    let b = copybot_storage_core::TinyBudgetClaim {
        experiment_id: "protected-usdc".into(),
        wallet: d.wallet.clone(),
        tx_signature: d.tx_signature.clone(),
        message_sha256: d.message_sha256.clone(),
        transaction_sha256: d.transaction_sha256.clone(),
        buy_lamports: Some(0),
        protected_capital: None,
        total_fee: p.total_fee,
        priority_fee: p.priority_fee,
        fee_slot: 151,
    };
    (d, b)
}
