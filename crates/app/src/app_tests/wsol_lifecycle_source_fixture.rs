//! One LOCAL source daemon scenario. Real private authorities; mocked chain/AMM only.
use super::{receipt_lot_custody_fixture as f, wsol_lifecycle_fixture as w};
use crate::execution_submit_adapter::*;
use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use copybot_config::*;
use copybot_storage_core::*;
use serde_json::{json, Value};
pub(super) fn bot_config(scope: &controlled_receipt_lot::Scope) -> Result<ExecutionConfig> {
    let mut c = super::protected_usdc_prepare::config(&scope.bot_wallet)?;
    let mut value = serde_json::to_value(scope)?;
    value["wsol_lifecycle"] = json!(TRANSACTION_OWNED_WSOL_V1);
    c.controlled_receipt_lot = Some(serde_json::from_value(value)?);
    c.pretrade_min_sol_reserve = 0.0;
    let s = c.owned_sell_preparation.as_mut().unwrap();
    s.fractional_inventory = Some(CONTROLLED_RECEIPT_LOT_V1.into());
    s.protected_bot_usdc_baseline = None;
    Ok(c)
}
pub(super) fn config(
    scope: &controlled_receipt_lot::Scope,
    root: &std::path::Path,
    bot: &ExecutionConfig,
) -> Result<ExecutionConfig> {
    let mut c = super::owner_buy_fixture::config(
        &scope.source_wallet,
        "http://127.0.0.1:1",
        &root.join("neverSTOP").to_string_lossy(),
    );
    let mut value = serde_json::to_value(scope)?;
    value["wsol_lifecycle"] = json!(TRANSACTION_OWNED_WSOL_V1);
    c.controlled_receipt_lot = Some(serde_json::from_value(value)?);
    c.owned_sell_preparation = None;
    c.pretrade_min_sol_reserve = 0.0;
    c.tiny_experiment.id = Some(scope.source_run_id.clone());
    c.tiny_experiment.policy_mode = TinyPolicyMode::ProtectedNativeCapital;
    c.quote_canary_buy_slippage_bps = 50;
    c.quote_canary_sell_slippage_bps = 50;
    c.max_submit_attempts = 1;
    let p = c.owner_technical_buy.as_mut().unwrap();
    p.policy = OWNER_EXECUTION_ROUNDTRIP_V1.into();
    p.run_id = scope.source_run_id.clone();
    p.intent_id = scope.source_buy_intent_id.clone();
    p.mint = OWNER_EXIT_MINT.into();
    p.min_reserve_lamports = 0;
    p.max_slippage_bps = 50;
    p.activated_at = bot.technical_cohort.as_ref().unwrap().activated_at.clone();
    p.expires_at = bot.technical_cohort.as_ref().unwrap().deadline.clone();
    p.source_copy_exit_gate = Some(OwnerSourceCopyGateConfig {
        policy: CONFIRMED_NATIVE_BOT_BUY_V1.into(),
        bot_state_db_path: root
            .join("actual-bot-gate.db")
            .to_string_lossy()
            .into_owned(),
        bot_wallet_pubkey: scope.bot_wallet.clone(),
        bot_run_id: scope.bot_run_id.clone(),
        bot_policy_identity: crate::execution_native_buy_rpc::policy_identity(bot)?,
        bot_kill_switch_path: root.join("BOTSTOP").to_string_lossy().into_owned(),
    });
    Ok(c)
}
pub(super) async fn buy(
    c: &ExecutionConfig,
    store: &SqliteStore,
    order: &ExecutionCanaryOrder,
) -> Result<(String, Value)> {
    let intent = crate::execution_owner_buy_authority::configured(c)?.unwrap();
    let now = Utc::now();
    store.prepare_tiny_native_policy_for_owner_buy(
        &intent.run_id,
        &intent.wallet,
        1_000_000_000,
        0,
        100,
        now,
        &intent,
        order,
        || Ok(now),
    )?;
    let mut q = super::owner_buy_wire_fixture::quote();
    q["outAmount"] = json!("10000");
    q["otherAmountThreshold"] = json!("9950");
    q["routePlan"][0]["swapInfo"]["outAmount"] = json!("10000");
    let mut metadata = super::priority_fee_fixture::metadata();
    metadata.quote_event_id = Some("LOCAL SOURCE producer quote".into());
    metadata.quote_in_amount_raw = Some("10000000".into());
    metadata.quote_out_amount_raw = Some("10000".into());
    metadata.quote_response_json = Some(q.to_string());
    metadata.route_plan_json = Some(q["routePlan"].to_string());
    let mut r = ExecutionSubmitRequest {
        order_id: order.order_id.clone(),
        signal_id: order.signal_id.clone(),
        client_order_id: order.client_order_id.clone(),
        attempt: 1,
        route: intent.route,
        wallet_id: c.canary_wallet_pubkey.clone(),
        token: OWNER_EXIT_MINT.into(),
        side: "buy".into(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 50,
        wallet_pubkey: c.canary_wallet_pubkey.clone(),
        entry_route_plan_json: None,
        metadata,
    };
    crate::execution_native_floor_policy::protected::prepare_request(store, c, &mut r, now).await?;
    let wallet = crate::execution_pumpswap_accounts::parse_pubkey(&r.wallet_pubkey, "LOCALsource")?;
    let mut ix = super::owner_buy_wire_fixture::instructions(wallet)?;
    ix.iter_mut()
        .find(|i| i.data.starts_with(&[229, 23, 203, 151, 122, 227, 173, 42]))
        .unwrap()
        .data[24..32]
        .copy_from_slice(&10000u64.to_le_bytes());
    let wire = w::build(c, &r, &ix)?;
    let receipt = w::exercise("source-buy", c, &r, &wire)?;
    Ok((wire, receipt))
}
pub(super) fn sell(case: &f::Case) -> Result<String> {
    let c = case.source_config.as_ref().context("LOCALsourcecfg")?;
    let c = crate::execution_owner_roundtrip::exit_config(c, &case.source)?
        .context("LOCAL source gate waits confirmed native BOT BUY")?;
    let intent = crate::execution_owner_exit_authority::configured(&c)?.unwrap();
    case.source.register_owner_exit_intent(&intent)?;
    let order = case
        .source
        .reserve_owner_exit_order(&intent.intent_id, || Ok(Utc::now()))?
        .order;
    let mut q = super::owner_exit_test_fixture::quote();
    q["inAmount"] = json!(intent.amount_raw.to_string());
    q["routePlan"][0]["swapInfo"]["inAmount"] = q["inAmount"].clone();
    let mut metadata = super::priority_fee_fixture::metadata();
    metadata.priority_fee_lamports = Some(0);
    metadata.priority_fee_json = Some(super::priority_fee_fixture::total_json(0));
    metadata.quote_event_id = Some("LOCAL SOURCE SELL quote".into());
    metadata.quote_in_amount_raw = Some(intent.amount_raw.to_string());
    metadata.quote_out_amount_raw = Some(super::owner_exit_test_fixture::OUT.to_string());
    metadata.quote_response_json = Some(q.to_string());
    metadata.route_plan_json = Some(q["routePlan"].to_string());
    let mut r = ExecutionSubmitRequest {
        order_id: order.order_id.clone(),
        signal_id: order.signal_id.clone(),
        client_order_id: order.client_order_id.clone(),
        attempt: 1,
        route: intent.route.clone(),
        wallet_id: intent.wallet.clone(),
        token: intent.mint.clone(),
        side: "sell".into(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 50,
        wallet_pubkey: intent.wallet.clone(),
        entry_route_plan_json: None,
        metadata,
    };
    crate::execution_controlled_receipt_lot::stamp_exit(
        &c,
        &case.source,
        &intent,
        &mut r,
        Utc::now(),
    )?;
    let wallet = crate::execution_pumpswap_accounts::parse_pubkey(&r.wallet_pubkey, "LOCALsource")?;
    let mut ix = super::owner_exit_test_fixture::instructions(wallet)?;
    let jup = crate::execution_pumpswap_accounts::parse_pubkey(
        crate::execution_owner_buy_wire::JUPITER,
        "LOCALjup",
    )?;
    let route = ix.iter_mut().find(|i| i.program_id == jup).unwrap();
    route.data[16..24].copy_from_slice(&intent.amount_raw.to_le_bytes());
    let wire = w::build(&c, &r, &ix)?;
    let receipt = w::exercise("source-sell", &c, &r, &wire)?;
    let proof =
        copybot_storage_core::ordered_sell_quote::fractional::inventory::prove_jupiter_source_sell(
            &receipt["result"],
            &intent.wallet,
            &intent.mint,
            6,
            intent.amount_raw,
        )?;
    assert!(proof.seeded);
    assert_eq!(proof.target_raw, intent.amount_raw);
    let mut invalid = receipt["result"].clone();
    invalid["meta"]["postTokenBalances"][0]["uiTokenAmount"]["amount"] = json!("0");
    assert!(
        copybot_storage_core::ordered_sell_quote::fractional::inventory::prove_jupiter_source_sell(
            &invalid,
            &intent.wallet,
            &intent.mint,
            6,
            intent.amount_raw
        )
        .is_err()
    );
    w::snapshot(
        "source-sell",
        &rusqlite::Connection::open(&case.source_path)?,
    )?;
    w::submitted(
        &case.source,
        &case.source_path,
        &order,
        &r.wallet_pubkey,
        "sell",
        f::SELL_SIG,
        &wire,
    )?;
    std::fs::write(case.root.path().join("actual-source-sell-wire"), wire)?;
    std::fs::write(
        case.root.path().join("actual-source-sell-receipt.json"),
        serde_json::to_vec(&receipt)?,
    )?;
    Ok(order.order_id)
}
