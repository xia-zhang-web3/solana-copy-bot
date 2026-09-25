//! Source producer to native BUY receipt and source-owned fractional SELL.
use super::native_buy_runner_tests::{setup_case, sell_quarter, tick};
use crate::execution_canary_route::NativeBuyMockIo;
use anyhow::Result;
use copybot_storage_core::SqliteStore;
use rusqlite::Connection;
use serde_json::json;
use std::sync::Arc;

static COHORT_TEST: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[test]
fn cohort_native_buy_zero_fee_uses_real_jupiter_assembler() -> Result<()> {
    use crate::execution_instruction_bundle_binding::BundleRequest;
    use crate::execution_submit_adapter::{
        ExecutionBuildPlanMetadata, ExecutionSubmitAdapter, ExecutionSubmitRequest,
        JupiterMetisDryRunExecutionAdapter,
    };
    use serde_json::Value;
    let quote: Value = serde_json::from_str(include_str!(
        "generic_buy_fixtures/owner-sol-usdc-20260924-quote.json"))?;
    let bundle: Value = serde_json::from_str(include_str!(
        "generic_buy_fixtures/owner-sol-usdc-20260924-instructions.json"))?;
    let wallet = "BwVw8ncEpWU7TwMTgysvwjQ85eEhKAMVbd7WU1iTE9Mk";
    let mut c = super::super::generic_buy_fixture::config("http://127.0.0.1:9");
    c.canary_route = "jupiter_swap_instructions".into();
    c.canary_wallet_pubkey = wallet.into();
    c.execution_signer_pubkey = wallet.into();
    c.pretrade_max_priority_fee_lamports = 50_000;
    c.technical_cohort = Some(copybot_config::TechnicalCohortConfig {
        policy: copybot_config::TECHNICAL_COHORT_V1.into(), activate: true,
        run_id: "assembler-fixture".into(), wallet_ids: vec!["leader".into()],
        mint_policy: copybot_config::CLASSIC_SPL_MINT_V1.into(),
        route: c.canary_route.clone(), activated_at: "fixture".into(),
        deadline: "fixture".into(), max_wait_seconds: 3600,
        max_buy_count: 1, max_source_sell_count: 1,
    });
    let request = ExecutionSubmitRequest {
        order_id: "exec-canary:native-buy-v1:fixture".into(),
        signal_id: "native-buy-v1:fixture".into(),
        client_order_id: "copybot:native-buy-v1:fixture".into(),
        attempt: 1, route: c.canary_route.clone(), wallet_id: "leader".into(),
        token: quote["outputMint"].as_str().unwrap().into(), side: "buy".into(),
        buy_size_sol: 0.01, slippage_tolerance_bps: 50,
        wallet_pubkey: wallet.into(), entry_route_plan_json: None,
        metadata: ExecutionBuildPlanMetadata {
            quote_event_id: Some("cohort-fixture-quote".into()),
            quote_status: Some("ok".into()),
            quote_in_amount_raw: Some("10000000".into()),
            quote_out_amount_raw: quote["outAmount"].as_str().map(str::to_owned),
            quote_response_json: Some(quote.to_string()),
            route_plan_json: Some(quote["routePlan"].to_string()),
            priority_fee_status: Some("ok".into()),
            priority_fee_lamports: Some(0),
            priority_fee_json: Some(super::super::priority_fee_fixture::total_json(0)),
            ..Default::default()
        },
    };
    let plan = JupiterMetisDryRunExecutionAdapter::new(c.clone())
        .build_transaction_plan(&request)?;
    let bound = BundleRequest::capture(&plan)?.bind(&bundle)?;
    let payload = crate::execution_guarded_generic_buy::assemble(
        &c, &plan, &bound, 50_000_001)?.serialized_transaction_base64;
    let fee = crate::execution_priority_fee_wire::decode_priority_fee(&payload)?;
    assert_eq!((fee.price, fee.total), (0, 0));
    Ok(())
}

#[tokio::test]
async fn cohort_actual_producer_after_wait_buys_once_and_source_sell_keeps_remainder() -> Result<()> {
    let _only_cohort = COHORT_TEST.lock().await;
    let mut case = setup_case("10000", false, true, true, true).await?;
    let db = Connection::open(&case.path)?;
    let count: i64 = db.query_row("SELECT count(*) FROM native_buy_fence_epochs", [], |r| r.get(0))?;
    assert_eq!(count, 2, "startup and periodic request-bound fences");
    for table in ["followlist", "discovery_candidate_sources", "discovery_strategy_state"] {
        let count: i64 = db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?;
        assert_eq!(count, 0, "{table} must not grant cohort authority");
    }
    let first = tick(&case, &case.config, &case.store, case.io.clone()).await?;
    assert_eq!(first.state_machine_reserved, 1, "{first:?}");
    assert_eq!(case.io.counts.lock().unwrap().send, 1);
    let owned = case.store.load_execution_canary_open_position(
        super::native_buy_runner_tests::fixture::MINT)?.expect("receipt-owned position");
    assert_eq!(owned.qty_exact.unwrap().raw(), 10000);
    let reopened = SqliteStore::open(&case.path)?;
    let repeated = tick(&case, &case.config, &reopened, case.io.clone()).await?;
    assert_eq!(repeated.state_machine_reserved, 0);
    assert_eq!(case.io.counts.lock().unwrap().send, 1);
    sell_quarter(&mut case).await?;
    let remaining = reopened.load_execution_canary_open_position(
        super::native_buy_runner_tests::fixture::MINT)?.expect("partial remains open");
    assert_eq!(remaining.qty_exact.unwrap().raw(), 7500);
    let sells: i64 = db.query_row("SELECT count(*) FROM rpc_owned_sell_handoffs", [], |r| r.get(0))?;
    assert_eq!(sells, 1);
    Ok(())
}

#[tokio::test]
async fn cohort_unknown_buy_reconciles_after_restart_without_second_send() -> Result<()> {
    let _only_cohort = COHORT_TEST.lock().await;
    let case = setup_case("10000", true, true, true, true).await?;
    let first = tick(&case, &case.config, &case.store, case.io.clone()).await?;
    assert_eq!(first.state_machine_reserved, 1, "{first:?}");
    assert!(case.store.load_execution_canary_open_position(
        super::native_buy_runner_tests::fixture::MINT)?.is_none());
    let reopened = SqliteStore::open(&case.path)?;
    let settled_io = Arc::new(NativeBuyMockIo {
        runner: None, initial_sol: case.io.initial_sol.clone(),
        fee_lamports: case.io.fee_lamports, fee_slot: case.io.fee_slot,
        expected_message_sha256: case.io.expected_message_sha256.clone(),
        submit_signature: case.io.submit_signature.clone(),
        confirmation: json!({"result":{"value":[{"err":null,"slot":120,
            "confirmationStatus":"confirmed"}]}}),
        receipt: case.io.receipt.clone(), counts: case.io.counts.clone(),
    });
    tick(&case, &case.config, &reopened, settled_io.clone()).await?;
    tick(&case, &case.config, &reopened, settled_io).await?;
    assert_eq!(case.io.counts.lock().unwrap().send, 1);
    assert_eq!(reopened.load_execution_canary_open_position(
        super::native_buy_runner_tests::fixture::MINT)?.unwrap().qty_exact.unwrap().raw(), 10000);
    let db = Connection::open(&case.path)?;
    let count: i64 = db.query_row("SELECT count(*) FROM execution_canary_dispatch WHERE side='buy'", [], |r| r.get(0))?;
    assert_eq!(count, 1);
    Ok(())
}
