use super::b60_http_tests::{responses, GENERIC, PUMP};
use super::*;
use crate::execution_quote_provider_selection::{
    QUOTE_SOURCE_GENERIC_METIS, QUOTE_SOURCE_PUMP_FUN_PAID,
};
use crate::execution_submit_adapter::{ExecutionBuildPlanMetadata, ExecutionTransactionPlan};
use std::time::Duration;
use tokio::net::TcpListener;

#[tokio::test]
async fn b60_refresh_copies_selected_request_start_before_response() -> Result<()> {
    for paid in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut cfg = super::b58_fixture::config(format!("http://{}", listener.local_addr()?));
        cfg.canary_buy_size_sol = 0.2;
        let mut old = super::execution_build_plan_refresh_contract::pump_fun_metadata();
        old.quote_source = Some(
            if paid {
                QUOTE_SOURCE_PUMP_FUN_PAID
            } else {
                QUOTE_SOURCE_GENERIC_METIS
            }
            .into(),
        );
        old.http_request_started_ts = Some(Utc::now() - chrono::Duration::seconds(60));
        old.quote_request_ts = old.http_request_started_ts;
        let signal = super::execution_state_machine_tiny_submit_route::tiny_route_signal(
            "timing-refresh",
            Utc::now(),
        );
        let client = reqwest::Client::new();
        let before = Utc::now();
        let (fresh, received) = tokio::time::timeout(Duration::from_secs(3), async {
            let replies = [("200 OK", if paid { PUMP } else { GENERIC })];
            tokio::join!(
                crate::execution_build_plan_refresh::refresh_tiny_buy_build_plan_metadata(
                    &client, &cfg, &signal, old
                ),
                responses(listener, &replies, 40)
            )
        })
        .await?;
        let fresh = fresh?;
        let received = received?;
        let actual = fresh.http_request_started_ts.unwrap();
        assert!(actual >= before && actual <= received[0]);
        assert!((fresh.quote_request_ts.unwrap() - actual).num_milliseconds() >= 40);
        assert_eq!(
            fresh.quote_out_amount_raw.as_deref(),
            Some(if paid { "2000000" } else { "1000000" })
        );
        assert!(crate::execution_build_plan_age::quote_age_ms_at_build(&fresh).unwrap() >= 40);
    }
    Ok(())
}
#[tokio::test]
async fn b60_migration_fallback_copies_actual_start_with_new_payload() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let cfg = super::b58_fixture::config(format!("http://{}", listener.local_addr()?));
    let client = reqwest::Client::new();
    let old = Utc::now() - chrono::Duration::seconds(60);
    let plan = plan(ExecutionBuildPlanMetadata {
        http_request_started_ts: Some(old),
        quote_response_available_ts: None,
        quote_request_ts: Some(old),
        ..ExecutionBuildPlanMetadata::default()
    });
    let before = Utc::now();
    let (fallback, received)=tokio::time::timeout(Duration::from_secs(3),async {
        tokio::join!(crate::execution_pump_fun_migration_fallback::refresh_pump_fun_paid_sell_to_generic_pumpswap_plan(&client,&cfg,&plan),responses(listener,&[("200 OK",GENERIC)],40))
    }).await?;
    let result = fallback?;
    let received = received?;
    let actual = result.metadata.http_request_started_ts.unwrap();
    assert!(actual >= before && actual <= received[0]);
    assert!((result.metadata.quote_request_ts.unwrap() - actual).num_milliseconds() >= 40);
    assert_eq!(
        result.metadata.quote_response_json.as_deref(),
        Some(GENERIC)
    );
    assert_eq!(result.order_id, plan.order_id);
    assert!(!result.submit_enabled);
    Ok(())
}
fn plan(metadata: ExecutionBuildPlanMetadata) -> ExecutionTransactionPlan {
    ExecutionTransactionPlan {
        plan_id: "timing".into(),
        order_id: "timing".into(),
        signal_id: "timing".into(),
        client_order_id: "timing".into(),
        attempt: 1,
        route: "metis".into(),
        token: "Token".into(),
        side: "sell".into(),
        buy_size_sol: 0.2,
        slippage_tolerance_bps: 50,
        wallet_pubkey: "synthetic".into(),
        entry_route_plan_json: None,
        metadata,
        swap_blueprint: Some(crate::execution_swap_blueprint::ExecutionSwapBlueprint {
            request_kind: "jupiter_swap_instructions_blueprint".into(),
            quote_event_id: "timing".into(),
            wallet_pubkey: None,
            input_mint: "Token".into(),
            output_mint: super::b58_fixture::SOL.into(),
            input_amount_raw: "200000000".into(),
            output_amount_raw: "1000000".into(),
            slippage_bps: 50.0,
            priority_fee: crate::execution_priority_fee::PriorityFee::TotalPriorityFeeLamports(
                5000,
            ),
            route_labels: vec![],
        }),
        serialized_transaction_payload_slot: None,
        submit_enabled: false,
    }
}
