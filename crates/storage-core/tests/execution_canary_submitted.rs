use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{CopySignalRow, Lamports, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS};
use copybot_storage_core::{
    ExecutionCanaryBuildPlanMetadata, SqliteStore, EXECUTION_ERROR_SIMULATION_FAILED,
    EXECUTION_SIMULATION_STATUS_FAILED, EXECUTION_SIMULATION_STATUS_NOT_RUN,
    EXECUTION_SIMULATION_STATUS_PASSED, EXECUTION_STATUS_CANARY_CANDIDATE,
    EXECUTION_STATUS_CANARY_CONFIRMED,
};
use tempfile::tempdir;

fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("timestamp")
        .with_timezone(&Utc)
}

fn open_migrated_store(name: &str) -> Result<SqliteStore> {
    let dir = tempdir()?;
    let db_path = dir.keep().join(format!("{name}.db"));
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    Ok(store)
}

#[test]
fn submitted_canary_order_query_is_bounded_to_route_and_status() -> Result<()> {
    let store = open_migrated_store("submitted-canary-query")?;
    let now = ts("2026-06-08T10:00:00Z");
    let old = submitted_order(
        &store,
        "buy-old",
        "metis-swap-instructions-dry-run",
        "tx-old",
        now,
    )?;
    let new = submitted_order(
        &store,
        "buy-new",
        "metis-swap-instructions-dry-run",
        "tx-new",
        now + Duration::seconds(10),
    )?;
    let other_route = submitted_order(&store, "buy-other", "other-route", "tx-other", now)?;
    let confirmed = submitted_order(
        &store,
        "buy-confirmed",
        "metis-swap-instructions-dry-run",
        "tx-confirmed",
        now,
    )?;
    store.mark_execution_canary_confirmed(&confirmed, now + Duration::seconds(1))?;
    let no_sig = submitted_without_signature(
        &store,
        "buy-no-sig",
        "metis-swap-instructions-dry-run",
        now + Duration::seconds(5),
    )?;
    let retry_ready = retry_ready_simulated_order(
        &store,
        "buy-retry-ready",
        "metis-swap-instructions-dry-run",
        now + Duration::seconds(6),
    )?;

    let retry_reason = "retry_after_unknown_submit_timeout";
    let one = store.list_reconcilable_execution_canary_orders_for_route(
        "metis-swap-instructions-dry-run",
        retry_reason,
        1,
    )?;
    let all = store.list_reconcilable_execution_canary_orders_for_route(
        "metis-swap-instructions-dry-run",
        retry_reason,
        10,
    )?;

    assert_eq!(one.len(), 1);
    assert_eq!(one[0].order_id, old);
    assert_eq!(
        all.iter()
            .map(|order| order.order_id.as_str())
            .collect::<Vec<_>>(),
        vec![
            old.as_str(),
            no_sig.as_str(),
            new.as_str(),
            retry_ready.as_str()
        ]
    );
    assert!(!all.iter().any(|order| order.order_id == other_route));
    assert!(!all
        .iter()
        .any(|order| order.status == EXECUTION_STATUS_CANARY_CONFIRMED));
    Ok(())
}

#[test]
fn submit_risk_summary_counts_pending_retry_ready_and_budget_blockers() -> Result<()> {
    let store = open_migrated_store("submit-risk-summary")?;
    let now = ts("2026-06-08T11:00:00Z");
    let signed = submitted_order(
        &store,
        "risk-signed",
        "metis-swap-instructions-dry-run",
        "tx-signed",
        now,
    )?;
    let no_sig = submitted_without_signature(
        &store,
        "risk-no-sig",
        "metis-swap-instructions-dry-run",
        now + Duration::seconds(10),
    )?;
    let retry_ready = retry_ready_simulated_order(
        &store,
        "risk-retry-ready",
        "metis-swap-instructions-dry-run",
        now + Duration::seconds(20),
    )?;
    let confirmed = submitted_order(
        &store,
        "risk-confirmed",
        "metis-swap-instructions-dry-run",
        "tx-confirmed",
        now + Duration::seconds(30),
    )?;
    store.mark_execution_canary_confirmed(&confirmed, now + Duration::seconds(35))?;

    let summary = store.execution_canary_submit_risk_summary(
        now + Duration::seconds(40),
        "retry_after_unknown_submit_timeout",
        1,
    )?;

    assert_eq!(summary.active_orders, 3);
    assert_eq!(summary.submitted_orders, 2);
    assert_eq!(summary.submitted_with_signature_orders, 1);
    assert_eq!(summary.submitted_without_signature_orders, 1);
    assert_eq!(summary.retry_ready_orders, 1);
    assert_eq!(summary.retry_budget_blocked_orders, 2);
    assert_eq!(summary.max_active_attempt, 2);
    let latest = summary
        .latest_active_order
        .expect("latest active order should exist");
    assert_eq!(latest.order_id, retry_ready);
    assert_eq!(latest.attempt, 2);
    assert!(!latest.tx_signature_present);
    assert_ne!(signed, no_sig);
    Ok(())
}

#[test]
fn failed_simulation_retry_candidate_clears_stale_build_metadata() -> Result<()> {
    let store = open_migrated_store("failed-simulation-retry-candidate")?;
    let now = ts("2026-06-08T12:00:00Z");
    let signal_id = "sell-failed-simulation";
    store.insert_copy_signal(&signal(signal_id, now))?;
    let reserve =
        store.reserve_execution_canary_order(signal_id, "metis-swap-instructions-dry-run", now)?;
    store.mark_execution_canary_built(&reserve.order.order_id, now + Duration::seconds(1))?;
    store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        now + Duration::seconds(2),
        EXECUTION_SIMULATION_STATUS_FAILED,
        Some("old simulation failed"),
    )?;
    store.mark_execution_canary_failed(
        &reserve.order.order_id,
        now + Duration::seconds(3),
        EXECUTION_ERROR_SIMULATION_FAILED,
        "old simulation failed",
    )?;
    store.record_execution_canary_build_plan_metadata(&metadata_for_order(
        &reserve.order.order_id,
        signal_id,
        &reserve.order.client_order_id,
        now,
    ))?;

    let retry = store.mark_execution_canary_failed_simulation_retry_candidate(
        &reserve.order.order_id,
        now + Duration::seconds(4),
        "retry_failed_sell_with_owned_position_amount",
    )?;

    assert_eq!(retry.status, EXECUTION_STATUS_CANARY_CANDIDATE);
    assert_eq!(retry.attempt, 2);
    assert_eq!(retry.err_code, None);
    assert_eq!(
        retry.simulation_status.as_deref(),
        Some(EXECUTION_SIMULATION_STATUS_NOT_RUN)
    );
    assert_eq!(
        retry.simulation_error.as_deref(),
        Some("retry_failed_sell_with_owned_position_amount")
    );
    assert!(store
        .load_execution_canary_build_plan_metadata(&reserve.order.order_id)?
        .is_none());
    Ok(())
}

fn retry_ready_simulated_order(
    store: &SqliteStore,
    signal_id: &str,
    route: &str,
    now: DateTime<Utc>,
) -> Result<String> {
    let order_id = submitted_without_signature(store, signal_id, route, now)?;
    store.mark_execution_canary_retry_after_submit_timeout(
        &order_id,
        now + Duration::seconds(10),
        Duration::seconds(1),
        "retry_after_unknown_submit_timeout",
    )?;
    Ok(order_id)
}

fn submitted_order(
    store: &SqliteStore,
    signal_id: &str,
    route: &str,
    tx_signature: &str,
    now: DateTime<Utc>,
) -> Result<String> {
    store.insert_copy_signal(&signal(signal_id, now))?;
    let reserve = store.reserve_execution_canary_order(signal_id, route, now)?;
    store.mark_execution_canary_built(&reserve.order.order_id, now + Duration::seconds(1))?;
    store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        now + Duration::seconds(2),
        EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    store.mark_execution_canary_submitted(
        &reserve.order.order_id,
        now + Duration::seconds(3),
        tx_signature,
    )?;
    Ok(reserve.order.order_id)
}

fn submitted_without_signature(
    store: &SqliteStore,
    signal_id: &str,
    route: &str,
    now: DateTime<Utc>,
) -> Result<String> {
    store.insert_copy_signal(&signal(signal_id, now))?;
    let reserve = store.reserve_execution_canary_order(signal_id, route, now)?;
    store.mark_execution_canary_built(&reserve.order.order_id, now + Duration::seconds(1))?;
    store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        now + Duration::seconds(2),
        EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    store.mark_execution_canary_submitted_unknown(
        &reserve.order.order_id,
        now + Duration::seconds(3),
        "timeout_without_signature",
    )?;
    Ok(reserve.order.order_id)
}

fn signal(signal_id: &str, ts: DateTime<Utc>) -> CopySignalRow {
    CopySignalRow {
        signal_id: signal_id.to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: "buy".to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn metadata_for_order(
    order_id: &str,
    signal_id: &str,
    client_order_id: &str,
    recorded_ts: DateTime<Utc>,
) -> ExecutionCanaryBuildPlanMetadata {
    ExecutionCanaryBuildPlanMetadata {
        order_id: order_id.to_string(),
        signal_id: signal_id.to_string(),
        client_order_id: client_order_id.to_string(),
        recorded_ts,
        quote_source: Some("test".to_string()),
        quote_event_id: Some("quote:old".to_string()),
        quote_status: Some("ok".to_string()),
        quote_in_amount_raw: Some("200000".to_string()),
        quote_out_amount_raw: Some("1000".to_string()),
        quote_response_json: None,
        quote_price_sol: Some(1.0),
        price_impact_pct: None,
        route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]".to_string()),
        priority_fee_source: Some("test".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(1),
        priority_fee_json: None,
        slippage_bps: Some(0.0),
        decision_status: Some("would_execute".to_string()),
        decision_reason: Some("within_slippage_limit".to_string()),
    }
}
