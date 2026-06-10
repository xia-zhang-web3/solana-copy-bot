use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{CopySignalRow, Lamports, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS};
use copybot_storage_core::{ExecutionCanaryBuildPlanMetadata, SqliteStore};
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
fn stale_not_run_candidate_query_requires_age_and_missing_metadata() -> Result<()> {
    let store = open_migrated_store("stale-not-run-candidate-query")?;
    let now = ts("2026-06-10T02:30:00Z");
    let route = "metis-swap-instructions-dry-run";
    let old = bare_sell_candidate_order(&store, "stale-candidate-old", route, now)?;
    let fresh = bare_sell_candidate_order(
        &store,
        "stale-candidate-fresh",
        route,
        now + Duration::seconds(60),
    )?;
    let with_metadata = bare_sell_candidate_order(&store, "stale-candidate-metadata", route, now)?;
    let other_route = bare_sell_candidate_order(&store, "stale-candidate-other", "other", now)?;
    let with_metadata_order = store
        .load_execution_canary_order(&with_metadata)?
        .expect("metadata candidate order");
    store.record_execution_canary_build_plan_metadata(&metadata_for_order(
        &with_metadata_order.order_id,
        &with_metadata_order.signal_id,
        &with_metadata_order.client_order_id,
        now + Duration::seconds(1),
    ))?;

    let one = store.list_stale_not_run_execution_canary_candidates_for_route(
        route,
        now + Duration::seconds(30),
        1,
    )?;
    let all = store.list_stale_not_run_execution_canary_candidates_for_route(
        route,
        now + Duration::seconds(30),
        10,
    )?;

    assert_eq!(one.len(), 1);
    assert_eq!(one[0].order_id, old);
    assert_eq!(
        all.iter()
            .map(|order| order.order_id.as_str())
            .collect::<Vec<_>>(),
        vec![old.as_str()]
    );
    assert!(!all.iter().any(|order| order.order_id == fresh));
    assert!(!all.iter().any(|order| order.order_id == with_metadata));
    assert!(!all.iter().any(|order| order.order_id == other_route));
    Ok(())
}

fn bare_sell_candidate_order(
    store: &SqliteStore,
    signal_id: &str,
    route: &str,
    now: DateTime<Utc>,
) -> Result<String> {
    let sell_signal = CopySignalRow {
        side: "sell".to_string(),
        ..signal(signal_id, now)
    };
    store.insert_copy_signal(&sell_signal)?;
    let reserve = store.reserve_execution_canary_order(signal_id, route, now)?;
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
        quote_request_ts: None,
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
