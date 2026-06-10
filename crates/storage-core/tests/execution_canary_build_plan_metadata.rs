use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{CopySignalRow, Lamports, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS};
use copybot_storage_core::{
    ExecutionCanaryBuildPlanMetadata, ExecutionCanaryBuildPlanMetadataRecordOutcome, SqliteStore,
};
use tempfile::tempdir;

fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("timestamp")
        .with_timezone(&Utc)
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
fn execution_canary_build_plan_metadata_records_and_reports_latest() -> Result<()> {
    let store = open_migrated_store("execution-canary-build-plan-metadata")?;
    let now = ts("2026-06-02T09:00:00Z");
    store.insert_copy_signal(&signal("buy-old", now - Duration::seconds(10)))?;
    store.insert_copy_signal(&signal("buy-new", now))?;
    let old = store.reserve_execution_canary_order("buy-old", "metis-canary", now)?;
    let new = store.reserve_execution_canary_order(
        "buy-new",
        "metis-canary",
        now + Duration::seconds(1),
    )?;

    let old_metadata = metadata_for_order(
        &old.order.order_id,
        "buy-old",
        &old.order.client_order_id,
        now,
    );
    let new_metadata = metadata_for_order(
        &new.order.order_id,
        "buy-new",
        &new.order.client_order_id,
        now + Duration::seconds(1),
    );
    let first = store.record_execution_canary_build_plan_metadata(&old_metadata)?;
    let second = store.record_execution_canary_build_plan_metadata(&new_metadata)?;
    let mut duplicate_metadata = new_metadata.clone();
    duplicate_metadata.recorded_ts = now + Duration::seconds(2);
    duplicate_metadata.quote_out_amount_raw = Some("654321".to_string());
    duplicate_metadata.priority_fee_lamports = Some(33_000);
    let duplicate = store.record_execution_canary_build_plan_metadata(&duplicate_metadata)?;
    let loaded = store
        .load_execution_canary_build_plan_metadata(&new.order.order_id)?
        .expect("metadata should load by order");
    let report = store.execution_canary_status_report(now + Duration::seconds(2))?;
    let latest = report
        .latest_build_plan_metadata
        .expect("status report should expose latest metadata");

    assert_eq!(
        first,
        ExecutionCanaryBuildPlanMetadataRecordOutcome::Inserted
    );
    assert_eq!(
        second,
        ExecutionCanaryBuildPlanMetadataRecordOutcome::Inserted
    );
    assert_eq!(
        duplicate,
        ExecutionCanaryBuildPlanMetadataRecordOutcome::Existing
    );
    assert_eq!(loaded.order_id, new.order.order_id);
    assert_eq!(loaded.quote_event_id.as_deref(), Some("quote:buy-new"));
    assert_eq!(
        loaded.quote_request_ts,
        Some(now + Duration::milliseconds(750))
    );
    assert_eq!(loaded.quote_in_amount_raw.as_deref(), Some("10000000"));
    assert_eq!(loaded.quote_out_amount_raw.as_deref(), Some("654321"));
    assert_eq!(
        loaded.quote_response_json.as_deref(),
        Some("{\"loadedLongtailToken\":true}")
    );
    assert_eq!(loaded.priority_fee_lamports, Some(33_000));
    assert_eq!(loaded.slippage_bps, Some(12.5));
    assert_eq!(loaded.decision_status.as_deref(), Some("would_execute"));
    assert_eq!(latest.order_id, new.order.order_id);
    assert_eq!(latest.priority_fee_source.as_deref(), Some("priority-api"));

    let readiness = store.execution_canary_readiness_summary(now + Duration::seconds(2))?;
    let readiness_latest = readiness.latest.expect("latest readiness order");
    assert_eq!(readiness.readiness_status, "would_enter");
    assert_eq!(
        readiness_latest.quote_event_id.as_deref(),
        Some("quote:buy-new")
    );
    assert_eq!(
        readiness_latest.quote_in_amount_raw.as_deref(),
        Some("10000000")
    );
    assert_eq!(readiness_latest.priority_fee_lamports, Some(33_000));
    Ok(())
}

#[test]
fn execution_canary_readiness_window_groups_recent_decisions() -> Result<()> {
    let store = open_migrated_store("execution-canary-readiness-window")?;
    let now = ts("2026-06-02T10:00:00Z");
    store.insert_copy_signal(&signal("buy-missing", now - Duration::seconds(3)))?;
    store.insert_copy_signal(&signal("buy-enter", now - Duration::seconds(2)))?;
    store.insert_copy_signal(&signal("buy-skip", now - Duration::seconds(1)))?;

    store.reserve_execution_canary_order(
        "buy-missing",
        "metis-canary",
        now - Duration::seconds(3),
    )?;
    let enter = store.reserve_execution_canary_order(
        "buy-enter",
        "metis-canary",
        now - Duration::seconds(2),
    )?;
    let skip = store.reserve_execution_canary_order(
        "buy-skip",
        "metis-canary",
        now - Duration::seconds(1),
    )?;

    store.record_execution_canary_build_plan_metadata(&metadata_for_order(
        &enter.order.order_id,
        "buy-enter",
        &enter.order.client_order_id,
        now - Duration::seconds(2),
    ))?;
    let mut skip_metadata = metadata_for_order(
        &skip.order.order_id,
        "buy-skip",
        &skip.order.client_order_id,
        now - Duration::seconds(1),
    );
    skip_metadata.quote_status = Some("rate_limited".to_string());
    skip_metadata.priority_fee_status = Some("429".to_string());
    skip_metadata.priority_fee_lamports = Some(44_000);
    skip_metadata.price_impact_pct = Some(0.12);
    skip_metadata.decision_status = Some("would_skip".to_string());
    skip_metadata.decision_reason = Some("buy_slippage_above_limit".to_string());
    store.record_execution_canary_build_plan_metadata(&skip_metadata)?;

    let window = store.execution_canary_readiness_window_summary(now, 10)?;

    assert_eq!(window.total_orders, 3);
    assert_eq!(window.metadata_orders, 2);
    assert_eq!(window.missing_metadata_orders, 1);
    assert_eq!(window.would_enter_orders, 1);
    assert_eq!(window.would_skip_orders, 1);
    assert_eq!(window.unknown_orders, 1);
    assert_eq!(window.latest_route.as_deref(), Some("metis-canary"));
    assert_eq!(window.latest_priority_fee_lamports, Some(44_000));
    assert_eq!(window.latest_price_impact_pct, Some(0.12));
    assert_eq!(window.latest_metadata_age_seconds, Some(1));
    assert_count(&window.decision_reasons, "buy_slippage_above_limit", 1);
    assert_count(&window.provider_errors, "quote:rate_limited", 1);
    assert_count(&window.provider_errors, "priority_fee:429", 1);
    assert_count(&window.routes, "metis-canary", 3);
    Ok(())
}

fn assert_count(
    counts: &[copybot_storage_core::ExecutionCanaryReadinessCount],
    key: &str,
    count: u64,
) {
    let actual = counts
        .iter()
        .find(|row| row.key == key)
        .map(|row| row.count);
    assert_eq!(actual, Some(count), "missing count for {key}");
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
        quote_source: Some("execution_quote_canary_event".to_string()),
        quote_event_id: Some(format!("quote:{signal_id}")),
        quote_request_ts: Some(recorded_ts - Duration::milliseconds(250)),
        quote_status: Some("ok".to_string()),
        quote_in_amount_raw: Some("10000000".to_string()),
        quote_out_amount_raw: Some("123456".to_string()),
        quote_response_json: Some("{\"loadedLongtailToken\":true}".to_string()),
        quote_price_sol: Some(0.000081),
        price_impact_pct: Some(0.05),
        route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]".to_string()),
        priority_fee_source: Some("priority-api".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(12_345),
        priority_fee_json: Some("{\"recommended\":12345}".to_string()),
        slippage_bps: Some(12.5),
        decision_status: Some("would_execute".to_string()),
        decision_reason: Some("inside_limits".to_string()),
    }
}
