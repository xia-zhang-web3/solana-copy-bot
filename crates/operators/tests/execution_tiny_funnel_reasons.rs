use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{CopySignalRow, Lamports, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS};
use copybot_operators::execution_canary_quote_pnl::build_report_from_db_path;
use copybot_storage_core::{
    ExecutionCanaryBuildPlanMetadata, ExecutionQuoteCanaryEventInsert, SqliteStore,
    EXECUTION_ERROR_SIMULATION_FAILED, EXECUTION_SIMULATION_STATUS_FAILED,
};
use tempfile::tempdir;

const ROUTE: &str = "metis-swap-instructions-dry-run";

fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("timestamp")
        .with_timezone(&Utc)
}

#[test]
fn report_explains_shadow_dropped_entry_flow() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;

    let base = ts("2026-06-09T12:00:00Z");
    record_buy_drop(
        &store,
        "cooldown",
        "TokenCooldown",
        "would_execute",
        "recent_sell_cooldown",
        base,
    )?;
    record_buy_drop(
        &store,
        "below-notional",
        "TokenBelowNotional",
        "would_execute",
        "below_notional",
        base + Duration::seconds(1),
    )?;
    record_buy_drop(
        &store,
        "skip",
        "TokenSkip",
        "would_skip",
        "entry_slippage_too_high",
        base + Duration::seconds(2),
    )?;
    drop(store);

    let report = build_report_from_db_path(&db_path, ts("2026-06-09T13:00:00Z"));
    let proof = report
        .tiny_execution_proof
        .as_ref()
        .unwrap_or_else(|| panic!("tiny proof missing: {:?}", report.error));
    let quality = report
        .tiny_execution_quality
        .as_ref()
        .unwrap_or_else(|| panic!("tiny quality missing: {:?}", report.error));

    assert_eq!(proof.entry_funnel.shadow_dropped_events, 3);
    assert_eq!(
        proof.entry_funnel.quote_would_execute_shadow_dropped_events,
        2
    );
    assert_reason(
        &proof.entry_funnel.shadow_drop_reason_counts,
        "entry_slippage_too_high",
        1,
    );
    assert_reason(
        &proof
            .entry_funnel
            .quote_would_execute_shadow_drop_reason_counts,
        "recent_sell_cooldown",
        1,
    );
    assert!(proof.entry_funnel.buckets.iter().any(|bucket| {
        bucket.shadow_gate_status == "shadow_dropped"
            && bucket.shadow_gate_reason == "below_notional"
            && bucket.events == 1
    }));

    assert_eq!(quality.verdict, "entry_flow_loss");
    assert_eq!(quality.shadow_gate_dropped_would_execute_events, 2);
    assert!(quality.top_flow_blockers.iter().any(|blocker| {
        blocker.stage == "shadow_gate"
            && blocker.reason == "shadow_dropped:recent_sell_cooldown"
            && blocker.count == 1
    }));
    Ok(())
}

#[test]
fn quality_includes_failed_entry_order_samples() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("runtime.db");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;

    let signal_ts = ts("2026-06-09T12:10:00Z");
    insert_signal(
        &store,
        "buy-market-not-found",
        "buy",
        "TokenMarketNotFound",
        signal_ts,
    )?;
    store.record_execution_quote_canary_event(&buy_quote(
        "market-not-found",
        "buy-market-not-found",
        "TokenMarketNotFound",
        signal_ts,
        "would_execute",
    ))?;
    record_failed_buy_order(&store, "buy-market-not-found", signal_ts)?;
    drop(store);

    let report = build_report_from_db_path(&db_path, ts("2026-06-09T13:00:00Z"));
    let quality = report
        .tiny_execution_quality
        .as_ref()
        .unwrap_or_else(|| panic!("tiny quality missing: {:?}", report.error));

    assert_eq!(quality.failed_entry_order_samples.len(), 1);
    let sample = &quality.failed_entry_order_samples[0];
    assert_eq!(sample.signal_id, "buy-market-not-found");
    assert_eq!(sample.token.as_deref(), Some("TokenMarketNotFound"));
    assert_eq!(
        sample.err_code.as_deref(),
        Some(EXECUTION_ERROR_SIMULATION_FAILED)
    );
    assert_eq!(sample.simulation_error_class, "market_not_found");
    assert_eq!(
        sample.quote_source.as_deref(),
        Some("execution_quote_canary_provider:generic_metis")
    );
    assert_eq!(sample.decision_reason.as_deref(), Some("fresh_entry"));
    Ok(())
}

fn record_buy_drop(
    store: &SqliteStore,
    suffix: &str,
    token: &str,
    decision_status: &str,
    drop_reason: &str,
    signal_ts: DateTime<Utc>,
) -> Result<()> {
    let signal_id = format!("buy-shadow-{suffix}");
    store.record_execution_quote_canary_event(&buy_quote(
        suffix,
        &signal_id,
        token,
        signal_ts,
        decision_status,
    ))?;
    store.record_execution_quote_canary_shadow_gate_event(
        &signal_id,
        "leader-wallet",
        token,
        "buy",
        "shadow_dropped",
        Some(drop_reason),
        signal_ts + Duration::milliseconds(20),
    )?;
    Ok(())
}

fn buy_quote(
    suffix: &str,
    signal_id: &str,
    token: &str,
    signal_ts: DateTime<Utc>,
    decision_status: &str,
) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: format!("quote:buy:shadow-drop:{suffix}"),
        signal_id: Some(signal_id.to_string()),
        shadow_closed_trade_id: None,
        wallet_id: "leader-wallet".to_string(),
        token: token.to_string(),
        side: "buy".to_string(),
        quote_status: "ok".to_string(),
        request_ts: signal_ts + Duration::milliseconds(10),
        signal_ts: Some(signal_ts),
        decision_delay_ms: Some(10),
        quote_latency_ms: Some(20),
        leader_notional_sol: Some(0.2),
        quote_in_amount_raw: Some("10000000".to_string()),
        quote_out_amount_raw: Some("500000".to_string()),
        quote_response_json: None,
        quote_price_sol: Some(0.0001),
        shadow_price_sol: Some(0.0001),
        slippage_bps: Some(12.5),
        price_impact_pct: Some(0.01),
        route_plan_json: None,
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(10_000),
        priority_fee_json: None,
        decision_status: Some(decision_status.to_string()),
        decision_reason: Some("inside_test_limit".to_string()),
        error: None,
    }
}

fn insert_signal(
    store: &SqliteStore,
    signal_id: &str,
    side: &str,
    token: &str,
    signal_ts: DateTime<Utc>,
) -> Result<()> {
    store.insert_copy_signal(&CopySignalRow {
        signal_id: signal_id.to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: side.to_string(),
        token: token.to_string(),
        notional_sol: 0.01,
        notional_lamports: Some(Lamports::new(10_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts: signal_ts,
        status: "shadow_recorded".to_string(),
    })?;
    Ok(())
}

fn record_failed_buy_order(
    store: &SqliteStore,
    signal_id: &str,
    signal_ts: DateTime<Utc>,
) -> Result<String> {
    let reserve = store.reserve_execution_canary_order(signal_id, ROUTE, signal_ts)?;
    store.record_execution_canary_build_plan_metadata(&ExecutionCanaryBuildPlanMetadata {
        order_id: reserve.order.order_id.clone(),
        signal_id: reserve.order.signal_id.clone(),
        client_order_id: reserve.order.client_order_id.clone(),
        recorded_ts: signal_ts + Duration::milliseconds(10),
        quote_source: Some("execution_quote_canary_provider:generic_metis".to_string()),
        quote_event_id: Some("quote:buy:market-not-found".to_string()),
        quote_status: Some("ok".to_string()),
        quote_in_amount_raw: Some("10000000".to_string()),
        quote_out_amount_raw: Some("500000".to_string()),
        quote_response_json: None,
        quote_price_sol: Some(0.0001),
        price_impact_pct: Some(0.01),
        route_plan_json: Some("[{\"swapInfo\":{\"label\":\"Metis\"}}]".to_string()),
        priority_fee_source: Some("execution_quote_canary_event".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(12_345),
        priority_fee_json: Some("{\"recommended\":12345}".to_string()),
        slippage_bps: Some(12.5),
        decision_status: Some("would_execute".to_string()),
        decision_reason: Some("fresh_entry".to_string()),
    })?;
    store.mark_execution_canary_built(
        &reserve.order.order_id,
        signal_ts + Duration::milliseconds(20),
    )?;
    store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        signal_ts + Duration::milliseconds(40),
        EXECUTION_SIMULATION_STATUS_FAILED,
        Some("Transaction simulation failed: market not found"),
    )?;
    store.mark_execution_canary_failed(
        &reserve.order.order_id,
        signal_ts + Duration::milliseconds(60),
        EXECUTION_ERROR_SIMULATION_FAILED,
        "Transaction simulation failed: market not found",
    )?;
    Ok(reserve.order.order_id)
}

fn assert_reason(
    counts: &[copybot_storage_core::ExecutionTinyEntryFunnelDropReasonCount],
    reason: &str,
    events: u64,
) {
    assert!(
        counts
            .iter()
            .any(|count| count.reason == reason && count.events == events),
        "missing reason {reason}: {counts:?}"
    );
}
