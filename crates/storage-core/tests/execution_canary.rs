use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{
    CopySignalRow, Lamports, TokenQuantity, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
};
use copybot_storage_core::{
    ExecutionCanaryRecordOutcome, ExecutionDryRunRecordOutcome, ExecutionQuoteCanaryEventInsert,
    ExecutionQuoteCanaryRecordOutcome, SqliteStore, EXECUTION_ERROR_SUBMIT_DISABLED,
    EXECUTION_SIMULATION_STATUS_DRY_RUN_SKIPPED, EXECUTION_SIMULATION_STATUS_NOT_RUN,
    EXECUTION_SIMULATION_STATUS_PASSED, EXECUTION_SIMULATION_STATUS_SKIPPED_NO_SUBMIT,
    EXECUTION_STATUS_CANARY_BUILT, EXECUTION_STATUS_CANARY_CANDIDATE,
    EXECUTION_STATUS_CANARY_CONFIRMED, EXECUTION_STATUS_CANARY_SIMULATED,
    EXECUTION_STATUS_CANARY_SUBMITTED, EXECUTION_STATUS_CANARY_SUBMIT_DISABLED,
    EXECUTION_STATUS_DRY_RUN_CONFIRMED,
};
use tempfile::tempdir;

fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("timestamp")
        .with_timezone(&Utc)
}

fn signal(signal_id: &str, side: &str, ts: DateTime<Utc>) -> CopySignalRow {
    CopySignalRow {
        signal_id: signal_id.to_string(),
        wallet_id: "leader-wallet".to_string(),
        side: side.to_string(),
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
fn execution_canary_candidates_skip_signals_that_already_have_orders() -> Result<()> {
    let store = open_migrated_store("execution-canary-candidates")?;
    let now = ts("2026-05-30T08:00:00Z");
    store.insert_copy_signal(&signal("buy-old", "buy", now - Duration::seconds(20)))?;
    store.insert_copy_signal(&signal("buy-new", "buy", now - Duration::seconds(10)))?;
    store.record_execution_dry_run_order("buy-new", "dry_run", now)?;

    let candidates = store.list_execution_canary_candidates(
        "shadow_recorded",
        now - Duration::minutes(1),
        10,
    )?;

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].signal_id, "buy-old");
    Ok(())
}

#[test]
fn execution_canary_candidates_are_entry_only_and_ignore_sells() -> Result<()> {
    let store = open_migrated_store("execution-canary-entry-only")?;
    let now = ts("2026-05-30T08:00:00Z");
    store.insert_copy_signal(&signal("sell-fresh", "sell", now - Duration::seconds(5)))?;
    store.insert_copy_signal(&signal("buy-fresh", "buy", now - Duration::seconds(20)))?;

    let candidates = store.list_execution_canary_candidates(
        "shadow_recorded",
        now - Duration::minutes(1),
        10,
    )?;

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].signal_id, "buy-fresh");
    assert_eq!(candidates[0].side, "buy");
    Ok(())
}

#[test]
fn execution_canary_candidates_ignore_stale_signals() -> Result<()> {
    let store = open_migrated_store("execution-canary-freshness")?;
    let now = ts("2026-05-30T08:00:00Z");
    store.insert_copy_signal(&signal("buy-old", "buy", now - Duration::minutes(10)))?;
    store.insert_copy_signal(&signal("buy-fresh", "buy", now - Duration::seconds(20)))?;

    let candidates = store.list_execution_canary_candidates(
        "shadow_recorded",
        now - Duration::minutes(1),
        10,
    )?;

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].signal_id, "buy-fresh");
    Ok(())
}

#[test]
fn execution_dry_run_order_recording_is_idempotent_per_signal() -> Result<()> {
    let store = open_migrated_store("execution-canary-idempotency")?;
    let now = ts("2026-05-30T08:00:00Z");
    store.insert_copy_signal(&signal("buy-1", "buy", now))?;

    let first = store.record_execution_dry_run_order("buy-1", "metis-dry-run", now)?;
    let second = store.record_execution_dry_run_order("buy-1", "metis-dry-run", now)?;
    let latest = store
        .latest_execution_dry_run_order()?
        .expect("dry-run order should exist");

    assert_eq!(first, ExecutionDryRunRecordOutcome::Inserted);
    assert_eq!(second, ExecutionDryRunRecordOutcome::Existing);
    assert_eq!(latest.signal_id, "buy-1");
    assert_eq!(latest.route, "metis-dry-run");
    assert_eq!(latest.status, EXECUTION_STATUS_DRY_RUN_CONFIRMED);
    assert_eq!(
        latest.simulation_status,
        EXECUTION_SIMULATION_STATUS_DRY_RUN_SKIPPED
    );
    assert_eq!(latest.attempt, 1);
    Ok(())
}

#[test]
fn execution_canary_order_reservation_is_idempotent_per_signal() -> Result<()> {
    let store = open_migrated_store("execution-canary-state-idempotency")?;
    let now = ts("2026-05-30T08:00:00Z");
    store.insert_copy_signal(&signal("buy-1", "buy", now))?;

    let first = store.reserve_execution_canary_order("buy-1", "metis-canary", now)?;
    let second = store.reserve_execution_canary_order("buy-1", "metis-canary", now)?;

    assert_eq!(first.outcome, ExecutionCanaryRecordOutcome::Inserted);
    assert_eq!(second.outcome, ExecutionCanaryRecordOutcome::Existing);
    assert_eq!(first.order.order_id, second.order.order_id);
    assert_eq!(first.order.status, EXECUTION_STATUS_CANARY_CANDIDATE);
    assert_eq!(
        first.order.simulation_status.as_deref(),
        Some(EXECUTION_SIMULATION_STATUS_NOT_RUN)
    );
    assert!(first.order.tx_signature.is_none());
    Ok(())
}

#[test]
fn execution_canary_order_reservation_ignores_existing_dry_run_order() -> Result<()> {
    let store = open_migrated_store("execution-canary-state-after-dry-run")?;
    let now = ts("2026-05-30T08:00:00Z");
    store.insert_copy_signal(&signal("buy-1", "buy", now))?;
    let dry_run = store.record_execution_dry_run_order("buy-1", "dry-run", now)?;

    let reserve = store.reserve_execution_canary_order(
        "buy-1",
        "metis-canary",
        now + Duration::seconds(1),
    )?;

    assert_eq!(dry_run, ExecutionDryRunRecordOutcome::Inserted);
    assert_eq!(reserve.outcome, ExecutionCanaryRecordOutcome::Inserted);
    assert!(reserve.order.order_id.starts_with("exec-canary:"));
    assert_eq!(
        store
            .load_execution_canary_order_by_signal("buy-1")?
            .expect("canary order should exist")
            .order_id,
        reserve.order.order_id
    );
    Ok(())
}

#[test]
fn execution_canary_order_transitions_follow_expected_lifecycle() -> Result<()> {
    let store = open_migrated_store("execution-canary-state-transitions")?;
    let now = ts("2026-05-30T08:00:00Z");
    store.insert_copy_signal(&signal("buy-1", "buy", now))?;
    let reserve = store.reserve_execution_canary_order("buy-1", "metis-canary", now)?;

    let built =
        store.mark_execution_canary_built(&reserve.order.order_id, now + Duration::seconds(1))?;
    assert_eq!(built.status, EXECUTION_STATUS_CANARY_BUILT);
    let simulated = store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        now + Duration::seconds(2),
        EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    assert_eq!(simulated.status, EXECUTION_STATUS_CANARY_SIMULATED);
    assert_eq!(
        simulated.simulation_status.as_deref(),
        Some(EXECUTION_SIMULATION_STATUS_PASSED)
    );
    let submitted = store.mark_execution_canary_submitted(
        &reserve.order.order_id,
        now + Duration::seconds(3),
        "tx-signature",
    )?;
    assert_eq!(submitted.status, EXECUTION_STATUS_CANARY_SUBMITTED);
    assert_eq!(submitted.tx_signature.as_deref(), Some("tx-signature"));
    let confirmed = store
        .mark_execution_canary_confirmed(&reserve.order.order_id, now + Duration::seconds(4))?;
    assert_eq!(confirmed.status, EXECUTION_STATUS_CANARY_CONFIRMED);
    assert!(confirmed.confirm_ts.is_some());
    Ok(())
}

#[test]
fn execution_canary_order_submit_disabled_after_simulation_preserves_simulation_result(
) -> Result<()> {
    let store = open_migrated_store("execution-canary-state-submit-disabled-after-sim")?;
    let now = ts("2026-05-30T08:00:00Z");
    store.insert_copy_signal(&signal("buy-1", "buy", now))?;
    let reserve = store.reserve_execution_canary_order("buy-1", "metis-canary", now)?;
    store.mark_execution_canary_built(&reserve.order.order_id, now + Duration::seconds(1))?;
    store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        now + Duration::seconds(2),
        EXECUTION_SIMULATION_STATUS_SKIPPED_NO_SUBMIT,
        Some("simulation_skipped"),
    )?;

    let disabled = store.mark_execution_canary_submit_disabled(
        &reserve.order.order_id,
        now + Duration::seconds(3),
        "submit_disabled_after_simulation",
    )?;

    assert_eq!(disabled.status, EXECUTION_STATUS_CANARY_SUBMIT_DISABLED);
    assert_eq!(
        disabled.err_code.as_deref(),
        Some(EXECUTION_ERROR_SUBMIT_DISABLED)
    );
    assert_eq!(
        disabled.simulation_status.as_deref(),
        Some(EXECUTION_SIMULATION_STATUS_SKIPPED_NO_SUBMIT)
    );
    assert_eq!(
        disabled.simulation_error.as_deref(),
        Some("simulation_skipped")
    );
    Ok(())
}

#[test]
fn execution_canary_order_rejects_unsafe_transition_after_terminal_state() -> Result<()> {
    let store = open_migrated_store("execution-canary-state-terminal")?;
    let now = ts("2026-05-30T08:00:00Z");
    store.insert_copy_signal(&signal("buy-1", "buy", now))?;
    let reserve = store.reserve_execution_canary_order("buy-1", "metis-canary", now)?;
    let disabled = store.mark_execution_canary_submit_disabled(
        &reserve.order.order_id,
        now + Duration::seconds(1),
        "no_submit_adapter",
    )?;

    assert_eq!(disabled.status, EXECUTION_STATUS_CANARY_SUBMIT_DISABLED);
    assert_eq!(
        disabled.err_code.as_deref(),
        Some(EXECUTION_ERROR_SUBMIT_DISABLED)
    );
    assert_eq!(
        disabled.simulation_status.as_deref(),
        Some(EXECUTION_SIMULATION_STATUS_SKIPPED_NO_SUBMIT)
    );
    assert!(disabled.confirm_ts.is_some());
    let error = store
        .mark_execution_canary_built(&reserve.order.order_id, now + Duration::seconds(2))
        .expect_err("terminal submit-disabled order must not transition back to built");
    let error_chain = format!("{error:#}");
    assert!(
        error_chain.contains("invalid execution canary transition"),
        "unexpected error: {error_chain}"
    );
    Ok(())
}

#[test]
fn execution_canary_status_report_counts_statuses_and_latest_error() -> Result<()> {
    let store = open_migrated_store("execution-canary-status-report")?;
    let now = ts("2026-05-30T08:00:00Z");
    store.insert_copy_signal(&signal("buy-candidate", "buy", now))?;
    store.insert_copy_signal(&signal("buy-built", "buy", now + Duration::seconds(1)))?;
    store.insert_copy_signal(&signal("buy-disabled", "buy", now + Duration::seconds(2)))?;

    let candidate = store.reserve_execution_canary_order("buy-candidate", "metis-canary", now)?;
    let built = store.reserve_execution_canary_order(
        "buy-built",
        "metis-canary",
        now + Duration::seconds(1),
    )?;
    store.mark_execution_canary_built(&built.order.order_id, now + Duration::seconds(2))?;
    let disabled = store.reserve_execution_canary_order(
        "buy-disabled",
        "metis-canary",
        now + Duration::seconds(3),
    )?;
    store.mark_execution_canary_submit_disabled(
        &disabled.order.order_id,
        now + Duration::seconds(4),
        "no_submit_adapter",
    )?;

    let report = store.execution_canary_status_report(now + Duration::seconds(5))?;

    assert_eq!(candidate.order.status, EXECUTION_STATUS_CANARY_CANDIDATE);
    assert_eq!(report.total, 3);
    assert_eq!(report.candidate, 1);
    assert_eq!(report.built, 1);
    assert_eq!(report.submit_disabled, 1);
    assert_eq!(report.active_count(), 2);
    assert_eq!(
        report
            .latest_order
            .as_ref()
            .expect("latest order should exist")
            .signal_id,
        "buy-disabled"
    );
    assert_eq!(
        report
            .latest_error_order
            .as_ref()
            .expect("latest error order should exist")
            .signal_id,
        "buy-disabled"
    );
    Ok(())
}

#[test]
fn execution_quote_entry_candidates_skip_recorded_quote_events() -> Result<()> {
    let store = open_migrated_store("execution-quote-entry")?;
    let now = ts("2026-05-30T08:00:00Z");
    store.insert_copy_signal(&signal("buy-quoted", "buy", now - Duration::seconds(20)))?;
    store.insert_copy_signal(&signal("buy-open", "buy", now - Duration::seconds(10)))?;

    let event = quote_event(
        "quote:entry:buy-quoted",
        Some("buy-quoted"),
        None,
        "buy",
        now,
    );
    let first = store.record_execution_quote_canary_event(&event)?;
    let second = store.record_execution_quote_canary_event(&event)?;
    let candidates = store.list_execution_quote_canary_entry_candidates(
        "shadow_recorded",
        now - Duration::minutes(1),
        10,
    )?;

    assert_eq!(first, ExecutionQuoteCanaryRecordOutcome::Inserted);
    assert_eq!(second, ExecutionQuoteCanaryRecordOutcome::Existing);
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].signal_id, "buy-open");
    Ok(())
}

#[test]
fn execution_quote_close_candidates_skip_recorded_quote_events() -> Result<()> {
    let store = open_migrated_store("execution-quote-close")?;
    let now = ts("2026-05-30T08:00:00Z");
    store.insert_shadow_closed_trade_exact(
        "close-quoted",
        "leader-wallet",
        "TokenMint",
        12.34,
        Some(TokenQuantity::new(1_234, 2)),
        0.2,
        0.24,
        0.04,
        now - Duration::minutes(2),
        now - Duration::seconds(20),
    )?;
    store.insert_shadow_closed_trade_exact(
        "close-open",
        "leader-wallet",
        "TokenMint",
        10.0,
        Some(TokenQuantity::new(1_000, 2)),
        0.2,
        0.18,
        -0.02,
        now - Duration::minutes(1),
        now - Duration::seconds(10),
    )?;

    let event = quote_event("quote:close:1", Some("close-quoted"), Some(1), "sell", now);
    store.record_execution_quote_canary_event(&event)?;
    let candidates =
        store.list_execution_quote_canary_close_candidates(now - Duration::minutes(1), 10)?;

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].signal_id, "close-open");
    assert_eq!(candidates[0].qty_raw.as_deref(), Some("1000"));
    assert_eq!(candidates[0].qty_decimals, Some(2));
    Ok(())
}

#[test]
fn execution_quote_close_candidates_can_filter_by_signal() -> Result<()> {
    let store = open_migrated_store("execution-quote-close-by-signal")?;
    let now = ts("2026-05-30T08:00:00Z");
    store.insert_shadow_closed_trade_exact(
        "close-target",
        "leader-wallet",
        "TokenMint",
        12.34,
        Some(TokenQuantity::new(1_234, 2)),
        0.2,
        0.24,
        0.04,
        now - Duration::minutes(2),
        now - Duration::seconds(20),
    )?;
    store.insert_shadow_closed_trade_exact(
        "close-other",
        "leader-wallet",
        "TokenMint",
        10.0,
        Some(TokenQuantity::new(1_000, 2)),
        0.2,
        0.18,
        -0.02,
        now - Duration::minutes(1),
        now - Duration::seconds(10),
    )?;

    let candidates =
        store.list_execution_quote_canary_close_candidates_for_signal("close-target", 10)?;

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].signal_id, "close-target");
    assert_eq!(candidates[0].qty_raw.as_deref(), Some("1234"));
    assert_eq!(candidates[0].qty_decimals, Some(2));
    Ok(())
}

fn quote_event(
    event_id: &str,
    signal_id: Option<&str>,
    shadow_closed_trade_id: Option<i64>,
    side: &str,
    now: DateTime<Utc>,
) -> ExecutionQuoteCanaryEventInsert {
    ExecutionQuoteCanaryEventInsert {
        event_id: event_id.to_string(),
        signal_id: signal_id.map(ToString::to_string),
        shadow_closed_trade_id,
        wallet_id: "leader-wallet".to_string(),
        token: "TokenMint".to_string(),
        side: side.to_string(),
        quote_status: "ok".to_string(),
        request_ts: now,
        signal_ts: Some(now - Duration::seconds(1)),
        decision_delay_ms: Some(1_000),
        quote_latency_ms: Some(20),
        leader_notional_sol: Some(0.2),
        quote_in_amount_raw: Some("200000000".to_string()),
        quote_out_amount_raw: Some("1000".to_string()),
        quote_price_sol: Some(0.02),
        shadow_price_sol: Some(0.02),
        slippage_bps: Some(0.0),
        price_impact_pct: Some(0.01),
        route_plan_json: Some("[]".to_string()),
        priority_fee_status: Some("ok".to_string()),
        priority_fee_lamports: Some(10),
        priority_fee_json: Some("{}".to_string()),
        decision_status: Some("would_execute".to_string()),
        decision_reason: Some("within_slippage_limit".to_string()),
        error: None,
    }
}
