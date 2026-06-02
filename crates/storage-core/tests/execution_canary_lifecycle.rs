use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::{CopySignalRow, Lamports, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS};
use copybot_storage_core::{
    SqliteStore, EXECUTION_CANARY_CONFIRM_DECISION_EXPIRE_UNSAFE,
    EXECUTION_CANARY_CONFIRM_DECISION_RETRY, EXECUTION_CANARY_CONFIRM_DECISION_WAIT,
    EXECUTION_ERROR_EXPIRED, EXECUTION_SIMULATION_STATUS_PASSED, EXECUTION_STATUS_CANARY_BUILT,
    EXECUTION_STATUS_CANARY_EXPIRED, EXECUTION_STATUS_CANARY_SIMULATED,
    EXECUTION_STATUS_CANARY_SUBMITTED,
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

fn mark_simulated_order(
    store: &SqliteStore,
    signal_id: &str,
    now: DateTime<Utc>,
) -> Result<String> {
    store.insert_copy_signal(&signal(signal_id, now))?;
    let reserve = store.reserve_execution_canary_order(signal_id, "metis-canary", now)?;
    store.mark_execution_canary_built(&reserve.order.order_id, now + Duration::seconds(1))?;
    store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        now + Duration::seconds(2),
        EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    Ok(reserve.order.order_id)
}

#[test]
fn execution_canary_timeout_can_expire_built_order() -> Result<()> {
    let store = open_migrated_store("execution-canary-expire-built")?;
    let now = ts("2026-05-30T08:00:00Z");
    store.insert_copy_signal(&signal("buy-built", now))?;
    let reserve = store.reserve_execution_canary_order("buy-built", "metis-canary", now)?;
    let built =
        store.mark_execution_canary_built(&reserve.order.order_id, now + Duration::seconds(1))?;
    assert_eq!(built.status, EXECUTION_STATUS_CANARY_BUILT);

    let expired = store.mark_execution_canary_expired(
        &reserve.order.order_id,
        now + Duration::minutes(5),
        "timeout_after_built",
    )?;
    let report = store.execution_canary_status_report(now + Duration::minutes(5))?;

    assert_eq!(expired.status, EXECUTION_STATUS_CANARY_EXPIRED);
    assert_eq!(expired.err_code.as_deref(), Some(EXECUTION_ERROR_EXPIRED));
    assert_eq!(
        expired.simulation_error.as_deref(),
        Some("timeout_after_built")
    );
    assert!(expired.confirm_ts.is_some());
    assert_eq!(report.expired, 1);
    assert_eq!(report.active_count(), 0);
    Ok(())
}

#[test]
fn execution_canary_confirm_timeout_waits_before_deadline() -> Result<()> {
    let store = open_migrated_store("execution-canary-confirm-wait")?;
    let now = ts("2026-05-30T08:00:00Z");
    let order_id = mark_simulated_order(&store, "buy-submit-wait", now)?;
    let submitted =
        store.mark_execution_canary_submitted(&order_id, now + Duration::seconds(3), "tx-sig")?;

    let decision = store.execution_canary_confirm_timeout_decision(
        &order_id,
        now + Duration::seconds(33),
        Duration::seconds(60),
    )?;
    let retry_error = store
        .mark_execution_canary_retry_after_submit_timeout(
            &order_id,
            now + Duration::seconds(33),
            Duration::seconds(60),
            "retry_before_timeout",
        )
        .expect_err("submitted order must not retry before confirm timeout");

    assert_eq!(submitted.status, EXECUTION_STATUS_CANARY_SUBMITTED);
    assert_eq!(submitted.submit_ts, now + Duration::seconds(3));
    assert_eq!(
        decision.decision_status,
        EXECUTION_CANARY_CONFIRM_DECISION_WAIT
    );
    assert_eq!(decision.elapsed_seconds, 30);
    assert!(format!("{retry_error:#}").contains("confirm_timeout_not_reached"));
    Ok(())
}

#[test]
fn execution_canary_confirm_timeout_blocks_retry_when_signature_exists() -> Result<()> {
    let store = open_migrated_store("execution-canary-confirm-signature")?;
    let now = ts("2026-05-30T08:00:00Z");
    let order_id = mark_simulated_order(&store, "buy-submit-signature", now)?;
    store.mark_execution_canary_submitted(&order_id, now + Duration::seconds(3), "tx-sig")?;

    let decision = store.execution_canary_confirm_timeout_decision(
        &order_id,
        now + Duration::seconds(64),
        Duration::seconds(60),
    )?;
    let retry_error = store
        .mark_execution_canary_retry_after_submit_timeout(
            &order_id,
            now + Duration::seconds(64),
            Duration::seconds(60),
            "retry_with_signature",
        )
        .expect_err("submitted order with tx signature must not retry");
    let expired = store.mark_execution_canary_expired(
        &order_id,
        now + Duration::seconds(65),
        "confirm_timeout_with_signature",
    )?;

    assert_eq!(
        decision.decision_status,
        EXECUTION_CANARY_CONFIRM_DECISION_EXPIRE_UNSAFE
    );
    assert_eq!(
        decision.decision_reason,
        "tx_signature_present_retry_unsafe"
    );
    assert!(format!("{retry_error:#}").contains("tx_signature_present_retry_unsafe"));
    assert_eq!(expired.status, EXECUTION_STATUS_CANARY_EXPIRED);
    assert_eq!(expired.err_code.as_deref(), Some(EXECUTION_ERROR_EXPIRED));
    Ok(())
}

#[test]
fn execution_canary_submitted_unknown_can_retry_after_timeout() -> Result<()> {
    let store = open_migrated_store("execution-canary-confirm-retry")?;
    let now = ts("2026-05-30T08:00:00Z");
    let order_id = mark_simulated_order(&store, "buy-submit-unknown", now)?;
    let submitted = store.mark_execution_canary_submitted_unknown(
        &order_id,
        now + Duration::seconds(3),
        "submit_returned_no_signature",
    )?;

    let decision = store.execution_canary_confirm_timeout_decision(
        &order_id,
        now + Duration::seconds(64),
        Duration::seconds(60),
    )?;
    let retry = store.mark_execution_canary_retry_after_submit_timeout(
        &order_id,
        now + Duration::seconds(64),
        Duration::seconds(60),
        "retry_after_unknown_submit_timeout",
    )?;
    let resubmitted = store.mark_execution_canary_submitted(
        &order_id,
        now + Duration::seconds(65),
        "tx-sig-2",
    )?;

    assert_eq!(submitted.status, EXECUTION_STATUS_CANARY_SUBMITTED);
    assert!(submitted.tx_signature.is_none());
    assert_eq!(
        decision.decision_status,
        EXECUTION_CANARY_CONFIRM_DECISION_RETRY
    );
    assert_eq!(decision.decision_reason, "no_tx_signature_retry_safe");
    assert_eq!(retry.status, EXECUTION_STATUS_CANARY_SIMULATED);
    assert_eq!(retry.attempt, 2);
    assert!(retry.tx_signature.is_none());
    assert_eq!(
        retry.simulation_error.as_deref(),
        Some("retry_after_unknown_submit_timeout")
    );
    assert_eq!(resubmitted.status, EXECUTION_STATUS_CANARY_SUBMITTED);
    assert_eq!(resubmitted.tx_signature.as_deref(), Some("tx-sig-2"));
    Ok(())
}

#[test]
fn execution_canary_timeout_rejects_terminal_order_expiry() -> Result<()> {
    let store = open_migrated_store("execution-canary-expire-terminal")?;
    let now = ts("2026-05-30T08:00:00Z");
    store.insert_copy_signal(&signal("buy-terminal", now))?;
    let reserve = store.reserve_execution_canary_order("buy-terminal", "metis-canary", now)?;
    store.mark_execution_canary_expired(
        &reserve.order.order_id,
        now + Duration::minutes(5),
        "timeout_after_candidate",
    )?;

    let error = store
        .mark_execution_canary_expired(
            &reserve.order.order_id,
            now + Duration::minutes(6),
            "second_timeout",
        )
        .expect_err("terminal expired order must not expire twice");
    let error_chain = format!("{error:#}");
    assert!(
        error_chain.contains("invalid execution canary transition"),
        "unexpected error: {error_chain}"
    );
    Ok(())
}
