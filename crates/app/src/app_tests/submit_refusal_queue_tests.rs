use super::buy_retry_queue_fixture::*;
use super::buy_retry_queue_http_fixture::QueueRpc;
use super::buy_retry_safety_fixture::reopen;
use super::submit_refusal_fixture::{capture, check_event};
use anyhow::Result;

#[tokio::test]
async fn submit_refusal_actual_tick_preserves_a_after_successful_sell_b_and_clean_reopen(
) -> Result<()> {
    let mut f = queue_fixture("b26-r1-queue", false).await?;
    f.config.canary_max_open_positions = 10;
    f.config.canary_batch_limit = 2;
    let a = buy_order(&f)?;
    let b = add_sell(&f, false)?;
    assert_eq!(
        crate::execution_canary_safety::pre_submit_safety_snapshot(&f.config, &f.store, f.now)?
            .blocked_reason,
        None
    );
    let mut rpc = QueueRpc::with_postawait_change(&mut f, true).await?;
    let tick = super::entry_risk_clock_fixture::at(
        f.now + chrono::Duration::seconds(3),
        super::ExecutionCanaryRunner::new(f.config.clone()).process_tick(&f.store, f.now),
    )
    .await?;
    assert!(tick.has_status_change());
    assert_eq!(tick.state_machine_safety_blocked, 0);
    assert_eq!(tick.state_machine_failed, 0);
    let preserved = buy_order(&f)?;
    assert_eq!(
        preserved.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SIMULATED
    );
    assert!(preserved.tx_signature.is_none() && preserved.err_code.is_none());
    confirmed(&f, &b)?;
    assert_eq!(
        tick.last_state_machine_order_id.as_deref(),
        Some(b.as_str())
    );
    let event = capture(|| crate::telemetry::record_execution_canary_tick(&tick));
    check_event(&event, &a.order_id, "initial_sol_order_changed", 1);
    assert_eq!(event["last_state_machine_order_id"], b);
    assert_eq!(
        rpc.trace()
            .iter()
            .filter(|s| *s == "sendTransaction:sell")
            .count(),
        1
    );
    assert!(!rpc.trace().iter().any(|s| s == "sendTransaction:buy"));
    assert_eq!(
        rpc.trace()
            .iter()
            .filter(|s| s.starts_with("funding:"))
            .count(),
        4
    );

    // A remains retryable by design. Pause new entry only after proving the first
    // tick allowed A/B; the subsequent receipt-only ticks must not replay its diagnostic.
    f.config.canary_entry_submit_enabled = false;
    // New periodic summary after reopen, with a legacy BUY receipt: no stale refusal.
    let pending = add_pending(&f, false)?;
    reopen(&mut f)?;
    let clean = super::entry_risk_clock_fixture::at(
        f.now + chrono::Duration::seconds(8),
        super::ExecutionCanaryRunner::new(f.config.clone())
            .process_tick(&f.store, f.now + chrono::Duration::seconds(4)),
    )
    .await?;
    check_event(
        &capture(|| crate::telemetry::record_execution_canary_tick(&clean)),
        "none",
        "none",
        0,
    );
    assert_eq!(buy_order(&f)?, preserved);
    *rpc.pending_receipt.lock().unwrap() = false;
    reopen(&mut f)?;
    let recovered = super::entry_risk_clock_fixture::at(
        f.now + chrono::Duration::seconds(20),
        super::ExecutionCanaryRunner::new(f.config.clone())
            .process_tick(&f.store, f.now + chrono::Duration::seconds(10)),
    )
    .await?;
    rpc.finish().await?;
    check_event(
        &capture(|| crate::telemetry::record_execution_canary_tick(&recovered)),
        "none",
        "none",
        0,
    );
    confirmed(&f, &pending)?;
    assert_eq!(buy_order(&f)?, preserved);
    assert_eq!(
        rpc.trace()
            .iter()
            .filter(|s| s.starts_with("funding:"))
            .count(),
        4
    );
    assert_eq!(
        rpc.trace()
            .iter()
            .filter(|s| *s == "sendTransaction:sell")
            .count(),
        1
    );
    assert_eq!(
        rusqlite::Connection::open(&f.db_path)?.query_row(
            "SELECT COUNT(*) FROM execution_failed_expense_ledger",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        0
    );
    Ok(())
}

#[test]
fn submit_refusal_bounded_pair_merge_does_not_promote_arbitrary_error() {
    use crate::execution_submit_refusal::{PreSubmitRefusal, PreSubmitRefusals};
    let mut summary = PreSubmitRefusals::default();
    summary.record(Some(PreSubmitRefusal::after_collection(
        "A",
        "initial_sol_order_changed",
    )));
    summary.merge(PreSubmitRefusals::default());
    assert_eq!(
        (summary.count(), summary.order_id(), summary.reason()),
        (1, "A", "initial_sol_order_changed")
    );
    summary.record(Some(PreSubmitRefusal::after_collection(
        "C",
        "SYNTHETIC_PRIVATE_PAYLOAD",
    )));
    let tick = crate::execution_canary::ExecutionCanaryTickSummary {
        pre_submit_refusals: summary,
        last_state_machine_order_id: Some("B".into()),
        last_error: Some("SYNTHETIC_PRIVATE_PAYLOAD".into()),
        ..Default::default()
    };
    assert!(tick.has_status_change());
    check_event(
        &capture(|| crate::telemetry::record_execution_canary_tick(&tick)),
        "C",
        "pre_submit_refused",
        2,
    );
}

#[test]
fn submit_refusal_future_footprint() {
    assert_eq!(
        std::mem::size_of::<crate::execution_submit_refusal::PreSubmitRefusals>(),
        std::mem::size_of::<usize>()
    );
    eprintln!(
        "B26_R1_FOOTPRINT diagnostic={} submit_outcome={} state_summary={} tick_summary={}",
        std::mem::size_of::<crate::execution_submit_refusal::PreSubmitRefusals>(),
        std::mem::size_of::<crate::execution_canary_submit_contract::ExecutionSubmitPlanOutcome>(),
        std::mem::size_of::<
            crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary,
        >(),
        std::mem::size_of::<crate::execution_canary::ExecutionCanaryTickSummary>()
    );
}
