use super::buy_retry_queue_fixture::*;
use super::buy_retry_queue_http_fixture::QueueRpc;
use super::buy_retry_safety_fixture::reopen;
use anyhow::Result;

#[tokio::test]
async fn initial_sol_limit_one_failed_buy_does_not_starve_sell_or_legacy_receipt() -> Result<()> {
    let mut f = queue_fixture("b26-funding-limit-one", false).await?;
    f.config.canary_max_open_positions = 10;
    f.config.canary_batch_limit = 1;
    let buy = buy_order(&f)?;
    let sell = add_sell(&f, false)?;
    let safety =
        crate::execution_canary_safety::pre_submit_safety_snapshot(&f.config, &f.store, f.now)?;
    assert_eq!(safety.blocked_reason, None, "{safety:?}");
    let mut rpc = QueueRpc::with_initial_sol_failure(&mut f, true).await?;
    let first = super::entry_risk_clock_fixture::at(
        f.now + chrono::Duration::seconds(3),
        super::ExecutionCanaryRunner::new(f.config.clone()).process_tick(&f.store, f.now),
    )
    .await?;
    assert_eq!(first.state_machine_existing, 1, "{first:?}");
    let failed = buy_order(&f)?;
    assert_eq!(
        failed.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED
    );
    assert!(failed
        .simulation_error
        .as_deref()
        .unwrap()
        .starts_with("initial_sol_insufficient:"));
    assert_eq!(failed.attempt, buy.attempt);
    assert!(failed.tx_signature.is_none());
    let pending = add_pending(&f, false)?;
    for n in 0..3 {
        reopen(&mut f)?;
        super::entry_risk_clock_fixture::at(
            f.now + chrono::Duration::seconds(8 + n),
            super::ExecutionCanaryRunner::new(f.config.clone())
                .process_tick(&f.store, f.now + chrono::Duration::seconds(4 + n)),
        )
        .await?;
        assert_eq!(buy_order(&f)?, failed);
    }
    confirmed(&f, &sell)?;
    let absent = f.store.load_execution_canary_order(&pending)?.unwrap();
    assert_eq!(
        absent.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED
    );
    *rpc.pending_receipt.lock().unwrap() = false;
    reopen(&mut f)?;
    super::entry_risk_clock_fixture::at(
        f.now + chrono::Duration::seconds(20),
        super::ExecutionCanaryRunner::new(f.config.clone())
            .process_tick(&f.store, f.now + chrono::Duration::seconds(10)),
    )
    .await?;
    rpc.finish().await?;
    confirmed(&f, &pending)?;
    let trace = rpc.trace();
    assert_eq!(
        trace
            .iter()
            .filter(|m| *m == "sendTransaction:sell")
            .count(),
        1
    );
    assert_eq!(
        trace.iter().filter(|m| m.starts_with("funding:")).count(),
        3
    );
    assert!(!trace.iter().any(|m| m == "sendTransaction:buy"));
    assert_eq!(
        rusqlite::Connection::open(&f.db_path)?.query_row(
            "SELECT COUNT(*) FROM execution_failed_expense_ledger",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        0
    );
    eprintln!("B26_QUEUE limit=1 funding_requests=3 buy_sends=0 sell_sends=1 legacy_receipt_reopened=true trace={trace:?}");
    Ok(())
}
