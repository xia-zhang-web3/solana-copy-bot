use super::{
    buy_retry_queue_fixture::*, buy_retry_queue_http_fixture::QueueRpc,
    buy_retry_safety_fixture::reopen,
};
use anyhow::Result;

#[tokio::test]
async fn b53_accounting_blocker_keeps_entry_on_and_allows_unrelated_sell_and_reconciliation(
) -> Result<()> {
    let mut f = queue_fixture("b50-entry-on-pending-sell", false).await?;
    f.config.canary_entry_submit_enabled = true;
    f.config.canary_max_open_positions = 10;
    f.config.canary_batch_limit = 1;
    let buy = buy_order(&f)?;
    let pending = add_pending(&f, true)?;
    let sell = add_sell(&f, false)?;
    let mut rpc = QueueRpc::new(&mut f, true).await?;
    // Older submitted receipt is picked first, then real RPC confirmation makes
    // it confirmed_unreconciled. No synthetic crash SQL or disabled entry flag.
    for n in 0..3 {
        reopen(&mut f)?;
        super::entry_risk_clock_fixture::at(
            f.now + chrono::Duration::seconds(8 + n),
            crate::execution_canary::ExecutionCanaryRunner::new(f.config.clone())
                .process_tick(&f.store, f.now + chrono::Duration::seconds(4 + n)),
        )
        .await?;
        assert_eq!(buy_order(&f)?, buy);
        assert_eq!(
            f.store
                .load_execution_canary_order(&pending)?
                .unwrap()
                .status,
            copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED
        );
        assert_eq!(
            crate::execution_canary_safety::pre_submit_safety_snapshot(&f.config, &f.store, f.now)?
                .blocked_reason,
            Some(copybot_storage_core::EXECUTION_ACCOUNTING_PENDING_REASON)
        );
    }
    confirmed(&f, &sell)?;
    rpc.finish().await?;
    let trace = rpc.trace();
    assert_eq!(
        trace
            .iter()
            .filter(|r| *r == "sendTransaction:sell")
            .count(),
        1
    );
    assert!(!trace
        .iter()
        .any(|r| r == "buy-quote" || r == "sendTransaction:buy"));
    assert!(
        trace
            .iter()
            .filter(|r| r.starts_with("getTransaction:"))
            .count()
            >= 3
    );
    println!("B53_QUEUE entry_enabled=true blocker=confirmed_accounting_pending buy_send=0 sell_send=1 pending_reopened=true trace={trace:?}");
    Ok(())
}

#[tokio::test]
async fn b53_ordinary_pending_and_accounting_failure_do_not_starve_sell_limit_one() -> Result<()> {
    for unavailable_accounting in [false, true] {
        let mut f = queue_fixture("b53-pending-limit-one", false).await?;
        f.config.canary_entry_submit_enabled = true;
        f.config.canary_max_open_positions = 10;
        f.config.canary_batch_limit = 1;
        let buy = buy_order(&f)?;
        let pending = add_pending(&f, true)?;
        let sell = add_sell(&f, false)?;
        let mut rpc = super::buy_retry_queue_http_fixture::QueueRpc::new(&mut f, false).await?;
        *rpc.ordinary_pending.lock().unwrap() = !unavailable_accounting;
        if unavailable_accounting {
            rusqlite::Connection::open(&f.db_path)?.execute_batch(
                "CREATE TRIGGER reject_buy_accounting BEFORE INSERT ON fills
                 WHEN NEW.order_id='exec-canary:b12-known'
                 BEGIN SELECT RAISE(ABORT,'synthetic unavailable BUY accounting'); END;",
            )?;
        }
        let first = super::entry_risk_clock_fixture::at(
            f.now + chrono::Duration::seconds(8),
            crate::execution_canary::ExecutionCanaryRunner::new(f.config.clone())
                .process_tick(&f.store, f.now + chrono::Duration::seconds(4)),
        )
        .await;
        assert_eq!(first.is_err(), unavailable_accounting, "{first:?}");
        let retained = f.store.load_execution_canary_order(&pending)?.unwrap();
        assert_eq!(
            retained.status,
            if unavailable_accounting {
                copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED
            } else {
                copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMITTED
            }
        );
        for n in 1..4 {
            reopen(&mut f)?;
            let result = super::entry_risk_clock_fixture::at(
                f.now + chrono::Duration::seconds(8 + n),
                crate::execution_canary::ExecutionCanaryRunner::new(f.config.clone())
                    .process_tick(&f.store, f.now + chrono::Duration::seconds(4 + n)),
            )
            .await;
            if !unavailable_accounting {
                result?;
            }
            assert_eq!(buy_order(&f)?, buy);
            assert_eq!(
                f.store.load_execution_canary_order(&pending)?.unwrap(),
                retained,
                "A identity and reason must survive B's progress"
            );
        }
        confirmed(&f, &sell)?;
        rpc.finish().await?;
        let trace = rpc.trace();
        assert_eq!(
            trace
                .iter()
                .filter(|r| *r == "sendTransaction:sell")
                .count(),
            1
        );
        assert!(!trace
            .iter()
            .any(|r| r == "buy-quote" || r == "sendTransaction:buy"));
        assert!(
            trace
                .iter()
                .filter(|r| r.ends_with("receipt-signature"))
                .count()
                >= 2
        );
        println!("B53_QUEUE_LIMIT1 accounting_unavailable={unavailable_accounting} retained_id={} retained_reason={:?} trace={trace:?}",retained.order_id,retained.simulation_error);
    }
    Ok(())
}
