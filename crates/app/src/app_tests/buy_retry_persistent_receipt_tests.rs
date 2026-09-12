// Imported auditor scenarios for B12/R1; assertions and fixture flow preserved.
use super::buy_retry_queue_fixture::*;
use super::buy_retry_queue_http_fixture::QueueRpc;
use super::buy_retry_safety_fixture::reopen;
use super::ExecutionCanaryRunner;
use anyhow::Result;
use rusqlite::Connection;

async fn run_persistent_receipt_case(remove_buy: bool) -> Result<()> {
    let mut f = queue_fixture(
        &format!("auditor-b12-persistent-receipt-{remove_buy}"),
        false,
    )
    .await?;
    f.config.canary_entry_submit_enabled = false;
    f.config.canary_batch_limit = 1;
    let original_buy = buy_order(&f)?;
    if remove_buy {
        // Fixture-only control: no unsigned BUY exists, hence no BUY safety policy
        // can be responsible for the two-query starvation.
        let conn = Connection::open(&f.db_path)?;
        conn.execute(
            "DELETE FROM execution_canary_build_plan_metadata WHERE order_id=?1",
            [&original_buy.order_id],
        )?;
        conn.execute(
            "DELETE FROM orders WHERE order_id=?1",
            [&original_buy.order_id],
        )?;
    }
    let sell = add_sell(&f, false)?; // supported NotSent suffix
    let pending = add_pending(&f, false)?;
    let mut rpc = QueueRpc::new(&mut f, true).await?; // receipt never becomes available
    for n in 0..3 {
        reopen(&mut f)?;
        let summary = ExecutionCanaryRunner::new(f.config.clone())
            .process_tick(&f.store, f.now + chrono::Duration::seconds(4 + n))
            .await?;
        assert!(summary.state_machine_existing <= 1, "{summary:?}");
        if remove_buy {
            assert!(f
                .store
                .load_execution_canary_order(&original_buy.order_id)?
                .is_none());
        } else {
            assert_eq!(buy_order(&f)?, original_buy);
        }
    }
    rpc.finish().await?;
    let sell_state = f.store.load_execution_canary_order(&sell)?.unwrap();
    let pending_state = f.store.load_execution_canary_order(&pending)?.unwrap();
    eprintln!(
        "auditor B12 remove_buy={remove_buy}, sell={}, pending={}, trace={:?}",
        sell_state.status,
        pending_state.status,
        rpc.trace()
    );
    assert_eq!(
        pending_state.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED
    );
    // Expected RED on both B11 and B12: SELL remains SIMULATED, 0 sell sends.
    // UNKNOWN query selects the persistent receipt each tick, remaining=0;
    // NotSent query is never reached. With one combined ordered query, tick 2
    // should select SELL because tick 1 advanced receipt last_attempt_at.
    confirmed(&f, &sell)?;
    assert_eq!(
        rpc.trace()
            .iter()
            .filter(|v| *v == "sendTransaction:sell")
            .count(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn b12_auditor_persistent_receipt_not_sent_sell_with_blocked_buy() -> Result<()> {
    run_persistent_receipt_case(false).await
}

#[tokio::test]
async fn b12_auditor_persistent_receipt_not_sent_sell_without_any_buy() -> Result<()> {
    run_persistent_receipt_case(true).await
}
