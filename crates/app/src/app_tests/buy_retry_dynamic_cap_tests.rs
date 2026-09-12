// Imported auditor scenarios for B12/R1; assertions and fixture flow preserved.
use super::buy_retry_queue_fixture::{
    add_pending, add_sell, buy_order, confirmed, queue_fixture, SELL_TOKEN,
};
use super::buy_retry_queue_http_fixture::QueueRpc;
use super::buy_retry_safety_fixture::reopen;
use anyhow::Result;
use chrono::Duration;

#[tokio::test]
async fn b12_auditor_successful_receipt_reaches_open_cap_after_selection() -> Result<()> {
    let mut f = queue_fixture("b12-auditor-dynamic-open", true).await?;
    f.config.canary_batch_limit = 3;
    f.config.canary_max_open_positions = 1;
    let pending = add_pending(&f, true)?;
    let original = buy_order(&f)?;
    let metadata = f
        .store
        .load_execution_canary_build_plan_metadata(&original.order_id)?;
    assert_eq!(f.store.execution_canary_open_position_count()?, 0);
    assert!(!f.store.execution_canary_accounting_pending()?);
    let mut rpc = QueueRpc::new(&mut f, false).await?;
    reopen(&mut f)?;
    let summary = f.sweep().await?;
    rpc.finish().await?;
    assert_eq!(
        summary.existing, 2,
        "both orders must have been selected: {summary:?}"
    );
    assert_eq!(summary.safety_blocked, 1, "{summary:?}");
    assert_eq!(summary.skipped_reason, Some("max_open_positions"));
    confirmed(&f, &pending)?;
    assert!(!f.store.execution_canary_accounting_pending()?);
    assert_eq!(f.store.execution_canary_open_position_count()?, 1);
    assert_eq!(buy_order(&f)?, original);
    assert_eq!(
        f.store
            .load_execution_canary_build_plan_metadata(&original.order_id)?,
        metadata
    );
    assert_eq!(
        rpc.trace(),
        [
            "getSignatureStatuses:receipt-signature",
            "getTransaction:receipt-signature"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn b12_auditor_successful_sell_receipt_reaches_loss_cap_after_selection() -> Result<()> {
    let mut f = queue_fixture("b12-auditor-dynamic-loss", true).await?;
    f.config.canary_batch_limit = 3;
    f.config.canary_max_open_positions = 10;
    f.config.canary_max_daily_loss_sol = 0.02;
    // Create the known SELL before the existing BUY, keeping the submitted timestamp causal.
    f.now -= Duration::seconds(10);
    let sell = add_sell(&f, false)?;
    f.now += Duration::seconds(10);
    f.store.mark_execution_canary_submitted(
        &sell,
        f.now - Duration::seconds(9),
        "b12-sell-signature",
    )?;
    let conn = rusqlite::Connection::open(&f.db_path)?;
    assert_eq!(
        conn.execute(
            "UPDATE positions SET cost_sol=0.03, cost_lamports=30000000 WHERE token=?1",
            [SELL_TOKEN]
        )?,
        1
    );
    drop(conn);
    let original = buy_order(&f)?;
    let metadata = f
        .store
        .load_execution_canary_build_plan_metadata(&original.order_id)?;
    assert_eq!(
        f.store
            .execution_canary_entry_safety_loss_sol_since(f.now - Duration::minutes(1))?,
        0.0
    );
    assert!(!f.store.execution_canary_accounting_pending()?);
    let mut rpc = QueueRpc::new(&mut f, false).await?;
    reopen(&mut f)?;
    let summary = f.sweep().await?;
    rpc.finish().await?;
    assert_eq!(
        summary.existing, 2,
        "both orders must have been selected: {summary:?}"
    );
    assert_eq!(summary.safety_blocked, 1, "{summary:?}");
    assert_eq!(summary.skipped_reason, Some("max_daily_loss"));
    confirmed(&f, &sell)?;
    assert!(!f.store.execution_canary_accounting_pending()?);
    assert_eq!(f.store.execution_canary_open_position_count()?, 0);
    assert_eq!(
        f.store
            .execution_canary_entry_safety_loss_sol_since(f.now - Duration::minutes(1))?,
        0.02
    );
    assert_eq!(buy_order(&f)?, original);
    assert_eq!(
        f.store
            .load_execution_canary_build_plan_metadata(&original.order_id)?,
        metadata
    );
    assert_eq!(
        rpc.trace(),
        [
            "getSignatureStatuses:b12-sell-signature",
            "getTransaction:b12-sell-signature"
        ]
    );
    Ok(())
}
