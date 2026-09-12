use super::buy_retry_queue_fixture::*;
use super::buy_retry_queue_http_fixture::QueueRpc;
use super::buy_retry_safety_fixture::{reopen, Block};
use super::ExecutionCanaryRunner;
use anyhow::Result;

#[tokio::test]
async fn buy_retry_queue_limit_one_preserves_sell_and_known_receipt_for_both_reasons() -> Result<()>
{
    for unknown in [false, true] {
        let mut f = queue_fixture(&format!("b12-queue-{unknown}"), unknown).await?;
        f.config.canary_entry_submit_enabled = false;
        f.config.canary_batch_limit = 1;
        let sell = add_sell(&f, unknown)?;
        let pending = add_pending(&f, false)?;
        let original = buy_order(&f)?;
        let old_metadata = f
            .store
            .load_execution_canary_build_plan_metadata(&original.order_id)?;
        let mut rpc = QueueRpc::new(&mut f, false).await?;
        for _ in 0..3 {
            reopen(&mut f)?;
            let s = ExecutionCanaryRunner::new(f.config.clone())
                .process_tick(&f.store, f.now + chrono::Duration::seconds(4))
                .await?;
            assert!(s.state_machine_existing <= 1, "{s:?}");
            assert_eq!(buy_order(&f)?, original);
            assert_eq!(
                f.store
                    .load_execution_canary_build_plan_metadata(&original.order_id)?,
                old_metadata
            );
        }
        rpc.finish().await?;
        confirmed(&f, &sell)?;
        confirmed(&f, &pending)?;
        assert_eq!(
            rpc.trace()
                .iter()
                .filter(|v| v.starts_with("sendTransaction"))
                .count(),
            1
        );
        assert!(rpc
            .trace()
            .iter()
            .any(|v| v == "getTransaction:receipt-signature"));
        assert!(f.calls().is_empty());
        eprintln!("B12 limit=1 unknown={unknown}: {:?}", rpc.trace());
    }
    Ok(())
}

#[tokio::test]
async fn buy_retry_queue_each_entry_guard_allows_sell_and_receipt_recovery() -> Result<()> {
    for block in [Block::Disabled, Block::Loss, Block::Open] {
        let mut f = queue_fixture(&format!("b12-sell-guard-{block:?}"), false).await?;
        f.config.canary_max_open_positions = 10;
        let sell = add_sell(&f, false)?;
        let pending = add_pending(&f, false)?;
        block.apply(&mut f)?;
        let original = buy_order(&f)?;
        let mut rpc = QueueRpc::new(&mut f, false).await?;
        for _ in 0..3 {
            f.sweep().await?;
        }
        rpc.finish().await?;
        confirmed(&f, &sell)?;
        confirmed(&f, &pending)?;
        assert_eq!(buy_order(&f)?, original);
        assert_eq!(
            rpc.trace()
                .iter()
                .filter(|v| v.starts_with("sendTransaction"))
                .count(),
            1
        );
    }
    Ok(())
}

#[tokio::test]
async fn buy_retry_queue_rechecks_after_receipt_makes_accounting_pending() -> Result<()> {
    let mut f = queue_fixture("b12-selection-race", true).await?;
    f.config.canary_max_open_positions = 10;
    f.config.canary_batch_limit = 3;
    let pending = add_pending(&f, true)?;
    let sell = add_sell(&f, true)?;
    assert!(!f.store.execution_canary_accounting_pending()?);
    let original = buy_order(&f)?;
    let mut rpc = QueueRpc::new(&mut f, true).await?;
    let s = f.sweep().await?;
    assert_eq!(
        s.existing, 3,
        "selection must include BUY before first receipt: {s:?}"
    );
    assert_eq!(s.safety_blocked, 1);
    assert_eq!(s.skipped_reason, Some("confirmed_accounting_pending"));
    assert!(f.store.execution_canary_accounting_pending()?);
    assert_eq!(
        f.store
            .load_execution_canary_order(&pending)?
            .unwrap()
            .status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED
    );
    assert_eq!(buy_order(&f)?, original);
    confirmed(&f, &sell)?;
    assert_eq!(
        rpc.trace()
            .iter()
            .filter(|v| v.starts_with("sendTransaction"))
            .count(),
        1
    );
    assert!(rpc.trace()[0].starts_with("getSignatureStatuses:receipt-signature"));
    // Previously sent recovery continues while new BUY entry remains disabled.
    f.config.canary_entry_submit_enabled = false;
    f.config.canary_batch_limit = 1;
    *rpc.pending_receipt.lock().unwrap() = false;
    f.sweep().await?;
    rpc.finish().await?;
    confirmed(&f, &pending)?;
    assert_eq!(buy_order(&f)?, original);
    eprintln!(
        "B12 selection/candidate change and recovery: {:?}",
        rpc.trace()
    );
    Ok(())
}

#[tokio::test]
async fn buy_retry_queue_unknown_exhausted_budget_expires_while_entry_blocked() -> Result<()> {
    let mut f = queue_fixture("b12-unknown-expiry", true).await?;
    f.config.canary_entry_submit_enabled = false;
    f.config.canary_batch_limit = 1;
    f.config.max_submit_attempts = 2;
    let original = buy_order(&f)?;
    assert_eq!(original.attempt, 3);
    let mut rpc = QueueRpc::new(&mut f, false).await?;
    reopen(&mut f)?;
    let s = f.sweep().await?;
    rpc.finish().await?;
    assert_eq!(s.expired, 1);
    let after = buy_order(&f)?;
    assert_eq!(
        after.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_EXPIRED
    );
    assert_eq!(after.attempt, original.attempt);
    assert_eq!(after.client_order_id, original.client_order_id);
    assert!(after.tx_signature.is_none());
    assert!(rpc.trace().is_empty());
    assert!(!f.store.execution_canary_fill_exists(&after.order_id)?);
    Ok(())
}

#[tokio::test]
async fn buy_retry_queue_kill_switch_and_disabled_keep_runner_quiet() -> Result<()> {
    for kill in [false, true] {
        let mut f = queue_fixture(&format!("b12-kill-{kill}"), false).await?;
        add_sell(&f, false)?;
        add_pending(&f, false)?;
        let flag = f.db_path.with_extension("kill");
        if kill {
            std::fs::write(&flag, b"test-only")?;
            f.config.canary_kill_switch_path = flag.to_string_lossy().into_owned();
        } else {
            f.config.canary_enabled = false;
        }
        let mut rpc = QueueRpc::new(&mut f, false).await?;
        let before = super::buy_retry_safety_fixture::rows(&f)?;
        let s = ExecutionCanaryRunner::new(f.config.clone())
            .process_tick(&f.store, f.now)
            .await?;
        rpc.finish().await?;
        assert_eq!(
            s.skipped_reason,
            Some(if kill {
                "kill_switch_active"
            } else {
                "disabled"
            })
        );
        assert!(rpc.trace().is_empty());
        assert_eq!(super::buy_retry_safety_fixture::rows(&f)?, before);
        if kill {
            std::fs::remove_file(flag)?;
        }
    }
    Ok(())
}
