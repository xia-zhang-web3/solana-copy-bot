use super::buy_retry_queue_fixture::*;
use super::buy_retry_queue_http_fixture::QueueRpc;
use super::buy_retry_safety_fixture::reopen;
use anyhow::Result;

#[tokio::test]
async fn root_b25_invalid_reserve_is_the_buy_blocker_with_permissive_existing_limits() -> Result<()>
{
    for invalid in [0.0, f64::NAN, f64::MAX] {
        let mut f = queue_fixture("root-b25-isolated-policy", false).await?;
        f.config.canary_max_open_positions = 10;
        f.config.pretrade_min_sol_reserve = invalid;
        let before = super::buy_retry_safety_fixture::rows(&f)?;
        let safety =
            crate::execution_canary_safety::pre_submit_safety_snapshot(&f.config, &f.store, f.now)?;
        assert_eq!(safety.blocked_reason, None, "{safety:?}");
        let mut rpc = QueueRpc::new(&mut f, true).await?;
        let out = crate::execution_canary_route::process_canary_state_machine_for_route(
            &f.config, &f.store, &f.signal, f.now,
        )
        .await;
        rpc.finish().await?;
        let out = out?;
        assert_eq!(
            out.skipped_reason,
            Some("native_floor_invalid_policy"),
            "{out:?}"
        );
        assert_eq!(out.safety_blocked, 1);
        assert!(rpc.trace().is_empty());
        assert_eq!(super::buy_retry_safety_fixture::rows(&f)?, before);
    }
    Ok(())
}

#[tokio::test]
async fn root_b25_invalid_reserve_preserves_limit_one_queue_without_open_limit_blocker(
) -> Result<()> {
    for invalid in [0.0, f64::NAN, f64::MAX] {
        let mut f = queue_fixture("root-b25-permissive-queue", false).await?;
        f.config.canary_max_open_positions = 10;
        f.config.canary_batch_limit = 1;
        f.config.pretrade_min_sol_reserve = invalid;
        let unsigned_buy = buy_order(&f)?;
        let sell = add_sell(&f, false)?;
        let safety =
            crate::execution_canary_safety::pre_submit_safety_snapshot(&f.config, &f.store, f.now)?;
        assert_eq!(safety.blocked_reason, None, "{safety:?}");
        let pending = add_pending(&f, false)?;
        let mut rpc = QueueRpc::new(&mut f, true).await?;
        for n in 0..3 {
            reopen(&mut f)?;
            let out = super::ExecutionCanaryRunner::new(f.config.clone())
                .process_tick(&f.store, f.now + chrono::Duration::seconds(4 + n))
                .await?;
            // existing counts visits; only non-refused visits spend the shared handler limit.
            let valid_handlers = out.state_machine_existing - out.source_sell_refusals.count();
            assert!(valid_handlers <= 1, "{out:?}");
            assert_eq!(
                out.source_sell_refusals.count(),
                0,
                "healthy SELL fixture: {out:?}"
            );
            assert_eq!(
                out.state_machine_skipped_reason,
                Some("native_floor_invalid_policy"),
                "{out:?}"
            );
            assert_eq!(buy_order(&f)?, unsigned_buy);
        }
        confirmed(&f, &sell)?;
        assert_eq!(
            rpc.trace()
                .iter()
                .filter(|v| *v == "sendTransaction:sell")
                .count(),
            1
        );
        assert!(rpc
            .trace()
            .iter()
            .all(|v| v != "buy-quote" && v != "sendTransaction:buy"));
        *rpc.pending_receipt.lock().unwrap() = false;
        reopen(&mut f)?;
        super::ExecutionCanaryRunner::new(f.config.clone())
            .process_tick(&f.store, f.now + chrono::Duration::seconds(8))
            .await?;
        rpc.finish().await?;
        confirmed(&f, &pending)?;
    }
    Ok(())
}
