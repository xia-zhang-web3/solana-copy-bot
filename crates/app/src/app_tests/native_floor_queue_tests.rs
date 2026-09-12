use super::buy_retry_queue_fixture::*;
use super::buy_retry_queue_http_fixture::QueueRpc;
use super::buy_retry_safety_fixture::reopen;
use anyhow::Result;

#[tokio::test]
async fn native_floor_invalid_policy_keeps_limit_one_sell_and_legacy_buy_receipt_progress(
) -> Result<()> {
    for invalid in [0.0, f64::NAN, f64::MAX] {
        let mut f = queue_fixture("b25-invalid-policy-queue", false).await?;
        f.config.pretrade_min_sol_reserve = invalid;
        f.config.canary_batch_limit = 1;
        let unsigned_buy = buy_order(&f)?;
        let sell = add_sell(&f, false)?;
        let pending = add_pending(&f, false)?; // old signed BUY, no new payload/guard
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
        // The old BUY's receipt becomes available after restart; invalid current policy must not stop it.
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
