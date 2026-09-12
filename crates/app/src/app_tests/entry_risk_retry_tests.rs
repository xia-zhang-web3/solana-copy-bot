use super::buy_retry_safety_fixture::rows;
use super::entry_cost_runtime_fixture::{as_of, receipt_server};
use super::entry_risk_clock_fixture::at;
use super::failed_expense_runtime_tests::failed_receipt;
use super::fresh_buy_size_runtime_fixture::RuntimeFixture;
use super::receipt_reconciliation_fixture::add_order;
use anyhow::Result;
use chrono::Duration;
use serde_json::json;

#[tokio::test]
async fn entry_risk_current_decision_clock_enforces_first_and_retry_fee_boundaries() -> Result<()> {
    for retry in [false, true] {
        for cap in [6999, 7000, 7001] {
            let mut f = RuntimeFixture::new(
                &format!("r1-fee-{retry}-{cap}"),
                20_000_000,
                200,
                10_000_000,
                100,
                retry,
            )
            .await?;
            f.config.canary_max_daily_loss_sol = f64::from(cap) / 1e9;
            let tick = as_of(&f);
            let id = add_order(&f.store, "earlier-same-tick", "sell", "FeeMint", tick, true)?;
            let mut receipt = failed_receipt("sell", 7000);
            receipt["result"]["transaction"]["message"]["accountKeys"][0]["pubkey"] =
                json!(f.config.canary_wallet_pubkey);
            let (url, task) = receipt_server(receipt, true).await?;
            let failed =
                crate::execution_submit_adapter::reconcile_execution_tiny_submit_confirmation(
                    &f.store,
                    &f.config,
                    &id,
                    &reqwest::Client::new(),
                    &url,
                    tick,
                    200,
                )
                .await;
            assert_eq!(task.await??, ["getSignatureStatuses", "getTransaction"]);
            assert_eq!(failed?.confirmation_failed, 1);
            assert_eq!(
                f.store.load_execution_canary_order(&id)?.unwrap().submit_ts,
                tick
            );
            assert_eq!(
                f.store
                    .execution_canary_entry_cost(tick)?
                    .known_total_lamports
                    .as_deref()
                    .unwrap(),
                "0"
            );
            let before = rows(&f)?;
            let decision = tick + Duration::seconds(2);
            let blocked = at(decision, async {
                if retry {
                    let out = f.sweep().await?;
                    Ok::<_, anyhow::Error>(out.skipped_reason)
                } else {
                    let out = f.hot().await?;
                    assert_eq!(out.quote_entry_existing, 1);
                    Ok(out.state_machine_skipped_reason)
                }
            })
            .await?;
            f.finish().await?;
            if cap <= 7000 {
                assert_eq!(blocked, Some("max_daily_loss"));
                assert!(f.calls().is_empty());
                assert_eq!(rows(&f)?, before);
            } else {
                assert_eq!(blocked, None);
                super::initial_sol_rpc_fixture::assert_funded_buy_trace(
                    &f.calls(),
                    &[
                        "quote",
                        "build-instructions",
                        "build-transaction",
                        "simulateTransaction",
                        "sendTransaction",
                        "getSignatureStatuses",
                        "getTransaction",
                    ],
                );
            }
            assert_eq!(
                f.store
                    .execution_canary_entry_cost(decision)?
                    .known_total_lamports
                    .as_deref()
                    .unwrap(),
                "7000"
            );
            assert_eq!(
                f.store.load_failed_expense_task(&id)?.unwrap().status,
                "complete"
            );
            eprintln!("R1 retry={retry}, cap={cap}, tick={tick}, decision={decision}, fee=7000, calls={:?}", f.calls());
        }
    }
    Ok(())
}

#[tokio::test]
async fn entry_risk_clock_equal_or_before_tick_blocks_without_changing_rows() -> Result<()> {
    for offset in [-1, 0] {
        let mut f =
            RuntimeFixture::new("r1-clock-invalid", 20_000_000, 200, 10_000_000, 100, false)
                .await?;
        let before = rows(&f)?;
        let out = at(as_of(&f) + Duration::seconds(offset), f.hot()).await?;
        f.finish().await?;
        assert_eq!(
            out.state_machine_skipped_reason,
            Some("risk_decision_clock_unordered")
        );
        assert!(out.state_machine_entry_cost.is_none());
        assert!(f.calls().is_empty());
        assert_eq!(rows(&f)?, before);
        assert_eq!(out.quote_entry_existing, 1);
    }
    Ok(())
}

#[tokio::test]
async fn entry_risk_unordered_clock_keeps_limit_one_sell_and_known_receipt_progress() -> Result<()>
{
    use super::buy_retry_queue_fixture::*;
    use super::buy_retry_queue_http_fixture::QueueRpc;
    use super::buy_retry_safety_fixture::reopen;
    use super::ExecutionCanaryRunner;
    let mut f = queue_fixture("r1-clock-sell", false).await?;
    f.config.canary_max_open_positions = 10;
    f.config.canary_batch_limit = 1;
    let sell = add_sell(&f, false)?;
    let known = add_pending(&f, true)?;
    let original = buy_order(&f)?;
    let mut rpc = QueueRpc::new(&mut f, false).await?;
    for n in 0..3 {
        reopen(&mut f)?;
        let tick = as_of(&f) + Duration::seconds(n);
        let out = at(
            tick,
            ExecutionCanaryRunner::new(f.config.clone()).process_tick(&f.store, tick),
        )
        .await?;
        assert!(out.state_machine_existing <= 1);
        assert_eq!(buy_order(&f)?, original);
    }
    rpc.finish().await?;
    confirmed(&f, &sell)?;
    confirmed(&f, &known)?;
    assert_eq!(
        rpc.trace()
            .iter()
            .filter(|s| s.starts_with("sendTransaction"))
            .count(),
        1
    );
    assert!(rpc
        .trace()
        .iter()
        .any(|s| s == "getTransaction:receipt-signature"));
    eprintln!("R1 unordered-clock SELL/reconcile trace: {:?}", rpc.trace());
    Ok(())
}
