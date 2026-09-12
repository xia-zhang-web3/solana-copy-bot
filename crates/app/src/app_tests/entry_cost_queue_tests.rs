use super::buy_retry_queue_fixture::*;
use super::buy_retry_queue_http_fixture::QueueRpc;
use super::buy_retry_safety_fixture::reopen;
use super::entry_cost_policy_tests::expense;
use super::entry_cost_runtime_fixture::{as_of, receipt_server};
use super::failed_expense_runtime_tests::failed_receipt;
use super::ExecutionCanaryRunner;
use anyhow::Result;
use chrono::Duration;
use serde_json::json;

#[tokio::test]
async fn entry_cost_exhausted_fee_keeps_limit_one_sell_and_receipt_progress() -> Result<()> {
    for unknown in [false, true] {
        let mut f = queue_fixture(&format!("b13-sell-{unknown}"), unknown).await?;
        f.config.canary_max_open_positions = 10;
        f.config.canary_batch_limit = 1;
        f.config.canary_max_daily_loss_sol = 0.02;
        expense(&f, "exhausted", Some(20_000_000), false, true)?;
        let sell = add_sell(&f, false)?;
        let pending = add_pending(&f, true)?;
        let original = buy_order(&f)?;
        let mut rpc = QueueRpc::new(&mut f, true).await?;
        for n in 0..3 {
            reopen(&mut f)?;
            let s = super::entry_risk_clock_fixture::at(
                as_of(&f) + Duration::seconds(n + 1),
                ExecutionCanaryRunner::new(f.config.clone())
                    .process_tick(&f.store, as_of(&f) + Duration::seconds(n)),
            )
            .await?;
            assert_ne!(
                s.state_machine_skipped_reason,
                Some("risk_decision_clock_unordered")
            );
            assert!(s.state_machine_existing <= 1);
            assert_eq!(buy_order(&f)?, original);
        }
        confirmed(&f, &sell)?;
        *rpc.pending_receipt.lock().unwrap() = false;
        reopen(&mut f)?;
        super::entry_risk_clock_fixture::at(
            as_of(&f) + Duration::seconds(5),
            ExecutionCanaryRunner::new(f.config.clone())
                .process_tick(&f.store, as_of(&f) + Duration::seconds(4)),
        )
        .await?;
        rpc.finish().await?;
        confirmed(&f, &pending)?;
        assert_eq!(
            rpc.trace()
                .iter()
                .filter(|v| v.starts_with("sendTransaction"))
                .count(),
            1
        );
        assert_eq!(buy_order(&f)?, original);
        assert_eq!(
            f.store
                .execution_canary_entry_cost(as_of(&f))?
                .failed_expenses
                .known_wallet_fee_lamports
                .as_deref(),
            Some("20000000")
        );
        eprintln!(
            "B13 fee-exhausted limit=1 unknown={unknown}: {:?}",
            rpc.trace()
        );
    }
    Ok(())
}

#[tokio::test]
async fn entry_cost_failed_receipt_between_selection_and_candidate_blocks_fresh_buy() -> Result<()>
{
    let mut f = queue_fixture("b13-dynamic-fee", false).await?;
    f.config.canary_max_open_positions = 10;
    f.config.canary_batch_limit = 3;
    f.config.canary_max_daily_loss_sol = 0.02;
    let pending = add_pending(&f, true)?;
    let original = buy_order(&f)?;
    assert_eq!(
        f.store
            .execution_canary_entry_cost(as_of(&f))?
            .known_total_lamports
            .as_deref()
            .unwrap(),
        "0"
    );
    let mut value = failed_receipt("buy", 20_000_000);
    value["result"]["transaction"]["message"]["accountKeys"][0]["pubkey"] =
        json!(f.config.canary_wallet_pubkey);
    let (url, task) = receipt_server(value, true).await?;
    f.config.submit_adapter_http_url = url.clone();
    f.config.quote_canary_base_url = url;
    let first = f.sweep().await?;
    assert_eq!(first.existing, 1);
    assert_eq!(first.skipped_reason, Some("unresolved_buy_dispatch"));
    assert_eq!(first.failed, 1);
    let out = f.sweep().await?;
    let calls = task.await??;
    assert_eq!(
        out.existing, 0,
        "fee cap defers BUY before retry selection: {out:?}"
    );
    assert_eq!(out.skipped_reason, Some("max_daily_loss"));
    assert_eq!(out.safety_blocked, 1);
    assert_eq!(out.failed, 0);
    assert_eq!(
        f.store.load_failed_expense_task(&pending)?.unwrap().status,
        "complete"
    );
    assert_eq!(buy_order(&f)?, original);
    assert_eq!(
        out.entry_cost
            .unwrap()
            .known_total_lamports
            .as_deref()
            .unwrap(),
        "20000000"
    );
    assert_eq!(calls, ["getSignatureStatuses", "getTransaction"]);
    eprintln!("B13 selected BUY then durable failed fee: {calls:?}");
    Ok(())
}
