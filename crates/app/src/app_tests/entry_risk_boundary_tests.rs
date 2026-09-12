use super::entry_cost_runtime_fixture::{as_of, receipt_server};
use super::failed_expense_runtime_tests::failed_receipt;
use super::fresh_buy_size_runtime_fixture::RuntimeFixture;
use super::receipt_reconciliation_fixture::add_order;
use anyhow::Result;
use chrono::Duration;
use serde_json::json;

async fn run(later: bool) -> Result<()> {
    let mut f = RuntimeFixture::new(
        &format!("root-b13-equality-{later}"),
        20_000_000,
        200,
        10_000_000,
        100,
        false,
    )
    .await?;
    f.config.canary_max_daily_loss_sol = 0.000007;
    let tick_now = as_of(&f);
    let failed_id = add_order(
        &f.store,
        "root-same-tick-failure",
        "buy",
        "FeeMint",
        tick_now,
        true,
    )?;
    let mut receipt = failed_receipt("buy", 7000);
    receipt["result"]["transaction"]["message"]["accountKeys"][0]["pubkey"] =
        json!(f.config.canary_wallet_pubkey);
    let (url, task) = receipt_server(receipt, true).await?;
    let failed = crate::execution_submit_adapter::reconcile_execution_tiny_submit_confirmation(
        &f.store,
        &f.config,
        &failed_id,
        &reqwest::Client::new(),
        &url,
        tick_now,
        200,
    )
    .await;
    assert_eq!(task.await??, ["getSignatureStatuses", "getTransaction"]);
    assert_eq!(failed?.confirmation_failed, 1);
    assert_eq!(
        f.store
            .load_failed_expense_task(&failed_id)?
            .unwrap()
            .status,
        "complete"
    );
    assert_eq!(
        f.store
            .load_failed_transaction_facts(&failed_id)?
            .unwrap()
            .wallet_fee()?
            .unwrap()
            .as_u64(),
        7000
    );
    assert!(!f.store.execution_canary_accounting_pending()?);
    let at = f.store.execution_canary_entry_cost(tick_now)?;
    let after = f
        .store
        .execution_canary_entry_cost(tick_now + Duration::nanoseconds(1))?;
    eprintln!(
        "ROOT B13 complete fee=7000, at={} after_1ns={} clock_advanced={later}",
        at.known_total_lamports.as_deref().unwrap(),
        after.known_total_lamports.as_deref().unwrap()
    );
    assert_eq!(after.known_total_lamports.as_deref().unwrap(), "7000");
    if later {
        f.now += Duration::nanoseconds(1);
    }
    let result = f.hot().await;
    f.finish().await?;
    let result = result?;
    eprintln!(
        "ROOT B13 second BUY: calls={:?}, reason={:?}",
        f.calls(),
        result.state_machine_skipped_reason
    );
    assert!(
        f.calls().is_empty(),
        "known fee cap must stop next BUY before execution: {:?}",
        f.calls()
    );
    assert_eq!(result.state_machine_skipped_reason, Some("max_daily_loss"));
    Ok(())
}
#[tokio::test]
async fn root_b13_same_tick_fee_must_block_next_buy() -> Result<()> {
    run(false).await
}
#[tokio::test]
async fn root_b13_later_clock_control_blocks_next_buy() -> Result<()> {
    run(true).await
}
