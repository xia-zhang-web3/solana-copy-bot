use super::buy_retry_safety_fixture::*;
use super::ExecutionCanaryRunner;
use anyhow::Result;

async fn blocked_runner(kind: Block) -> Result<()> {
    let mut f = fixture(&format!("b12-red-{kind:?}")).await?;
    kind.apply(&mut f)?;
    reopen(&mut f)?;
    let before = rows(&f)?;
    let summary = ExecutionCanaryRunner::new(f.config.clone())
        .process_tick(&f.store, f.now + chrono::Duration::seconds(4))
        .await?;
    f.finish().await?;
    assert!(
        f.calls().is_empty(),
        "BUY crossed HTTP boundary: {:?}; {summary:?}",
        f.calls()
    );
    assert_eq!(summary.state_machine_safety_blocked, 1, "{summary:?}");
    assert_eq!(summary.state_machine_skipped_reason, Some(kind.reason()));
    assert_eq!(
        rows(&f)?,
        before,
        "blocked retry must retain every business row"
    );
    Ok(())
}

#[tokio::test]
async fn buy_retry_safety_disabled_entry_runner_red_green() -> Result<()> {
    blocked_runner(Block::Disabled).await
}
#[tokio::test]
async fn buy_retry_safety_closed_loss_at_positive_cap_runner_red_green() -> Result<()> {
    blocked_runner(Block::Loss).await
}
#[tokio::test]
async fn buy_retry_safety_actual_open_count_at_cap_runner_red_green() -> Result<()> {
    blocked_runner(Block::Open).await
}
