use super::buy_retry_safety_fixture::{reopen, rows};
use super::entry_cost_runtime_fixture::*;
use super::fresh_buy_size_runtime_fixture::RuntimeFixture;
use super::ExecutionCanaryRunner;
use anyhow::Result;

async fn exact_cap_blocks(retry: bool) -> Result<()> {
    let mut f = RuntimeFixture::new(
        &format!("b13-exact-cap-{retry}"),
        20_000_000,
        200,
        10_000_000,
        100,
        retry,
    )
    .await?;
    f.config.canary_max_daily_loss_sol = 0.02;
    closed_loss(&f, 19_999_997)?;
    failed_fee(&f, 3).await?;
    assert!(!f.store.execution_canary_accounting_pending()?);
    assert_eq!(f.store.execution_canary_open_position_count()?, 0);
    let before = rows(&f)?;
    reopen(&mut f)?;
    let s = if retry {
        ExecutionCanaryRunner::new(f.config.clone())
            .process_tick(&f.store, as_of(&f))
            .await?
    } else {
        f.hot().await?
    };
    f.finish().await?;
    eprintln!(
        "B13 actual failed receipt then BUY retry={retry}: calls={:?}, reason={:?}",
        f.calls(),
        s.state_machine_skipped_reason
    );
    assert!(
        f.calls().is_empty(),
        "execution crossed BUY guard: {:?}",
        f.calls()
    );
    assert_eq!(s.state_machine_safety_blocked, 1);
    assert_eq!(s.state_machine_skipped_reason, Some("max_daily_loss"));
    if !retry {
        assert_eq!(s.quote_entry_existing, 1);
    }
    assert_eq!(rows(&f)?, before);
    Ok(())
}

#[tokio::test]
async fn entry_cost_initial_buy_blocks_closed_plus_failed_fee_at_exact_cap() -> Result<()> {
    exact_cap_blocks(false).await
}

#[tokio::test]
async fn entry_cost_reopened_retry_blocks_closed_plus_failed_fee_at_exact_cap() -> Result<()> {
    exact_cap_blocks(true).await
}
