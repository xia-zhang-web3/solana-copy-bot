use super::entry_cash_guard_tests::partial;
use super::entry_cost_runtime_fixture::as_of;
use super::fresh_buy_size_runtime_fixture::RuntimeFixture;
use anyhow::Result;
use chrono::Duration;

#[tokio::test]
async fn entry_cash_available_to_unavailable_replaces_summary_but_no_read_sell_preserves_it(
) -> Result<()> {
    use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary as State;
    use crate::execution_canary_summary::apply_state_machine_summary;
    let mut f = RuntimeFixture::new("b16-summary", 20_000_000, 200, 10_000_000, 100, false).await?;
    f.config.canary_max_open_positions = 10;
    f.config.canary_max_daily_loss_sol = 7e-9;
    partial(&f)?;
    let mut summary = crate::execution_canary::ExecutionCanaryTickSummary::default();
    for broken in [false, true] {
        if broken {
            super::entry_cash_guard_tests::historical_duplicate(&f)?;
        }
        let read = crate::execution_canary_safety::pre_submit_safety_snapshot(
            &f.config,
            &f.store,
            as_of(&f) + Duration::nanoseconds(1),
        )?;
        assert_eq!(
            read.blocked_reason,
            Some(if broken {
                "cash_loss_unavailable"
            } else {
                "max_daily_loss"
            })
        );
        apply_state_machine_summary(
            &mut summary,
            State {
                entry_cost: read.entry_cost,
                daily_loss_sol: read.daily_loss_sol,
                skipped_reason: read.blocked_reason,
                ..State::default()
            },
        );
        let observed = summary.state_machine_entry_cost.clone();
        assert_eq!(
            observed.as_ref().unwrap().known_total_lamports.is_none(),
            broken
        );
        if broken {
            assert_eq!(
                observed.as_ref().unwrap().partial_known_subtotal_lamports,
                "0"
            );
            assert_eq!(summary.state_machine_daily_loss_sol, 0.0);
        }
        // A SELL summary which did not read cost must retain the latest explicit snapshot.
        apply_state_machine_summary(
            &mut summary,
            State {
                sell_execute: 1,
                ..State::default()
            },
        );
        assert_eq!(summary.state_machine_entry_cost, observed);
    }
    f.finish().await?;
    assert!(f.calls().is_empty());
    Ok(())
}

#[tokio::test]
async fn entry_cash_below_cap_keeps_hot_and_retry_execution_path() -> Result<()> {
    for retry in [false, true] {
        let mut f = RuntimeFixture::new(
            &format!("b16-below-{retry}"),
            20_000_000,
            200,
            10_000_000,
            100,
            retry,
        )
        .await?;
        f.config.canary_max_open_positions = 10;
        f.config.canary_max_daily_loss_sol = 8e-9;
        partial(&f)?;
        let outcome =
            super::entry_risk_clock_fixture::at(as_of(&f) + Duration::nanoseconds(1), async {
                if retry {
                    super::ExecutionCanaryRunner::new(f.config.clone())
                        .process_tick(&f.store, as_of(&f))
                        .await
                } else {
                    f.hot().await
                }
            })
            .await;
        f.finish().await?;
        assert_ne!(
            outcome?.state_machine_skipped_reason,
            Some("max_daily_loss")
        );
        assert_eq!(
            f.calls()
                .iter()
                .filter(|s| s.as_str() == "sendTransaction")
                .count(),
            1
        );
    }
    Ok(())
}
