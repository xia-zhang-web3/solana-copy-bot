use super::buy_retry_queue_fixture::*;
use super::buy_retry_queue_http_fixture::QueueRpc;
use super::buy_retry_safety_fixture::reopen;
use super::entry_cash_guard_tests::{cash, partial};
use super::entry_cost_runtime_fixture::as_of;
use super::entry_risk_clock_fixture::{at, sequence};
use super::ExecutionCanaryRunner;
use anyhow::Result;
use chrono::Duration;
use rusqlite::Connection;

#[tokio::test]
async fn entry_cash_known_cap_and_unavailable_keep_limit_one_sell_and_receipt_progress(
) -> Result<()> {
    for unavailable in [false, true] {
        for unknown in [false, true] {
            let mut f =
                queue_fixture(&format!("b16-progress-{unavailable}-{unknown}"), unknown).await?;
            f.config.canary_max_open_positions = 10;
            f.config.canary_batch_limit = 1;
            f.config.canary_max_daily_loss_sol = 7e-9;
            partial(&f)?;
            if unavailable {
                cash::settle(
                    &f.store,
                    &Connection::open(&f.db_path)?,
                    "exec-canary:duplicate",
                    "partial-signature",
                    &f.config.canary_wallet_pubkey,
                    "PartialMint",
                    1,
                    -4,
                    as_of(&f),
                )?;
            }
            let sell = add_sell(&f, unknown)?;
            let pending = add_pending(&f, true)?;
            let original = buy_order(&f)?;
            let initial = crate::execution_canary_safety::pre_submit_safety_snapshot(
                &f.config,
                &f.store,
                as_of(&f) + Duration::seconds(1),
            )?;
            assert_eq!(
                initial.blocked_reason,
                Some(if unavailable {
                    "cash_loss_unavailable"
                } else {
                    "max_daily_loss"
                })
            );
            assert_eq!(
                initial.entry_cost.unwrap().known_total_lamports.is_none(),
                unavailable
            );
            let mut rpc = QueueRpc::new(&mut f, true).await?;
            for n in 0..3 {
                reopen(&mut f)?;
                let tick = as_of(&f) + Duration::seconds(n);
                let s = at(
                    tick + Duration::seconds(1),
                    ExecutionCanaryRunner::new(f.config.clone()).process_tick(&f.store, tick),
                )
                .await?;
                assert!(s.state_machine_existing <= 1);
                // Once A has a confirmed status but no receipt, its existing accounting
                // blocker may take precedence on later passes. SELL must still progress.
                assert!(matches!(
                    s.state_machine_skipped_reason,
                    Some(
                        "max_daily_loss" | "cash_loss_unavailable" | "confirmed_accounting_pending"
                    )
                ));
                if let Some(cost) = s.state_machine_entry_cost {
                    assert_eq!(cost.known_total_lamports.is_none(), unavailable);
                }
                assert_eq!(buy_order(&f)?, original);
            }
            confirmed(&f, &sell)?;
            *rpc.pending_receipt.lock().unwrap() = false;
            reopen(&mut f)?;
            let tick = as_of(&f) + Duration::seconds(4);
            let result = at(
                tick + Duration::seconds(1),
                ExecutionCanaryRunner::new(f.config.clone()).process_tick(&f.store, tick),
            )
            .await;
            rpc.finish().await?;
            result?;
            confirmed(&f, &pending)?;
            assert_eq!(
                rpc.trace()
                    .iter()
                    .filter(|s| s.starts_with("sendTransaction"))
                    .count(),
                1
            );
            assert_eq!(buy_order(&f)?, original);
            eprintln!(
                "B16 LIMIT=1 unavailable={unavailable} unknown={unknown}: {:?}",
                rpc.trace()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn entry_cash_partial_receipt_during_sweep_blocks_selected_buy_with_fresh_cutoff(
) -> Result<()> {
    let mut f = queue_fixture("b16-selected-buy", true).await?;
    f.config.canary_max_open_positions = 10;
    f.config.canary_batch_limit = 3;
    f.config.canary_max_daily_loss_sol = 7e-9;
    f.now -= Duration::seconds(10);
    let sell = add_sell(&f, false)?;
    f.now += Duration::seconds(10);
    f.store.mark_execution_canary_submitted(
        &sell,
        f.now - Duration::seconds(9),
        "b12-sell-signature",
    )?;
    Connection::open(&f.db_path)?.execute("UPDATE positions SET qty=300,qty_raw='300',cost_sol=0.030000021,cost_lamports=30000021 WHERE token=?1",[SELL_TOKEN])?;
    let original = buy_order(&f)?;
    let tick = as_of(&f);
    assert_eq!(f.store.execution_canary_entry_cost(tick)?.known_total()?, 0);
    let mut rpc = QueueRpc::new(&mut f, false).await?;
    let result = sequence(
        [
            tick + Duration::nanoseconds(1),
            tick + Duration::nanoseconds(2),
        ],
        f.sweep(),
    )
    .await;
    rpc.finish().await?;
    let s = result?;
    assert_eq!(s.existing, 2);
    assert_eq!(s.safety_blocked, 1);
    assert_eq!(s.skipped_reason, Some("max_daily_loss"));
    confirmed(&f, &sell)?;
    assert_eq!(
        f.store
            .load_execution_canary_open_position(SELL_TOKEN)?
            .unwrap()
            .qty_exact
            .unwrap()
            .raw(),
        200
    );
    assert_eq!(f.store.execution_canary_entry_cost(tick)?.known_total()?, 0);
    let after = f
        .store
        .execution_canary_entry_cost(tick + Duration::nanoseconds(2))?;
    assert_eq!(after.closed_loss.loss_lamports, "0");
    assert_eq!(after.known_total()?, 7);
    assert_eq!(s.entry_cost.unwrap(), after);
    assert_eq!(buy_order(&f)?, original);
    assert_eq!(
        rpc.trace(),
        [
            "getSignatureStatuses:b12-sell-signature",
            "getTransaction:b12-sell-signature"
        ]
    );
    Ok(())
}
