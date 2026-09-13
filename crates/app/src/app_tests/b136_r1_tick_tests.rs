//! Root V2 boundary, adapted to allow scheduling to return before RPC handshake.
use super::{
    b136_config, b136_endpoint_tests::sends, b136_fixture::Fixture, b136_r1_hooks as hooks,
    b136_server::Server, strict_quote_fixture as q,
};
use crate::execution_canary::ExecutionCanaryRunner;
use anyhow::Result;
use chrono::Utc;
use std::time::Duration;

pub(super) async fn pending(f: &Fixture, s: &Server) -> Result<copybot_config::AppConfig> {
    *s.fault.lock().unwrap() = "unknown".into();
    let c = b136_config::load(f, &s.url, true)?;
    f.ingress(&c).await?;
    let initial = f.runner(&c)?;
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            q::tick(&initial, &f.db).await?;
            if !f.rows("rpc_owned_sell_dispatches")?.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    tokio::time::sleep(Duration::from_millis(1150)).await;
    let _ = q::tick(&initial, &f.db).await;
    drop(initial);
    idle(f).await?;
    assert_eq!(f.db.store.owned_sell_dispatch_ids(1)?.len(), 1);
    assert_eq!(sends(s), 1);
    Ok(c)
}
pub(super) async fn idle(f: &Fixture) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(2), async {
        while hooks::read(&f.db.path).running != 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await?;
    Ok(())
}
pub(super) fn stopped(
    f: &Fixture,
    c: &copybot_config::AppConfig,
) -> Result<copybot_config::AppConfig> {
    let start = Utc::now() - chrono::Duration::seconds(7200);
    f.db.sql.execute("UPDATE execution_tiny_experiment SET activated_at=?1,deadline=?2,state='stopped',stop_reason='operator_stop'",rusqlite::params![start.to_rfc3339(),(start+chrono::Duration::seconds(3600)).to_rfc3339()])?;
    std::fs::write(&c.execution.canary_kill_switch_path, b"stop")?;
    let mut c = c.clone();
    c.execution.owned_sell_preparation = None;
    c.execution.canary_tiny_submit_enabled = false;
    c.execution.canary_enabled = false;
    c.execution.quote_canary_enabled = false;
    copybot_config::validate_association_delivery(&c)?;
    Ok(c)
}
pub(super) async fn harvest(
    f: &Fixture,
    r: &ExecutionCanaryRunner,
) -> Result<crate::execution_canary::ExecutionCanaryTickSummary> {
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            // Actual tick, no cancelling 250ms fixture wrapper.
            let summary = r.process_tick(&f.db.store, Utc::now()).await?;
            if summary.orphan_recovery_checked > 0 {
                return Ok(summary);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?
}
pub(super) fn exact_settlement(
    f: &Fixture,
    s: &Server,
    c: &copybot_config::AppConfig,
) -> Result<()> {
    let id: String =
        f.db.sql
            .query_row("SELECT order_id FROM rpc_owned_sell_dispatches", [], |r| {
                r.get(0)
            })?;
    let cash =
        f.db.store
            .load_execution_canary_cash_settlement(&id)?
            .unwrap();
    assert_eq!(
        (cash.sold_quantity.raw(), cash.remaining_quantity.raw()),
        (7000, 0)
    );
    assert_eq!(cash.allocated_entry_basis.as_u64(), 10000);
    assert_eq!(cash.wallet_native_cash_delta.as_i128(), 981000);
    assert_eq!(cash.cash_result_delta.as_i128(), 971000);
    assert_eq!(
        f.db.sql.query_row(
            "SELECT actual_fee FROM execution_tiny_reservations WHERE side='sell'",
            [],
            |r| r.get::<_, u64>(0)
        )?,
        19000
    );
    assert_eq!(f.db.sql.query_row("SELECT fee_bound FROM execution_tiny_reservations WHERE side='buy' AND actual_fee IS NULL", [], |r|r.get::<_,u64>(0))?, 100000);
    assert_eq!(f.rows("fills")?.len(), 2, "one historical BUY, one SELL");
    assert_eq!(sends(s), 1);
    assert_eq!(
        super::b135_hooks::count(&c.execution.execution_signer_keypair_path),
        1
    );
    Ok(())
}
#[tokio::test]
async fn b136_root_pending_recovery_rpc_must_not_hold_main_tick() -> Result<()> {
    let f = Fixture::new().await?;
    let s = Server::new().await?;
    let c = pending(&f, &s).await?;
    let disabled = stopped(&f, &c)?;
    let recovery = f.runner(&disabled)?;
    assert!(recovery.is_enabled());
    let before = f.rows("positions")?;
    let fills_before = f.rows("fills")?;
    let count = hooks::read(&f.db.path).scheduled;
    let (at_rpc, release_rpc) = s.hold("getSignatureStatuses");
    // First actual tick may return BEFORE the handshake. Neither early return
    // nor lack of handshake can by itself pass this test.
    tokio::time::timeout(
        Duration::from_millis(200),
        recovery.process_tick(&f.db.store, Utc::now()),
    )
    .await??;
    tokio::time::timeout(Duration::from_secs(2), at_rpc).await??;
    let clone = recovery.clone();
    drop(recovery); // Remaining clone retains the same job, not a new pool.
    for _ in 0..20 {
        tokio::time::timeout(
            Duration::from_millis(200),
            clone.process_tick(&f.db.store, Utc::now()),
        )
        .await??;
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let counts = hooks::read(&f.db.path);
    assert_eq!(counts.scheduled, count + 1);
    assert_eq!(counts.running, 1);
    assert_eq!(counts.max_running, 1);
    assert_eq!(f.rows("positions")?, before);
    assert_eq!(f.rows("fills")?, fills_before);
    assert_eq!(sends(&s), 1);
    *s.fault.lock().unwrap() = String::new();
    release_rpc.send(()).unwrap();
    let done = harvest(&f, &clone).await?;
    assert_eq!(
        (
            done.orphan_recovery_checked,
            done.orphan_recovery_reconciled
        ),
        (1, 1)
    );
    assert!(done.last_error.is_none());
    assert!(done.has_status_change());
    exact_settlement(&f, &s, &c)?;
    let positions = f.rows("positions")?;
    let fills = f.rows("fills")?;
    let fees = f.rows("execution_tiny_reservations")?;
    drop(clone);
    idle(&f).await?;
    f.ingress(&disabled).await?;
    let reopened = f.runner(&disabled)?;
    for _ in 0..10 {
        reopened.process_tick(&f.db.store, Utc::now()).await?;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(f.rows("positions")?, positions);
    assert_eq!(f.rows("fills")?, fills);
    assert_eq!(f.rows("execution_tiny_reservations")?, fees);
    exact_settlement(&f, &s, &c)?;
    println!("B136_R1_V2 rpc_reached=true first_tick_returned_before_release=true subsequent_actual_ticks=20 shared_jobs=1 settlement_before_release=0 harvested=1 settlement_once=1 raw=7000 cash=981000 result=971000 fee=19000 buy_unknown_hold=100000 sign=1 send=1 stop+expired+policy_removed=true reopen_once=true");
    s.healthy();
    Ok(())
}
