//! Root V2 boundary, adapted to allow scheduling to return before RPC handshake.
use super::{
    association_parent_fixture as parent, b136_config, b136_endpoint_tests::sends,
    b136_fixture::Fixture, b136_r1_hooks as hooks, b136_server::Server, strict_quote_fixture as q,
};
use crate::execution_canary::ExecutionCanaryRunner;
use anyhow::{ensure, Context, Result};
use chrono::Utc;
use std::time::Duration;

pub(super) async fn pending(f: &Fixture, s: &Server) -> Result<copybot_config::AppConfig> {
    *s.fault.lock().unwrap() = "unknown".into();
    let c = b136_config::load(f, &s.url, true)?;
    f.ingress(&c).await?;
    let initial = f.runner(&c)?;
    await_dispatch(f, s, &initial).await?;
    tokio::time::sleep(Duration::from_millis(1150)).await;
    initial.process_tick(&f.db.store, Utc::now()).await?;
    drop(initial);
    idle(f).await?;
    assert_eq!(f.db.store.owned_sell_dispatch_ids(1)?.len(), 1);
    assert_eq!(sends(s), 1);
    Ok(c)
}
pub(super) async fn await_dispatch(
    f: &Fixture, s: &Server, initial: &ExecutionCanaryRunner,
) -> Result<()> {
    let quote_wait_deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    let mut last = None;
    loop {
        s.check()?;
        if !f.rows("rpc_owned_sell_dispatches")?.is_empty() {
            break;
        }
        let deadline = pending_deadline(f, s, quote_wait_deadline)?;
        ensure!(tokio::time::Instant::now() < deadline,
            "pending dispatch deadline: stage={:?}", pending_stage(f, s));
        let summary =
            match tokio::time::timeout_at(deadline, initial.process_tick(&f.db.store, Utc::now()))
                .await
            {
                Ok(summary) => summary.with_context(|| {
                    format!(
                        "pending tick error: last={last:?} stage={:?}",
                        pending_stage(f, s)
                    )
                })?,
                Err(elapsed) => {
                    s.check()?;
                    if !f.rows("rpc_owned_sell_dispatches")?.is_empty() {
                        break;
                    }
                    // A quote may have completed while this tick was waiting.
                    // Switch from the pre-quote wait to its actual freshness clock.
                    if pending_deadline(f, s, quote_wait_deadline)? > tokio::time::Instant::now() {
                        continue;
                    }
                    return Err(elapsed).with_context(|| {
                        format!(
                            "pending tick timeout: last={last:?} stage={:?}",
                            pending_stage(f, s)
                        )
                    });
                }
            };
        last = Some(summary);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}
fn pending_deadline(
    f: &Fixture,
    s: &Server,
    quote_wait_deadline: tokio::time::Instant,
) -> Result<tokio::time::Instant> {
    let Some(quote) =
        f.db.store
            .load_strict_sell_quote(&q::id(&f.meta), parent::limits(), Utc::now())?
    else {
        return Ok(quote_wait_deadline);
    };
    ensure!(
        quote.outcome == copybot_storage_core::ordered_sell_quote::QuoteOutcome::Current,
        "pending quote terminal: outcome={:?} reason={:?} stage={:?}",
        quote.outcome,
        quote.reason,
        pending_stage(f, s),
    );
    let started = quote.http_started.context("pending quote clock missing")?;
    let until = started
        + chrono::Duration::milliseconds(
            copybot_storage_core::ordered_sell_quote::MAX_QUOTE_AGE_MS,
        );
    let remaining = (until - Utc::now()).to_std().unwrap_or(Duration::ZERO);
    Ok(tokio::time::Instant::now() + remaining)
}
fn pending_stage(f: &Fixture, s: &Server) -> Result<String> {
    let quote =
        f.db.store
            .load_strict_sell_quote(&q::id(&f.meta), parent::limits(), Utc::now())?;
    let methods: Vec<String> = s
        .calls
        .lock()
        .unwrap()
        .iter()
        .take(24)
        .map(|call| call["method"].as_str().unwrap_or("instructions").to_owned())
        .collect();
    Ok(format!(
        "quote={:?} reason={:?} started={:?} handoffs={} unsigned={} dispatches={} job={:?} calls={methods:?} terminal={:?}",
        quote.as_ref().map(|q| &q.outcome),
        quote.as_ref().and_then(|q| q.reason.as_deref()),
        quote.as_ref().and_then(|q| q.http_started),
        f.rows("rpc_owned_sell_handoffs")?.len(),
        f.handoffs()?,
        f.rows("rpc_owned_sell_dispatches")?.len(),
        hooks::read(&f.db.path),
        s.terminal(),
    ))
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
    harvest_until(f, r, |s| s.orphan_recovery_reconciled > 0).await
}
pub(super) async fn harvest_pending(
    f: &Fixture,
    r: &ExecutionCanaryRunner,
) -> Result<crate::execution_canary::ExecutionCanaryTickSummary> {
    harvest_until(f, r, |s| {
        s.orphan_recovery_checked > 0 && s.last_error.is_some()
    })
    .await
}
async fn harvest_until(
    f: &Fixture,
    r: &ExecutionCanaryRunner,
    complete: impl Fn(&crate::execution_canary::ExecutionCanaryTickSummary) -> bool,
) -> Result<crate::execution_canary::ExecutionCanaryTickSummary> {
    let mut last = None;
    let done = tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            // Actual tick, no cancelling 250ms fixture wrapper.
            let summary = r.process_tick(&f.db.store, Utc::now()).await?;
            if complete(&summary) {
                return Ok(summary);
            }
            last = Some(summary);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    done.with_context(|| format!(
        "recovery did not reach required result: last={last:?} job={:?} dispatches={:?} fees={:?}",
        hooks::read(&f.db.path), f.rows("rpc_owned_sell_dispatches"), f.rows("execution_tiny_reservations")
    ))?
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
async fn b136_pending_delayed_fee_keeps_actual_dispatch_within_quote_deadline() -> Result<()> {
    let f = Fixture::new().await?;
    let s = Server::new().await?;
    let (at_first_fee, release_first_fee) = s.hold("getFeeForMessage:1");
    let started = tokio::time::Instant::now();
    let (pending_result, held) = tokio::join!(pending(&f, &s), async {
        wait_for_held_fee(&f, &s, at_first_fee, started).await?;
        tokio::time::sleep_until(started + Duration::from_millis(3900)).await;
        let (at_second_fee, release_second_fee) = s.hold("getFeeForMessage:2");
        release_first_fee
            .send(())
            .map_err(|_| anyhow::anyhow!("first held fee peer exited"))?;
        let reached_at = wait_for_held_fee(&f, &s, at_second_fee, started).await?;
        ensure!(
            reached_at >= started + Duration::from_millis(3900),
            "second fee RPC was not late"
        );
        let min_hold = Duration::from_millis(150);
        tokio::time::sleep_until(reached_at + min_hold).await;
        ensure!(
            tokio::time::Instant::now() >= reached_at + min_hold,
            "held fee RPC was released immediately"
        );
        s.check()?;
        let quote =
            f.db.store
                .load_strict_sell_quote(&q::id(&f.meta), parent::limits(), Utc::now())?;
        ensure!(
            quote.as_ref().map(|q| &q.outcome)
                == Some(&copybot_storage_core::ordered_sell_quote::QuoteOutcome::Current),
            "held fee release lost fresh quote: stage={:?}",
            pending_stage(&f, &s)
        );
        release_second_fee
            .send(())
            .map_err(|_| anyhow::anyhow!("held fee peer exited"))?;
        Ok::<_, anyhow::Error>(())
    });
    held?;
    pending_result?;
    assert_eq!(f.rows("rpc_owned_sell_dispatches")?.len(), 1);
    assert_eq!(sends(&s), 1);
    s.healthy();
    Ok(())
}
pub(super) async fn wait_for_held_fee(
    f: &Fixture,
    s: &Server,
    mut reached: tokio::sync::oneshot::Receiver<()>,
    started: tokio::time::Instant,
) -> Result<tokio::time::Instant> {
    let quote_wait_deadline = started + Duration::from_secs(4);
    loop {
        s.check()?;
        let deadline = pending_deadline(f, s, quote_wait_deadline)?;
        ensure!(
            tokio::time::Instant::now() < deadline,
            "held fee RPC not reached: stage={:?}",
            pending_stage(f, s)
        );
        tokio::select! {
            result = &mut reached => {
                result.context("held fee peer exited")?;
                return Ok(tokio::time::Instant::now());
            },
            _ = tokio::time::sleep(Duration::from_millis(10)) => {},
        }
    }
}
#[tokio::test]
async fn b136_mock_peer_survives_cancelled_socket() -> Result<()> {
    let s = Server::new().await?;
    let socket = tokio::net::TcpStream::connect(s.url.trim_start_matches("http://")).await?;
    drop(socket); // A cancelled RPC can close after accept, before sending any bytes.
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        reqwest::Client::new()
            .post(&s.url)
            .json(
                &serde_json::json!({"jsonrpc":"2.0","id":1,"method":"getGenesisHash","params":[]}),
            )
            .send(),
    )
    .await??;
    let wire: serde_json::Value = response.json().await?;
    assert_eq!(wire["result"], "11111111111111111111111111111111");
    use tokio::io::AsyncWriteExt;
    let mut partial = tokio::net::TcpStream::connect(s.url.trim_start_matches("http://")).await?;
    partial
        .write_all(b"POST / HTTP/1.1\r\nContent-Length: 100\r\n\r\n{}")
        .await?;
    drop(partial);
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        reqwest::Client::new()
            .post(&s.url)
            .json(
                &serde_json::json!({"jsonrpc":"2.0","id":2,"method":"getGenesisHash","params":[]}),
            )
            .send(),
    )
    .await??;
    assert_eq!(
        response.json::<serde_json::Value>().await?["result"],
        "11111111111111111111111111111111"
    );
    s.healthy();
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
    tokio::time::timeout(Duration::from_secs(2), at_rpc)
        .await
        .with_context(|| {
            format!(
                "getSignatureStatuses not reached: server={:?} job={:?} dispatches={:?}",
                s.terminal(),
                hooks::read(&f.db.path),
                f.rows("rpc_owned_sell_dispatches")
            )
        })??;
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
