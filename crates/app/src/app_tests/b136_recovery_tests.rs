use super::{
    b136_config,
    b136_endpoint_tests::{finish, sends},
    b136_fixture::Fixture,
    b136_r1_tick_tests::await_dispatch,
    b136_server::Server,
    strict_quote_fixture as q,
};
use anyhow::Result;
use std::time::Duration;
#[tokio::test]
async fn b136_concurrent_runners_one_signature_and_send() -> Result<()> {
    let f = Fixture::new().await?;
    let s = Server::new().await?;
    let c = b136_config::load(&f, &s.url, true)?;
    f.ingress(&c).await?;
    let r = f.runner(&c)?;
    let other = f.runner(&c)?;
    let (at, release) = s.hold("isBlockhashValid");
    q::tick(&r, &f.db).await?;
    tokio::time::timeout(Duration::from_secs(4), at).await??;
    for _ in 0..3 {
        q::tick(&other, &f.db).await?;
    }
    assert_eq!(f.rows("rpc_owned_sell_handoffs")?.len(), 1);
    release.send(()).unwrap();
    finish(&f, &r).await?;
    q::tick(&other, &f.db).await?;
    assert_eq!(sends(&s), 1);
    assert_eq!(
        super::b135_hooks::count(&c.execution.execution_signer_keypair_path),
        1
    );
    Ok(())
}
#[test]
fn b136_recovery_dispatch_wait_uses_quote_phase_clock() {
    use super::b136_r1_tick_tests::dispatch_wait_deadline_at;
    let wall = chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_000, 0).unwrap();
    let mono = tokio::time::Instant::now();
    let prequote_deadline = mono + Duration::from_secs(4);
    let deadline = |quote_start_ms: Option<i64>, elapsed_ms: i64| {
        dispatch_wait_deadline_at(
            quote_start_ms.map(|ms| wall + chrono::Duration::milliseconds(ms)),
            prequote_deadline,
            wall + chrono::Duration::milliseconds(elapsed_ms),
            mono + Duration::from_millis(elapsed_ms as u64),
        )
    };
    // At 4.05s the old total-4s wait is exhausted, but an actual Current
    // quote has its own immutable clock. No scheduling or RPC speed is assumed.
    let after_old_wait = mono + Duration::from_millis(4050);
    assert!(deadline(None, 4050) < after_old_wait);
    assert_eq!(deadline(Some(0), 4050), mono + Duration::from_secs(5));
    assert!(deadline(Some(0), 4050) > after_old_wait);
    assert_eq!(deadline(Some(3000), 4050), mono + Duration::from_secs(8));
    // Repeated polls never refresh either phase, including exact/past expiry.
    assert_eq!(deadline(None, 3000), prequote_deadline);
    assert_eq!(deadline(Some(3000), 6000), mono + Duration::from_secs(8));
    for elapsed_ms in [5000, 6000] {
        let now = mono + Duration::from_millis(elapsed_ms as u64);
        assert!(deadline(Some(0), elapsed_ms) <= now);
    }
}

#[tokio::test]
async fn b136_recovery_dispatch_wait_stalled_quote_stays_bounded_without_signing() -> Result<()> {
    let f = Fixture::new().await?;
    let s = Server::new().await?;
    let c = b136_config::load(&f, &s.url, true)?;
    f.ingress(&c).await?;
    let r = f.runner(&c)?;
    let (reached, release) = s.hold("quote");
    let started = tokio::time::Instant::now();
    let result = tokio::time::timeout(Duration::from_millis(
        copybot_storage_core::ordered_sell_quote::MAX_QUOTE_AGE_MS as u64), async {
        tokio::join!(await_dispatch(&f, &s, &r), reached)
    }).await;
    // Cancel the quote task before unblocking its peer; it must not create a
    // preparation job or sign after the test's terminal wait result.
    drop(r);
    let _ = release.send(());
    super::b136_r1_tick_tests::idle(&f).await?;
    let (dispatch, at_quote) = result?;
    at_quote?;
    assert!(dispatch.is_err(), "stalled quote gained dispatch");
    assert!(started.elapsed() >= Duration::from_secs(4));
    assert_eq!(sends(&s),0);
    assert_eq!(super::b135_hooks::count(&c.execution.execution_signer_keypair_path),0);
    assert!(f.rows("rpc_owned_sell_dispatches")?.is_empty());
    assert!(f.rows("rpc_owned_sell_handoffs")?.is_empty());
    Ok(())
}
#[tokio::test]
async fn b136_recovery_success_failed_unknown_stop_expiry_and_config_removal() -> Result<()> {
    for fault in [
        "unknown",
        "blockhash_not_found",
        "receipt_unknown",
        "failed",
        "failed_unknown_fee",
        "success_unknown_fee",
    ] {
        let f = Fixture::new().await?;
        let s = Server::new().await?;
        *s.fault.lock().unwrap() = fault.into();
        let c = b136_config::load(&f, &s.url, true)?;
        f.ingress(&c).await?;
        let r = f.runner(&c)?;
        let before = f.rows("positions")?;
        await_dispatch(&f, &s, &r).await?;
        tokio::time::sleep(Duration::from_millis(1150)).await;
        r.process_tick(&f.db.store, chrono::Utc::now()).await?;
        drop(r);
        super::b136_r1_tick_tests::idle(&f).await?;
        let id: String =
            f.db.sql
                .query_row("SELECT order_id FROM rpc_owned_sell_dispatches", [], |r| {
                    r.get(0)
                })?;
        assert_eq!(sends(&s), 1);
        if fault != "success_unknown_fee" {
            assert_eq!(
                f.rows("positions")?,
                before,
                "no successful receipt with known operands"
            );
        }
        if fault != "failed" {
            assert!(f.db.sql.query_row("SELECT actual_fee IS NULL AND fee_bound=100000 FROM execution_tiny_reservations WHERE side='sell'",[],|r|r.get::<_,bool>(0))?);
        }
        // Valid expired horizon, explicit stop and removal of the new-trade policy.
        let start = chrono::Utc::now() - chrono::Duration::seconds(7200);
        f.db.sql.execute("UPDATE execution_tiny_experiment SET activated_at=?1,deadline=?2,state='stopped',stop_reason='operator_stop'",rusqlite::params![start.to_rfc3339(),(start+chrono::Duration::seconds(3600)).to_rfc3339()])?;
        std::fs::write(&c.execution.canary_kill_switch_path, b"stop")?;
        let mut stopped = c.clone();
        stopped.execution.owned_sell_preparation = Default::default();
        stopped.execution.canary_tiny_submit_enabled = false;
        stopped.execution.canary_enabled = false;
        stopped.execution.quote_canary_enabled = false;
        // A valid disabled config and actual ingestion constructor retain obligations.
        copybot_config::validate_association_delivery(&stopped)?;
        f.ingress(&stopped).await?;
        let recovery = f.runner(&stopped)?;
        assert!(recovery.is_enabled());
        *s.fault.lock().unwrap() = if fault.starts_with("failed") {
            "failed".into()
        } else {
            String::new()
        };
        // Ticks now schedule/harvest; two immediate ticks need not finish I/O.
        if !f.db.store.owned_sell_dispatch_ids(1)?.is_empty() {
            super::b136_r1_tick_tests::harvest(&f, &recovery).await?;
        }
        assert_eq!(sends(&s), 1);
        assert_eq!(
            super::b135_hooks::count(&c.execution.execution_signer_keypair_path),
            1
        );
        let fee = f.db.sql.query_row(
            "SELECT actual_fee FROM execution_tiny_reservations WHERE side='sell'",
            [],
            |r| r.get::<_, Option<u64>>(0),
        )?;
        assert_eq!(
            fee,
            if fault == "success_unknown_fee" {
                None
            } else {
                Some(19000)
            },
            "fault={fault} server={:?} dispatches={:?} reservations={:?}",
            s.terminal(),
            f.rows("rpc_owned_sell_dispatches"),
            f.rows("execution_tiny_reservations")
        );
        if fault.starts_with("failed") {
            assert!(f
                .db
                .store
                .load_execution_canary_cash_settlement(&id)?
                .is_none());
            assert_eq!(f.rows("positions")?, before);
            assert_eq!(f.rows("execution_failed_expense_ledger")?.len(), 1);
        } else {
            assert!(f
                .db
                .store
                .load_execution_canary_cash_settlement(&id)?
                .is_some());
        }
        let fees = f.rows("execution_tiny_reservations")?;
        let after = f.rows("positions")?;
        q::tick(&recovery, &f.db).await?;
        assert_eq!(f.rows("execution_tiny_reservations")?, fees);
        assert_eq!(f.rows("positions")?, after);
        println!("B136_RECOVERY {fault} stop+expired+policy_removed sends=1 actual_fee={fee:?} replay_once=true");
        drop(recovery);
        super::b136_r1_tick_tests::idle(&f).await?;
        s.healthy();
    }
    Ok(())
}
#[tokio::test]
async fn b136_transport_timeout_committed_signature_reconciles_without_resend() -> Result<()> {
    let f = Fixture::new().await?;
    let s = Server::new().await?;
    let c = b136_config::load(&f, &s.url, true)?;
    f.ingress(&c).await?;
    let r = f.runner(&c)?;
    let (at, release) = s.hold("sendTransaction");
    q::tick(&r, &f.db).await?;
    tokio::time::timeout(Duration::from_secs(4), at).await??;
    let before = f.rows("positions")?;
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(f.rows("positions")?, before);
    assert_eq!(f.rows("rpc_owned_sell_dispatches")?.len(), 1);
    drop(r);
    release.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    f.ingress(&c).await?;
    finish(&f, &f.runner(&c)?).await?;
    assert_eq!(sends(&s), 1);
    assert_eq!(
        super::b135_hooks::count(&c.execution.execution_signer_keypair_path),
        1
    );
    s.healthy();
    Ok(())
}
#[tokio::test]
async fn b136_receipt_cannot_settle_reopened_position_generation() -> Result<()> {
    let f = Fixture::new().await?;
    let s = Server::new().await?;
    let c = b136_config::load(&f, &s.url, true)?;
    f.ingress(&c).await?;
    let r = f.runner(&c)?;
    let (at, release) = s.hold("getTransaction:3");
    q::tick(&r, &f.db).await?;
    tokio::time::timeout(Duration::from_secs(4), at).await??;
    f.db.sql
        .execute("UPDATE positions SET opened_ts='2026-09-10T00:00:00Z'", [])?;
    let before = f.rows("positions")?;
    release.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    let _ = q::tick(&r, &f.db).await;
    drop(r);
    let mut stopped = c.clone();
    stopped.execution.owned_sell_preparation = None;
    stopped.execution.canary_tiny_submit_enabled = false;
    let recovery = f.runner(&stopped)?;
    let _ = q::tick(&recovery, &f.db).await;
    assert_eq!(sends(&s), 1);
    assert_eq!(f.rows("positions")?, before);
    assert_eq!(f.rows("fills")?.len(), 1, "original BUY only");
    let reason: String = f.db.sql.query_row(
        "SELECT reason FROM execution_canary_receipt_proofs WHERE side='sell'",
        [],
        |r| r.get(0),
    )?;
    assert!(reason.contains("OwnedPositionChanged"), "{reason}");
    Ok(())
}
