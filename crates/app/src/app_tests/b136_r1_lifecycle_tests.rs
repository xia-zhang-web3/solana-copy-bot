use super::{
    b136_endpoint_tests::sends,
    b136_fixture::Fixture,
    b136_r1_hooks as hooks,
    b136_r1_tick_tests::{exact_settlement, harvest, harvest_pending, idle, pending, stopped},
    b136_server::Server,
};
use anyhow::{Context, Result};
use chrono::Utc;
use std::time::Duration;

#[tokio::test]
async fn b136_r1_pending_completion_releases_job_slot_and_retains_unknown_hold() -> Result<()> {
    for fault in ["unknown", "receipt_unknown", "failed_unknown_fee"] {
        let f = Fixture::new().await?;
        let s = Server::new().await?;
        let c = pending(&f, &s).await?;
        let disabled = stopped(&f, &c)?;
        let r = f.runner(&disabled)?;
        let before = f.rows("positions")?;
        *s.fault.lock().unwrap() = fault.into();
        let (at, release) = s.hold("getSignatureStatuses");
        r.process_tick(&f.db.store, Utc::now()).await?;
        tokio::time::timeout(Duration::from_secs(2), at)
            .await
            .with_context(|| {
                format!(
                    "getSignatureStatuses not reached: server={:?} job={:?} dispatches={:?}",
                    s.terminal(),
                    hooks::read(&f.db.path),
                    f.rows("rpc_owned_sell_dispatches")
                )
            })??;
        // Hold the NEXT batch separately, after the first one reports pending.
        let next_method = if fault == "unknown" {
            "getSignatureStatuses"
        } else {
            "getTransaction"
        };
        // For receipt/failed cases, install the next hold only after the first
        // batch exits; its already-finished handle retains the completion.
        release.send(()).unwrap();
        idle(&f).await?;
        let count = hooks::read(&f.db.path).scheduled;
        let (at_next, release_next) = s.hold(next_method);
        let done = harvest_pending(&f, &r).await?;
        assert_eq!(done.orphan_recovery_reconciled, 0);
        assert!(
            done.last_error.is_some(),
            "pending result must be observable"
        );
        assert!(done.has_status_change());
        r.process_tick(&f.db.store, Utc::now()).await?;
        tokio::time::timeout(Duration::from_secs(2), at_next).await??;
        for _ in 0..5 {
            tokio::time::timeout(
                Duration::from_millis(200),
                r.process_tick(&f.db.store, Utc::now()),
            )
            .await??;
        }
        assert_eq!(hooks::read(&f.db.path).scheduled, count + 1);
        assert_eq!(f.rows("positions")?, before);
        assert_eq!(f.rows("fills")?.len(), 1);
        assert_eq!(f.db.sql.query_row("SELECT fee_bound FROM execution_tiny_reservations WHERE side='sell' AND actual_fee IS NULL", [], |r|r.get::<_,u64>(0))?, 100000);
        assert_eq!(sends(&s), 1);
        *s.fault.lock().unwrap() = if fault.starts_with("failed") {
            "failed".into()
        } else {
            String::new()
        };
        release_next.send(()).unwrap();
        let resolved = harvest(&f, &r).await?;
        assert_eq!(resolved.orphan_recovery_reconciled, 1);
        if fault.starts_with("failed") {
            assert_eq!(f.rows("positions")?, before);
            assert_eq!(f.rows("execution_failed_expense_ledger")?.len(), 1);
            assert_eq!(
                f.db.sql.query_row(
                    "SELECT actual_fee FROM execution_tiny_reservations WHERE side='sell'",
                    [],
                    |r| r.get::<_, u64>(0)
                )?,
                19000
            );
        } else {
            exact_settlement(&f, &s, &c)?;
        }
        let fees = f.rows("execution_tiny_reservations")?;
        let fills = f.rows("fills")?;
        let expenses = f.rows("execution_failed_expense_ledger")?;
        drop(r);
        idle(&f).await?;
        let reopened = f.runner(&disabled)?;
        for _ in 0..5 {
            reopened.process_tick(&f.db.store, Utc::now()).await?;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(f.rows("execution_tiny_reservations")?, fees);
        assert_eq!(f.rows("fills")?, fills);
        assert_eq!(f.rows("execution_failed_expense_ledger")?, expenses);
        assert_eq!(sends(&s), 1);
        assert_eq!(
            super::b135_hooks::count(&c.execution.execution_signer_keypair_path),
            1
        );
        println!("B136_R1_PENDING fault={fault} pending_visible=true hold=100000 next_batch_handshake=true completion_harvested=true once=true sends=1");
        s.healthy();
    }
    Ok(())
}

#[tokio::test]
async fn b136_r1_last_clone_drop_cancels_held_receipt_before_release_then_reopen_settles(
) -> Result<()> {
    let f = Fixture::new().await?;
    let s = Server::new().await?;
    let c = pending(&f, &s).await?;
    let disabled = stopped(&f, &c)?;
    let r = f.runner(&disabled)?;
    *s.fault.lock().unwrap() = String::new();
    let before = f.rows("positions")?;
    let reservations = f.rows("execution_tiny_reservations")?;
    let (at, release) = s.hold("getTransaction");
    r.process_tick(&f.db.store, Utc::now()).await?;
    tokio::time::timeout(Duration::from_secs(2), at).await??;
    assert_eq!(hooks::read(&f.db.path).running, 1);
    let clone = r.clone();
    drop(r);
    assert_eq!(hooks::read(&f.db.path).running, 1, "clone retains job");
    let signature: String = f.db.sql.query_row(
        "SELECT tx_signature FROM execution_canary_dispatch WHERE side='sell'",
        [],
        |r| r.get(0),
    )?;
    drop(clone);
    idle(&f).await?; // This MUST complete with the server still holding receipt.
    assert_eq!(f.rows("positions")?, before);
    assert_eq!(f.rows("execution_tiny_reservations")?, reservations);
    assert_eq!(f.rows("fills")?.len(), 1);
    release.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        f.rows("positions")?,
        before,
        "cancelled job cannot settle on late response"
    );
    f.ingress(&disabled).await?;
    let r = f.runner(&disabled)?;
    assert_eq!(harvest(&f, &r).await?.orphan_recovery_reconciled, 1);
    exact_settlement(&f, &s, &c)?;
    assert_eq!(
        f.db.sql.query_row(
            "SELECT tx_signature FROM execution_canary_dispatch WHERE side='sell'",
            [],
            |r| r.get::<_, String>(0)
        )?,
        signature
    );
    assert_eq!(hooks::read(&f.db.path).max_running, 1);
    println!("B136_R1_DROP held_receipt=true worker_exited_before_release=true cancelled_settlement=0 durable_signature_and_hold=true reopened_reconcile_only=true send=1");
    s.healthy();
    Ok(())
}

#[tokio::test]
async fn b136_r1_background_database_failure_is_harvested_and_recovery_can_continue() -> Result<()>
{
    let f = Fixture::new().await?;
    let s = Server::new().await?;
    let c = pending(&f, &s).await?;
    let disabled = stopped(&f, &c)?;
    let r = f.runner(&disabled)?;
    let view: String = f.db.sql.query_row(
        "SELECT sql FROM sqlite_master WHERE name='execution_canary_unresolved_dispatch'",
        [],
        |r| r.get(0),
    )?;
    let before = f.rows("positions")?;
    let fees = f.rows("execution_tiny_reservations")?;
    // Fixture-only schema fault at the unchanged durable selection API.
    f.db.sql
        .execute_batch("DROP VIEW execution_canary_unresolved_dispatch")?;
    r.process_tick(&f.db.store, Utc::now()).await?;
    let error = harvest(&f, &r).await.unwrap_err();
    let chain = format!("{error:#}");
    assert!(
        chain.contains("owned recovery receipt batch failed") && chain.contains("no such table"),
        "{chain}"
    );
    assert_eq!(f.rows("positions")?, before);
    assert_eq!(f.rows("execution_tiny_reservations")?, fees);
    f.db.sql.execute_batch(&view)?;
    *s.fault.lock().unwrap() = String::new();
    assert_eq!(harvest(&f, &r).await?.orphan_recovery_reconciled, 1);
    exact_settlement(&f, &s, &c)?;
    println!("B136_R1_ERROR actual_tick_error=true sqlite_cause_preserved=true no_silent_green=true slot_reusable=true send=1");
    Ok(())
}

#[test]
fn b136_r1_parent_shutdown_does_not_strand_or_panic_cancelled_recovery() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (f, s, release) = runtime.block_on(async {
        let f = Fixture::new().await?;
        let s = Server::new().await?;
        let c = pending(&f, &s).await?;
        let disabled = stopped(&f, &c)?;
        let r = f.runner(&disabled)?;
        let (at, release) = s.hold("getSignatureStatuses");
        r.process_tick(&f.db.store, Utc::now()).await?;
        tokio::time::timeout(Duration::from_secs(2), at)
            .await
            .with_context(|| {
                format!(
                    "getSignatureStatuses not reached: server={:?} job={:?} dispatches={:?}",
                    s.terminal(),
                    hooks::read(&f.db.path),
                    f.rows("rpc_owned_sell_dispatches")
                )
            })??;
        assert_eq!(hooks::read(&f.db.path).running, 1);
        drop(r); // No yield or grace wait before shutting down the parent runtime.
        Ok::<_, anyhow::Error>((f, s, release))
    })?;
    runtime.shutdown_timeout(Duration::from_secs(2));
    assert_eq!(hooks::read(&f.db.path).running, 0);
    assert_eq!(f.rows("fills")?.len(), 1);
    assert_eq!(sends(&s), 1);
    assert_eq!(f.db.sql.query_row("SELECT fee_bound FROM execution_tiny_reservations WHERE side='sell' AND actual_fee IS NULL", [], |r|r.get::<_,u64>(0))?, 100000);
    drop(release);
    println!("B136_R1_SHUTDOWN parent_closed_immediately=true held_worker_exited=true send=1 settlement=0 hold=100000");
    Ok(())
}
