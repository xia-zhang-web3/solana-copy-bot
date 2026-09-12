//! Permanent producer/consumer boundaries. Synthetic holdings and local replay;
//! the frozen timer/HTTP oracle remains separate temporary evidence.
use super::{
    association_fixture as a, association_sell_fixture as s, b102_wake_hooks as h, b70_hooks,
    b97_fixture as b,
};
use crate::association_consumer::AssociationConsumer;
use anyhow::{ensure, Result};
use chrono::Utc;
use copybot_core_types::TokenQuantity;
use serde_json::Value;
use std::{collections::HashSet, time::Duration};
use tokio::sync::mpsc::Sender;
async fn parked(
    label: &str,
    lots: usize,
) -> Result<(
    a::Db,
    Value,
    AssociationConsumer,
    Sender<copybot_ingestion::ReplayInput>,
)> {
    let db = a::Db::new(label)?;
    let m = s::meta()?;
    s::seed(&db, &m)?;
    for _ in 0..lots {
        db.store.insert_shadow_lot_exact(
            m["source"]["signer"].as_str().unwrap(),
            m["our"]["token_out"].as_str().unwrap(),
            1.0,
            Some(TokenQuantity::new(1000, 3)),
            0.000001,
            Utc::now() - chrono::Duration::hours(3),
        )?;
    }
    let install = h::Installed::new()?;
    let started = a::start(&db, &a::config(&m), label).await;
    drop(install);
    let (mut c, tx) = started?;
    let feed = tokio::spawn(async move {
        for (n, name) in ["sell", "source", "our", "chain-block"].iter().enumerate() {
            tx.send(s::update(name, n as u64 + 1)).await?;
        }
        Ok::<_, anyhow::Error>(tx)
    });
    b::until(&mut c, &db, || {
        Ok(db.identities()? == 3
            && idle(&db)?
            && db.sql.query_row(
                "SELECT count(*) FROM association_inbox_identities WHERE terminal IS NOT NULL",
                [],
                |r| r.get::<_, i64>(0),
            )? == 3)
    })
    .await?;
    let tx = feed.await??;
    // Include a completed poll ACK: SQL idle alone could be a pending old result.
    loop {
        if tokio::time::timeout(Duration::from_millis(20), c.poll(&db.store))
            .await
            .is_err()
            && c.pending.is_none()
        {
            break;
        }
    }
    assert_eq!(b::count(&db, "ordered_source_sell_intents")?, 0);
    Ok((db, m, c, tx))
}
fn idle(db: &a::Db) -> Result<bool> {
    Ok(b::idle(db)? && b::count(db, "association_shadow_sell_work")? == 0)
}
fn first(db: &a::Db) -> Result<String> {
    Ok(db.sql.query_row(
        "SELECT first_binding FROM association_sell_preparations LIMIT 1",
        [],
        |r| r.get(0),
    )?)
}
async fn producer(
    db: &a::Db,
    wake: &crate::association_consumer::shadow_wake::ShadowWake,
) -> Result<crate::stale_close::StaleLotCleanupStats> {
    crate::stale_close::close_stale_shadow_lots_with_recovery(
        &db.store,
        &mut HashSet::new(),
        1,
        2,
        false,
        false,
        None,
        Utc::now(),
        Some(wake),
    )
    .await
}
#[tokio::test]
#[ignore = "hash-bound local replay; actual consumer held after SQLite pending read"]
async fn b102_wake_during_blocking_task_second_commit_before_old_ack_and_cancel() -> Result<()> {
    let _serial = b70_hooks::acquire().await;
    let (db, _m, mut c, tx) = parked("b102-pending", 2).await?;
    let binding = first(&db)?;
    let hold = h::Hold::arm();
    c.shadow_wake.notify();
    tokio::time::timeout(Duration::from_secs(3),async {
        while !hold.hit() {
            tokio::select!{r=c.poll(&db.store)=>{r?;},_=tokio::time::sleep(Duration::from_millis(2))=>{}}
        } Ok::<_,anyhow::Error>(())
    }).await??;
    assert!(c.pending.is_some());
    assert!(!c.pending.as_ref().unwrap().is_finished());
    let pending_id = c.pending.as_ref().unwrap().id();
    // Two real close commits, same pair, while the actual task holds old false.
    assert_eq!(producer(&db, &c.shadow_wake).await?.terminal_zero_closed, 2);
    assert_eq!(b::count(&db, "shadow_closed_trades")?, 2);
    assert_eq!(b::count(&db, "association_shadow_sell_work")?, 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(15), c.poll(&db.store))
            .await
            .is_err()
    );
    assert_eq!(c.pending.as_ref().unwrap().id(), pending_id);
    hold.release();
    c.poll(&db.store).await?; // ACK the old false, without consuming newer Notify
    hold.verify();
    drop(hold);
    assert!(!tx.is_closed());
    b::until(&mut c, &db, || {
        Ok(b::count(&db, "ordered_source_sell_intents")? == 1 && idle(&db)?)
    })
    .await?;
    assert_eq!(b::count(&db, "source_sell_signature_claims")?, 1);
    assert_eq!(first(&db)?, binding);
    let state = s::snapshot(&db)?;
    c.shadow_wake.notify();
    c.poll(&db.store).await?;
    assert_eq!(s::snapshot(&db)?, state);
    assert_eq!(b::count(&db, "ordered_source_sell_intents")?, 1);
    Ok(())
}
#[tokio::test]
#[ignore = "local replay; first committed close must wake even when next FIFO returns Err"]
async fn b102_partial_batch_error_keeps_first_wake_and_later_retry_is_automatic() -> Result<()> {
    let _serial = b70_hooks::acquire().await;
    let (db, _m, mut c, tx) = parked("b102-partial-error", 2).await?;
    let binding = first(&db)?;
    db.sql.execute_batch("CREATE TRIGGER second_close_fault BEFORE INSERT ON shadow_closed_trades WHEN (SELECT count(*) FROM shadow_closed_trades)>0 BEGIN SELECT RAISE(ABORT,'second close fault'); END;")?;
    assert!(producer(&db, &c.shadow_wake).await.is_err());
    assert_eq!(b::count(&db, "shadow_closed_trades")?, 1);
    assert_eq!(b::count(&db, "association_shadow_sell_work")?, 1);
    b::until(&mut c, &db, || idle(&db)).await?;
    assert_eq!(b::count(&db, "ordered_source_sell_intents")?, 0); // remaining MissingOrigin
    assert_eq!(first(&db)?, binding);
    db.sql.execute_batch("DROP TRIGGER second_close_fault")?;
    assert_eq!(producer(&db, &c.shadow_wake).await?.terminal_zero_closed, 1);
    b::until(&mut c, &db, || {
        Ok(b::count(&db, "ordered_source_sell_intents")? == 1 && idle(&db)?)
    })
    .await?;
    assert_eq!(b::count(&db, "source_sell_signature_claims")?, 1);
    assert_eq!(first(&db)?, binding);
    ensure!(!tx.is_closed());
    Ok(())
}
#[tokio::test]
#[ignore = "local producer commit before notification with ordinary consumer restart"]
async fn b102_app_restart_recovers_close_without_original_notification() -> Result<()> {
    let _serial = b70_hooks::acquire().await;
    let (db, m, c, tx) = parked("b102-lost-wake", 1).await?;
    let binding = first(&db)?;
    let lost = c.shadow_wake.clone();
    drop(c);
    drop(tx);
    producer(&db, &lost).await?;
    drop(lost); // notification has no receiver
    let (mut c, _tx) = a::start(&db, &a::config(&m), "b102-restarted").await?;
    b::until(&mut c, &db, || {
        Ok(b::count(&db, "ordered_source_sell_intents")? == 1 && idle(&db)?)
    })
    .await?;
    assert_eq!(b::count(&db, "source_sell_signature_claims")?, 1);
    assert_eq!(first(&db)?, binding);
    Ok(())
}
