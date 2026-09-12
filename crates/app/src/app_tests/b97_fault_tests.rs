use super::{
    association_fixture as f, association_parent_fixture as p, association_sell_fixture as s,
    b97_fixture as b, b97_sqlite_hooks as h,
};
use anyhow::Result;
use copybot_storage_core::{association_inbox::AssociationInbox, ordered_source_sell::*};
use std::future::Future;
use std::{task::Poll, time::Duration};

#[tokio::test]
#[ignore = "real SQLite faults in actual off-thread consumer; hash-bound replay"]
async fn b97_actual_precommit_postcommit_readback_and_commit_failure_stop_intake() -> Result<()> {
    for mode in [h::Mode::PreRead, h::Mode::PostRead, h::Mode::Commit] {
        let (db, m) = b::observed("b97-fault").await?;
        let before = s::snapshot(&db)?;
        let config = f::config(&m);
        let install = h::Installed::new()?;
        let started = f::start(&db, &config, "fault").await;
        drop(install);
        let (mut consumer, tx) = started?;
        let fault = h::Fault::arm(mode);
        let error = tokio::time::timeout(Duration::from_secs(5), async {
            for _ in 0..30 {
                match consumer.poll(&db.store).await {
                    Ok(()) => {}
                    Err(e) => return Ok::<_, anyhow::Error>(e),
                }
            }
            anyhow::bail!("fault not reached")
        })
        .await??;
        assert!(
            error.to_string().contains("association inbox failed"),
            "{error:#}"
        );
        fault.verify();
        drop(fault);
        tokio::time::timeout(Duration::from_secs(1), tx.closed()).await?;
        let expected = if mode == h::Mode::PostRead { 1 } else { 0 };
        assert_eq!(b::count(&db, "ordered_source_sell_intents")?, expected);
        assert_eq!(b::count(&db, "source_sell_signature_claims")?, expected);
        assert_eq!(s::snapshot(&db)?, before);
        let preserved = b::saved(&db, m["sell"]["signature"].as_str().unwrap())?;
        drop(consumer);
        let (mut consumer, _tx) = f::start(&db, &config, "recover").await?;
        b::until(&mut consumer, &db, || {
            Ok(b::count(&db, "ordered_source_sell_intents")? == 1 && b::idle(&db)?)
        })
        .await?;
        assert_eq!(b::count(&db, "source_sell_signature_claims")?, 1);
        if preserved.is_some() {
            assert_eq!(
                b::saved(&db, m["sell"]["signature"].as_str().unwrap())?,
                preserved
            );
        }
        assert_eq!(s::snapshot(&db)?, before);
    }
    Ok(())
}

#[tokio::test]
#[ignore = "actual Pending held after SQLite commit, canceled before ACK"]
async fn b97_actual_pending_commit_before_ack_cancel_and_restart_one_identity() -> Result<()> {
    let (db, m) = b::observed("b97-ack").await?;
    let before = s::snapshot(&db)?;
    let config = f::config(&m);
    let install = h::Installed::new()?;
    let started = f::start(&db, &config, "held-ack").await;
    drop(install);
    let (mut consumer, _tx) = started?;
    let gate = h::Fault::arm(h::Mode::HoldPost);
    tokio::time::timeout(Duration::from_secs(5), async {
        while !gate.hit() {
            tokio::select! {
                result=consumer.poll(&db.store)=>{result?;}
                _=tokio::time::sleep(Duration::from_millis(2))=>{}
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    assert_eq!(b::count(&db, "ordered_source_sell_intents")?, 1);
    let id = consumer.pending.as_ref().unwrap().id();
    assert!(!consumer.pending.as_ref().unwrap().is_finished());
    {
        let mut pending = Box::pin(consumer.poll(&db.store));
        std::future::poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    assert_eq!(consumer.pending.as_ref().unwrap().id(), id);
    // Independent maintenance future runs while the real SQL writer is suspended.
    let (send, recv) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        send.send(97).unwrap();
    });
    assert_eq!(recv.await?, 97);
    let saved = b::saved(&db, m["sell"]["signature"].as_str().unwrap())?;
    let pending = consumer.pending.take().unwrap();
    drop(consumer);
    gate.release();
    let (_, envelope, result) = pending.await?;
    result?;
    drop(envelope);
    gate.verify();
    drop(gate);
    let (mut restarted, _tx) = f::start(&db, &config, "after-lost-ack").await?;
    b::until(&mut restarted, &db, || b::idle(&db)).await?;
    assert_eq!(b::count(&db, "ordered_source_sell_intents")?, 1);
    assert_eq!(b::count(&db, "source_sell_signature_claims")?, 1);
    assert_eq!(
        b::saved(&db, m["sell"]["signature"].as_str().unwrap())?,
        saved
    );
    assert_eq!(s::snapshot(&db)?, before);
    Ok(())
}

#[tokio::test]
#[ignore = "actual consumer stage INSERT refusal stops receive before ACK"]
async fn b97_actual_stage_ignore_abort_preserve_failed_turn_and_stop_intake() -> Result<()> {
    for action in ["IGNORE", "ABORT,'refused'"] {
        let (db, m) = b::observed("b97-insert-fault").await?;
        let before = s::snapshot(&db)?;
        db.sql.execute_batch(&format!("CREATE TRIGGER fault BEFORE INSERT ON ordered_source_sell_intents BEGIN SELECT RAISE({action}); END;"))?;
        let (mut consumer, tx) = f::start(&db, &f::config(&m), "fault").await?;
        let mut failed = false;
        for _ in 0..30 {
            let cursor: String = db.sql.query_row(
                "SELECT json_array(after_signature,complete) FROM association_sell_bootstrap",
                [],
                |r| r.get(0),
            )?;
            if consumer.poll(&db.store).await.is_err() {
                assert_eq!(db.sql.query_row("SELECT json_array(after_signature,complete) FROM association_sell_bootstrap",[],|r|r.get::<_,String>(0))?,cursor);
                failed = true;
                break;
            }
        }
        assert!(failed);
        tokio::time::timeout(Duration::from_secs(1), tx.closed()).await?;
        assert_eq!(b::count(&db, "ordered_source_sell_intents")?, 0);
        assert_eq!(b::count(&db, "source_sell_signature_claims")?, 0);
        assert_eq!(s::snapshot(&db)?, before);
        db.sql.execute_batch("DROP TRIGGER fault")?;
        let mut inbox = AssociationInbox::open_ordered_sell_consumer(&db.path, p::limits())?;
        while inbox.has_sell_preparation_work()? {
            inbox.recover_sell_preparation()?;
        }
        assert_eq!(b::count(&db, "ordered_source_sell_intents")?, 1);
        assert_eq!(
            inbox.revalidate_ordered_source_sell_intent(&format!(
                "source-sell:{}",
                m["sell"]["signature"].as_str().unwrap()
            ))?,
            OrderedSellDecision::ValidatedNow
        );
    }
    Ok(())
}
