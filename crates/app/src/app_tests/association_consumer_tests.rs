use super::association_fixture as f;
use anyhow::Result;
use copybot_core_types::association_delivery::*;
use copybot_ingestion::ReplayInput;
use std::{future::Future, task::Poll};
#[tokio::test]
#[ignore = "explicit validated local protobuf fixtures"]
async fn b89_actual_consumer_unknown_time_a_b_restart_and_zero_financial_actions() -> Result<()> {
    let m = f::metadata("fixture.json");
    let c = f::config(&m);
    let db = f::Db::new("generation")?;
    let before = db.financial_counts()?;
    db.position("A", m["token"].as_str().unwrap())?;
    let (mut consumer, tx) = f::start(&db, &c, "first-session").await?;
    consumer.poll(&db.store).await?; // durable session start
    tx.send(f::update("missing", 1)).await?;
    consumer.poll(&db.store).await?; // admission, before block
    let (_, binding, terminal, _) = f::row(&db)?;
    assert!(binding.contains("A"));
    assert!(terminal.is_none());
    db.sql.execute(
        "UPDATE positions SET state='closed' WHERE position_id='A'",
        [],
    )?;
    db.position("B", m["token"].as_str().unwrap())?;
    tx.send(f::update("block", 2)).await?;
    consumer.poll(&db.store).await?;
    tx.send(ReplayInput::End(3)).await?;
    drop(tx);
    f::drain(&mut consumer, &db).await?;
    drop(consumer);
    let (a, b, t, conflict) = f::row(&db)?;
    let a: AdmissionFacts = serde_json::from_str(&a)?;
    assert_eq!(a.message_time, MessageTime::Missing);
    assert_eq!(b, binding);
    assert!(!conflict);
    assert!(matches!(
        serde_json::from_str::<Terminal>(&t.unwrap())?,
        Terminal::ProviderAsserted(_)
    ));
    let (mut reopened, tx) = f::start(&db, &c, "restart-session").await?;
    let producer = tokio::spawn(async move {
        tx.send(f::update("missing", 1)).await.unwrap();
        tx.send(f::update("block", 2)).await.unwrap();
        tx.send(ReplayInput::End(3)).await.unwrap();
    });
    f::drain(&mut reopened, &db).await?;
    producer.await?;
    assert_eq!(f::row(&db)?.1, binding);
    assert_eq!(db.identities()?, 1);
    assert_eq!(db.financial_counts()?, before);
    Ok(())
}
#[tokio::test]
#[ignore = "explicit validated local protobuf fixtures"]
async fn b89_held_sqlite_write_keeps_unrelated_app_task_running_and_snapshots_before_await(
) -> Result<()> {
    let m = f::metadata("fixture.json");
    let c = f::config(&m);
    let db = f::Db::new("held")?;
    db.position("A", m["token"].as_str().unwrap())?;
    let (mut consumer, tx) = f::start(&db, &c, "held-session").await?;
    consumer.poll(&db.store).await?;
    db.sql.execute_batch("BEGIN IMMEDIATE")?;
    tx.send(f::update("missing", 1)).await?;
    // Poll the same production consumer branch until its one write is in flight.
    // Holding the SQLite write lock is the cause; no arbitrary sleep asserts progress.
    loop {
        {
            let mut future = Box::pin(consumer.poll(&db.store));
            std::future::poll_fn(|cx| {
                assert!(future.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        if consumer.pending.is_some() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(db.identities()?, 0);
    let (done, wait) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        done.send("unrelated-maintenance-completed").unwrap();
    });
    assert_eq!(wait.await?, "unrelated-maintenance-completed");
    assert!(consumer.pending.is_some());
    assert_eq!(db.identities()?, 0);
    db.sql.execute(
        "UPDATE positions SET state='closed' WHERE position_id='A'",
        [],
    )?;
    db.position("B", m["token"].as_str().unwrap())?;
    db.sql.execute_batch("COMMIT")?;
    consumer.poll(&db.store).await?;
    let (_, binding, _, _) = f::row(&db)?;
    let binding: CandidateGeneration = serde_json::from_str(&binding)?;
    assert!(matches!(binding,CandidateGeneration::AppObserved{position_id,..} if position_id=="A"));
    tx.send(ReplayInput::End(2)).await?;
    drop(tx);
    f::drain(&mut consumer, &db).await?;
    Ok(())
}
#[tokio::test]
#[ignore = "explicit validated local protobuf fixtures"]
async fn b89_actual_consumer_unknown_binding_conflict_and_time_matrix() -> Result<()> {
    for name in ["missing", "invalid", "plus1ns"] {
        let m = f::metadata("fixture.json");
        let c = f::config(&m);
        let db = f::Db::new(name)?;
        let before = db.financial_counts()?;
        let (mut consumer, tx) = f::start(&db, &c, "time-session").await?;
        let label = name.to_string();
        let producer = tokio::spawn(async move {
            for v in [
                f::update(&label, 1),
                f::update("block", 2),
                f::update("conflict", 3),
                ReplayInput::End(4),
            ] {
                tx.send(v).await.unwrap();
            }
        });
        f::drain(&mut consumer, &db).await?;
        producer.await?;
        let (a, b, _, conflict) = f::row(&db)?;
        assert_eq!(
            serde_json::from_str::<CandidateGeneration>(&b)?,
            CandidateGeneration::Unknown
        );
        assert!(conflict);
        let a: AdmissionFacts = serde_json::from_str(&a)?;
        match name {
            "missing" => assert_eq!(a.message_time, MessageTime::Missing),
            "invalid" => assert_eq!(a.message_time, MessageTime::InvalidNanos(-1)),
            _ => assert_eq!(
                a.message_time,
                MessageTime::CreatedAt {
                    seconds: 10,
                    nanos: 1
                }
            ),
        };
        assert_eq!(db.financial_counts()?, before);
    }
    Ok(())
}

#[tokio::test]
#[ignore = "explicit validated local protobuf fixtures"]
async fn b89_write_failure_stops_receive_without_ack() -> Result<()> {
    let m = f::metadata("fixture.json");
    let c = f::config(&m);
    let db = f::Db::new("failed-write-stop")?;
    let (mut consumer, tx) = f::start(&db, &c, "failed-write-session").await?;
    consumer.poll(&db.store).await?;
    db.sql.execute_batch("CREATE TRIGGER ignore_delivery BEFORE INSERT ON association_inbox_events BEGIN SELECT RAISE(IGNORE); END;")?;
    tx.send(f::update("missing", 1)).await?;
    assert!(consumer.poll(&db.store).await.is_err());
    tokio::time::timeout(std::time::Duration::from_secs(1), tx.closed()).await?;
    assert_eq!(db.identities()?, 0);
    let count: i64 =
        db.sql
            .query_row("SELECT count(*) FROM association_inbox_events", [], |r| {
                r.get(0)
            })?;
    assert_eq!(count, 1); // only prior session start
    assert!(consumer.poll(&db.store).await.is_err());
    Ok(())
}
