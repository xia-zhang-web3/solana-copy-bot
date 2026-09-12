use super::{
    association_fixture as f, association_parent_fixture as p, association_sell_fixture as s,
    b93_fixture as receipt, b97_fixture as b, strict_quote_fixture as q,
};
use anyhow::{ensure, Result};
use chrono::Utc;
use copybot_storage_core::ordered_sell_quote::*;
use std::time::Duration;
#[tokio::test]
#[ignore = "captured inputs, seeded receipt-backed holdings, loopback only"]
async fn strict_quote_actual_partial_receipt_7000_stale_retry_4000_restart() -> Result<()> {
    let (db, m) = q::seeded("strict-partial").await?;
    let first = q::first(&db, &m)?;
    let mut server = q::Server::new().await?;
    let r = q::runner(&db, &m, &server.url, true)?;
    q::tick(&r, &db).await?;
    let held = server.request().await?;
    assert_eq!(held.query["amount"], "7000");
    assert!(db
        .store
        .load_strict_sell_quote(&q::id(&m), p::limits(), Utc::now())?
        .is_none());
    // Actual bounded tick returns while HTTP is held. New delivery intake and
    // an already-authorized bootstrap continuation still run through the consumer.
    let (mut consumer, tx) = f::start(&db, &f::config(&m), "strict-held-replay").await?;
    b::until(&mut consumer, &db, || b::idle(&db)).await?;
    let events = b::count(&db, "association_inbox_events")?;
    let path = p::root("direct").join("sell.pb");
    tx.send(copybot_ingestion::ReplayInput::Update {
        offset_ns: 1,
        payload: std::fs::read(path)?,
    })
    .await?;
    b::until(&mut consumer, &db, || {
        Ok(b::count(&db, "association_inbox_events")? > events && b::idle(&db)?)
    })
    .await?;
    q::tick(&r, &db).await?;
    let facts = receipt::receipt(&db, &m, "strict-partial", 3000)?;
    receipt::settle(&db, &facts)?;
    let after_seeded_partial = s::snapshot(&db)?;
    held.answer()?;
    let stale = q::result(&db, &m).await?;
    assert_eq!(stale.outcome, QuoteOutcome::Stale);
    assert_eq!(stale.binding.as_ref().unwrap().raw, 7000);
    assert_eq!(receipt::raw(&db, &m)?, 4000);
    // Next ordinary tick refreshes the exact amount; no status flip or rebinding.
    q::tick(&r, &db).await?;
    let request = server.request().await?;
    assert_eq!(request.query["amount"], "4000");
    request.answer()?;
    let out = q::result(&db, &m).await?;
    assert_eq!(out.outcome, QuoteOutcome::Current);
    assert_eq!(out.binding.unwrap().raw, 4000);
    assert_eq!(q::first(&db, &m)?, first);
    assert_eq!(s::snapshot(&db)?, after_seeded_partial);
    drop(r);
    let restarted = q::runner(&db, &m, &server.url, true)?;
    q::tick(&restarted, &db).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(server.seen.lock().unwrap().len(), 2);
    assert_eq!(
        b::count(&db, "ordered_sell_quote_results")?,
        1,
        "one retained observation, no attempt journal"
    );
    tx.send(copybot_ingestion::ReplayInput::End(2)).await?;
    f::drain(&mut consumer, &db).await?;
    std::fs::write(
        db.path.with_extension("strict-partial.json"),
        serde_json::to_vec_pretty(
            &serde_json::json!({"requests":*server.seen.lock().unwrap(),"stale":stale,"final":db.store.load_strict_sell_quote(&q::id(&m),p::limits(),Utc::now())?,"financial_actions_outside_explicit_fixture_settlement":0}),
        )?,
    )?;
    Ok(())
}
#[tokio::test]
#[ignore = "captured inputs and loopback quote-only mode controls"]
async fn strict_quote_flags_off_duplicate_and_no_execution_rows() -> Result<()> {
    let (db, m) = q::seeded("strict-mode").await?;
    let server = q::Server::new().await?;
    let off = q::runner(&db, &m, &server.url, false)?;
    let before = s::snapshot(&db)?;
    q::tick(&off, &db).await?;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(b::count(&db, "ordered_sell_quote_results")?, 0);
    assert!(server.seen.lock().unwrap().is_empty());
    let out = q::quote(&db, &m).await?;
    assert_eq!(out.binding.as_ref().unwrap().raw, 7000);
    assert!(out.event_time.is_none() && out.event_delay_ns.is_none());
    assert_eq!(b::count(&db, "execution_quote_canary_events")?, 0);
    assert_eq!(s::snapshot(&db)?, before);
    assert!(db
        .store
        .load_execution_canary_order_by_signal(&q::id(&m))?
        .is_none());
    let c = f::config(&m);
    for tiny in [false, true] {
        let mut e = c.execution.clone();
        e.enabled = !tiny;
        e.canary_tiny_submit_enabled = tiny;
        assert!(crate::execution_canary::ExecutionCanaryRunner::new(e)
            .for_ingestion(&c.ingestion, &db.path.to_string_lossy())
            .is_err());
    }
    Ok(())
}
#[tokio::test]
#[ignore = "captured inputs and held HTTP late identity conflict"]
async fn strict_quote_late_conflict_and_http_response_identity_refuse() -> Result<()> {
    for case in [
        "late_conflict",
        "wrong_amount",
        "wrong_mint",
        "no_response_mint",
    ] {
        let (db, m) = q::seeded(case).await?;
        let before = s::snapshot(&db)?;
        let mut server = q::Server::new().await?;
        let runner = q::runner(&db, &m, &server.url, true)?;
        q::tick(&runner, &db).await?;
        let req = server.request().await?;
        if case == "late_conflict" {
            db.sql.execute(
                "UPDATE association_inbox_identities SET conflict=1 WHERE signature=?1",
                [m["sell"]["signature"].as_str().unwrap()],
            )?;
            req.answer()?;
        } else {
            let mut body = serde_json::json!({"inputMint":req.query["inputMint"],"outputMint":req.query["outputMint"],"inAmount":req.query["amount"],"outAmount":"1000","swapMode":"ExactIn"});
            match case {
                "wrong_amount" => body["inAmount"] = "6999".into(),
                "wrong_mint" => body["inputMint"] = "other".into(),
                _ => {
                    body.as_object_mut().unwrap().remove("inputMint");
                }
            }
            req.reply.send(body).unwrap();
        }
        let out = q::result(&db, &m).await?;
        ensure!(
            out.outcome
                == if case == "late_conflict" {
                    QuoteOutcome::Stale
                } else {
                    QuoteOutcome::Unknown
                },
            "{out:?}"
        );
        assert_eq!(s::snapshot(&db)?, before);
    }
    Ok(())
}
#[tokio::test]
#[ignore = "two actual captured SELL admissions; refused A and ready B through app quote tick"]
async fn strict_quote_actual_refused_a_does_not_block_http_b() -> Result<()> {
    let m = s::meta()?;
    let db = f::Db::new("strict-a-b")?;
    s::seed(&db, &m)?;
    let (mut consumer, tx) = f::start(&db, &f::config(&m), "strict-a-b").await?;
    let root = std::path::PathBuf::from(std::env::var("B90_FIXTURE_DIR")?)
        .parent()
        .unwrap()
        .join("b97");
    let producer = tokio::spawn(async move {
        for (n, name) in ["sell-0", "sell-1"].iter().enumerate() {
            tx.send(copybot_ingestion::ReplayInput::Update {
                offset_ns: n as u64 + 1,
                payload: std::fs::read(root.join(format!("{name}.pb")))?,
            })
            .await?;
        }
        tx.send(s::update("source", 3)).await?;
        tx.send(s::update("our", 4)).await?;
        tx.send(copybot_ingestion::ReplayInput::Update {
            offset_ns: 5,
            payload: std::fs::read(root.join("many-block.pb"))?,
        })
        .await?;
        tx.send(copybot_ingestion::ReplayInput::End(6)).await?;
        Ok::<_, anyhow::Error>(())
    });
    f::drain(&mut consumer, &db).await?;
    producer.await??;
    let ids = db
        .sql
        .prepare("SELECT intent_id,signature FROM ordered_source_sell_intents ORDER BY intent_id")?
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    assert_eq!(ids.len(), 2);
    db.sql.execute(
        "UPDATE association_inbox_identities SET conflict=1 WHERE signature=?1",
        [&ids[0].1],
    )?;
    let before = s::snapshot(&db)?;
    let mut server = q::Server::new().await?;
    let r = q::runner(&db, &m, &server.url, true)?;
    q::tick(&r, &db).await?;
    server.request().await?.answer()?;
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(out) =
                db.store
                    .load_strict_sell_quote(&ids[1].0, p::limits(), Utc::now())?
            {
                return Ok::<_, anyhow::Error>(out);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??;
    assert_eq!(result.outcome, QuoteOutcome::Current);
    assert_eq!(result.binding.unwrap().intent_id, ids[1].0);
    assert_eq!(
        db.store
            .load_strict_sell_quote(&ids[0].0, p::limits(), Utc::now())?
            .unwrap()
            .outcome,
        QuoteOutcome::Unknown
    );
    assert_eq!(s::snapshot(&db)?, before);
    assert_eq!(server.seen.lock().unwrap().len(), 1);
    Ok(())
}
