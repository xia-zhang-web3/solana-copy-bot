use super::{
    association_fixture as f, association_parent_fixture as p, association_sell_fixture as s,
    b97_fixture as b,
};
use anyhow::Result;
use copybot_core_types::association_delivery::*;
use copybot_ingestion::ReplayInput;
use copybot_storage_core::association_inbox::AssociationInbox;
use std::time::Duration;

fn root() -> Result<std::path::PathBuf> {
    Ok(std::path::PathBuf::from(std::env::var("B90_FIXTURE_DIR")?)
        .parent()
        .unwrap()
        .join("b97"))
}
#[tokio::test]
#[ignore = "hash-bound partial Block last parent; no repeated SELL"]
async fn b97_actual_last_parent_only_after_all_terminals_stages() -> Result<()> {
    let m = p::meta("direct")?;
    let template = f::Db::new("b97-parent-template")?;
    s::seed(&template, &m)?;
    p::stage(
        &template,
        "direct",
        &m,
        p::frames(&m),
        "parent-template",
        false,
    )
    .await?;
    let db = f::Db::new("b97-last-parent")?;
    s::seed(&db, &m)?;
    let mut inbox = AssociationInbox::open(&db.path, p::limits())?;
    let mut omitted = 0;
    for (event, candidate) in b::event_rows(&template)? {
        if matches!(&event.event,DeliveryEvent::Parent(e) if e.child.slot==120) {
            omitted += 1;
            continue;
        }
        inbox.persist(&event, &candidate)?;
    }
    assert_eq!(omitted, 1);
    while inbox.has_sell_preparation_work()? {
        inbox.recover_sell_preparation()?;
    }
    let before = s::snapshot(&db)?;
    let first = p::read(&db, &m)?.first;
    let (mut consumer, tx) = f::start(&db, &f::config(&m), "last-parent").await?;
    b::until(&mut consumer, &db, || b::idle(&db)).await?;
    assert_eq!(b::count(&db, "ordered_source_sell_intents")?, 0);
    let ids = db.identities()?;
    tx.send(ReplayInput::Update {
        offset_ns: 1,
        payload: std::fs::read(root()?.join("late-parent.pb"))?,
    })
    .await?;
    b::until(&mut consumer, &db, || {
        Ok(b::count(&db, "ordered_source_sell_intents")? == 1 && b::idle(&db)?)
    })
    .await?;
    assert_eq!(db.identities()?, ids);
    assert_eq!(
        b::saved(&db, m["sell"]["signature"].as_str().unwrap())?
            .unwrap()
            .first,
        first
    );
    assert_eq!(b::count(&db, "source_sell_signature_claims")?, 1);
    assert_eq!(s::snapshot(&db)?, before);
    tx.send(ReplayInput::End(2)).await?;
    f::drain(&mut consumer, &db).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "offline derived17 SELL fanout; actual consumer Unknown A and ready B"]
async fn b97_actual_many_dependents_bounded_turns_unknown_a_and_idle() -> Result<()> {
    let m = s::meta()?;
    let db = f::Db::new("b97-many")?;
    let (mut consumer, tx) = f::start(&db, &f::config(&m), "many").await?;
    tx.send(s::update("sell", 1)).await?;
    b::until(&mut consumer, &db, || Ok(db.identities()? == 1)).await?;
    let unknown = p::read(&db, &m)?.first;
    assert_eq!(unknown.candidate, CandidateGeneration::Unknown);
    s::seed(&db, &m)?;
    let before = s::snapshot(&db)?;
    let input = tx.clone();
    let path = root()?;
    let producer = tokio::spawn(async move {
        for n in 0..17 {
            input
                .send(ReplayInput::Update {
                    offset_ns: n + 2,
                    payload: std::fs::read(path.join(format!("sell-{n}.pb")))?,
                })
                .await?;
        }
        input.send(s::update("source", 20)).await?;
        input.send(s::update("our", 21)).await?;
        input
            .send(ReplayInput::Update {
                offset_ns: 22,
                payload: std::fs::read(path.join("many-block.pb"))?,
            })
            .await?;
        Ok::<_, anyhow::Error>(())
    });
    let mut turns = 0;
    let mut prior = 0;
    tokio::time::timeout(Duration::from_secs(10), async {
        while prior < 17 || !b::idle(&db)? {
            consumer.poll(&db.store).await?;
            let next = b::count(&db, "ordered_source_sell_intents")?;
            assert!(
                (0..=2).contains(&(next - prior)),
                "one direct and one continuation at most"
            );
            turns += 1;
            assert!(turns < 300);
            prior = next;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    producer.await??;
    assert!(turns > 17);
    assert_eq!(b::count(&db, "source_sell_signature_claims")?, 17);
    assert!(b::saved(&db, m["sell"]["signature"].as_str().unwrap())?.is_none());
    assert_eq!(p::read(&db, &m)?.first, unknown);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), consumer.poll(&db.store))
            .await
            .is_err()
    );
    assert_eq!(s::snapshot(&db)?, before);
    tx.send(ReplayInput::End(23)).await?;
    f::drain(&mut consumer, &db).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "actual startup/recovery at configured cap fails closed repeatedly"]
async fn b97_actual_stage_cap_stops_intake_and_reopen_preserves_unstaged_cursor() -> Result<()> {
    let (db, m) = b::observed("b97-cap").await?;
    let mut config = f::config(&m);
    let usage = AssociationInbox::open(&db.path, p::limits())?.usage()?;
    config
        .ingestion
        .yellowstone_association
        .as_mut()
        .unwrap()
        .inbox
        .count = usage.0 + 1;
    let before = s::snapshot(&db)?;
    for n in 0..2 {
        let (mut consumer, tx) = f::start(&db, &config, &format!("cap-{n}")).await?;
        let error = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Err(e) = consumer.poll(&db.store).await {
                    return e;
                }
            }
        })
        .await?;
        assert!(format!("{error:#}").contains("inbox full"));
        tokio::time::timeout(Duration::from_secs(1), tx.closed()).await?;
        assert_eq!(b::count(&db, "ordered_source_sell_intents")?, 0);
        assert_eq!(b::count(&db, "source_sell_signature_claims")?, 0);
        assert_eq!(s::snapshot(&db)?, before);
    }
    Ok(())
}
