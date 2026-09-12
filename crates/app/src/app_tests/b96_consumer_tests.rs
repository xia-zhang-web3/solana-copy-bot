//! Real decoder/AssociationConsumer to SQLite, automatic staging, with explicit fresh revalidation.
use super::{
    association_fixture as f, association_parent_fixture as p, association_sell_fixture as s,
};
use anyhow::Result;
use copybot_storage_core::{association_inbox::AssociationInbox, ordered_source_sell::*};

#[tokio::test]
#[ignore = "explicit hash-bound offline protobuf fixtures"]
async fn b96_actual_consumer_unknown_time_within_automatic_stage() -> Result<()> {
    replay(false).await
}
#[tokio::test]
#[ignore = "explicit hash-bound offline protobuf fixtures"]
async fn b96_actual_consumer_unknown_time_across_automatic_stage() -> Result<()> {
    replay(true).await
}
async fn replay(across: bool) -> Result<()> {
    let m = if across {
        p::meta("direct")?
    } else {
        s::meta()?
    };
    let db = f::Db::new(&format!("b96-consumer-{across}"))?;
    s::seed(&db, &m)?;
    let before = db.financial_counts()?;
    if across {
        p::stage(&db, "direct", &m, p::frames(&m), "b96-across", false).await?;
    } else {
        let c = f::config(&m);
        let (mut consumer, tx) = f::start(&db, &c, "b96-within").await?;
        let producer = tokio::spawn(async move {
            for (i, name) in ["sell", "source", "our", "chain-block"].iter().enumerate() {
                tx.send(s::update(name, i as u64 + 1)).await?;
            }
            tx.send(copybot_ingestion::ReplayInput::End(5)).await?;
            Ok::<_, anyhow::Error>(())
        });
        f::drain(&mut consumer, &db).await?;
        producer.await??;
    }
    assert_eq!(db.financial_counts()?, before);
    assert_eq!(
        db.sql.query_row(
            "SELECT count(*) FROM ordered_source_sell_intents",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        1,
        "actual consumer must automatically stage one strict intent"
    );
    let sig = m["sell"]["signature"].as_str().unwrap();
    let mut inbox = AssociationInbox::open(&db.path, p::limits())?;
    let preparation = inbox.sell_preparation(sig)?.unwrap();
    let actual = inbox.stage_ordered_source_sell_intent(sig, PROVIDER_ORDER_STRICT_V1)?;
    let OrderedSellStage::Existing(intent) = actual else {
        panic!("actual storage API: {actual:?}")
    };
    assert_eq!(intent.first, preparation.first);
    assert_eq!(
        intent.first.sell.admission.message_time,
        copybot_core_types::association_delivery::MessageTime::Missing
    );
    let json = serde_json::to_value(&intent)?;
    assert!(json.get("event_ts").is_none());
    assert!(json.get("ts_utc").is_none());
    assert_eq!(db.financial_counts()?, before);
    drop(inbox);
    let mut inbox = AssociationInbox::open(&db.path, p::limits())?;
    assert_eq!(
        inbox.revalidate_ordered_source_sell_intent(&intent.intent_id)?,
        OrderedSellDecision::ValidatedNow
    );
    assert_eq!(
        inbox.stage_ordered_source_sell_intent(sig, PROVIDER_ORDER_STRICT_V1)?,
        OrderedSellStage::Existing(intent.clone())
    );
    assert!(db
        .store
        .load_execution_source_sell_intent(&intent.intent_id)?
        .is_none());
    std::fs::write(
        db.path.with_extension("ordered.json"),
        serde_json::to_vec_pretty(&intent)?,
    )?;
    Ok(())
}
#[tokio::test]
#[ignore = "derived protobuf created_at pair, identical Info/graph bytes"]
async fn b96_actual_consumer_future_created_at_preserves_strict_decision() -> Result<()> {
    let m = s::meta()?;
    let db = f::Db::new("b96-consumer-future")?;
    s::seed(&db, &m)?;
    let before = db.financial_counts()?;
    let c = f::config(&m);
    let (mut consumer, tx) = f::start(&db, &c, "b96-future").await?;
    let payload = std::fs::read(std::env::var("B96_FUTURE_SELL")?)?;
    let producer = tokio::spawn(async move {
        tx.send(copybot_ingestion::ReplayInput::Update {
            offset_ns: 1,
            payload,
        })
        .await?;
        for (i, name) in ["source", "our", "chain-block"].iter().enumerate() {
            tx.send(s::update(name, i as u64 + 2)).await?;
        }
        tx.send(copybot_ingestion::ReplayInput::End(5)).await?;
        Ok::<_, anyhow::Error>(())
    });
    f::drain(&mut consumer, &db).await?;
    producer.await??;
    assert_eq!(
        db.sql.query_row(
            "SELECT count(*) FROM ordered_source_sell_intents",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        1
    );
    let mut inbox = AssociationInbox::open(&db.path, p::limits())?;
    let sig = m["sell"]["signature"].as_str().unwrap();
    let OrderedSellStage::Existing(i) =
        inbox.stage_ordered_source_sell_intent(sig, PROVIDER_ORDER_STRICT_V1)?
    else {
        panic!("future clock must not change strict order outcome")
    };
    assert_eq!(
        i.first.message_clock_check,
        copybot_storage_core::association_sell_preparation::MessageClockCheck::FutureVsAppDequeue
    );
    assert_eq!(
        inbox.revalidate_ordered_source_sell_intent(&i.intent_id)?,
        OrderedSellDecision::ValidatedNow
    );
    assert_eq!(db.financial_counts()?, before);
    std::fs::write(
        db.path.with_extension("ordered.json"),
        serde_json::to_vec_pretty(&i)?,
    )?;
    Ok(())
}
