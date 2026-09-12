//! Hash-bound replay through actual ingestion, with observation-only retained DB setup.
use super::{
    association_fixture as f, association_parent_fixture as p, association_sell_fixture as s,
};
use crate::association_consumer::AssociationConsumer;
use anyhow::Result;
use copybot_core_types::association_delivery::*;
use copybot_storage_core::{association_inbox::AssociationInbox, ordered_source_sell::*};
use std::time::Duration;

pub fn count(db: &f::Db, table: &str) -> Result<i64> {
    Ok(db
        .sql
        .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?)
}
pub fn saved(db: &f::Db, signature: &str) -> Result<Option<OrderedSourceSellIntent>> {
    AssociationInbox::open(&db.path, p::limits())?
        .load_ordered_source_sell_intent_history(&format!("source-sell:{signature}"))
}
pub fn event_rows(db: &f::Db) -> Result<Vec<(Delivery, CandidateGeneration)>> {
    let mut q = db
        .sql
        .prepare("SELECT delivery FROM association_inbox_events ORDER BY session,sequence")?;
    let rows = q
        .query_map([], |r| r.get::<_, String>(0))?
        .map(|r| {
            let d: Delivery = serde_json::from_str(&r?)?;
            let candidate = match &d.event {
                DeliveryEvent::Admission(a) => db.store.association_candidate(&a.facts),
                _ => CandidateGeneration::Unknown,
            };
            Ok((d, candidate))
        })
        .collect();
    rows
}
pub async fn observed(label: &str) -> Result<(f::Db, serde_json::Value)> {
    let m = s::meta()?;
    let template = f::Db::new(&format!("{label}-capture"))?;
    s::seed(&template, &m)?;
    let (mut c, tx) = f::start(&template, &f::config(&m), "retained-input").await?;
    let producer = tokio::spawn(async move {
        for (n, name) in ["sell", "source", "our", "chain-block"].iter().enumerate() {
            tx.send(s::update(name, n as u64 + 1)).await?;
        }
        tx.send(copybot_ingestion::ReplayInput::End(5)).await?;
        Ok::<_, anyhow::Error>(())
    });
    f::drain(&mut c, &template).await?;
    producer.await??;
    let db = f::Db::new(label)?;
    s::seed(&db, &m)?;
    let mut observation = AssociationInbox::open(&db.path, p::limits())?;
    for (d, candidate) in event_rows(&template)? {
        observation.persist_at(
            &d,
            &candidate,
            chrono::DateTime::from_timestamp(10, 0).unwrap(),
        )?;
    }
    while observation.has_sell_preparation_work()? {
        observation.recover_sell_preparation()?;
    }
    assert_eq!(count(&db, "ordered_source_sell_intents")?, 0);
    Ok((db, m))
}
pub async fn until(
    c: &mut AssociationConsumer,
    db: &f::Db,
    condition: impl Fn() -> Result<bool>,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        for _ in 0..1000 {
            if condition()? {
                return Ok(());
            }
            c.poll(&db.store).await?;
        }
        anyhow::bail!("bounded consumer failed to drain")
    })
    .await?
}
pub fn idle(db: &f::Db) -> Result<bool> {
    Ok(db.sql.query_row("SELECT NOT EXISTS(SELECT 1 FROM association_sell_work) AND NOT EXISTS(SELECT 1 FROM association_parent_work WHERE pending=1) AND NOT EXISTS(SELECT 1 FROM association_sell_bootstrap WHERE complete=0)",[],|r|r.get(0))?)
}
