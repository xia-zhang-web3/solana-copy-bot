//! Legacy execution tests need observation-only preparation. Decode the same
//! retained frames, but explicitly select the unchanged storage observation API.
use super::{association_fixture as f, association_parent_fixture as p};
use anyhow::Result;
use copybot_core_types::association_delivery::{CandidateGeneration, DeliveryEvent};
use copybot_ingestion::{IngestionService, ReplayInput};
use copybot_storage_core::association_inbox::AssociationInbox;
use serde_json::Value;

pub async fn stage(db: &f::Db, m: &Value, session: &str, root: std::path::PathBuf) -> Result<()> {
    let config = f::config(m);
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let mut service = IngestionService::with_replay(&config, rx, session.into())?;
    let mut receiver = service.take_delivery(session.into())?.unwrap();
    let frames = p::frames(m);
    let producer = tokio::spawn(async move {
        for (n, name) in frames.iter().enumerate() {
            tx.send(ReplayInput::Update {
                offset_ns: n as u64 + 1,
                payload: std::fs::read(root.join(format!("{name}.pb")))?,
            })
            .await?;
        }
        tx.send(ReplayInput::End(frames.len() as u64 + 1)).await?;
        Ok::<_, anyhow::Error>(())
    });
    let mut inbox = AssociationInbox::open(&db.path, p::limits())?;
    while let Some(envelope) = receiver.next().await? {
        let candidate = match &envelope.delivery.event {
            DeliveryEvent::Admission(a) => db.store.association_candidate(&a.facts),
            _ => CandidateGeneration::Unknown,
        };
        inbox.persist(&envelope.delivery, &candidate)?;
    }
    producer.await??;
    while inbox.has_sell_preparation_work()? {
        inbox.recover_sell_preparation()?;
    }
    let intents: i64 = db.sql.query_row(
        "SELECT count(*) FROM ordered_source_sell_intents",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(intents, 0, "legacy fixture must stay observation-only");
    Ok(())
}
