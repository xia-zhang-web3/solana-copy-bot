//! Synthetic protobuf inputs only; receipt seeding is a test precondition.
use super::{association_fixture as f, association_sell_fixture as s};
use anyhow::Result;
use copybot_ingestion::ReplayInput;
use copybot_storage_core::{association_inbox::*, association_sell_preparation::*};
use serde_json::Value;
use std::path::PathBuf;
pub fn root(case: &str) -> PathBuf {
    PathBuf::from(std::env::var("B91_FIXTURE_DIR").unwrap()).join(case)
}
pub fn meta(case: &str) -> Result<Value> {
    Ok(serde_json::from_slice(&std::fs::read(
        root(case).join("chain.json"),
    )?)?)
}
pub fn frames(m: &Value) -> Vec<String> {
    m["frames"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().into())
        .collect()
}
pub fn limits() -> InboxLimits {
    InboxLimits {
        count: 20000,
        bytes: 128 << 20,
        busy_ms: 100,
    }
}
pub fn read(db: &f::Db, m: &Value) -> Result<ValidatedPreparation> {
    Ok(AssociationInbox::open(&db.path, limits())?
        .sell_preparation(m["sell"]["signature"].as_str().unwrap())?
        .unwrap())
}
pub async fn stage(
    db: &f::Db,
    case: &str,
    m: &Value,
    names: Vec<String>,
    session: &str,
    reset: bool,
) -> Result<()> {
    let c = f::config(m);
    let before = s::snapshot(db)?;
    let (mut consumer, tx) = f::start(db, &c, session).await?;
    let path = root(case);
    let producer = tokio::spawn(async move {
        let mut at = 1;
        if reset {
            at = 121_000_000_000;
            tx.send(ReplayInput::Tick(at)).await?;
            tx.send(ReplayInput::Reset(at + 1)).await?;
            at += 2;
        }
        for name in names {
            tx.send(ReplayInput::Update {
                offset_ns: at,
                payload: std::fs::read(path.join(format!("{name}.pb")))?,
            })
            .await?;
            at += 1;
        }
        tx.send(ReplayInput::End(at)).await?;
        Ok::<(), anyhow::Error>(())
    });
    f::drain(&mut consumer, db).await?;
    producer.await??;
    assert_eq!(s::snapshot(db)?, before);
    Ok(())
}
pub fn save(db: &f::Db, m: &Value, label: &str) -> Result<()> {
    let p = read(db, m)?;
    std::fs::write(
        db.path.with_extension(format!("{label}.json")),
        serde_json::to_vec_pretty(
            &serde_json::json!({"first":p.first,"initial":p.historical_initial,"current":p.current,"financial_delta":0}),
        )?,
    )?;
    Ok(())
}
