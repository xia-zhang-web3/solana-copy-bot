//! Pre-repair causal baseline: actual durable producer has no financial BUY handoff.
use super::{association_fixture as f, association_parent_fixture as p, b135_fixture};
use crate::execution_canary::ExecutionCanaryRunner;
use anyhow::Result;
use chrono::Utc;
use copybot_ingestion::ReplayInput;
use copybot_storage_core::SqliteStore;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT: AtomicUsize = AtomicUsize::new(0);

#[tokio::test]
async fn native_buy_baseline_actual_ingress_has_no_buy_handoff() -> Result<()> {
    let input = b135_fixture::inputs();
    let meta: serde_json::Value = serde_json::from_slice(&std::fs::read(input.join("chain.json"))?)?;
    let config = f::config(&meta);
    let path = format!(
        "file:native-buy-baseline-{}?mode=memory&cache=shared",
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(&std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations"))?;
    let db = f::Db {
        sql: rusqlite::Connection::open(&path)?,
        path: path.into(),
        store,
    };
    let (mut consumer, sender) = f::start(&db, &config, "native-buy-baseline").await?;
    let names = p::frames(&meta);
    let producer = tokio::spawn(async move {
        for (n, name) in names.iter().enumerate() {
            sender.send(ReplayInput::Update {
                offset_ns: n as u64 + 1,
                payload: std::fs::read(input.join(format!("{name}.pb")))?,
            }).await?;
        }
        sender.send(ReplayInput::End(9)).await?;
        Ok::<_, anyhow::Error>(())
    });
    f::drain(&mut consumer, &db).await?;
    producer.await??;
    assert!(db.identities()? >= 3, "actual producer must persist source, own and SELL admission");
    let runner = ExecutionCanaryRunner::new(config.execution.clone())
        .for_ingestion(&config.ingestion, &db.path.to_string_lossy())?;
    runner.process_tick(&db.store, Utc::now()).await?;
    let counts = db.financial_counts()?;
    assert_eq!(counts[1], 0, "no copy signal from native admission");
    assert_eq!(counts[2], 0, "strict branch never enters BUY reservation");
    assert_eq!(counts[3], 0, "no canonical BUY fill or H");
    Ok(())
}
