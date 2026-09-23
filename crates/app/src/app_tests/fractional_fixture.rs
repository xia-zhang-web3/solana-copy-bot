//! Synthetic saved frames + real receipt writers. All SQLite connections are RAM URIs.
use super::{association_fixture as f, association_sell_fixture as s};
use anyhow::Result;
use chrono::Utc;
use copybot_storage_core::{ordered_sell_quote::*, SqliteStore};
use serde_json::Value;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};
static NEXT: AtomicUsize = AtomicUsize::new(0);
pub(super) struct Fixture {
    pub db: f::Db,
    pub meta: Value,
    pub root: crate::app_tests::temporary_output_fixture::OutputRoot,
}
pub(super) fn inputs() -> PathBuf {
    super::b135_fixture::inputs()
}
pub(super) async fn native_binding() -> Result<Value> {
    let f = Fixture::new().await?;
    let p = super::association_parent_fixture::read(&f.db, &f.meta)?;
    Ok(serde_json::to_value(&p.current)?)
}
#[tokio::test]
async fn fractional_export_native_bindings() -> Result<()> {
    let f = Fixture::new().await?;
    let p = super::association_parent_fixture::read(&f.db, &f.meta)?;
    let path = f.root.path().join("native.json");
    std::fs::write(&path, serde_json::to_vec(&p.current)?)?;
    let saved: Value = serde_json::from_slice(&std::fs::read(path)?)?;
    assert_eq!(saved, serde_json::to_value(p.current)?);
    Ok(())
}
impl Fixture {
    pub async fn new() -> Result<Self> {
        Self::with_prefix(false).await
    }
    pub async fn with_prefix(_prefix: bool) -> Result<Self> {
        let root = crate::app_tests::temporary_output_fixture::OutputRoot::new("fractional")?;
        let path = PathBuf::from(format!(
            "file:fractional-{}?mode=memory&cache=shared",
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut store = SqliteStore::open(&path)?;
        store
            .run_migrations(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations"))?;
        let db = f::Db {
            sql: rusqlite::Connection::open(&path)?,
            path,
            store,
        };
        let meta: Value = serde_json::from_slice(&std::fs::read(inputs().join("chain.json"))?)?;
        s::seed(&db, &meta)?;
        let config = f::config(&meta);
        let (mut consumer, sender) = f::start(&db, &config, "fractional-native").await?;
        let frames = super::association_parent_fixture::frames(&meta);
        let producer = tokio::spawn(async move {
            for (i, name) in frames.iter().enumerate() {
                // The prefix block adds a synthetic token transfer before the SELL.
                let input = if _prefix && matches!(name.as_str(), "sell" | "block-150") {
                    super::fractional_synthetic_fixture::prefix_inputs().join(format!("{name}.pb"))
                } else {
                    inputs().join(format!("{name}.pb"))
                };
                sender
                    .send(copybot_ingestion::ReplayInput::Update {
                        offset_ns: i as u64 + 1,
                        payload: std::fs::read(input)?,
                    })
                    .await?;
            }
            sender.send(copybot_ingestion::ReplayInput::End(9)).await?;
            Ok::<_, anyhow::Error>(())
        });
        f::drain(&mut consumer, &db).await?;
        producer.await??;
        let facts = super::b93_fixture::receipt(&db, &meta, "fractional-prior", 6000)?;
        db.store
            .record_execution_canary_receipt_facts(&facts, Utc::now())?;
        db.store
            .apply_execution_canary_sell_settlement(&facts, Utc::now())?;
        assert_eq!(
            db.store
                .load_execution_canary_open_position(meta["our"]["token_out"].as_str().unwrap())?
                .unwrap()
                .qty_exact
                .unwrap()
                .raw(),
            1000
        );
        Ok(Self { db, meta, root })
    }
    pub fn from_parts(db: f::Db, meta: Value) -> Result<Self> {
        Ok(Self {
            db,
            meta,
            root: crate::app_tests::temporary_output_fixture::OutputRoot::new("fractional-native")?,
        })
    }
    pub fn claim(&self) -> Result<QuoteClaim> {
        let QuoteClaimStep::Claimed(claim) = self
            .db
            .store
            .claim_strict_sell_quote_for_owned_preparation(
                super::association_parent_fixture::limits(),
                "http://127.0.0.1:1/",
                Utc::now,
            )?
        else {
            anyhow::bail!("fractional_fixture_claim_refused")
        };
        Ok(claim)
    }
}
