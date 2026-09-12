use crate::association_consumer::AssociationConsumer;
use anyhow::Result;
use copybot_config::{AppConfig, AssociationDeliveryConfig, DeliveryBudget};
use copybot_ingestion::{IngestionService, ReplayInput};
use copybot_storage_core::SqliteStore;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};
static NEXT: AtomicUsize = AtomicUsize::new(0);
pub(crate) fn fixtures() -> PathBuf {
    std::env::var("B89_FIXTURE_DIR")
        .expect("explicit validated B89 fixtures")
        .into()
}
pub(crate) fn metadata(name: &str) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(fixtures().join(name)).unwrap()).unwrap()
}
pub(crate) fn config(meta: &serde_json::Value) -> AppConfig {
    let mut c = AppConfig::default();
    c.ingestion.source = "yellowstone_grpc".into();
    c.ingestion.yellowstone_grpc_url = "http://127.0.0.1:1".into();
    c.ingestion.yellowstone_x_token = "fixture".into();
    c.execution.enabled = false;
    c.execution.canary_tiny_submit_enabled = false;
    c.ingestion.yellowstone_delivery_mode = "durable_association_v1".into();
    let strings = |name| {
        meta[name]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_owned())
            .collect()
    };
    c.ingestion.yellowstone_program_ids = strings("programs");
    c.ingestion.raydium_program_ids = strings("raydium");
    c.ingestion.pumpswap_program_ids = strings("pumpswap");
    let b = |count, bytes| DeliveryBudget { count, bytes };
    c.ingestion.yellowstone_association = Some(AssociationDeliveryConfig {
        pending: b(1024, 16 << 20),
        blocks: b(32, 64 << 20),
        history: b(2048, 32 << 20),
        outputs: b(17, 4 << 20),
        queue: b(4, 8 << 20),
        inbox: b(20000, 128 << 20),
        input_bytes: 8 << 20,
        metadata_bytes: 32 << 20,
        pending_ttl_ms: 60_000,
        block_ttl_ms: 60_000,
        history_ttl_ms: 120_000,
        tick_ms: 1000,
        sqlite_busy_ms: 5000,
    });
    c
}
pub(crate) struct Db {
    pub path: PathBuf,
    pub store: SqliteStore,
    pub sql: rusqlite::Connection,
}
impl Db {
    pub fn new(name: &str) -> Result<Self> {
        let root = PathBuf::from(std::env::var("B89_DB_DIR")?);
        std::fs::create_dir_all(&root)?;
        let path = root.join(format!(
            "{name}-{}-{}.sqlite",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        assert!(!path.exists());
        let mut store = SqliteStore::open(&path)?;
        store
            .run_migrations(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations"))?;
        let sql = rusqlite::Connection::open(&path)?;
        Ok(Self { path, store, sql })
    }
    pub fn position(&self, id: &str, token: &str) -> Result<()> {
        self.sql.execute("INSERT INTO positions(position_id,token,qty,cost_sol,opened_ts,state,accounting_bucket) VALUES(?1,?2,1,1,'2026-09-09T00:00:00.000000001+00:00','open','execution_canary')",rusqlite::params![id,token])?;
        Ok(())
    }
    pub fn financial_counts(&self) -> Result<Vec<i64>> {
        [
            "observed_swaps",
            "copy_signals",
            "orders",
            "fills",
            "shadow_lots",
            "execution_source_sell_intents",
            "execution_source_sell_promotions",
            "execution_canary_receipt_proofs",
        ]
        .iter()
        .map(|table| {
            Ok(self
                .sql
                .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))?)
        })
        .collect()
    }
    pub fn identities(&self) -> Result<usize> {
        Ok(self.sql.query_row(
            "SELECT count(*) FROM association_inbox_identities",
            [],
            |r| r.get(0),
        )?)
    }
}
pub(crate) fn update(name: &str, ns: u64) -> ReplayInput {
    ReplayInput::Update {
        offset_ns: ns,
        payload: std::fs::read(fixtures().join(format!("{name}.pb"))).unwrap(),
    }
}
pub(crate) async fn start(
    db: &Db,
    c: &AppConfig,
    session: &str,
) -> Result<(AssociationConsumer, tokio::sync::mpsc::Sender<ReplayInput>)> {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let mut service = IngestionService::with_replay(c, rx, session.into())?;
    let consumer =
        AssociationConsumer::start(&mut service, &c.ingestion, &db.path.to_string_lossy())
            .await?
            .unwrap();
    Ok((consumer, tx))
}
pub(crate) async fn drain(c: &mut AssociationConsumer, db: &Db) -> Result<()> {
    loop {
        match c.poll(&db.store).await {
            Ok(()) => {}
            Err(e) if e.to_string() == "association delivery stopped" => break,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
pub(crate) fn row(db: &Db) -> Result<(String, String, Option<String>, bool)> {
    Ok(db.sql.query_row(
        "SELECT admission,candidate,terminal,conflict FROM association_inbox_identities LIMIT 1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )?)
}
