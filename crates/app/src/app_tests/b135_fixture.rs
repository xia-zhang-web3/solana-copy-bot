use super::{
    association_fixture as f, association_parent_fixture as p, association_sell_fixture as s,
};
use anyhow::Result;
use chrono::Utc;
use copybot_config::AppConfig;
use copybot_storage_core::SqliteStore;
use serde_json::{json, Value};
use std::path::PathBuf;
pub(super) struct Fixture {
    pub db: f::Db,
    pub meta: Value,
    pub root: super::temporary_output_fixture::OutputRoot,
}
pub(super) fn inputs() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/app_tests/fixtures/b135")
}
impl Fixture {
    pub async fn new() -> Result<Self> {
        Self::with_protected(false).await
    }
    pub async fn with_protected(protected: bool) -> Result<Self> {
        let root = super::temporary_output_fixture::OutputRoot::new("b135")?;
        let path = root.path().join("state.db");
        let mut store = SqliteStore::open(&path)?;
        store
            .run_migrations(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations"))?;
        let db = f::Db {
            sql: rusqlite::Connection::open(&path)?,
            store,
            path,
        };
        let meta: Value = serde_json::from_slice(&std::fs::read(inputs().join("chain.json"))?)?;
        s::seed(&db, &meta)?;
        let wallet = meta["our"]["signer"].as_str().unwrap();
        let order = db
            .store
            .load_execution_canary_open_position(meta["our"]["token_out"].as_str().unwrap())?
            .unwrap();
        let buy: String = db
            .sql
            .query_row("SELECT order_id FROM orders LIMIT 1", [], |r| r.get(0))?;
        if protected {
            let now = Utc::now();
            db.store.prepare_tiny_native_policy(
                "b135",
                wallet,
                100_000_000,
                50_000_001,
                151,
                now,
                || Ok(Utc::now()),
            )?;
        } else {
            db.store
                .activate_tiny_experiment("b135", wallet, Utc::now())?;
        }
        // Historical successful BUY is an explicit synthetic precondition. These
        // matching dispatch/reservation operands are never submitted by this test.
        db.sql.execute(
            "UPDATE execution_tiny_experiment SET buy_order_id=?1,token=?2,position_id=?3",
            rusqlite::params![buy, order.token, order.position_id],
        )?;
        db.sql.execute("INSERT INTO execution_canary_dispatch(order_id,signal_id,client_order_id,route,attempt,wallet,token,side,tx_signature,message_sha256,transaction_sha256,claimed_at,transport_note) SELECT order_id,signal_id,client_order_id,route,attempt,?1,?2,'buy',tx_signature,?3,?4,'2026-09-09T00:00:00Z','unknown' FROM orders WHERE order_id=?5",rusqlite::params![wallet,order.token,"a".repeat(64),"b".repeat(64),buy])?;
        db.sql.execute("INSERT INTO execution_tiny_reservations(order_id,experiment_id,tx_signature,wallet,side,message_sha256,transaction_sha256,buy_lamports,fee_bound,priority_fee,fee_slot,reserved_at) SELECT order_id,'b135',tx_signature,?1,'buy',?2,?3,10000000,100000,10000,'120','2026-09-09T00:00:00Z' FROM orders WHERE order_id=?4",rusqlite::params![wallet,"a".repeat(64),"b".repeat(64),buy])?;
        Ok(Self { db, meta, root })
    }
    pub fn config(&self, url: &str) -> Result<AppConfig> {
        let mut c = f::config(&self.meta);
        // The exact same wire is intentionally readable by e3 (unknown field is
        // ignored there); causal RED is the actual runner outcome, not compilation.
        c.execution = serde_json::from_value(json!({
            "enabled":false,"canary_tiny_submit_enabled":false,"canary_enabled":true,
            "canary_route":"jupiter_swap_instructions","canary_wallet_pubkey":self.meta["our"]["signer"],
            "execution_signer_pubkey":self.meta["our"]["signer"],
            "execution_signer_keypair_path":self.root.path().join("never-load-key"),
            "quote_canary_enabled":true,"quote_canary_base_url":url,"quote_canary_timeout_ms":4000,
            "priority_fee_canary_rpc_url":url,"priority_fee_canary_enabled":false,
            "quote_canary_pump_fun_parallel_enabled":false,"quote_canary_public_parallel_enabled":false,
            "swap_instructions_dry_run_enabled":true,"swap_transaction_dry_run_enabled":true,
            "canary_batch_limit":1,"pretrade_max_priority_fee_lamports":22000,
            "tiny_experiment":{"id":"b135","activate":false},
            "owned_sell_preparation":{"policy":"rpc_finalized_cross_slot_owned_sell_v1","rpc_url":url,"genesis_hash":"11111111111111111111111111111111","identity":"b135-loopback"}
        }))?;
        copybot_config::validate_association_delivery(&c)?;
        Ok(c)
    }
    pub async fn ingress(&self, c: &AppConfig) -> Result<()> {
        let (mut consumer, tx) = f::start(&self.db, c, "b135-actual").await?;
        let names = p::frames(&self.meta);
        let producer = tokio::spawn(async move {
            for (n, name) in names.iter().enumerate() {
                tx.send(copybot_ingestion::ReplayInput::Update {
                    offset_ns: n as u64 + 1,
                    payload: std::fs::read(inputs().join(format!("{name}.pb")))?,
                })
                .await?;
            }
            tx.send(copybot_ingestion::ReplayInput::End(9)).await?;
            Ok::<_, anyhow::Error>(())
        });
        f::drain(&mut consumer, &self.db).await?;
        producer.await??;
        // Reopen through the actual consumer/recovery boundary too.
        let (mut reopened, tx) = f::start(&self.db, c, "b135-recovery").await?;
        tx.send(copybot_ingestion::ReplayInput::End(1)).await?;
        f::drain(&mut reopened, &self.db).await?;
        Ok(())
    }
    pub fn handoffs(&self) -> Result<i64> {
        let exists: bool = self.db.sql.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='rpc_owned_sell_handoffs')",
            [],
            |r| r.get(0),
        )?;
        if !exists {
            return Ok(0);
        }
        Ok(self.db.sql.query_row(
            "SELECT count(*) FROM rpc_owned_sell_handoffs WHERE state='unsigned_prepared'",
            [],
            |r| r.get(0),
        )?)
    }
    pub fn runner(&self, c: &AppConfig) -> Result<crate::execution_canary::ExecutionCanaryRunner> {
        crate::execution_canary::ExecutionCanaryRunner::new(c.execution.clone())
            .for_ingestion(&c.ingestion, &self.db.path.to_string_lossy())
    }
    pub async fn drive(&self, r: &crate::execution_canary::ExecutionCanaryRunner) -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(4), async {
            loop {
                super::strict_quote_fixture::tick(r, &self.db).await?;
                if self.handoffs()? == 1 {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await?
    }
    pub fn rows(&self, table: &str) -> Result<Vec<String>> {
        let mut q = self
            .db
            .sql
            .prepare(&format!("SELECT * FROM {table} ORDER BY 1"))?;
        let n = q.column_count();
        let rows = q
            .query_map([], |r| {
                Ok(format!(
                    "{:?}",
                    (0..n)
                        .map(|i| r.get::<_, rusqlite::types::Value>(i))
                        .collect::<rusqlite::Result<Vec<_>>>()?
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }
}
