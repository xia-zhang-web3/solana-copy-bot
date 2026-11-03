//! Pinned stopped live operands, copied privately; clocks below are historical.
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_storage_core::{
    association_inbox::InboxLimits,
    ordered_sell_quote::{QuoteClaimStep, QuoteObservation},
    rpc_owned_sell_handoff::{dispatch::Prepared, Handoff},
    SqliteStore,
};
use rusqlite::{types::Value, Connection};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

pub const PIN: &str = "6d3dd4d010619643d04dbe4b2f2728f4b86e5dd41664f6ef8ce3e9db79a1069b";
pub fn limits() -> InboxLimits {
    InboxLimits {
        count: 4096,
        bytes: 16 << 20,
        busy_ms: 2000,
    }
}
pub struct Fixture {
    _dir: tempfile::TempDir,
    pub path: PathBuf,
    pub store: SqliteStore,
    pub old: Prepared,
}
pub fn open() -> Result<Fixture> {
    let input = std::env::var("COPYBOT_EXPIRED_SELL_DB")?;
    let bytes = std::fs::read(&input)?;
    ensure!(
        format!("{:x}", Sha256::digest(&bytes)) == PIN,
        "stopped snapshot pin"
    );
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("private.sqlite");
    std::fs::write(&path, bytes)?;
    // Normal sessions read their own finalized SOURCE witnesses. Project only
    // that self-DB path out of the original Linux namespace, never the owner DB.
    {
        let c = Connection::open(&path)?;
        let triggers = c.prepare("SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name IN ('real_session_sessions','real_session_cycles')")?
            .query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
        c.execute_batch("BEGIN")?;
        for (name, _) in &triggers {
            c.execute_batch(&format!("DROP TRIGGER {name}"))?;
        }
        c.execute("UPDATE real_session_sessions SET identity_json=replace(identity_json,?1,?2),scope_json=replace(scope_json,?1,?2)",
            rusqlite::params!["/opt/copybot/state/live_runtime.db",path.to_str().unwrap()])?;
        c.execute(
            "UPDATE real_session_cycles SET scope_json=replace(scope_json,?1,?2)",
            rusqlite::params!["/opt/copybot/state/live_runtime.db", path.to_str().unwrap()],
        )?;
        for (_, ddl) in &triggers {
            c.execute_batch(ddl)?;
        }
        c.execute_batch("COMMIT")?;
    }
    let mut store = SqliteStore::open(&path)?;
    ensure!(
        store.run_migrations(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations"))? == 2,
        "only 0099 and 0100 upgrades expected"
    );
    let c = Connection::open(&path)?;
    let handoff = c.query_row("SELECT intent_id,order_id,owner,snapshot,config_sha256,authority,quote,experiment_id,wallet,deadline FROM rpc_owned_sell_handoffs", [], |r| {
        Ok(serde_json::json!({"intent_id":r.get::<_,String>(0)?,"order_id":r.get::<_,String>(1)?,
            "owner":r.get::<_,String>(2)?,"snapshot":serde_json::from_str::<serde_json::Value>(&r.get::<_,String>(3)?).unwrap(),
            "config_sha256":r.get::<_,String>(4)?,"authority":r.get::<_,String>(5)?,
            "quote":serde_json::from_str::<serde_json::Value>(&r.get::<_,String>(6)?).unwrap(),
            "experiment_id":r.get::<_,String>(7)?,"wallet":r.get::<_,String>(8)?,"deadline":r.get::<_,String>(9)?}))
    })?;
    let mut h: Handoff = serde_json::from_value(handoff)?;
    // Runtime-only facts are deliberately not in durable JSON. Re-read them
    // through the public graph API before exercising the actual dispatch claim.
    h.snapshot = store.owned_sell_snapshot(&h.snapshot.quote, limits())?;
    let (payload,message_sha256,total_fee,priority_fee) = c.query_row(
        "SELECT unsigned_payload,message_sha256,total_fee,priority_fee FROM rpc_owned_sell_handoffs", [],
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
    let old = Prepared {
        handoff: h,
        experiment: store.owned_sell_experiment_snapshot()?,
        limits: (limits().count, limits().bytes, limits().busy_ms),
        payload,
        message_sha256,
        total_fee,
        priority_fee,
    };
    Ok(Fixture {
        _dir: dir,
        path,
        store,
        old,
    })
}
impl Fixture {
    pub fn after(&self) -> DateTime<Utc> {
        self.old.handoff.deadline + Duration::milliseconds(250)
    }
    pub fn claim(&self, now: DateTime<Utc>) -> Result<QuoteClaimStep> {
        self.store.claim_strict_sell_quote_for_owned_preparation(
            limits(),
            &self.old.handoff.snapshot.quote.endpoint,
            || now,
        )
    }
    pub fn fresh(
        &self,
    ) -> Result<(
        copybot_storage_core::rpc_owned_sell_snapshot::OwnedSellSnapshot,
        QuoteObservation,
    )> {
        let now = self.after();
        let QuoteClaimStep::Claimed(claim) = self.claim(now)? else {
            anyhow::bail!("expiry quote not scheduled")
        };
        let mut q = self.old.handoff.quote.clone();
        q.binding = Some(claim.binding.clone());
        q.http_started = Some(now);
        q.http_response = Some(now + Duration::milliseconds(100));
        q.quote_response_available_ts = q.http_response;
        q.http_ended = now + Duration::milliseconds(150);
        let q = self
            .store
            .complete_strict_sell_quote(&claim, limits(), q, || {
                now + Duration::milliseconds(200)
            })?;
        let snapshot = self.store.owned_sell_snapshot(&claim.binding, limits())?;
        Ok((snapshot, q))
    }
    pub fn recover(&self) -> Result<Handoff> {
        let (snapshot, q) = self.fresh()?;
        let h = &self.old.handoff;
        self.store.recover_owned_sell_handoff(
            &snapshot,
            &q,
            limits(),
            &h.config_sha256,
            &h.authority,
            &h.experiment_id,
            &h.wallet,
            || self.after() + Duration::milliseconds(200),
        )
    }
    pub fn scalar(&self, sql: &str) -> Result<i64> {
        Ok(Connection::open(&self.path)?.query_row(sql, [], |r| r.get(0))?)
    }
    /// Corrupted/partial negative evidence on a disposable DB only.
    pub fn related_row(&self, table: &str) -> Result<()> {
        let c = Connection::open(&self.path)?;
        c.execute_batch("PRAGMA foreign_keys=OFF")?;
        if table == "execution_order_sources" {
            c.execute("INSERT INTO execution_order_sources(identity_id,owned_sell_intent_id) VALUES(?1,?1)",[&self.old.handoff.intent_id])?;
            return Ok(());
        }
        let mut s = c.prepare(&format!("SELECT * FROM {table} LIMIT 1"))?;
        let names = s
            .column_names()
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>();
        let mut row = s.query_row([], |r| {
            (0..names.len())
                .map(|i| r.get::<_, Value>(i))
                .collect::<rusqlite::Result<Vec<_>>>()
        })?;
        for (name, value) in names.iter().zip(row.iter_mut()) {
            if name == "order_id" {
                *value = Value::Text(self.old.order_id());
            }
            if name == "signal_id" {
                *value = Value::Text(self.old.handoff.intent_id.clone());
            }
            if name == "client_order_id" {
                *value = Value::Text(self.old.handoff.owner.clone());
            }
            if name == "fill_id" {
                *value = Value::Text("negative-stray-fill".into());
            }
            if name == "id" {
                *value = Value::Null;
            }
            if name == "tx_signature" {
                *value = Value::Text("negative-stray-signature".into());
            }
        }
        let placeholders = vec!["?"; names.len()].join(",");
        c.execute(
            &format!(
                "INSERT INTO {table}({}) VALUES({placeholders})",
                names.join(",")
            ),
            rusqlite::params_from_iter(row),
        )
        .with_context(|| format!("negative {table}"))?;
        Ok(())
    }
}
