//! Private hash-bound actual39 DB/RPC fixture. Only task-owned temporary DB changes.
use anyhow::{ensure, Context, Result};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{association_inbox::InboxLimits, ordered_sell_quote::*, SqliteStore};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
pub(super) const SELL: &str =
    "YKHhWEutMsSpmCRNsBGvsU17ZLdJBYAk7ExSxUUzMTgVZi55cP7dcJx7c9p3zaHzF1oHJJuP4EKXxuHsctLCYmg";
pub(super) const BOT: &str = "BwVw8ncEpWU7TwMTgysvwjQ85eEhKAMVbd7WU1iTE9Mk";
pub(super) const H: u64 = 1065935;
pub(super) struct Case {
    pub store: SqliteStore,
    pub sql: rusqlite::Connection,
    pub config: ExecutionConfig,
    pub limits: InboxLimits,
    pub rpc: Vec<(Value, Value)>,
    pub root: super::temporary_output_fixture::OutputRoot,
}
pub(super) fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
impl Case {
    pub fn new() -> Result<Self> {
        let input = PathBuf::from(
            std::env::var_os("COPY39_SQLITE_FIXTURE").context("saved39 SQLite fixture required")?,
        );
        let rpc_root = PathBuf::from(
            std::env::var_os("COPY39_RPC_FIXTURE").context("saved39 RPC fixture required")?,
        );
        let receipt: Value =
            serde_json::from_slice(&std::fs::read(input.join("INPUT_BINDING.json"))?)?;
        let db = std::fs::read(input.join("actual-bot.db"))?;
        ensure!(
            digest(&db) == receipt["read_only_input_sha256"],
            "actual39_db_integrity"
        );
        let bytes = std::fs::read(input.join("execution.json"))?;
        ensure!(
            digest(&bytes) == receipt["execution_json_sha256"],
            "actual39_config_integrity"
        );
        let config: ExecutionConfig = serde_json::from_slice(&bytes)?;
        ensure!(
            !config.enabled && config.canary_wallet_pubkey == BOT,
            "actual39_config_scope"
        );
        let limits: Value = serde_json::from_slice(&std::fs::read(input.join("limits.json"))?)?;
        let limits = InboxLimits {
            count: limits["count"].as_u64().unwrap() as usize,
            bytes: limits["bytes"].as_u64().unwrap() as usize,
            busy_ms: limits["busy_ms"].as_u64().unwrap(),
        };
        let root = super::temporary_output_fixture::OutputRoot::new("saved39-offline")?;
        let path = root.path().join("replay.db");
        std::fs::write(&path, &db)?;
        let sql = rusqlite::Connection::open(&path)?;
        ensure!(
            sql.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))? == "ok",
            "actual39_db_invalid"
        );
        // Rewind only the offline clone's exhausted producer attempt. Original39 never rearms.
        sql.execute("DELETE FROM fractional_sell_decisions", [])?;
        sql.execute("DELETE FROM ordered_sell_quote_results", [])?;
        sql.execute("DELETE FROM ordered_sell_quote_cursor", [])?;
        ensure!(
            sql.query_row("SELECT count(*) FROM rpc_owned_sell_handoffs", [], |r| r
                .get::<_, u64>(0))?
                == 0,
            "actual39_existing_handoff"
        );
        let store = SqliteStore::open(&path)?;
        let mut rpc = vec![];
        let manifest_bytes = std::fs::read(rpc_root.join("TRACE_INPUTS.json"))?;
        ensure!(
            digest(&manifest_bytes)
                == "72b134ddb163c1a2594213f2feb9738965b5ee4ae7285e3489f88c7a4c3f197e",
            "actual39_rpc_manifest"
        );
        let manifest: Value = serde_json::from_slice(&manifest_bytes)?;
        for n in 18359..=18364 {
            let filename = format!("response-{n:06}.json");
            let bytes = std::fs::read(rpc_root.join(&filename))?;
            let entry = manifest["files"]
                .as_array()
                .unwrap()
                .iter()
                .find(|v| v["path"].as_str().unwrap().ends_with(&filename))
                .unwrap();
            ensure!(digest(&bytes) == entry["sha256"], "actual39_rpc_integrity");
            let meta_name = format!("response-{n:06}.meta.json");
            let meta_bytes = std::fs::read(rpc_root.join(&meta_name))?;
            let meta_entry = manifest["files"]
                .as_array()
                .unwrap()
                .iter()
                .find(|v| v["path"].as_str().unwrap().ends_with(&meta_name))
                .unwrap();
            ensure!(
                digest(&meta_bytes) == meta_entry["sha256"],
                "actual39_rpc_request_integrity"
            );
            let meta: Value = serde_json::from_slice(&meta_bytes)?;
            rpc.push((meta["request"].clone(), serde_json::from_slice(&bytes)?));
        }
        Ok(Self {
            store,
            sql,
            config,
            limits,
            rpc,
            root,
        })
    }
    pub fn claim(&self) -> Result<QuoteClaim> {
        let step = self.store.claim_strict_sell_quote_for_owned_preparation(
            self.limits,
            &self.config.quote_canary_base_url,
            chrono::Utc::now,
        )?;
        let QuoteClaimStep::Claimed(claim) = step else {
            anyhow::bail!("actual39_offline_quote_claim: {step:?}")
        };
        ensure!(
            claim.binding.raw == H && claim.intent_id == format!("source-sell:{SELL}"),
            "actual39_lot_identity"
        );
        Ok(claim)
    }
    pub async fn bind(&mut self, claim: QuoteClaim) -> Result<QuoteClaim> {
        let mut rpc = self.rpc.clone().into_iter();
        let out = crate::execution_owned_sell_rpc::fractional::bind(
            &mut self.store,
            &self.config,
            claim,
            self.limits,
            move |request| {
                let (expected, response) = rpc.next().expect("unexpected extra read");
                assert_eq!(
                    request, expected,
                    "collector must use exact saved request identity"
                );
                std::future::ready(Ok(response))
            },
        )
        .await?;
        Ok(out)
    }
}
