//! Saved40 input graph in a genuinely new SQLite DB. Original consumed bytes never reset.
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{association_inbox::InboxLimits, ordered_sell_quote::*, SqliteStore};
use rusqlite::{params_from_iter, types::Value as SqlValue, Connection, OpenFlags};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

pub(super) const SELL: &str =
    "4exvDo6Wv4ADMKsMC2ZQf7nng3SH7epBQqEUuGckMPA5ndDJAyjWGdwXDLRwZoWFTXTKBUaSp5GePMZnCeVUTTnj";
pub(super) const BOT: &str = "BwVw8ncEpWU7TwMTgysvwjQ85eEhKAMVbd7WU1iTE9Mk";
pub(super) const H: u64 = 1_041_505;
const DB_HASH: &str = "0c9b6fffabca09c40d1acf10c969ab1fa8769da4f664b037b8f246bf098db2b0";
const MANIFEST_HASH: &str = "00b6f69f4a133f502556e1d5a26ca16c9ca9b1b911f5b0c6e70802623012747c";
const CONFIG_HASH: &str = "ad48cd79a5caa31d25ba2ea58970de904129fa42edae0f7ebcb2b29d9d15298d";
// Fixed canonical input graph only. Output claims/cursors/collecting are deliberately
// not imported into NEW DB. No DELETE, reset, replacement or reopening of old40.
const INPUT_TABLES: &[&str] = &[
    "copy_signals",
    "orders",
    "positions",
    "fills",
    "execution_order_sources",
    "execution_quote_canary_events",
    "execution_quote_canary_provider_samples",
    "execution_canary_build_plan_metadata",
    "execution_canary_receipt_proofs",
    "execution_canary_receipt_facts",
    "execution_receipt_native_observations",
    "observed_retention_boundary",
    "execution_canary_dispatch",
    "execution_canary_reconcile_attempts",
    "association_inbox_identities",
    "association_inbox_events",
    "association_sell_preparations",
    "association_sell_dependencies",
    "association_sell_bootstrap",
    "association_parent_blocks",
    "association_parent_hashes",
    "association_parent_dependencies",
    "association_parent_work",
    "source_sell_signature_claims",
    "ordered_source_sell_intents",
    "execution_tiny_experiment",
    "execution_tiny_native_policy",
    "execution_tiny_reservations",
    "execution_tiny_capital_evidence",
    "native_buy_fences",
    "native_buy_session_state",
    "native_buy_decisions",
    "native_buy_scan_cursor",
    "native_buy_technical_cohort",
    "native_buy_fence_epochs",
    "native_buy_cohort_decisions",
    "association_replay_cursor",
];

pub(super) struct Rpc {
    pub request: Value,
    pub response: Value,
    pub raw: Vec<u8>,
}
pub(super) struct Case {
    pub store: SqliteStore,
    pub sql: Connection,
    pub config: ExecutionConfig,
    pub limits: InboxLimits,
    pub rpc: Vec<Rpc>,
    pub late: Rpc,
    pub path: PathBuf,
    original: PathBuf,
    pub root: crate::app_tests::temporary_output_fixture::OutputRoot,
}
pub(super) fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub(super) fn start() -> DateTime<Utc> {
    "2026-10-14T19:27:40Z".parse().unwrap()
}
fn input(name: &str) -> Result<PathBuf> {
    Ok(PathBuf::from(
        std::env::var_os(name).with_context(|| format!("{name} required"))?,
    ))
}
fn rows(sql: &Connection, table: &str) -> Result<Vec<Vec<SqlValue>>> {
    let mut query = sql.prepare(&format!("SELECT * FROM \"{table}\""))?;
    let count = query.column_count();
    let result = query.query_map([], |r| (0..count).map(|i| r.get(i)).collect())?;
    Ok(result.collect::<rusqlite::Result<_>>()?)
}
fn fresh_graph(source: &Connection, target: &Connection) -> Result<()> {
    // Report every runtime-initialized column difference together, before importing
    // any financial operands. Never truncate a saved row to fit a smaller schema.
    let columns = |sql: &Connection, table: &str| -> Result<Vec<String>> {
        Ok(sql
            .prepare(&format!("PRAGMA table_info(\"{table}\")"))?
            .query_map([], |r| r.get(1))?
            .collect::<rusqlite::Result<_>>()?)
    };
    let mut differences = vec![];
    for table in INPUT_TABLES {
        let from = columns(source, table)?;
        let to = columns(target, table)?;
        if from != to {
            differences.push(format!("{table}: source={from:?}; target={to:?}"));
        }
    }
    ensure!(
        differences.is_empty(),
        "saved40 input schema differs: {}",
        differences.join("; ")
    );
    target
        .execute_batch("PRAGMA foreign_keys=ON; BEGIN IMMEDIATE; PRAGMA defer_foreign_keys=ON;")?;
    for table in INPUT_TABLES {
        let expected = rows(source, table)?;
        let existing = rows(target, table)?;
        for row in &expected {
            // Accepted migration singleton/automatic trigger rows may already be
            // present. Skip only exact row equality; every other collision errors.
            if existing.contains(row) {
                continue;
            }
            let args = std::iter::repeat_n("?", row.len())
                .collect::<Vec<_>>()
                .join(",");
            target.execute(
                &format!("INSERT INTO \"{table}\" VALUES({args})"),
                params_from_iter(row),
            )?;
        }
        let actual = rows(target, table)?;
        ensure!(
            actual.len() == expected.len() && expected.iter().all(|r| actual.contains(r)),
            "copy40 canonical input table differs: {table}"
        );
    }
    target.execute_batch("COMMIT")?;
    ensure!(
        target.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))? == "ok",
        "copy40 new input graph integrity"
    );
    ensure!(
        target
            .prepare("PRAGMA foreign_key_check")?
            .query([])?
            .next()?
            .is_none(),
        "copy40 new input graph foreign keys"
    );
    for table in [
        "fractional_sell_decisions",
        "ordered_sell_quote_results",
        "ordered_sell_quote_cursor",
    ] {
        ensure!(
            target.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                .get::<_, u64>(0))?
                == 0,
            "copy40 new input graph imported spent output"
        );
    }
    Ok(())
}
fn initialize_saved_metadata(source: &Connection, target: &SqliteStore) -> Result<()> {
    // Use the accepted writer's initializer and the exact saved BUY metadata,
    // rather than ad hoc ALTER or an invented row. Full row equality follows.
    let mut query = source.prepare("SELECT * FROM execution_canary_build_plan_metadata LIMIT 1")?;
    let names = query
        .column_names()
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let values = query.query_row([], |r| {
        (0..names.len())
            .map(|i| r.get::<_, SqlValue>(i))
            .collect::<rusqlite::Result<Vec<_>>>()
    })?;
    let mut object = serde_json::Map::new();
    for (name, value) in names.into_iter().zip(values) {
        let value = match value {
            SqlValue::Null => Value::Null,
            SqlValue::Integer(v) => v.into(),
            SqlValue::Real(v) => serde_json::json!(v),
            SqlValue::Text(v) => v.into(),
            SqlValue::Blob(_) => anyhow::bail!("unexpected saved40 metadata BLOB"),
        };
        object.insert(name, value);
    }
    target.record_execution_canary_build_plan_metadata(&serde_json::from_value(Value::Object(
        object,
    ))?)?;
    Ok(())
}
fn load_rpc(pin: &Value) -> Result<Rpc> {
    let raw = std::fs::read(pin["path"].as_str().context("saved40 response path")?)?;
    ensure!(digest(&raw) == pin["sha256"], "saved40 raw response hash");
    let meta_raw = std::fs::read(pin["meta_path"].as_str().context("saved40 meta path")?)?;
    ensure!(
        digest(&meta_raw) == pin["meta_sha256"],
        "saved40 raw request metadata hash"
    );
    let meta: Value = serde_json::from_slice(&meta_raw)?;
    ensure!(
        meta["request"] == pin["request"] && meta["response_sha256"] == pin["sha256"],
        "saved40 request response provenance"
    );
    let response: Value = serde_json::from_slice(&raw)?;
    ensure!(
        response["jsonrpc"] == "2.0" && response["id"] == meta["request"]["id"],
        "saved40 envelope"
    );
    Ok(Rpc {
        request: meta["request"].clone(),
        response,
        raw,
    })
}
impl Case {
    pub fn new(spent: bool) -> Result<Self> {
        let original = input("COPY40_READINESS_DB")?;
        let db = std::fs::read(&original)?;
        ensure!(digest(&db) == DB_HASH, "saved40 original consumed DB hash");
        let manifest_raw = std::fs::read(input("COPY40_READINESS_INPUTS")?)?;
        ensure!(
            digest(&manifest_raw) == MANIFEST_HASH,
            "saved40 inputs manifest hash"
        );
        let manifest: Value = serde_json::from_slice(&manifest_raw)?;
        let config_raw = std::fs::read(input("COPY40_READINESS_CONFIG")?)?;
        ensure!(
            digest(&config_raw) == CONFIG_HASH,
            "saved40 actual config hash"
        );
        let settings: toml::Value = toml::from_str(std::str::from_utf8(&config_raw)?)?;
        let mut config: ExecutionConfig = settings["execution"].clone().try_into()?;
        ensure!(
            !config.enabled && config.canary_wallet_pubkey == BOT,
            "saved40 execution scope"
        );
        let association = &settings["ingestion"]["yellowstone_association"];
        let limits = InboxLimits {
            count: association["inbox"]["count"].as_integer().unwrap() as usize,
            bytes: association["inbox"]["bytes"].as_integer().unwrap() as usize,
            busy_ms: association["sqlite_busy_ms"].as_integer().unwrap() as u64,
        };
        let root = crate::app_tests::temporary_output_fixture::OutputRoot::new("copy40-readiness")?;
        // Only the mocked local kill-switch pathname differs from the exact config.
        config.canary_kill_switch_path =
            root.path().join("MOCK_STOP").to_string_lossy().into_owned();
        let path = root.path().join(if spent {
            "spent-unchanged.db"
        } else {
            "NEW-input-graph.db"
        });
        ensure!(!path.exists(), "saved40 fixture must be new");
        let store = if spent {
            std::fs::write(&path, &db)?;
            SqliteStore::open(&path)?
        } else {
            let mut new = SqliteStore::open(&path)?;
            new.run_migrations(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations"))?;
            // The daemon's accepted table initializer appends quote_response_json
            // after migrations. Apply that same initializer to this NEW fixture.
            assert!(new
                .list_execution_quote_canary_entry_candidates(
                    "OFFLINE_FIXTURE_INITIALIZE_ONLY",
                    start(),
                    0
                )?
                .is_empty());
            new
        };
        let sql = Connection::open(&path)?;
        if !spent {
            let pinned = root.path().join("pinned-input.db");
            std::fs::write(&pinned, &db)?;
            let source = Connection::open_with_flags(
                format!("file:{}?mode=ro&immutable=1", pinned.display()),
                OpenFlags::SQLITE_OPEN_READ_ONLY
                    | OpenFlags::SQLITE_OPEN_URI
                    | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?;
            source.execute_batch("PRAGMA query_only=ON")?;
            initialize_saved_metadata(&source, &store)?;
            fresh_graph(&source, &sql)?;
        }
        let rpc = manifest["original_responses"]
            .as_array()
            .context("saved40 responses")?
            .iter()
            .map(load_rpc)
            .collect::<Result<Vec<_>>>()?;
        let late = load_rpc(&manifest["corrected_response"])?;
        let corroboration = load_rpc(&manifest["independent_historical_ata"])?;
        ensure!(
            corroboration.response["result"]["value"]["data"]["parsed"]["info"]["tokenAmount"]
                ["amount"]
                == "1041512",
            "saved40 independent historical pre amount"
        );
        ensure!(
            rpc[4].request == late.request,
            "saved40 empty and corrected request differs"
        );
        // No input data, receipt/admission dates, old claim owner or old decision is edited.
        ensure!(
            digest(&std::fs::read(&original)?) == DB_HASH,
            "saved40 source changed"
        );
        Ok(Self {
            store,
            sql,
            config,
            limits,
            rpc,
            late,
            path,
            original,
            root,
        })
    }
    pub fn claim(&self) -> Result<QuoteClaim> {
        let step = self.store.claim_strict_sell_quote_for_owned_preparation(
            self.limits,
            &self.config.quote_canary_base_url,
            start,
        )?;
        let QuoteClaimStep::Claimed(claim) = step else {
            anyhow::bail!("saved40 new input claim: {step:?}")
        };
        ensure!(
            claim.intent_id == format!("source-sell:{SELL}") && claim.binding.raw == H,
            "saved40 canonical lot"
        );
        Ok(claim)
    }
    pub fn original_unchanged(&self) -> Result<()> {
        ensure!(
            digest(&std::fs::read(&self.original)?) == DB_HASH,
            "saved40 original consumed bytes changed"
        );
        Ok(())
    }
    pub fn sell_dispatches(&self) -> Result<u64> {
        Ok(self
            .sql
            .query_row("SELECT count(*) FROM rpc_owned_sell_dispatches", [], |r| {
                r.get(0)
            })?)
    }
    pub fn unproven(&self) -> Result<()> {
        let state: String =
            self.sql
                .query_row("SELECT state FROM fractional_sell_decisions", [], |r| {
                    r.get(0)
                })?;
        ensure!(
            state == "collecting",
            "failed saved40 observation became proven"
        );
        ensure!(
            self.sell_dispatches()? == 0
                && self
                    .sql
                    .query_row("SELECT count(*) FROM rpc_owned_sell_handoffs", [], |r| r
                        .get::<_, u64>(0))?
                    == 0,
            "failed saved40 observation reached handoff/dispatch"
        );
        self.original_unchanged()
    }
}
