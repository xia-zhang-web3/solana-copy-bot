//! Immutable actual37 corpus on owned TEMP SQLite, not fresh events/quotes or a daemon replay.
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_storage_core::{
    association_inbox::InboxLimits,
    ordered_sell_quote::{
        fractional::inventory::{Evidence, Page, TOKEN, TOKEN22},
        QuoteBinding, QuoteClaim, QuoteClaimStep,
    },
    SqliteStore,
};
use rusqlite::Connection;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{fs, path::PathBuf};

pub fn limits() -> InboxLimits {
    InboxLimits {
        count: 4096,
        bytes: 16 << 20,
        busy_ms: 100,
    }
}
pub fn original_clock() -> DateTime<Utc> {
    // Explicit mocked original collector clock. No timestamps in source events are altered.
    "2026-10-13T13:54:44.688104+00:00".parse().unwrap()
}
pub struct Saved {
    pub _temp: tempfile::TempDir,
    pub path: PathBuf,
    pub store: SqliteStore,
    pub evidence: Evidence,
    pub original: QuoteClaim,
    pub producer: String,
    pub snapshot_wire: String,
}
impl Saved {
    pub fn load() -> Result<Self> {
        let root = PathBuf::from(std::env::var("COPYBOT_SAVED37_POSTFLIGHT")?);
        let http = PathBuf::from(std::env::var("COPYBOT_SAVED37_HTTP")?);
        let source = root.join("raw-bot-state/live_runtime.db");
        let source_bytes = fs::read(&source)?;
        assert_eq!(
            format!("{:x}", Sha256::digest(&source_bytes)),
            "a52511bcd145f4bc88616f4a4882198fe52f5cce439f859a2fcbc4effe97113b"
        );
        assert_eq!(
            fs::metadata(root.join("raw-bot-state/live_runtime.db-wal"))?.len(),
            0
        );
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("saved37.sqlite");
        fs::write(&path, source_bytes)?;
        let sql = Connection::open(&path)?;
        let (intent_id, owner, lease, wire): (String, String, String, String) = sql.query_row(
            "SELECT intent_id,owner,lease_until,binding FROM ordered_sell_quote_results",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )?;
        let binding: QuoteBinding = serde_json::from_str(&wire)?;
        let producer = sql.query_row(
            "SELECT producer_identity FROM fractional_sell_decisions",
            [],
            |r| r.get(0),
        )?;
        let snapshot_wire = binding.snapshot_version.clone();
        let read = |i: &str| -> Result<Value> {
            let value: Value =
                serde_json::from_slice(&fs::read(http.join(format!("response-{i}.json")))?)?;
            Ok(value["result"].clone())
        };
        let evidence = Evidence {
            version: 1,
            slot: 456_465_334,
            block: read("017272")?,
            parent: read("017273")?,
            pages: vec![
                Page {
                    program: TOKEN22.into(),
                    cursor: None,
                    response: read("017274")?,
                },
                Page {
                    program: TOKEN.into(),
                    cursor: None,
                    response: read("017275")?,
                },
            ],
            execution_accounts: read("017276")?,
        };
        let store = SqliteStore::open(&path)?;
        Ok(Self {
            _temp: temp,
            path,
            store,
            evidence,
            producer,
            snapshot_wire,
            original: QuoteClaim {
                intent_id,
                attempt: 1,
                owner,
                binding,
                lease_until: lease.parse()?,
            },
        })
    }
    pub fn sql(&self) -> Result<Connection> {
        Ok(Connection::open(&self.path)?)
    }
    pub fn fresh_claim(&self) -> Result<QuoteClaim> {
        // Only fresh test-owned quote state is modeled. All native/parent/financial graph remains actual.
        // Original consumed37 rows are never upgraded/reset; their refusal is tested separately.
        self.sql()?.execute_batch(
            "DELETE FROM fractional_sell_decisions; DELETE FROM ordered_sell_quote_results;",
        )?;
        let step = self.store.claim_strict_sell_quote(
            limits(),
            &self.original.binding.endpoint,
            original_clock,
        )?;
        let QuoteClaimStep::Claimed(claim) = step else {
            anyhow::bail!("fresh TEMP quote claim not produced: {step:?}")
        };
        self.store
            .begin_fractional_sell(&claim, limits(), original_clock(), &self.producer)?;
        Ok(claim)
    }
    pub fn state(&self) -> Result<Vec<String>> {
        let c = self.sql()?;
        let mut result = vec![];
        for table in ["ordered_sell_quote_results", "fractional_sell_decisions"] {
            let mut stmt = c.prepare(&format!("SELECT * FROM {table}"))?;
            let cols = stmt.column_count();
            for row in stmt.query_map([], |r| {
                Ok((0..cols)
                    .map(|i| format!("{:?}", r.get_ref(i).unwrap()))
                    .collect::<Vec<_>>()
                    .join("|"))
            })? {
                result.push(format!("{table}:{}", row?));
            }
        }
        Ok(result)
    }
    pub fn financial(&self) -> Result<Vec<String>> {
        let c = self.sql()?;
        let mut out = vec![];
        for table in [
            "orders",
            "fills",
            "positions",
            "execution_canary_dispatch",
            "execution_canary_receipt_proofs",
            "execution_canary_receipt_facts",
            "execution_tiny_experiment",
            "execution_tiny_reservations",
        ] {
            let mut stmt = c.prepare(&format!("SELECT * FROM {table}"))?;
            let cols = stmt.column_count();
            for row in stmt.query_map([], |r| {
                Ok((0..cols)
                    .map(|i| format!("{:?}", r.get_ref(i).unwrap()))
                    .collect::<Vec<_>>()
                    .join("|"))
            })? {
                out.push(format!("{table}:{}", row?));
            }
        }
        Ok(out)
    }
    pub fn decision_state(&self) -> Result<(String, Option<String>, Option<String>)> {
        Ok(self.sql()?.query_row(
            "SELECT state,decision,evidence FROM fractional_sell_decisions",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?)
    }
    pub fn expected_digest(&self) -> String {
        format!(
            "strict_quote_snapshot_v2:sha256:{:x}",
            Sha256::digest(self.snapshot_wire.as_bytes())
        )
    }
    pub fn assert_bound(&self, out: &QuoteClaim) -> Result<usize> {
        let wire: String =
            self.sql()?
                .query_row("SELECT binding FROM ordered_sell_quote_results", [], |r| {
                    r.get(0)
                })?;
        assert_eq!(serde_json::from_str::<QuoteBinding>(&wire)?, out.binding);
        assert!(wire.len() <= 131_072);
        let decision = out
            .binding
            .fractional
            .as_ref()
            .context("fractional proof absent")?;
        assert_eq!(decision.generation, out.binding.snapshot_version);
        assert_eq!(decision.inventory.numerator, 1_067_375);
        assert_eq!(decision.inventory.denominator, "1067375");
        assert_eq!(decision.inventory.target_index, 1442);
        assert_eq!(decision.owned_raw, 1_067_086);
        assert_eq!(decision.selected_raw, 1_067_086);
        assert_eq!(self.decision_state()?.0, "proven");
        Ok(wire.len())
    }
}
