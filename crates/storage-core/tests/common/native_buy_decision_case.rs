//! Isolated RAM Case shared by native selector integration tests.
use super::fixture;
use anyhow::Result;
use chrono::{Duration, Utc};
use copybot_core_types::association_delivery::{
    AdmissionFacts, BlockTime, CandidateGeneration, DeliveryEvent, ProviderAssertion, Terminal,
};
use copybot_storage_core::{
    association_inbox::AssociationInbox, native_buy::NativeBuyFence, SqliteStore,
};
use rusqlite::params;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_CASE: AtomicUsize = AtomicUsize::new(0);

pub(super) struct Case {
    _anchor: SqliteStore,
    pub(super) sql: rusqlite::Connection,
    pub(super) inbox: AssociationInbox,
    pub(super) path: std::path::PathBuf,
    pub(super) now: chrono::DateTime<Utc>,
    pub(super) admission: AdmissionFacts,
}
impl Case {
    pub(super) fn new() -> Result<Self> {
        let path = std::path::PathBuf::from(format!(
            "file:native-buy-neg-{}-{}?mode=memory&cache=shared",
            std::process::id(),
            NEXT_CASE.fetch_add(1, Ordering::Relaxed)
        ));
        let mut anchor = SqliteStore::open(&path)?;
        anchor.run_migrations(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?;
        copybot_storage_core::ensure_discovery_v2_schema(&anchor)?;
        let now = Utc::now();
        let sql = rusqlite::Connection::open(&path)?;
        sql.execute(
            "INSERT INTO followlist(wallet_id,added_at,active) VALUES('leader',?1,1)",
            [(now - Duration::seconds(3)).to_rfc3339()],
        )?;
        sql.execute("INSERT INTO discovery_candidate_sources(wallet_id,source_cohort,window_start,updated_at) VALUES('leader','candidate','window',?1)",[(now-Duration::seconds(3)).to_rfc3339()])?;
        sql.execute("INSERT INTO discovery_strategy_state(id,publication_runtime_mode,publication_last_published_at,publication_last_published_window_start,publication_policy_fingerprint,publication_wallet_ids_json,updated_at) VALUES(1,'healthy',?1,'window','policy','[\"leader\"]',?2)",params![(now-Duration::seconds(3)).to_rfc3339(),now.to_rfc3339()])?;
        let mut admission = fixture::facts();
        admission.facts.signature = "source-buy".into();
        admission.facts.token_in = "So11111111111111111111111111111111111111112".into();
        admission.facts.token_out = "classic-mint".into();
        admission
            .facts
            .exact_amounts
            .as_mut()
            .unwrap()
            .amount_in_raw = "1428".into();
        admission
            .facts
            .exact_amounts
            .as_mut()
            .unwrap()
            .amount_out_raw = "1000".into();
        let inbox = AssociationInbox::open(&path, fixture::limits())?;
        Ok(Self {
            _anchor: anchor,
            sql,
            inbox,
            path,
            now,
            admission,
        })
    }
    pub(super) fn fence(&mut self, session: &str, slot: u64) -> Result<()> {
        self.inbox.record_native_buy_fence(&NativeBuyFence {
            session: session.into(),
            processed_slot: slot,
            sampled_at: self.now - Duration::seconds(1),
            genesis_hash: "genesis".into(),
            policy_identity: "config-v1".into(),
        })
    }
    pub(super) fn admit(&mut self) -> Result<()> {
        self.inbox.persist_at(
            &fixture::event(1, DeliveryEvent::Admission(self.admission.clone())),
            &CandidateGeneration::Unknown,
            self.now,
        )
    }
    pub(super) fn terminal(&mut self, result: Terminal) -> Result<()> {
        self.inbox.persist_at(
            &fixture::event(
                2,
                DeliveryEvent::Terminal {
                    signature: self.admission.facts.signature.clone(),
                    expected: self.admission.clone(),
                    result,
                },
            ),
            &CandidateGeneration::Unknown,
            self.now,
        )
    }
    pub(super) fn asserted(&self) -> Terminal {
        Terminal::ProviderAsserted(ProviderAssertion {
            slot: self.admission.facts.slot,
            signature: self.admission.facts.signature.clone(),
            blockhash: "blockhash".into(),
            transaction_index: 1,
            block_time: BlockTime::Missing,
        })
    }
    pub(super) fn store(&self) -> Result<SqliteStore> {
        SqliteStore::open(&self.path)
    }
}
