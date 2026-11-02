//! Explicit LOCAL backend; the legacy singleton is only an immutable FK anchor.
use crate::{
    association_inbox::AssociationInbox, controlled_receipt_lot::Scope,
    native_buy::TechnicalCohortAuthority, SqliteStore,
};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
#[path = "local_bounded_budget.rs"]
pub(crate) mod budget;
#[path = "local_bounded_source.rs"]
pub(crate) mod source;
#[path = "local_bounded_state.rs"]
pub(crate) mod state;
#[path = "local_bounded_native.rs"]
pub(crate) mod native;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceCycle {
    pub cycle_id: String,
    pub buy_signature: String,
    pub sell_signature: String,
    pub buy_message_sha256: String,
    pub sell_message_sha256: String,
    pub acquired_raw: u64,
    pub sold_raw: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Model {
    pub policy: String,
    pub session_id: String,
    pub max_cycles: u32,
    pub gross_buy_cap: u64,
    pub fee_cap: u64,
    pub cycles: Vec<SourceCycle>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Authority {
    pub model: Model,
    pub cohort: TechnicalCohortAuthority,
    pub scope: Scope,
    pub genesis: String,
}
pub(crate) fn active(c: &Connection) -> Result<bool> {
    let table: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='local_bounded_sessions')",
        [],
        |r| r.get(0),
    )?;
    let found = table
        && c.query_row(
            "SELECT EXISTS(SELECT 1 FROM local_bounded_sessions)",
            [],
            |r| r.get::<_, bool>(0),
        )?;
    ensure!(
        !found || cfg!(feature = "local-bounded-session"),
        "local_session_feature_required"
    );
    Ok(found)
}
pub(crate) fn load(c: &Connection) -> Result<Option<Authority>> {
    if !active(c)? {
        return Ok(None);
    }
    let raw: String = c.query_row(
        "SELECT identity_json FROM local_bounded_sessions WHERE singleton=1",
        [],
        |r| r.get(0),
    )?;
    let a: Authority = serde_json::from_str(&raw)?;
    validate(&a)?;
    Ok(Some(a))
}
fn validate(a: &Authority) -> Result<()> {
    ensure!(
        cfg!(feature = "local-bounded-session")
            && a.genesis == "11111111111111111111111111111111"
            && a.model.policy == "local_bounded_session_v1"
            && a.model.max_cycles == 2
            && a.model.cycles.len() == 2
            && a.model.gross_buy_cap == 20_000_000
            && a.model.fee_cap == 400_000
            && a.model.session_id == a.cohort.run_id
            && a.scope.bot_run_id == a.model.session_id
            && a.cohort.wallet_ids == vec![a.scope.source_wallet.clone()]
            && a.cohort.mint_policy == crate::native_buy::CLASSIC_SPL_MINT_POLICY
            && a.scope.source_buy_intent_id == a.model.cycles[0].cycle_id
            && a.cohort.max_buy_count == 2
            && a.cohort.deadline > a.cohort.activated_at
            && a.cohort.deadline - a.cohort.activated_at <= chrono::Duration::seconds(700),
        "local_session_authority"
    );
    a.scope.run(&a.scope.bot_wallet)?;
    let mut signatures = std::collections::HashSet::new();
    let mut hashes = std::collections::HashSet::new();
    for (i, p) in a.model.cycles.iter().enumerate() {
        ensure!(
            !p.cycle_id.is_empty()
                && p.cycle_id.len() <= 96
                && p.acquired_raw > 0
                && p.sold_raw == p.acquired_raw
                && !a.model.cycles[..i].iter().any(|q| q.cycle_id == p.cycle_id
                    || q.buy_signature == p.buy_signature
                    || q.sell_signature == p.sell_signature)
                && p.buy_signature != p.sell_signature,
            "local_session_cycle_identity"
        );
        for sig in [&p.buy_signature, &p.sell_signature] {
            ensure!(
                (64..=88).contains(&sig.len())
                    && sig.bytes().all(|b| b.is_ascii_alphanumeric())
                    && signatures.insert(sig),
                "local_session_source_signature"
            );
        }
        for hash in [&p.buy_message_sha256, &p.sell_message_sha256] {
            ensure!(
                hash.len() == 64
                    && hash.bytes().all(|b| b.is_ascii_hexdigit())
                    && hashes.insert(hash),
                "local_session_source_message"
            );
        }
    }
    Ok(())
}
fn register(c: &Connection, a: &Authority) -> Result<()> {
    validate(a)?;
    ensure!(!crate::real_session::active(c)?,"local_session_real_authority_forbidden");
    if let Some(old) = load(c)? {
        ensure!(old == *a, "local_session_identity_conflict");
        return Ok(());
    }
    let tiny: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM execution_tiny_experiment)",
        [],
        |r| r.get(0),
    )?;
    ensure!(!tiny, "local_session_existing_grant_forbidden");
    c.execute(
        "INSERT INTO local_bounded_sessions VALUES(1,?1,?2,?3,?4)",
        params![
            a.model.session_id,
            serde_json::to_string(a)?,
            serde_json::to_string(&a.cohort)?,
            serde_json::to_string(&a.scope)?
        ],
    )?;
    for (i, p) in a.model.cycles.iter().enumerate() {
        let mut scope = a.scope.clone();
        scope.source_buy_intent_id = p.cycle_id.clone();
        c.execute("INSERT INTO local_bounded_cycles(session_id,cycle_no,cycle_id,scope_json,source_buy_signature,source_sell_signature,state,last_decision_at)
            VALUES(?1,?2,?3,?4,?5,?6,'planned',?7)",params![a.model.session_id,i+1,p.cycle_id,serde_json::to_string(&scope)?,p.buy_signature,p.sell_signature,a.cohort.activated_at.to_rfc3339()])?;
    }
    Ok(())
}
impl SqliteStore {
    pub fn register_local_bounded_session(&self, a: &Authority) -> Result<()> {
        self.with_immediate_transaction_retry("register local session", |c| register(c, a))
    }
    pub fn local_bounded_scope(&self) -> Result<Option<Scope>> {
        if !active(&self.conn)? {return Ok(None)}
        state::scope(&self.conn)
    }
    pub fn refresh_local_bounded_session(&self, now: DateTime<Utc>) -> Result<()> {
        self.with_immediate_transaction_retry("advance local session", |c| state::refresh(c, now))
    }
}
impl AssociationInbox {
    pub fn register_local_bounded_session(&mut self, a: &Authority) -> Result<()> {
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        register(&tx, a)?;
        tx.commit()?;
        Ok(())
    }
}
