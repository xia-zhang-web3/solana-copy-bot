use super::{NativeBuyFence, TechnicalCohortAuthority, CLASSIC_SPL_MINT_POLICY};
use crate::association_inbox::AssociationInbox;
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

pub(crate) const MIGRATION: &str = "0088_native_buy_technical_cohort.sql";
const MAX_EPOCHS: i64 = 256;

pub(crate) fn available(c: &Connection) -> Result<bool> {
    Ok(c.query_row("SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=?1)",
        [MIGRATION], |r| r.get(0))?)
}

impl TechnicalCohortAuthority {
    fn validate(&self) -> Result<()> {
        ensure!((1..=128).contains(&self.run_id.len())
            && self.run_id.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b)),
            "technical_cohort_run_id");
        ensure!((1..=3).contains(&self.wallet_ids.len())
            && self.wallet_ids.iter().all(|w| (1..=128).contains(&w.len()) && w.trim() == w)
            && self.wallet_ids.iter().enumerate().all(|(i,w)| !self.wallet_ids[..i].contains(w)),
            "technical_cohort_wallets");
        ensure!(self.mint_policy == CLASSIC_SPL_MINT_POLICY, "technical_cohort_mint_policy");
        ensure!(self.max_buy_count == 1, "technical_cohort_buy_limit");
        ensure!(self.activated_at < self.deadline
            && self.deadline - self.activated_at <= Duration::hours(24),
            "technical_cohort_deadline");
        ensure!((1..=128).contains(&self.policy_identity.len()), "technical_cohort_policy_identity");
        Ok(())
    }
}

/// The exact authority is immutable and may be registered again only identically.
/// Call before opening source intake. It cannot reset a consumed BUY or deadline.
impl AssociationInbox {
    pub fn register_technical_cohort_authority(&mut self, authority: &TechnicalCohortAuthority) -> Result<()> {
        ensure!(available(&self.conn)?, "technical_cohort_migration_required");
        authority.validate()?;
        let tx = self.conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let wallets = serde_json::to_string(&authority.wallet_ids)?;
        let old: Option<(String,String,String,String,String,i64,String)> = tx.query_row(
            "SELECT run_id,wallet_ids_json,mint_policy,activated_at,deadline,max_buy_count,policy_identity FROM native_buy_technical_cohort WHERE singleton=1",
            [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?))).optional()?;
        if let Some(old) = old {
            ensure!(old == (authority.run_id.clone(),wallets,authority.mint_policy.clone(),
                authority.activated_at.to_rfc3339(),authority.deadline.to_rfc3339(),
                i64::from(authority.max_buy_count),authority.policy_identity.clone()),
                "technical_cohort_authority_conflict");
        } else {
            tx.execute("INSERT INTO native_buy_technical_cohort(singleton,run_id,wallet_ids_json,mint_policy,activated_at,deadline,max_buy_count,policy_identity) VALUES(1,?1,?2,?3,?4,?5,?6,?7)",
                params![authority.run_id,wallets,authority.mint_policy,
                    authority.activated_at.to_rfc3339(),authority.deadline.to_rfc3339(),
                    authority.max_buy_count,authority.policy_identity])?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Append a checked processed-slot sample. Initial call also creates the
    /// immutable session fence; later calls retain it and append new epochs.
    pub fn record_native_buy_fence_epoch(&mut self, fence: &NativeBuyFence) -> Result<()> {
        ensure!(available(&self.conn)?, "technical_cohort_migration_required");
        let authority = load(&self.conn)?.context("technical_cohort_authority_missing")?;
        ensure!(fence.policy_identity == authority.policy_identity
            && !fence.session.is_empty() && fence.session.len() <= 128
            && fence.processed_slot > 0 && fence.processed_slot <= i64::MAX as u64
            && (1..=128).contains(&fence.genesis_hash.len())
            && fence.sampled_at >= authority.activated_at && fence.sampled_at < authority.deadline,
            "technical_cohort_fence_invalid");
        let tx = self.conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let initial: Option<(i64,String,String,String)> = tx.query_row(
            "SELECT processed_slot,sampled_at,genesis_hash,policy_identity FROM native_buy_fences WHERE session=?1",
            [&fence.session], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
        if let Some((slot,sampled,genesis,policy)) = initial {
            ensure!(genesis == fence.genesis_hash && policy == fence.policy_identity,
                "technical_cohort_fence_session_conflict");
            let first_at = parse(&sampled).context("technical_cohort_initial_fence_clock")?;
            ensure!(fence.sampled_at >= first_at && fence.processed_slot >= slot as u64,
                "technical_cohort_fence_regression");
            let active: Option<String> = tx.query_row(
                "SELECT active_session FROM native_buy_session_state WHERE id=1",[],|r|r.get(0)).optional()?.flatten();
            ensure!(active.as_deref() == Some(fence.session.as_str()),
                "technical_cohort_fence_session_closed");
        } else {
            let old_admission: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM association_inbox_identities WHERE first_session=?1)",
                [&fence.session], |r|r.get(0))?;
            ensure!(!old_admission, "technical_cohort_fence_after_admission");
            let sessions: i64 = tx.query_row(
                "SELECT count(*) FROM native_buy_fences",[],|r|r.get(0))?;
            ensure!(sessions < i64::try_from(self.limits.count)?,
                "technical_cohort_fence_session_capacity");
            tx.execute("INSERT INTO native_buy_fences(session,processed_slot,sampled_at,genesis_hash,policy_identity) VALUES(?1,?2,?3,?4,?5)",
                params![fence.session,fence.processed_slot as i64,fence.sampled_at.to_rfc3339(),fence.genesis_hash,fence.policy_identity])?;
            tx.execute("INSERT INTO native_buy_session_state(id,active_session) VALUES(1,?1) ON CONFLICT(id) DO UPDATE SET active_session=excluded.active_session", [&fence.session])?;
        }
        let previous: Option<(i64,i64,String)> = tx.query_row(
            "SELECT epoch_id,processed_slot,sampled_at FROM native_buy_fence_epochs WHERE session=?1 ORDER BY epoch_id DESC LIMIT 1",
            [&fence.session], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        if let Some((_,slot,sampled)) = &previous {
            let previous_at = parse(sampled).context("technical_cohort_fence_clock")?;
            ensure!(fence.sampled_at >= previous_at && fence.processed_slot >= *slot as u64,
                "technical_cohort_fence_regression");
            if fence.sampled_at == previous_at && fence.processed_slot == *slot as u64 {
                tx.commit()?;
                return Ok(());
            }
            ensure!(fence.sampled_at > previous_at, "technical_cohort_fence_clock_conflict");
        }
        let count: i64 = tx.query_row("SELECT count(*) FROM native_buy_fence_epochs", [], |r|r.get(0))?;
        ensure!(count < MAX_EPOCHS, "technical_cohort_fence_capacity");
        tx.execute("INSERT INTO native_buy_fence_epochs(session,processed_slot,sampled_at,genesis_hash,policy_identity) VALUES(?1,?2,?3,?4,?5)",
            params![fence.session,fence.processed_slot as i64,fence.sampled_at.to_rfc3339(),fence.genesis_hash,fence.policy_identity])?;
        tx.commit()?;
        Ok(())
    }
}

pub(crate) fn load(c: &Connection) -> Result<Option<TechnicalCohortAuthority>> {
    if !available(c)? { return Ok(None); }
    let row: Option<(String,String,String,String,String,i64,String)> = c.query_row(
        "SELECT run_id,wallet_ids_json,mint_policy,activated_at,deadline,max_buy_count,policy_identity FROM native_buy_technical_cohort WHERE singleton=1",
        [], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?))).optional()?;
    row.map(|(run_id,wallets,mint_policy,activated,deadline,count,policy_identity)| {
        let a = TechnicalCohortAuthority { run_id, wallet_ids:serde_json::from_str(&wallets)?,
            mint_policy, activated_at:parse(&activated).context("technical_cohort_activation_clock")?,
            deadline:parse(&deadline).context("technical_cohort_deadline_clock")?,
            max_buy_count:count.try_into()?, policy_identity };
        a.validate()?;
        Ok(a)
    }).transpose()
}

#[derive(Debug)]
pub(crate) struct Epoch {
    pub id: i64,
    pub session: String,
    pub slot: i64,
    pub sampled_at: DateTime<Utc>,
    pub genesis: String,
    pub policy: String,
}
pub(crate) fn latest_epoch(c: &Connection, session: &str) -> Result<Option<Epoch>> {
    let row: Option<(i64,String,i64,String,String,String)> = c.query_row(
        "SELECT epoch_id,session,processed_slot,sampled_at,genesis_hash,policy_identity FROM native_buy_fence_epochs WHERE session=?1 ORDER BY epoch_id DESC LIMIT 1",
        [session], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))).optional()?;
    row.map(|(id,session,slot,sampled,genesis,policy)| Ok(Epoch {
        id,session,slot,sampled_at:parse(&sampled).context("technical_cohort_fence_clock")?,genesis,policy
    })).transpose()
}
pub(crate) fn epoch(c: &Connection, id: i64) -> Result<Option<Epoch>> {
    let row: Option<(i64,String,i64,String,String,String)> = c.query_row(
        "SELECT epoch_id,session,processed_slot,sampled_at,genesis_hash,policy_identity FROM native_buy_fence_epochs WHERE epoch_id=?1",
        [id], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))).optional()?;
    row.map(|(id,session,slot,sampled,genesis,policy)| Ok(Epoch {
        id,session,slot,sampled_at:parse(&sampled).context("technical_cohort_fence_clock")?,genesis,policy
    })).transpose()
}

#[derive(Debug)]
pub(crate) struct DecisionBinding {
    pub run_id: String,
    pub epoch_id: i64,
    pub wallet: String,
    pub mint: String,
    pub admitted_at: String,
    pub policy_identity: String,
}
pub(crate) fn binding(c: &Connection, signature: &str) -> Result<Option<DecisionBinding>> {
    if !available(c)? { return Ok(None); }
    Ok(c.query_row("SELECT run_id,fence_epoch_id,wallet,mint,admitted_at,policy_identity FROM native_buy_cohort_decisions WHERE signature=?1",
        [signature], |r|Ok(DecisionBinding {
            run_id:r.get(0)?,epoch_id:r.get(1)?,wallet:r.get(2)?,mint:r.get(3)?,
            admitted_at:r.get(4)?,policy_identity:r.get(5)?
        })).optional()?)
}
pub(crate) fn parse(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s).ok().map(|t| t.with_timezone(&Utc))
}
