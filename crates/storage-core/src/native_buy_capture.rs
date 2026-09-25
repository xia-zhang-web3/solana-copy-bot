use super::{cohort, NativeBuyFence};
use crate::association_inbox::AssociationInbox;
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::association_delivery::{AdmissionFacts, Delivery, DeliveryEvent};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

const SOL: &str = "So11111111111111111111111111111111111111112";
const MIGRATION: &str = "0083_native_buy_decision.sql";

pub(crate) fn available(c: &Connection) -> Result<bool> {
    Ok(c.query_row(
        "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=?1)",
        [MIGRATION],
        |r| r.get(0),
    )?)
}

impl AssociationInbox {
    /// Call only after the request-bound processed getSlot and pinned-genesis check.
    /// A different fence cannot replace the first value for this session.
    pub fn record_native_buy_fence(&mut self, fence: &NativeBuyFence) -> Result<()> {
        ensure!(available(&self.conn)?, "native_buy_migration_required");
        ensure!(
            !fence.session.is_empty() && fence.session.len() <= 128
                && fence.processed_slot > 0 && fence.processed_slot <= i64::MAX as u64
                && (1..=128).contains(&fence.genesis_hash.len())
                && (1..=128).contains(&fence.policy_identity.len()),
            "native_buy_fence_invalid"
        );
        let tx = self.conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let prior: Option<(i64, String, String, String)> = tx.query_row(
            "SELECT processed_slot,sampled_at,genesis_hash,policy_identity FROM native_buy_fences WHERE session=?1",
            [&fence.session],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        ).optional()?;
        if let Some((slot, sampled, genesis, policy)) = prior {
            ensure!(slot == fence.processed_slot as i64
                && sampled == fence.sampled_at.to_rfc3339()
                && genesis == fence.genesis_hash && policy == fence.policy_identity,
                "native_buy_fence_conflict");
            let active: Option<String> = tx.query_row(
                "SELECT active_session FROM native_buy_session_state WHERE id=1", [],
                |r| r.get(0)).optional()?.flatten();
            ensure!(active.as_deref() == Some(fence.session.as_str()),
                "native_buy_fence_session_replay");
        } else {
            let old_admission: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM association_inbox_identities WHERE first_session=?1)",
                [&fence.session], |r| r.get(0))?;
            ensure!(!old_admission, "native_buy_fence_after_admission");
            let count: i64 = tx.query_row("SELECT count(*) FROM native_buy_fences", [], |r| r.get(0))?;
            ensure!(count < i64::try_from(self.limits.count)?, "native_buy_fence_capacity");
            tx.execute("INSERT INTO native_buy_fences(session,processed_slot,sampled_at,genesis_hash,policy_identity) VALUES(?1,?2,?3,?4,?5)",
                params![fence.session, fence.processed_slot as i64, fence.sampled_at.to_rfc3339(), fence.genesis_hash, fence.policy_identity])?;
            tx.execute("INSERT INTO native_buy_session_state(id,active_session) VALUES(1,?1) ON CONFLICT(id) DO UPDATE SET active_session=excluded.active_session", [&fence.session])?;
        }
        tx.commit()?;
        Ok(())
    }
}

/// Runs inside the inbox's first-admission transaction, before its ACK.
pub(crate) fn at_admission(c: &Connection, d: &Delivery, observed: DateTime<Utc>) -> Result<()> {
    if !available(c)? { return Ok(()); }
    let DeliveryEvent::Admission(a) = &d.event else { return Ok(()); };
    let f = &a.facts;
    if f.token_in != SOL || f.token_out == SOL || f.program_fallback
        || f.signature.is_empty() || f.wallet.is_empty() || f.token_out.is_empty()
    { return Ok(()); }
    let Some(amount) = exact_lamports(a) else { return Ok(()); };
    if let Some(authority) = cohort::load(c)? {
        return cohort_admission(c, d, observed, amount, &authority);
    }
    let fence: Option<(i64, String, String, String)> = c.query_row(
        "SELECT processed_slot,sampled_at,genesis_hash,policy_identity FROM native_buy_fences WHERE session=?1",
        [&d.session], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))).optional()?;
    let Some((slot, sampled, genesis, policy)) = fence else { return Ok(()); };
    let Ok(sampled_at) = DateTime::parse_from_rfc3339(&sampled) else { return Ok(()); };
    if slot <= 0 || f.slot <= slot as u64 || sampled_at.with_timezone(&Utc) > observed
        || genesis.is_empty() || policy.is_empty() { return Ok(()); }
    let current: Option<String> = c.query_row("SELECT active_session FROM native_buy_session_state WHERE id=1", [], |r| r.get(0)).optional()?.flatten();
    if current.as_deref() != Some(d.session.as_str()) { return Ok(()); }
    let Some((follow_id, follow_added_at)) = follow(c, &f.wallet)? else { return Ok(()); };
    let Some((cohort, window, updated)) = cohort(c, &f.wallet)? else { return Ok(()); };
    let Some((fingerprint, published)) = publication(c, &f.wallet, &window)? else { return Ok(()); };
    if !before(&follow_added_at, observed) || !before(&updated, observed)
        || !before(&published, observed) { return Ok(()); }
    let signal_id = format!("native-buy-v1:{}", f.signature);
    let decision_id = format!("native-buy-decision-v1:{}", f.signature);
    if signal_id.len() > 256 || decision_id.len() > 256 { return Ok(()); }
    c.execute("INSERT OR IGNORE INTO native_buy_decisions(signature,decision_id,signal_id,first_session,first_sequence,admission,admitted_at,wallet,mint,source_slot,amount_lamports,follow_id,follow_added_at,source_cohort,cohort_window_start,cohort_updated_at,publication_fingerprint,publication_published_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)",
        params![f.signature,decision_id,signal_id,d.session,i64::try_from(d.sequence)?,serde_json::to_string(a)?,observed.to_rfc3339(),f.wallet,f.token_out,i64::try_from(f.slot)?,amount,follow_id,follow_added_at,cohort,window,updated,fingerprint,published])?;
    Ok(())
}

fn cohort_admission(
    c: &Connection, d: &Delivery, observed: DateTime<Utc>, amount: i64,
    authority: &super::TechnicalCohortAuthority,
) -> Result<()> {
    let DeliveryEvent::Admission(a) = &d.event else { return Ok(()); };
    let f = &a.facts;
    if observed < authority.activated_at || observed >= authority.deadline
        || !authority.wallet_ids.contains(&f.wallet) { return Ok(()); }
    let consumed: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM native_buy_cohort_decisions WHERE run_id=?1)",
        [&authority.run_id], |r|r.get(0))?;
    if consumed { return Ok(()); }
    let Some(epoch) = cohort::latest_epoch(c, &d.session)? else { return Ok(()); };
    if epoch.session != d.session || epoch.slot <= 0 || f.slot <= epoch.slot as u64
        || epoch.sampled_at < authority.activated_at || epoch.sampled_at > observed
        || observed.signed_duration_since(epoch.sampled_at) > chrono::Duration::seconds(120)
        || epoch.genesis.is_empty() || epoch.policy != authority.policy_identity
    { return Ok(()); }
    let active: Option<String> = c.query_row(
        "SELECT active_session FROM native_buy_session_state WHERE id=1",[],|r|r.get(0)).optional()?.flatten();
    if active.as_deref() != Some(d.session.as_str()) { return Ok(()); }
    let signal_id = format!("native-buy-v1:{}", f.signature);
    let decision_id = format!("native-buy-decision-v1:{}", f.signature);
    if signal_id.len() > 256 || decision_id.len() > 256 { return Ok(()); }
    let inserted = c.execute("INSERT OR IGNORE INTO native_buy_decisions(signature,decision_id,signal_id,first_session,first_sequence,admission,admitted_at,wallet,mint,source_slot,amount_lamports,follow_id,follow_added_at,source_cohort,cohort_window_start,cohort_updated_at,publication_fingerprint,publication_published_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,0,'','','','','','')",
        params![f.signature,decision_id,signal_id,d.session,i64::try_from(d.sequence)?,
            serde_json::to_string(a)?,observed.to_rfc3339(),f.wallet,f.token_out,
            i64::try_from(f.slot)?,amount])?;
    if inserted == 1 {
        c.execute("INSERT INTO native_buy_cohort_decisions(signature,run_id,fence_epoch_id,wallet,mint,admitted_at,policy_identity) VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![f.signature,authority.run_id,epoch.id,f.wallet,f.token_out,
                observed.to_rfc3339(),authority.policy_identity])?;
    }
    Ok(())
}

pub(crate) fn mark_late(c: &Connection, signature: &str) -> Result<()> {
    if available(c)? {
        c.execute("UPDATE native_buy_decisions SET late=1 WHERE signature=?1", [signature])?;
    }
    Ok(())
}

pub(crate) fn close_session(c: &Connection, session: &str) -> Result<()> {
    if available(c)? {
        c.execute("UPDATE native_buy_session_state SET active_session=NULL WHERE id=1 AND active_session=?1", [session])?;
    }
    Ok(())
}

fn exact_lamports(a: &AdmissionFacts) -> Option<i64> {
    let x = a.facts.exact_amounts.as_ref()?;
    (x.amount_in_decimals == 9 && x.amount_out_raw.parse::<u64>().ok()? > 0)
        .then_some(x.amount_in_raw.parse::<i64>().ok()?)
        .filter(|n| *n > 0)
}
fn follow(c: &Connection, wallet: &str) -> Result<Option<(i64, String)>> {
    Ok(c.query_row("SELECT id,added_at FROM followlist WHERE wallet_id=?1 AND active=1 AND removed_at IS NULL LIMIT 1", [wallet], |r| Ok((r.get(0)?,r.get(1)?))).optional()?)
}
fn cohort(c: &Connection, wallet: &str) -> Result<Option<(String, String, String)>> {
    let row: Option<(String,String,String)> = c.query_row("SELECT source_cohort,window_start,updated_at FROM discovery_candidate_sources WHERE wallet_id=?1", [wallet], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    Ok(row.filter(|(a,b,c)| !a.is_empty() && !b.is_empty() && !c.is_empty()))
}
fn publication(c: &Connection, wallet: &str, window: &str) -> Result<Option<(String, String)>> {
    let exists: bool = c.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='discovery_strategy_state')", [], |r| r.get(0))?;
    if !exists { return Ok(None); }
    let row: Option<(String, Option<String>, Option<String>, Option<String>, Option<String>)> = c.query_row(
        "SELECT publication_runtime_mode,publication_last_published_at,publication_last_published_window_start,publication_policy_fingerprint,publication_wallet_ids_json FROM discovery_strategy_state WHERE id=1", [],
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
    let Some((mode,Some(published),Some(published_window),Some(fingerprint),Some(ids))) = row else { return Ok(None); };
    if mode != "healthy" || published_window != window || fingerprint.is_empty() || ids.len() > 1<<20 { return Ok(None); }
    let Ok(wallets) = serde_json::from_str::<Vec<String>>(&ids) else { return Ok(None); };
    Ok(wallets.iter().any(|id| id == wallet).then_some((fingerprint,published)))
}
fn before(raw: &str, now: DateTime<Utc>) -> bool {
    DateTime::parse_from_rfc3339(raw).is_ok_and(|t| t.with_timezone(&Utc) <= now)
}
