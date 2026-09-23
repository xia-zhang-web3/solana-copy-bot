use super::{NativeBuyCandidate, NativeBuyPending};
use crate::association_inbox;
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::association_delivery::{CandidateGeneration, Terminal};
use rusqlite::{Connection, OptionalExtension};

#[derive(Debug, Clone)]
pub(super) struct Decision {
    pub signature: String,
    pub decision_id: String,
    pub signal_id: String,
    pub session: String,
    pub sequence: i64,
    pub admission: String,
    pub admitted_at: String,
    pub wallet: String,
    pub mint: String,
    pub slot: i64,
    pub amount: i64,
    pub follow_id: i64,
    pub follow_added_at: String,
    pub cohort: String,
    pub window: String,
    pub cohort_updated_at: String,
    pub publication_fingerprint: String,
    pub published_at: String,
    pub finalized_at: Option<String>,
    pub finalized_slot: Option<i64>,
    pub late: bool,
}

pub(super) fn load(c: &Connection, signature: &str) -> Result<Option<Decision>> {
    Ok(c.query_row("SELECT signature,decision_id,signal_id,first_session,first_sequence,admission,admitted_at,wallet,mint,source_slot,amount_lamports,follow_id,follow_added_at,source_cohort,cohort_window_start,cohort_updated_at,publication_fingerprint,publication_published_at,finalized_at,finalized_slot,late FROM native_buy_decisions WHERE signature=?1", [signature], |r| Ok(Decision {
        signature:r.get(0)?, decision_id:r.get(1)?, signal_id:r.get(2)?, session:r.get(3)?, sequence:r.get(4)?, admission:r.get(5)?, admitted_at:r.get(6)?, wallet:r.get(7)?, mint:r.get(8)?, slot:r.get(9)?, amount:r.get(10)?, follow_id:r.get(11)?, follow_added_at:r.get(12)?, cohort:r.get(13)?, window:r.get(14)?, cohort_updated_at:r.get(15)?, publication_fingerprint:r.get(16)?, published_at:r.get(17)?, finalized_at:r.get(18)?, finalized_slot:r.get(19)?, late:r.get(20)?
    })).optional()?)
}

/// All lookups use the caller's connection. Inside the dispatch BEGIN IMMEDIATE
/// this is one atomic eligibility check; it never starts a nested transaction.
pub(super) fn valid(
    c: &Connection,
    d: &Decision,
    now: DateTime<Utc>,
    max_age_seconds: Option<u64>,
    need_finality: bool,
) -> Result<bool> {
    if d.late || d.sequence < 0 || d.slot <= 0 || d.amount <= 0
        || d.decision_id != format!("native-buy-decision-v1:{}", d.signature)
        || d.signal_id != format!("native-buy-v1:{}", d.signature)
    { return Ok(false); }
    let Some(admitted) = time(&d.admitted_at) else { return Ok(false); };
    if now < admitted || max_age_seconds.is_some_and(|age| too_old(admitted, now, age)) {
        return Ok(false);
    }
    let Some(first) = association_inbox::identity(c, &d.signature)? else { return Ok(false); };
    let Some(Terminal::ProviderAsserted(assertion)) = first.terminal else { return Ok(false); };
    if first.conflict || first.recovery || !matches!(first.candidate, CandidateGeneration::Unknown)
        || first.first_session != d.session || first.first_sequence != d.sequence as u64
        || serde_json::to_string(&first.admission)? != d.admission
        || first.admission.facts.signature != d.signature
        || first.admission.facts.wallet != d.wallet
        || first.admission.facts.token_out != d.mint
        || first.admission.facts.slot != d.slot as u64
        || first.admission.facts.token_in != "So11111111111111111111111111111111111111112"
        || first.admission.facts.program_fallback
        || assertion.signature != d.signature || assertion.slot != d.slot as u64
        || assertion.blockhash.is_empty() || assertion.blockhash.len() > 128
    { return Ok(false); }
    let amount = first.admission.facts.exact_amounts.as_ref().and_then(|x|
        (x.amount_in_decimals == 9 && x.amount_out_raw.parse::<u64>().ok()? > 0)
            .then(|| x.amount_in_raw.parse::<i64>().ok()).flatten());
    if amount != Some(d.amount) { return Ok(false); }
    let exact = first.admission.facts.exact_amounts.as_ref();
    if exact.is_none_or(|x| x.amount_out_decimals > 18
        || x.amount_out_raw.parse::<u64>().ok().is_none_or(|v| v == 0))
    { return Ok(false); }
    let fence: Option<(i64,String,String,String)> = c.query_row(
        "SELECT processed_slot,sampled_at,genesis_hash,policy_identity FROM native_buy_fences WHERE session=?1",
        [&d.session], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
    let Some((fence_slot,sampled,genesis,policy)) = fence else { return Ok(false); };
    let Some(sampled_at) = time(&sampled) else { return Ok(false); };
    if fence_slot <= 0 || d.slot <= fence_slot || sampled_at > admitted || now < sampled_at
        || genesis.is_empty() || policy.is_empty()
        || max_age_seconds.is_some_and(|age| too_old(sampled_at, now, age))
    { return Ok(false); }
    let active: Option<String> = c.query_row("SELECT active_session FROM native_buy_session_state WHERE id=1", [], |r| r.get(0)).optional()?.flatten();
    if active.as_deref() != Some(d.session.as_str()) { return Ok(false); }
    let follow: Option<(i64,String)> = c.query_row("SELECT id,added_at FROM followlist WHERE wallet_id=?1 AND active=1 AND removed_at IS NULL LIMIT 1", [&d.wallet], |r| Ok((r.get(0)?,r.get(1)?))).optional()?;
    if follow.as_ref() != Some(&(d.follow_id,d.follow_added_at.clone())) { return Ok(false); }
    let cohort: Option<(String,String,String)> = c.query_row("SELECT source_cohort,window_start,updated_at FROM discovery_candidate_sources WHERE wallet_id=?1", [&d.wallet], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    if cohort.as_ref() != Some(&(d.cohort.clone(),d.window.clone(),d.cohort_updated_at.clone())) { return Ok(false); }
    let exists: bool = c.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='discovery_strategy_state')", [], |r| r.get(0))?;
    if !exists { return Ok(false); }
    let publication: Option<(String,Option<String>,Option<String>,Option<String>,Option<String>)> = c.query_row("SELECT publication_runtime_mode,publication_last_published_at,publication_last_published_window_start,publication_policy_fingerprint,publication_wallet_ids_json FROM discovery_strategy_state WHERE id=1", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
    let Some((mode,Some(published),Some(window),Some(fingerprint),Some(ids))) = publication else { return Ok(false); };
    if mode != "healthy" || published != d.published_at || window != d.window
        || fingerprint != d.publication_fingerprint || ids.len() > 1<<20
        || !serde_json::from_str::<Vec<String>>(&ids).is_ok_and(|v| v.iter().any(|w| w == &d.wallet))
    { return Ok(false); }
    if [d.follow_added_at.as_str(),d.cohort_updated_at.as_str(),d.published_at.as_str()].iter().any(|raw| time(raw).is_none_or(|t| t > admitted)) { return Ok(false); }
    if need_finality {
        let (Some(finalized),Some(slot)) = (d.finalized_at.as_deref().and_then(time), d.finalized_slot) else { return Ok(false); };
        if finalized < admitted || finalized > now || slot != d.slot { return Ok(false); }
    } else if d.finalized_at.is_some() != d.finalized_slot.is_some() { return Ok(false); }
    Ok(true)
}

fn time(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s).ok().map(|t| t.with_timezone(&Utc))
}
fn too_old(start: DateTime<Utc>, now: DateTime<Utc>, max: u64) -> bool {
    let Ok(max) = i64::try_from(max) else { return true; };
    now.signed_duration_since(start) > chrono::Duration::seconds(max)
}

pub(super) fn candidate(d: &Decision) -> Option<NativeBuyCandidate> {
    Some(NativeBuyCandidate {
        signature:d.signature.clone(), signal_id:d.signal_id.clone(), decision_id:d.decision_id.clone(),
        wallet:d.wallet.clone(), mint:d.mint.clone(), slot:u64::try_from(d.slot).ok()?,
        amount_lamports:u64::try_from(d.amount).ok()?, admitted_at:time(&d.admitted_at)?,
    })
}
pub(super) fn pending(d: &Decision) -> Option<NativeBuyPending> {
    Some(NativeBuyPending { signature:d.signature.clone(),slot:u64::try_from(d.slot).ok()?,wallet:d.wallet.clone(),mint:d.mint.clone(),first_session:d.session.clone(),decision_id:d.decision_id.clone() })
}
