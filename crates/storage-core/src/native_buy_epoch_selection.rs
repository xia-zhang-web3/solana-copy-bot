//! Select from intact epoch history, then retain that exact proof on recheck.
use super::{
    cohort::{self, Epoch},
    TechnicalCohortAuthority,
};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{params, Connection, OptionalExtension};

/// A later processed head may overtake a delayed live event. Select the newest
/// still-valid earlier-slot proof only after validating the entire session chain.
pub(super) fn select(
    c: &Connection,
    session: &str,
    source_slot: u64,
    observed: DateTime<Utc>,
    authority: &TechnicalCohortAuthority,
) -> Result<Option<Epoch>> {
    let Some(epochs) = history(c, session, observed, authority)? else {
        return Ok(None);
    };
    Ok(epochs.into_iter().rev().find(|e| {
        (e.slot as u64) < source_slot
            && observed.signed_duration_since(e.sampled_at) <= Duration::seconds(120)
    }))
}

/// Read-only on the caller's transaction. Refreshes may extend the history but
/// cannot replace the decision's proof or renew its admission/epoch age.
pub(super) fn pinned(
    c: &Connection,
    session: &str,
    epoch_id: i64,
    now: DateTime<Utc>,
    authority: &TechnicalCohortAuthority,
) -> Result<Option<Epoch>> {
    let Some(epochs) = history(c, session, now, authority)? else {
        return Ok(None);
    };
    Ok(epochs.into_iter().find(|e| e.id == epoch_id))
}

fn history(
    c: &Connection,
    session: &str,
    now: DateTime<Utc>,
    authority: &TechnicalCohortAuthority,
) -> Result<Option<Vec<Epoch>>> {
    let active: Option<String> = c
        .query_row(
            "SELECT active_session FROM native_buy_session_state WHERE id=1",
            [],
            |r| r.get(0),
        )
        .optional()?
        .flatten();
    if active.as_deref() != Some(session) {
        return Ok(None);
    }
    let initial: Option<(i64,String,String,String)> = c.query_row(
        "SELECT processed_slot,sampled_at,genesis_hash,policy_identity FROM native_buy_fences WHERE session=?1",
        [session], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
    let Some((slot, sampled, genesis, policy)) = initial else {
        return Ok(None);
    };
    let sampled = cohort::parse(&sampled).context("technical_cohort_initial_fence_clock")?;
    ensure!(
        slot > 0
            && (1..=128).contains(&genesis.len())
            && policy == authority.policy_identity
            && sampled >= authority.activated_at
            && sampled < authority.deadline
            && sampled <= now,
        "technical_cohort_initial_fence_invalid"
    );
    let mut q = c.prepare(
        "SELECT epoch_id,session,processed_slot,sampled_at,genesis_hash,policy_identity
         FROM native_buy_fence_epochs WHERE session=?1 ORDER BY epoch_id LIMIT ?2",
    )?;
    let rows = q.query_map(params![session, cohort::MAX_EPOCHS + 1], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, i64>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, String>(4)?,
            r.get::<_, String>(5)?,
        ))
    })?;
    let mut epochs: Vec<Epoch> = Vec::new();
    for row in rows {
        let (id, session, slot, raw, genesis, policy) = row?;
        epochs.push(Epoch {
            id,
            session,
            slot,
            sampled_at: cohort::parse(&raw).context("technical_cohort_fence_clock")?,
            genesis,
            policy,
        });
    }
    ensure!(
        epochs.len() <= cohort::MAX_EPOCHS as usize,
        "technical_cohort_fence_capacity"
    );
    let Some(first) = epochs.first() else {
        return Ok(None);
    };
    ensure!(
        first.slot == slot
            && first.sampled_at == sampled
            && first.genesis == genesis
            && first.policy == policy,
        "technical_cohort_initial_epoch_conflict"
    );
    for (i, e) in epochs.iter().enumerate() {
        ensure!(
            e.id > 0
                && e.session == session
                && e.slot > 0
                && e.sampled_at >= authority.activated_at
                && e.sampled_at < authority.deadline
                && e.sampled_at <= now
                && e.genesis == genesis
                && e.policy == policy,
            "technical_cohort_epoch_invalid"
        );
        if let Some(previous) = i.checked_sub(1).map(|j| &epochs[j]) {
            ensure!(
                e.id > previous.id && e.slot >= previous.slot && e.sampled_at > previous.sampled_at,
                "technical_cohort_fence_regression"
            );
        }
    }
    Ok(Some(epochs))
}
