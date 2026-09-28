use super::*;
use crate::association_inbox::{identity, InboxLimits};
use copybot_core_types::association_delivery::{AdmissionFacts, Terminal};
use copybot_core_types::association_parent::{BlockKey, ParentObservation};
use std::collections::HashSet;

pub(super) fn block(c: &Connection, b: &BlockCheckpoint, limits: InboxLimits) -> Result<()> {
    ensure!(
        b.scope.valid()
            && b.observation.issue.is_none()
            && b.observation.expected_issue().is_none()
            && b.observation.parent.slot > 0,
        "association_replay_checkpoint_identity"
    );
    ensure!(
        b.executed_transaction_count == b.supplied_transaction_count
            && b.supplied_transaction_count <= 4096
            && b.claims.len() <= limits.count,
        "association_replay_incomplete_block"
    );
    node(c, &b.observation.child, limits.bytes)?;
    let mut seen = HashSet::new();
    let mut indices = HashSet::new();
    for claim in &b.claims {
        ensure!(
            claim.transaction_index < b.executed_transaction_count
                && seen.insert(&claim.signature)
                && indices.insert(claim.transaction_index),
            "association_replay_claim_identity"
        );
        let row =
            identity(c, &claim.signature)?.context("association_replay_claim_not_committed")?;
        ensure!(
            !row.conflict
                && !row.recovery
                && row.admission.info == claim.info
                && row.admission.facts.slot == b.observation.child.slot
                && b.scope.wallets.contains(&row.admission.facts.wallet),
            "association_replay_claim_changed"
        );
        ensure!(
            matches!(row.terminal,Some(Terminal::ProviderAsserted(ref a))
            if a.slot==b.observation.child.slot && a.blockhash==b.observation.child.hash
                && a.transaction_index==claim.transaction_index && a.signature==claim.signature),
            "association_replay_claim_unresolved"
        );
    }
    Ok(())
}
pub(super) fn event(c: &Connection, h: &DurableCheckpoint) -> Result<()> {
    let wire: String = c.query_row(
        "SELECT delivery FROM association_inbox_events WHERE session=?1 AND sequence=?2",
        params![h.session, i64::try_from(h.sequence)?],
        |r| r.get(0),
    )?;
    let d: Delivery = serde_json::from_str(&wire)?;
    ensure!(
        d.session == h.session
            && d.sequence == h.sequence
            && d.event == DeliveryEvent::ParentCheckpoint(h.block.clone())
            && h.from_slot > 0
            && h.from_slot <= h.block.observation.parent.slot,
        "association_replay_event_binding"
    );
    Ok(())
}
fn node(c: &Connection, k: &BlockKey, bytes: usize) -> Result<ParentObservation> {
    let key = serde_json::to_string(k)?;
    let (n, conflict): (usize,bool) = c.query_row(
        "SELECT length(CAST(first_observation AS BLOB)),contradiction IS NOT NULL FROM association_parent_blocks WHERE block_key=?1",
        [&key], |r| Ok((r.get(0)?,r.get(1)?)))
        .optional()?.context("association_replay_parent_gap")?;
    ensure!(
        n <= bytes && !conflict,
        "association_replay_parent_conflict_or_bound"
    );
    let wire: String = c.query_row(
        "SELECT first_observation FROM association_parent_blocks WHERE block_key=?1",
        [&key],
        |r| r.get(0),
    )?;
    let p: ParentObservation = serde_json::from_str(&wire)?;
    ensure!(
        p.child == *k && p.issue.is_none() && p.expected_issue().is_none(),
        "association_replay_parent_invalid"
    );
    for endpoint in [&p.child, &p.parent] {
        let (slot, conflict): (String,Option<String>) = c.query_row(
            "SELECT first_slot,contradiction_slot FROM association_parent_hashes WHERE block_hash=?1",
            [&endpoint.hash], |r| Ok((r.get(0)?,r.get(1)?)))?;
        ensure!(
            slot == endpoint.slot.to_string() && conflict.is_none(),
            "association_replay_hash_conflict"
        );
    }
    Ok(p)
}
pub(super) fn linked(
    c: &Connection,
    later: &BlockKey,
    earlier: &BlockKey,
    limits: InboxLimits,
) -> Result<()> {
    let mut cursor = later.clone();
    let mut remaining = limits.bytes;
    for _ in 0..limits.count {
        if cursor == *earlier {
            return Ok(());
        }
        ensure!(
            cursor.slot > earlier.slot,
            "association_replay_branch_mismatch"
        );
        let p = node(c, &cursor, remaining)?;
        let charge = 512usize
            .checked_add(serde_json::to_vec(&p)?.len())
            .context("association_replay_parent_bound")?;
        remaining = remaining
            .checked_sub(charge)
            .context("association_replay_parent_bound")?;
        cursor = p.parent;
    }
    anyhow::bail!("association_replay_parent_bound")
}
pub(super) fn floor(c: &Connection, b: &BlockCheckpoint, limits: InboxLimits) -> Result<u64> {
    let mut floor = b.observation.parent.slot;
    let mut remaining = limits.bytes;
    let mut statement = c.prepare("SELECT length(CAST(admission AS BLOB)),admission FROM association_inbox_identities WHERE terminal IS NULL")?;
    let mut rows = statement.query([])?;
    let mut count = 0usize;
    while let Some(row) = rows.next()? {
        count += 1;
        ensure!(count <= limits.count, "association_replay_pending_bound");
        let n: usize = row.get(0)?;
        remaining = remaining
            .checked_sub(n + 512)
            .context("association_replay_pending_bound")?;
        let a: AdmissionFacts = serde_json::from_str(&row.get::<_, String>(1)?)?;
        if b.scope.wallets.contains(&a.facts.wallet) {
            floor = floor.min(a.facts.slot);
        }
    }
    ensure!(floor > 0, "association_replay_floor_invalid");
    Ok(floor)
}
pub(super) fn overlap(
    c: &Connection,
    h: &DurableCheckpoint,
    limits: InboxLimits,
) -> Result<Vec<AdmissionFacts>> {
    let mut statement = c.prepare("SELECT length(CAST(admission AS BLOB)),admission FROM association_inbox_identities WHERE CAST(json_extract(admission,'$.facts.slot') AS INTEGER) BETWEEN ?1 AND ?2 ORDER BY signature")?;
    let mut rows = statement.query(params![
        i64::try_from(h.from_slot)?,
        i64::try_from(h.block.observation.child.slot)?
    ])?;
    let mut remaining = limits.bytes;
    let mut count = 0usize;
    let mut overlap = Vec::new();
    while let Some(row) = rows.next()? {
        count += 1;
        ensure!(count <= limits.count, "association_replay_overlap_bound");
        let n: usize = row.get(0)?;
        remaining = remaining
            .checked_sub(n + 512)
            .context("association_replay_overlap_bound")?;
        let a: AdmissionFacts = serde_json::from_str(&row.get::<_, String>(1)?)?;
        ensure!(
            a.facts.slot >= h.from_slot && a.facts.slot <= h.block.observation.child.slot,
            "association_replay_overlap_identity"
        );
        if h.block.scope.wallets.contains(&a.facts.wallet) {
            overlap.push(a);
        }
    }
    Ok(overlap)
}
