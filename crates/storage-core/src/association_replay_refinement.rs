//! Session-local UNKNOWN stays immutable. Only a committed full-block replay
//! supplies an additional provider observation for continuity and owned SELL.
use super::*;
use crate::association_inbox::{InboxIdentity, InboxLimits};
use copybot_core_types::association_delivery::*;
use copybot_core_types::association_recovery::CheckpointClaim;

fn reset(row: &InboxIdentity) -> bool {
    !row.conflict
        && !row.recovery
        && row.terminal == Some(Terminal::Unresolved(Unresolved::SessionReset))
}
fn terminals(c: &Connection, signature: &str, l: InboxLimits) -> Result<Vec<Delivery>> {
    let mut q = c.prepare("SELECT delivery FROM association_inbox_events WHERE json_extract(delivery,'$.event.Terminal.signature')=?1 ORDER BY session,sequence")?;
    let mut rows = q.query([signature])?;
    let mut out = vec![];
    let mut bytes = l.bytes;
    while let Some(row) = rows.next()? {
        let wire: String = row.get(0)?;
        bytes = bytes
            .checked_sub(wire.len() + 512)
            .context("association_reset_evidence_bound")?;
        ensure!(out.len() < l.count, "association_reset_evidence_bound");
        out.push(serde_json::from_str(&wire)?);
    }
    Ok(out)
}
fn recovered(d: &Delivery) -> Option<(&AdmissionFacts, &ProviderAssertion)> {
    let DeliveryEvent::Terminal {
        signature,
        expected,
        result: Terminal::ProviderAsserted(a),
    } = &d.event
    else {
        return None;
    };
    if signature != &a.signature || signature != &expected.facts.signature {
        return None;
    }
    let MessageTime::RecoveredBlock { block_time } = expected.message_time else {
        return None;
    };
    let time = match block_time {
        Some(t) => BlockTime::Seconds(t),
        None => BlockTime::Missing,
    };
    (a.slot == expected.facts.slot && a.block_time == time).then_some((expected, a))
}
/// Deferral is not resolution: first terminal, native BUY eligibility and cursor
/// remain unchanged. Conflicting provider assertions are never deferred.
pub(crate) fn defer(
    c: &Connection,
    d: &Delivery,
    row: &InboxIdentity,
    l: InboxLimits,
) -> Result<bool> {
    if !reset(row) || row.first_session == d.session || !schema::available(c)? {
        return Ok(false);
    }
    let Some((expected, a)) = recovered(d) else {
        return Ok(false);
    };
    if expected.facts != row.admission.facts || expected.info != row.admission.info {
        return Ok(false);
    }
    let head: Option<String> = c
        .query_row(
            "SELECT head FROM association_replay_cursor WHERE id=1",
            [],
            |r| r.get(0),
        )
        .optional()?
        .flatten();
    let Some(head) = head else {
        return Ok(false);
    };
    let head: DurableCheckpoint = serde_json::from_str(&head)?;
    let head = read(c, &head.block.scope, l.bytes)?.context("association_reset_cursor_missing")?;
    validate::event(c, &head)?;
    if !head.block.scope.wallets.contains(&expected.facts.wallet) {
        return Ok(false);
    }
    for prior in terminals(c, &a.signature, l)? {
        if let DeliveryEvent::Terminal {
            result: Terminal::ProviderAsserted(old),
            ..
        } = prior.event
        {
            if old != *a {
                return Ok(false);
            }
        }
    }
    Ok(true)
}
pub(super) fn claim(
    c: &Connection,
    d: &Delivery,
    b: &BlockCheckpoint,
    claim: &CheckpointClaim,
    row: &InboxIdentity,
    l: InboxLimits,
) -> Result<Option<ProviderAssertion>> {
    if !reset(row) {
        return Ok(None);
    }
    ensure!(
        d.event == DeliveryEvent::ParentCheckpoint(b.clone()),
        "association_reset_checkpoint_binding"
    );
    // A caller-supplied assertion cannot replace a durable replay origin.
    let prior = read(c, &b.scope, l.bytes)?.context("association_reset_cursor_missing")?;
    validate::event(c, &prior)?;
    // SessionReset itself is durable. A crash may precede the separate Reset
    // marker; the new Started session and linked checkpoint below are required.
    for terminal in terminals(c, &claim.signature, l)? {
        if terminal.session != d.session
            || terminal.sequence >= d.sequence
            || terminal.session == row.first_session
        {
            continue;
        }
        let Some((expected, a)) = recovered(&terminal) else {
            continue;
        };
        if expected.facts != row.admission.facts
            || expected.info != row.admission.info
            || a.slot != b.observation.child.slot
            || a.blockhash != b.observation.child.hash
            || a.transaction_index != claim.transaction_index
            || a.signature != claim.signature
        {
            continue;
        }
        let started: bool = c.query_row("SELECT EXISTS(SELECT 1 FROM association_inbox_events WHERE session=?1 AND sequence<?2 AND json_extract(delivery,'$.event.Session')='StartedContinuityUnknown')",params![d.session,i64::try_from(terminal.sequence)?],|r|r.get(0))?;
        let admitted: bool = c.query_row("SELECT EXISTS(SELECT 1 FROM association_inbox_events WHERE session=?1 AND sequence<?2 AND json_extract(delivery,'$.event.Admission')=json(?3))",params![d.session,i64::try_from(terminal.sequence)?,serde_json::to_string(expected)?],|r|r.get(0))?;
        if started && admitted {
            return Ok(Some(a.clone()));
        }
    }
    Ok(None)
}
/// This view is deliberately NOT used by native BUY or public first identity.
/// It is recomputed from immutable events and the current durable parent chain.
pub(crate) fn observed(
    c: &Connection,
    row: &InboxIdentity,
    l: InboxLimits,
) -> Result<Option<Terminal>> {
    if !reset(row) || !schema::available(c)? {
        return Ok(row.terminal.clone());
    }
    let mut q = c.prepare("SELECT delivery FROM association_inbox_events WHERE json_extract(delivery,'$.event.ParentCheckpoint.observation.child.slot')=?1 ORDER BY session,sequence")?;
    let mut rows = q.query([i64::try_from(row.admission.facts.slot)?])?;
    let mut remaining = l.bytes;
    let mut count = 0;
    while let Some(r) = rows.next()? {
        let wire: String = r.get(0)?;
        remaining = remaining
            .checked_sub(wire.len() + 512)
            .context("association_reset_evidence_bound")?;
        count += 1;
        ensure!(count <= l.count, "association_reset_evidence_bound");
        let d: Delivery = serde_json::from_str(&wire)?;
        let DeliveryEvent::ParentCheckpoint(ref b) = d.event else {
            continue;
        };
        let Some(claimed) = b
            .claims
            .iter()
            .find(|a| a.signature == row.admission.facts.signature)
        else {
            continue;
        };
        let Some(head) = read(c, &b.scope, l.bytes)? else {
            continue;
        };
        validate::event(c, &head)?;
        validate::linked(c, &head.block.observation.child, &b.observation.child, l)?;
        validate::block(c, b, l, &d)?;
        if let Some(a) = claim(c, &d, b, claimed, row, l)? {
            return Ok(Some(Terminal::ProviderAsserted(a)));
        }
    }
    Ok(row.terminal.clone())
}
