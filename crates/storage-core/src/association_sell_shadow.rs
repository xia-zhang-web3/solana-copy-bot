//! Same-transaction open-lot scan, sharing the evaluation's bounded graph reader.
use super::*;
use crate::{
    association_sell_shadow_types::*,
    shadow_lot_origin::{self, ShadowLotOrigin},
    SHADOW_LOT_OPEN_EPS,
};
use rusqlite::{params, OptionalExtension};

fn unknown(reason: Reason) -> ShadowEvidence {
    ShadowEvidence {
        scan: ShadowScan::Unknown(reason),
        lots: vec![],
    }
}
fn relation(before: &Check, after: &Check) -> ShadowRelation {
    // A failed forward proof is never itself a reverse proof. Equality is a conflict.
    for c in [before, after] {
        if let Check::Blocked(r) = c {
            if !matches!(r, Reason::NonIncreasingIndex | Reason::ParentNotAncestor) {
                return ShadowRelation::Conflict(r.clone());
            }
        }
    }
    match (before, after) {
        (Check::ProviderOrderedWithinBlock | Check::ProviderOrderedAcrossBlocks, _) => {
            ShadowRelation::BeforeSell
        }
        (_, Check::ProviderOrderedWithinBlock | Check::ProviderOrderedAcrossBlocks) => {
            ShadowRelation::AfterSell
        }
        (
            Check::Blocked(Reason::NonIncreasingIndex),
            Check::Blocked(Reason::NonIncreasingIndex),
        ) => ShadowRelation::Conflict(Reason::NonIncreasingIndex),
        (Check::Unknown(r), _) | (_, Check::Unknown(r)) => ShadowRelation::Unknown(r.clone()),
        (Check::Blocked(r), _) => ShadowRelation::Conflict(r.clone()),
    }
}
fn identity(
    o: &ShadowLotOrigin,
    a: &AnchorEvidence,
    sell: &AnchorEvidence,
    b: &FirstBinding,
) -> Option<Check> {
    if o.validate().is_err() {
        return Some(Check::Blocked(Reason::ShadowOriginConflict));
    }
    for anchor in [a, sell] {
        if let Err(c) = order::assertion(anchor) {
            return Some(c);
        }
    }
    let af = &a.identity.as_ref()?.admission.facts;
    let sf = &sell.identity.as_ref()?.admission;
    if sell.identity.as_ref() != Some(&b.sell) {
        return Some(Check::Blocked(Reason::AnchorIdentityChanged));
    }
    if !o.matches(af)
        || o.wallet != b.sell.admission.facts.wallet
        || o.token_out != b.sell.admission.facts.token_in
        || sf.facts.token_out != SOL
    {
        return Some(Check::Blocked(Reason::SourceFactsConflict));
    }
    if !order::exact_valid(sf) {
        return Some(Check::Blocked(Reason::AmountConflict));
    }
    let (Some(buy), Some(sell)) = (&o.exact_amounts, &sf.facts.exact_amounts) else {
        return Some(Check::Unknown(Reason::MissingExactAmounts));
    };
    if buy.amount_out_decimals != sell.amount_in_decimals || sell.amount_out_decimals != 9 {
        return Some(Check::Blocked(Reason::DecimalsConflict));
    }
    None
}
fn anchor_bytes(c: &Connection, sig: &str) -> Result<usize> {
    Ok(c.query_row("SELECT 512+length(CAST(admission AS BLOB))+length(CAST(candidate AS BLOB))+length(CAST(first_session AS BLOB))+coalesce(length(CAST(terminal AS BLOB)),0) FROM association_inbox_identities WHERE signature=?1",
        [sig], |r| r.get(0)).optional()?.unwrap_or(512))
}
pub(super) fn read(
    c: &Connection,
    b: &FirstBinding,
    l: InboxLimits,
    graph: &mut parent_graph::Reader<'_>,
) -> Result<ShadowEvidence> {
    shadow_lot_origin::schema::required(c)?;
    let wallet = &b.sell.admission.facts.wallet;
    let token = &b.sell.admission.facts.token_in;
    // qty range + id ordering uses the 0071 index. At most N+1 relevant open lots
    // are scanned; closed lots and other pairs cannot consume or hide this set.
    let mut q = c.prepare("SELECT l.id,512+length(CAST(l.risk_context AS BLOB))+coalesce(length(CAST(l.qty_raw AS BLOB)),0)+coalesce(length(CAST(o.origin AS BLOB)),0)+coalesce(length(CAST(o.signal_id AS BLOB)),0) FROM shadow_lots l INDEXED BY idx_shadow_lots_pair_qty_id LEFT JOIN shadow_lot_origins o ON o.lot_id=l.id WHERE l.wallet_id=?1 AND l.token=?2 AND l.qty>?3 ORDER BY l.qty,l.id LIMIT ?4")?;
    let mut rows = q.query(params![
        wallet,
        token,
        SHADOW_LOT_OPEN_EPS,
        i64::try_from(l.count.saturating_add(1))?
    ])?;
    let mut ids = vec![];
    let mut bytes = 512usize;
    while let Some(row) = rows.next()? {
        let size: usize = row.get(1)?;
        bytes = bytes.saturating_add(size);
        if ids.len() >= l.count || bytes > l.bytes {
            return Ok(unknown(Reason::LookupBound));
        }
        if let Err(c) = graph.charge(size) {
            return Ok(unknown(match c {
                Check::Unknown(r) => r,
                _ => Reason::LookupBound,
            }));
        }
        ids.push(row.get::<_, i64>(0)?);
    }
    let sell = evaluate::anchor(c, &b.sell.admission.facts.signature)?;
    let mut result = ShadowEvidence {
        scan: ShadowScan::Complete,
        lots: vec![],
    };
    let mut output_bytes = 512usize;
    for id in ids {
        let (qty, raw, decimals, risk, signal, wire): (f64,Option<String>,Option<u8>,String,Option<String>,Option<String>) = c.query_row(
            "SELECT l.qty,l.qty_raw,l.qty_decimals,l.risk_context,o.signal_id,o.origin FROM shadow_lots l LEFT JOIN shadow_lot_origins o ON o.lot_id=l.id WHERE l.id=?1", [id],
            |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)))?;
        let mut e = ShadowLotEvidence {
            lot_id: id,
            qty_bits: qty.to_bits(),
            qty_raw: raw,
            qty_decimals: decimals,
            risk_context: risk,
            origin: None,
            anchor: None,
            origin_to_sell: Check::Unknown(Reason::MissingShadowOrigin),
            sell_to_origin: Check::Unknown(Reason::MissingShadowOrigin),
            relation: ShadowRelation::Unknown(Reason::MissingShadowOrigin),
        };
        if let Some(wire) = wire {
            match serde_json::from_str::<ShadowLotOrigin>(&wire) {
                Ok(o) if o.validate().is_ok() && signal.as_ref() == Some(&o.signal_id) => {
                    let size = anchor_bytes(c, &o.signature)?;
                    bytes = bytes.saturating_add(size);
                    if bytes > l.bytes {
                        return Ok(unknown(Reason::LookupBound));
                    }
                    if let Err(Check::Unknown(r)) = graph.charge(size) {
                        return Ok(unknown(r));
                    }
                    let a = evaluate::anchor(c, &o.signature)?;
                    if let Some(check) = identity(&o, &a, &sell, b) {
                        e.origin_to_sell = check.clone();
                        e.sell_to_origin = check;
                    } else {
                        e.origin_to_sell = order::ordered(&a, &sell, graph)?;
                        e.sell_to_origin = order::ordered(&sell, &a, graph)?;
                    }
                    e.origin = Some(o);
                    e.anchor = Some(a);
                }
                _ => {
                    e.origin_to_sell = Check::Blocked(Reason::ShadowOriginConflict);
                    e.sell_to_origin = e.origin_to_sell.clone();
                }
            }
            e.relation = relation(&e.origin_to_sell, &e.sell_to_origin);
        }
        output_bytes = output_bytes.saturating_add(serde_json::to_vec(&e)?.len());
        if output_bytes > l.bytes {
            return Ok(unknown(Reason::LookupBound));
        }
        result.lots.push(e);
    }
    Ok(result)
}
