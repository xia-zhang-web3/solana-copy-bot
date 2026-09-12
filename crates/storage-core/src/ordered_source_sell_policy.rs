//! Opt-in stricter provider order policy over accepted95 evidence, not network history.
use super::*;
use crate::association_sell_preparation::{
    Check, Evaluation, FirstBinding, FirstWitness, ValidatedPreparation,
};
use crate::association_sell_shadow_types::{ShadowRelation, ShadowScan as Scan};
use copybot_core_types::association_delivery::CandidateGeneration;
use rusqlite::Connection;

fn positive(c: &Check) -> bool {
    matches!(
        c,
        Check::ProviderOrderedWithinBlock | Check::ProviderOrderedAcrossBlocks
    )
}
pub(crate) fn check(c: &Connection, p: &ValidatedPreparation) -> Result<OrderedSellDecision> {
    use OrderedSellDecision::*;
    use OrderedSellReason::*;
    let b = &p.first;
    let e = &p.current;
    let CandidateGeneration::AppObserved {
        position_id, token, ..
    } = &b.candidate
    else {
        return Ok(Unknown(FirstGenerationUnknown));
    };
    if !positive(&e.selected_chain) {
        return Ok(match &e.selected_chain {
            Check::Unknown(_) => Unknown(SelectedChain(e.selected_chain.clone())),
            _ => Blocked(SelectedChain(e.selected_chain.clone())),
        });
    }
    let Some(position) =
        crate::execution_canary_position_open::load_open_position_by_token(c, token)?
    else {
        return Ok(Blocked(NoPositivePosition));
    };
    if position.position_id != *position_id
        || !position.qty.is_finite()
        || position.qty <= 1e-12
        || position.qty_exact.is_some_and(|q| q.raw() == 0)
    {
        return Ok(Blocked(NoPositivePosition));
    }
    evidence(&p.first, &p.current)
}
pub(super) fn evidence(b: &FirstBinding, e: &Evaluation) -> Result<OrderedSellDecision> {
    use OrderedSellDecision::*;
    use OrderedSellReason::*;
    if !matches!(b.candidate, CandidateGeneration::AppObserved { .. }) {
        return Ok(Unknown(FirstGenerationUnknown));
    }
    if !positive(&e.selected_chain) {
        return Ok(match &e.selected_chain {
            Check::Unknown(_) => Unknown(SelectedChain(e.selected_chain.clone())),
            _ => Blocked(SelectedChain(e.selected_chain.clone())),
        });
    }
    if b.contributors_fingerprint.is_none() || e.contributors_fingerprint.is_none() {
        return Ok(Unknown(MissingFinancialSet));
    }
    if b.contributors.is_empty() || e.current_contributors.is_empty() {
        return Ok(Unknown(EmptyContributors));
    }
    let FirstWitness::Selected(w) = &b.witness else {
        return Ok(Unknown(MissingFinancialSet));
    };
    if b.contributors != e.current_contributors
        || b.contributors_fingerprint != e.contributors_fingerprint
        || !e.current_contributors.contains(&w.receipt)
        || e.contributor_orders.len() != e.current_contributors.len()
    {
        return Ok(Blocked(ContributorSetMismatch));
    }
    if !e.unproven_links.is_empty() {
        return Ok(Blocked(UnprovenLinks));
    }
    if !e.pending_buys.is_empty() {
        return Ok(Blocked(PendingBuys));
    }
    // Check every validated local contributor, never just the selected receipt.
    for (r, o) in e.current_contributors.iter().zip(&e.contributor_orders) {
        if r.contributor.order_id != o.order_id || r.contributor.tx_signature != o.receipt_signature
        {
            return Ok(Blocked(ContributorSetMismatch));
        }
        if !positive(&o.relative_to_sell) {
            let reason = ContributorOrder {
                order_id: o.order_id.clone(),
                check: o.relative_to_sell.clone(),
            };
            return Ok(if matches!(o.relative_to_sell, Check::Unknown(_)) {
                Unknown(reason)
            } else {
                Blocked(reason)
            });
        }
    }
    let Some(shadow) = &e.shadow else {
        return Ok(Unknown(OrderedSellReason::ShadowScan(
            crate::association_sell_preparation::Reason::HistoricalNoSnapshot,
        )));
    };
    if let Scan::Unknown(r) = &shadow.scan {
        return Ok(Unknown(OrderedSellReason::ShadowScan(r.clone())));
    }
    for lot in &shadow.lots {
        if lot.relation != ShadowRelation::AfterSell {
            let reason = ShadowLot {
                lot_id: lot.lot_id,
                relation: lot.relation.clone(),
            };
            return Ok(if matches!(lot.relation, ShadowRelation::Unknown(_)) {
                Unknown(reason)
            } else {
                Blocked(reason)
            });
        }
    }
    Ok(ValidatedNow)
}
