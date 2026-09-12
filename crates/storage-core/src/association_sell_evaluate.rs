use super::*;
use rusqlite::{params, OptionalExtension};

pub(super) fn anchor(c: &Connection, signature: &str) -> Result<AnchorEvidence> {
    let i = crate::association_inbox::identity(c, signature)?;
    Ok(AnchorEvidence {
        signature: signature.into(),
        identity: i.as_ref().map(anchor_identity),
        terminal: i.as_ref().and_then(|i| i.terminal.clone()),
        conflict: i.as_ref().is_some_and(|i| i.conflict),
        recovery: i.as_ref().is_some_and(|i| i.recovery),
    })
}
pub(super) fn keys(b: &FirstBinding) -> Vec<String> {
    let mut keys = std::collections::BTreeSet::new();
    keys.insert(b.sell.admission.facts.signature.clone());
    if let FirstWitness::Selected(w) = &b.witness {
        keys.insert(w.source_signature.clone());
    }
    keys.extend(
        b.contributors
            .iter()
            .map(|r| r.contributor.tx_signature.clone()),
    );
    keys.into_iter().collect()
}
pub(super) fn evaluate(c: &Connection, b: &FirstBinding, l: InboxLimits) -> Result<Evaluation> {
    let mut graph = parent_graph::Reader::new(c, l)?;
    let mut e = Evaluation {
        shadow: None,
        parent_paths: vec![],
        parent_dependencies: vec![],
        selected_chain: Check::Unknown(Reason::InitialCandidateUnknown),
        anchors: vec![],
        contributor_orders: vec![],
        current_contributors: b.contributors.clone(),
        contributors_fingerprint: None,
        unproven_links: b.unproven_links.clone(),
        pending_buys: b.pending_buys.clone(),
        limitations: [
            "canonical_finalized_fork_unknown",
            "history_is_at_most_proven_subset",
            "remaining_ownership_unknown",
            "fifo_unknown",
            "sell_after_all_buys_not_proven",
            "message_time_is_not_transaction_time",
            "unapplied_pending_buys_not_owned",
        ]
        .map(str::to_owned)
        .to_vec(),
        trade_authority: "trade_authority_none".into(),
    };
    let signature = &b.sell.admission.facts.signature;
    let mut changed = false;
    for key in keys(b) {
        let a = anchor(c, &key)?;
        let pinned: Option<Option<String>> = c.query_row(
            "SELECT first_identity FROM association_sell_dependencies WHERE sell_signature=?1 AND anchor_signature=?2",
            params![signature,key], |r|r.get(0)).optional()?;
        let pinned = pinned.context("missing SELL dependency row")?;
        if let Some(pinned) = pinned {
            let pinned: AnchorIdentity = serde_json::from_str(&pinned)?;
            if a.identity.as_ref() != Some(&pinned) {
                changed = true;
            }
        }
        e.anchors.push(a);
    }
    let sell = e
        .anchors
        .iter()
        .find(|a| &a.signature == signature)
        .context("missing SELL anchor")?;
    if sell.identity.as_ref() != Some(&b.sell) {
        changed = true;
    }
    let mut invalidation = changed.then_some(Check::Blocked(Reason::AnchorIdentityChanged));
    if b.contributors_fingerprint.is_some() {
        let current = financial::read(c, &b.sell.admission.facts.token_in, l)?;
        match current {
            None => invalidation = Some(Check::Unknown(Reason::LookupBound)),
            Some(f) => {
                if f.generation != b.candidate {
                    invalidation = Some(Check::Blocked(Reason::GenerationChanged));
                } else if Some(&f.fingerprint) != b.contributors_fingerprint.as_ref() {
                    invalidation = Some(Check::Blocked(Reason::FinancialSetChanged));
                }
                e.current_contributors = f.contributors;
                e.contributors_fingerprint = Some(f.fingerprint);
                e.unproven_links = f.unproven;
                e.pending_buys = f.pending;
            }
        }
    }
    // New financial contributors remain visible even though they invalidate the
    // first snapshot. Reading their anchors does not bind them as a new witness.
    for r in &e.current_contributors {
        if !e
            .anchors
            .iter()
            .any(|a| a.signature == r.contributor.tx_signature)
        {
            e.anchors.push(anchor(c, &r.contributor.tx_signature)?);
        }
    }
    let sell = e
        .anchors
        .iter()
        .find(|a| &a.signature == signature)
        .context("missing SELL anchor")?;
    for r in &e.current_contributors {
        let a = e
            .anchors
            .iter()
            .find(|a| a.signature == r.contributor.tx_signature)
            .context("missing contributor anchor")?;
        let check = order::receipt_sell_order(r, a, sell, &mut graph)?;
        e.contributor_orders.push(ContributorOrder {
            order_id: r.contributor.order_id.clone(),
            receipt_signature: r.contributor.tx_signature.clone(),
            relative_to_sell: check,
        });
    }
    // Never select a new witness here, including Unknown-first preparations.
    e.selected_chain = match &b.witness {
        FirstWitness::Unknown(reason) => Check::Unknown(reason.clone()),
        FirstWitness::Selected(w) => order::chain(c, b, w, &e.anchors, &mut graph)?,
    };
    if e.anchors.iter().any(|a| a.conflict) {
        invalidation = Some(Check::Blocked(Reason::AnchorConflict));
    }
    if let Some(check) = invalidation {
        e.selected_chain = check;
    }
    e.shadow = Some(shadow::read(c, b, l, &mut graph)?);
    (e.parent_paths, e.parent_dependencies) = graph.finish();
    Ok(e)
}
