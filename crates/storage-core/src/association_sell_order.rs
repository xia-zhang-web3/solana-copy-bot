//! Only provider-asserted within-block order. No fork ancestry is inferred.
use super::*;
use copybot_core_types::association_delivery::{ProviderAssertion, Terminal};

pub(super) fn assertion(a: &AnchorEvidence) -> std::result::Result<&ProviderAssertion, Check> {
    if a.conflict {
        return Err(Check::Blocked(Reason::AnchorConflict));
    }
    if a.recovery {
        return Err(Check::Unknown(Reason::Recovery));
    }
    let identity = a
        .identity
        .as_ref()
        .ok_or(Check::Unknown(Reason::MissingAnchor))?;
    match &a.terminal {
        None => Err(Check::Unknown(Reason::MissingTerminal)),
        Some(Terminal::Unresolved(_)) => Err(Check::Unknown(Reason::UnresolvedTerminal)),
        Some(Terminal::ProviderAsserted(p)) => {
            if p.signature != a.signature
                || identity.admission.facts.signature != a.signature
                || p.slot != identity.admission.facts.slot
            {
                return Err(Check::Blocked(Reason::IdentityConflict));
            }
            if p.blockhash.trim().is_empty() {
                return Err(Check::Unknown(Reason::EmptyBlockhash));
            }
            Ok(p)
        }
    }
}
pub(super) fn ordered(
    a: &AnchorEvidence,
    b: &AnchorEvidence,
    graph: &mut parent_graph::Reader<'_>,
) -> Result<Check> {
    let first = match assertion(a) {
        Ok(a) => a,
        Err(e) => return Ok(e),
    };
    let second = match assertion(b) {
        Ok(a) => a,
        Err(e) => return Ok(e),
    };
    graph.ordered(first, second)
}
fn positive(check: &Check) -> bool {
    matches!(
        check,
        Check::ProviderOrderedWithinBlock | Check::ProviderOrderedAcrossBlocks
    )
}

pub(super) fn exact_valid(a: &AdmissionFacts) -> bool {
    a.facts.exact_amounts.as_ref().is_none_or(|e| {
        [&e.amount_in_raw, &e.amount_out_raw]
            .into_iter()
            .all(|raw| {
                raw.parse::<u128>()
                    .is_ok_and(|v| v > 0 && v.to_string() == *raw)
            })
            && e.amount_in_decimals <= 38
            && e.amount_out_decimals <= 38
    })
}
pub(super) fn receipt_identity(r: &ReceiptAnchor, a: &AnchorEvidence) -> Option<Check> {
    let f = match &a.identity {
        Some(i) => &i.admission.facts,
        None => return Some(Check::Unknown(Reason::MissingAnchor)),
    };
    if f.signature != r.contributor.tx_signature
        || f.slot != r.slot
        || f.wallet != r.wallet
        || f.token_in != SOL
        || f.token_out != r.token
    {
        return Some(Check::Blocked(Reason::IdentityConflict));
    }
    let Some(e) = &f.exact_amounts else {
        return Some(Check::Unknown(Reason::MissingExactAmounts));
    };
    if !exact_valid(&a.identity.as_ref().unwrap().admission)
        || e.amount_out_raw != r.raw
        || e.amount_out_decimals != r.decimals
        || e.amount_in_decimals != 9
    {
        return Some(Check::Blocked(Reason::AmountConflict));
    }
    // Receipt native delta includes fees/rent. Never compare it with swap input.
    None
}
/// Per-contributor order uses the same SELL units as the selected chain.
pub(super) fn receipt_sell_order(
    r: &ReceiptAnchor,
    our: &AnchorEvidence,
    sell: &AnchorEvidence,
    graph: &mut parent_graph::Reader<'_>,
) -> Result<Check> {
    if let Some(check) = receipt_identity(r, our) {
        return Ok(check);
    }
    let Some(identity) = &sell.identity else {
        return Ok(Check::Unknown(Reason::MissingAnchor));
    };
    let facts = &identity.admission.facts;
    if facts.token_in != r.token || facts.token_out != SOL {
        return Ok(Check::Blocked(Reason::IdentityConflict));
    }
    if !exact_valid(&identity.admission) {
        return Ok(Check::Blocked(Reason::AmountConflict));
    }
    let Some(e) = &facts.exact_amounts else {
        return Ok(Check::Unknown(Reason::MissingExactAmounts));
    };
    if e.amount_in_decimals != r.decimals || e.amount_out_decimals != 9 {
        return Ok(Check::Blocked(Reason::DecimalsConflict));
    }
    ordered(our, sell, graph)
}
pub(super) fn chain(
    c: &Connection,
    b: &FirstBinding,
    w: &Witness,
    anchors: &[AnchorEvidence],
    graph: &mut parent_graph::Reader<'_>,
) -> Result<Check> {
    let get = |s: &str| {
        anchors
            .iter()
            .find(|a| a.signature == s)
            .context("missing preparation dependency")
    };
    let sell = get(&b.sell.admission.facts.signature)?;
    let source = get(&w.source_signature)?;
    let our = get(&w.receipt.contributor.tx_signature)?;
    if source.signature == our.signature
        || source.signature == sell.signature
        || our.signature == sell.signature
    {
        return Ok(Check::Blocked(Reason::SignatureSubstitution));
    }
    for a in [source, our, sell] {
        if let Err(check) = assertion(a) {
            return Ok(check);
        }
    }
    let source_facts = &source.identity.as_ref().unwrap().admission;
    let sf = &source_facts.facts;
    let sell_facts = &sell.identity.as_ref().unwrap().admission.facts;
    if sf.wallet != w.receipt.contributor.source_wallet
        || sf.wallet != sell_facts.wallet
        || sf.token_in != SOL
        || sf.token_out != w.receipt.token
        || sell_facts.token_in != w.receipt.token
        || sell_facts.token_out != SOL
    {
        return Ok(Check::Blocked(Reason::IdentityConflict));
    }
    if !exact_valid(source_facts) || !exact_valid(&sell.identity.as_ref().unwrap().admission) {
        return Ok(Check::Blocked(Reason::AmountConflict));
    }
    // These are different swaps: compare mint units, never leader/bot quantities.
    let mut missing = false;
    for (facts, buy) in [
        (source_facts, true),
        (&sell.identity.as_ref().unwrap().admission, false),
    ] {
        let Some(e) = &facts.facts.exact_amounts else {
            missing = true;
            continue;
        };
        let (token_decimals, sol_decimals) = if buy {
            (e.amount_out_decimals, e.amount_in_decimals)
        } else {
            (e.amount_in_decimals, e.amount_out_decimals)
        };
        if token_decimals != w.receipt.decimals || sol_decimals != 9 {
            return Ok(Check::Blocked(Reason::DecimalsConflict));
        }
    }
    if missing {
        return Ok(Check::Unknown(Reason::MissingExactAmounts));
    }
    if let Some(check) = receipt_identity(&w.receipt, our) {
        return Ok(check);
    }
    if let Some(reason) = financial::observed_source(c, w, source_facts)? {
        return Ok(Check::Blocked(reason));
    }
    let first = ordered(source, our, graph)?;
    if !positive(&first) {
        return Ok(first);
    }
    let second = ordered(our, sell, graph)?;
    if !positive(&second) {
        return Ok(second);
    }
    Ok(
        if first == Check::ProviderOrderedAcrossBlocks
            || second == Check::ProviderOrderedAcrossBlocks
        {
            Check::ProviderOrderedAcrossBlocks
        } else {
            Check::ProviderOrderedWithinBlock
        },
    )
}
