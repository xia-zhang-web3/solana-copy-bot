//! Reuses the accepted financial validator in the caller's transaction snapshot.
use super::*;
use crate::{execution_canary_buy_attribution, receipt_facts_rows, ExecutionCanaryBuyAttribution};
use rusqlite::OptionalExtension;

pub(super) struct Financial {
    pub generation: CandidateGeneration,
    pub contributors: Vec<ReceiptAnchor>,
    pub fingerprint: String,
    pub unproven: Vec<String>,
    pub pending: Vec<String>,
}
pub(super) fn generation(c: &Connection, token: &str) -> Result<CandidateGeneration> {
    let mut q = c.prepare("SELECT position_id,opened_ts FROM positions WHERE token=?1 AND state='open' AND accounting_bucket='execution_canary' LIMIT 2")?;
    let rows = q
        .query_map([token], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(match rows.as_slice() {
        [(id, opened)] if !id.is_empty() => CandidateGeneration::AppObserved {
            position_id: id.clone(),
            opened_ts: opened.clone(),
            token: token.into(),
        },
        _ => CandidateGeneration::Unknown,
    })
}
/// The accepted reader validates ledger-wide receipt collisions. Bound its entire
/// input domain first, using the existing inbox count/byte budgets, not a subset
/// of convenient BUYs. At most N+1 financial rows are visited by this preflight.
fn within_budget(c: &Connection, l: InboxLimits) -> Result<bool> {
    let (mut n, mut bytes) = (0usize, 0usize);
    for table in [
        "positions",
        "fills",
        "orders",
        "copy_signals",
        "execution_canary_receipt_proofs",
        "execution_canary_receipt_facts",
    ] {
        let q = c.prepare(&format!("SELECT * FROM {table} LIMIT 0"))?;
        let charge = q
            .column_names()
            .iter()
            .map(|name| format!("coalesce(length(CAST(\"{name}\" AS BLOB)),0)"))
            .collect::<Vec<_>>()
            .join("+");
        let (count, size): (usize, usize) = c.query_row(&format!(
            "SELECT count(*),coalesce(sum(charge),0) FROM (SELECT 512+{charge} AS charge FROM {table} LIMIT ?1)"),
            [i64::try_from(l.count.saturating_sub(n).saturating_add(1))?],
            |r| Ok((r.get(0)?,r.get(1)?)))?;
        n = n.saturating_add(count);
        bytes = bytes.saturating_add(size);
        if n > l.count || bytes > l.bytes {
            return Ok(false);
        }
    }
    Ok(true)
}
pub(super) fn read(c: &Connection, token: &str, l: InboxLimits) -> Result<Option<Financial>> {
    if !within_budget(c, l)? {
        return Ok(None);
    }
    let generation = generation(c, token)?;
    let attr = execution_canary_buy_attribution::read_on_conn(c, token)?;
    let mut contributors = vec![];
    let mut unproven = vec![];
    if let ExecutionCanaryBuyAttribution::Open(a) = &attr {
        for p in &a.proven_contributors {
            // Identity/confirmation/destination/amount validation is the reader above.
            let f = receipt_facts_rows::load(c, &p.order_id)?
                .context("validated receipt disappeared")?;
            let delta = f.token_delta.context("validated token delta disappeared")?;
            contributors.push(ReceiptAnchor {
                contributor: p.clone(),
                slot: f.slot,
                wallet: f.wallet_pubkey.clone(),
                token: f.token.clone(),
                raw: delta.raw.to_string(),
                decimals: delta.decimals,
                receipt_fingerprint: format!("receipt_facts_v1:{f:?}"),
            });
        }
        unproven = a.unproven_links.iter().map(|v| format!("{v:?}")).collect();
    }
    // Missing signals/fills and non-confirmed BUYs remain pending after the status fix.
    let mut q = c.prepare("SELECT o.order_id,o.status,o.tx_signature FROM orders o LEFT JOIN copy_signals s ON s.signal_id=o.signal_id WHERE (s.token=?1 OR s.signal_id IS NULL) AND (lower(s.side)='buy' OR s.signal_id IS NULL) AND (o.status!=?2 OR s.signal_id IS NULL OR NOT EXISTS(SELECT 1 FROM fills f WHERE f.order_id=o.order_id)) ORDER BY o.order_id")?;
    let pending = q
        .query_map(
            rusqlite::params![token, crate::EXECUTION_STATUS_CANARY_CONFIRMED],
            |r| {
                Ok(format!(
                    "{:?}",
                    (
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, Option<String>>(2)?
                    )
                ))
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let fingerprint = serde_json::to_string(&(1u8, &attr, &contributors, &pending))?;
    Ok(Some(Financial {
        generation,
        contributors,
        fingerprint,
        unproven,
        pending,
    }))
}
pub(super) fn source_signature(c: &Connection, signal: &str, wallet: &str, token: &str) -> Result<Option<String>> {
    if let Some(signature) = signal.strip_prefix("native-buy-v1:") {
        if signature.is_empty() || signature.contains(':') || signature.chars().any(char::is_whitespace) {
            return Ok(None);
        }
        let bound: bool = c.query_row(
            "SELECT EXISTS(SELECT 1 FROM native_buy_decisions WHERE signature=?1 AND signal_id=?2 AND wallet=?3 AND mint=?4 AND late=0 AND finalized_at IS NOT NULL AND finalized_slot=source_slot)",
            rusqlite::params![signature,signal,wallet,token],
            |r| r.get(0),
        )?;
        return Ok(bound.then(|| signature.to_owned()));
    }
    let suffix = format!(":{wallet}:buy:{token}");
    let Some(signature) = signal.strip_prefix("shadow:").and_then(|s| s.strip_suffix(&suffix)) else { return Ok(None); };
    Ok((!signature.is_empty()
        && !signature.contains(':')
        && !signature.chars().any(char::is_whitespace))
    .then(|| signature.to_owned()))
}
pub(super) fn first(
    c: &Connection,
    sell: &InboxIdentity,
    fresh: bool,
    observed: Option<(i64, u32)>,
    l: InboxLimits,
) -> Result<FirstBinding> {
    use copybot_core_types::association_delivery::MessageTime;
    let clock = match (&sell.admission.message_time, observed) {
        (_, None) => MessageClockCheck::HistoricalUnknown,
        (MessageTime::Missing, _) => MessageClockCheck::Missing,
        (MessageTime::CreatedAt { seconds, nanos }, Some(at)) => {
            if *nanos >= 1_000_000_000
                || chrono::DateTime::from_timestamp(*seconds, *nanos).is_none()
            {
                MessageClockCheck::Invalid
            } else if (*seconds, *nanos) > at {
                MessageClockCheck::FutureVsAppDequeue
            } else {
                MessageClockCheck::NotFutureVsAppDequeue
            }
        }
        _ => MessageClockCheck::Invalid,
    };
    let mut b = FirstBinding {
        app_dequeue_clock: observed,
        message_clock_check: clock,
        version: 1,
        sell: anchor_identity(sell),
        candidate: sell.candidate.clone(),
        witness: FirstWitness::Unknown(Reason::InitialCandidateUnknown),
        contributors: vec![],
        contributors_fingerprint: None,
        unproven_links: vec![],
        pending_buys: vec![],
    };
    if !fresh {
        b.witness = FirstWitness::Unknown(Reason::HistoricalNoSnapshot);
        return Ok(b);
    }
    if matches!(sell.candidate, CandidateGeneration::Unknown) {
        return Ok(b);
    }
    let token = &sell.admission.facts.token_in;
    if generation(c, token)? != sell.candidate {
        b.witness = FirstWitness::Unknown(Reason::GenerationChanged);
        return Ok(b);
    }
    let Some(f) = read(c, token, l)? else {
        b.witness = FirstWitness::Unknown(Reason::LookupBound);
        return Ok(b);
    };
    b.contributors_fingerprint = Some(f.fingerprint);
    b.contributors = f.contributors;
    b.unproven_links = f.unproven;
    b.pending_buys = f.pending;
    // Stable first fill of this source, never closest-by-time or replaced on retry.
    b.witness = match b.contributors.iter().find(|p| p.contributor.source_wallet == sell.admission.facts.wallet) {
        None => FirstWitness::Unknown(Reason::NoLeaderContributor),
        Some(receipt) => match source_signature(c, &receipt.contributor.signal_id, &receipt.contributor.source_wallet, token)? {
            None => FirstWitness::Unknown(Reason::MalformedSignal),
            Some(source_signature) => FirstWitness::Selected(Witness {
                receipt: receipt.clone(), source_signature,
                durable_source: "validated_fill_position+confirmed_order+canonical_copy_signal+receipt_facts_and_proof".into(),
            }),
        },
    };
    Ok(b)
}
/// Optional retained source facts can contradict the link. Retention absence
/// never invokes time-based search or a different source signature.
pub(super) fn observed_source(
    c: &Connection,
    witness: &Witness,
    a: &AdmissionFacts,
) -> Result<Option<Reason>> {
    type Row = (
        String,
        String,
        String,
        i64,
        Option<String>,
        Option<u8>,
        Option<String>,
        Option<u8>,
        f64,
        f64,
    );
    let row: Option<Row> = c.query_row("SELECT wallet_id,token_in,token_out,slot,qty_in_raw,qty_in_decimals,qty_out_raw,qty_out_decimals,qty_in,qty_out FROM observed_swaps WHERE signature=?1",
        [&witness.source_signature], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?,r.get(9)?))).optional()?;
    let Some((wallet, ti, to, slot, ri, di, ro, do_, qi, qo)) = row else {
        return Ok(None);
    };
    let f = &a.facts;
    if wallet != f.wallet
        || ti != f.token_in
        || to != f.token_out
        || u64::try_from(slot).ok() != Some(f.slot)
        || qi.to_bits() != f.amount_in_bits
        || qo.to_bits() != f.amount_out_bits
    {
        return Ok(Some(Reason::SourceFactsConflict));
    }
    if ri.is_some() || di.is_some() || ro.is_some() || do_.is_some() {
        let Some(e) = &f.exact_amounts else {
            return Ok(Some(Reason::MissingExactAmounts));
        };
        if ri.as_deref() != Some(&e.amount_in_raw)
            || di != Some(e.amount_in_decimals)
            || ro.as_deref() != Some(&e.amount_out_raw)
            || do_ != Some(e.amount_out_decimals)
        {
            return Ok(Some(Reason::SourceFactsConflict));
        }
    }
    Ok(None)
}
