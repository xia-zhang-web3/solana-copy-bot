//! One caller-owned SQLite snapshot; never opens/reinitializes the ingestion inbox.
use super::*;
use crate::{association_sell_preparation::FirstWitness, ordered_source_sell as ordered};
use anyhow::{ensure, Context};
use copybot_core_types::association_delivery::CandidateGeneration;
use rusqlite::Connection;

pub(super) fn read(
    c: &Connection,
    id: &str,
    l: InboxLimits,
    endpoint: &str,
) -> Result<std::result::Result<QuoteBinding, String>> {
    ordered::schema::required(c)?;
    let Some(i) = ordered::rows::load(c, id)? else {
        return Ok(Err("missing_intent".into()));
    };
    let signature = &i.first.sell.admission.facts.signature;
    let Some(p) = crate::association_sell_preparation::on_connection(c, signature, l)? else {
        return Ok(Err("missing_preparation".into()));
    };
    if p.first != i.first || ordered::ownership::legacy_exists(c, signature)? {
        return Ok(Err("first_or_ownership_changed".into()));
    }
    let decision = ordered::policy::check(c, &p)?;
    if decision != ordered::OrderedSellDecision::ValidatedNow {
        return Ok(Err(format!("strict_policy:{decision:?}")));
    }
    let CandidateGeneration::AppObserved {
        position_id,
        opened_ts,
        token,
    } = &i.first.candidate
    else {
        return Ok(Err("generation_unknown".into()));
    };
    let position = crate::execution_canary_position_open::load_open_position_by_token(c, token)?
        .context("validated strict position disappeared")?;
    let Some(q) = position.qty_exact.filter(|q| q.raw() > 0) else {
        return Ok(Err("owned_raw_or_decimals_unknown".into()));
    };
    if position.position_id != *position_id
        || p.current
            .current_contributors
            .iter()
            .any(|r| r.decimals != q.decimals())
    {
        return Ok(Err("owned_generation_or_decimals_conflict".into()));
    }
    let FirstWitness::Selected(w) = &i.first.witness else {
        return Ok(Err("source_unknown".into()));
    };
    // Do not copy protobuf payloads. These canonical first/pinned references have
    // already been checked against their current complete identity by the evaluator.
    let anchors = p
        .current
        .anchors
        .iter()
        .map(|a| {
            (
                &a.signature,
                a.identity
                    .as_ref()
                    .map(|i| (&i.first_session, i.first_sequence)),
                &a.terminal,
                a.conflict,
                a.recovery,
            )
        })
        .collect::<Vec<_>>();
    let snapshot_version = serde_json::to_string(&(
        "strict_quote_snapshot_v1",
        &i.first.candidate,
        &i.staged_at,
        &p.current.selected_chain,
        &p.current.contributors_fingerprint,
        &p.current.contributor_orders,
        &p.current.shadow,
        &p.current.pending_buys,
        &p.current.unproven_links,
        &p.current.parent_paths,
        anchors,
    ))?;
    ensure!(
        snapshot_version.len() <= 120_000,
        "strict quote snapshot budget"
    );
    Ok(Ok(QuoteBinding {
        version: 1,
        intent_id: id.into(),
        policy: i.policy,
        position_id: position_id.clone(),
        position_opened_ts: opened_ts.clone(),
        source_signature: w.source_signature.clone(),
        source_wallet: w.receipt.contributor.source_wallet.clone(),
        mint: token.clone(),
        output_mint: SOL.into(),
        side: "sell".into(),
        provider: crate::PROVIDER_GENERIC_METIS.into(),
        endpoint: endpoint.into(),
        raw: q.raw(),
        decimals: q.decimals(),
        snapshot_version,
    }))
}
