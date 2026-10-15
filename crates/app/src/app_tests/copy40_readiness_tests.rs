//! Actual saved40 Rust/SQLite path; provider/quote/handoff clocks explicitly MOCK.
use super::{
    copy40_readiness_cases::{RawSaved, SyntheticClock},
    copy40_readiness_fixture::{start, Case, BOT, H, SELL},
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage_core::{ordered_sell_quote::*, SqliteStore};
use serde_json::json;

async fn bind(
    case: &mut Case,
    claim: QuoteClaim,
    rpc: &mut RawSaved,
    clock: SyntheticClock,
) -> Result<QuoteClaim> {
    crate::execution_owned_sell_rpc::fractional::bind_transport_with_clock(
        &mut case.store,
        &case.config,
        claim,
        case.limits,
        rpc,
        clock,
    )
    .await
}
fn claim_identity(case: &Case) -> Result<(u64, String, Option<String>)> {
    Ok(case.sql.query_row(
        "SELECT attempt,owner,lease_until FROM ordered_sell_quote_results",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?)
}
fn mock_quote_handoff(case: &mut Case, claim: &QuoteClaim, now: DateTime<Utc>) -> Result<()> {
    let b = &claim.binding;
    let body = json!({"inputMint":b.mint,"outputMint":b.output_mint,"inAmount":H.to_string(),
        "outAmount":"9900000","otherAmountThreshold":"9850500","swapMode":"ExactIn","slippageBps":50,
        "routePlan":[{"percent":100,"swapInfo":{"label":"Raydium","ammKey":"11111111111111111111111111111111",
            "inputMint":b.mint,"outputMint":b.output_mint,"inAmount":H.to_string(),"outAmount":"9900000"}}]});
    crate::execution_quote_http::cohort::validate(
        &body,
        &b.mint,
        &b.output_mint,
        &H.to_string(),
        50,
    )?;
    let sample = crate::execution_quote_http::quote_sample_from_json(body.clone())?;
    assert_eq!(sample.in_amount, H.to_string());
    let quote = QuoteObservation {
        version: 1,
        binding: Some(b.clone()),
        outcome: QuoteOutcome::Current,
        reason: None,
        http_started: Some(now),
        http_response: Some(now),
        quote_response_available_ts: Some(now),
        http_ended: now,
        response_in_raw: Some(H.to_string()),
        response_out_raw: Some("9900000".into()),
        response_sha256: Some(crate::execution_owned_sell_rpc::digest(body.to_string())),
        event_time: None,
        event_delay_ns: None,
    };
    let quote = case
        .store
        .complete_strict_sell_quote(claim, case.limits, quote, || now)?;
    assert_eq!(quote.outcome, QuoteOutcome::Current);
    let snapshot = case.store.owned_sell_snapshot(b, case.limits)?;
    assert_eq!(snapshot.sell.facts.signature, SELL);
    assert_eq!(snapshot.facts.len(), 1);
    assert_eq!(
        snapshot.facts[0].token_delta.as_ref().unwrap().raw,
        i128::from(H)
    );
    let handoff = case.store.reserve_owned_sell_handoff(
        &snapshot,
        &quote,
        case.limits,
        &crate::execution_owned_sell_rpc::identity(&case.config)?,
        "EXPLICIT OFFLINE SYNTHETIC MOCK finalized quote authority; no signer/send",
        case.config.tiny_experiment.id.as_deref().unwrap(),
        BOT,
        || now,
    )?;
    case.store
        .recheck_owned_sell_handoff(&handoff, case.limits, now)?;
    assert!(case
        .store
        .reserve_owned_sell_handoff(
            &snapshot,
            &quote,
            case.limits,
            &crate::execution_owned_sell_rpc::identity(&case.config)?,
            "duplicate MOCK",
            case.config.tiny_experiment.id.as_deref().unwrap(),
            BOT,
            || now
        )
        .is_err());
    assert_eq!(case.sell_dispatches()?, 0);
    Ok(())
}

#[tokio::test]
#[ignore = "explicit hash-bound private40 fixture; no provider I/O"]
async fn copy40_readiness_saved_raw_sqlite_same_claim_to_mock_quote_handoff() -> Result<()> {
    let mut case = Case::new(false)?;
    let original = case.claim()?;
    let before = claim_identity(&case)?;
    let mut raw = RawSaved::new(&case, "positive");
    let clock = SyntheticClock::new(&case, "positive");
    let now = clock.now.clone();
    let claim = bind(&mut case, original.clone(), &mut raw, clock).await?;
    assert_eq!(claim.owner, original.owner);
    assert_eq!(claim.attempt, original.attempt);
    assert_eq!(claim.lease_until, original.lease_until);
    assert_eq!(claim_identity(&case)?, before);
    assert_eq!(
        *now.lock().unwrap(),
        start() + chrono::Duration::seconds(12)
    );
    assert_eq!(raw.calls.len(), 12);
    assert_eq!(raw.observations, 3);
    assert!(raw.decision_ids.iter().all(|id| id == &raw.decision_ids[0]));
    let proof = claim.binding.fractional.as_ref().unwrap();
    assert_eq!(proof.inventory.target_index, 260);
    assert_eq!(proof.inventory.numerator, 1041512);
    assert_eq!(proof.inventory.denominator, "1041512");
    assert_eq!(claim.binding.raw, H);
    assert_eq!(
        proof
            .protected_execution_baseline
            .as_ref()
            .unwrap()
            .amount_raw,
        100
    );
    let state: String =
        case.sql
            .query_row("SELECT state FROM fractional_sell_decisions", [], |r| {
                r.get(0)
            })?;
    assert_eq!(state, "proven");
    mock_quote_handoff(&mut case, &claim, *now.lock().unwrap())?;
    assert_eq!(
        case.sql
            .query_row("SELECT count(*) FROM rpc_owned_sell_handoffs", [], |r| r
                .get::<_, u64>(0))?,
        1
    );
    assert_eq!(case.sell_dispatches()?, 0);
    case.original_unchanged()?;
    if let Some(path) = std::env::var_os("COPY40_READINESS_REPORT") {
        let report = json!({"status":"SAVED40_RUST_SQLITE_MOCK_HANDOFF_PASS_NOT_LIVE",
            "clock":"EXPLICIT_SYNTHETIC_HISTORICAL_START_2026-10-14T19:27:40Z",
            "observations":3,"raw_mock_reads":raw.calls.len(),"elapsed_synthetic_seconds":12,
            "same_original_claim_owner_attempt_lease":true,"single_collecting_decision":true,
            "original0c9_bytes_unchanged":true,"new_input_graph_no_spent_output_import":true,
            "source_pre_raw":1041512,"bot_lot_raw":H,"protected_baseline_raw":100,
            "quote":"MOCK_NOT_LIVE_AVAILABILITY","handoff":"EXISTING_NATIVE_SQLITE_API",
            "new_handoffs":1,"new_dispatches":0,"provider_requests":0,"signs":0,"sends":0,
            "live_copy_proven":false});
        std::fs::write(path, serde_json::to_vec_pretty(&report)?)?;
    }
    assert!(case.root.path().exists());
    Ok(())
}

#[tokio::test]
#[ignore = "explicit hash-bound private40 fixture; no provider I/O"]
async fn copy40_readiness_finite_missing_and_wait_fences_remain_fail_closed() -> Result<()> {
    for fault in [
        "never",
        "deadline_wait",
        "cutoff_wait",
        "stop_wait",
        "generation_wait",
        "pending_wait",
        "unknown_wait",
    ] {
        let mut case = Case::new(false)?;
        let original = case.claim()?;
        let before = claim_identity(&case)?;
        let mut raw = RawSaved::new(&case, fault);
        let clock = SyntheticClock::new(&case, fault);
        let result = bind(&mut case, original.clone(), &mut raw, clock).await;
        assert!(result.is_err(), "{fault} unexpectedly proved");
        assert!(
            raw.calls.len() >= 6,
            "{fault}: initial missing observation not exercised"
        );
        assert_eq!(
            raw.calls.len(),
            if fault == "never" { 12 } else { 6 },
            "{fault}"
        );
        assert_eq!(claim_identity(&case)?, before, "{fault} changed claim");
        case.unproven()?;
        if fault == "unknown_wait" {
            assert_eq!(
                case.sql.query_row(
                    "SELECT count(*) FROM execution_canary_unresolved_dispatch",
                    [],
                    |r| r.get::<_, u64>(0)
                )?,
                1
            );
            let reopened = SqliteStore::open(&case.path)?;
            assert!(reopened
                .begin_fractional_sell(&original, case.limits, start(), &"a".repeat(64))
                .is_err());
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "explicit hash-bound private40 fixture; no provider I/O"]
async fn copy40_readiness_wrong_identity_bank_amount_and_bot_lot_never_wait() -> Result<()> {
    for fault in [
        "owner",
        "program",
        "slot",
        "bank",
        "native_bank",
        "wrong_nonzero",
        "short_bot",
        "excess_bot",
        "malformed_body",
        "wrong_id",
        "transport_unknown",
    ] {
        let mut case = Case::new(false)?;
        let original = case.claim()?;
        let mut raw = RawSaved::new(&case, fault);
        let clock = SyntheticClock::new(&case, fault);
        let now = clock.now.clone();
        let error = bind(&mut case, original, &mut raw, clock)
            .await
            .unwrap_err();
        if fault == "native_bank" {
            assert!(
                format!("{error:#}").contains("fraction_native_bank_conflict"),
                "{error:#}"
            );
            assert_eq!(raw.calls.len(), 6, "post-classification native bank fence");
        }
        assert_eq!(
            *now.lock().unwrap(),
            start(),
            "{fault} was retried as not-ready"
        );
        assert!(raw.calls.len() <= 6, "{fault} reread");
        case.unproven()?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "explicit hash-bound private40 fixture; no provider I/O"]
async fn copy40_readiness_cumulative_wire_budget_survives_wait() -> Result<()> {
    let mut case = Case::new(false)?;
    let original = case.claim()?;
    let mut raw = RawSaved::new(&case, "never");
    raw.precharge_for_second_observation()?;
    let clock = SyntheticClock::new(&case, "never");
    let now = clock.now.clone();
    let result = bind(&mut case, original, &mut raw, clock).await;
    assert!(result.is_err());
    assert_eq!(*now.lock().unwrap(), start() + chrono::Duration::seconds(6));
    assert_eq!(
        raw.calls.len(),
        7,
        "must fail only on second observation, after initial complete empty page"
    );
    assert_eq!(raw.observations, 2);
    assert_eq!(
        raw.remaining_before[6], 1,
        "same transport budget was reset across wait"
    );
    case.unproven()?;
    Ok(())
}

#[test]
#[ignore = "explicit hash-bound private40 fixture; no provider I/O"]
fn copy40_readiness_consumed_original_clone_and_restart_never_rearm() -> Result<()> {
    let case = Case::new(true)?;
    let state: String =
        case.sql
            .query_row("SELECT state FROM fractional_sell_decisions", [], |r| {
                r.get(0)
            })?;
    assert_eq!(state, "collecting");
    assert!(case.claim().is_err());
    let reopened = SqliteStore::open(&case.path)?;
    let step = reopened.claim_strict_sell_quote_for_owned_preparation(
        case.limits,
        &case.config.quote_canary_base_url,
        start,
    )?;
    assert!(!matches!(step, QuoteClaimStep::Claimed(_)));
    assert_eq!(case.sell_dispatches()?, 0);
    case.original_unchanged()
}
