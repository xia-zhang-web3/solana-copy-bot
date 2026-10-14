//! Existing SELL path with actual saved39 evidence; quote/build I/O explicitly MOCK.
use super::copy39_saved_sell_fixture::{Case, BOT, H, SELL};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage_core::ordered_sell_quote::*;
use serde_json::json;
#[tokio::test]
#[ignore = "explicit hash-bound private actual39 DB/RPC fixture, no provider I/O"]
async fn copy39_actual_collector_sqlite_to_mock_quote_and_handoff() -> Result<()> {
    let mut f = Case::new()?;
    let original = f.claim()?;
    let claim = f.bind(original).await?;
    assert_eq!(claim.binding.raw, H);
    let proof = claim.binding.fractional.as_ref().unwrap();
    assert_eq!(proof.inventory.target_index, 1067);
    assert_eq!(proof.inventory.numerator, 1065900);
    assert_eq!(proof.inventory.denominator, "1065900");
    assert_eq!(
        proof
            .protected_execution_baseline
            .as_ref()
            .unwrap()
            .amount_raw,
        100
    );
    let state: String =
        f.sql
            .query_row("SELECT state FROM fractional_sell_decisions", [], |r| {
                r.get(0)
            })?;
    assert_eq!(state, "proven");
    // No event/receipt timestamps are modified. Replay the historical budget clock
    // only at the mocked quote/handoff boundary, never grant live freshness.
    let now: DateTime<Utc> = "2026-10-14T11:03:15Z".parse()?;
    let b = &claim.binding;
    let block = &f.rpc[1].1["result"];
    let target = &block["transactions"][1067];
    let mut keys = target["transaction"]["message"]["accountKeys"]
        .as_array()
        .unwrap()
        .clone();
    for kind in ["writable", "readonly"] {
        keys.extend_from_slice(target["meta"]["loadedAddresses"][kind].as_array().unwrap());
    }
    let route = target["transaction"]["message"]["instructions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| {
            keys[i["programIdIndex"].as_u64().unwrap() as usize]
                == copybot_storage_core::ordered_sell_quote::fractional::inventory::JUPITER
        })
        .unwrap();
    let amm_key = keys[route["accounts"][11].as_u64().unwrap() as usize].clone();
    let body = json!({"inputMint":b.mint,"outputMint":b.output_mint,"inAmount":H.to_string(),"outAmount":"9900000","otherAmountThreshold":"9850500","swapMode":"ExactIn","slippageBps":50,"routePlan":[{"percent":100,"swapInfo":{"label":"Raydium","ammKey":amm_key,"inputMint":b.mint,"outputMint":b.output_mint,"inAmount":H.to_string(),"outAmount":"9900000"}}]});
    crate::execution_quote_http::cohort::validate(
        &body,
        &b.mint,
        &b.output_mint,
        &H.to_string(),
        50,
    )?;
    let sample = crate::execution_quote_http::quote_sample_from_json(body.clone())?;
    assert_eq!(sample.in_amount, H.to_string());
    let observation = QuoteObservation {
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
    let q = f
        .store
        .complete_strict_sell_quote(&claim, f.limits, observation, || now)?;
    assert_eq!(q.outcome, QuoteOutcome::Current);
    let snapshot = f.store.owned_sell_snapshot(b, f.limits)?;
    assert_eq!(snapshot.sell.facts.signature, SELL);
    assert_eq!(snapshot.receipts.len(), 1);
    assert_eq!(snapshot.facts.len(), 1);
    assert_eq!(
        snapshot.facts[0].token_delta.as_ref().unwrap().raw,
        i128::from(H)
    );
    let experiment = f.config.tiny_experiment.id.as_deref().unwrap();
    let handoff = f.store.reserve_owned_sell_handoff(
        &snapshot,
        &q,
        f.limits,
        &crate::execution_owned_sell_rpc::identity(&f.config)?,
        "OFFLINE MOCK quote/finalized execution authority; no signer or send",
        experiment,
        BOT,
        || now,
    )?;
    f.store
        .recheck_owned_sell_handoff(&handoff, f.limits, now)?;
    let count: u64 = f
        .sql
        .query_row("SELECT count(*) FROM rpc_owned_sell_handoffs", [], |r| {
            r.get(0)
        })?;
    assert_eq!(count, 1);
    let dispatch: u64 =
        f.sql
            .query_row("SELECT count(*) FROM rpc_owned_sell_dispatches", [], |r| {
                r.get(0)
            })?;
    assert_eq!(dispatch, 0);
    assert!(f
        .store
        .reserve_owned_sell_handoff(
            &snapshot,
            &q,
            f.limits,
            &crate::execution_owned_sell_rpc::identity(&f.config)?,
            "duplicate MOCK",
            experiment,
            BOT,
            || now
        )
        .is_err());
    let report = json!({"schema":"copy39_actual_collector_mock_handoff_replay_v1","actual_full_block_transactions":1997,"actual_target_index":1067,
        "actual_rpc_responses":6,"source_sell":SELL,"owned_raw":H,"selected_raw":b.raw,"protected_raw":100,
        "fraction_state":state,"quote_outcome":"Current","handoff_count":count,"dispatch_count":dispatch,
        "quote":"EXPLICIT_SYNTHETIC_MOCK_NOT_LIVE_AVAILABILITY","quote_handoff_clock":"EXPLICIT_HISTORICAL_MOCK",
        "preserved_native_admission_and_receipt_dates":true,"original39_quote_claims_unchanged":true,
        "provider_requests":0,"signs":0,"sends":0,"live_copy_proven":false});
    if let Some(path) = std::env::var_os("COPY39_REPLAY_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&report)?)?;
    }
    if let Some(path) = std::env::var_os("COPY39_REPLAY_SQLITE_OUT") {
        f.sql.execute("VACUUM INTO ?1", [path.to_str().unwrap()])?;
    }
    assert!(f.root.path().exists());
    Ok(())
}
