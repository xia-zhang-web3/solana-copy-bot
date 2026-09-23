//! Independent narrow causal boundary regression; deliberately RED on candidate.
//! No HTTP, signer, runner launch, or financial writes. The only SQLite write is
//! a synthetic observed source row in shared RAM. This does not claim full tick
//! coverage: it checks actual observed lookup, actual price/slippage derivation,
//! and actual quote decision finalization used by build_entry_quote_event.
//! Include the unchanged current price module solely to access its pub(super)
//! function; no financial implementation is copied or changed in this harness.
mod actual_price {
    include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/execution_quote_canary_provider_compare.rs"));
}

use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::{CopySignalRow, Lamports, COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS};
use copybot_storage_core::{ExecutionQuoteCanaryEventInsert, SqliteStore};
use crate::execution_quote_canary_helpers::{
    apply_quote_sample_to_event, finalize_quote_decision, load_matching_observed_entry_leg,
    price_sol_per_token, QuoteSample, SOL_MINT,
};
use serde_json::json;

const SIGNATURE: &str = "reviewer-synthetic-source-signature";
const WALLET: &str = "reviewer-synthetic-source-wallet";
const MINT: &str = "reviewer-synthetic-classic-mint";

#[test]
fn reviewer_native_quote_rejects_unbound_observed_row() -> Result<()> {
    let path = format!("file:reviewer-native-quote-{}?mode=memory&cache=shared", std::process::id());
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations")))?;
    let sql = rusqlite::Connection::open(&path)?;
    let now = Utc::now();
    // Give the native path the strongest ordinary observed evidence: the exact
    // source row exists, and the fresh external quote matches its unit price.
    sql.execute(
        "INSERT INTO observed_swaps(signature,wallet_id,dex,token_in,token_out,qty_in,qty_out,slot,ts,qty_in_raw,qty_in_decimals,qty_out_raw,qty_out_decimals) VALUES(?1,?2,'synthetic',?3,?4,0.01,1.0,100,?5,'10000000',9,'1000',3)",
        rusqlite::params![SIGNATURE,WALLET,SOL_MINT,MINT,now.to_rfc3339()],
    )?;
    assert!(store.load_execution_canary_observed_leg_by_signature(SIGNATURE)?.is_some());
    let legacy = signal(format!("shadow:{SIGNATURE}:{WALLET}:buy:{MINT}"), "shadow", now);
    let native = signal(format!("native-buy-v1:{SIGNATURE}"), "native_buy_fenced_v1", now);
    let control = quote_at_financial_boundary(&store, &legacy, now)?;
    assert!(control.shadow_price_sol.is_some());
    assert_eq!(control.slippage_bps, Some(0.0));
    assert_eq!(control.decision_status.as_deref(), Some("would_execute"));
    let candidate = quote_at_financial_boundary(&store, &native, now)?;
    assert_eq!(candidate.quote_status, "ok");
    assert_eq!(candidate.quote_in_amount_raw, control.quote_in_amount_raw);
    assert_eq!(candidate.quote_out_amount_raw, control.quote_out_amount_raw);
    assert_eq!(candidate.quote_price_sol, control.quote_price_sol);
    // A legacy observed row is not native admission authority. The positive
    // producer-backed price/quote path is exercised by native_buy_route_mock_tests.
    assert!(store.native_buy_source_amounts(
        &native.signal_id, &format!("native-buy-decision-v1:{SIGNATURE}"), now, 10,
    )?.is_none());
    assert_eq!(candidate.decision_status.as_deref(), Some("unknown"));
    assert_eq!(candidate.decision_reason.as_deref(), Some("missing_slippage_bps"));
    Ok(())
}

fn signal(signal_id: String, status: &str, now: DateTime<Utc>) -> CopySignalRow {
    CopySignalRow {
        signal_id,
        wallet_id: WALLET.into(),
        side: "buy".into(),
        token: MINT.into(),
        notional_sol: 0.01,
        notional_lamports: Some(Lamports::new(10_000_000)),
        notional_origin: COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
        ts: now,
        status: status.into(),
    }
}

fn quote_at_financial_boundary(
    store: &SqliteStore,
    signal: &CopySignalRow,
    now: DateTime<Utc>,
) -> Result<ExecutionQuoteCanaryEventInsert> {
    // These are the same observed lookup and reference-field assignments used
    // by execution_quote_canary_builders.rs:24,39-49. Quote HTTP and mint-decimal
    // acquisition are the only omitted external operands; both sides get the
    // same successful raw QuoteSample and known classic SPL decimals=3.
    let observed = load_matching_observed_entry_leg(store, signal)?;
    let mut event = ExecutionQuoteCanaryEventInsert {
        http_request_started_ts: None,
        quote_response_available_ts: None,
        event_id: format!("quote:entry:{}", signal.signal_id),
        signal_id: Some(signal.signal_id.clone()),
        shadow_closed_trade_id: None,
        wallet_id: signal.wallet_id.clone(),
        token: signal.token.clone(),
        side: "buy".into(),
        quote_status: "skipped".into(),
        request_ts: now,
        signal_ts: Some(signal.ts),
        decision_delay_ms: None,
        quote_latency_ms: None,
        leader_notional_sol: observed.as_ref().map(|v| v.sol_notional).or(Some(signal.notional_sol)),
        quote_in_amount_raw: None,
        quote_out_amount_raw: None,
        quote_response_json: None,
        quote_price_sol: None,
        shadow_price_sol: observed.as_ref().and_then(|v| price_sol_per_token(v.sol_notional,v.token_qty)),
        slippage_bps: None,
        price_impact_pct: None,
        route_plan_json: None,
        priority_fee_status: None,
        priority_fee_lamports: None,
        priority_fee_json: None,
        decision_status: None,
        decision_reason: None,
        error: None,
    };
    apply_quote_sample_to_event(&mut event, QuoteSample {
        http_request_started_ts: Some(now),
        quote_response_available_ts: Some(now),
        in_amount: "10000000".into(),
        out_amount: "1000".into(),
        response_json: json!({"inputMint":SOL_MINT,"outputMint":MINT,"inAmount":"10000000","outAmount":"1000","swapMode":"ExactIn","slippageBps":500}).to_string(),
        price_impact_pct: Some(0.0),
        route_plan_json: Some("[]".into()),
        in_decimals: Some(9),
        out_decimals: Some(3),
        latency_ms: 0,
    });
    (event.quote_price_sol, event.slippage_bps) = actual_price::buy_quote_price_and_slippage(&event, 3);
    if signal.status == "native_buy_fenced_v1" {
        event.signal_ts = None;
        event.decision_delay_ms = None;
    }
    finalize_quote_decision(&mut event, 500);
    Ok(event)
}
