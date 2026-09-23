//! Native quote/plan/hold/dispatch consumers, with explicit synthetic I/O operands.
use super::{fractional_fixture::Fixture, fractional_tests as f};
use crate::execution_submit_adapter::{
    ExecutionBuildPlanMetadata, ExecutionSubmitAdapter, ExecutionSubmitRequest,
    JupiterMetisDryRunExecutionAdapter,
};
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::Utc;
use copybot_storage_core::{ordered_sell_quote::*, rpc_owned_sell_handoff::dispatch::Prepared, *};
use serde_json::json;
pub(super) fn budget(f: &Fixture) -> Result<()> {
    let wallet = f.meta["our"]["signer"].as_str().unwrap();
    let token = f.meta["our"]["token_out"].as_str().unwrap();
    let position =
        f.db.store
            .load_execution_canary_open_position(token)?
            .unwrap();
    let buy:String=f.db.sql.query_row("SELECT o.order_id FROM orders o JOIN copy_signals s ON s.signal_id=o.signal_id WHERE s.side='buy'",[],|r|r.get(0))?;
    // Test-only pre-existing experiment; no daemon activation or real wallet exists.
    f.db.store
        .activate_tiny_experiment("fractional-test", wallet, Utc::now())?;
    f.db.sql.execute(
        "UPDATE execution_tiny_experiment SET buy_order_id=?1,token=?2,position_id=?3",
        rusqlite::params![buy, token, position.position_id],
    )?;
    f.db.sql.execute("INSERT INTO execution_canary_dispatch(order_id,signal_id,client_order_id,route,attempt,wallet,token,side,tx_signature,message_sha256,transaction_sha256,claimed_at,transport_note) SELECT order_id,signal_id,client_order_id,route,attempt,?1,?2,'buy',tx_signature,?3,?4,submit_ts,'unknown' FROM orders WHERE order_id=?5",rusqlite::params![wallet,token,"a".repeat(64),"b".repeat(64),buy])?;
    f.db.sql.execute("INSERT INTO execution_tiny_reservations(order_id,experiment_id,tx_signature,wallet,side,message_sha256,transaction_sha256,buy_lamports,fee_bound,priority_fee,fee_slot,reserved_at) SELECT order_id,'fractional-test',tx_signature,?1,'buy',?2,?3,10000000,100000,10000,'120',submit_ts FROM orders WHERE order_id=?4",rusqlite::params![wallet,"a".repeat(64),"b".repeat(64),buy])?;
    Ok(())
}
pub(super) fn prepare(f: &Fixture, claim: &QuoteClaim) -> Result<Prepared> {
    let c = f::config(f)?;
    let l = super::association_parent_fixture::limits();
    let b = &claim.binding;
    let body = json!({"inputMint":b.mint,"outputMint":b.output_mint,"inAmount":b.raw.to_string(),"outAmount":"1000000","otherAmountThreshold":"950000","swapMode":"ExactIn","slippageBps":100,"routePlan":[{"swapInfo":{"label":"Jupiter"}}]});
    let now = Utc::now();
    let q = QuoteObservation {
        version: 1,
        binding: Some(b.clone()),
        outcome: QuoteOutcome::Current,
        reason: None,
        http_started: Some(now),
        http_response: Some(now),
        quote_response_available_ts: Some(now),
        http_ended: now,
        response_in_raw: Some(b.raw.to_string()),
        response_out_raw: Some("1000000".into()),
        response_sha256: Some(crate::execution_owned_sell_rpc::digest(body.to_string())),
        event_time: None,
        event_delay_ns: None,
    };
    let q =
        f.db.store
            .complete_strict_sell_quote(claim, l, q, Utc::now)?;
    assert_eq!(q.outcome, QuoteOutcome::Current);
    let snapshot = f.db.store.owned_sell_snapshot(b, l)?;
    let h = f.db.store.reserve_owned_sell_handoff(
        &snapshot,
        &q,
        l,
        &crate::execution_owned_sell_rpc::identity(&c)?,
        "synthetic mocked finalized RPC authority; no signing permit",
        "fractional-test",
        &c.canary_wallet_pubkey,
        Utc::now,
    )?;
    let request=ExecutionSubmitRequest {order_id:h.order_id.clone(),signal_id:h.intent_id.clone(),client_order_id:h.owner.clone(),attempt:1,route:c.canary_route.clone(),wallet_id:b.source_wallet.clone(),token:b.mint.clone(),side:"sell".into(),buy_size_sol:0.0,slippage_tolerance_bps:100,wallet_pubkey:c.canary_wallet_pubkey.clone(),entry_route_plan_json:None,metadata:ExecutionBuildPlanMetadata {quote_event_id:Some(h.intent_id.clone()),quote_source:Some(b.provider.clone()),quote_request_ts:Some(now),http_request_started_ts:Some(now),quote_response_available_ts:Some(now),quote_status:Some("ok".into()),quote_in_amount_raw:Some(b.raw.to_string()),quote_out_amount_raw:Some("1000000".into()),quote_response_json:Some(body.to_string()),route_plan_json:Some(body["routePlan"].to_string()),priority_fee_status:Some("ok".into()),priority_fee_source:Some("configured_cap".into()),priority_fee_lamports:Some(22000),priority_fee_json:Some(json!({"version":1,"source":"configured_cap","unit":"total_priority_fee_lamports","value":22000}).to_string()),..Default::default()}};
    let plan =
        JupiterMetisDryRunExecutionAdapter::new(c.clone()).build_transaction_plan(&request)?;
    assert_eq!(
        plan.swap_blueprint.as_ref().unwrap().input_amount_raw,
        b.raw.to_string()
    );
    let captured = crate::execution_instruction_bundle_binding::BundleRequest::capture(&plan)?;
    let mut bundle = super::generic_sell_synthetic_fixture::bundle(
        crate::execution_pumpswap_accounts::parse_pubkey(&c.canary_wallet_pubkey, "fixture")?,
        1_400_000,
        10000,
    );
    // Executable SPL transfer fixture binds the selected raw in actual unsigned wire.
    // No claim of real DEX execution; swap/fee/receipt I/O is synthetic.
    let e = f::evidence()?;
    let source = e.execution_accounts["value"][0]["pubkey"].clone();
    let destination = e.pages[1].response["value"][1]["pubkey"].clone();
    let mut data = vec![3];
    data.extend_from_slice(&b.raw.to_le_bytes());
    bundle["swapInstruction"] = json!({"programId":fractional::inventory::TOKEN,"accounts":[{"pubkey":source,"isSigner":false,"isWritable":true},{"pubkey":destination,"isSigner":false,"isWritable":true},{"pubkey":c.canary_wallet_pubkey,"isSigner":true,"isWritable":false}],"data":STANDARD.encode(&data)});
    let mut assembly = c.clone();
    assembly.canary_tiny_submit_enabled = true;
    let unsigned = crate::execution_guarded_generic_sell::assemble(
        &assembly,
        &plan,
        &captured.bind(&bundle)?,
    )?;
    let raw = STANDARD.decode(&unsigned.serialized_transaction_base64)?;
    assert!(raw.windows(data.len()).any(|w| w == data));
    let (message, fee) = crate::execution_priority_fee_wire::decode_priority_fee_message(
        &unsigned.serialized_transaction_base64,
    )?;
    f.db.store.complete_owned_sell_handoff(
        &h,
        l,
        &unsigned.serialized_transaction_base64,
        &message.binding.message_sha256,
        19000,
        fee.total,
        Utc::now,
    )?;
    let prepared = Prepared {
        handoff: h,
        experiment: f.db.store.owned_sell_experiment_snapshot()?,
        limits: (l.count, l.bytes, l.busy_ms),
        payload: unsigned.serialized_transaction_base64,
        message_sha256: message.binding.message_sha256,
        total_fee: 19000,
        priority_fee: fee.total,
    };
    // Exercise the actual native pre-sign request/quantity guard, not the legacy
    // amount=min(H,wallet) proof. The strict runner never calls Selection::amount.
    let selection = crate::execution_source_sell_guard::amount::Selection::new(
        &f.db
            .store
            .load_execution_canary_open_position(&b.mint)?
            .unwrap(),
    )?;
    selection.recheck(&f.db.store)?;
    let mut native = request;
    native.order_id = prepared.order_id();
    native.metadata.rpc_owned_sell = Some(Box::new(prepared.clone()));
    native.metadata.rpc_owned_live =
        Some(crate::execution_owned_sell_prepare::submit::guard::Live(
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
        ));
    assert!(native.metadata.owned_sell_amount.is_none());
    assert_eq!(
        crate::execution_owned_sell_prepare::submit::guard::request(&f.db.store, &native)?,
        prepared
    );
    native.metadata.quote_in_amount_raw = Some((b.raw + 1).to_string());
    assert!(
        crate::execution_owned_sell_prepare::submit::guard::request(&f.db.store, &native).is_err()
    );
    Ok(prepared)
}
pub(super) fn dispatch(f: &Fixture, p: &Prepared) -> Result<ExecutionCanaryDispatch> {
    let h = &p.handoff;
    // Mock signer/transport identity, never a user's key or a real signed transaction.
    let d = ExecutionCanaryDispatch {
        order_id: p.order_id(),
        signal_id: h.intent_id.clone(),
        client_order_id: h.owner.clone(),
        route: "jupiter_swap_instructions".into(),
        attempt: 1,
        wallet: h.wallet.clone(),
        token: h.snapshot.quote.mint.clone(),
        side: "sell".into(),
        tx_signature: bs58::encode([231u8; 64]).into_string(),
        message_sha256: p.message_sha256.clone(),
        transaction_sha256: "d".repeat(64),
    };
    let b = TinyBudgetClaim {
        experiment_id: h.experiment_id.clone(),
        wallet: h.wallet.clone(),
        tx_signature: d.tx_signature.clone(),
        message_sha256: d.message_sha256.clone(),
        transaction_sha256: d.transaction_sha256.clone(),
        buy_lamports: Some(0),
        protected_capital: None,
        total_fee: p.total_fee,
        priority_fee: p.priority_fee,
        fee_slot: 151,
    };
    assert!(matches!(
        f.db.store
            .claim_owned_sell_dispatch(p, &d, &b, || Ok(Utc::now()))?,
        ExecutionDispatchClaim::New
    ));
    assert!(matches!(
        f.db.store
            .claim_owned_sell_dispatch(p, &d, &b, || Ok(Utc::now()))?,
        ExecutionDispatchClaim::Existing
    ));
    Ok(d)
}
pub(super) fn receipt(d: &ExecutionCanaryDispatch, sold: u64) -> ExecutionCanaryReceiptFacts {
    use copybot_core_types::{Lamports, SignedLamports};
    ExecutionCanaryReceiptFacts {
        order_id: d.order_id.clone(),
        tx_signature: d.tx_signature.clone(),
        wallet_pubkey: d.wallet.clone(),
        token: d.token.clone(),
        side: "sell".into(),
        slot: 151,
        wallet_native_pre: Lamports::new(100000000),
        wallet_native_post: Lamports::new(100981000),
        wallet_native_delta: SignedLamports::new(981000),
        transaction_fee: Some(Lamports::new(19000)),
        fee_coverage: ReceiptFeeCoverage::Known,
        fee_payer: Some(d.wallet.clone()),
        token_delta: Some(ReceiptTokenDelta {
            raw: -i128::from(sold),
            decimals: 3,
        }),
        token_coverage: ReceiptTokenCoverage::PairedBalances,
        token_coverage_reason: None,
        wsol_coverage: ReceiptWsolCoverage::Unresolved,
        block_time: None,
        decomposition: ReceiptDecomposition::Unresolved,
    }
}
