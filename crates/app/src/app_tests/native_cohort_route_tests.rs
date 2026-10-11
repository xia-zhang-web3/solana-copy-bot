//! Real quote HTTP/callers with local Raydium ABI/receipt controls; never live proof.
#[path = "native_cohort_route_fixture.rs"] mod native;
#[path = "native_cohort_route_http_fixture.rs"] mod local;
#[path = "../execution_strict_quote_http.rs"] mod strict;
use crate::app_tests::{run15_buy_wire_fixture as wire, run15_rpc_proof_fixture as receipt,
    owner_exit_test_fixture as exit};
use crate::execution_quote_canary::{ExecutionQuoteCanaryRunner, ExecutionQuoteCanaryTickSummary};
use crate::execution_quote_http::cohort;
use crate::execution_submit_adapter::*;
use crate::execution_build_plan_refresh::refresh_tiny_buy_build_plan_metadata as refresh;
use crate::execution_instruction_bundle_binding::BundleRequest;
use copybot_storage_core::ordered_sell_quote::{QuoteBinding, QuoteOutcome};
use anyhow::{Context, Result};
use chrono::Utc;
use serde_json::{json, Value};
const SOL: &str = crate::execution_quote_canary_helpers::SOL_MINT;

fn request(c: &native::Case, metadata: ExecutionBuildPlanMetadata) -> ExecutionSubmitRequest {
    ExecutionSubmitRequest { order_id:"route28-local-order".into(), signal_id:c.signal.signal_id.clone(),
        client_order_id:"route28-local-client".into(), attempt:1, route:c.config.canary_route.clone(),
        wallet_id:c.signal.wallet_id.clone(), token:c.signal.token.clone(), side:"buy".into(),
        buy_size_sol:0.01, slippage_tolerance_bps:50, wallet_pubkey:c.config.canary_wallet_pubkey.clone(),
        entry_route_plan_json:None, metadata }
}
fn plan(r: &ExecutionSubmitRequest) -> Result<ExecutionTransactionPlan> {
    let mut p = NoSubmitExecutionAdapter.build_transaction_plan(r)?;
    p.swap_blueprint = Some(crate::execution_swap_blueprint::build_execution_swap_blueprint(r)?);
    Ok(p)
}
fn sell_binding(c: &native::Case) -> QuoteBinding {
    QuoteBinding { version:1, intent_id:"source-sell:local28".into(), policy:"provider_order_strict_v1".into(),
        position_id:"local-position".into(), position_opened_ts:c.now.to_rfc3339(), source_signature:"local28".into(),
        source_wallet:c.signal.wallet_id.clone(), mint:exit::USDC.into(), output_mint:SOL.into(),side:"sell".into(),
        provider:"generic_metis".into(),endpoint:c.config.quote_canary_base_url.clone(),raw:1_167_085,decimals:6,
        fractional:None,snapshot_version:"local-checked-snapshot".into() }
}
#[tokio::test]
async fn native_cohort_route_initial_refresh_retry_to_buy_wire_and_receipt() -> Result<()> {
    let mut c = native::setup()?;
    let server = local::Server::start(wire::quote(receipt::MINT,10_000_000,100_000_000,50)).await?;
    c.config.quote_canary_base_url = server.url.clone();
    // Even an enabled parallel provider cannot inject a Pump quote into this closed profile.
    c.config.quote_canary_pump_fun_parallel_enabled = true;
    let runner = ExecutionQuoteCanaryRunner::new(c.config.clone());
    let bundle = runner.build_entry_quote_event_inner(&c.store,&c.signal,c.now,None,None).await?;
    assert_eq!(bundle.event.quote_status,"ok");
    let mut summary = ExecutionQuoteCanaryTickSummary::default();
    runner.record_native_buy_bundle(&c.store,&c.guard,bundle,&mut summary)?;
    assert_eq!(summary.entry_inserted,1);
    let mut metadata = crate::execution_build_plan_metadata::load_execution_build_plan_metadata(&c.store,&c.signal.signal_id)?;
    assert_eq!(crate::execution_canary_entry_gate::validate_execution_canary_entry_metadata(&c.config,&metadata),None);
    assert_eq!(metadata.priority_fee_lamports,Some(50_000));
    for _ in 0..2 { // Same real function invoked by initial submission and persisted BUY retry.
        metadata = refresh(&reqwest::Client::new(),&c.config,&c.signal,metadata).await?;
        assert_eq!(metadata.decision_status.as_deref(),Some("would_execute"));
        assert_eq!(metadata.slippage_bps,Some(0.0));
    }
    server.assert_profile(3,"V1");
    let r = request(&c,metadata);
    let p = plan(&r)?;
    let ixs = wire::instructions(exit::key().verifying_key().to_bytes(),receipt::MINT,10_000_000,100_000_000)?;
    let bound = BundleRequest::capture(&p)?.bind(&local::bundle(&ixs))?;
    let (_, parsed) = bound.verified_parts(&p)?;
    let payload = crate::execution_native_floor::prepare_final_native_floor(
        exit::key().verifying_key().to_bytes(),parsed.blockhash(),parsed.instructions(),160_200_031)?;
    crate::execution_owner_buy_wire::verify_cohort(&r,payload.payload())?;
    crate::execution_priority_fee_proof::prove(&r,payload.payload(),50_000)?;
    let (receipt_payload, tx) = receipt::model_follower()?;
    let binding = crate::execution_transaction_wire::decode_message(&receipt_payload, |_| Ok(()))?;
    let built = crate::execution_transaction_wire::decode_message(payload.payload(), |_| Ok(()))?;
    assert_eq!(binding.binding.message_bytes,built.binding.message_bytes);
    let (anchor, facts) = receipt::anchor(&tx,100_000_000);
    crate::execution_owned_sell_rpc::decode::buy(&tx,&anchor,&facts)?;
    assert!(!c.guard.check_at(&c.store,c.now+chrono::Duration::seconds(121))?);
    Ok(())
}
#[tokio::test]
async fn native_cohort_route_strict_sell_to_real_assembler_and_receipt() -> Result<()> {
    let mut c = native::setup()?;
    let server = local::Server::start(exit::quote()).await?;
    c.config.quote_canary_base_url = server.url.clone();
    c.config.swap_instructions_dry_run_enabled = true;c.config.swap_transaction_dry_run_enabled = true;
    let b = sell_binding(&c);let mut checks = 0;
    let (q,body) = strict::fetch_with_body(&reqwest::Client::new(),&c.config,&b,
        || { checks+=1;Ok(true) }).await;
    assert_eq!(q.outcome,QuoteOutcome::Current);assert!(checks>=3);
    assert_eq!(q.response_in_raw.as_deref(),Some("1167085"));
    server.assert_profile(1,"V1");
    let raw:Value=serde_json::from_str(body.as_deref().context("body")?)?;
    let mut r=request(&c,ExecutionBuildPlanMetadata { quote_event_id:Some(b.intent_id.clone()),
        quote_status:Some("ok".into()),quote_source:Some("generic_metis".into()),quote_response_json:body,
        quote_in_amount_raw:q.response_in_raw,quote_out_amount_raw:q.response_out_raw,
        route_plan_json:Some(raw["routePlan"].to_string()),priority_fee_status:Some("ok".into()),
        priority_fee_lamports:Some(22_000),priority_fee_json:Some(crate::app_tests::priority_fee_fixture::total_json(22_000)),
        ..Default::default() });
    r.side="sell".into();r.token=exit::USDC.into();r.signal_id=b.intent_id;
    let p=plan(&r)?;let bound=BundleRequest::capture(&p)?.bind(&exit::bundle()?)?;
    let built=crate::execution_guarded_generic_sell::assemble(&c.config,&p,&bound)?;
    // Source-owned SELL uses its own assembler, without the owner-exit-only floor.
    // Decode the actual output and bind the complete existing Raydium route ABI.
    let message=crate::execution_transaction_wire::decode_message(&built.serialized_transaction_base64, |_|Ok(()))?;
    let expected=exit::instructions(exit::key().verifying_key().to_bytes())?;
    let route=message.instructions.iter().find(|i| i.program.pubkey==expected[3].program_id).context("route")?;
    assert_eq!(route.data,expected[3].data);
    assert_eq!(route.accounts.iter().map(|a|a.pubkey).collect::<Vec<_>>(),
        expected[3].accounts.iter().map(|a|a.pubkey).collect::<Vec<_>>());
    let sig=bs58::encode([71;64]).into_string();
    let tx=exit::receipt(&built.serialized_transaction_base64,&sig,&r.wallet_pubkey)?;
    let proof=copybot_storage_core::ExecutionCanaryReceiptProof{tx_signature:sig,wallet_pubkey:r.wallet_pubkey,
        token:r.token,side:"sell".into(),confirmation_status:"confirmed".into(),slot:Some(120),confirmed_at:Utc::now(),reason:"synthetic-local".into()};
    let facts=crate::execution_submit_adapter::test_route_receipt_facts("route28-sell",&proof,&json!({"result":tx}))?.facts;
    assert_eq!(facts.token_delta.context("token delta")?.raw,-1_167_085);
    assert_eq!(facts.wallet_native_post.as_u64()-facts.wallet_native_pre.as_u64(),11_995_000);
    Ok(())
}
#[tokio::test]
async fn native_cohort_route_saved28_v2_pump_refused_before_refresh_or_source_sell() -> Result<()> {
    let saved:Value=serde_json::from_str(include_str!("fixtures/route28-fresh-quote.json"))?;
    assert_eq!(saved["instructionVersion"],"V2");assert_eq!(saved["routePlan"][0]["swapInfo"]["label"],"Pump.fun Amm");
    let server=local::Server::start(saved.clone()).await?;let mut c=native::setup()?;
    c.config.quote_canary_base_url=server.url.clone();c.config.quote_canary_pump_fun_parallel_enabled=true;
    let ordinary=crate::execution_quote_http::fetch_quote_sample(&reqwest::Client::new(),&c.config,SOL,
        saved["outputMint"].as_str().unwrap(),"10000000",50).await?;
    assert_eq!(ordinary.response_json,serde_json::to_string(&saved)?);
    // Old generic request accepted this quote. The new profile refuses the same exact bytes.
    assert!(cohort::fetch(&reqwest::Client::new(),&c.config,SOL,saved["outputMint"].as_str().unwrap(),"10000000",50).await.is_err());
    let m=ExecutionBuildPlanMetadata { quote_event_id:Some("local28".into()),quote_source:Some(crate::execution_quote_provider_selection::QUOTE_SOURCE_PUMP_FUN_PAID.into()),..Default::default() };
    let fresh=refresh(&reqwest::Client::new(),&c.config,&c.signal,m).await?;
    assert_eq!(fresh.quote_status.as_deref(),Some("error"));assert!(fresh.quote_response_available_ts.is_none());
    let (q,_)=strict::fetch_with_body(&reqwest::Client::new(),&c.config,&sell_binding(&c),||Ok(true)).await;
    assert_eq!(q.outcome,QuoteOutcome::Unknown);assert!(q.quote_response_available_ts.is_none());
    assert_eq!(server.calls.lock().unwrap().len(),4,"no Pump fallback or hidden retry");
    Ok(())
}
#[test]
fn native_cohort_route_profile_mutations_and_scope_are_closed() -> Result<()> {
    let q=wire::quote(receipt::MINT,10_000_000,100_000_000,50);
    cohort::validate(&q,SOL,receipt::MINT,"10000000",50)?;
    for (path,value) in [("/instructionVersion",json!("V2")),("/outputMint",json!(exit::USDC)),
        ("/inAmount",json!("10000001")),("/slippageBps",json!(51)),("/otherAmountThreshold",json!("1")),
        ("/platformFee",json!({"feeBps":1})),("/routePlan/0/swapInfo/label",json!("Pump.fun Amm")),
        ("/routePlan/0/swapInfo/inAmount",json!("10000001")),("/routePlan/0/swapInfo/ammKey",json!("invalid"))] {
        let mut wrong=q.clone();*wrong.pointer_mut(path).unwrap()=value;
        assert!(cohort::validate(&wrong,SOL,receipt::MINT,"10000000",50).is_err(),"{path}");
    }
    let mut wrong=q.clone();wrong["routePlan"][0]["bps"]=json!(5000);
    assert!(cohort::validate(&wrong,SOL,receipt::MINT,"10000000",50).is_err());
    let mut split=q.clone();split["routePlan"].as_array_mut().unwrap().push(q["routePlan"][0].clone());
    assert!(cohort::validate(&split,SOL,receipt::MINT,"10000000",50).is_err());
    let c=native::setup()?;
    assert!(cohort::applies(&c.config,&c.signal.signal_id,&c.signal.wallet_id,"buy"));
    assert!(cohort::applies(&c.config,"source-sell:local",&c.signal.wallet_id,"sell"));
    for mode in 0..7 {
        let mut config=c.config.clone();let mut id=c.signal.signal_id.clone();let mut wallet=c.signal.wallet_id.clone();let mut side="buy";
        match mode {0=>config.technical_cohort=None,1=>config.technical_cohort.as_mut().unwrap().activate=false,
            2=>config.tiny_experiment.id=Some("foreign".into()),3=>wallet="foreign".into(),
            4=>id="owner-buy:local".into(),5=>side="sell",6=>config.tiny_experiment.activate=false,_=>unreachable!()};
        assert!(!cohort::applies(&config,&id,&wallet,side),"mode={mode}");
    }
    Ok(())
}
#[tokio::test]
async fn native_cohort_route_refresh_keeps_price_refusal_and_sell_recheck() -> Result<()> {
    let mut c=native::setup()?;
    let server=local::Server::start(wire::quote(receipt::MINT,10_000_000,99_000_000,50)).await?;
    c.config.quote_canary_base_url=server.url.clone();
    let metadata=ExecutionBuildPlanMetadata{quote_event_id:Some("local28".into()),
        quote_in_amount_raw:Some("10000000".into()),quote_out_amount_raw:Some("100000000".into()),
        quote_price_sol:Some(0.1),slippage_bps:Some(0.0),
        quote_response_json:Some(json!({"meta":{"outDecimals":9}}).to_string()),..Default::default()};
    let fresh=refresh(&reqwest::Client::new(),&c.config,&c.signal,metadata).await?;
    assert_eq!(fresh.decision_status.as_deref(),Some("would_skip"));
    assert_eq!(fresh.decision_reason.as_deref(),Some("fresh_submit_quote_slippage_above_limit"));
    *server.reply.lock().unwrap()=exit::quote();let mut n=0;
    let (q,_)=strict::fetch_with_body(&reqwest::Client::new(),&c.config,&sell_binding(&c),
        || {n+=1;Ok(n<2)}).await;
    assert_eq!(q.outcome,QuoteOutcome::Stale);assert!(q.quote_response_available_ts.is_none());
    server.assert_profile(2,"V1");
    Ok(())
}
#[tokio::test]
async fn native_cohort_route_noncohort_strict_sell_retains_generic_v2() -> Result<()> {
    let mut c=native::setup()?;c.config.technical_cohort=None;
    let server=local::Server::start(exit::quote()).await?;c.config.quote_canary_base_url=server.url.clone();
    let (q,_)=strict::fetch_with_body(&reqwest::Client::new(),&c.config,&sell_binding(&c),||Ok(true)).await;
    assert_eq!(q.outcome,QuoteOutcome::Current);server.assert_profile(1,"V2");Ok(())
}

#[tokio::test]
async fn native_cohort_route_correct_sell_v2_and_pump_rejected_by_profile() -> Result<()> {
    let mut c=native::setup()?;
    let server=local::Server::start(exit::quote()).await?;c.config.quote_canary_base_url=server.url.clone();
    for mode in 0..2 {
        let mut q=exit::quote();
        if mode==0 {q["instructionVersion"]=json!("V2");}
        else {q["routePlan"][0]["swapInfo"]["label"]=json!("Pump.fun Amm");}
        *server.reply.lock().unwrap()=q;
        let (out,_)=strict::fetch_with_body(&reqwest::Client::new(),&c.config,&sell_binding(&c),||Ok(true)).await;
        assert_eq!(out.outcome,QuoteOutcome::Unknown);
        assert!(out.reason.as_deref().unwrap().starts_with("cohort_quote_"));
        assert!(out.quote_response_available_ts.is_none());
    }
    server.assert_profile(2,"V1");Ok(())
}
