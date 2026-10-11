//! Causal configured-fee repair using actual saved25 amount/quote operands.
#[path = "native_cohort_fee_fixture.rs"]
mod fixture;
use anyhow::Result;
use chrono::{Duration, Utc};
use crate::execution_canary_entry_gate::validate_execution_canary_entry_metadata as gate;
use crate::execution_quote_canary::ExecutionQuoteCanaryRunner;
use crate::execution_quote_canary_helpers::PriorityFeeSample;
use crate::execution_submit_adapter::*;
use crate::execution_priority_fee_proof as proof;
use serde_json::{json, Value};

fn request(case:&fixture::Case, metadata:ExecutionBuildPlanMetadata) -> ExecutionSubmitRequest {
    ExecutionSubmitRequest{order_id:"fee25-order".into(),signal_id:case.signal.signal_id.clone(),
        client_order_id:"fee25-client".into(),attempt:1,route:case.config.canary_route.clone(),
        wallet_id:case.signal.wallet_id.clone(),token:case.signal.token.clone(),side:"buy".into(),
        buy_size_sol:0.01,slippage_tolerance_bps:50,wallet_pubkey:case.config.canary_wallet_pubkey.clone(),
        entry_route_plan_json:None,metadata}
}

#[tokio::test]
async fn native_cohort_fee25_saved_early_reaches_entry_blueprint_and_wire() -> Result<()> {
    let case=fixture::setup()?;
    assert!(!case.config.priority_fee_canary_enabled);
    let metadata=fixture::metadata(&case,&case.config,"early",None).await?;
    assert_eq!(metadata.decision_status.as_deref(),Some("would_execute"));
    assert!((metadata.slippage_bps.unwrap()-20.989).abs()<0.001);
    assert_eq!(gate(&case.config,&metadata),None,"saved25 early quote must no longer miss fee");
    let mut missing=metadata.clone();
    missing.priority_fee_status=None;missing.priority_fee_lamports=None;missing.priority_fee_json=None;
    assert_eq!(gate(&case.config,&missing),Some("missing_priority_fee"));
    assert_eq!(metadata.priority_fee_lamports,Some(50_000));
    let tagged:Value=serde_json::from_str(metadata.priority_fee_json.as_deref().unwrap())?;
    assert_eq!(tagged["source"],"native_cohort_configured_cap");
    assert_eq!(tagged["unit"],"total_priority_fee_lamports");
    assert_eq!(tagged["value"],50_000);
    let request=request(&case,metadata);
    let mut plan=NoSubmitExecutionAdapter.build_transaction_plan(&request)?;
    plan.swap_blueprint=Some(crate::execution_swap_blueprint::build_execution_swap_blueprint(&request)?);
    let body=crate::execution_swap_http_request::swap_request_body(&plan,&request.wallet_pubkey,"local-fee25")?;
    assert_eq!(body["prioritizationFeeLamports"],50_000);
    assert!(body.get("computeUnitPriceMicroLamports").is_none());
    let payload=crate::app_tests::priority_fee_fixture::transaction([8;32],1_000_000,50_000);
    proof::prove(&request,&payload,50_000)?;
    let above=crate::app_tests::priority_fee_fixture::transaction([8;32],1_000_000,50_001);
    assert!(proof::prove(&request,&above,50_000).unwrap_err().to_string().contains("priority_fee_cap_exceeded"));
    Ok(())
}

#[tokio::test]
async fn native_cohort_fee25_late_saved_quote_keeps_price_refusal() -> Result<()> {
    let case=fixture::setup()?;
    let metadata=fixture::metadata(&case,&case.config,"late",None).await?;
    assert_eq!(metadata.priority_fee_lamports,Some(50_000));
    assert!((metadata.slippage_bps.unwrap()-78.777).abs()<0.001);
    assert_eq!(metadata.decision_status.as_deref(),Some("would_skip"));
    assert_eq!(gate(&case.config,&metadata),Some("entry_decision_not_execute"));
    Ok(())
}

#[tokio::test]
async fn native_cohort_fee25_explicit_bad_sample_is_not_repaired() -> Result<()> {
    for (raw,lamports) in [
        ("{}".to_owned(),None),
        (json!({"version":1,"source":"provided","unit":"micro_lamports_per_compute_unit","value":1}).to_string(),Some(1)),
        (json!({"version":1,"source":"provided","unit":"total_priority_fee_lamports","value":2}).to_string(),Some(1)),
    ] {
        let case=fixture::setup()?;
        let sample=PriorityFeeSample{status:"ok".into(),lamports,json:Some(raw.clone()),error:None};
        let metadata=fixture::metadata(&case,&case.config,"early",Some(&sample)).await?;
        assert_eq!(metadata.priority_fee_json.as_deref(),Some(raw.as_str()));
        assert_eq!(gate(&case.config,&metadata),Some("missing_priority_fee"));
    }
    Ok(())
}

#[test]
fn native_cohort_fee25_scope_exclusions_do_not_mint_configured_fee() -> Result<()> {
    for mode in 0..8 {
        let case=fixture::setup()?;
        let mut config=case.config.clone();let mut signal=case.signal.clone();
        match mode {
            0=>config.priority_fee_canary_enabled=true,
            1=>config.technical_cohort=None,
            2=>config.technical_cohort.as_mut().unwrap().activate=false,
            3=>config.tiny_experiment.activate=false,
            4=>config.tiny_experiment.id=Some("foreign-run".into()),
            5=>config.technical_cohort.as_mut().unwrap().wallet_ids=vec!["foreign-wallet".into()],
            6=>signal.side="sell".into(),
            7=>{signal.status="shadow".into();signal.signal_id="owner-buy:local".into();},
            _=>unreachable!(),
        }
        let (event,_)=ExecutionQuoteCanaryRunner::new(config)
            .initial_entry_quote_event(&case.store,&signal,case.now,None)?;
        assert!(event.priority_fee_status.is_none(),"mode={mode}");
        assert!(event.priority_fee_json.is_none(),"mode={mode}");
    }
    Ok(())
}

#[test]
fn native_cohort_fee25_uses_existing_lower_cap_and_refuses_invalid_cap() -> Result<()> {
    for cap in [1,22_000,50_000,0,50_001] {
        let case=fixture::setup()?;let mut config=case.config.clone();
        config.pretrade_max_priority_fee_lamports=cap;
        let event=ExecutionQuoteCanaryRunner::new(config)
            .initial_entry_quote_event(&case.store,&case.signal,case.now,None);
        if (1..=50_000).contains(&cap) {assert_eq!(event?.0.priority_fee_lamports,Some(cap));}
        else {assert!(event.err().unwrap().to_string().contains("native_cohort_priority_fee_cap"));}
    }
    Ok(())
}

#[tokio::test]
async fn native_cohort_fee25_guard_expiry_and_foreign_binding_still_refuse() -> Result<()> {
    let case=fixture::setup()?;
    assert!(!case.guard.check_at(&case.store,case.now+Duration::seconds(121))?);
    let mut foreign=case.config.clone();foreign.technical_cohort.as_mut().unwrap().run_id="foreign".into();
    let guard=crate::execution_canary_route::NativeBuyGuard::new(&foreign,&case.signal.signal_id,
        &format!("native-buy-decision-v1:{}",case.signal.signal_id.strip_prefix("native-buy-v1:").unwrap()),case.now)?;
    assert!(!guard.check(&case.store)?);
    let mut signal=case.signal.clone();signal.signal_id="native-buy-v1:absent".into();
    assert!(ExecutionQuoteCanaryRunner::new(case.config.clone())
        .initial_entry_quote_event(&case.store,&signal,case.now,None).is_err());
    Ok(())
}

#[tokio::test]
async fn native_cohort_fee25_durable_proof_binds_payload_and_identity() -> Result<()> {
    let case=fixture::setup()?;
    let metadata=fixture::metadata(&case,&case.config,"early",None).await?;
    let request=request(&case,metadata);
    let plan=NoSubmitExecutionAdapter.build_transaction_plan(&request)?;
    let payload=crate::app_tests::priority_fee_fixture::transaction([8;32],1_000_000,50_000);
    let mut envelope=crate::execution_signing_envelope::build_signed_transaction_execution_envelope(
        &request,&plan,crate::execution_signing_envelope::ExecutionSignedTransactionPayload{
            signed_transaction_base64:payload.clone(),tx_signature_hint:Some("local-placeholder".into())})?;
    envelope.priority_fee_proof=Some(proof::prove(&request,&payload,50_000)?);
    proof::persist(&case.store,&request,&plan,&envelope,50_000,Utc::now())?;
    let intent=execution_submit_intent_from_signed_envelope(&request,&envelope,"local-fixture".into())?;
    proof::validate_submit(&case.store,&request,&envelope,&intent,50_000)?;
    let mut altered=request.clone();altered.client_order_id="foreign-client".into();
    assert!(proof::validate_submit(&case.store,&altered,&envelope,&intent,50_000).is_err());
    let changed=crate::app_tests::priority_fee_fixture::transaction([8;32],1_000_000,49_999);
    let mut changed_envelope=envelope.clone();changed_envelope.signed_transaction_base64=Some(changed.clone());
    let mut changed_intent=intent.clone();changed_intent.signed_transaction_base64=changed;
    assert!(proof::validate_submit(&case.store,&request,&changed_envelope,&changed_intent,50_000).is_err());
    Ok(())
}
