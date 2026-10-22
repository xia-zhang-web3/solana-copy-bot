//! Synthetic signing/confirmation only; no real wallet key or external transport.
use super::{owner_exit_test_fixture as old,source_recovery_fixture::Server,wsol_lifecycle_fixture as w};
use crate::execution_submit_adapter::*;
use crate::execution_signing_envelope::*;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD,Engine};
use ed25519_dalek::Signer;
use serde_json::json;

pub(super) struct Adapter<'a>{pub config:copybot_config::ExecutionConfig,pub server:&'a Server}
impl ExecutionSubmitAdapter for Adapter<'_>{
    fn native_floor_config(&self)->Result<&copybot_config::ExecutionConfig>{Ok(&self.config)}
    fn priority_fee_cap(&self)->u64{self.config.pretrade_max_priority_fee_lamports}
    fn build_transaction_plan(&self,r:&ExecutionSubmitRequest)->Result<ExecutionTransactionPlan>{
        JupiterMetisDryRunExecutionAdapter::new(self.config.clone()).build_transaction_plan(r)
    }
    fn simulate_transaction_plan<'a>(&'a self,_:&'a ExecutionTransactionPlan)->ExecutionSimulationFuture<'a>{
        Box::pin(async{Ok(ExecutionSimulationResult{status:"ok".into(),error:None})})
    }
    fn build_signing_envelope(&self,r:&ExecutionSubmitRequest,p:&ExecutionTransactionPlan)->Result<ExecutionSigningEnvelope>{
        let wallet=old::key().verifying_key().to_bytes();
        assert_eq!(r.wallet_pubkey,old::wallet(),"only a public synthetic key can be used here");
        let mut ix=old::instructions(wallet)?;
        ix.iter_mut().find(|i|i.data.starts_with(&[229,23,203,151,122,227,173,42])).unwrap()
            .data[16..24].copy_from_slice(&r.metadata.quote_in_amount_raw.as_ref().unwrap().parse::<u64>()?.to_le_bytes());
        let wire=w::build(&self.config,r,&ix)?;
        let mut raw=STANDARD.decode(wire)?;
        let signature=old::key().sign(&raw[65..]);raw[1..65].copy_from_slice(&signature.to_bytes());
        let payload=STANDARD.encode(raw);let sig=bs58::encode(signature.to_bytes()).into_string();
        let mut bank=super::wsol_lifecycle_oracle::Bank::new(wallet)?;
        bank.canonical(77_777_777); // unrelated owner WSOL survives the modeled recovery
        let mut receipt=bank.execute(&payload)?;
        receipt["result"]["transaction"]["signatures"]=json!([sig]);receipt["result"]["slot"]=json!(151);
        self.server.state.lock().unwrap().receipt=receipt;
        let mut e=build_signed_transaction_execution_envelope(r,p,ExecutionSignedTransactionPayload{
            signed_transaction_base64:payload.clone(),tx_signature_hint:Some(sig)})?;
        e.priority_fee_proof=Some(crate::execution_priority_fee_proof::prove(r,&payload,self.priority_fee_cap())?);
        Ok(e)
    }
    fn plan_submit(&self,r:&ExecutionSubmitRequest)->Result<ExecutionSubmitPlan>{NoSubmitExecutionAdapter.plan_submit(r)}
    fn plan_submit_with_envelope(&self,r:&ExecutionSubmitRequest,e:&ExecutionSigningEnvelope)->Result<ExecutionSubmitPlan>{
        Ok(ExecutionSubmitPlan::SubmitReady(execution_submit_intent_from_signed_envelope(r,e,"LOCAL synthetic SOURCE recovery".into())?))
    }
}
