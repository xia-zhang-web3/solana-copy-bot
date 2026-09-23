//! Synthetic unsigned build and signature bytes for the native runner test.
use crate::execution_signing_envelope::{
    build_signed_transaction_execution_envelope, ExecutionSignedTransactionPayload,
    ExecutionSigningEnvelope,
};
use crate::execution_submit_adapter::{
    execution_submit_intent_from_signed_envelope, ExecutionSimulationFuture,
    ExecutionSimulationResult, ExecutionSubmitAdapter, ExecutionSubmitPlan,
    ExecutionSubmitRequest, ExecutionTransactionPlan, NoSubmitExecutionAdapter,
};
use anyhow::Result;
use copybot_config::ExecutionConfig;

#[derive(Debug)]
pub(crate) struct NativeBuyMockAdapter {
    pub config: ExecutionConfig,
    pub payload: String,
    pub signature: String,
    pub counts: std::sync::Arc<std::sync::Mutex<super::NativeBuyMockCounts>>,
}

impl ExecutionSubmitAdapter for NativeBuyMockAdapter {
    fn native_floor_config(&self) -> Result<&ExecutionConfig> { Ok(&self.config) }
    fn priority_fee_cap(&self) -> u64 { self.config.pretrade_max_priority_fee_lamports }
    fn build_transaction_plan(&self, request: &ExecutionSubmitRequest) -> Result<ExecutionTransactionPlan> {
        self.counts.lock().unwrap().unsigned_build += 1;
        NoSubmitExecutionAdapter.build_transaction_plan(request)
    }
    fn simulate_transaction_plan<'a>(&'a self, _: &'a ExecutionTransactionPlan) -> ExecutionSimulationFuture<'a> {
        Box::pin(async { Ok(ExecutionSimulationResult { status: "ok".into(), error: None }) })
    }
    fn build_signing_envelope(&self, request: &ExecutionSubmitRequest, plan: &ExecutionTransactionPlan) -> Result<ExecutionSigningEnvelope> {
        self.counts.lock().unwrap().signing_envelope += 1;
        let mut envelope = build_signed_transaction_execution_envelope(request, plan,
            ExecutionSignedTransactionPayload { signed_transaction_base64: self.payload.clone(),
                tx_signature_hint: Some(self.signature.clone()) })?;
        envelope.priority_fee_proof = Some(crate::execution_priority_fee_proof::prove(
            request, &self.payload, self.priority_fee_cap())?);
        Ok(envelope)
    }
    fn plan_submit(&self, request: &ExecutionSubmitRequest) -> Result<ExecutionSubmitPlan> {
        NoSubmitExecutionAdapter.plan_submit(request)
    }
    fn plan_submit_with_envelope(&self, request: &ExecutionSubmitRequest,
        envelope: &ExecutionSigningEnvelope) -> Result<ExecutionSubmitPlan> {
        Ok(ExecutionSubmitPlan::SubmitReady(execution_submit_intent_from_signed_envelope(
            request, envelope, "native-runner-test".into())?))
    }
}
