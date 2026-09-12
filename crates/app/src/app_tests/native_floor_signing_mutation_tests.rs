use super::priority_fee_route_fixture::{Fixture, Route};
use crate::execution_signing_envelope::*;
use crate::execution_submit_adapter::*;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use copybot_config::ExecutionConfig;
use ed25519_dalek::{Signer, SigningKey};
use std::sync::atomic::{AtomicUsize, Ordering};

struct ChangingSigner<'a> {
    config: &'a ExecutionConfig,
    calls: AtomicUsize,
}
impl ExecutionSubmitAdapter for ChangingSigner<'_> {
    fn native_floor_config(&self) -> Result<&ExecutionConfig> {
        Ok(self.config)
    }
    fn build_transaction_plan(
        &self,
        _: &ExecutionSubmitRequest,
    ) -> Result<ExecutionTransactionPlan> {
        unreachable!()
    }
    fn simulate_transaction_plan<'a>(
        &'a self,
        _: &'a ExecutionTransactionPlan,
    ) -> ExecutionSimulationFuture<'a> {
        unreachable!()
    }
    fn plan_submit(&self, _: &ExecutionSubmitRequest) -> Result<ExecutionSubmitPlan> {
        unreachable!()
    }
    fn sign_serialized_transaction(
        &self,
        _: &ExecutionSubmitRequest,
        _: &ExecutionTransactionPlan,
        payload: &ExecutionSerializedTransactionPayload,
    ) -> Result<Option<ExecutionSignedTransactionPayload>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut wire = STANDARD.decode(&payload.serialized_transaction_base64)?;
        let decoded = crate::execution_transaction_wire::decode_message(
            &payload.serialized_transaction_base64,
            |_| Ok(()),
        )?;
        assert_eq!(usize::from(wire[68]), decoded.binding.accounts.len());
        wire[69 + 32 * decoded.binding.accounts.len()] ^= 1; // fresh blockhash, guard/wallet/R unchanged
        let key = SigningKey::from_bytes(&[11; 32]);
        let signature = key.sign(&wire[65..]);
        wire[1..65].copy_from_slice(&signature.to_bytes());
        Ok(Some(ExecutionSignedTransactionPayload {
            signed_transaction_base64: STANDARD.encode(wire),
            tx_signature_hint: Some(bs58::encode(signature.to_bytes()).into_string()),
        }))
    }
}

struct MissingPolicy;
impl ExecutionSubmitAdapter for MissingPolicy {
    fn build_transaction_plan(
        &self,
        _: &ExecutionSubmitRequest,
    ) -> Result<ExecutionTransactionPlan> {
        unreachable!()
    }
    fn simulate_transaction_plan<'a>(
        &'a self,
        _: &'a ExecutionTransactionPlan,
    ) -> ExecutionSimulationFuture<'a> {
        unreachable!()
    }
    fn plan_submit(&self, _: &ExecutionSubmitRequest) -> Result<ExecutionSubmitPlan> {
        unreachable!()
    }
    fn sign_serialized_transaction(
        &self,
        _: &ExecutionSubmitRequest,
        _: &ExecutionTransactionPlan,
        _: &ExecutionSerializedTransactionPayload,
    ) -> Result<Option<ExecutionSignedTransactionPayload>> {
        panic!("no signing without explicit policy")
    }
}

#[tokio::test]
async fn native_floor_postsign_rejects_message_change_and_missing_adapter_policy() -> Result<()> {
    let mut f = Fixture::new(Route::Direct, 200_000, 1_400_000).await?;
    let plan = f.adapter.build_transaction_plan(&f.request)?;
    f.adapter.simulate_transaction_plan(&plan).await?;
    let adapter = ChangingSigner {
        config: &f.config,
        calls: AtomicUsize::new(0),
    };
    assert_eq!(
        adapter
            .build_signing_envelope(&f.request, &plan)
            .unwrap_err()
            .to_string(),
        "native_floor_message_changed_after_signing"
    );
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        MissingPolicy
            .build_signing_envelope(&f.request, &plan)
            .unwrap_err()
            .to_string(),
        "native_floor_policy_unavailable"
    );
    f.finish().await?;
    assert_eq!(f.sends(), 0);
    Ok(())
}
