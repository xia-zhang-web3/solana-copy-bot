use super::generic_sell_fixture::*;
use super::generic_sell_loopback::*;
use super::generic_sell_test_support::*;
use crate::execution_submit_adapter::*;
use anyhow::Result;
use serde_json::Value;
#[tokio::test]
async fn batch116_actual_slot_presign_guards_precede_counting_boundary() -> Result<()> {
    let r = run(Replies::default(), |_| {}, |_| {}).await?;
    assert!(r.result.is_ok());
    let boundary = CountingBoundary {
        config: r.config.clone(),
        calls: std::cell::Cell::new(0),
    };
    let envelope = boundary.build_signing_envelope(&r.request, &r.plan)?;
    assert_eq!(boundary.calls.get(), 1);
    assert_eq!(envelope.serialized_transaction_base64, r.payload());
    assert!(envelope.priority_fee_proof.is_some());
    for arm in ["attempt", "old-v0", "fee"] {
        boundary.calls.set(0);
        let mut request = r.request.clone();
        let mut plan = r.plan.clone();
        plan.serialized_transaction_payload_slot=Some(crate::execution_serialized_transaction_slot::ExecutionSerializedTransactionPayloadSlot::new());
        let payer = crate::execution_instruction_bundle::pubkey(PAYER)?;
        let bytes = match arm {
            "old-v0" => serde_json::from_str::<Value>(OLD_SWAP)?["swapTransaction"]
                .as_str()
                .unwrap()
                .to_owned(),
            "fee" => super::priority_fee_fixture::transaction(payer, 200_000, 110_001),
            _ => {
                request.attempt += 1;
                r.payload().unwrap()
            }
        };
        plan.serialized_transaction_payload_slot
            .as_ref()
            .unwrap()
            .store(
                crate::execution_signing_envelope::ExecutionSerializedTransactionPayload {
                    source: "test-tampered-slot".into(),
                    serialized_transaction_base64: bytes,
                },
            )?;
        assert!(
            boundary.build_signing_envelope(&request, &plan).is_err(),
            "{arm}"
        );
        // Existing envelope alignment for attempt runs after the no-key boundary;
        // fee refusal always precedes it. This test does not claim otherwise.
        assert_eq!(boundary.calls.get(), usize::from(arm == "attempt"), "{arm}");
    }
    Ok(())
}
struct CountingBoundary {
    config: copybot_config::ExecutionConfig,
    calls: std::cell::Cell<usize>,
}
impl ExecutionSubmitAdapter for CountingBoundary {
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
    fn native_floor_config(&self) -> Result<&copybot_config::ExecutionConfig> {
        Ok(&self.config)
    }
    fn priority_fee_cap(&self) -> u64 {
        self.config.pretrade_max_priority_fee_lamports
    }
    fn sign_serialized_transaction(
        &self,
        _: &ExecutionSubmitRequest,
        _: &ExecutionTransactionPlan,
        _: &crate::execution_signing_envelope::ExecutionSerializedTransactionPayload,
    ) -> Result<Option<crate::execution_signing_envelope::ExecutionSignedTransactionPayload>> {
        self.calls.set(self.calls.get() + 1);
        Ok(None) // no keys, no signature, no send
    }
}
