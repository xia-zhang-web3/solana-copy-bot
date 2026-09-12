use super::generic_buy_fixture::*;
use super::generic_buy_loopback::*;
use super::generic_buy_test_support::*;
use crate::execution_instruction_bundle_binding::BundleRequest;
use crate::execution_submit_adapter::*;
use anyhow::Result;
use serde_json::{json, Value};

#[tokio::test]
async fn batch111_actual_adapter_rejects_quote_blueprint_wallet_mismatch() -> Result<()> {
    for field in [
        "inAmount",
        "outAmount",
        "inputMint",
        "outputMint",
        "slippageBps",
        "routePlan",
        "swapMode",
        "otherAmountThreshold",
    ] {
        let r = run(
            Replies::default(),
            "buy",
            |_| {},
            |plan| {
                let mut quote: Value =
                    serde_json::from_str(plan.metadata.quote_response_json.as_ref().unwrap())
                        .unwrap();
                quote[field] = match field {
                    "slippageBps" => json!(501),
                    "routePlan" => json!([]),
                    "otherAmountThreshold" => json!("0"),
                    _ => json!("wrong"),
                };
                plan.metadata.quote_response_json = Some(quote.to_string());
            },
        )
        .await?;
        r.assert_rejected(field, 0);
        assert_eq!(r.server.count("swap-instructions"), 0);
    }
    for field in ["wallet", "quote-event", "amount", "fee"] {
        let r = run(
            Replies::default(),
            "buy",
            |_| {},
            |plan| match field {
                "wallet" => {
                    plan.swap_blueprint.as_mut().unwrap().wallet_pubkey =
                        Some(bs58::encode([3; 32]).into_string())
                }
                "quote-event" => plan.metadata.quote_event_id = Some("other".into()),
                "amount" => plan.metadata.quote_in_amount_raw = Some("123".into()),
                "fee" => {
                    plan.metadata.priority_fee_json =
                        Some(super::priority_fee_fixture::total_json(1))
                }
                _ => unreachable!(),
            },
        )
        .await?;
        r.assert_rejected(field, 0);
    }
    for which in [0, 1, 2] {
        let r = run(
            Replies::default(),
            "buy",
            |config| match which {
                0 => config.execution_signer_pubkey = bs58::encode([3; 32]).into_string(),
                1 => config.canary_wallet_pubkey = bs58::encode([3; 32]).into_string(),
                _ => config.pretrade_min_sol_reserve = 0.0,
            },
            |_| {},
        )
        .await?;
        r.assert_rejected("current-wallet-policy", 0);
    }
    Ok(())
}

#[test]
fn batch111_bound_bundle_cannot_cross_plan_attempt_or_quote() -> Result<()> {
    let cfg = config("http://127.0.0.1:1");
    let request = request("buy", serde_json::from_str(QUOTE)?)?;
    let plan =
        JupiterMetisDryRunExecutionAdapter::new(cfg.clone()).build_transaction_plan(&request)?;
    let bundle = BundleRequest::capture(&plan)?.bind(&serde_json::from_str(INSTRUCTIONS)?)?;
    for change in 0..10 {
        let mut changed = plan.clone();
        match change {
            0 => changed.attempt += 1,
            1 => changed.plan_id.push('x'),
            2 => changed.order_id.push('x'),
            3 => changed.client_order_id.push('x'),
            4 => changed.wallet_pubkey = bs58::encode([3; 32]).into_string(),
            5 => changed.metadata.quote_event_id = Some("other".into()),
            6 => changed.metadata.quote_response_json = Some("{}".into()),
            7 => {
                changed.metadata.http_request_started_ts =
                    Some(chrono::DateTime::from_timestamp(1, 0).unwrap())
            }
            8 => changed.buy_size_sol = 0.02,
            _ => changed.submit_enabled = true,
        }
        assert!(
            crate::execution_guarded_generic_buy::assemble(&cfg, &changed, &bundle, 50_000_001)
                .is_err(),
            "change{change}"
        );
        assert!(changed
            .serialized_transaction_payload_slot
            .as_ref()
            .unwrap()
            .load()?
            .is_none());
    }
    let (_, instructions) = bundle.verified_parts(&plan)?;
    let payer = crate::execution_instruction_bundle::pubkey(PAYER)?;
    let raw = crate::execution_solana_tx::serialize_unsigned_legacy_transaction(
        payer,
        instructions.blockhash(),
        instructions.instructions(),
    )?;
    assert_eq!(raw.len(), 1180);
    let result = crate::execution_guarded_generic_buy::assemble(&cfg, &plan, &bundle, 50_000_001)?;
    assert_eq!(
        super::generic_buy_decode::verify(
            &result.serialized_transaction_base64,
            &serde_json::from_str(INSTRUCTIONS)?,
            PAYER,
            50_000_001
        ),
        1197
    );
    let mut changed_policy = cfg;
    changed_policy.pretrade_min_sol_reserve = 0.075;
    assert!(crate::execution_guarded_generic_buy::assemble(
        &changed_policy,
        &plan,
        &bundle,
        50_000_001
    )
    .is_err());
    Ok(())
}

#[tokio::test]
async fn batch111_actual_slot_presign_guards_precede_counting_boundary() -> Result<()> {
    let r = run(Replies::default(), "buy", |_| {}, |_| {}).await?;
    assert!(r.result.is_ok());
    let boundary = CountingBoundary {
        config: r.config.clone(),
        calls: std::cell::Cell::new(0),
    };
    let envelope = boundary.build_signing_envelope(&r.request, &r.plan)?;
    assert_eq!(boundary.calls.get(), 1);
    assert_eq!(envelope.serialized_transaction_base64, r.payload());
    assert!(envelope.priority_fee_proof.is_some());
    for arm in ["attempt", "old-v0", "fee", "floor"] {
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
            "fee" => super::priority_fee_fixture::guarded_transaction(payer, 200_000, 110_001),
            "floor" => super::priority_fee_fixture::transaction(payer, 200_000, 110_000),
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
        // fee/floor refusal always precedes it. This test does not claim otherwise.
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
