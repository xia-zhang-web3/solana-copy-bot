use super::priority_fee_route_fixture::{Fixture, Route};
use crate::execution_canary_submit_contract::{
    record_execution_tiny_submit_plan, ExecutionTinySubmitGate,
};
use crate::execution_signing_envelope::*;
use crate::execution_solana_tx::{
    serialize_unsigned_legacy_transaction, SolanaAccountMeta, SolanaInstruction,
};
use crate::execution_submit_adapter::*;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use ed25519_dalek::{Signer, SigningKey};

// Deliberately custom SubmitReady adapter: it has no native-floor policy override/proof.
struct Ready {
    rewrite: Option<String>,
}
impl ExecutionSubmitAdapter for Ready {
    fn build_transaction_plan(
        &self,
        request: &ExecutionSubmitRequest,
    ) -> Result<ExecutionTransactionPlan> {
        NoSubmitExecutionAdapter.build_transaction_plan(request)
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
    fn plan_submit_with_envelope(
        &self,
        request: &ExecutionSubmitRequest,
        envelope: &ExecutionSigningEnvelope,
    ) -> Result<ExecutionSubmitPlan> {
        let mut intent =
            execution_submit_intent_from_signed_envelope(request, envelope, "synthetic".into())?;
        if let Some(payload) = &self.rewrite {
            intent.signed_transaction_base64 = payload.clone();
        }
        Ok(ExecutionSubmitPlan::SubmitReady(intent))
    }
}

fn payload(case: &str) -> Result<ExecutionSignedTransactionPayload> {
    let key = SigningKey::from_bytes(&[if case == "wallet" { 12 } else { 11 }; 32]);
    let payer = key.verifying_key().to_bytes();
    let mut instructions = super::priority_fee_fixture::budget(200_000, 10_000);
    if case != "removed" {
        let reserve = if case == "amount" {
            50_000_002_u64
        } else {
            50_000_001
        };
        instructions.push(SolanaInstruction {
            program_id: [0; 32],
            accounts: vec![
                SolanaAccountMeta::signer_writable(payer),
                SolanaAccountMeta::writable(payer),
            ],
            data: [2_u32.to_le_bytes().to_vec(), reserve.to_le_bytes().to_vec()].concat(),
        });
        if case == "early" {
            let price = instructions.remove(1);
            instructions.push(price);
        }
    }
    let mut wire = serialize_unsigned_legacy_transaction(payer, [9; 32], &instructions)?;
    let signature = key.sign(&wire[65..]);
    key.verifying_key().verify_strict(&wire[65..], &signature)?;
    wire[1..65].copy_from_slice(&signature.to_bytes());
    Ok(ExecutionSignedTransactionPayload {
        signed_transaction_base64: STANDARD.encode(wire),
        tx_signature_hint: Some(bs58::encode(signature.to_bytes()).into_string()),
    })
}

fn reseal(f: &Fixture, case: &str) -> Result<ExecutionSigningEnvelope> {
    let plan = NoSubmitExecutionAdapter.build_transaction_plan(&f.request)?;
    let mut envelope =
        build_signed_transaction_execution_envelope(&f.request, &plan, payload(case)?)?;
    let proof = crate::execution_priority_fee_proof::prove(
        &f.request,
        envelope.signed_transaction_base64.as_deref().unwrap(),
        f.config.pretrade_max_priority_fee_lamports,
    )?;
    envelope.priority_fee_proof = Some(proof);
    crate::execution_priority_fee_proof::persist(
        &f.store,
        &f.request,
        &plan,
        &envelope,
        f.config.pretrade_max_priority_fee_lamports,
        f.now,
    )?;
    Ok(envelope)
}

#[tokio::test]
async fn native_floor_actual_submit_intent_requires_current_policy_and_final_guard() -> Result<()> {
    for (case, expected) in [
        ("removed", "native_floor_program"),
        ("early", "native_floor_program"),
        ("wallet", "native_floor_wallet_payer"),
        ("amount", "native_floor_reserve_mismatch"),
        ("current_r", "native_floor_reserve_mismatch"),
        ("invalid_r", "native_floor_invalid_policy"),
        ("default_gate", "native_floor_invalid_policy"),
        ("intent", "priority_fee_submit_payload_changed"),
        ("guarded", ""),
        (
            "funding_zero",
            "initial_sol_insufficient:observed=0:required=64097561:shortfall=64097561",
        ),
        ("missing_funding_policy", "initial_sol_policy_unavailable"),
    ] {
        let mut f = Fixture::new(Route::Direct, 200_000, 200_000).await?;
        if case == "funding_zero" {
            f.funding.lock().unwrap().balance = 0;
        }
        let original = f.build().await?.envelope.unwrap();
        let envelope = if matches!(case, "removed" | "early" | "wallet" | "amount" | "guarded") {
            reseal(&f, case)?
        } else {
            original
        };
        let mut gate = ExecutionTinySubmitGate::from_config(&f.config);
        match case {
            "current_r" => gate.pretrade_min_sol_reserve = 0.075,
            "missing_funding_policy" => gate.buy_safety_config = None,
            "invalid_r" => gate.pretrade_min_sol_reserve = f64::NAN,
            "default_gate" => {
                gate = ExecutionTinySubmitGate {
                    allow_rpc_submit: true,
                    ..Default::default()
                }
            }
            _ => {}
        }
        let adapter = Ready {
            rewrite: if case == "intent" {
                Some(payload("removed")?.signed_transaction_base64)
            } else {
                None
            },
        };
        // All semantic negative controls have complete, current durable priority/equality proof.
        if case != "intent" {
            let ExecutionSubmitPlan::SubmitReady(intent) =
                adapter.plan_submit_with_envelope(&f.request, &envelope)?
            else {
                unreachable!()
            };
            crate::execution_priority_fee_proof::validate_submit(
                &f.store,
                &f.request,
                &envelope,
                &intent,
                gate.pretrade_max_priority_fee_lamports,
            )?;
        }
        let out = record_execution_tiny_submit_plan(
            &f.store,
            &adapter,
            &f.request,
            &envelope,
            &gate,
            &RpcExecutionSubmitTransport::new(f.config.submit_adapter_http_url.clone()),
            f.now,
        )
        .await?;
        f.finish().await?;
        if case == "guarded" {
            assert_eq!(out.submitted, 1, "{out:?}");
            assert_eq!(f.sends(), 1);
        } else {
            assert_eq!(out.failed, 1, "{case}: {out:?}");
            assert_eq!(out.error.as_deref(), Some(expected), "{case}");
            assert_eq!(f.sends(), 0, "{case}");
            assert!(f
                .store
                .load_execution_canary_order(&f.request.order_id)?
                .unwrap()
                .tx_signature
                .is_none());
        }
        let funding_calls = f
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, r)| {
                r["id"]
                    .as_str()
                    .is_some_and(|id| id.starts_with("native-funding-"))
            })
            .count();
        assert_eq!(
            funding_calls,
            if matches!(case, "guarded" | "funding_zero") {
                3
            } else {
                0
            },
            "{case}"
        );
        assert_eq!(
            f.conn()?.query_row(
                "SELECT COUNT(*) FROM execution_failed_expense_ledger",
                [],
                |r| r.get::<_, i64>(0)
            )?,
            0
        );
    }
    Ok(())
}
