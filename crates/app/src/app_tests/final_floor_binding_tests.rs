use super::final_floor_fixture::*;
use crate::execution_native_setup::interpret_native_setup;
use crate::execution_priority_fee_proof::prove;
use crate::execution_signing_envelope::{
    build_serialized_transaction_execution_envelope, validate_execution_signing_envelope,
    ExecutionSerializedTransactionPayload,
};
use crate::execution_submit_adapter::{ExecutionSubmitAdapter, NoSubmitExecutionAdapter};
use anyhow::Result;

#[test]
fn final_floor_old_byte_proof_and_unsigned_envelope_do_not_match_new_message() -> Result<()> {
    for buy in [false, true] {
        for extension in [false, true] {
            let mut instructions = direct(buy, extension, 10_000_000)?;
            let old = payload(&instructions)?;
            instructions.push(transfer(WALLET, WALLET, 2_000_000));
            let next = payload(&instructions)?;
            let request = request(buy);
            let plan = NoSubmitExecutionAdapter.build_transaction_plan(&request)?;
            let old_proof = prove(&request, &old, 500_000)?;
            let next_proof = prove(&request, &next, 500_000)?;
            assert_ne!(old_proof, next_proof);
            let mut envelope = build_serialized_transaction_execution_envelope(
                &request,
                &plan,
                ExecutionSerializedTransactionPayload {
                    source: "b23-synthetic-layout".into(),
                    serialized_transaction_base64: old,
                },
            )?;
            validate_execution_signing_envelope(&envelope, &request, &plan)?;
            assert!(envelope.signed_transaction_base64.is_none());
            assert!(envelope.tx_signature_hint.is_none());
            envelope.serialized_transaction_base64 = Some(next);
            let error =
                validate_execution_signing_envelope(&envelope, &request, &plan).unwrap_err();
            assert_eq!(
                error.to_string(),
                "signing envelope payload fingerprint mismatch"
            );
            // Signed message bytes changed; we neither fabricate a valid old signature
            // nor call a signer to produce a new one in app layout tests.
        }
    }
    Ok(())
}

#[tokio::test]
async fn final_floor_old_fee_accounts_binding_rejects_guarded_payload() -> Result<()> {
    for buy in [false, true] {
        for extension in [false, true] {
            let mut instructions = direct(buy, extension, 10_000_000)?;
            let old = payload(&instructions)?;
            let input = super::native_setup_rpc_fixture::responses(&old)?;
            let facts = input.collect(&old).await?;
            interpret_native_setup(&old, WALLET, &facts)?;
            instructions.push(transfer(WALLET, WALLET, 2_000_000));
            let next = payload(&instructions)?;
            let error = interpret_native_setup(&next, WALLET, &facts).unwrap_err();
            assert_eq!(error.to_string(), "native_setup_requirements_mismatch");
            let fresh = input.collect(&next).await?;
            interpret_native_setup(&next, WALLET, &fresh)?;
            assert_eq!(fresh.fee().value, facts.fee().value);
            assert_eq!(fresh.requested_keys(), facts.requested_keys());
            assert_ne!(
                fresh.requirements().binding.message_sha256,
                facts.requirements().binding.message_sha256
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn final_floor_simulation_records_different_bytes_and_new_outcome() -> Result<()> {
    use super::rpc_simulation_http_fixture::{valid, Server, Step};
    use crate::execution_transaction_rpc_simulation::{
        verify_serialized_transaction_rpc_simulation as simulate, RpcSimulationOutcome,
    };
    use copybot_config::ExecutionConfig;
    use serde_json::json;
    use std::time::Duration;

    for buy in [false, true] {
        for extension in [false, true] {
            let mut instructions = direct(buy, extension, 10_000_000)?;
            let old = payload(&instructions)?;
            let guard_index = instructions.len();
            instructions.push(transfer(WALLET, WALLET, 2_000_000));
            let next = payload(&instructions)?;
            let mut rejected = valid();
            rejected["result"]["value"]["err"] =
                json!({"InstructionError":[guard_index,{"Custom":1}]});
            let server = Server::new(vec![
                Step::json("simulateTransaction", valid()),
                Step::json("simulateTransaction", rejected),
            ])
            .await?;
            let config = ExecutionConfig {
                canary_tiny_submit_enabled: true,
                submit_adapter_http_url: server.url.clone(),
                ..Default::default()
            };
            let client = reqwest::Client::new();
            let before = simulate(
                &client,
                &config,
                &old,
                "b23-loopback-protocol",
                Duration::from_secs(2),
            )
            .await;
            let after = simulate(
                &client,
                &config,
                &next,
                "b23-loopback-protocol",
                Duration::from_secs(2),
            )
            .await;
            let requests = server.finish().await?;
            assert_eq!(before?, RpcSimulationOutcome::Passed { slot: 0 });
            assert!(after
                .unwrap_err()
                .to_string()
                .contains("RPC simulation failed"));
            assert_eq!(requests.len(), 2);
            assert_eq!(requests[0]["params"][0], old);
            assert_eq!(requests[1]["params"][0], next);
            assert_ne!(requests[0]["params"][0], requests[1]["params"][0]);
            // Synthetic RPC protocol control only: not PumpSwap or Bank execution.
            // Passed is not a cached byte-bound proof; the new message needs a new call.
        }
    }
    Ok(())
}
