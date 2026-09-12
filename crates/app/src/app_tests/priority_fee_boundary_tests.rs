use crate::execution_quote_canary_priority_fee::PriorityFeeSampler;
use crate::execution_signing_envelope::ExecutionSerializedTransactionPayload;
use crate::execution_solana_tx::{serialize_unsigned_legacy_transaction, SolanaInstruction};
use crate::execution_submit_adapter::*;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use copybot_config::ExecutionConfig;
use serde_json::json;
use std::io::{Read, Write};

fn fee_transaction(limit: u32, price: u64) -> String {
    let budget = bs58::decode("ComputeBudget111111111111111111111111111111")
        .into_vec()
        .unwrap()
        .try_into()
        .unwrap();
    let mut limit_data = vec![2];
    limit_data.extend(limit.to_le_bytes());
    let mut price_data = vec![3];
    price_data.extend(price.to_le_bytes());
    STANDARD.encode(
        serialize_unsigned_legacy_transaction(
            [7; 32],
            [9; 32],
            &[
                SolanaInstruction {
                    program_id: budget,
                    accounts: vec![],
                    data: limit_data,
                },
                SolanaInstruction {
                    program_id: budget,
                    accounts: vec![],
                    data: price_data,
                },
            ],
        )
        .unwrap(),
    )
}

fn request() -> ExecutionSubmitRequest {
    ExecutionSubmitRequest {
        order_id: "fee-order".into(),
        signal_id: "fee-signal".into(),
        client_order_id: "fee-client".into(),
        attempt: 1,
        route: "metis-canary".into(),
        wallet_id: "leader".into(),
        token: "mint".into(),
        side: "buy".into(),
        buy_size_sol: 0.01,
        slippage_tolerance_bps: 500,
        wallet_pubkey: bs58::encode([7; 32]).into_string(),
        entry_route_plan_json: None,
        metadata: ExecutionBuildPlanMetadata {
            http_request_started_ts: None,
            quote_response_available_ts: None,
            priority_fee_status: Some("ok".into()),
            priority_fee_lamports: Some(120_000),
            priority_fee_json: Some(
                json!({"version":1,"source":"verified_total",
                "unit":"total_priority_fee_lamports","value":120000})
                .to_string(),
            ),
            ..Default::default()
        },
    }
}

#[tokio::test]
async fn priority_fee_sample_does_not_store_cu_price_as_total_lamports() -> Result<()> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = [0; 4096];
        stream.read(&mut buf).unwrap();
        let body = r#"{"jsonrpc":"2.0","id":1,"result":{"recommended":600000}}"#;
        write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
    });
    let mut config = ExecutionConfig::default();
    config.priority_fee_canary_enabled = true;
    config.priority_fee_canary_rpc_url = format!("http://{address}");
    let sample = PriorityFeeSampler::new(config, reqwest::Client::new())
        .sample_if_enabled()
        .await
        .unwrap();
    server.join().unwrap();
    assert_eq!(sample.status, "ok");
    assert_eq!(
        sample.lamports, None,
        "CU price cannot populate total lamports"
    );
    let tagged: serde_json::Value = serde_json::from_str(sample.json.as_deref().unwrap())?;
    assert_eq!(tagged["unit"], "micro_lamports_per_compute_unit");
    assert_eq!(tagged["value"], 600000);
    Ok(())
}

#[test]
fn priority_fee_over_cap_refused_before_key_file_access() -> Result<()> {
    let request = request();
    let plan = NoSubmitExecutionAdapter.build_transaction_plan(&request)?;
    let config = ExecutionConfig {
        execution_signer_pubkey: request.wallet_pubkey.clone(),
        execution_signer_keypair_path: "/nonexistent/synthetic-fee-test-key.json".into(),
        pretrade_max_priority_fee_lamports: 500_000,
        ..Default::default()
    };
    let payload = ExecutionSerializedTransactionPayload {
        source: "synthetic".into(),
        serialized_transaction_base64: fee_transaction(1_400_000, 600_000),
    };
    let error = sign_serialized_transaction_from_config(&config, &request, &plan, &payload)
        .unwrap_err()
        .to_string();
    assert!(error.contains("priority_fee_cap_exceeded"), "{error}");
    Ok(())
}

#[test]
fn priority_fee_formula_rounding_total_and_direct_budget_are_exact() -> Result<()> {
    use crate::execution_priority_fee::{PriorityFee as F, RequestedComputeUnitLimit as L};
    assert_eq!(
        F::MicroLamportsPerComputeUnit(600_000).total(L::checked(200_000)?)?,
        120_000
    );
    assert_eq!(
        F::MicroLamportsPerComputeUnit(600_000).total(L::checked(1_400_000)?)?,
        840_000
    );
    assert_eq!(
        F::TotalPriorityFeeLamports(120_000).total(L::checked(1_400_000)?)?,
        120_000
    );
    assert_eq!(F::MicroLamportsPerComputeUnit(1).total(L::checked(1)?)?, 1);
    assert_eq!(
        F::MicroLamportsPerComputeUnit(0).total(L::checked(1_400_000)?)?,
        0
    );
    assert!(F::MicroLamportsPerComputeUnit(u64::MAX)
        .total(L::checked(1_400_000)?)
        .is_err());
    assert!(L::checked(0).is_err());
    assert!(L::checked(1_400_001).is_err());
    for total in [0, 1, 120_000, 500_000, u64::MAX] {
        let limit = L::checked(1_400_000)?;
        let price = F::TotalPriorityFeeLamports(total).price_for_limit(limit)?;
        let encoded = crate::execution_priority_fee_wire::decode_priority_fee(&fee_transaction(
            limit.get(),
            price,
        ))?;
        assert!(encoded.total <= total);
        // Direct builder's floor is never silently increased to the next micro-lamport.
        assert_eq!(u128::from(price), u128::from(total) * 1_000_000 / 1_400_000);
    }
    assert!(F::TotalPriorityFeeLamports(u64::MAX)
        .price_for_limit(L::checked(1)?)
        .is_err());
    Ok(())
}

#[test]
fn priority_fee_cap_boundary_and_zero_still_require_complete_proof() -> Result<()> {
    let req = request();
    for (price, cap, allowed) in [
        (2_500_000, 500_000, true),
        (2_500_001, 500_000, false),
        (600_000, 0, true),
        (0, 1, true),
        (u64::MAX, 0, true),
    ] {
        assert_eq!(
            crate::execution_priority_fee_proof::prove(&req, &fee_transaction(200_000, price), cap)
                .is_ok(),
            allowed
        );
    }
    assert!(crate::execution_priority_fee_proof::prove(
        &req,
        &fee_transaction(1_400_000, u64::MAX),
        0
    )
    .is_err());
    assert!(crate::execution_priority_fee_proof::prove(&req, "AQIDBA==", 0).is_err());
    Ok(())
}

#[test]
fn priority_fee_samples_preserve_preference_and_block_ambiguous_history() -> Result<()> {
    use crate::execution_priority_fee::*;
    for (input, expected) in [
        (json!({"recommended":0,"per_compute_unit":{"high":99}}), 0),
        (
            json!({"per_compute_unit":{"high":"600000","medium":1}}),
            600000,
        ),
        (
            json!({"per_compute_unit":{"medium":42},"per_transaction":{"high":120000}}),
            42,
        ),
    ] {
        let (sample, tagged) = sample_quicknode_fee(&input)?;
        assert_eq!(sample, PriorityFee::MicroLamportsPerComputeUnit(expected));
        assert_eq!(tagged_fee(Some(&tagged))?, sample);
    }
    for input in [
        json!({}),
        json!({"recommended":-1}),
        json!({"recommended":0.1}),
        json!({"recommended":"18446744073709551616"}),
        json!({"recommended":"bad","per_compute_unit":{"high":1}}),
        json!({"per_transaction":{"high":120000}}),
    ] {
        assert!(sample_quicknode_fee(&input).is_err());
    }
    let mut req = request();
    req.metadata.priority_fee_json = Some(r#"{"recommended":120000}"#.into());
    assert!(metadata_fee(&req.metadata).is_err());
    assert!(
        crate::execution_priority_fee_proof::prove(&req, &fee_transaction(200_000, 1), 500000)
            .is_err()
    );
    req.metadata.priority_fee_json =
        Some(json!({"version":1,"source":"unknown","unit":"unknown","value":120000}).to_string());
    assert!(metadata_fee(&req.metadata).is_err());
    // A known total is used as a total and capping does not rewrite the original sample.
    req = request();
    let capped = cap_metadata_total(100000, req.metadata.clone());
    assert_eq!(
        metadata_fee(&capped)?,
        PriorityFee::TotalPriorityFeeLamports(100000)
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(capped.priority_fee_json.as_deref().unwrap())?
            ["value"],
        120000
    );
    assert_eq!(cap_metadata_total(0, req.metadata.clone()), req.metadata);
    Ok(())
}

#[test]
fn priority_fee_wire_rejects_duplicate_spoofed_missing_and_malformed_budget() -> Result<()> {
    use crate::execution_priority_fee_wire::decode_priority_fee;
    let good = super::priority_fee_fixture::budget(200_000, 600_000);
    let mut bad_cases = vec![vec![], vec![good[0].clone()], vec![good[1].clone()]];
    for index in [0, 1] {
        let mut duplicate = good.clone();
        duplicate.push(good[index].clone());
        bad_cases.push(duplicate);
        let mut malformed = good.clone();
        malformed[index].data.push(0);
        bad_cases.push(malformed);
        let mut wrong_id = good.clone();
        wrong_id[index].program_id = [3; 32];
        bad_cases.push(wrong_id);
        let mut accounts = good.clone();
        accounts[index]
            .accounts
            .push(crate::execution_solana_tx::SolanaAccountMeta::readonly(
                [8; 32],
            ));
        bad_cases.push(accounts);
    }
    for opcode in [0, 5, 255] {
        let mut unsupported = good.clone();
        unsupported.push(SolanaInstruction {
            program_id: good[0].program_id,
            accounts: vec![],
            data: vec![opcode],
        });
        bad_cases.push(unsupported);
    }
    for instructions in bad_cases {
        let tx = serialize_unsigned_legacy_transaction([7; 32], [9; 32], &instructions)?;
        assert!(decode_priority_fee(&STANDARD.encode(tx)).is_err());
    }
    let wire = STANDARD.decode(fee_transaction(200_000, 600_000))?;
    for (offset, value) in [(0, 0), (66, 1), (166, 255), (166, 0), (168, 255)] {
        let mut malformed = wire.clone();
        malformed[offset] = value;
        assert!(
            decode_priority_fee(&STANDARD.encode(malformed)).is_err(),
            "offset {offset}"
        );
    }
    for len in 0..wire.len() {
        assert!(decode_priority_fee(&STANDARD.encode(&wire[..len])).is_err());
    }
    let mut noncanonical = wire.clone();
    noncanonical.splice(0..1, [0x81, 0]);
    assert!(decode_priority_fee(&STANDARD.encode(noncanonical)).is_err());
    let mut trailing = wire.clone();
    trailing.push(0);
    assert!(decode_priority_fee(&STANDARD.encode(trailing)).is_err());
    let mut versioned = wire.clone();
    versioned.insert(65, 0x80);
    versioned.push(0);
    assert_eq!(
        decode_priority_fee(&STANDARD.encode(&versioned))?.total,
        120000
    );
    versioned[65] = 0x81;
    assert!(decode_priority_fee(&STANDARD.encode(&versioned)).is_err());
    versioned[65] = 0x80;
    *versioned.last_mut().unwrap() = 1;
    versioned.extend([0; 32]);
    versioned.extend([0, 0]);
    assert!(decode_priority_fee(&STANDARD.encode(versioned)).is_err());
    Ok(())
}

#[test]
fn priority_fee_message_rebuilt_during_signing_requires_new_proof() -> Result<()> {
    use crate::execution_signing_envelope::ExecutionSignedTransactionPayload;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct MutatingSigner(AtomicUsize);
    impl ExecutionSubmitAdapter for MutatingSigner {
        fn native_floor_config(&self) -> Result<&ExecutionConfig> {
            Ok(crate::app_tests::priority_fee_fixture::dry_run_config())
        }
        fn build_transaction_plan(
            &self,
            req: &ExecutionSubmitRequest,
        ) -> Result<ExecutionTransactionPlan> {
            let mut plan = NoSubmitExecutionAdapter.build_transaction_plan(req)?;
            let slot = crate::execution_serialized_transaction_slot::ExecutionSerializedTransactionPayloadSlot::new();
            slot.store(ExecutionSerializedTransactionPayload {
                source: "synthetic".into(),
                serialized_transaction_base64: fee_transaction(200_000, 600_000),
            })?;
            plan.serialized_transaction_payload_slot = Some(slot);
            Ok(plan)
        }
        fn simulate_transaction_plan<'a>(
            &'a self,
            _: &'a ExecutionTransactionPlan,
        ) -> ExecutionSimulationFuture<'a> {
            unreachable!()
        }
        fn sign_serialized_transaction(
            &self,
            _: &ExecutionSubmitRequest,
            _: &ExecutionTransactionPlan,
            _: &ExecutionSerializedTransactionPayload,
        ) -> Result<Option<ExecutionSignedTransactionPayload>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Some(ExecutionSignedTransactionPayload {
                signed_transaction_base64: fee_transaction(200_000, 1),
                tx_signature_hint: None,
            }))
        }
        fn plan_submit(&self, _: &ExecutionSubmitRequest) -> Result<ExecutionSubmitPlan> {
            unreachable!()
        }
    }
    let adapter = MutatingSigner(AtomicUsize::new(0));
    let req = request();
    let plan = adapter.build_transaction_plan(&req)?;
    let error = adapter.build_signing_envelope(&req, &plan).unwrap_err();
    assert!(error
        .to_string()
        .contains("priority_fee_message_changed_after_proof"));
    assert_eq!(adapter.0.load(Ordering::SeqCst), 1);
    for invalid in [
        "AQIDBA==".to_string(),
        fee_transaction(0, 0),
        fee_transaction(1_400_000, u64::MAX),
    ] {
        let adapter = MutatingSigner(AtomicUsize::new(0));
        let plan = adapter.build_transaction_plan(&req)?;
        plan.serialized_transaction_payload_slot
            .as_ref()
            .unwrap()
            .store(ExecutionSerializedTransactionPayload {
                source: "synthetic".into(),
                serialized_transaction_base64: invalid,
            })?;
        assert!(adapter.build_signing_envelope(&req, &plan).is_err());
        assert_eq!(adapter.0.load(Ordering::SeqCst), 0);
    }
    Ok(())
}
