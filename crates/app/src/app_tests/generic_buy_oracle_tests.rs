//! Frozen before production edits. Loopback success is synthetic, never network proof.
use super::generic_buy_fixture::*;
use super::generic_buy_loopback::*;
use crate::execution_submit_adapter::*;
use anyhow::Result;
use serde_json::{json, Value};

#[tokio::test]
async fn batch111_frozen_actual_adapter_oracle() -> Result<()> {
    let server = Server::start(Replies::default());
    let request = request("buy", serde_json::from_str(QUOTE)?)?;
    let cfg = config(&server.base);
    let adapter = JupiterMetisDryRunExecutionAdapter::new(cfg.clone());
    let plan = adapter.build_transaction_plan(&request)?;
    let result = adapter.simulate_transaction_plan(&plan).await;
    let payload = plan
        .serialized_transaction_payload_slot
        .as_ref()
        .unwrap()
        .load()?;
    let summary = json!({"synthetic_loopback_simulation":true,"adapter":format!("{result:?}"),
        "payload":payload.as_ref().map(|p| &p.serialized_transaction_base64),
        "swap_calls":server.count("/swap"),"simulation_payloads":server.simulations(),
        "calls":server.calls().iter().map(|c| json!({"path":c.path,"body":c.raw})).collect::<Vec<_>>()});
    if let Ok(dir) = std::env::var("BATCH111_OUTPUT") {
        std::fs::write(
            std::path::Path::new(&dir).join("actual-adapter.json"),
            serde_json::to_vec_pretty(&summary)?,
        )?;
    }
    result?;
    let payload = payload.expect("healthy client simulation must store exact payload");
    assert_eq!(
        server.simulations(),
        vec![payload.serialized_transaction_base64.clone()]
    );
    let fee = crate::execution_priority_fee_proof::prove(
        &request,
        &payload.serialized_transaction_base64,
        22_000,
    )?;
    let fee = serde_json::to_value(fee)?;
    assert_eq!(fee["requested_compute_unit_limit"], 172647);
    assert_eq!(fee["micro_lamports_per_compute_unit"], 127427);
    assert_eq!(fee["total_priority_fee_lamports"], 22000);
    crate::execution_native_floor_policy::verify_signing_payload(
        &cfg,
        &request,
        &plan,
        &payload.serialized_transaction_base64,
    )?
    .unwrap();
    // Direct unsigned API; no production file signer or key loader is called.
    let envelope =
        crate::execution_signing_envelope::build_serialized_transaction_execution_envelope(
            &request,
            &plan,
            payload.clone(),
        )?;
    assert_eq!(
        envelope.serialized_transaction_base64.as_ref(),
        Some(&payload.serialized_transaction_base64)
    );
    assert_eq!(
        super::generic_buy_decode::verify(
            &payload.serialized_transaction_base64,
            &serde_json::from_str(INSTRUCTIONS)?,
            PAYER,
            50_000_001
        ),
        1197
    );
    assert_eq!(server.count("/swap"), 0);
    let instructions: Vec<_> = server
        .calls()
        .into_iter()
        .filter(|c| c.path.ends_with("swap-instructions"))
        .collect();
    assert_eq!(instructions.len(), 1);
    assert_eq!(instructions[0].raw, REQUEST);
    assert!(request.metadata.http_request_started_ts.is_none());
    assert!(!plan.submit_enabled);
    Ok(())
}

#[test]
fn batch111_old_v0_refusal_and_healthy_legacy_control() -> Result<()> {
    let request = request("buy", serde_json::from_str(QUOTE)?)?;
    let cfg = config("http://127.0.0.1:1");
    let plan =
        JupiterMetisDryRunExecutionAdapter::new(cfg.clone()).build_transaction_plan(&request)?;
    let old: Value = serde_json::from_str(OLD_SWAP)?;
    let old = old["swapTransaction"].as_str().unwrap();
    for error in [
        crate::execution_priority_fee_proof::prove(&request, old, 22_000).unwrap_err(),
        crate::execution_native_floor_policy::verify_signing_payload(&cfg, &request, &plan, old)
            .unwrap_err(),
    ] {
        assert!(error
            .to_string()
            .contains("priority_fee_unresolved_account_index"));
    }
    let payer = crate::execution_pumpswap_accounts::parse_pubkey(PAYER, "public payer")?;
    let healthy = super::priority_fee_fixture::guarded_transaction(payer, 200_000, 110_000);
    crate::execution_priority_fee_proof::prove(&request, &healthy, 22_000)?;
    crate::execution_native_floor_policy::verify_signing_payload(&cfg, &request, &plan, &healthy)?
        .unwrap();
    let envelope =
        crate::execution_signing_envelope::build_serialized_transaction_execution_envelope(
            &request,
            &plan,
            crate::execution_signing_envelope::ExecutionSerializedTransactionPayload {
                source: "synthetic-control".into(),
                serialized_transaction_base64: healthy.clone(),
            },
        )?;
    assert_eq!(envelope.serialized_transaction_base64, Some(healthy));
    let sell400: Value = serde_json::from_str(include_str!("generic_buy_fixtures/sell400.json"))?;
    assert_eq!(sell400["errorCode"], "NO_ROUTES_FOUND");
    Ok(())
}
