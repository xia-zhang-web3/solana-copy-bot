//! Frozen before production changes. R4 replay is not fresh network evidence.
use super::generic_sell_fixture::*;
use super::generic_sell_loopback::*;
use crate::execution_submit_adapter::*;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::json;
use sha2::{Digest, Sha256};

#[test]
fn batch116_exact_r4_v0_still_alt_red() -> Result<()> {
    let payload = old_v0();
    let bytes = STANDARD.decode(&payload)?;
    assert_eq!(bytes.len(), 808);
    assert_eq!(
        format!("{:x}", Sha256::digest(&bytes)),
        "b986c681cd10401d24fb5596d18c912094fb298f78f604b8dfd3c8210a059e68"
    );
    let error = crate::execution_priority_fee_proof::prove(&request()?, &payload, 22_000)
        .unwrap_err()
        .to_string();
    assert!(error.contains("priority_fee_unresolved_account_index"));
    save(
        "source-alt-red.json",
        &json!({"error":error,"bytes":808,"unchanged_decoder":true}),
    )
}

#[tokio::test]
async fn batch116_frozen_actual_adapter_oracle() -> Result<()> {
    let server = Server::start(Replies::default());
    let request = request()?;
    let cfg = config(&server.base);
    let adapter = JupiterMetisDryRunExecutionAdapter::new(cfg.clone());
    let plan = adapter.build_transaction_plan(&request)?;
    let result = adapter.simulate_transaction_plan(&plan).await;
    let payload = plan
        .serialized_transaction_payload_slot
        .as_ref()
        .unwrap()
        .load()?;
    save(
        "actual-adapter.json",
        &json!({"adapter":format!("{result:?}"),
        "payload":payload.as_ref().map(|p| &p.serialized_transaction_base64),
        "calls":server.calls(),"swap_calls":server.count("/swap"),
        "simulation_payloads":server.simulations(),"recorded_simulation_reused":true,
        "keyloader_calls":0,"sign_calls":0,"send_calls":0}),
    )?;
    result?;
    let payload = payload.expect("Passed must admit the exact simulated payload");
    assert_eq!(payload.serialized_transaction_base64, legacy());
    assert_eq!(server.simulations(), vec![legacy()]);
    assert_eq!(server.count("/swap"), 0);
    assert_eq!(server.count("/swap-instructions"), 1);
    assert_eq!(server.count_kind("synthetic_direct_refusal"), 1);
    assert_eq!(server.count_kind("recorded_legacy_simulation"), 1);
    assert_eq!(server.count_kind("unexpected"), 0);
    let fee = crate::execution_priority_fee_proof::prove(&request, &legacy(), 22_000)?;
    let fee = serde_json::to_value(fee)?;
    assert_eq!(fee["requested_compute_unit_limit"], 116872);
    assert_eq!(fee["micro_lamports_per_compute_unit"], 188240);
    assert_eq!(fee["total_priority_fee_lamports"], 22000);
    assert!(crate::execution_native_floor_policy::required_for_plan(&cfg, &plan)?.is_none());
    assert_eq!(cfg.pretrade_min_sol_reserve, 0.05);
    // Actual unsigned envelope API, deliberately without invoking a signer adapter.
    let envelope =
        crate::execution_signing_envelope::build_serialized_transaction_execution_envelope(
            &request,
            &plan,
            payload.clone(),
        )?;
    assert_eq!(envelope.serialized_transaction_base64, Some(legacy()));
    assert!(envelope.signed_transaction_base64.is_none() && !envelope.submit_enabled);
    assert_eq!(
        super::generic_sell_decode::verify(&legacy(), &serde_json::from_str(INSTRUCTIONS)?, PAYER),
        1082
    );
    assert_eq!(
        format!("{:x}", Sha256::digest(STANDARD.decode(legacy())?)),
        "95c9053f0e8b83fff79ed14c20d84d26d8e40429699b06df81057e21462b0f32"
    );
    save(
        "unsigned-proof.json",
        &json!({"fee":fee,"envelope":format!("{envelope:?}"),
        "packet_bytes":1082,"instructions":5,"reserve_added":false,"independent_wire":true}),
    )
}
