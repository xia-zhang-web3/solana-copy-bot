use super::token2022_ata_boundary_tests::Boundary;
use super::token2022_ata_inputs_tests::{save, Spec};
use super::token2022_ata_rpc_tests::{Hook, Server};
use crate::execution_solana_tx::{PubkeyBytes, SolanaInstruction};
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use std::path::Path;

pub(super) const JUPITER: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
pub(super) const UVA: &str = "FrParQqNHTsSw7N1wH3zLHdDwse9qXgTQsTBbopoiPVa";
pub(super) const UNPROVEN: &str = "initial_sol_jupiter_funding_unproven";

pub(super) fn instructions(spec: &Spec) -> Result<Vec<SolanaInstruction>> {
    Ok(
        crate::execution_transaction_wire::decode_message(&spec.payload, |_| Ok(()))?
            .instructions
            .into_iter()
            .map(|ix| SolanaInstruction {
                program_id: ix.program.pubkey,
                accounts: ix.accounts,
                data: ix.data,
            })
            .collect(),
    )
}

pub(super) fn rebind(spec: &Spec, payload: String) -> Result<Spec> {
    let decoded = crate::execution_transaction_wire::decode_message(&payload, |_| Ok(()))?;
    let keys: Vec<_> = decoded
        .binding
        .accounts
        .iter()
        .map(|a| bs58::encode(a.pubkey).into_string())
        .collect();
    let mut out = Spec::from_frozen(&json!({
        "payload":payload, "message_base64":STANDARD.encode(&decoded.binding.message_bytes),
        "keys":keys
    }));
    for (key, row) in spec.keys.as_array().unwrap().iter().zip(&spec.rows) {
        if out.keys.as_array().unwrap().contains(key) {
            out.set(key.as_str().unwrap(), row.clone());
        }
    }
    out.fee = spec.fee;
    out.rent = spec.rent;
    out.rent170 = spec.rent170;
    Ok(out)
}

pub(super) fn serialize(
    spec: &Spec,
    wallet: PubkeyBytes,
    instructions: &[SolanaInstruction],
) -> Result<Spec> {
    let wire = crate::execution_solana_tx::serialize_unsigned_legacy_transaction(
        wallet,
        [55; 32],
        instructions,
    )?;
    assert_eq!(wire[0], 1);
    assert!(wire[1..65].iter().all(|b| *b == 0));
    rebind(spec, STANDARD.encode(wire))
}

pub(super) fn non_jupiter() -> Result<Spec> {
    // This existing fixture consists of actual System/ATA instructions. It has
    // never contained a Jupiter instruction or had a program ID substituted.
    let spec = Spec::synthetic(false)?;
    let mut ix = instructions(&spec)?;
    // The existing unsigned boundary uses a 22k cap. Supply a normal 2k
    // compute budget before building its independently bound envelope.
    ix.splice(0..2, super::priority_fee_fixture::budget(200_000, 10_000));
    ix.push(super::native_funding_fixture::transfer(
        spec.wallet,
        spec.wallet,
        super::token2022_ata_inputs_tests::RESERVE,
    ));
    serialize(&spec, spec.wallet, &ix)
}

pub(super) async fn invoke(
    boundary: &Boundary,
    spec: &Spec,
    dir: &Path,
    hook: Option<Hook>,
) -> Result<Option<crate::execution_canary_submit_contract::ExecutionSubmitPlanOutcome>> {
    let before = boundary.sql()?;
    let state = crate::execution_tiny_submit_state::eligible(&boundary.store, &boundary.request)
        .map_err(anyhow::Error::msg)?;
    let server = Server::start(spec, hook).await?;
    let transport = crate::execution_submit_adapter::RpcExecutionSubmitTransport::new(
        server.rpc.endpoint.clone(),
    );
    let out = crate::execution_initial_sol_submit::before_send(
        &boundary.store,
        &boundary.request,
        &boundary.envelope,
        &boundary.intent,
        &boundary.gate,
        &transport,
        &state,
        boundary.now,
    )
    .await;
    let trace = server.finish(dir).await?;
    if boundary.request.side.eq_ignore_ascii_case("sell") {
        assert!(trace.is_empty());
    }
    assert!(trace
        .iter()
        .all(|r| r.request["method"] != "sendTransaction"));
    save(
        dir,
        "before-send.json",
        &json!({
            "result":format!("{out:#?}"),"before":before,"after":boundary.sql()?,
            "collection_requests":trace.len(),"key_load_calls":0,"sign_calls":0,"send_calls":0,
            "scope":"real funding collector and before_send; synthetic facts and unsigned bytes; not daemon E2E"
        }),
    );
    Ok(out)
}

pub(super) async fn refused(spec: &Spec, label: &str) -> Result<()> {
    let dir = super::token2022_ata_inputs_tests::output(&format!("b123-{label}"));
    spec.save(&dir);
    let boundary = Boundary::new(spec, "buy", &dir)?;
    let out = invoke(&boundary, spec, &dir, None).await?.unwrap();
    assert_eq!(out.failed, 1, "{label}: {out:?}");
    assert_eq!(out.error.as_deref(), Some(UNPROVEN), "{label}: {out:?}");
    let order = boundary
        .store
        .load_execution_canary_order(&boundary.request.order_id)?
        .unwrap();
    assert_eq!(order.status, "execution_canary_failed");
    assert_eq!(order.tx_signature, None);
    assert_eq!(boundary.sql()?["failed_expenses"], 0);
    assert_eq!(boundary.sql()?["fills"], 0);
    assert!(boundary
        .store
        .load_execution_canary_dispatch(&boundary.request.order_id)?
        .is_none());
    assert!(boundary
        .store
        .load_execution_canary_receipt_proof(&boundary.request.order_id)?
        .is_none());
    Ok(())
}

pub(super) fn system(lamports: u64) -> Value {
    super::initial_sol_rpc_fixture::system(lamports)
}
