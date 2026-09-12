//! Explicit signed local payloads for submit-contract tests; no production key access.
use crate::execution_signing_envelope::*;
use crate::execution_submit_adapter::*;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use copybot_storage_core::SqliteStore;
use ed25519_dalek::{Signer, SigningKey};

pub(super) fn payer() -> [u8; 32] {
    SigningKey::from_bytes(&[7; 32]).verifying_key().to_bytes()
}
pub(super) fn payload(side: &str) -> ExecutionSignedTransactionPayload {
    let tx = if side == "buy" {
        super::tiny_transport_fixture::buy(payer(), [9; 32], 10_000_000)
    } else {
        super::priority_fee_fixture::transaction(payer(), 200_000, 10_000)
    };
    let mut bytes = STANDARD.decode(tx).unwrap();
    let key = SigningKey::from_bytes(&[7; 32]);
    let sig = key.sign(&bytes[65..]);
    key.verifying_key()
        .verify_strict(&bytes[65..], &sig)
        .unwrap();
    bytes[1..65].copy_from_slice(&sig.to_bytes());
    ExecutionSignedTransactionPayload {
        signed_transaction_base64: STANDARD.encode(bytes),
        tx_signature_hint: Some(bs58::encode(sig.to_bytes()).into_string()),
    }
}
pub(super) fn config(wallet: &str) -> copybot_config::ExecutionConfig {
    let c = super::initial_sol_rpc_fixture::buy_config(wallet);
    let mut c = super::b126_config_fixture::activated(&c).unwrap();
    c.canary_wallet_pubkey = wallet.into();
    c.execution_signer_pubkey = wallet.into();
    c
}
pub(super) fn envelope(
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<ExecutionSigningEnvelope> {
    assert_eq!(request.wallet_pubkey, bs58::encode(payer()).into_string());
    let plan = NoSubmitExecutionAdapter.build_transaction_plan(request)?;
    let envelope =
        super::priority_fee_fixture::proven_envelope(request, &plan, payload(&request.side))?;
    crate::execution_priority_fee_proof::persist(store, request, &plan, &envelope, 0, now)?;
    Ok(envelope)
}
pub(super) async fn final_fee(listener: &tokio::net::TcpListener) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let (mut stream, _) =
        tokio::time::timeout(std::time::Duration::from_secs(3), listener.accept()).await??;
    let request = super::native_rpc_fixture::read_request(&mut stream).await?;
    assert_eq!(request["method"], "getFeeForMessage");
    let body = super::initial_sol_rpc_fixture::FundingRpc::default()
        .reply(&request)
        .to_string();
    stream
        .write_all(
            format!(
                "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
        .await?;
    Ok(())
}
