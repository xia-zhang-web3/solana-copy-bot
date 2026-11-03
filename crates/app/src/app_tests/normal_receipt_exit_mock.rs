//! Full-state LOCAL signatures require a scoped crypto seam; no real key is read.
use crate::execution_signing_envelope::{ExecutionSerializedTransactionPayload,ExecutionSignedTransactionPayload};
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD,Engine};
use sha2::{Digest,Sha256};
use std::{cell::RefCell,future::Future};
pub(super) const MARKER:&str="EXPLICIT_LOCAL_NORMAL_EXIT_MOCK_SIGNATURE";
type MockIdentity=([u8;32],Vec<u8>,Vec<u8>);
tokio::task_local!{static SIGNED:RefCell<Vec<MockIdentity>>;}
pub(super) async fn scope<F:Future>(f:F)->F::Output{
 SIGNED.scope(RefCell::new(Vec::new()),Box::pin(f)).await
}
pub(super) fn signed(payload:&ExecutionSerializedTransactionPayload)->Result<ExecutionSignedTransactionPayload>{
 let mut bytes=STANDARD.decode(&payload.serialized_transaction_base64)?;
 let binding=crate::execution_transaction_wire::decode_message(&payload.serialized_transaction_base64,|_|Ok(()))?.binding;
 anyhow::ensure!(binding.signature_count==1 && bs58::encode(binding.accounts[0].pubkey).into_string()==super::normal_receipt_exit_saved_fixture::BOT,"LOCAL signature pinned wallet");
 // Only the signature is modeled. All real message/identity/fee/wire guards run.
 bytes[1..65].fill(42);
 SIGNED.with(|s|s.borrow_mut().push((binding.accounts[0].pubkey,Sha256::digest(&binding.message_bytes).to_vec(),bytes[1..65].to_vec())));
 Ok(ExecutionSignedTransactionPayload{signed_transaction_base64:STANDARD.encode(&bytes),
  tx_signature_hint:Some(bs58::encode(&bytes[1..65]).into_string())})
}
pub(crate) fn matches(key:&[u8;32],message:&[u8],signature:&[u8])->bool{
 SIGNED.try_with(|s|s.borrow().iter().any(|(k,m,g)|k==key && m.as_slice()==Sha256::digest(message).as_slice() && g==signature)).unwrap_or(false)
}
