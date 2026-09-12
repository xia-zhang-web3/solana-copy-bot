//! Synthetic non-DEX instructions for existing SELL boundary fixtures only.
//! Separate from immutable R4 provider records and their independent wire oracle.
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};

pub(super) fn bundle(payer: [u8; 32], limit: u32, price: u64) -> Value {
    let compute: Vec<_> = super::priority_fee_fixture::budget(limit, price)
        .into_iter()
        .map(|ix| {
            json!({"programId":bs58::encode(ix.program_id).into_string(),
            "accounts":[],"data":STANDARD.encode(ix.data)})
        })
        .collect();
    json!({
        "tokenLedgerInstruction":null,"computeBudgetInstructions":compute,
        "setupInstructions":[],"swapInstruction":{
            "programId":"MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr",
            "accounts":[{"pubkey":bs58::encode(payer).into_string(),"isSigner":true,"isWritable":true}],
            "data":STANDARD.encode(b"synthetic-sell-boundary")},
        "cleanupInstruction":null,"otherInstructions":[],"addressLookupTableAddresses":[],
        "blockhashWithMetadata":{"blockhash":vec![9;32],"lastValidBlockHeight":1,
            "fetchedAt":{"secs_since_epoch":1,"nanos_since_epoch":0}},
        "simulationError":null
    })
}
