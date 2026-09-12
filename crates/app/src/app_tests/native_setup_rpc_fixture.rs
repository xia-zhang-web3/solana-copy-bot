use super::native_funding_fixture::WALLET;
use super::native_rpc_fixture::{Fixture, Reply};
use crate::execution_native_funding::decode_native_funding_requirements;
use crate::execution_native_rpc::{types::*, NativeFundingRpcClient};
use crate::execution_solana_tx::PubkeyBytes;
use anyhow::{ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use std::time::Duration;

// Mutable synthetic provider INPUTS only. No NativeFundingRpcFacts construction or
// mutation here: even the negative/large/layout fixtures go through the real collector.
pub(super) struct SetupResponses {
    client: NativeFundingRpcClient,
    pub(super) rows: Vec<KeyedAccountObservation>,
    pub(super) fee: Option<u64>,
    pub(super) fee_slot: u64,
    pub(super) accounts_slot: u64,
    pub(super) floor: Option<u64>,
}

pub(super) fn responses(payload: &str) -> Result<SetupResponses> {
    let decoded = decode_native_funding_requirements(payload, WALLET)?;
    Ok(SetupResponses {
        client: NativeFundingRpcClient::new()?,
        rows: decoded
            .binding
            .accounts
            .iter()
            .map(|a| KeyedAccountObservation {
                pubkey: a.pubkey,
                account: AccountObservation::Absent,
            })
            .collect(),
        fee: Some(19_000),
        fee_slot: 51,
        accounts_slot: 55,
        floor: Some(50),
    })
}

pub(super) fn set(input: &mut SetupResponses, key: PubkeyBytes, value: AccountObservation) {
    input
        .rows
        .iter_mut()
        .find(|row| row.pubkey == key)
        .unwrap()
        .account = value;
}

impl SetupResponses {
    pub(super) async fn collect(&self, payload: &str) -> Result<NativeFundingRpcFacts> {
        let rows: Vec<_> = self.rows.iter().map(|row| match &row.account {
            AccountObservation::Absent => Value::Null,
            AccountObservation::Present { lamports, owner_program, executable, data } => json!({
                "lamports": lamports, "owner": bs58::encode(owner_program).into_string(),
                "executable": executable, "data": [STANDARD.encode(data), "base64"], "space": data.len(),
            }),
        }).collect();
        let (fee, fee_slot, accounts_slot) = (self.fee, self.fee_slot, self.accounts_slot);
        let server = Fixture::start(true, move |request| {
            let is_fee = request["method"] == "getFeeForMessage";
            Reply::json(json!({"jsonrpc":"2.0", "id":request["id"], "result":{
                "context":{"slot":if is_fee {fee_slot} else {accounts_slot}},
                "value":if is_fee {json!(fee)} else {json!(rows)},
            }}))
        })
        .await?;
        let result = self
            .client
            .collect(
                &server.endpoint,
                Duration::from_secs(2),
                payload,
                WALLET,
                self.floor,
            )
            .await;
        let calls = server.finish().await?;
        // Finish and await ALL handlers before any assertions, including invalid replies.
        ensure!(
            calls.len() == 2,
            "setup fixture expected exactly two RPC calls"
        );
        for method in ["getFeeForMessage", "getMultipleAccounts"] {
            ensure!(
                calls
                    .iter()
                    .filter(|c| c.request["method"] == method)
                    .count()
                    == 1,
                "setup fixture expected one call per method"
            );
        }
        result
    }
}
