use super::native_rpc_fixture::{Fixture, Reply, Trace};
pub(super) use super::native_setup_fixture::*;
pub(super) use crate::execution_native_ata_funding::{plan_classic_ata_funding as plan, types::*};
use crate::execution_native_funding::decode_native_funding_requirements;
use crate::execution_native_rpc::{rent_types::ClassicAtaFundingFacts, NativeFundingRpcClient};
use crate::execution_solana_tx::{PubkeyBytes, SolanaInstruction};
use anyhow::{ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use std::time::Duration;

pub(super) const RENT_METHOD: &str = "getMinimumBalanceForRentExemption";
pub(super) const RENT_ID: &str = "native-funding-classic-ata-rent";

pub(super) fn rpc_success(r: &Value) -> Value {
    if r["method"] == RENT_METHOD {
        json!({"jsonrpc":"2.0", "id":RENT_ID, "result":2_039_280})
    } else {
        super::native_rpc_fixture::success(r)
    }
}
pub(super) struct Inputs {
    pub rows: Vec<KeyedAccountObservation>,
    pub fee: Option<u64>,
    pub rent: u64,
    client: NativeFundingRpcClient,
}
impl Inputs {
    pub fn new(payload: &str, rent: u64) -> Result<Self> {
        let binding = decode_native_funding_requirements(payload, WALLET)?.binding;
        Ok(Self {
            rows: binding
                .accounts
                .iter()
                .map(|a| KeyedAccountObservation {
                    pubkey: a.pubkey,
                    account: AccountObservation::Absent,
                })
                .collect(),
            fee: None,
            rent,
            client: NativeFundingRpcClient::new()?,
        })
    }
    pub fn set(&mut self, key: PubkeyBytes, account: AccountObservation) {
        self.rows
            .iter_mut()
            .find(|r| r.pubkey == key)
            .unwrap()
            .account = account;
    }
    pub async fn collect(&self, payload: &str) -> Result<ClassicAtaFundingFacts> {
        let rows: Vec<_> = self.rows.iter().map(|r| match &r.account {
            AccountObservation::Absent => Value::Null,
            AccountObservation::Present { lamports, owner_program, executable, data } => json!({"lamports":lamports,"owner":bs58::encode(owner_program).into_string(),"executable":executable,"data":[STANDARD.encode(data),"base64"]}),
        }).collect();
        let (fee, rent) = (self.fee, self.rent);
        let server = Fixture::start_with_in_flight(3, move |r| {
            let result = match r["method"].as_str().unwrap() {
                RENT_METHOD => json!(rent),
                "getFeeForMessage" => json!({"context":{"slot":51},"value":fee}),
                "getMultipleAccounts" => json!({"context":{"slot":55},"value":rows}),
                _ => panic!("unexpected fixture method"),
            };
            Reply::json(json!({"jsonrpc":"2.0","id":r["id"],"result":result}))
        })
        .await?;
        let result = self
            .client
            .collect_with_classic_ata_rent(
                &server.endpoint,
                Duration::from_secs(2),
                payload,
                WALLET,
                Some(50),
            )
            .await;
        let trace = server.finish().await?;
        exact_calls(&trace, 3)?;
        result
    }
}
pub(super) fn exact_calls(trace: &[Trace], count: usize) -> Result<()> {
    ensure!(trace.len() == count, "expected {count} RPCs: {trace:?}");
    let methods = if count == 3 {
        vec!["getFeeForMessage", "getMultipleAccounts", RENT_METHOD]
    } else {
        vec!["getFeeForMessage", "getMultipleAccounts"]
    };
    for method in methods {
        ensure!(
            trace
                .iter()
                .filter(|r| r.request["method"] == method)
                .count()
                == 1,
            "method trace {trace:?}"
        );
    }
    ensure!(
        trace.iter().all(|r| r.completed.is_some()),
        "unfinished handler"
    );
    Ok(())
}
pub(super) fn create(mint: PubkeyBytes) -> SolanaInstruction {
    ata(
        WALLET,
        mint,
        token_program_id(),
        associated_token_address(&WALLET, &mint, &token_program_id()),
    )
}
pub(super) fn foreign_payer(instructions: &mut [SolanaInstruction]) -> Result<String> {
    instructions[2].accounts[0].pubkey = PEER;
    instructions[2].accounts[0].is_signer = false;
    let mut wire = STANDARD.decode(payload(instructions)?)?;
    ensure!(&wire[101..133] == PEER, "fixture second signer ordering");
    wire[65] = 2;
    wire[0] = 2;
    wire.splice(1..1, [0; 64]);
    Ok(STANDARD.encode(wire))
}
