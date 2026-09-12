use crate::live_inventory::LivePortfolioSnapshot;
use crate::live_inventory::{InventoryFailure as Failure, TokenProgram};
use crate::live_inventory_parse::{valid_pubkey, InventoryParser};
use anyhow::{Context, Result};
use reqwest::blocking::Client;
use serde_json::{json, Value};
use std::time::Duration;

const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;

#[derive(Clone)]
pub(crate) struct LivePortfolioRpcClient {
    client: Client,
    rpc_url: String,
    max_token_accounts: usize,
}

impl LivePortfolioRpcClient {
    pub(crate) fn new(rpc_url: &str, timeout_ms: u64, max_token_accounts: usize) -> Result<Self> {
        Ok(Self {
            client: Client::builder()
                .timeout(Duration::from_millis(timeout_ms.max(1)))
                .build()
                .context("failed building live portfolio RPC client")?,
            rpc_url: rpc_url.to_string(),
            max_token_accounts,
        })
    }

    pub(crate) fn fetch_snapshot(&self, wallet: &str) -> Result<LivePortfolioSnapshot, Failure> {
        if !valid_pubkey(wallet) {
            return Err(Failure::Malformed);
        }
        let response = self.post_rpc("getBalance", json!([wallet, {"commitment": "confirmed"}]))?;
        let sol_slot = response_slot(&response)?;
        let lamports = response
            .pointer("/result/value")
            .and_then(Value::as_u64)
            .ok_or(Failure::Protocol)?;
        let mut parser = InventoryParser::new(self.max_token_accounts);
        let (classic_accounts, classic_slot) =
            self.fetch_program(wallet, TokenProgram::Classic, &mut parser)?;
        let (token_2022_accounts, token_2022_slot) =
            self.fetch_program(wallet, TokenProgram::Token2022, &mut parser)?;
        Ok(LivePortfolioSnapshot {
            sol_balance: lamports as f64 / LAMPORTS_PER_SOL,
            token_positions: parser.finish()?,
            classic_accounts,
            token_2022_accounts,
            sol_slot,
            classic_slot,
            token_2022_slot,
        })
    }

    fn fetch_program(
        &self,
        wallet: &str,
        program: TokenProgram,
        parser: &mut InventoryParser,
    ) -> Result<(usize, u64), Failure> {
        let response = self.post_rpc("getTokenAccountsByOwner", json!([
            wallet, {"programId": program.id()}, {"encoding": "jsonParsed", "commitment": "confirmed"}
        ]))?;
        let slot = response_slot(&response)?;
        Ok((parser.append(&response, wallet, program)?, slot))
    }

    fn post_rpc(&self, method: &str, params: Value) -> Result<Value, Failure> {
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let response: Value = self
            .client
            .post(&self.rpc_url)
            .json(&body)
            .send()
            .map_err(|_| Failure::Transport)?
            .error_for_status()
            .map_err(|_| Failure::Transport)?
            .json()
            .map_err(|_| Failure::Protocol)?;
        if response.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
            || response.get("id").and_then(Value::as_u64) != Some(1)
            || response.get("error").is_some()
            || !response.get("result").is_some_and(Value::is_object)
        {
            return Err(Failure::Protocol);
        }
        Ok(response)
    }
}

fn response_slot(response: &Value) -> Result<u64, Failure> {
    response
        .pointer("/result/context/slot")
        .and_then(Value::as_u64)
        .ok_or(Failure::Protocol)
}
