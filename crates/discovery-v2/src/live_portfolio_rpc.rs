use crate::live_portfolio::{LivePortfolioSnapshot, LiveTokenPosition};
use anyhow::{anyhow, bail, Context, Result};
use reqwest::blocking::Client;
use serde_json::{json, Value};
use std::time::Duration;

const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;

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

    pub(crate) fn fetch_snapshot(&self, wallet: &str) -> Result<LivePortfolioSnapshot> {
        let sol_balance = self.fetch_sol_balance(wallet)?;
        let token_positions = self.fetch_token_positions(wallet)?;
        Ok(LivePortfolioSnapshot {
            sol_balance,
            token_positions,
        })
    }

    fn fetch_sol_balance(&self, wallet: &str) -> Result<f64> {
        let response = self.post_rpc("getBalance", json!([wallet, {"commitment": "confirmed"}]))?;
        let lamports = response
            .get("result")
            .and_then(|result| result.get("value"))
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("getBalance response missing result.value"))?;
        Ok(lamports as f64 / LAMPORTS_PER_SOL)
    }

    fn fetch_token_positions(&self, wallet: &str) -> Result<Vec<LiveTokenPosition>> {
        let response = self.post_rpc(
            "getTokenAccountsByOwner",
            json!([
                wallet,
                {"programId": TOKEN_PROGRAM_ID},
                {"encoding": "jsonParsed", "commitment": "confirmed"}
            ]),
        )?;
        let accounts = response
            .get("result")
            .and_then(|result| result.get("value"))
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("token account response missing result.value"))?;
        if accounts.len() > self.max_token_accounts {
            bail!(
                "wallet has {} token accounts, max live portfolio budget is {}",
                accounts.len(),
                self.max_token_accounts
            );
        }
        let mut positions = Vec::new();
        for account in accounts {
            let Some(info) = account
                .pointer("/account/data/parsed/info")
                .and_then(Value::as_object)
            else {
                continue;
            };
            let Some(mint) = info.get("mint").and_then(Value::as_str) else {
                continue;
            };
            let Some(amount) = info
                .get("tokenAmount")
                .and_then(parse_token_amount)
                .filter(|value| value.is_finite() && *value > 0.0)
            else {
                continue;
            };
            positions.push(LiveTokenPosition {
                mint: mint.to_string(),
                amount,
            });
        }
        Ok(positions)
    }

    fn post_rpc(&self, method: &str, params: Value) -> Result<Value> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });
        let response: Value = self
            .client
            .post(&self.rpc_url)
            .json(&body)
            .send()
            .with_context(|| format!("{method} RPC request failed"))?
            .error_for_status()
            .with_context(|| format!("{method} RPC returned non-success status"))?
            .json()
            .with_context(|| format!("{method} RPC response was not JSON"))?;
        if let Some(error) = response.get("error") {
            bail!("{method} RPC returned error: {error}");
        }
        Ok(response)
    }
}

fn parse_token_amount(token_amount: &Value) -> Option<f64> {
    if let Some(value) = token_amount
        .get("uiAmountString")
        .and_then(Value::as_str)
        .and_then(|raw| raw.parse::<f64>().ok())
    {
        return Some(value);
    }
    if let Some(value) = token_amount.get("uiAmount").and_then(Value::as_f64) {
        return Some(value);
    }
    let raw = token_amount.get("amount")?.as_str()?.parse::<f64>().ok()?;
    let decimals = token_amount.get("decimals")?.as_u64()?;
    Some(raw / 10f64.powi(decimals.min(38) as i32))
}
