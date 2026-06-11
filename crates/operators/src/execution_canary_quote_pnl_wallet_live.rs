use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::ExecutionTinyProofReport;
use reqwest::blocking::Client;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::Duration as StdDuration;

use crate::execution_canary_quote_pnl_sell_side::SellSideDiagnosticsReport;
use crate::execution_canary_quote_pnl_wallet::{
    build_wallet_reconciliation_from_parts, WalletReconciliationReport, WalletSellQuoteProof,
    WalletTokenBalance,
};

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const SPL_TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const TOKEN_ACCOUNT_PROGRAMS: [&str; 2] = [SPL_TOKEN_PROGRAM, TOKEN_2022_PROGRAM];

pub(crate) fn build_live_wallet_reconciliation(
    config: &ExecutionConfig,
    proof: &ExecutionTinyProofReport,
    sell_side: &SellSideDiagnosticsReport,
    as_of: DateTime<Utc>,
) -> WalletReconciliationReport {
    let owner = config.canary_wallet_pubkey.trim().to_string();
    if owner.is_empty() {
        return failed_report(as_of, owner, "missing execution.canary_wallet_pubkey");
    }
    let rpc_url = config.priority_fee_canary_rpc_url.trim();
    if rpc_url.is_empty() {
        return failed_report(
            as_of,
            owner,
            "missing execution.priority_fee_canary_rpc_url",
        );
    }
    let client = match Client::builder()
        .timeout(StdDuration::from_millis(
            u64::from(config.quote_canary_timeout_ms).clamp(500, 10_000),
        ))
        .build()
    {
        Ok(client) => client,
        Err(error) => return failed_report(as_of, owner, format!("http client: {error}")),
    };

    let mut errors = Vec::new();
    let sol_balance_lamports = rpc_sol_balance(&client, rpc_url, &owner)
        .map_err(|error| errors.push(error))
        .ok();
    let token_accounts = rpc_token_accounts(&client, rpc_url, &owner)
        .map_err(|error| errors.push(error))
        .unwrap_or_default();
    let nonzero_balances: Vec<_> = token_accounts
        .iter()
        .filter(|balance| amount_is_nonzero(&balance.amount_raw))
        .cloned()
        .collect();
    let slippage_bps = config
        .quote_canary_sell_slippage_bps
        .max(config.quote_canary_slippage_bps)
        .min(u64::from(u32::MAX)) as u32;
    let quote_base_url = config.quote_canary_base_url.trim();
    let mut quote_proofs = BTreeMap::new();
    if quote_base_url.is_empty() {
        errors.push("missing execution.quote_canary_base_url".to_string());
    } else {
        for balance in &nonzero_balances {
            quote_proofs.insert(
                balance.mint.clone(),
                quote_sell(&client, quote_base_url, balance, slippage_bps),
            );
        }
    }

    build_wallet_reconciliation_from_parts(
        as_of,
        owner,
        sol_balance_lamports,
        token_accounts.len() as u64,
        nonzero_balances,
        &proof.open_positions,
        sell_side,
        quote_proofs,
        errors,
    )
}

fn failed_report(
    as_of: DateTime<Utc>,
    owner_pubkey: String,
    error: impl Into<String>,
) -> WalletReconciliationReport {
    build_wallet_reconciliation_from_parts(
        as_of,
        owner_pubkey,
        None,
        0,
        Vec::new(),
        &[],
        &SellSideDiagnosticsReport {
            recent_sell_orders: 0,
            failed_sell_orders: 0,
            simulation_failed_orders: 0,
            build_failed_orders: 0,
            terminal_no_route_orders: 0,
            terminal_simulation_orders: 0,
            tx_signature_present_failed_orders: 0,
            open_position_count: 0,
            open_position_tokens: Vec::new(),
            failure_token_count: 0,
            failures_by_token: Vec::new(),
        },
        BTreeMap::new(),
        vec![error.into()],
    )
}

fn amount_is_nonzero(raw: &str) -> bool {
    raw.parse::<u128>().is_ok_and(|amount| amount > 0)
}

fn rpc_sol_balance(client: &Client, rpc_url: &str, owner: &str) -> Result<u64, String> {
    let result = rpc_call(client, rpc_url, "getBalance", json!([owner]))?;
    result
        .get("value")
        .and_then(Value::as_u64)
        .ok_or_else(|| "getBalance missing result.value".to_string())
}

fn rpc_token_accounts(
    client: &Client,
    rpc_url: &str,
    owner: &str,
) -> Result<Vec<WalletTokenBalance>, String> {
    let mut balances = Vec::new();
    for program_id in TOKEN_ACCOUNT_PROGRAMS {
        let result = rpc_call(
            client,
            rpc_url,
            "getTokenAccountsByOwner",
            json!([owner, {"programId": program_id}, {"encoding": "jsonParsed"}]),
        )?;
        let accounts = result
            .get("value")
            .and_then(Value::as_array)
            .ok_or_else(|| "getTokenAccountsByOwner missing result.value".to_string())?;
        balances.extend(accounts.iter().map(parse_wallet_token_balance));
    }
    Ok(balances)
}

fn parse_wallet_token_balance(account: &Value) -> WalletTokenBalance {
    let info = &account["account"]["data"]["parsed"]["info"];
    let amount = &info["tokenAmount"];
    WalletTokenBalance {
        token_account: string_field(account, "pubkey").unwrap_or_default(),
        mint: string_field(info, "mint").unwrap_or_default(),
        amount_raw: string_field(amount, "amount").unwrap_or_default(),
        ui_amount_string: string_field(amount, "uiAmountString").unwrap_or_default(),
        decimals: amount.get("decimals").and_then(Value::as_u64).unwrap_or(0) as u8,
    }
}

fn rpc_call(client: &Client, rpc_url: &str, method: &str, params: Value) -> Result<Value, String> {
    let response = client
        .post(rpc_url)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
        .send()
        .map_err(|error| format!("{method} request failed: {error}"))?;
    let status = response.status();
    let value: Value = response
        .json()
        .map_err(|error| format!("{method} response json failed: {error}"))?;
    if !status.is_success() {
        return Err(format!("{method} returned HTTP {status}: {value}"));
    }
    if let Some(error) = value.get("error") {
        return Err(format!("{method} returned RPC error: {error}"));
    }
    value
        .get("result")
        .cloned()
        .ok_or_else(|| format!("{method} missing result"))
}

fn quote_sell(
    client: &Client,
    base_url: &str,
    balance: &WalletTokenBalance,
    slippage_bps: u32,
) -> WalletSellQuoteProof {
    let url = format!(
        "{}/quote?inputMint={}&outputMint={}&amount={}&slippageBps={}",
        base_url.trim_end_matches('/'),
        balance.mint,
        SOL_MINT,
        balance.amount_raw,
        slippage_bps
    );
    let response = match client.get(url).send() {
        Ok(response) => response,
        Err(error) => return quote_error(None, format!("quote request failed: {error}")),
    };
    let status = response.status();
    let http_status = Some(status.as_u16());
    let value: Value = match response.json() {
        Ok(value) => value,
        Err(error) => {
            return quote_error(http_status, format!("quote response json failed: {error}"))
        }
    };
    if !status.is_success() {
        let error_code = string_field(&value, "errorCode");
        let quote_status = if error_code.as_deref() == Some("NO_ROUTES_FOUND") {
            "no_route"
        } else {
            "error"
        };
        return WalletSellQuoteProof {
            status: quote_status.to_string(),
            http_status,
            error: string_field(&value, "error"),
            error_code,
            out_amount_raw: None,
            out_sol: None,
            price_impact_pct: None,
            route_labels: Vec::new(),
        };
    }
    let out_amount_raw = string_field(&value, "outAmount");
    WalletSellQuoteProof {
        status: "ok".to_string(),
        http_status,
        error: None,
        error_code: None,
        out_sol: out_amount_raw
            .as_deref()
            .and_then(|raw| raw.parse::<u64>().ok())
            .map(|lamports| lamports as f64 / 1_000_000_000.0),
        out_amount_raw,
        price_impact_pct: string_field(&value, "priceImpactPct"),
        route_labels: route_labels(&value),
    }
}

fn quote_error(http_status: Option<u16>, error: String) -> WalletSellQuoteProof {
    WalletSellQuoteProof {
        status: "error".to_string(),
        http_status,
        error: Some(error),
        error_code: None,
        out_amount_raw: None,
        out_sol: None,
        price_impact_pct: None,
        route_labels: Vec::new(),
    }
}

fn route_labels(value: &Value) -> Vec<String> {
    value
        .get("routePlan")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| string_field(&entry["swapInfo"], "label"))
        .collect()
}

fn string_field(value: &Value, name: &str) -> Option<String> {
    value
        .get(name)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}
