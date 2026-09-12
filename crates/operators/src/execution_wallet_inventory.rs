//! Validate exact inventory operands before any quote request.
use crate::execution_canary_quote_pnl_wallet::WalletTokenBalance;
use crate::execution_wallet_quote::raw_amount;
use reqwest::blocking::Client;
use serde_json::{json, Value};
use std::collections::BTreeSet;
const PROGRAMS: [&str; 2] = [
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
];

pub(crate) struct Inventory {
    pub balances: Vec<WalletTokenBalance>,
    pub account_count: u64,
    pub complete: bool,
    pub errors: Vec<String>,
}
pub(crate) fn valid_pubkey(raw: &str) -> bool {
    bs58::decode(raw).into_vec().is_ok_and(|b| b.len() == 32)
}
pub(crate) fn rpc_call(
    client: &Client,
    url: &str,
    method: &str,
    params: Value,
) -> Result<Value, String> {
    let response = client
        .post(url)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
        .send()
        .map_err(|_| format!("{method}_request_failed"))?;
    let status = response.status();
    let value: Value = response
        .json()
        .map_err(|_| format!("{method}_invalid_json"))?;
    if !status.is_success() || value.get("error").is_some() {
        return Err(format!("{method}_rpc_failed"));
    }
    value
        .get("result")
        .cloned()
        .ok_or_else(|| format!("{method}_missing_result"))
}
pub(crate) fn token_accounts(client: &Client, url: &str, owner: &str) -> Inventory {
    let mut inventory = Inventory {
        balances: Vec::new(),
        account_count: 0,
        complete: true,
        errors: Vec::new(),
    };
    let mut seen = BTreeSet::new();
    for program in PROGRAMS {
        let result = rpc_call(
            client,
            url,
            "getTokenAccountsByOwner",
            json!([owner,{"programId":program},{"encoding":"jsonParsed","commitment":"confirmed"}]),
        );
        match result.as_ref().ok().and_then(|v| v["value"].as_array()) {
            None => {
                inventory.complete = false;
                inventory
                    .errors
                    .push("token_program_inventory_unavailable".into());
            }
            Some(rows) => {
                inventory.account_count += rows.len() as u64;
                for row in rows {
                    match parse_balance(row, owner, program) {
                        Ok(balance) => {
                            if !seen.insert(balance.token_account.clone()) {
                                inventory.complete = false;
                                inventory.errors.push("duplicate_token_account".into());
                            }
                            inventory.balances.push(balance);
                        }
                        Err(reason) => {
                            inventory.complete = false;
                            inventory.errors.push(reason.into());
                        }
                    }
                }
            }
        }
    }
    inventory.errors.sort();
    inventory.errors.dedup();
    inventory
}
fn parse_balance(
    row: &Value,
    owner: &str,
    program: &str,
) -> Result<WalletTokenBalance, &'static str> {
    let parsed = &row["account"]["data"]["parsed"];
    let info = &parsed["info"];
    if row["account"]["owner"].as_str() != Some(program)
        || info["owner"].as_str() != Some(owner)
        || parsed["type"].as_str() != Some("account")
    {
        return Err("inventory_owner_or_program_mismatch");
    }
    let account = row["pubkey"]
        .as_str()
        .filter(|s| valid_pubkey(s))
        .ok_or("invalid_token_account")?;
    let mint = info["mint"]
        .as_str()
        .filter(|s| valid_pubkey(s))
        .ok_or("invalid_token_mint")?;
    let amount = &info["tokenAmount"];
    let raw = amount["amount"]
        .as_str()
        .and_then(raw_amount)
        .ok_or("invalid_token_amount")?;
    let decimals = amount["decimals"]
        .as_u64()
        .and_then(|d| u8::try_from(d).ok())
        .ok_or("invalid_token_decimals")?;
    Ok(WalletTokenBalance {
        token_account: account.into(),
        mint: mint.into(),
        amount_raw: raw.to_string(),
        decimals,
        ui_amount_string: amount["uiAmountString"].as_str().unwrap_or("").into(),
    })
}
