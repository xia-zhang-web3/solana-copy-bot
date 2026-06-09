use super::{ExecutionConfirmedBuyFill, ExecutionConfirmedFill, ExecutionConfirmedSellFill};
use anyhow::{anyhow, Context, Result};
use copybot_core_types::TokenQuantity;
use serde_json::{json, Value};
use std::time::Duration;

pub(crate) async fn fetch_actual_confirmed_fill(
    http: &reqwest::Client,
    rpc_url: &str,
    wallet_pubkey: &str,
    tx_signature: &str,
    expected: &ExecutionConfirmedFill,
    timeout_ms: u64,
) -> Result<Option<ExecutionConfirmedFill>> {
    let rpc_url = rpc_url.trim();
    if rpc_url.is_empty() || wallet_pubkey.trim().is_empty() || tx_signature.trim().is_empty() {
        return Ok(None);
    }
    let response = http
        .post(rpc_url)
        .timeout(Duration::from_millis(timeout_ms.max(1)))
        .json(&transaction_request(tx_signature))
        .send()
        .await
        .context("confirmed transaction RPC request failed")?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("confirmed transaction RPC body read failed")?;
    if !status.is_success() {
        return Err(anyhow!(
            "confirmed transaction RPC returned HTTP {status}: {body}"
        ));
    }
    confirmed_fill_from_transaction_json(
        wallet_pubkey,
        expected,
        serde_json::from_str(&body).context("confirmed transaction RPC JSON decode failed")?,
    )
}

pub(crate) fn confirmed_fill_from_transaction_json(
    wallet_pubkey: &str,
    expected: &ExecutionConfirmedFill,
    value: Value,
) -> Result<Option<ExecutionConfirmedFill>> {
    if value.get("error").is_some() {
        return Ok(None);
    }
    let Some(result) = value.get("result").filter(|item| !item.is_null()) else {
        return Ok(None);
    };
    let Some((pre_raw, post_raw, decimals)) =
        token_balance_delta_inputs(result, wallet_pubkey, expected_token(expected))?
    else {
        return Ok(None);
    };
    let token_delta = post_raw - pre_raw;
    match expected {
        ExecutionConfirmedFill::Buy(fill) => {
            actual_buy_fill(result, wallet_pubkey, fill, token_delta, decimals)
        }
        ExecutionConfirmedFill::Sell(fill) => {
            actual_sell_fill(result, wallet_pubkey, fill, token_delta, decimals)
        }
    }
}

fn transaction_request(tx_signature: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": "execution-confirmed-fill",
        "method": "getTransaction",
        "params": [
            tx_signature,
            {
                "encoding": "jsonParsed",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0
            }
        ]
    })
}

fn actual_buy_fill(
    result: &Value,
    wallet_pubkey: &str,
    fill: &ExecutionConfirmedBuyFill,
    token_delta: i128,
    decimals: u8,
) -> Result<Option<ExecutionConfirmedFill>> {
    if token_delta <= 0 {
        return Ok(None);
    }
    let raw = u64::try_from(token_delta).context("buy token delta does not fit u64")?;
    let qty_exact = TokenQuantity::new(raw, decimals);
    let cost_sol = wallet_sol_delta(result, wallet_pubkey)
        .filter(|lamports| *lamports < 0)
        .map(|lamports| (-lamports) as f64 / 1_000_000_000.0)
        .unwrap_or(fill.cost_sol);
    Ok(Some(ExecutionConfirmedFill::Buy(
        ExecutionConfirmedBuyFill {
            order_id: fill.order_id.clone(),
            token: fill.token.clone(),
            qty: qty_exact.as_f64(),
            qty_exact: Some(qty_exact),
            cost_sol,
            fill_ts: fill.fill_ts,
        },
    )))
}

fn actual_sell_fill(
    result: &Value,
    wallet_pubkey: &str,
    fill: &ExecutionConfirmedSellFill,
    token_delta: i128,
    decimals: u8,
) -> Result<Option<ExecutionConfirmedFill>> {
    if token_delta >= 0 {
        return Ok(None);
    }
    let raw = u64::try_from(-token_delta).context("sell token delta does not fit u64")?;
    let target_qty_exact = TokenQuantity::new(raw, decimals);
    let target_qty = target_qty_exact.as_f64();
    let exit_price_sol = wallet_sol_delta(result, wallet_pubkey)
        .filter(|lamports| *lamports > 0)
        .map(|lamports| (lamports as f64 / 1_000_000_000.0) / target_qty)
        .filter(|price| price.is_finite() && *price > 0.0)
        .unwrap_or(fill.exit_price_sol);
    Ok(Some(ExecutionConfirmedFill::Sell(
        ExecutionConfirmedSellFill {
            order_id: fill.order_id.clone(),
            token: fill.token.clone(),
            target_qty,
            target_qty_exact: Some(target_qty_exact),
            exit_price_sol,
            dust_qty_epsilon: fill.dust_qty_epsilon,
            fill_ts: fill.fill_ts,
        },
    )))
}

fn expected_token(fill: &ExecutionConfirmedFill) -> &str {
    match fill {
        ExecutionConfirmedFill::Buy(fill) => &fill.token,
        ExecutionConfirmedFill::Sell(fill) => &fill.token,
    }
}

fn token_balance_delta_inputs(
    result: &Value,
    wallet_pubkey: &str,
    mint: &str,
) -> Result<Option<(i128, i128, u8)>> {
    let pre = token_balance_sum(result, "/meta/preTokenBalances", wallet_pubkey, mint)?;
    let post = token_balance_sum(result, "/meta/postTokenBalances", wallet_pubkey, mint)?;
    match (pre, post) {
        (None, None) => Ok(None),
        (Some((pre_raw, decimals)), None) => Ok(Some((pre_raw, 0, decimals))),
        (None, Some((post_raw, decimals))) => Ok(Some((0, post_raw, decimals))),
        (Some((pre_raw, pre_decimals)), Some((post_raw, post_decimals))) => {
            if pre_decimals != post_decimals {
                return Ok(None);
            }
            Ok(Some((pre_raw, post_raw, pre_decimals)))
        }
    }
}

fn token_balance_sum(
    result: &Value,
    path: &str,
    wallet_pubkey: &str,
    mint: &str,
) -> Result<Option<(i128, u8)>> {
    let Some(items) = result.pointer(path).and_then(Value::as_array) else {
        return Ok(None);
    };
    let mut total = 0_i128;
    let mut decimals = None;
    let mut found = false;
    for item in items {
        if item.get("owner").and_then(Value::as_str) != Some(wallet_pubkey)
            || item.get("mint").and_then(Value::as_str) != Some(mint)
        {
            continue;
        }
        let amount = item
            .pointer("/uiTokenAmount/amount")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("confirmed token balance missing raw amount"))?
            .parse::<i128>()
            .context("confirmed token balance raw amount is invalid")?;
        let item_decimals = item
            .pointer("/uiTokenAmount/decimals")
            .and_then(Value::as_u64)
            .and_then(|value| u8::try_from(value).ok())
            .ok_or_else(|| anyhow!("confirmed token balance missing decimals"))?;
        if decimals.is_some_and(|existing| existing != item_decimals) {
            return Ok(None);
        }
        decimals = Some(item_decimals);
        total += amount;
        found = true;
    }
    Ok(found.then_some((total, decimals.unwrap_or(0))))
}

fn wallet_sol_delta(result: &Value, wallet_pubkey: &str) -> Option<i128> {
    let index = wallet_account_index(result, wallet_pubkey)?;
    let pre = result
        .pointer("/meta/preBalances")
        .and_then(Value::as_array)?
        .get(index)?
        .as_i64()?;
    let post = result
        .pointer("/meta/postBalances")
        .and_then(Value::as_array)?
        .get(index)?
        .as_i64()?;
    Some(i128::from(post) - i128::from(pre))
}

fn wallet_account_index(result: &Value, wallet_pubkey: &str) -> Option<usize> {
    let keys = result
        .pointer("/transaction/message/accountKeys")
        .and_then(Value::as_array)?;
    for (index, key) in keys.iter().enumerate() {
        let pubkey = key
            .as_str()
            .or_else(|| key.get("pubkey").and_then(Value::as_str))?;
        if pubkey == wallet_pubkey {
            return Some(index);
        }
    }
    None
}
