use super::tiny_submit::tiny_submit_runtime_block_reason;
use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_core_types::TokenQuantity;
use copybot_storage_core::{ExecutionCanaryPositionRecordOutcome, SqliteStore};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::Duration;

const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

#[derive(Debug, Clone, PartialEq, Eq)]
struct WalletTokenBalance {
    mint: String,
    raw: u64,
    decimals: u8,
}

pub(super) async fn process_tiny_submit_orphan_position_recovery_for_route(
    config: &ExecutionConfig,
    store: &SqliteStore,
    now: DateTime<Utc>,
) -> Result<ExecutionCanaryStateMachineSummary> {
    let mut summary = ExecutionCanaryStateMachineSummary::default();
    if let Some(reason) = tiny_submit_runtime_block_reason(config) {
        summary.safety_blocked = 1;
        summary.skipped_reason = Some(reason);
        return Ok(summary);
    }
    if config.priority_fee_canary_rpc_url.trim().is_empty()
        || config.canary_wallet_pubkey.trim().is_empty()
    {
        return Ok(summary);
    }

    let balances = match fetch_wallet_token_balances(config).await {
        Ok(balances) => balances,
        Err(error) => {
            summary.orphan_recovery_errors = 1;
            summary.skipped_reason = Some("orphan_recovery_rpc_error");
            summary.last_error = Some(format!("{error:#}"));
            return Ok(summary);
        }
    };
    summary.orphan_recovery_checked = balances.len();

    let limit = config.canary_max_open_positions.max(1) as usize;
    for balance in balances {
        if summary.orphan_recovery_recovered >= limit {
            break;
        }
        if store
            .load_execution_canary_open_position(&balance.mint)?
            .is_some()
        {
            continue;
        }
        if !store.has_execution_canary_position_history(&balance.mint)? {
            summary.orphan_recovery_skipped_no_history += 1;
            continue;
        }
        let qty = TokenQuantity::new(balance.raw, balance.decimals);
        let order_id = recovery_order_id(&balance, now);
        let result = store.record_execution_canary_open_position(
            &order_id,
            &balance.mint,
            qty.as_f64(),
            Some(qty),
            config.canary_buy_size_sol.max(0.0),
            now,
        )?;
        if result.outcome != ExecutionCanaryPositionRecordOutcome::Existing {
            summary.orphan_recovery_recovered += 1;
            summary.last_orphan_recovery_token = Some(balance.mint);
        }
    }
    summary.open_positions = store.execution_canary_open_position_count()?;
    Ok(summary)
}

async fn fetch_wallet_token_balances(config: &ExecutionConfig) -> Result<Vec<WalletTokenBalance>> {
    let response = reqwest::Client::new()
        .post(config.priority_fee_canary_rpc_url.trim())
        .timeout(Duration::from_millis(config.quote_canary_timeout_ms.max(1)))
        .json(&json!({
            "jsonrpc": "2.0",
            "id": "execution-canary-orphan-recovery",
            "method": "getTokenAccountsByOwner",
            "params": [
                config.canary_wallet_pubkey.as_str(),
                {"programId": TOKEN_PROGRAM_ID},
                {"encoding": "jsonParsed", "commitment": "confirmed"}
            ]
        }))
        .send()
        .await
        .context("wallet token account RPC request failed")?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("wallet token account RPC body read failed")?;
    if !status.is_success() {
        return Err(anyhow!(
            "wallet token account RPC returned HTTP {status}: {body}"
        ));
    }
    parse_wallet_token_balances(
        serde_json::from_str(&body).context("wallet token account RPC JSON decode failed")?,
    )
}

fn parse_wallet_token_balances(value: Value) -> Result<Vec<WalletTokenBalance>> {
    if let Some(error) = value.get("error") {
        return Err(anyhow!("wallet token account RPC error: {error}"));
    }
    let accounts = value
        .get("result")
        .and_then(|result| result.get("value"))
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("wallet token account response missing result.value"))?;
    let mut by_mint: BTreeMap<String, WalletTokenBalance> = BTreeMap::new();
    for account in accounts {
        let Some(info) = account.pointer("/account/data/parsed/info") else {
            continue;
        };
        let Some(mint) = info.get("mint").and_then(Value::as_str) else {
            continue;
        };
        let Some(amount) = info.get("tokenAmount") else {
            continue;
        };
        let Some(raw) = amount
            .get("amount")
            .and_then(Value::as_str)
            .and_then(|raw| raw.parse::<u64>().ok())
            .filter(|raw| *raw > 0)
        else {
            continue;
        };
        let Some(decimals) = amount
            .get("decimals")
            .and_then(Value::as_u64)
            .and_then(|raw| u8::try_from(raw).ok())
        else {
            continue;
        };
        let entry = by_mint
            .entry(mint.to_string())
            .or_insert(WalletTokenBalance {
                mint: mint.to_string(),
                raw: 0,
                decimals,
            });
        if entry.decimals != decimals {
            return Err(anyhow!("wallet token account decimals mismatch for {mint}"));
        }
        entry.raw = entry
            .raw
            .checked_add(raw)
            .ok_or_else(|| anyhow!("wallet token account raw amount overflow for {mint}"))?;
    }
    Ok(by_mint.into_values().collect())
}

fn recovery_order_id(balance: &WalletTokenBalance, now: DateTime<Utc>) -> String {
    format!(
        "recovery-orphan:{}:{}:{}",
        balance.mint,
        balance.raw,
        now.timestamp_micros()
    )
}
