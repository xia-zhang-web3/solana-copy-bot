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

const SPL_TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const TOKEN_ACCOUNT_PROGRAM_IDS: [&str; 2] = [SPL_TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID];
const ORPHAN_RECOVERY_TERMINAL_WRITE_OFF_REASON: &str = "orphan_recovery_terminal_write_off";

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
    let balances_by_mint: BTreeMap<String, WalletTokenBalance> = balances
        .iter()
        .map(|balance| (balance.mint.clone(), balance.clone()))
        .collect();
    summary.orphan_recovery_checked = balances.iter().filter(|balance| balance.raw > 0).count();
    reconcile_recovery_orphan_positions(store, &balances_by_mint, now, &mut summary)?;

    let limit = config.canary_max_open_positions.max(1) as usize;
    for balance in balances {
        if balance.raw == 0 {
            continue;
        }
        if summary.orphan_recovery_recovered >= limit {
            break;
        }
        if let Some(position) = store.load_execution_canary_open_position(&balance.mint)? {
            if position
                .position_id
                .starts_with("exec-canary-pos:recovery-orphan:")
            {
                if let Some(opened_ts) = store.execution_canary_recovery_opened_ts(&balance.mint)? {
                    store.retimestamp_execution_canary_orphan_open_position(
                        &balance.mint,
                        opened_ts,
                    )?;
                }
            }
            continue;
        }
        let Some(opened_ts) = store.execution_canary_recovery_opened_ts(&balance.mint)? else {
            summary.orphan_recovery_skipped_no_history += 1;
            continue;
        };
        if store.has_execution_canary_terminal_write_off_for_token(&balance.mint)? {
            summary.last_orphan_recovery_token = Some(balance.mint);
            summary
                .skipped_reason
                .get_or_insert(ORPHAN_RECOVERY_TERMINAL_WRITE_OFF_REASON);
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
            opened_ts,
        )?;
        if result.outcome != ExecutionCanaryPositionRecordOutcome::Existing {
            summary.orphan_recovery_recovered += 1;
            summary.last_orphan_recovery_token = Some(balance.mint);
        }
    }
    summary.open_positions = store.execution_canary_open_position_count()?;
    Ok(summary)
}

fn reconcile_recovery_orphan_positions(
    store: &SqliteStore,
    balances_by_mint: &BTreeMap<String, WalletTokenBalance>,
    now: DateTime<Utc>,
    summary: &mut ExecutionCanaryStateMachineSummary,
) -> Result<()> {
    for position in store.list_execution_canary_open_recovery_orphan_positions()? {
        let Some(position_qty) = position.qty_exact else {
            summary.orphan_recovery_errors += 1;
            summary.last_error = Some(format!(
                "recovery orphan {} missing exact qty",
                position.position_id
            ));
            continue;
        };
        let wallet_raw = match balances_by_mint.get(&position.token) {
            Some(balance) => {
                if balance.decimals != position_qty.decimals() {
                    summary.orphan_recovery_errors += 1;
                    summary.last_error = Some(format!(
                        "recovery orphan wallet decimals mismatch for {}",
                        position.token
                    ));
                    continue;
                }
                balance.raw
            }
            None => 0,
        };
        if store.has_execution_canary_terminal_write_off_for_token(&position.token)? {
            store
                .close_execution_canary_open_position(
                    &position.token,
                    position_qty.as_f64(),
                    Some(position_qty),
                    0.0,
                    1e-9,
                    now,
                )
                .with_context(|| {
                    format!(
                        "failed closing terminal write-off recovery orphan {}",
                        position.position_id
                    )
                })?;
            summary.orphan_recovery_reconciled += 1;
            summary.last_orphan_recovery_token = Some(position.token);
            summary
                .skipped_reason
                .get_or_insert(ORPHAN_RECOVERY_TERMINAL_WRITE_OFF_REASON);
            continue;
        }
        if wallet_raw >= position_qty.raw() {
            continue;
        }
        let missing_qty =
            TokenQuantity::new(position_qty.raw() - wallet_raw, position_qty.decimals());
        store
            .close_execution_canary_open_position(
                &position.token,
                missing_qty.as_f64(),
                Some(missing_qty),
                0.0,
                1e-9,
                now,
            )
            .with_context(|| {
                format!(
                    "failed reconciling recovery orphan {} to wallet balance",
                    position.position_id
                )
            })?;
        summary.orphan_recovery_reconciled += 1;
        summary.last_orphan_recovery_token = Some(position.token);
    }
    Ok(())
}

async fn fetch_wallet_token_balances(config: &ExecutionConfig) -> Result<Vec<WalletTokenBalance>> {
    let mut by_mint: BTreeMap<String, WalletTokenBalance> = BTreeMap::new();
    for program_id in TOKEN_ACCOUNT_PROGRAM_IDS {
        merge_wallet_token_balances(
            &mut by_mint,
            fetch_wallet_token_balances_for_program(config, program_id).await?,
        )?;
    }
    Ok(by_mint.into_values().collect())
}

async fn fetch_wallet_token_balances_for_program(
    config: &ExecutionConfig,
    program_id: &str,
) -> Result<Vec<WalletTokenBalance>> {
    let response = reqwest::Client::new()
        .post(config.priority_fee_canary_rpc_url.trim())
        .timeout(Duration::from_millis(config.quote_canary_timeout_ms.max(1)))
        .json(&json!({
            "jsonrpc": "2.0",
            "id": "execution-canary-orphan-recovery",
            "method": "getTokenAccountsByOwner",
            "params": [
                config.canary_wallet_pubkey.as_str(),
                {"programId": program_id},
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

fn merge_wallet_token_balances(
    by_mint: &mut BTreeMap<String, WalletTokenBalance>,
    balances: Vec<WalletTokenBalance>,
) -> Result<()> {
    for balance in balances {
        let entry = by_mint
            .entry(balance.mint.clone())
            .or_insert(WalletTokenBalance {
                mint: balance.mint.clone(),
                raw: 0,
                decimals: balance.decimals,
            });
        if entry.decimals != balance.decimals {
            return Err(anyhow!(
                "wallet token account decimals mismatch for {}",
                balance.mint
            ));
        }
        entry.raw = entry.raw.checked_add(balance.raw).ok_or_else(|| {
            anyhow!(
                "wallet token account raw amount overflow for {}",
                balance.mint
            )
        })?;
    }
    Ok(())
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
