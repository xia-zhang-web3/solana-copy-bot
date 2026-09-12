use crate::execution_canary_quote_pnl_sell_side::SellSideDiagnosticsReport;
use crate::execution_canary_quote_pnl_wallet::{
    build_wallet_reconciliation_from_parts, WalletReconciliationReport,
};
use crate::execution_wallet_inventory::{rpc_call, token_accounts, valid_pubkey, Inventory};
use crate::execution_wallet_quote::{quote_sell, raw_amount, WalletQuoteRequest};
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::SqliteStore;
use reqwest::blocking::Client;
use serde_json::json;
use std::time::Duration;

pub(crate) fn build_live_wallet_reconciliation(
    config: &ExecutionConfig,
    store: &SqliteStore,
    sell_side: &SellSideDiagnosticsReport,
    as_of: DateTime<Utc>,
) -> WalletReconciliationReport {
    // The full exact cohort is separate from the display-limited tiny proof.
    let positions = store.list_execution_canary_open_positions();
    let owner = config.canary_wallet_pubkey.trim().to_string();
    let mut errors = Vec::new();
    let mut inventory = Inventory {
        balances: Vec::new(),
        account_count: 0,
        complete: false,
        errors: Vec::new(),
    };
    let mut sol = None;
    let mut proofs = Vec::new();
    let client = Client::builder()
        .timeout(Duration::from_millis(
            u64::from(config.quote_canary_timeout_ms).clamp(500, 10_000),
        ))
        .build();
    if !valid_pubkey(&owner) {
        errors.push("invalid_wallet_owner".into());
    } else if config.priority_fee_canary_rpc_url.trim().is_empty() {
        errors.push("missing_wallet_rpc_url".into());
    } else if let Ok(client) = client {
        let rpc_url = config.priority_fee_canary_rpc_url.trim();
        sol = rpc_call(&client, rpc_url, "getBalance", json!([owner]))
            .ok()
            .and_then(|v| v["value"].as_u64());
        if sol.is_none() {
            errors.push("native_balance_unavailable".into());
        }
        inventory = token_accounts(&client, rpc_url, &owner);
        errors.extend(inventory.errors.clone());
        let slippage = config
            .quote_canary_sell_slippage_bps
            .max(config.quote_canary_slippage_bps)
            .min(u64::from(u32::MAX)) as u32;
        for balance in &inventory.balances {
            if raw_amount(&balance.amount_raw).is_some_and(|v| v > 0)
                && inventory
                    .balances
                    .iter()
                    .filter(|b| b.token_account == balance.token_account)
                    .count()
                    == 1
            {
                let request = WalletQuoteRequest::for_balance(&owner, balance);
                proofs.push(quote_sell(
                    &client,
                    config.quote_canary_base_url.trim(),
                    request,
                    slippage,
                ));
            }
        }
    } else {
        errors.push("http_client_unavailable".into());
    }
    if positions.is_err() {
        errors.push("bot_positions_read_failed".into());
    }
    build_wallet_reconciliation_from_parts(
        as_of,
        owner,
        sol,
        inventory.account_count,
        inventory.balances,
        positions
            .as_deref()
            .map_err(|_| "bot_positions_read_failed"),
        sell_side,
        proofs,
        inventory.complete,
        errors,
    )
}
