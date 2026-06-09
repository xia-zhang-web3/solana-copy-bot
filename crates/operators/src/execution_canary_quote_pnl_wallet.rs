use chrono::{DateTime, Utc};
use copybot_storage_core::ExecutionTinyProofOpenPosition;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

use crate::execution_canary_quote_pnl_sell_side::SellSideDiagnosticsReport;

const NEAR_ZERO_QUOTE_LAMPORTS: u64 = 10_000;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WalletReconciliationReport {
    pub as_of: DateTime<Utc>,
    pub owner_pubkey: String,
    pub source_status: String,
    pub errors: Vec<String>,
    pub sol_balance_lamports: Option<u64>,
    pub sol_balance_sol: Option<f64>,
    pub token_account_count: u64,
    pub zero_token_account_count: u64,
    pub nonzero_token_account_count: u64,
    pub bot_open_position_count: u64,
    pub matched_open_position_count: u64,
    pub unmatched_open_position_count: u64,
    pub unmatched_open_position_tokens: Vec<String>,
    pub terminal_no_route_leftover_count: u64,
    pub failed_sell_leftover_count: u64,
    pub untracked_nonzero_count: u64,
    pub quote_ok_count: u64,
    pub quote_no_route_count: u64,
    pub near_zero_quote_count: u64,
    pub near_zero_lamports_threshold: u64,
    pub balances: Vec<WalletTokenReconciliation>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WalletTokenReconciliation {
    pub mint: String,
    pub token_account: String,
    pub amount_raw: String,
    pub ui_amount_string: String,
    pub decimals: u8,
    pub classification: String,
    pub bot_open_position: Option<WalletOpenPositionSummary>,
    pub sell_failure: Option<WalletSellFailureSummary>,
    pub sell_quote: Option<WalletSellQuoteProof>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WalletOpenPositionSummary {
    pub position_id: String,
    pub qty: f64,
    pub cost_sol: f64,
    pub opened_ts: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WalletSellFailureSummary {
    pub orders: u64,
    pub terminal_no_route_orders: u64,
    pub terminal_simulation_orders: u64,
    pub latest_error: Option<String>,
    pub next_action: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WalletTokenBalance {
    pub token_account: String,
    pub mint: String,
    pub amount_raw: String,
    pub ui_amount_string: String,
    pub decimals: u8,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WalletSellQuoteProof {
    pub status: String,
    pub http_status: Option<u16>,
    pub error: Option<String>,
    pub error_code: Option<String>,
    pub out_amount_raw: Option<String>,
    pub out_sol: Option<f64>,
    pub price_impact_pct: Option<String>,
    pub route_labels: Vec<String>,
}

pub fn build_wallet_reconciliation_from_parts(
    as_of: DateTime<Utc>,
    owner_pubkey: String,
    sol_balance_lamports: Option<u64>,
    token_account_count: u64,
    balances: Vec<WalletTokenBalance>,
    open_positions: &[ExecutionTinyProofOpenPosition],
    sell_side: &SellSideDiagnosticsReport,
    quote_proofs: BTreeMap<String, WalletSellQuoteProof>,
    errors: Vec<String>,
) -> WalletReconciliationReport {
    let open_by_token: BTreeMap<_, _> = open_positions
        .iter()
        .map(|position| (position.token.clone(), position))
        .collect();
    let balance_tokens: BTreeSet<_> = balances
        .iter()
        .map(|balance| balance.mint.clone())
        .collect();
    let failure_by_token: BTreeMap<_, _> = sell_side
        .failures_by_token
        .iter()
        .map(|failure| (failure.token.clone(), failure))
        .collect();

    let mut rows = Vec::with_capacity(balances.len());
    let mut terminal_no_route_leftover_count = 0;
    let mut failed_sell_leftover_count = 0;
    let mut untracked_nonzero_count = 0;
    let mut quote_ok_count = 0;
    let mut quote_no_route_count = 0;
    let mut near_zero_quote_count = 0;

    for balance in balances {
        let open_position =
            open_by_token
                .get(&balance.mint)
                .map(|position| WalletOpenPositionSummary {
                    position_id: position.position_id.clone(),
                    qty: position.qty,
                    cost_sol: position.cost_sol,
                    opened_ts: position.opened_ts,
                });
        let failure = failure_by_token
            .get(&balance.mint)
            .map(|failure| WalletSellFailureSummary {
                orders: failure.orders,
                terminal_no_route_orders: failure.terminal_no_route_orders,
                terminal_simulation_orders: failure.terminal_simulation_orders,
                latest_error: failure.latest_error.clone(),
                next_action: failure.next_action.clone(),
            });
        let classification = classify_balance(open_position.is_some(), failure.as_ref());
        match classification {
            "terminal_no_route_leftover" => terminal_no_route_leftover_count += 1,
            "failed_sell_leftover" => failed_sell_leftover_count += 1,
            "untracked_wallet_balance" => untracked_nonzero_count += 1,
            _ => {}
        }
        let sell_quote = quote_proofs.get(&balance.mint).cloned();
        if let Some(quote) = &sell_quote {
            if quote.status == "ok" {
                quote_ok_count += 1;
            }
            if quote.status == "no_route" {
                quote_no_route_count += 1;
            }
            if is_near_zero_quote(quote) {
                near_zero_quote_count += 1;
            }
        }
        rows.push(WalletTokenReconciliation {
            mint: balance.mint,
            token_account: balance.token_account,
            amount_raw: balance.amount_raw,
            ui_amount_string: balance.ui_amount_string,
            decimals: balance.decimals,
            classification: classification.to_string(),
            bot_open_position: open_position,
            sell_failure: failure,
            sell_quote,
        });
    }
    rows.sort_by(|left, right| {
        left.classification
            .cmp(&right.classification)
            .then(left.mint.cmp(&right.mint))
    });

    let unmatched_open_position_tokens: Vec<_> = open_by_token
        .keys()
        .filter(|token| !balance_tokens.contains(*token))
        .cloned()
        .collect();
    let matched_open_position_count = open_by_token.len() - unmatched_open_position_tokens.len();
    let source_status = if errors.is_empty() { "ok" } else { "partial" };
    WalletReconciliationReport {
        as_of,
        owner_pubkey,
        source_status: source_status.to_string(),
        errors,
        sol_balance_lamports,
        sol_balance_sol: sol_balance_lamports.map(|lamports| lamports as f64 / 1_000_000_000.0),
        token_account_count,
        zero_token_account_count: token_account_count.saturating_sub(rows.len() as u64),
        nonzero_token_account_count: rows.len() as u64,
        bot_open_position_count: open_by_token.len() as u64,
        matched_open_position_count: matched_open_position_count as u64,
        unmatched_open_position_count: unmatched_open_position_tokens.len() as u64,
        unmatched_open_position_tokens,
        terminal_no_route_leftover_count,
        failed_sell_leftover_count,
        untracked_nonzero_count,
        quote_ok_count,
        quote_no_route_count,
        near_zero_quote_count,
        near_zero_lamports_threshold: NEAR_ZERO_QUOTE_LAMPORTS,
        balances: rows,
    }
}

fn classify_balance(
    has_open_position: bool,
    failure: Option<&WalletSellFailureSummary>,
) -> &'static str {
    if has_open_position {
        return "bot_open_position";
    }
    if failure.is_some_and(|failure| failure.terminal_no_route_orders > 0) {
        return "terminal_no_route_leftover";
    }
    if failure.is_some() {
        return "failed_sell_leftover";
    }
    "untracked_wallet_balance"
}

fn is_near_zero_quote(quote: &WalletSellQuoteProof) -> bool {
    quote.status == "ok"
        && quote
            .out_amount_raw
            .as_deref()
            .and_then(|raw| raw.parse::<u64>().ok())
            .is_some_and(|lamports| lamports <= NEAR_ZERO_QUOTE_LAMPORTS)
}
