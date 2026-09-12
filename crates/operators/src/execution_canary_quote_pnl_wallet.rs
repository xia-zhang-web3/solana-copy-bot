use crate::execution_wallet_marks::{bot_marks, bound_quote, quote_reason, quote_value};
pub use crate::execution_wallet_marks::{BotRemainderMark, QuoteCoverage};
use crate::execution_wallet_quote::raw_amount;
pub use crate::execution_wallet_quote::WalletQuoteRequest;
use chrono::{DateTime, Utc};
use copybot_storage_core::ExecutionCanaryOwnedPosition;
use serde::Serialize;
use std::collections::BTreeMap;

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
    pub inventory_complete: bool,
    pub wallet_account_mark: QuoteCoverage,
    pub bot_remainder_mark: BotRemainderMark,
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
    pub request: WalletQuoteRequest,
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
    open_positions: Result<&[ExecutionCanaryOwnedPosition], &str>,
    sell_side: &SellSideDiagnosticsReport,
    quote_proofs: Vec<WalletSellQuoteProof>,
    inventory_complete: bool,
    errors: Vec<String>,
) -> WalletReconciliationReport {
    let invalid_account_count = token_account_count.saturating_sub(balances.len() as u64);
    let inventory_complete = inventory_complete
        && invalid_account_count == 0
        && balances.iter().all(|b| {
            balances
                .iter()
                .filter(|other| other.token_account == b.token_account)
                .count()
                == 1
                && raw_amount(&b.amount_raw).is_some()
        });
    let zero_token_account_count = balances
        .iter()
        .filter(|b| raw_amount(&b.amount_raw) == Some(0))
        .count() as u64;
    let quote_proofs: Vec<_> = quote_proofs
        .into_iter()
        .filter(|q| {
            balances
                .iter()
                .filter(|b| b.token_account == q.request.token_account)
                .count()
                == 1
        })
        .collect();
    let bot_remainder_mark = bot_marks(
        &owner_pubkey,
        inventory_complete,
        &balances,
        open_positions,
        &quote_proofs,
    );
    let positions = open_positions.unwrap_or(&[]);
    let balances: Vec<_> = balances
        .into_iter()
        .filter(|b| raw_amount(&b.amount_raw).is_some_and(|n| n > 0))
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
        let bound_position = bot_remainder_mark.positions.iter().find(|p| {
            p.binding_reason.is_none()
                && p.token_account.as_deref() == Some(balance.token_account.as_str())
        });
        let open_position = bound_position
            .and_then(|mark| positions.iter().find(|p| p.position_id == mark.position_id))
            .map(|p| WalletOpenPositionSummary {
                position_id: p.position_id.clone(),
                qty: p.qty,
                cost_sol: p.cost_sol,
                opened_ts: p.opened_ts,
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
        let classification = if !bot_remainder_mark.positions_loaded
            || (open_position.is_none() && positions.iter().any(|p| p.token == balance.mint))
        {
            "ambiguous_bot_attribution"
        } else {
            classify_balance(open_position.is_some(), failure.as_ref())
        };
        match classification {
            "terminal_no_route_leftover" => terminal_no_route_leftover_count += 1,
            "failed_sell_leftover" => failed_sell_leftover_count += 1,
            "untracked_wallet_balance" => untracked_nonzero_count += 1,
            _ => {}
        }
        let sell_quote = bound_quote(&owner_pubkey, &balance, &quote_proofs).cloned();
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
            .then(left.token_account.cmp(&right.token_account))
    });

    let unmatched_open_position_tokens: Vec<_> = bot_remainder_mark
        .positions
        .iter()
        .filter(|p| p.binding_reason.is_some())
        .map(|p| p.token.clone())
        .collect();
    let matched_open_position_count = positions.len() - unmatched_open_position_tokens.len();
    let values: Vec<_> = rows
        .iter()
        .map(|r| quote_value(r.sell_quote.as_ref()))
        .collect();
    let mut reasons: Vec<_> = rows
        .iter()
        .filter_map(|r| quote_reason(r.sell_quote.as_ref()))
        .collect();
    if !inventory_complete {
        reasons.push("wallet_inventory_incomplete".into());
    }
    reasons.sort();
    reasons.dedup();
    reasons.truncate(16);
    let mut wallet_account_mark = QuoteCoverage::from_values(
        "full_wallet_accounts_gross_independent_quotes",
        inventory_complete,
        &values,
        reasons,
    );
    wallet_account_mark.unknown_items += invalid_account_count;
    let source_status = if errors.is_empty() && inventory_complete {
        "ok"
    } else {
        "partial"
    };
    WalletReconciliationReport {
        as_of,
        owner_pubkey,
        source_status: source_status.to_string(),
        errors,
        sol_balance_lamports,
        sol_balance_sol: sol_balance_lamports.map(|lamports| lamports as f64 / 1_000_000_000.0),
        token_account_count,
        zero_token_account_count,
        nonzero_token_account_count: rows.len() as u64,
        bot_open_position_count: positions.len() as u64,
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
        inventory_complete,
        wallet_account_mark,
        bot_remainder_mark,
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
