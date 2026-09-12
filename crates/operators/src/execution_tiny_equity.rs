use serde::Serialize;

use crate::execution_canary_quote_pnl_wallet::WalletReconciliationReport;

const ZERO_TOKEN_ACCOUNT_RENT_LAMPORTS: u64 = 2_039_280;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EquityViewReport {
    pub liquid_sol: Option<f64>,
    pub open_position_count: u64,
    pub open_position_cost_sol: Option<f64>,
    pub open_position_quote_value_sol: Option<f64>,
    pub open_unrealized_pnl_sol: Option<f64>,
    pub untracked_quote_value_sol: Option<f64>,
    pub terminal_leftover_quote_value_sol: Option<f64>,
    pub recoverable_zero_ata_rent_sol: Option<f64>,
    pub wallet_mark_value_sol: Option<f64>,
    pub wallet_mark_value_without_recoverable_rent_sol: Option<f64>,
}

pub fn build_equity_view(
    open_position_count: u64,
    open_position_cost_sol: Option<f64>,
    open_position_quote_value_sol: Option<f64>,
    wallet: Option<&WalletReconciliationReport>,
) -> EquityViewReport {
    let open_unrealized_pnl_sol = open_position_quote_value_sol
        .zip(open_position_cost_sol)
        .map(|(value, cost)| value - cost);
    let liquid_sol = wallet.and_then(|wallet| wallet.sol_balance_sol);
    let untracked_quote_value_sol =
        wallet.and_then(|wallet| quote_value_for_class(wallet, "untracked_wallet_balance"));
    let terminal_leftover_quote_value_sol = wallet.and_then(|wallet| {
        combine_marks(&[
            quote_value_for_class(wallet, "terminal_no_route_leftover"),
            quote_value_for_class(wallet, "failed_sell_leftover"),
        ])
    });
    let recoverable_zero_ata_rent_sol = wallet.filter(|w| w.inventory_complete).map(|wallet| {
        wallet.zero_token_account_count as f64 * ZERO_TOKEN_ACCOUNT_RENT_LAMPORTS as f64
            / 1_000_000_000.0
    });
    let wallet_mark_value_sol = combine_marks(&[
        liquid_sol,
        open_position_quote_value_sol,
        untracked_quote_value_sol,
        terminal_leftover_quote_value_sol,
        recoverable_zero_ata_rent_sol,
    ]);
    let wallet_mark_value_without_recoverable_rent_sol = combine_marks(&[
        liquid_sol,
        open_position_quote_value_sol,
        untracked_quote_value_sol,
        terminal_leftover_quote_value_sol,
    ]);

    EquityViewReport {
        liquid_sol,
        open_position_count,
        open_position_cost_sol,
        open_position_quote_value_sol,
        open_unrealized_pnl_sol,
        untracked_quote_value_sol,
        terminal_leftover_quote_value_sol,
        recoverable_zero_ata_rent_sol,
        wallet_mark_value_sol,
        wallet_mark_value_without_recoverable_rent_sol,
    }
}

fn quote_value_for_class(wallet: &WalletReconciliationReport, classification: &str) -> Option<f64> {
    if !wallet.inventory_complete
        || !wallet.bot_remainder_mark.positions_loaded
        || wallet
            .balances
            .iter()
            .any(|b| b.classification == "ambiguous_bot_attribution")
    {
        return None;
    }
    wallet
        .balances
        .iter()
        .filter(|balance| balance.classification == classification)
        .try_fold(0.0, |sum, balance| {
            crate::execution_wallet_marks::quote_value(balance.sell_quote.as_ref()).map(|v| sum + v)
        })
}

fn combine_marks(values: &[Option<f64>]) -> Option<f64> {
    values
        .iter()
        .try_fold(0.0, |sum, value| value.map(|value| sum + value))
}
