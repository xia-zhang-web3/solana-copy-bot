use crate::FailedExpenseReport;
use anyhow::{ensure, Context, Result};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ClosedEntryLoss {
    pub loss_lamports: String,
    pub basis: String,
    pub window_basis: String,
    pub positions: u64,
    pub lamport_backed_positions: u64,
    pub legacy_f64_positions: u64,
    pub legacy_null_positions: u64,
    pub exact: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CashEntryLoss {
    pub additional_loss_lamports: Option<String>,
    pub day_gross_negative_lamports: Option<String>,
    pub validated_day_events: Option<u64>,
    pub undated_obligations: Option<crate::UndatedSellCashObligations>,
    pub coverage: String,
    pub unavailable_reason: Option<String>,
    pub window_basis: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExecutionCanaryEntryCost {
    pub since: String,
    pub as_of: String,
    pub scope: String,
    pub comparison_basis: String,
    pub closed_loss: ClosedEntryLoss,
    pub failed_expenses: FailedExpenseReport,
    pub cash_loss: CashEntryLoss,
    pub policy: String,
    /// CLOSED + known failed only; explicitly partial when cash is unavailable.
    pub partial_known_subtotal_lamports: String,
    /// Arithmetic subtotal, not complete cohort loss or economic PnL.
    pub known_total_lamports: Option<String>,
    pub economic_pnl_lamports: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EntryCostCap {
    pub cap_sol_decimal: String,
    pub cap_lamports_ceiling: String,
    pub exhausted: bool,
}

impl ExecutionCanaryEntryCost {
    pub fn known_total(&self) -> Result<u128> {
        ensure!(
            self.cash_loss.unavailable_reason.is_none(),
            "cash_loss_unavailable"
        );
        let cash = self
            .cash_loss
            .additional_loss_lamports
            .as_deref()
            .context("cash_loss_unavailable")?;
        self.partial_known_subtotal()?
            .checked_add(super::math::canonical_u128(cash)?)
            .context("entry cost known total overflow")
    }

    pub fn partial_known_subtotal(&self) -> Result<u128> {
        let closed = super::math::canonical_u128(&self.closed_loss.loss_lamports)?;
        // Empty/unknown cohort contributes no known fee, without changing its null/coverage.
        let failed = self
            .failed_expenses
            .known_wallet_fee_lamports
            .as_deref()
            .map(super::math::canonical_u128)
            .transpose()?
            .unwrap_or(0);
        closed
            .checked_add(failed)
            .context("entry cost known subtotal overflow")
    }

    pub fn check_cap(&self, cap_sol: f64) -> Result<EntryCostCap> {
        ensure!(
            self.failed_expenses.coverage != "schema_unavailable",
            "entry cost failed-expense schema unavailable"
        );
        let total = self.known_total()?;
        ensure!(
            Some(total.to_string()) == self.known_total_lamports,
            "entry cost subtotal mismatch"
        );
        let ceiling = super::math::cap_lamports_ceiling(cap_sol)?;
        Ok(EntryCostCap {
            cap_sol_decimal: cap_sol.to_string(),
            cap_lamports_ceiling: ceiling.to_string(),
            exhausted: total >= ceiling,
        })
    }

    /// Display only; unavailable cash shows the explicitly partial CLOSED+failed subtotal.
    /// Guard comparisons use check_cap, never this approximation.
    pub fn approximate_known_loss_sol(&self) -> Result<f64> {
        Ok(if self.cash_loss.unavailable_reason.is_some() {
            self.partial_known_subtotal()?
        } else {
            self.known_total()?
        } as f64
            / 1_000_000_000.0)
    }

    pub fn selected_cost_complete(&self) -> bool {
        self.closed_loss.exact
            && self.cash_loss.unavailable_reason.is_none()
            && self.cash_loss.additional_loss_lamports.is_some()
            && self.failed_expenses.coverage == "complete_selected_cohort"
    }
}
