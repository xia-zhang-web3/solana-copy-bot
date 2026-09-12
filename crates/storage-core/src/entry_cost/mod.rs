mod cash;
mod closed;
mod failed;
mod math;
mod types;
pub use types::{CashEntryLoss, ClosedEntryLoss, EntryCostCap, ExecutionCanaryEntryCost};

use crate::SqliteDiscoveryStore;
use anyhow::Result;
use chrono::{DateTime, Utc};

impl SqliteDiscoveryStore {
    /// Current BUY cost guard only; all components share one SQLite read snapshot.
    pub fn execution_canary_entry_cost(
        &self,
        as_of: DateTime<Utc>,
    ) -> Result<ExecutionCanaryEntryCost> {
        let since = DateTime::<Utc>::from_naive_utc_and_offset(
            as_of
                .date_naive()
                .and_hms_opt(0, 0, 0)
                .expect("UTC midnight"),
            Utc,
        );
        let tx = self.conn.unchecked_transaction()?;
        let closed_loss = closed::read(&tx, since)?;
        let failed_expenses = failed::read(&tx, since, as_of)?;
        let cash_loss = cash::read(&tx, since, as_of)?;
        let mut value = ExecutionCanaryEntryCost {
            since: since.to_rfc3339(), as_of: as_of.to_rfc3339(),
            scope: "all_DB_canary_positions_and_exec_canary_cash_and_failed_claims_no_config_wallet_or_route_filter".into(),
            comparison_basis: "checked_u128_lamports_against_ceiling_of_shortest_decimal_config_SOL_no_epsilon".into(),
            closed_loss, failed_expenses, cash_loss, known_total_lamports: None,
            policy: "sum_per_persisted_position_max_original_CLOSED_loss_floor_and_day_gross_negative_cash_plus_known_failed_fees_once_not_net_or_economic_daily_loss".into(),
            partial_known_subtotal_lamports: String::new(),
            economic_pnl_lamports: None,
        };
        value.partial_known_subtotal_lamports = value.partial_known_subtotal()?.to_string();
        if value.cash_loss.unavailable_reason.is_none() {
            value.known_total_lamports = Some(value.known_total()?.to_string());
        }
        tx.commit()?;
        Ok(value)
    }
}
