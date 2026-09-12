mod read;
mod types;
pub(crate) use read::{obligations, visit_events};
pub use types::{ExecutionCanarySellCashDay, RecognizedSellCashEvents, UndatedSellCashObligations};

use crate::SqliteDiscoveryStore;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};

impl SqliteDiscoveryStore {
    /// Read-only cash event model; the entry guard applies a separate CLOSED overlap policy.
    /// Uses parsed local accounting timestamps in [UTC midnight, as_of).
    /// All canary cash claims are validated, including those outside the window;
    /// current undated obligations have no attribution to the requested day.
    pub fn execution_canary_sell_cash_day(
        &self,
        as_of: DateTime<Utc>,
    ) -> Result<ExecutionCanarySellCashDay> {
        let since = as_of
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .context("cash day midnight out of range")?
            .and_utc();
        let tx = self
            .conn
            .unchecked_transaction()
            .context("begin SELL cash day read snapshot")?;
        let known_events = read::events(&tx, since, as_of)?;
        let undated_obligations = read::obligations(&tx)?;
        tx.commit().context("finish SELL cash day read snapshot")?;
        Ok(ExecutionCanarySellCashDay {
            since: since.to_rfc3339(),
            as_of: as_of.to_rfc3339(),
            scope: "all_DB_exec_canary_receipt_native_cash_SELL_events_no_wallet_route_or_position_state_filter".into(),
            window_basis: "parsed_settlement_ts_local_accounting_time_in_UTC_day_start_inclusive_as_of_exclusive".into(),
            coverage: "validated_dated_events_only_full_day_history_unproven".into(),
            known_events, undated_obligations,
            full_day_cash_result_lamports: None,
            economic_pnl_lamports: None,
            decomposition: "unresolved".into(),
        })
    }
}
