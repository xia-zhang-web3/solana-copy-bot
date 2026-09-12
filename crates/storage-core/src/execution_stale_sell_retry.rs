//! Retire only an unchanged unsigned stale-amount attempt for the existing build retry.
use crate::{
    ExecutionCanaryOrder, SqliteDiscoveryStore, EXECUTION_ERROR_BUILD_FAILED,
    EXECUTION_STATUS_CANARY_BUILT, EXECUTION_STATUS_CANARY_CANDIDATE,
    EXECUTION_STATUS_CANARY_FAILED, EXECUTION_STATUS_CANARY_SIMULATED,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::CopySignalRow;
use rusqlite::params;

impl SqliteDiscoveryStore {
    /// No attempt increment and no payload/accounting writes: the normal bounded
    /// failed-build consumer must obtain a new wallet/quote/proof for the next attempt.
    pub fn mark_execution_canary_stale_sell_amount_failed(
        &self,
        expected: &ExecutionCanaryOrder,
        signal: &CopySignalRow,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        self.with_immediate_transaction_retry("unsigned stale SELL amount refusal", |conn| {
            if !signal.side.eq_ignore_ascii_case("sell")
                || expected.signal_id != signal.signal_id
                || ![
                    EXECUTION_STATUS_CANARY_CANDIDATE,
                    EXECUTION_STATUS_CANARY_BUILT,
                    EXECUTION_STATUS_CANARY_SIMULATED,
                ]
                .contains(&expected.status.as_str())
                || expected
                    .tx_signature
                    .as_deref()
                    .is_some_and(|s| !s.trim().is_empty())
                || self
                    .load_execution_canary_order(&expected.order_id)?
                    .as_ref()
                    != Some(expected)
            {
                return Ok(false);
            }
            let Some(saved) = self.load_copy_signal_by_signal_id(&signal.signal_id)? else {
                return Ok(false);
            };
            if !same_signal(signal, &saved)
                || self
                    .execution_sell_intent_block_in_snapshot(conn, &saved)?
                    .is_some()
                || self
                    .execution_canary_receipt_submit_block_reason(
                        &expected.order_id,
                        &saved.token,
                        &saved.side,
                    )?
                    .is_some()
            {
                return Ok(false);
            }
            // A durable dispatch or receipt is authoritative even if a corrupt/stale
            // order row looks unsigned. Never retire an accounting obligation.
            let sent: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM execution_canary_dispatch WHERE order_id=?1)
                  OR EXISTS(SELECT 1 FROM execution_canary_receipt_proofs WHERE order_id=?1)
                  OR EXISTS(SELECT 1 FROM execution_canary_receipt_facts WHERE order_id=?1)
                  OR EXISTS(SELECT 1 FROM fills WHERE order_id=?1)",
                [&expected.order_id],
                |r| r.get(0),
            )?;
            if sent {
                return Ok(false);
            }
            conn.execute(
                "UPDATE orders SET status=?2, confirm_ts=?3, err_code=?4,
                simulation_error='source_sell_amount_stale' WHERE order_id=?1",
                params![
                    expected.order_id,
                    EXECUTION_STATUS_CANARY_FAILED,
                    now.to_rfc3339(),
                    EXECUTION_ERROR_BUILD_FAILED
                ],
            )?;
            Ok(true)
        })
    }
}
fn same_signal(a: &CopySignalRow, b: &CopySignalRow) -> bool {
    a.signal_id == b.signal_id
        && a.wallet_id == b.wallet_id
        && a.side == b.side
        && a.token == b.token
        && a.status == b.status
        && a.ts == b.ts
        && a.notional_sol.to_bits() == b.notional_sol.to_bits()
        && a.notional_lamports == b.notional_lamports
        && a.notional_origin == b.notional_origin
}
