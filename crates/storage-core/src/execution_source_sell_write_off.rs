use crate::{
    execution_canary_position_close::write_off_exact_position_on_conn,
    execution_canary_position_open::load_position_by_id,
    execution_canary_retry_terminal::mark_terminal_sell_blocked_on_conn, source_sell_intent_rows,
    source_sell_promotion_guard, source_sell_write_off_eligibility,
    ExecutionSourceSellWriteOffKind as Kind, ExecutionSourceSellWriteOffOutcome as Outcome,
    SqliteDiscoveryStore, EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
    EXECUTION_CANARY_POSITION_STATE_OPEN,
};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;

impl SqliteDiscoveryStore {
    /// Fresh authority, terminal mark and exact generation close share one transaction.
    pub fn write_off_execution_source_sell(
        &self,
        order_id: &str,
        kind: Kind,
        now: DateTime<Utc>,
    ) -> Result<Outcome> {
        self.with_immediate_transaction_retry("source SELL automatic write-off", |conn| {
            let Some(order) = self.load_execution_canary_order(order_id)? else {
                return Ok(Outcome::Refused("source_sell_write_off_order_missing"));
            };
            let Some(binding) = source_sell_promotion_guard::binding_for_signal(self, conn, &order.signal_id)? else {
                return Ok(Outcome::NotPromoted);
            };
            if let Some(reason) = source_sell_write_off_eligibility::refusal(&order, kind) {
                return Ok(Outcome::Refused(reason));
            }
            let receipt: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM execution_canary_receipt_proofs WHERE order_id=?1)
                OR EXISTS(SELECT 1 FROM execution_canary_receipt_facts WHERE order_id=?1)
                OR EXISTS(SELECT 1 FROM fills WHERE order_id=?1)", [order_id], |r|r.get(0))?;
            if receipt { return Ok(Outcome::Refused("source_sell_write_off_receipt_present")); }
            let signal = self.load_copy_signal_by_signal_id(&order.signal_id)?.context("source write-off signal missing")?;
            // No LIMIT 1 mint reader may select authority when several generations are OPEN.
            let mut stmt = conn.prepare("SELECT position_id FROM positions WHERE token=?1 AND accounting_bucket=?2 AND state=?3 LIMIT 2")?;
            let ids = stmt.query_map(params![signal.token, EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET, EXECUTION_CANARY_POSITION_STATE_OPEN], |r|r.get::<_,String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            if ids.len() > 1 { return Ok(Outcome::Refused("source_sell_write_off_ambiguous_position")); }
            if let Some(reason) = source_sell_promotion_guard::block_reason(self, conn, &signal, &binding)? {
                return Ok(Outcome::Refused(reason));
            }
            let staged = source_sell_intent_rows::load(conn, &binding.intent_id)?.context("source write-off staging missing")?;
            let position = load_position_by_id(conn, &staged.position_id)?.context("source write-off position missing")?;
            ensure!(ids.as_slice() == [position.position_id.clone()], "source write-off generation identity changed");
            if matches!(kind, Kind::DustNoRoute) && !position.qty_exact.is_some_and(|q|q.raw()==1 && q.decimals()>0) {
                return Ok(Outcome::Refused("source_sell_write_off_not_exact_dust"));
            }
            mark_terminal_sell_blocked_on_conn(conn, order_id, matches!(kind, Kind::TerminalSimulation { .. }), kind.reason())?;
            let close_result = write_off_exact_position_on_conn(conn, &position, now)?;
            let terminal = self.load_execution_canary_order(order_id)?.context("source write-off terminal order missing")?;
            let mut expected = order.clone();
            expected.err_code = Some(if matches!(kind, Kind::TerminalSimulation { .. }) {
                crate::EXECUTION_ERROR_TERMINAL_SELL_SIMULATION_FAILED
            } else { crate::EXECUTION_ERROR_TERMINAL_SELL_NO_ROUTE }.into());
            expected.simulation_error = Some(match order.simulation_error.as_deref() {
                Some(old) if !old.trim().is_empty() => format!("{}: {old}", kind.reason()),
                _ => kind.reason().into(),
            });
            ensure!(terminal == expected, "source write-off terminal mark changed unexpectedly");
            Ok(Outcome::WrittenOff {position_id: position.position_id, close_result, order: terminal})
        }).with_context(|| format!("automatic source SELL write-off order {order_id}"))
    }
}
