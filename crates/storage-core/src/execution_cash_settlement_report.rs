use crate::{
    execution_cash_settlement::validate_existing, receipt_facts_identity::validate_identity,
    receipt_facts_rows, ExecutionCashSettlementReport, ExecutionCashSettlementReportRow,
    SqliteDiscoveryStore,
};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};

impl SqliteDiscoveryStore {
    pub fn execution_cash_settlement_report(
        &self,
        since: DateTime<Utc>,
        as_of: DateTime<Utc>,
        limit: u32,
    ) -> Result<ExecutionCashSettlementReport> {
        let tx = self.conn.unchecked_transaction()?;
        let report = on_conn(&tx, since, as_of, limit)?;
        tx.commit()?;
        Ok(report)
    }
}

/// Order-driven, not shadow-driven: includes owned-only SELL and all unsupported rows.
/// No SQL SUM over INTEGER: accumulation is checked i128 over the full window.
pub(crate) fn on_conn(
    conn: &Connection,
    since: DateTime<Utc>,
    as_of: DateTime<Utc>,
    limit: u32,
) -> Result<ExecutionCashSettlementReport> {
    let mut statement = conn.prepare(
        "SELECT o.order_id,COALESCE(s.token,p.token,f.token),o.status,f.accounting_basis
        FROM orders o LEFT JOIN copy_signals s ON s.signal_id=o.signal_id
        LEFT JOIN execution_canary_receipt_proofs p ON p.order_id=o.order_id
        LEFT JOIN fills f ON f.order_id=o.order_id
        WHERE o.order_id LIKE 'exec-canary:%'
          AND (lower(s.side)='sell' OR lower(p.side)='sell' OR f.accounting_basis='receipt_native_cash')
          AND COALESCE(o.confirm_ts,o.submit_ts)>=?1 AND COALESCE(o.confirm_ts,o.submit_ts)<=?2
        ORDER BY COALESCE(o.confirm_ts,o.submit_ts) DESC,o.order_id",
    )?;
    let rows = statement.query_map(params![since.to_rfc3339(), as_of.to_rfc3339()], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, Option<String>>(3)?,
        ))
    })?;
    let mut report = ExecutionCashSettlementReport::default();
    let (mut native_sum, mut result_sum) = (0_i128, 0_i128);
    for row in rows {
        let (order_id, token, status, basis) = row?;
        let facts = receipt_facts_rows::load(conn, &order_id)?;
        let mut sample = ExecutionCashSettlementReportRow {
            order_id: order_id.clone(),
            token,
            status,
            accounting_basis: basis.clone().unwrap_or("unsettled".into()),
            position_id: None,
            sold_raw: None,
            decimals: None,
            wallet_native_cash_delta_lamports: None,
            allocated_entry_basis_lamports: None,
            cash_result_delta_lamports: None,
            swap_price_sol: None,
            economic_pnl_sol: None,
            transaction_fee_lamports: facts
                .as_ref()
                .and_then(|f| f.transaction_fee.map(|v| v.as_u64().to_string())),
            fee_coverage: facts
                .as_ref()
                .map(|f| f.fee_coverage.as_str())
                .unwrap_or("missing_facts")
                .into(),
            fee_payer: facts.as_ref().and_then(|f| f.fee_payer.clone()),
            decomposition: "unresolved".into(),
        };
        report.total_sell_orders += 1;
        match basis.as_deref() {
            Some("receipt_native_cash") => {
                let facts = facts.context("settled order missing receipt facts")?;
                validate_identity(conn, &facts)?;
                let cash = validate_existing(conn, &order_id)?;
                ensure!(
                    cash.token == sample.token,
                    "settlement report token mismatch"
                );
                native_sum = native_sum
                    .checked_add(cash.wallet_native_cash_delta.as_i128())
                    .context("cash report native sum overflow")?;
                result_sum = result_sum
                    .checked_add(cash.cash_result_delta.as_i128())
                    .context("cash report result sum overflow")?;
                report.settled_orders += 1;
                sample.position_id = Some(cash.position_id);
                sample.sold_raw = Some(cash.sold_quantity.raw().to_string());
                sample.decimals = Some(cash.sold_quantity.decimals());
                sample.wallet_native_cash_delta_lamports =
                    Some(cash.wallet_native_cash_delta.as_i128().to_string());
                sample.allocated_entry_basis_lamports =
                    Some(cash.allocated_entry_basis.as_u64().to_string());
                sample.cash_result_delta_lamports =
                    Some(cash.cash_result_delta.as_i128().to_string());
            }
            Some("legacy_unclassified") => report.legacy_fill_orders += 1,
            None => report.unsettled_orders += 1,
            Some(_) => anyhow::bail!("unsupported stored fill basis"),
        }
        if report.rows.len() < limit as usize {
            report.rows.push(sample);
        }
    }
    report.rows_truncated = (report.rows.len() as u64) < report.total_sell_orders;
    if report.settled_orders > 0 {
        report.known_wallet_native_delta_lamports = Some(native_sum.to_string());
        report.known_cash_result_delta_lamports = Some(result_sum.to_string());
        if report.settled_orders == report.total_sell_orders {
            report.cohort_cash_result_delta_lamports = Some(result_sum.to_string());
        }
    }
    Ok(report)
}
