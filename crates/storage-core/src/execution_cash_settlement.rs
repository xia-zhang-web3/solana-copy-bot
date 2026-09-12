use crate::{
    execution_canary_fill_marker::fill_exists,
    execution_canary_receipt::complete_receipt_accounting, execution_canary_receipt_facts::merge,
    execution_canary_sell_settlement::plan, execution_cash_settlement_rows,
    receipt_facts_identity::validate_identity, receipt_facts_rows, ExecutionCanaryCashSettlement,
    ExecutionCanaryCashSettlementResult, ExecutionCanaryReceiptFacts,
    SellSettlementUnsupported as Unsupported, SettlementSqliteSignedValue, SqliteDiscoveryStore,
    EXECUTION_STATUS_CANARY_CONFIRMED,
};
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};

impl SqliteDiscoveryStore {
    /// Fresh operands are mandatory even on replay. Facts must already be durable.
    /// Revalidation, replan, inventory, evidence/fill and completion share one write tx.
    pub fn apply_execution_canary_sell_settlement(
        &self,
        fresh: &ExecutionCanaryReceiptFacts,
        accounted_at: DateTime<Utc>,
    ) -> Result<ExecutionCanaryCashSettlementResult> {
        self.with_immediate_transaction_retry("receipt native SELL settlement", |conn| {
            apply_on_conn(conn, fresh, accounted_at)
        })
    }

    pub fn load_execution_canary_cash_settlement(
        &self,
        id: &str,
    ) -> Result<Option<ExecutionCanaryCashSettlement>> {
        execution_cash_settlement_rows::load(&self.conn, id)
    }

    /// Validate identity before an app replay fast path, without another receipt RPC.
    pub fn validate_execution_canary_cash_settlement_replay(
        &self,
        id: &str,
        wallet: &str,
    ) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        if execution_cash_settlement_rows::load(&tx, id)?.is_some() {
            let facts =
                receipt_facts_rows::load(&tx, id)?.ok_or(Unsupported::MissingReceiptFacts)?;
            ensure!(
                facts.wallet_pubkey == wallet,
                "cash settlement replay wallet mismatch"
            );
            validate_fresh(&tx, &facts)?;
            validate_existing(&tx, id)?;
        }
        tx.commit()?;
        Ok(())
    }
}

fn apply_on_conn(
    conn: &Connection,
    fresh: &ExecutionCanaryReceiptFacts,
    accounted_at: DateTime<Utc>,
) -> Result<ExecutionCanaryCashSettlementResult> {
    validate_fresh(conn, fresh)?;
    if fill_exists(conn, &fresh.order_id)? {
        return Ok(ExecutionCanaryCashSettlementResult {
            already_accounted: true,
            settlement: validate_existing(conn, &fresh.order_id)?,
        });
    }
    crate::sell_receipt_ownership::validate(conn, &fresh.order_id)?;
    let p = plan(conn, &fresh.order_id)?;
    let native = sql_signed(p.native_delta_sqlite)?;
    let delta = sql_signed(p.cash_result_delta_sqlite)?;
    let accumulated = sql_signed(p.accumulated_cash_result_sqlite)?;
    let position = &p.expected_position;
    let cost = i64::try_from(p.remaining_entry_basis.as_u64())?;
    let closed_ts = (p.remaining_quantity.raw() == 0).then(|| accounted_at.to_rfc3339());
    let changed = conn.execute(
        "UPDATE positions SET qty=?2,qty_raw=?3,qty_decimals=?4,
         cost_sol=?5,cost_lamports=?6,pnl_sol=NULL,pnl_lamports=?7,state=?8,closed_ts=?9
         WHERE position_id=?1 AND token=?10 AND accounting_bucket=?11 AND state=?12
           AND qty_raw=?13 AND qty_decimals=?4 AND cost_lamports=?14 AND pnl_lamports=?15",
        params![
            position.position_id,
            p.remaining_quantity.as_f64(),
            p.remaining_quantity.raw().to_string(),
            p.remaining_quantity.decimals(),
            cost as f64 / 1e9,
            cost,
            accumulated,
            p.remaining_position_state,
            closed_ts,
            position.token,
            position.accounting_bucket,
            position.state,
            position.quantity.raw().to_string(),
            i64::try_from(position.entry_basis.as_u64())?,
            i64::try_from(position.accumulated_cash_result.as_i128())?
        ],
    )?;
    ensure!(changed == 1, "cash settlement expected position changed");
    conn.execute(
        "INSERT INTO fills(order_id,token,qty,avg_price,fee,slippage_bps,
         notional_lamports,fee_lamports,qty_raw,qty_decimals,accounting_basis,position_id,
         wallet_native_delta_lamports,entry_basis_lamports,cash_result_delta_lamports,
         accumulated_cash_result_lamports,remaining_qty_raw,remaining_cost_lamports,settlement_ts)
         VALUES(?1,?2,?3,NULL,NULL,NULL,NULL,NULL,?4,?5,'receipt_native_cash',?6,?7,?8,?9,?10,?11,?12,?13)",
        params![
            fresh.order_id, fresh.token, p.sold_quantity.as_f64(),
            p.sold_quantity.raw().to_string(), p.sold_quantity.decimals(), position.position_id,
            native, i64::try_from(p.allocated_entry_basis.as_u64())?, delta, accumulated,
            p.remaining_quantity.raw().to_string(), cost, accounted_at.to_rfc3339()
        ],
    )?;
    complete_receipt_accounting(conn, &fresh.order_id)?;
    Ok(ExecutionCanaryCashSettlementResult {
        already_accounted: false,
        settlement: validate_existing(conn, &fresh.order_id)?,
    })
}

fn validate_fresh(conn: &Connection, fresh: &ExecutionCanaryReceiptFacts) -> Result<()> {
    fresh.validate()?;
    validate_identity(conn, fresh)?;
    ensure!(fresh.side == "sell", Unsupported::NotSell);
    let token = fresh
        .token_delta
        .ok_or(Unsupported::UnresolvedTokenCoverage)?;
    ensure!(token.raw < 0, Unsupported::NonNegativeTokenDelta);
    ensure!(
        token
            .raw
            .checked_neg()
            .and_then(|v| u64::try_from(v).ok())
            .is_some(),
        Unsupported::TokenQuantityOutOfRange
    );
    let durable =
        receipt_facts_rows::load(conn, &fresh.order_id)?.ok_or(Unsupported::MissingReceiptFacts)?;
    ensure!(
        merge(&durable, fresh)? == durable,
        "fresh receipt facts must be durable before settlement"
    );
    Ok(())
}

pub(crate) fn validate_existing(
    conn: &Connection,
    id: &str,
) -> Result<ExecutionCanaryCashSettlement> {
    let settlement =
        execution_cash_settlement_rows::load(conn, id)?.ok_or(Unsupported::AlreadyAccounted)?;
    let (status, reason): (String, String) = conn.query_row(
        "SELECT o.status,p.reason FROM orders o
        JOIN execution_canary_receipt_proofs p ON p.order_id=o.order_id WHERE o.order_id=?1",
        [id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    ensure!(
        status == EXECUTION_STATUS_CANARY_CONFIRMED && reason == "accounting_complete",
        "cash settlement completion incomplete"
    );
    let facts = receipt_facts_rows::load(conn, id)?.ok_or(Unsupported::MissingReceiptFacts)?;
    ensure!(
        facts.wallet_native_delta == settlement.wallet_native_cash_delta
            && facts
                .token_delta
                .is_some_and(|q| q.raw == -i128::from(settlement.sold_quantity.raw())
                    && q.decimals == settlement.sold_quantity.decimals())
            && facts.token == settlement.token,
        "cash settlement evidence mismatch"
    );
    Ok(settlement)
}

fn sql_signed(value: SettlementSqliteSignedValue) -> Result<i64> {
    match value {
        SettlementSqliteSignedValue::Fits(v) => Ok(v),
        SettlementSqliteSignedValue::OutOfRange => Err(Unsupported::SqliteIntegerOutOfRange.into()),
    }
}
