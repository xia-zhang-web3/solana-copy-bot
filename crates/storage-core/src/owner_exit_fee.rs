//! Fee obligation for a single signed owner exit. UNKNOWN keeps its full bound.
use crate::{ExecutionCanaryDispatch, OwnerExitIntent, TinyBudgetClaim};
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};

pub(crate) fn claim(
    conn: &Connection,
    i: &OwnerExitIntent,
    d: &ExecutionCanaryDispatch,
    b: &TinyBudgetClaim,
) -> Result<()> {
    let committed: u64 = conn.query_row(
        "SELECT (SELECT COALESCE(SUM(COALESCE(actual_fee,fee_bound)),0)
                 FROM execution_tiny_reservations)
              +(SELECT COALESCE(SUM(COALESCE(actual_fee,fee_bound)),0)
                 FROM owner_exit_fee_reservations)",
        [],
        |r| r.get(0),
    )?;
    ensure!(
        committed
            .checked_add(b.total_fee)
            .is_some_and(|v| v <= crate::tiny_experiment::TINY_TOTAL_FEE),
        "owner_exit_cumulative_fee_exhausted"
    );
    conn.execute(
        "INSERT INTO owner_exit_fee_reservations(
        order_id,run_id,tx_signature,wallet,fee_bound,priority_fee,fee_slot,state)
        VALUES(?1,?2,?3,?4,?5,?6,?7,'pending')",
        params![
            d.order_id,
            i.run_id,
            d.tx_signature,
            i.wallet,
            i64::try_from(b.total_fee)?,
            i64::try_from(b.priority_fee)?,
            b.fee_slot.to_string()
        ],
    )?;
    existing(conn, i, d, b)
}

pub(crate) fn existing(
    conn: &Connection,
    i: &OwnerExitIntent,
    d: &ExecutionCanaryDispatch,
    b: &TinyBudgetClaim,
) -> Result<()> {
    let same: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM owner_exit_fee_reservations
        WHERE order_id=?1 AND run_id=?2 AND tx_signature=?3 AND wallet=?4
          AND fee_bound=?5 AND priority_fee=?6 AND fee_slot=?7)",
        params![
            d.order_id,
            i.run_id,
            d.tx_signature,
            i.wallet,
            i64::try_from(b.total_fee)?,
            i64::try_from(b.priority_fee)?,
            b.fee_slot.to_string()
        ],
        |r| r.get(0),
    )?;
    ensure!(same, "owner_exit_fee_reservation_changed");
    Ok(())
}

pub(crate) fn settle(
    conn: &Connection,
    order: &str,
    signature: &str,
    wallet: &str,
    payer: Option<&str>,
    fee: Option<u64>,
    outcome: &str,
    now: DateTime<Utc>,
) -> Result<()> {
    let old: Option<(String, String, Option<u64>, String)> = conn
        .query_row(
            "SELECT tx_signature,wallet,actual_fee,state
         FROM owner_exit_fee_reservations WHERE order_id=?1",
            [order],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()?;
    let Some((bound_signature, bound_wallet, actual, state)) = old else {
        return Ok(());
    };
    ensure!(
        signature == bound_signature && wallet == bound_wallet,
        "owner_exit_fee_receipt_identity"
    );
    ensure!(
        matches!(outcome, "successful" | "failed"),
        "owner_exit_fee_outcome"
    );
    ensure!(
        payer.is_none_or(|p| p == wallet),
        "owner_exit_fee_payer_conflict"
    );
    let Some(fee) = fee.filter(|_| payer == Some(wallet)) else {
        return Ok(());
    };
    if let Some(actual) = actual {
        ensure!(
            actual == fee && state == outcome,
            "owner_exit_fee_receipt_conflict"
        );
        return Ok(());
    }
    conn.execute(
        "UPDATE owner_exit_fee_reservations
        SET actual_fee=?2,state=?3,reconciled_at=?4
        WHERE order_id=?1 AND state='pending' AND actual_fee IS NULL",
        params![order, i64::try_from(fee)?, outcome, now.to_rfc3339()],
    )?;
    let saved: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM owner_exit_fee_reservations
        WHERE order_id=?1 AND tx_signature=?2 AND wallet=?3 AND actual_fee=?4 AND state=?5)",
        params![order, signature, wallet, i64::try_from(fee)?, outcome],
        |r| r.get(0),
    )?;
    ensure!(saved, "owner_exit_fee_settlement_missing");
    // Even a fee above the signed bound stays recorded as the observed fact.
    Ok(())
}
