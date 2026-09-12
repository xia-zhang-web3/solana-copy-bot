use super::ClosedEntryLoss;
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};

pub(super) fn read(conn: &Connection, since: DateTime<Utc>) -> Result<ClosedEntryLoss> {
    let mut value = ClosedEntryLoss {
        loss_lamports: "0".into(),
        basis: "sum_negative_per_closed_position_pnl_lamports_else_SQLite_ROUND_COALESCE_pnl_SOL_times_1e9_legacy_bounded_2pow53".into(),
        window_basis: "closed_ts_gte_UTC_day_start_no_upper_cutoff_excludes_recovery_orphan_positions".into(),
        positions: 0, lamport_backed_positions: 0, legacy_f64_positions: 0,
        legacy_null_positions: 0, exact: true,
    };
    let mut stmt = conn.prepare(
        "SELECT pnl_lamports,CASE WHEN pnl_lamports IS NULL THEN pnl_sol END,
         CASE WHEN pnl_lamports IS NULL THEN ROUND(COALESCE(pnl_sol,0.0)*1000000000.0) ELSE 0.0 END
         FROM positions WHERE accounting_bucket=?1 AND state=?2 AND closed_ts>=?3
         AND position_id NOT LIKE 'exec-canary-pos:recovery-orphan:%'",
    )?;
    let rows = stmt.query_map(
        params![
            crate::EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
            crate::EXECUTION_CANARY_POSITION_STATE_CLOSED,
            since.to_rfc3339()
        ],
        |r| {
            Ok((
                r.get::<_, Option<i64>>(0)?,
                r.get::<_, Option<f64>>(1)?,
                r.get::<_, f64>(2)?,
            ))
        },
    )?;
    let mut loss = 0_u128;
    for row in rows {
        let (lamports, legacy, rounded) = row?;
        value.positions += 1;
        if lamports.is_some() {
            value.lamport_backed_positions += 1;
        } else {
            value.exact = false;
            if legacy.is_some() {
                value.legacy_f64_positions += 1;
            } else {
                value.legacy_null_positions += 1;
            }
        }
        let amount = loss_amount(lamports, rounded)?;
        loss = loss
            .checked_add(amount)
            .context("closed entry loss overflow")?;
    }
    value.loss_lamports = loss.to_string();
    Ok(value)
}

// Shared by the original CLOSED subtotal and each persisted position's overlap.
// SQLite still supplies the original ROUND/COALESCE result, with unchanged bounds.
pub(super) fn loss_amount(lamports: Option<i64>, rounded: f64) -> Result<u128> {
    if let Some(raw) = lamports {
        return Ok(if raw < 0 {
            (-i128::from(raw)) as u128
        } else {
            0
        });
    }
    ensure!(rounded.is_finite(), "invalid legacy closed result");
    if rounded < 0.0 {
        ensure!(
            -rounded <= 9_007_199_254_740_992.0,
            "legacy closed loss outside exact integer conversion bound"
        );
        Ok((-rounded) as u128)
    } else {
        Ok(0)
    }
}
