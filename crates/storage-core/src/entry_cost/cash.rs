use super::CashEntryLoss;
use crate::{sell_cash_day, ExecutionCanaryCashSettlement};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};

// A cash-domain failure is a BUY-only unavailable component, never a known zero.
// Keep the existing fatal SQLite classification and error chain intact.
pub(super) fn read(
    conn: &Connection,
    since: DateTime<Utc>,
    as_of: DateTime<Utc>,
) -> Result<CashEntryLoss> {
    match validated(conn, since, as_of) {
        Ok(value) => Ok(value),
        Err(error) if crate::is_fatal_sqlite_anyhow_error(&error) => Err(error),
        Err(_) => Ok(CashEntryLoss {
            additional_loss_lamports: None,
            day_gross_negative_lamports: None,
            validated_day_events: None,
            undated_obligations: None,
            coverage: "unavailable".into(),
            unavailable_reason: Some("cash_loss_unavailable".into()),
            window_basis: WINDOW.into(),
        }),
    }
}

const WINDOW: &str = "parsed_settlement_ts_[UTC_day_start,fresh_as_of)_overlap_by_persisted_position_id_with_original_CLOSED_floor";

struct Position {
    id: String,
    token: String,
    closed: u128,
    gross: u128,
}

fn validated(
    conn: &Connection,
    since: DateTime<Utc>,
    as_of: DateTime<Utc>,
) -> Result<CashEntryLoss> {
    // Each group does one primary-key lookup. The CLOSED eligibility predicate and
    // scalar decoder retain the old lexical cutoff, no upper bound, and orphan rule.
    let mut lookup = conn.prepare(
        "SELECT token,accounting_bucket,
         CASE WHEN eligible THEN pnl_lamports ELSE 0 END,
         CASE WHEN eligible AND pnl_lamports IS NULL THEN ROUND(COALESCE(pnl_sol,0.0)*1000000000.0) ELSE 0.0 END
         FROM (SELECT *,state=?2 AND closed_ts>=?3
               AND position_id NOT LIKE 'exec-canary-pos:recovery-orphan:%' AS eligible
               FROM positions WHERE position_id=?1)",
    )?;
    let mut current: Option<Position> = None;
    let (mut increment, mut gross, mut events) = (0_u128, 0_u128, 0_u64);
    sell_cash_day::visit_events(
        conn,
        since,
        as_of,
        |cash: &ExecutionCanaryCashSettlement, selected| {
            if current.as_ref().is_none_or(|p| p.id != cash.position_id) {
                if let Some(p) = current.take() {
                    finish(p, &mut increment)?;
                }
                let (token, bucket, raw, rounded): (String, String, Option<i64>, f64) = lookup
                    .query_row(
                        params![
                            cash.position_id,
                            crate::EXECUTION_CANARY_POSITION_STATE_CLOSED,
                            since.to_rfc3339()
                        ],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                    )
                    .optional()?
                    .context("cash position attribution missing")?;
                ensure!(
                    bucket == crate::EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
                    "cash position attribution bucket mismatch"
                );
                current = Some(Position {
                    id: cash.position_id.clone(),
                    token,
                    closed: super::closed::loss_amount(raw, rounded)?,
                    gross: 0,
                });
            }
            let p = current.as_mut().expect("position loaded");
            ensure!(
                p.token == cash.token,
                "cash position attribution token mismatch"
            );
            if selected {
                events = events
                    .checked_add(1)
                    .context("cash entry event count overflow")?;
                let delta = cash.cash_result_delta.as_i128();
                if delta < 0 {
                    let loss = delta.unsigned_abs();
                    p.gross = p
                        .gross
                        .checked_add(loss)
                        .context("position gross cash loss overflow")?;
                    gross = gross
                        .checked_add(loss)
                        .context("gross cash entry loss overflow")?;
                }
            }
            Ok(())
        },
    )?;
    if let Some(p) = current {
        finish(p, &mut increment)?;
    }
    Ok(CashEntryLoss {
        additional_loss_lamports: Some(increment.to_string()),
        day_gross_negative_lamports: Some(gross.to_string()),
        validated_day_events: Some(events),
        undated_obligations: Some(sell_cash_day::obligations(conn)?),
        coverage: "validated_dated_cash_claims_only_history_unproven".into(),
        unavailable_reason: None,
        window_basis: WINDOW.into(),
    })
}

fn finish(position: Position, total: &mut u128) -> Result<()> {
    *total = total
        .checked_add(position.gross.saturating_sub(position.closed))
        .context("additional cash entry loss overflow")?;
    Ok(())
}
