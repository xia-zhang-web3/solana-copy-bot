use crate::{DiscoveryRuntimeCursor, ObservedSwapCursorPage, SqliteDiscoveryStore};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::{ExactSwapAmounts, SwapEvent};
use rusqlite::{params, params_from_iter, types::Value as SqlValue, OptionalExtension, Row};
use std::time::Instant;

impl SqliteDiscoveryStore {
    pub fn observed_swaps_tail_cursor_read_only(&self) -> Result<Option<DiscoveryRuntimeCursor>> {
        if !self.sqlite_table_exists("observed_swaps")? {
            return Ok(None);
        }
        let raw = self
            .conn
            .query_row(
                "SELECT ts, slot, signature
                 FROM observed_swaps
                 ORDER BY ts DESC, slot DESC, signature DESC
                 LIMIT 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .context("failed loading observed_swaps tail cursor")?;
        raw.map(|(ts, slot, signature)| {
            Ok(DiscoveryRuntimeCursor {
                ts_utc: parse_rfc3339_utc(&ts, "observed_swaps.ts")?,
                slot: slot.max(0) as u64,
                signature,
            })
        })
        .transpose()
    }

    pub fn load_recent_observed_swaps_since(
        &self,
        since: DateTime<Utc>,
        limit: usize,
    ) -> Result<(Vec<SwapEvent>, bool)> {
        if limit == 0 || !self.sqlite_table_exists("observed_swaps")? {
            return Ok((Vec::new(), false));
        }
        let retained_limit = limit.min(i64::MAX as usize);
        let query_limit = retained_limit.saturating_add(1).min(i64::MAX as usize) as i64;
        let mut stmt = self.conn.prepare(
            "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                    qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
             FROM observed_swaps
             WHERE ts >= ?1
             ORDER BY ts DESC, slot DESC, signature DESC
             LIMIT ?2",
        )?;
        let mut rows = stmt.query(params![since.to_rfc3339(), query_limit])?;
        let mut swaps = Vec::new();
        while let Some(row) = rows.next()? {
            swaps.push(row_to_swap_event(row)?);
        }
        let truncated = swaps.len() > retained_limit;
        if truncated {
            swaps.truncate(retained_limit);
        }
        swaps.reverse();
        Ok((swaps, truncated))
    }

    pub fn for_each_observed_swap_in_window_after_cursor_with_budget<F>(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        cursor: Option<&DiscoveryRuntimeCursor>,
        limit: usize,
        deadline: Instant,
        mut on_swap: F,
    ) -> Result<ObservedSwapCursorPage>
    where
        F: FnMut(SwapEvent) -> Result<()>,
    {
        if limit == 0
            || Instant::now() >= deadline
            || !self.sqlite_table_exists("observed_swaps")?
        {
            return Ok(ObservedSwapCursorPage {
                rows_seen: 0,
                time_budget_exhausted: Instant::now() >= deadline,
            });
        }
        let limit = limit.min(i64::MAX as usize) as i64;
        let (query, values): (&str, Vec<SqlValue>) = match cursor {
            Some(cursor) => (
                "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                        qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
                 FROM observed_swaps
                 WHERE ts >= ?1 AND ts <= ?2 AND (ts, slot, signature) > (?3, ?4, ?5)
                 ORDER BY ts ASC, slot ASC, signature ASC
                 LIMIT ?6",
                vec![
                    since.to_rfc3339().into(),
                    until.to_rfc3339().into(),
                    cursor.ts_utc.to_rfc3339().into(),
                    (cursor.slot as i64).into(),
                    cursor.signature.clone().into(),
                    limit.into(),
                ],
            ),
            None => (
                "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                        qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
                 FROM observed_swaps
                 WHERE ts >= ?1 AND ts <= ?2
                 ORDER BY ts ASC, slot ASC, signature ASC
                 LIMIT ?3",
                vec![
                    since.to_rfc3339().into(),
                    until.to_rfc3339().into(),
                    limit.into(),
                ],
            ),
        };
        let mut stmt = self.conn.prepare(query)?;
        let mut rows = stmt.query(params_from_iter(values))?;
        let mut seen = 0usize;
        while let Some(row) = rows.next()? {
            if Instant::now() >= deadline {
                return Ok(ObservedSwapCursorPage {
                    rows_seen: seen,
                    time_budget_exhausted: true,
                });
            }
            on_swap(row_to_swap_event(row)?)?;
            seen = seen.saturating_add(1);
        }
        Ok(ObservedSwapCursorPage {
            rows_seen: seen,
            time_budget_exhausted: false,
        })
    }

    pub fn insert_observed_swap(&self, swap: &SwapEvent) -> Result<bool> {
        let in_raw = swap
            .exact_amounts
            .as_ref()
            .map(|value| value.amount_in_raw.as_str());
        let in_decimals = swap
            .exact_amounts
            .as_ref()
            .map(|value| i64::from(value.amount_in_decimals));
        let out_raw = swap
            .exact_amounts
            .as_ref()
            .map(|value| value.amount_out_raw.as_str());
        let out_decimals = swap
            .exact_amounts
            .as_ref()
            .map(|value| i64::from(value.amount_out_decimals));
        let slot = swap.slot as i64;
        let ts = swap.ts_utc.to_rfc3339();
        let changed = self.conn.execute(
            "INSERT OR IGNORE INTO observed_swaps(
                signature, wallet_id, dex, token_in, token_out, qty_in, qty_out,
                qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals, slot, ts
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                &swap.signature,
                &swap.wallet,
                &swap.dex,
                &swap.token_in,
                &swap.token_out,
                swap.amount_in,
                swap.amount_out,
                in_raw,
                in_decimals,
                out_raw,
                out_decimals,
                slot,
                ts,
            ],
        )?;
        Ok(changed > 0)
    }

    pub fn insert_observed_swaps_batch(&self, swaps: &[SwapEvent]) -> Result<Vec<bool>> {
        swaps
            .iter()
            .map(|swap| self.insert_observed_swap(swap))
            .collect()
    }
}

fn row_to_swap_event(row: &Row<'_>) -> Result<SwapEvent> {
    let ts_raw: String = row.get(8)?;
    let slot_raw: i64 = row.get(7)?;
    Ok(SwapEvent {
        signature: row.get(0)?,
        wallet: row.get(1)?,
        dex: row.get(2)?,
        token_in: row.get(3)?,
        token_out: row.get(4)?,
        amount_in: row.get(5)?,
        amount_out: row.get(6)?,
        slot: slot_raw.max(0) as u64,
        ts_utc: parse_rfc3339_utc(&ts_raw, "observed_swaps.ts")?,
        exact_amounts: read_exact_swap_amounts(row)?,
    })
}

fn read_exact_swap_amounts(row: &Row<'_>) -> Result<Option<ExactSwapAmounts>> {
    let amount_in_raw: Option<String> = row.get(9)?;
    let amount_in_decimals_raw: Option<i64> = row.get(10)?;
    let amount_out_raw: Option<String> = row.get(11)?;
    let amount_out_decimals_raw: Option<i64> = row.get(12)?;
    match (
        amount_in_raw,
        amount_in_decimals_raw,
        amount_out_raw,
        amount_out_decimals_raw,
    ) {
        (Some(amount_in_raw), Some(in_dec), Some(amount_out_raw), Some(out_dec)) => {
            Ok(Some(ExactSwapAmounts {
                amount_in_raw,
                amount_in_decimals: u8::try_from(in_dec)
                    .with_context(|| format!("invalid qty_in_decimals: {in_dec}"))?,
                amount_out_raw,
                amount_out_decimals: u8::try_from(out_dec)
                    .with_context(|| format!("invalid qty_out_decimals: {out_dec}"))?,
            }))
        }
        (None, None, None, None) => Ok(None),
        _ => Err(anyhow!(
            "observed swap exact amount columns are partially populated"
        )),
    }
}

pub(crate) fn parse_rfc3339_utc(raw: &str, field: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc))
        .with_context(|| format!("invalid {field} rfc3339 value: {raw}"))
}
