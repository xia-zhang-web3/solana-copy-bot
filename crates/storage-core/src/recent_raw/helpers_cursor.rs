use crate::DiscoveryRuntimeCursor;
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::{ExactSwapAmounts, SwapEvent};
use rusqlite::Row;
use std::{cmp::Ordering, time::Instant};

pub(super) fn row_to_swap_event(row: &Row<'_>) -> Result<SwapEvent> {
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
        slot: parse_sqlite_slot(slot_raw, "observed_swaps.slot")?,
        ts_utc: parse_rfc3339_utc(&ts_raw, "observed_swaps.ts")?,
        exact_amounts: read_exact_swap_amounts(row)?,
    })
}

pub(super) fn read_exact_swap_amounts(row: &Row<'_>) -> Result<Option<ExactSwapAmounts>> {
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
        _ => bail!("observed swap exact amount columns are partially populated"),
    }
}

pub(super) fn parse_cursor(
    ts_raw: Option<String>,
    slot_raw: Option<i64>,
    signature: Option<String>,
) -> Result<Option<DiscoveryRuntimeCursor>> {
    match (ts_raw, slot_raw, signature) {
        (Some(ts_raw), Some(slot_raw), Some(signature)) => Ok(Some(DiscoveryRuntimeCursor {
            ts_utc: parse_rfc3339_utc(&ts_raw, "recent_raw_journal_state.cursor_ts")?,
            slot: parse_sqlite_slot(
                slot_raw,
                "recent_raw_journal_state.covered_through_cursor_slot",
            )?,
            signature,
        })),
        (None, None, None) => Ok(None),
        _ => bail!("recent_raw_journal_state cursor columns are partially populated"),
    }
}

pub(super) fn parse_optional_rfc3339_utc(
    raw: Option<String>,
    field_name: &str,
) -> Result<Option<DateTime<Utc>>> {
    raw.map(|raw| parse_rfc3339_utc(&raw, field_name))
        .transpose()
}

pub(super) fn parse_rfc3339_utc(raw: &str, field_name: &str) -> Result<DateTime<Utc>> {
    if !raw.ends_with("+00:00") {
        bail!("{field_name} timestamp must use canonical UTC offset +00:00: {raw}");
    }
    let parsed = DateTime::parse_from_rfc3339(raw)
        .with_context(|| format!("invalid {field_name} timestamp value: {raw}"))?;
    if parsed.offset().local_minus_utc() != 0 {
        bail!("{field_name} timestamp must use UTC offset +00:00: {raw}");
    }
    Ok(parsed.with_timezone(&Utc))
}

pub(super) fn parse_sqlite_slot(raw: i64, field_name: &str) -> Result<u64> {
    if raw < 0 {
        bail!("{field_name} is negative: {raw}");
    }
    Ok(raw as u64)
}

pub(super) fn cursor_cmp(
    left: &DiscoveryRuntimeCursor,
    right: &DiscoveryRuntimeCursor,
) -> Ordering {
    left.ts_utc
        .cmp(&right.ts_utc)
        .then_with(|| left.slot.cmp(&right.slot))
        .then_with(|| left.signature.cmp(&right.signature))
}

pub(super) fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u64::MAX as u128) as u64
}

pub(super) fn far_deadline() -> Instant {
    Instant::now() + std::time::Duration::from_secs(24 * 60 * 60)
}
