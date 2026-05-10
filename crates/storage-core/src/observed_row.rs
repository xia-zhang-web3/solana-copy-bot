use crate::observed_timestamp::parse_rfc3339_utc;
use anyhow::{anyhow, Context, Result};
use copybot_core_types::{ExactSwapAmounts, SwapEvent};
use rusqlite::Row;

pub(crate) fn row_to_swap_event(row: &Row<'_>) -> Result<SwapEvent> {
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

pub(crate) fn parse_sqlite_slot(raw: i64, field: &str) -> Result<u64> {
    if raw < 0 {
        return Err(anyhow!("{field} is negative: {raw}"));
    }
    Ok(raw as u64)
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
