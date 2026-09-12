use crate::{
    SellSettlementExpectedPosition, SellSettlementUnsupported as Unsupported,
    EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET, EXECUTION_CANARY_POSITION_STATE_OPEN,
};
use anyhow::{ensure, Context, Result};
use copybot_core_types::{Lamports, SignedLamports, TokenQuantity};
use rusqlite::{params, types::Value, Connection};

/// Deliberately does not use the legacy float/fallback position row decoder.
pub(crate) fn load(conn: &Connection, token: &str) -> Result<SellSettlementExpectedPosition> {
    let mut stmt = conn.prepare(
        "SELECT position_id, token, accounting_bucket, state, opened_ts, closed_ts,
         qty_raw, qty_decimals, cost_lamports, pnl_lamports FROM positions
         WHERE token = ?1 AND accounting_bucket = ?2 AND state = ?3 LIMIT 2",
    )?;
    let mut rows = stmt.query(params![
        token,
        EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,
        EXECUTION_CANARY_POSITION_STATE_OPEN
    ])?;
    let row = rows.next()?.ok_or(Unsupported::NoOwnedPosition)?;
    let values = (0..10)
        .map(|i| row.get::<_, Value>(i))
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(rows.next()?.is_none(), Unsupported::MultipleOwnedPositions);
    let text = |index: usize| -> Result<&str> {
        match &values[index] {
            Value::Text(value) => Ok(value),
            _ => anyhow::bail!("settlement position text domain invalid at {index}"),
        }
    };
    let raw = match &values[6] {
        Value::Null => return Err(Unsupported::MissingPositionQuantity.into()),
        Value::Text(value) => {
            ensure!(
                !value.is_empty()
                    && value.bytes().all(|c| c.is_ascii_digit())
                    && (value == "0" || !value.starts_with('0')),
                "settlement position raw integer not canonical"
            );
            // A canonical unsigned integer outside u64 is a domain rejection,
            // even when it is too large for a temporary u128 representation.
            value
                .parse::<u64>()
                .map_err(|_| Unsupported::PositionQuantityOutOfRange)?
        }
        _ => anyhow::bail!("settlement position raw text domain invalid"),
    };
    ensure!(raw > 0, Unsupported::PositionQuantityOutOfRange);
    let decimals = integer(&values[7], Unsupported::MissingPositionQuantity)?;
    let decimals = u8::try_from(decimals).context("settlement position decimals invalid")?;
    let cost = integer(&values[8], Unsupported::MissingEntryBasis)?;
    let cost = u64::try_from(cost).context("settlement position entry basis negative")?;
    let accumulated = integer(&values[9], Unsupported::MissingAccumulatedCashResult)?;
    ensure!(
        !text(0)?.trim().is_empty(),
        "settlement position identity empty"
    );
    let closed_ts = match &values[5] {
        Value::Null => None,
        _ => Some(text(5)?.to_owned()),
    };
    Ok(SellSettlementExpectedPosition {
        position_id: text(0)?.into(),
        token: text(1)?.into(),
        accounting_bucket: text(2)?.into(),
        state: text(3)?.into(),
        opened_ts: text(4)?.into(),
        closed_ts,
        quantity: TokenQuantity::new(raw, decimals),
        entry_basis: Lamports::new(cost),
        accumulated_cash_result: SignedLamports::new(i128::from(accumulated)),
    })
}

fn integer(value: &Value, missing: Unsupported) -> Result<i64> {
    match value {
        Value::Null => Err(missing.into()),
        Value::Integer(value) => Ok(*value),
        _ => anyhow::bail!("settlement position requires SQLite INTEGER, found {value:?}"),
    }
}
