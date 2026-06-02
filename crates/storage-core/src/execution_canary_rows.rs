use crate::ExecutionCanaryOrder;
use chrono::{DateTime, Utc};

pub(crate) fn execution_canary_order_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ExecutionCanaryOrder> {
    let submit_ts_raw: String = row.get(3)?;
    let confirm_ts_raw: Option<String> = row.get(4)?;
    let submit_ts = parse_ts(3, &submit_ts_raw)?;
    let confirm_ts = confirm_ts_raw
        .as_deref()
        .map(|raw| parse_ts(4, raw))
        .transpose()?;
    let attempt_raw: i64 = row.get(11)?;
    let attempt = u32::try_from(attempt_raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            11,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })?;
    Ok(ExecutionCanaryOrder {
        order_id: row.get(0)?,
        signal_id: row.get(1)?,
        route: row.get(2)?,
        submit_ts,
        confirm_ts,
        status: row.get(5)?,
        err_code: row.get(6)?,
        client_order_id: row.get(7)?,
        tx_signature: row.get(8)?,
        simulation_status: row.get(9)?,
        simulation_error: row.get(10)?,
        attempt,
    })
}

fn parse_ts(column: usize, raw: &str) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                column,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })
}
