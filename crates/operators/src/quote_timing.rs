use chrono::{DateTime, Utc};
use rusqlite::{types::Type, Row};

pub(crate) fn read_start(row: &Row<'_>, index: usize) -> rusqlite::Result<Option<DateTime<Utc>>> {
    row.get::<_, Option<String>>(index)?
        .map(|raw| {
            DateTime::parse_from_rfc3339(&raw)
                .map(|ts| ts.with_timezone(&Utc))
                .map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(error))
                })
        })
        .transpose()
}

pub(crate) fn read_delay(
    row: &Row<'_>,
    source: usize,
    actual: usize,
) -> rusqlite::Result<Option<u64>> {
    Ok(read_start(row, source)?
        .zip(read_start(row, actual)?)
        .and_then(|(source, actual)| {
            if actual < source {
                None
            } else {
                u64::try_from((actual - source).num_milliseconds()).ok()
            }
        }))
}
