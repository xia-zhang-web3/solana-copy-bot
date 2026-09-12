use anyhow::Error;

/// Only recognized row-data decoding/identity failures are local. In particular,
/// SQLite constraints, missing schema, I/O, locks and writer invariant errors are not.
pub(super) fn local_data(error: &Error) -> bool {
    for cause in error.chain() {
        if let Some(sql) = cause.downcast_ref::<rusqlite::Error>() {
            return matches!(
                sql,
                rusqlite::Error::FromSqlConversionFailure(..)
                    | rusqlite::Error::IntegralValueOutOfRange(..)
                    | rusqlite::Error::InvalidColumnType(..)
            );
        }
    }
    let root = error.root_cause();
    if root.is::<chrono::ParseError>() || root.is::<std::num::TryFromIntError>() {
        return true;
    }
    let reason = root.to_string();
    matches!(
        reason.as_str(),
        "invalid staged SELL event identity"
            | "invalid staged SELL witness identity"
            | "malformed source SELL promotion identity"
            | "conflicting source SELL promotion associations"
            | "observed swap exact amount columns are partially populated"
    ) || reason.starts_with("observed_swaps.ts must use canonical UTC offset +00:00: ")
        || reason.starts_with("observed_swaps.slot is negative: ")
}
