//! Exact durable rows, never reconstructed authority, for the transfer audit.
use anyhow::{Context, Result};
use rusqlite::{types::ValueRef, Connection};

pub(super) fn row(c: &Connection, table: &str, column: &str, id: &str) -> Result<Option<String>> {
    // Callers supply only static internal table/column names.
    let mut s = c.prepare(&format!("SELECT * FROM {table} WHERE {column}=?1"))?;
    let names = s
        .column_names()
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>();
    let mut rows = s.query([id])?;
    let Some(r) = rows.next()? else {
        return Ok(None);
    };
    let mut data = serde_json::Map::new();
    for (n, name) in names.into_iter().enumerate() {
        let value = match r.get_ref(n)? {
            ValueRef::Null => serde_json::Value::Null,
            ValueRef::Integer(v) => serde_json::json!(v),
            ValueRef::Real(v) => serde_json::Value::Number(
                serde_json::Number::from_f64(v).context("unsigned_exit_archive_number")?,
            ),
            ValueRef::Text(v) => serde_json::Value::String(std::str::from_utf8(v)?.into()),
            ValueRef::Blob(_) => anyhow::bail!("unsigned_exit_archive_blob"),
        };
        data.insert(name, value);
    }
    Ok(Some(serde_json::to_string(&data)?))
}
