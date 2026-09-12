use anyhow::Result;
use rusqlite::{types::Value, Connection};

/// Search complete durable fixture tables for the actual signature/payload;
/// a persisted SHA proves binding but cannot recover a reconciliation signature.
pub(super) fn locations(conn: &Connection, needle: &str) -> Result<Vec<String>> {
    let tables = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")?
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut found = Vec::new();
    for table in tables {
        let mut stmt =
            conn.prepare(&format!("SELECT * FROM \"{}\"", table.replace('"', "\"\"")))?;
        let names = stmt
            .column_names()
            .iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>();
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            for (i, name) in names.iter().enumerate() {
                let bytes = match row.get::<_, Value>(i)? {
                    Value::Text(t) => t.into_bytes(),
                    Value::Blob(b) => b,
                    _ => continue,
                };
                if bytes.windows(needle.len()).any(|w| w == needle.as_bytes()) {
                    found.push(format!("{table}.{name}"));
                }
            }
        }
    }
    found.sort();
    found.dedup();
    Ok(found)
}
