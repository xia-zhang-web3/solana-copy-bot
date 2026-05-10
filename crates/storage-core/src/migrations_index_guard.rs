use crate::SqliteDiscoveryStore;
use anyhow::{Context, Result};
use rusqlite::{params, OptionalExtension};

impl SqliteDiscoveryStore {
    pub(crate) fn drop_sqlite_indexes(&self, index_names: &[&str]) -> Result<()> {
        for index_name in index_names {
            let sql = format!("DROP INDEX IF EXISTS {index_name}");
            self.conn.execute(&sql, []).with_context(|| {
                format!("failed dropping malformed empty sqlite index {index_name}")
            })?;
        }
        Ok(())
    }

    pub(crate) fn sqlite_table_has_rows(&self, table: &str) -> Result<bool> {
        if !self.sqlite_table_exists(table)? {
            return Ok(false);
        }
        let sql = format!("SELECT 1 FROM {table} LIMIT 1");
        Ok(self
            .conn
            .query_row(&sql, [], |row| row.get::<_, i64>(0))
            .optional()
            .with_context(|| format!("failed checking sqlite table row presence: {table}"))?
            .is_some())
    }

    pub(crate) fn sqlite_index_is_valid(
        &self,
        table: &str,
        index_name: &str,
        expected_columns: &[&str],
    ) -> Result<bool> {
        let index_flags = self
            .conn
            .query_row(
                &format!(
                    "SELECT [unique], partial FROM pragma_index_list('{table}') WHERE name = ?1"
                ),
                [index_name],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()
            .with_context(|| format!("failed checking sqlite index flags for {index_name}"))?;
        let Some((unique, partial)) = index_flags else {
            return Ok(false);
        };
        if unique != 0 || partial != 0 {
            return Ok(false);
        }

        let pragma = format!(
            "SELECT name, [desc], coll, key FROM pragma_index_xinfo('{index_name}') ORDER BY seqno"
        );
        let mut stmt = self.conn.prepare(&pragma).with_context(|| {
            format!("failed preparing sqlite index introspection for {index_name}")
        })?;
        let key_columns = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .into_iter()
            .filter(|(_, _, _, key)| *key != 0)
            .collect::<Vec<_>>();
        if key_columns.len() != expected_columns.len() {
            return Ok(false);
        }
        for ((name, desc, coll, _), expected_column) in key_columns.iter().zip(expected_columns) {
            if name.as_deref() != Some(*expected_column) {
                return Ok(false);
            }
            if *desc != 0 || coll.as_deref() != Some("BINARY") {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) fn sqlite_migration_version_recorded(&self, version: &str) -> Result<bool> {
        Ok(self
            .conn
            .query_row(
                "SELECT 1 FROM schema_migrations WHERE version = ?1 LIMIT 1",
                params![version],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .with_context(|| format!("failed checking schema_migrations for {version}"))?
            .is_some())
    }

    pub(crate) fn record_satisfied_deferred_migration(&self, version: &str) -> Result<()> {
        self.conn
            .execute(
                "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?1, datetime('now'))",
                params![version],
            )
            .with_context(|| format!("failed recording satisfied deferred migration {version}"))?;
        tracing::info!(version = version, "deferred migration already satisfied");
        Ok(())
    }
}
