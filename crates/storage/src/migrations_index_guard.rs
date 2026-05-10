use super::*;

impl SqliteStore {
    pub(super) fn drop_sqlite_indexes<'a>(
        &self,
        index_names: impl IntoIterator<Item = &'a str>,
    ) -> Result<()> {
        for index_name in index_names {
            let sql = format!("DROP INDEX IF EXISTS {index_name}");
            self.conn.execute(&sql, []).with_context(|| {
                format!("failed dropping malformed empty sqlite index {index_name}")
            })?;
        }
        Ok(())
    }

    pub(super) fn sqlite_table_has_rows(&self, table_name: &str) -> Result<bool> {
        if !self.sqlite_table_exists(table_name)? {
            return Ok(false);
        }
        let sql = format!("SELECT 1 FROM {table_name} LIMIT 1");
        Ok(self
            .conn
            .query_row(&sql, [], |row| row.get::<_, i64>(0))
            .optional()
            .with_context(|| format!("failed checking sqlite table row presence: {table_name}"))?
            .is_some())
    }

    pub(super) fn sqlite_migration_version_recorded(&self, version: &str) -> Result<bool> {
        Ok(self
            .conn
            .query_row(
                "SELECT 1 FROM schema_migrations WHERE version = ?1 LIMIT 1",
                params![version],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .with_context(|| format!("failed checking sqlite migration version {version}"))?
            .is_some())
    }

    pub(crate) fn observed_swaps_sol_leg_index_valid(&self) -> Result<bool> {
        self.observed_swaps_index_valid(
            "idx_observed_swaps_sol_leg_ts_slot_signature",
            &["ts", "slot", "signature"],
            true,
            Some(OBSERVED_SWAPS_SOL_LEG_PREDICATE),
        )
    }

    fn observed_swaps_index_valid(
        &self,
        index_name: &str,
        expected_columns: &[&str],
        expected_partial: bool,
        expected_predicate: Option<&str>,
    ) -> Result<bool> {
        self.sqlite_index_valid(
            "observed_swaps",
            index_name,
            expected_columns,
            expected_partial,
            expected_predicate,
        )
    }

    pub(super) fn sqlite_index_valid(
        &self,
        table_name: &str,
        index_name: &str,
        expected_columns: &[&str],
        expected_partial: bool,
        expected_predicate: Option<&str>,
    ) -> Result<bool> {
        if !self.sqlite_table_exists(table_name)? {
            return Ok(false);
        }
        let index_flags = self
            .conn
            .query_row(
                &format!(
                    "SELECT [unique], partial FROM pragma_index_list('{table_name}') WHERE name = ?1"
                ),
                params![index_name],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()
            .with_context(|| format!("failed checking legacy sqlite index flags for {index_name}"))?;
        let Some((unique, partial)) = index_flags else {
            return Ok(false);
        };
        if unique != 0 || (partial != 0) != expected_partial {
            return Ok(false);
        }

        let pragma = format!(
            "SELECT name, [desc], coll, key
             FROM pragma_index_xinfo('{index_name}')
             ORDER BY seqno"
        );
        let mut stmt = self.conn.prepare(&pragma).with_context(|| {
            format!("failed preparing legacy sqlite index introspection for {index_name}")
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
        for ((name, desc, coll, _), expected) in key_columns.iter().zip(expected_columns.iter()) {
            if name.as_deref() != Some(*expected) {
                return Ok(false);
            }
            if *desc != 0 || coll.as_deref() != Some("BINARY") {
                return Ok(false);
            }
        }
        let Some(expected_predicate) = expected_predicate else {
            return Ok(true);
        };

        let index_sql: Option<String> = self
            .conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?1 LIMIT 1",
                params![index_name],
                |row| row.get(0),
            )
            .optional()
            .with_context(|| format!("failed reading legacy sqlite index sql for {index_name}"))?;
        let Some(index_sql) = index_sql else {
            return Ok(false);
        };
        let compact_index_sql = compact_sql_preserving_strings(&index_sql);
        let Some((_, predicate)) = compact_index_sql.split_once("where") else {
            return Ok(false);
        };
        let predicate = predicate.strip_suffix(';').unwrap_or(predicate);
        Ok(predicate == compact_sql_preserving_strings(expected_predicate))
    }
}

fn compact_sql_preserving_strings(sql: &str) -> String {
    let mut compact = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut in_string = false;
    while let Some(ch) = chars.next() {
        if in_string {
            compact.push(ch);
            if ch == '\'' {
                if chars.peek() == Some(&'\'') {
                    compact.push(chars.next().expect("peeked escaped quote"));
                } else {
                    in_string = false;
                }
            }
            continue;
        }
        if ch == '\'' {
            in_string = true;
            compact.push(ch);
            continue;
        }
        if ch.is_whitespace() || matches!(ch, '"' | '`' | '[' | ']') {
            continue;
        }
        compact.extend(ch.to_lowercase());
    }
    compact
}
