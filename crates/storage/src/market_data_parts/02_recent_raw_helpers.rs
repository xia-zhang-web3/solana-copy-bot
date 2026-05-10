use super::*;

#[cfg(test)]
thread_local! {
    pub(super) static RECENT_RAW_BULK_WRITE_BUDGET_HOOK: RefCell<Option<Box<dyn FnMut(usize, usize) -> bool>>> =
        RefCell::new(None);
}

#[cfg(test)]
thread_local! {
    pub(super) static EXACT_WALLET_ACTIVITY_PRE_ROW_BUDGET_EXHAUSTION_HOOK: RefCell<bool> =
        RefCell::new(false);
}

#[cfg(test)]
pub(super) struct RecentRawBulkWriteBudgetHookGuard;

#[cfg(test)]
impl Drop for RecentRawBulkWriteBudgetHookGuard {
    fn drop(&mut self) {
        RECENT_RAW_BULK_WRITE_BUDGET_HOOK.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}

#[cfg(test)]
pub(super) fn install_recent_raw_bulk_write_budget_hook<F>(
    hook: F,
) -> RecentRawBulkWriteBudgetHookGuard
where
    F: FnMut(usize, usize) -> bool + 'static,
{
    RECENT_RAW_BULK_WRITE_BUDGET_HOOK.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(
            slot.is_none(),
            "recent_raw bulk write hook already installed"
        );
        *slot = Some(Box::new(hook));
    });
    RecentRawBulkWriteBudgetHookGuard
}

#[cfg(test)]
pub(super) struct ExactWalletActivityPreRowBudgetExhaustionGuard;

#[cfg(test)]
impl Drop for ExactWalletActivityPreRowBudgetExhaustionGuard {
    fn drop(&mut self) {
        EXACT_WALLET_ACTIVITY_PRE_ROW_BUDGET_EXHAUSTION_HOOK.with(|slot| {
            *slot.borrow_mut() = false;
        });
    }
}

#[cfg(test)]
pub(super) fn force_exact_wallet_activity_pre_row_budget_exhaustion_for_test(
) -> ExactWalletActivityPreRowBudgetExhaustionGuard {
    EXACT_WALLET_ACTIVITY_PRE_ROW_BUDGET_EXHAUSTION_HOOK.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(
            !*slot,
            "exact wallet activity pre-row budget exhaustion hook already installed"
        );
        *slot = true;
    });
    ExactWalletActivityPreRowBudgetExhaustionGuard
}

#[cfg(test)]
pub(super) fn recent_raw_bulk_write_test_hook_requests_budget_exhaustion(
    processed_rows: usize,
    inserted_rows: usize,
) -> bool {
    RECENT_RAW_BULK_WRITE_BUDGET_HOOK.with(|slot| {
        slot.borrow_mut()
            .as_mut()
            .is_some_and(|hook| hook(processed_rows, inserted_rows))
    })
}

#[cfg(not(test))]
pub(super) fn recent_raw_bulk_write_test_hook_requests_budget_exhaustion(
    _processed_rows: usize,
    _inserted_rows: usize,
) -> bool {
    false
}

pub(super) fn parse_rfc3339_utc(raw: &str, field_name: &str) -> Result<DateTime<Utc>> {
    if !raw.ends_with("+00:00") {
        bail!("{field_name} must use canonical UTC offset +00:00: {raw}");
    }
    let parsed = DateTime::parse_from_rfc3339(raw)
        .with_context(|| format!("invalid {field_name} timestamp value: {raw}"))?;
    if parsed.offset().local_minus_utc() != 0 {
        bail!("{field_name} must use UTC offset +00:00: {raw}");
    }
    Ok(parsed.with_timezone(&Utc))
}

pub(super) fn parse_optional_rfc3339_utc(
    raw: Option<String>,
    field_name: &str,
) -> Result<Option<DateTime<Utc>>> {
    raw.map(|raw| parse_rfc3339_utc(&raw, field_name))
        .transpose()
}

pub(super) fn parse_sqlite_slot(raw: i64, field_name: &str) -> Result<u64> {
    anyhow::ensure!(raw >= 0, "{field_name} is negative: {raw}");
    Ok(raw as u64)
}

pub(super) fn parse_optional_day_start_utc(
    raw: Option<String>,
    field_name: &str,
) -> Result<Option<DateTime<Utc>>> {
    raw.map(|raw| {
        let day = NaiveDate::parse_from_str(&raw, "%Y-%m-%d")
            .with_context(|| format!("invalid {field_name} day value: {raw}"))?;
        Ok(DateTime::<Utc>::from_naive_utc_and_offset(
            day.and_hms_opt(0, 0, 0).expect("midnight utc day"),
            Utc,
        ))
    })
    .transpose()
}

pub(super) fn discovery_runtime_cursor_cmp(
    left: &DiscoveryRuntimeCursor,
    right: &DiscoveryRuntimeCursor,
) -> Ordering {
    left.ts_utc
        .cmp(&right.ts_utc)
        .then_with(|| left.slot.cmp(&right.slot))
        .then_with(|| left.signature.cmp(&right.signature))
}

pub(super) fn ensure_recent_raw_journal_tables_on_conn(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS observed_swaps (
            signature TEXT PRIMARY KEY,
            wallet_id TEXT NOT NULL,
            dex TEXT NOT NULL,
            token_in TEXT NOT NULL,
            token_out TEXT NOT NULL,
            qty_in REAL NOT NULL,
            qty_out REAL NOT NULL,
            qty_in_raw TEXT,
            qty_in_decimals INTEGER,
            qty_out_raw TEXT,
            qty_out_decimals INTEGER,
            slot INTEGER NOT NULL,
            ts TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS recent_raw_journal_state (
            id INTEGER PRIMARY KEY CHECK(id = 1),
            covered_since_ts TEXT,
            covered_through_cursor_ts TEXT,
            covered_through_cursor_slot INTEGER,
            covered_through_cursor_signature TEXT,
            row_count INTEGER NOT NULL DEFAULT 0,
            last_batch_rows INTEGER NOT NULL DEFAULT 0,
            last_batch_completed_at TEXT,
            last_pruned_rows INTEGER NOT NULL DEFAULT 0,
            last_pruned_at TEXT,
            updated_at TEXT NOT NULL
        );",
    )
    .context("failed ensuring recent raw journal tables exist")?;
    ensure_recent_raw_observed_swaps_ts_index_empty_safe(conn)?;
    ensure_recent_raw_observed_swaps_non_utc_index_empty_safe(conn)
}

fn ensure_recent_raw_observed_swaps_ts_index_empty_safe(conn: &Connection) -> Result<()> {
    if observed_swaps_ts_slot_signature_index_is_valid(conn)? {
        return Ok(());
    }
    let has_rows = conn
        .query_row("SELECT 1 FROM observed_swaps LIMIT 1", [], |row| {
            row.get::<_, i64>(0)
        })
        .optional()
        .context(
            "failed checking observed_swaps row presence before legacy recent_raw index ensure",
        )?
        .is_some();
    if has_rows {
        bail!(
            "legacy recent_raw observed_swaps index idx_observed_swaps_ts_slot_signature \
             is missing or malformed on a non-empty table; run migration 0007 offline"
        );
    }
    conn.execute_batch(
        "DROP INDEX IF EXISTS idx_observed_swaps_ts_slot_signature;
         CREATE INDEX idx_observed_swaps_ts_slot_signature
            ON observed_swaps(ts, slot, signature);",
    )
    .context("failed preparing empty legacy recent_raw observed_swaps ts index")?;
    Ok(())
}

fn ensure_recent_raw_observed_swaps_non_utc_index_empty_safe(conn: &Connection) -> Result<()> {
    if super::recent_raw_timestamp_guard::ensure_recent_raw_timestamp_guard_index_valid(conn)
        .is_ok()
    {
        return Ok(());
    }
    let has_rows = conn
        .query_row("SELECT 1 FROM observed_swaps LIMIT 1", [], |row| {
            row.get::<_, i64>(0)
        })
        .optional()
        .context(
            "failed checking observed_swaps row presence before legacy recent_raw timestamp guard index ensure",
        )?
        .is_some();
    if has_rows {
        bail!(
            "legacy recent_raw observed_swaps index idx_observed_swaps_non_utc_ts \
             is missing or malformed on a non-empty table; run migration 0040 offline"
        );
    }
    conn.execute_batch(&format!(
        "DROP INDEX IF EXISTS {index};
         CREATE INDEX {index}
            ON observed_swaps(ts)
            WHERE {predicate};",
        index = super::recent_raw_timestamp_guard::OBSERVED_SWAPS_NON_UTC_TIMESTAMP_INDEX,
        predicate = super::recent_raw_timestamp_guard::OBSERVED_SWAPS_NON_UTC_TIMESTAMP_PREDICATE
    ))
    .context("failed preparing empty legacy recent_raw timestamp guard index")?;
    Ok(())
}

fn observed_swaps_ts_slot_signature_index_is_valid(conn: &Connection) -> Result<bool> {
    let index_flags = conn
        .query_row(
            "SELECT [unique], partial
             FROM pragma_index_list('observed_swaps')
             WHERE name = 'idx_observed_swaps_ts_slot_signature'",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .context("failed checking legacy recent_raw observed_swaps ts index flags")?;
    let Some((unique, partial)) = index_flags else {
        return Ok(false);
    };
    if unique != 0 || partial != 0 {
        return Ok(false);
    }
    let mut stmt = conn
        .prepare(
            "SELECT name, [desc], coll, key
             FROM pragma_index_xinfo('idx_observed_swaps_ts_slot_signature')
             ORDER BY seqno",
        )
        .context("failed preparing legacy recent_raw observed_swaps ts index introspection")?;
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
    if key_columns.len() != 3 {
        return Ok(false);
    }
    for ((name, desc, coll, _), expected) in
        key_columns.iter().zip(["ts", "slot", "signature"].iter())
    {
        if name.as_deref() != Some(*expected) {
            return Ok(false);
        }
        if *desc != 0 || coll.as_deref() != Some("BINARY") {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn recent_raw_journal_bulk_insert_sql(row_count: usize) -> String {
    let row_placeholders = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    let placeholders = std::iter::repeat_n(row_placeholders, row_count)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT OR IGNORE INTO observed_swaps(
            signature,
            wallet_id,
            dex,
            token_in,
            token_out,
            qty_in,
            qty_out,
            qty_in_raw,
            qty_in_decimals,
            qty_out_raw,
            qty_out_decimals,
            slot,
            ts
         ) VALUES {placeholders}"
    )
}

pub(super) fn recent_raw_journal_effective_bulk_insert_chunk_rows(
    requested_chunk_rows: Option<usize>,
    sqlite_variable_limit: usize,
) -> usize {
    let requested_or_default =
        requested_chunk_rows.unwrap_or(RECENT_RAW_JOURNAL_BULK_INSERT_HARD_CAP_ROWS);
    let sqlite_limit_rows = sqlite_variable_limit / RECENT_RAW_JOURNAL_BULK_INSERT_PARAMS_PER_ROW;
    requested_or_default
        .min(RECENT_RAW_JOURNAL_BULK_INSERT_HARD_CAP_ROWS)
        .min(sqlite_limit_rows)
        .max(1)
}

pub(super) fn recent_raw_journal_sqlite_variable_limit(conn: &Connection) -> usize {
    unsafe {
        rusqlite::ffi::sqlite3_limit(
            conn.handle(),
            rusqlite::ffi::SQLITE_LIMIT_VARIABLE_NUMBER,
            -1,
        )
    }
    .max(0) as usize
}

pub(super) fn recent_raw_elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u64::MAX as u128) as u64
}

pub(super) fn push_recent_raw_journal_bulk_insert_values(
    values: &mut Vec<SqlValue>,
    swap: &SwapEvent,
) {
    values.push(SqlValue::Text(swap.signature.clone()));
    values.push(SqlValue::Text(swap.wallet.clone()));
    values.push(SqlValue::Text(swap.dex.clone()));
    values.push(SqlValue::Text(swap.token_in.clone()));
    values.push(SqlValue::Text(swap.token_out.clone()));
    values.push(SqlValue::Real(swap.amount_in));
    values.push(SqlValue::Real(swap.amount_out));
    values.push(
        swap.exact_amounts
            .as_ref()
            .map(|value| SqlValue::Text(value.amount_in_raw.clone()))
            .unwrap_or(SqlValue::Null),
    );
    values.push(
        swap.exact_amounts
            .as_ref()
            .map(|value| SqlValue::Integer(i64::from(value.amount_in_decimals)))
            .unwrap_or(SqlValue::Null),
    );
    values.push(
        swap.exact_amounts
            .as_ref()
            .map(|value| SqlValue::Text(value.amount_out_raw.clone()))
            .unwrap_or(SqlValue::Null),
    );
    values.push(
        swap.exact_amounts
            .as_ref()
            .map(|value| SqlValue::Integer(i64::from(value.amount_out_decimals)))
            .unwrap_or(SqlValue::Null),
    );
    values.push(SqlValue::Integer(swap.slot as i64));
    values.push(SqlValue::Text(swap.ts_utc.to_rfc3339()));
}

pub(super) fn recent_raw_journal_sqlite_error_is_operation_interrupted(
    error: &rusqlite::Error,
) -> bool {
    error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted)
}

pub(super) fn recent_raw_journal_anyhow_error_is_operation_interrupted(
    error: &anyhow::Error,
) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<rusqlite::Error>()
            .is_some_and(recent_raw_journal_sqlite_error_is_operation_interrupted)
            || cause
                .to_string()
                .to_ascii_lowercase()
                .contains("interrupted")
    })
}
