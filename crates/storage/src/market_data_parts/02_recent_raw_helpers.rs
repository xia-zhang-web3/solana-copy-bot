#[cfg(test)]
thread_local! {
    static RECENT_RAW_BULK_WRITE_BUDGET_HOOK: RefCell<Option<Box<dyn FnMut(usize, usize) -> bool>>> =
        RefCell::new(None);
}

#[cfg(test)]
thread_local! {
    static EXACT_WALLET_ACTIVITY_PRE_ROW_BUDGET_EXHAUSTION_HOOK: RefCell<bool> =
        RefCell::new(false);
}

#[cfg(test)]
struct RecentRawBulkWriteBudgetHookGuard;

#[cfg(test)]
impl Drop for RecentRawBulkWriteBudgetHookGuard {
    fn drop(&mut self) {
        RECENT_RAW_BULK_WRITE_BUDGET_HOOK.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}

#[cfg(test)]
fn install_recent_raw_bulk_write_budget_hook<F>(hook: F) -> RecentRawBulkWriteBudgetHookGuard
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
struct ExactWalletActivityPreRowBudgetExhaustionGuard;

#[cfg(test)]
impl Drop for ExactWalletActivityPreRowBudgetExhaustionGuard {
    fn drop(&mut self) {
        EXACT_WALLET_ACTIVITY_PRE_ROW_BUDGET_EXHAUSTION_HOOK.with(|slot| {
            *slot.borrow_mut() = false;
        });
    }
}

#[cfg(test)]
fn force_exact_wallet_activity_pre_row_budget_exhaustion_for_test(
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
fn recent_raw_bulk_write_test_hook_requests_budget_exhaustion(
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
fn recent_raw_bulk_write_test_hook_requests_budget_exhaustion(
    _processed_rows: usize,
    _inserted_rows: usize,
) -> bool {
    false
}

fn parse_rfc3339_utc(raw: &str, field_name: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .with_context(|| format!("invalid {field_name} timestamp value: {raw}"))
}

fn parse_optional_rfc3339_utc(
    raw: Option<String>,
    field_name: &str,
) -> Result<Option<DateTime<Utc>>> {
    raw.map(|raw| parse_rfc3339_utc(&raw, field_name))
        .transpose()
}

fn parse_optional_day_start_utc(
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

fn discovery_runtime_cursor_cmp(
    left: &DiscoveryRuntimeCursor,
    right: &DiscoveryRuntimeCursor,
) -> Ordering {
    left.ts_utc
        .cmp(&right.ts_utc)
        .then_with(|| left.slot.cmp(&right.slot))
        .then_with(|| left.signature.cmp(&right.signature))
}

fn ensure_recent_raw_journal_tables_on_conn(conn: &Connection) -> Result<()> {
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
        CREATE INDEX IF NOT EXISTS idx_observed_swaps_ts_slot_signature
            ON observed_swaps(ts, slot, signature);
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
    .context("failed ensuring recent raw journal tables exist")
}

fn recent_raw_journal_bulk_insert_sql(row_count: usize) -> String {
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

fn recent_raw_journal_effective_bulk_insert_chunk_rows(
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

fn recent_raw_journal_sqlite_variable_limit(conn: &Connection) -> usize {
    unsafe {
        rusqlite::ffi::sqlite3_limit(
            conn.handle(),
            rusqlite::ffi::SQLITE_LIMIT_VARIABLE_NUMBER,
            -1,
        )
    }
    .max(0) as usize
}

fn recent_raw_elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u64::MAX as u128) as u64
}

fn push_recent_raw_journal_bulk_insert_values(values: &mut Vec<SqlValue>, swap: &SwapEvent) {
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

fn recent_raw_journal_sqlite_error_is_operation_interrupted(error: &rusqlite::Error) -> bool {
    error.sqlite_error_code() == Some(ErrorCode::OperationInterrupted)
}

fn recent_raw_journal_anyhow_error_is_operation_interrupted(error: &anyhow::Error) -> bool {
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
