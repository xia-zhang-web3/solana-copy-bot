use crate::{SqliteContentionSnapshot, SqliteDiscoveryStore};
use anyhow::{anyhow, Context, Result};
use rusqlite::{Connection, ErrorCode};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration as StdDuration;

const SQLITE_WRITE_MAX_RETRIES: usize = 3;
const SQLITE_WRITE_RETRY_BACKOFF_MS: [u64; SQLITE_WRITE_MAX_RETRIES] = [100, 300, 700];
const SQLITE_OPERATOR_BEGIN_BACKOFF_MS: [u64; 6] = [250, 500, 1_000, 2_000, 4_000, 8_000];
static SQLITE_WRITE_RETRY_TOTAL: AtomicU64 = AtomicU64::new(0);
static SQLITE_BUSY_ERROR_TOTAL: AtomicU64 = AtomicU64::new(0);

impl SqliteDiscoveryStore {
    pub(crate) fn execute_with_retry<F>(&self, operation: F) -> rusqlite::Result<usize>
    where
        F: FnMut(&Connection) -> rusqlite::Result<usize>,
    {
        self.execute_with_retry_result(operation)
    }

    pub(crate) fn execute_with_retry_result<T, F>(&self, mut operation: F) -> rusqlite::Result<T>
    where
        F: FnMut(&Connection) -> rusqlite::Result<T>,
    {
        for attempt in 0..=SQLITE_WRITE_MAX_RETRIES {
            match operation(&self.conn) {
                Ok(result) => return Ok(result),
                Err(error) => {
                    let retryable = is_retryable_sqlite_error(&error);
                    if retryable {
                        note_sqlite_busy_error();
                    }
                    if attempt < SQLITE_WRITE_MAX_RETRIES && retryable {
                        note_sqlite_write_retry();
                        std::thread::sleep(StdDuration::from_millis(
                            SQLITE_WRITE_RETRY_BACKOFF_MS[attempt],
                        ));
                        continue;
                    }
                    return Err(error);
                }
            }
        }
        unreachable!("retry loop must return on success or terminal error");
    }

    pub(crate) fn with_immediate_transaction_retry<T, F>(
        &self,
        transaction_name: &str,
        mut operation: F,
    ) -> Result<T>
    where
        F: FnMut(&Connection) -> Result<T>,
    {
        for attempt in 0..=SQLITE_WRITE_MAX_RETRIES {
            if let Err(error) = self.conn.execute_batch("BEGIN IMMEDIATE TRANSACTION") {
                let error = anyhow!(error)
                    .context(format!("failed to open {transaction_name} transaction"));
                let retryable = is_retryable_sqlite_anyhow_error(&error);
                if retryable {
                    note_sqlite_busy_error();
                }
                if attempt < SQLITE_WRITE_MAX_RETRIES && retryable {
                    note_sqlite_write_retry();
                    std::thread::sleep(StdDuration::from_millis(
                        SQLITE_WRITE_RETRY_BACKOFF_MS[attempt],
                    ));
                    continue;
                }
                return Err(error).with_context(|| format!("failed to run {transaction_name}"));
            }

            let tx_result = operation(&self.conn);
            match tx_result {
                Ok(result) => {
                    if let Err(error) = self.conn.execute_batch("COMMIT") {
                        let error = anyhow!(error)
                            .context(format!("failed to commit {transaction_name} transaction"));
                        let _ = self.conn.execute_batch("ROLLBACK");
                        let retryable = is_retryable_sqlite_anyhow_error(&error);
                        if retryable {
                            note_sqlite_busy_error();
                        }
                        if attempt < SQLITE_WRITE_MAX_RETRIES && retryable {
                            note_sqlite_write_retry();
                            std::thread::sleep(StdDuration::from_millis(
                                SQLITE_WRITE_RETRY_BACKOFF_MS[attempt],
                            ));
                            continue;
                        }
                        return Err(error)
                            .with_context(|| format!("failed to run {transaction_name}"));
                    }
                    return Ok(result);
                }
                Err(error) => {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    let retryable = is_retryable_sqlite_anyhow_error(&error);
                    if retryable {
                        note_sqlite_busy_error();
                    }
                    if attempt < SQLITE_WRITE_MAX_RETRIES && retryable {
                        note_sqlite_write_retry();
                        std::thread::sleep(StdDuration::from_millis(
                            SQLITE_WRITE_RETRY_BACKOFF_MS[attempt],
                        ));
                        continue;
                    }
                    return Err(error).with_context(|| format!("failed to run {transaction_name}"));
                }
            }
        }
        unreachable!("retry loop must return on success or terminal error");
    }

    pub(crate) fn begin_immediate_transaction_with_operator_retry(
        &self,
        transaction_name: &str,
    ) -> Result<()> {
        for attempt in 0..=SQLITE_OPERATOR_BEGIN_BACKOFF_MS.len() {
            match self.conn.execute_batch("BEGIN IMMEDIATE TRANSACTION") {
                Ok(()) => return Ok(()),
                Err(error) => {
                    let error = anyhow!(error)
                        .context(format!("failed to open {transaction_name} transaction"));
                    let retryable = is_retryable_sqlite_anyhow_error(&error);
                    if retryable {
                        note_sqlite_busy_error();
                    }
                    let Some(backoff_ms) = SQLITE_OPERATOR_BEGIN_BACKOFF_MS.get(attempt) else {
                        return Err(error)
                            .with_context(|| format!("failed to run {transaction_name}"));
                    };
                    if !retryable {
                        return Err(error)
                            .with_context(|| format!("failed to run {transaction_name}"));
                    }
                    note_sqlite_write_retry();
                    std::thread::sleep(StdDuration::from_millis(*backoff_ms));
                }
            }
        }
        unreachable!("retry loop must return on success or terminal error");
    }
}

pub fn sqlite_contention_snapshot() -> SqliteContentionSnapshot {
    SqliteContentionSnapshot {
        write_retry_total: SQLITE_WRITE_RETRY_TOTAL.load(Ordering::Relaxed),
        busy_error_total: SQLITE_BUSY_ERROR_TOTAL.load(Ordering::Relaxed),
    }
}

fn note_sqlite_write_retry() {
    SQLITE_WRITE_RETRY_TOTAL.fetch_add(1, Ordering::Relaxed);
}

fn note_sqlite_busy_error() {
    SQLITE_BUSY_ERROR_TOTAL.fetch_add(1, Ordering::Relaxed);
}

fn is_retryable_sqlite_message(message: &str) -> bool {
    let lowered = message.to_ascii_lowercase();
    lowered.contains("database is locked")
        || lowered.contains("database is busy")
        || lowered.contains("database table is locked")
}

fn is_fatal_sqlite_io_message(message: &str) -> bool {
    let lowered = message.to_ascii_lowercase();
    lowered.contains("disk i/o error")
        || lowered.contains("i/o error within the xshmmap method")
        || lowered.contains("database or disk is full")
        || lowered.contains("disk full")
}

pub(crate) fn is_retryable_sqlite_error(error: &rusqlite::Error) -> bool {
    match error {
        rusqlite::Error::SqliteFailure(code, message) => {
            matches!(
                code.code,
                ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
            ) || message
                .as_deref()
                .map(is_retryable_sqlite_message)
                .unwrap_or(false)
        }
        _ => is_retryable_sqlite_message(&error.to_string()),
    }
}

pub fn is_retryable_sqlite_anyhow_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        if let Some(sqlite_error) = cause.downcast_ref::<rusqlite::Error>() {
            return is_retryable_sqlite_error(sqlite_error);
        }
        is_retryable_sqlite_message(&cause.to_string())
    })
}

pub fn is_fatal_sqlite_anyhow_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| is_fatal_sqlite_io_message(&cause.to_string()))
}
