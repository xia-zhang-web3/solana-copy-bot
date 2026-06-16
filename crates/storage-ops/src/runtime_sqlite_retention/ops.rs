use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OpenFlags};
use std::path::Path;

use super::cli::Cli;

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct DeleteTotals {
    pub(super) rows: u64,
    pub(super) batches: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct CheckpointResult {
    pub(super) busy: i64,
    pub(super) log_frames: i64,
    pub(super) checkpointed_frames: i64,
}

pub(super) struct ExecutedMaintenance {
    pub(super) observed_cutoff: DateTime<Utc>,
    pub(super) canary_cutoff: DateTime<Utc>,
    pub(super) observed: DeleteTotals,
    pub(super) canary_events: DeleteTotals,
    pub(super) canary_provider_samples: DeleteTotals,
    pub(super) canary_shadow_gate: DeleteTotals,
    pub(super) canary_ts_indexes_created: bool,
    pub(super) checkpoint: Option<CheckpointResult>,
    pub(super) vacuum_attempted: bool,
}

pub(super) fn execute_commit(
    cli: &Cli,
    db_path: &Path,
    observed_cutoff: DateTime<Utc>,
    canary_cutoff: DateTime<Utc>,
) -> Result<ExecutedMaintenance> {
    let conn = Connection::open_with_flags(
        db_path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("failed opening runtime DB {}", db_path.display()))?;
    conn.busy_timeout(std::time::Duration::from_secs(30))?;
    let canary_ts_indexes_created = if cli.create_canary_ts_indexes {
        create_canary_ts_indexes(&conn)?
    } else {
        false
    };
    let observed = delete_observed_swaps(
        &conn,
        observed_cutoff,
        cli.observed_batch_size,
        cli.max_observed_rows,
    )?;
    let canary_events = delete_batched(
        &conn,
        "execution_quote_canary_events",
        "request_ts",
        canary_cutoff,
        cli.canary_batch_size,
        cli.max_canary_rows,
    )?;
    let canary_provider_samples = delete_batched(
        &conn,
        "execution_quote_canary_provider_samples",
        "request_ts",
        canary_cutoff,
        cli.canary_batch_size,
        cli.max_canary_rows,
    )?;
    let canary_shadow_gate = delete_batched(
        &conn,
        "execution_quote_canary_shadow_gate_events",
        "recorded_ts",
        canary_cutoff,
        cli.canary_batch_size,
        cli.max_canary_rows,
    )?;
    let checkpoint = if cli.checkpoint_truncate {
        Some(checkpoint_truncate(&conn)?)
    } else {
        None
    };
    if let Some(output) = &cli.vacuum_into {
        vacuum_into(&conn, output)?;
    }
    Ok(ExecutedMaintenance {
        observed_cutoff,
        canary_cutoff,
        observed,
        canary_events,
        canary_provider_samples,
        canary_shadow_gate,
        canary_ts_indexes_created,
        checkpoint,
        vacuum_attempted: cli.vacuum_into.is_some(),
    })
}

fn delete_observed_swaps(
    conn: &Connection,
    cutoff: DateTime<Utc>,
    batch_size: u64,
    max_rows: u64,
) -> Result<DeleteTotals> {
    if max_rows == 0 {
        return Ok(DeleteTotals::default());
    }
    let cutoff = cutoff.to_rfc3339();
    let mut totals = DeleteTotals::default();
    while totals.rows < max_rows {
        let limit = batch_size.min(max_rows - totals.rows).min(i64::MAX as u64) as i64;
        let deleted =
            conn.execute(
                "DELETE FROM observed_swaps
                 WHERE rowid IN (
                    SELECT rowid FROM observed_swaps INDEXED BY idx_observed_swaps_ts_slot_signature
                    WHERE ts < ?1
                    ORDER BY ts ASC, slot ASC, signature ASC
                    LIMIT ?2
                 )",
                params![cutoff, limit],
            )
            .context("failed deleting observed_swaps retention batch")? as u64;
        if deleted == 0 {
            break;
        }
        totals.rows += deleted;
        totals.batches += 1;
    }
    Ok(totals)
}

fn delete_batched(
    conn: &Connection,
    table: &str,
    ts_column: &str,
    cutoff: DateTime<Utc>,
    batch_size: u64,
    max_rows: u64,
) -> Result<DeleteTotals> {
    if max_rows == 0 || !table_exists(conn, table)? {
        return Ok(DeleteTotals::default());
    }
    let cutoff = cutoff.to_rfc3339();
    let mut totals = DeleteTotals::default();
    let sql = format!(
        "DELETE FROM {table}
         WHERE rowid IN (
            SELECT rowid FROM {table}
            WHERE {ts_column} < ?1
            ORDER BY {ts_column} ASC
            LIMIT ?2
         )"
    );
    while totals.rows < max_rows {
        let limit = batch_size.min(max_rows - totals.rows).min(i64::MAX as u64) as i64;
        let deleted = conn
            .execute(&sql, params![cutoff, limit])
            .with_context(|| format!("failed deleting {table} retention batch"))?
            as u64;
        if deleted == 0 {
            break;
        }
        totals.rows += deleted;
        totals.batches += 1;
    }
    Ok(totals)
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    let exists: i64 = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
        [table],
        |row| row.get(0),
    )?;
    Ok(exists != 0)
}

fn create_canary_ts_indexes(conn: &Connection) -> Result<bool> {
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_execution_quote_canary_events_request_ts_only
            ON execution_quote_canary_events(request_ts);
         CREATE INDEX IF NOT EXISTS idx_execution_quote_canary_provider_samples_request_ts_only
            ON execution_quote_canary_provider_samples(request_ts);
         CREATE INDEX IF NOT EXISTS idx_execution_quote_canary_shadow_gate_recorded_ts_only
            ON execution_quote_canary_shadow_gate_events(recorded_ts);",
    )
    .context("failed creating quote canary timestamp indexes")?;
    Ok(true)
}

fn checkpoint_truncate(conn: &Connection) -> Result<CheckpointResult> {
    let (busy, log_frames, checkpointed_frames): (i64, i64, i64) = conn
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .context("failed running WAL checkpoint truncate")?;
    Ok(CheckpointResult {
        busy,
        log_frames,
        checkpointed_frames,
    })
}

fn vacuum_into(conn: &Connection, output: &Path) -> Result<()> {
    if output.exists() {
        bail!("VACUUM output already exists: {}", output.display());
    }
    let parent = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| anyhow::anyhow!("VACUUM output has no parent: {}", output.display()))?;
    if !parent.exists() {
        bail!("VACUUM output parent does not exist: {}", parent.display());
    }
    conn.execute_batch(&format!("VACUUM INTO '{}'", sql_literal_path(output)))
        .with_context(|| format!("failed running VACUUM INTO {}", output.display()))?;
    Ok(())
}

fn sql_literal_path(path: &Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}
