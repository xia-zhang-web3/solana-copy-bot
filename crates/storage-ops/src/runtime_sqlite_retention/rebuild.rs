use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OpenFlags};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

use super::rebuild_checks::{run_rebuild_cheap_checks, RebuildCheapCheckReport};
use super::schema_sql::{
    load_schema_objects, qualify_for_schema, quote_ident, SchemaObject, SchemaObjectKind,
};

const COMPACT_SCHEMA: &str = "compact";
const OBSERVED_SWAPS: &str = "observed_swaps";
const OBSERVED_SOL_LEG_SWAPS: &str = "observed_sol_leg_swaps";
const OBSERVED_SOL_LEG_COVERAGE: &str = "observed_sol_leg_coverage";
const CANARY_EVENTS: &str = "execution_quote_canary_events";
const CANARY_PROVIDER_SAMPLES: &str = "execution_quote_canary_provider_samples";
const CANARY_SHADOW_GATE: &str = "execution_quote_canary_shadow_gate_events";

#[derive(Debug, Clone)]
pub(super) struct RebuildReport {
    pub(super) tables_copied: u64,
    pub(super) rows_copied: u64,
    pub(super) observed_rows_copied: u64,
    pub(super) sol_leg_rows_copied: u64,
    pub(super) canary_rows_copied: u64,
    pub(super) indexes_created: u64,
    pub(super) triggers_created: u64,
    pub(super) views_created: u64,
    pub(super) integrity_check: Option<String>,
    pub(super) foreign_key_violations: Option<u64>,
    pub(super) full_integrity_deferred: bool,
    pub(super) cheap_checks: Option<RebuildCheapCheckReport>,
    pub(super) phase_timings_ms: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct RebuildOptions {
    pub(super) defer_full_integrity: bool,
}

#[derive(Debug, Clone, Copy)]
enum CopyPlan<'a> {
    All,
    Since {
        ts_column: &'a str,
        cutoff: DateTime<Utc>,
    },
    SolLegCoverage {
        observed_cutoff: DateTime<Utc>,
    },
}

pub(super) fn execute_rebuild(
    source_path: &Path,
    output_path: &Path,
    observed_cutoff: DateTime<Utc>,
    canary_cutoff: DateTime<Utc>,
    options: RebuildOptions,
) -> Result<RebuildReport> {
    if output_path.exists() {
        bail!("rebuild output already exists: {}", output_path.display());
    }
    let parent = output_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!("rebuild output has no parent: {}", output_path.display())
        })?;
    if !parent.exists() {
        bail!("rebuild output parent does not exist: {}", parent.display());
    }

    let conn = Connection::open_with_flags(
        source_path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("failed opening runtime DB {}", source_path.display()))?;
    conn.busy_timeout(std::time::Duration::from_secs(30))?;
    attach_output(&conn, output_path)?;
    let result = rebuild_attached(&conn, observed_cutoff, canary_cutoff, options);
    let detach_result =
        conn.execute_batch(&format!("DETACH DATABASE {}", quote_ident(COMPACT_SCHEMA)));
    match (result, detach_result) {
        (Ok(report), Ok(())) => Ok(report),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error).context("failed detaching rebuild output"),
    }
}

fn rebuild_attached(
    conn: &Connection,
    observed_cutoff: DateTime<Utc>,
    canary_cutoff: DateTime<Utc>,
    options: RebuildOptions,
) -> Result<RebuildReport> {
    conn.execute_batch(&format!(
        "PRAGMA {}.foreign_keys = OFF;
         PRAGMA {}.auto_vacuum = INCREMENTAL;
         PRAGMA {}.synchronous = NORMAL;",
        quote_ident(COMPACT_SCHEMA),
        quote_ident(COMPACT_SCHEMA),
        quote_ident(COMPACT_SCHEMA)
    ))
    .context("failed preparing compact rebuild pragmas")?;

    let mut phase_timings_ms = BTreeMap::new();
    let objects = timed(&mut phase_timings_ms, "load_schema", || {
        load_schema_objects(conn)
    })?;
    timed(&mut phase_timings_ms, "create_tables", || {
        create_schema_objects(conn, &objects, SchemaObjectKind::Table)
    })?;
    let mut report = timed(&mut phase_timings_ms, "copy_tables", || {
        copy_tables(conn, &objects, observed_cutoff, canary_cutoff)
    })?;
    timed(&mut phase_timings_ms, "copy_sqlite_sequence", || {
        copy_sqlite_sequence(conn)
    })?;
    report.indexes_created = timed(&mut phase_timings_ms, "create_indexes", || {
        create_schema_objects(conn, &objects, SchemaObjectKind::Index)
    })?;
    let canary_indexes = timed(&mut phase_timings_ms, "create_canary_indexes", || {
        create_compact_canary_ts_indexes(conn)
    })?;
    report.indexes_created = report.indexes_created.saturating_add(canary_indexes);
    report.triggers_created = timed(&mut phase_timings_ms, "create_triggers", || {
        create_schema_objects(conn, &objects, SchemaObjectKind::Trigger)
    })?;
    report.views_created = timed(&mut phase_timings_ms, "create_views", || {
        create_schema_objects(conn, &objects, SchemaObjectKind::View)
    })?;
    timed(&mut phase_timings_ms, "set_output_pragmas", || {
        set_output_pragmas(conn)
    })?;
    if options.defer_full_integrity {
        let cheap_checks = timed(&mut phase_timings_ms, "cheap_checks", || {
            run_rebuild_cheap_checks(conn, observed_cutoff, canary_cutoff)
        })?;
        cheap_checks.enforce()?;
        report.cheap_checks = Some(cheap_checks);
        report.full_integrity_deferred = true;
    } else {
        report.integrity_check = Some(timed(&mut phase_timings_ms, "integrity_check", || {
            integrity_check(conn)
        })?);
    }
    report.foreign_key_violations =
        Some(timed(&mut phase_timings_ms, "foreign_key_check", || {
            foreign_key_violations(conn)
        })?);
    report.phase_timings_ms = phase_timings_ms;
    enforce_compact_verification(&report)?;
    Ok(report)
}

fn enforce_compact_verification(report: &RebuildReport) -> Result<()> {
    let integrity_ok =
        report.full_integrity_deferred || report.integrity_check.as_deref() == Some("ok");
    if !integrity_ok || report.foreign_key_violations != Some(0) {
        bail!(
            "compact rebuild verification failed: integrity_check={} foreign_key_violations={}",
            report.integrity_check.as_deref().unwrap_or("deferred"),
            report
                .foreign_key_violations
                .map(|count| count.to_string())
                .unwrap_or_else(|| "not_run".to_string())
        );
    }
    Ok(())
}

fn timed<T>(
    timings: &mut BTreeMap<String, u64>,
    phase: &'static str,
    work: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let started = Instant::now();
    let result = work();
    let elapsed = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    timings.insert(phase.to_string(), elapsed);
    result
}

fn create_schema_objects(
    conn: &Connection,
    objects: &[SchemaObject],
    kind: SchemaObjectKind,
) -> Result<u64> {
    let mut created = 0u64;
    for object in objects.iter().filter(|object| object.kind == kind) {
        let sql = qualify_for_schema(object, COMPACT_SCHEMA)
            .with_context(|| format!("failed qualifying schema object {}", object.name))?;
        conn.execute_batch(&sql)
            .with_context(|| format!("failed creating compact schema object {}", object.name))?;
        created = created.saturating_add(1);
    }
    Ok(created)
}

fn copy_tables(
    conn: &Connection,
    objects: &[SchemaObject],
    observed_cutoff: DateTime<Utc>,
    canary_cutoff: DateTime<Utc>,
) -> Result<RebuildReport> {
    let mut report = RebuildReport {
        tables_copied: 0,
        rows_copied: 0,
        observed_rows_copied: 0,
        sol_leg_rows_copied: 0,
        canary_rows_copied: 0,
        indexes_created: 0,
        triggers_created: 0,
        views_created: 0,
        integrity_check: None,
        foreign_key_violations: None,
        full_integrity_deferred: false,
        cheap_checks: None,
        phase_timings_ms: BTreeMap::new(),
    };
    for object in objects
        .iter()
        .filter(|object| object.kind == SchemaObjectKind::Table)
    {
        let plan = copy_plan_for_table(&object.name, observed_cutoff, canary_cutoff);
        let copied = copy_table(conn, &object.name, plan)
            .with_context(|| format!("failed copying compact table {}", object.name))?;
        report.tables_copied = report.tables_copied.saturating_add(1);
        report.rows_copied = report.rows_copied.saturating_add(copied);
        match object.name.as_str() {
            OBSERVED_SWAPS => report.observed_rows_copied = copied,
            OBSERVED_SOL_LEG_SWAPS => report.sol_leg_rows_copied = copied,
            CANARY_EVENTS | CANARY_PROVIDER_SAMPLES | CANARY_SHADOW_GATE => {
                report.canary_rows_copied = report.canary_rows_copied.saturating_add(copied);
            }
            _ => {}
        }
    }
    Ok(report)
}

fn copy_plan_for_table<'a>(
    table: &str,
    observed_cutoff: DateTime<Utc>,
    canary_cutoff: DateTime<Utc>,
) -> CopyPlan<'a> {
    match table {
        OBSERVED_SWAPS | OBSERVED_SOL_LEG_SWAPS => CopyPlan::Since {
            ts_column: "ts",
            cutoff: observed_cutoff,
        },
        OBSERVED_SOL_LEG_COVERAGE => CopyPlan::SolLegCoverage { observed_cutoff },
        CANARY_EVENTS | CANARY_PROVIDER_SAMPLES => CopyPlan::Since {
            ts_column: "request_ts",
            cutoff: canary_cutoff,
        },
        CANARY_SHADOW_GATE => CopyPlan::Since {
            ts_column: "recorded_ts",
            cutoff: canary_cutoff,
        },
        _ => CopyPlan::All,
    }
}

fn copy_table(conn: &Connection, table: &str, plan: CopyPlan<'_>) -> Result<u64> {
    match plan {
        CopyPlan::All => copy_table_all(conn, table),
        CopyPlan::Since { ts_column, cutoff } => copy_table_since(conn, table, ts_column, cutoff),
        CopyPlan::SolLegCoverage { observed_cutoff } => {
            copy_sol_leg_coverage(conn, observed_cutoff)
        }
    }
}

fn copy_table_all(conn: &Connection, table: &str) -> Result<u64> {
    let sql = format!(
        "INSERT INTO {}.{} SELECT * FROM main.{}",
        quote_ident(COMPACT_SCHEMA),
        quote_ident(table),
        quote_ident(table)
    );
    Ok(conn.execute(&sql, [])? as u64)
}

fn copy_table_since(
    conn: &Connection,
    table: &str,
    ts_column: &str,
    cutoff: DateTime<Utc>,
) -> Result<u64> {
    let sql = format!(
        "INSERT INTO {}.{} SELECT * FROM main.{} WHERE {} >= ?1",
        quote_ident(COMPACT_SCHEMA),
        quote_ident(table),
        quote_ident(table),
        quote_ident(ts_column)
    );
    Ok(conn.execute(&sql, params![cutoff.to_rfc3339()])? as u64)
}

fn copy_sol_leg_coverage(conn: &Connection, observed_cutoff: DateTime<Utc>) -> Result<u64> {
    let compact = quote_ident(COMPACT_SCHEMA);
    let coverage = quote_ident(OBSERVED_SOL_LEG_COVERAGE);
    let sol_leg = quote_ident(OBSERVED_SOL_LEG_SWAPS);
    let sql = format!(
        "INSERT INTO {compact}.{coverage}(id, covered_from_ts, covered_through_ts, updated_at)
         SELECT
            1,
            ?1,
            COALESCE((SELECT MAX(ts) FROM {compact}.{sol_leg}), ?1),
            strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')"
    );
    Ok(conn.execute(&sql, params![observed_cutoff.to_rfc3339()])? as u64)
}

fn copy_sqlite_sequence(conn: &Connection) -> Result<()> {
    let exists: i64 = conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM main.sqlite_schema
            WHERE type='table' AND name='sqlite_sequence'
         )",
        [],
        |row| row.get(0),
    )?;
    if exists == 0 {
        return Ok(());
    }
    conn.execute_batch(&format!(
        "DELETE FROM {}.sqlite_sequence;
         INSERT INTO {}.sqlite_sequence(name, seq)
         SELECT name, seq FROM main.sqlite_sequence;",
        quote_ident(COMPACT_SCHEMA),
        quote_ident(COMPACT_SCHEMA)
    ))
    .context("failed copying sqlite_sequence")?;
    Ok(())
}

fn create_compact_canary_ts_indexes(conn: &Connection) -> Result<u64> {
    let mut created = 0u64;
    for (table, index, column) in [
        (
            CANARY_EVENTS,
            "idx_execution_quote_canary_events_request_ts_only",
            "request_ts",
        ),
        (
            CANARY_PROVIDER_SAMPLES,
            "idx_execution_quote_canary_provider_samples_request_ts_only",
            "request_ts",
        ),
        (
            CANARY_SHADOW_GATE,
            "idx_execution_quote_canary_shadow_gate_recorded_ts_only",
            "recorded_ts",
        ),
    ] {
        if !compact_table_exists(conn, table)? {
            continue;
        }
        let sql = format!(
            "CREATE INDEX IF NOT EXISTS {}.{} ON {}({})",
            quote_ident(COMPACT_SCHEMA),
            quote_ident(index),
            quote_ident(table),
            quote_ident(column)
        );
        conn.execute_batch(&sql)
            .with_context(|| format!("failed creating compact canary index {index}"))?;
        created = created.saturating_add(1);
    }
    Ok(created)
}

fn compact_table_exists(conn: &Connection, table: &str) -> Result<bool> {
    let exists: i64 = conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM compact.sqlite_schema
            WHERE type='table' AND name=?1
         )",
        [table],
        |row| row.get(0),
    )?;
    Ok(exists != 0)
}

fn set_output_pragmas(conn: &Connection) -> Result<()> {
    let user_version: i64 = conn.query_row("PRAGMA main.user_version", [], |row| row.get(0))?;
    let application_id: i64 = conn.query_row("PRAGMA main.application_id", [], |row| row.get(0))?;
    conn.execute_batch(&format!(
        "PRAGMA {}.user_version = {};
         PRAGMA {}.application_id = {};
         PRAGMA {}.journal_mode = WAL;",
        quote_ident(COMPACT_SCHEMA),
        user_version,
        quote_ident(COMPACT_SCHEMA),
        application_id,
        quote_ident(COMPACT_SCHEMA)
    ))
    .context("failed setting compact output pragmas")?;
    Ok(())
}

fn integrity_check(conn: &Connection) -> Result<String> {
    let sql = format!("PRAGMA {}.integrity_check", quote_ident(COMPACT_SCHEMA));
    conn.query_row(&sql, [], |row| row.get(0))
        .context("failed running compact integrity_check")
}

fn foreign_key_violations(conn: &Connection) -> Result<u64> {
    let sql = format!("PRAGMA {}.foreign_key_check", quote_ident(COMPACT_SCHEMA));
    let mut stmt = conn
        .prepare(&sql)
        .context("failed preparing compact foreign_key_check")?;
    let mut rows = stmt.query([])?;
    let mut count = 0u64;
    while rows.next()?.is_some() {
        count = count.saturating_add(1);
    }
    Ok(count)
}

fn attach_output(conn: &Connection, output_path: &Path) -> Result<()> {
    conn.execute_batch(&format!(
        "ATTACH DATABASE '{}' AS compact",
        sql_literal_path(output_path)
    ))
    .with_context(|| format!("failed attaching compact output {}", output_path.display()))?;
    Ok(())
}

fn sql_literal_path(path: &Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}
