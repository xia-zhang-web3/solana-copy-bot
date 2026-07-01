use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};

use super::schema_sql::quote_ident;

const COMPACT_SCHEMA: &str = "compact";
const OBSERVED_SWAPS: &str = "observed_swaps";
const OBSERVED_SOL_LEG_SWAPS: &str = "observed_sol_leg_swaps";
const OBSERVED_SOL_LEG_COVERAGE: &str = "observed_sol_leg_coverage";
const CANARY_EVENTS: &str = "execution_quote_canary_events";
const CANARY_PROVIDER_SAMPLES: &str = "execution_quote_canary_provider_samples";
const CANARY_SHADOW_GATE: &str = "execution_quote_canary_shadow_gate_events";
const COMPACT_EXTRA_INDEXES: &[&str] = &[
    "idx_execution_quote_canary_events_request_ts_only",
    "idx_execution_quote_canary_provider_samples_request_ts_only",
    "idx_execution_quote_canary_shadow_gate_recorded_ts_only",
];

#[derive(Debug, Clone, Default)]
pub(super) struct RebuildCheapCheckReport {
    pub(super) quick_check: String,
    pub(super) tables_checked: u64,
    pub(super) row_count_mismatches: u64,
    pub(super) schema_mismatches: u64,
    pub(super) critical_empty_tables: Vec<String>,
}

impl RebuildCheapCheckReport {
    pub(super) fn enforce(&self) -> Result<()> {
        if self.quick_check != "ok"
            || self.row_count_mismatches != 0
            || self.schema_mismatches != 0
            || !self.critical_empty_tables.is_empty()
        {
            bail!(
                "compact rebuild cheap checks failed: quick_check={} row_count_mismatches={} schema_mismatches={} critical_empty_tables={:?}",
                self.quick_check,
                self.row_count_mismatches,
                self.schema_mismatches,
                self.critical_empty_tables
            );
        }
        Ok(())
    }
}

pub(super) fn run_rebuild_cheap_checks(
    conn: &Connection,
    observed_cutoff: DateTime<Utc>,
    canary_cutoff: DateTime<Utc>,
) -> Result<RebuildCheapCheckReport> {
    let mut report = RebuildCheapCheckReport {
        quick_check: quick_check(conn)?,
        ..RebuildCheapCheckReport::default()
    };
    report.schema_mismatches = schema_mismatch_count(conn)?;
    reconcile_table_counts(conn, observed_cutoff, canary_cutoff, &mut report)?;
    enforce_retention_bounds(conn, OBSERVED_SWAPS, "ts", observed_cutoff)?;
    enforce_retention_bounds(conn, OBSERVED_SOL_LEG_SWAPS, "ts", observed_cutoff)?;
    enforce_retention_bounds(conn, CANARY_EVENTS, "request_ts", canary_cutoff)?;
    enforce_retention_bounds(conn, CANARY_PROVIDER_SAMPLES, "request_ts", canary_cutoff)?;
    enforce_retention_bounds(conn, CANARY_SHADOW_GATE, "recorded_ts", canary_cutoff)?;
    collect_critical_empty_tables(conn, &mut report)?;
    Ok(report)
}

fn quick_check(conn: &Connection) -> Result<String> {
    let sql = format!("PRAGMA {}.quick_check", quote_ident(COMPACT_SCHEMA));
    conn.query_row(&sql, [], |row| row.get(0))
        .context("failed running compact quick_check")
}

fn schema_mismatch_count(conn: &Connection) -> Result<u64> {
    let mut mismatches = schema_name_diff_count(conn, "main", COMPACT_SCHEMA, false)?;
    mismatches =
        mismatches.saturating_add(schema_name_diff_count(conn, COMPACT_SCHEMA, "main", true)?);
    Ok(mismatches)
}

fn schema_name_diff_count(
    conn: &Connection,
    left: &str,
    right: &str,
    allow_compact_extras: bool,
) -> Result<u64> {
    let allowed_filter = if allow_compact_extras {
        let names = COMPACT_EXTRA_INDEXES
            .iter()
            .map(|name| format!("'{name}'"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("AND NOT (type = 'index' AND name IN ({names}))")
    } else {
        String::new()
    };
    let sql = format!(
        "SELECT COUNT(*)
         FROM (
            SELECT type, name FROM {}.sqlite_schema
            WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'
              {allowed_filter}
            EXCEPT
            SELECT type, name FROM {}.sqlite_schema
            WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'
         )",
        quote_ident(left),
        quote_ident(right)
    );
    conn.query_row(&sql, [], |row| row.get::<_, u64>(0))
        .with_context(|| format!("failed comparing schema object names {left}->{right}"))
}

fn reconcile_table_counts(
    conn: &Connection,
    observed_cutoff: DateTime<Utc>,
    canary_cutoff: DateTime<Utc>,
    report: &mut RebuildCheapCheckReport,
) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT name
         FROM main.sqlite_schema
         WHERE type = 'table'
           AND sql IS NOT NULL
           AND name NOT LIKE 'sqlite_%'
         ORDER BY name",
    )?;
    let names = stmt.query_map([], |row| row.get::<_, String>(0))?;
    for name in names {
        let table = name?;
        let source = expected_source_count(conn, &table, observed_cutoff, canary_cutoff)?;
        let compact = compact_table_count(conn, &table)?;
        report.tables_checked = report.tables_checked.saturating_add(1);
        if source != compact {
            report.row_count_mismatches = report.row_count_mismatches.saturating_add(1);
        }
    }
    Ok(())
}

fn expected_source_count(
    conn: &Connection,
    table: &str,
    observed_cutoff: DateTime<Utc>,
    canary_cutoff: DateTime<Utc>,
) -> Result<u64> {
    match table {
        OBSERVED_SWAPS | OBSERVED_SOL_LEG_SWAPS => {
            filtered_count(conn, "main", table, "ts", observed_cutoff)
        }
        OBSERVED_SOL_LEG_COVERAGE => Ok(1),
        CANARY_EVENTS | CANARY_PROVIDER_SAMPLES => {
            filtered_count(conn, "main", table, "request_ts", canary_cutoff)
        }
        CANARY_SHADOW_GATE => filtered_count(conn, "main", table, "recorded_ts", canary_cutoff),
        _ => table_count(conn, "main", table),
    }
}

fn compact_table_count(conn: &Connection, table: &str) -> Result<u64> {
    table_count(conn, COMPACT_SCHEMA, table)
}

fn table_count(conn: &Connection, schema: &str, table: &str) -> Result<u64> {
    let sql = format!(
        "SELECT COUNT(*) FROM {}.{}",
        quote_ident(schema),
        quote_ident(table)
    );
    conn.query_row(&sql, [], |row| row.get::<_, u64>(0))
        .with_context(|| format!("failed counting {schema}.{table}"))
}

fn filtered_count(
    conn: &Connection,
    schema: &str,
    table: &str,
    ts_column: &str,
    cutoff: DateTime<Utc>,
) -> Result<u64> {
    let sql = format!(
        "SELECT COUNT(*) FROM {}.{} WHERE {} >= ?1",
        quote_ident(schema),
        quote_ident(table),
        quote_ident(ts_column)
    );
    conn.query_row(&sql, params![cutoff.to_rfc3339()], |row| {
        row.get::<_, u64>(0)
    })
    .with_context(|| format!("failed counting retained rows for {schema}.{table}"))
}

fn enforce_retention_bounds(
    conn: &Connection,
    table: &str,
    ts_column: &str,
    cutoff: DateTime<Utc>,
) -> Result<()> {
    if !compact_table_exists(conn, table)? {
        return Ok(());
    }
    let sql = format!(
        "SELECT MIN({}), MAX({}), COUNT(*) FROM {}.{}",
        quote_ident(ts_column),
        quote_ident(ts_column),
        quote_ident(COMPACT_SCHEMA),
        quote_ident(table)
    );
    let (min_ts, _max_ts, count): (Option<String>, Option<String>, u64) =
        conn.query_row(&sql, [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
    if count == 0 {
        return Ok(());
    }
    let Some(min_ts) = min_ts else {
        bail!("compact retention bounds missing min timestamp for {table}");
    };
    let parsed = DateTime::parse_from_rfc3339(&min_ts)
        .with_context(|| format!("failed parsing compact retention min timestamp for {table}"))?
        .with_timezone(&Utc);
    if parsed < cutoff {
        bail!("compact retention bounds violated for {table}: min_ts={min_ts}");
    }
    Ok(())
}

fn collect_critical_empty_tables(
    conn: &Connection,
    report: &mut RebuildCheapCheckReport,
) -> Result<()> {
    for table in ["followlist", "wallet_metrics"] {
        if compact_table_exists(conn, table)? && compact_table_count(conn, table)? == 0 {
            report.critical_empty_tables.push(table.to_string());
        }
    }
    if compact_table_exists(conn, "shadow_closed_trades")?
        && table_count(conn, "main", "shadow_closed_trades")? > 0
        && compact_table_count(conn, "shadow_closed_trades")? == 0
    {
        report
            .critical_empty_tables
            .push("shadow_closed_trades".to_string());
    }
    Ok(())
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
