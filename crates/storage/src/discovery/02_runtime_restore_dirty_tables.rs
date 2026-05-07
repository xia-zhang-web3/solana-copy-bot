use super::RuntimeArtifactRestoreDirtyTable;
use anyhow::{Context, Result};
use rusqlite::Connection;

fn runtime_artifact_restore_table_category(table: &str) -> &'static str {
    match table {
        "risk_events" => "risk gating",
        "wallets"
        | "wallet_metrics"
        | "followlist"
        | "observed_swaps"
        | "discovery_strategy_state"
        | "discovery_runtime_state"
        | "discovery_recent_raw_restore_state"
        | "discovery_persisted_rebuild_state"
        | "trusted_wallet_metrics_snapshots"
        | "wallet_activity_days" => "discovery runtime",
        "positions" | "trades" | "execution_orders" | "copy_signals" => "execution runtime",
        "system_heartbeat" | "alert_delivery_state" => "runtime sidecar",
        _ if table.starts_with("shadow_") => "shadow accounting",
        _ => "durable runtime state",
    }
}

fn quote_sql_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

pub(crate) fn runtime_artifact_restore_dirty_tables_on_conn(
    conn: &Connection,
) -> Result<Vec<RuntimeArtifactRestoreDirtyTable>> {
    let mut stmt = conn
        .prepare(
            "SELECT name
             FROM sqlite_master
             WHERE type = 'table'
               AND name NOT LIKE 'sqlite_%'
               AND name <> 'schema_migrations'
             ORDER BY name ASC",
        )
        .context("failed to prepare runtime artifact restore table inventory query")?;
    let table_names = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .context("failed to query runtime artifact restore table inventory")?;

    let mut dirty_tables = Vec::new();
    for table_name in table_names {
        let table_name = table_name
            .context("failed reading sqlite_master.name during runtime restore preflight")?;
        let has_rows: i64 = conn
            .query_row(
                &format!(
                    "SELECT EXISTS(SELECT 1 FROM {} LIMIT 1)",
                    quote_sql_identifier(&table_name)
                ),
                [],
                |row| row.get(0),
            )
            .with_context(|| {
                format!(
                    "failed checking table {table_name} during runtime artifact restore preflight"
                )
            })?;
        if has_rows != 0 {
            dirty_tables.push(RuntimeArtifactRestoreDirtyTable {
                table: table_name.clone(),
                category: runtime_artifact_restore_table_category(&table_name),
            });
        }
    }

    Ok(dirty_tables)
}

pub(crate) fn format_runtime_artifact_restore_dirty_tables(
    dirty_tables: &[RuntimeArtifactRestoreDirtyTable],
) -> String {
    dirty_tables
        .iter()
        .map(|entry| format!("{} ({})", entry.table, entry.category))
        .collect::<Vec<_>>()
        .join(", ")
}
