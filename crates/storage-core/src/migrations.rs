use crate::{
    observed_sol_leg_projection::{
        observed_sol_leg_projection_covering_index_is_valid, PROJECTION_COVERING_INDEX,
    },
    observed_timestamp::observed_swaps_non_utc_timestamp_index_is_valid,
    report_startup_step_progress, run_observed_startup_step,
    schema_indexes::{observed_swaps_read_index_is_valid, observed_swaps_sol_leg_index_is_valid},
    SqliteDiscoveryStore, SqliteStartupPolicy, StartupStepOutcome, StartupStepProgressReporter,
};
use anyhow::{anyhow, Context, Result};
use rusqlite::{params, OptionalExtension};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration as StdDuration;

const STARTUP_DEFERRED_MIGRATIONS: &[&str] = &[
    "0003_token_quality_indexes.sql",
    "0007_observed_swaps_time_scan_index.sql",
    "0008_shadow_perf_indexes.sql",
    "0039_observed_swaps_sol_leg_ts_index.sql",
    "0040_observed_swaps_non_utc_ts_index.sql",
    "0043_observed_sol_leg_projection_covering_index.sql",
];

#[derive(Clone, Copy)]
enum StartupMigrationDecision {
    Apply,
    Defer,
    RecordSatisfied,
}

fn satisfied_deferred_migration_decision(recorded: bool) -> StartupMigrationDecision {
    if recorded {
        StartupMigrationDecision::Apply
    } else {
        StartupMigrationDecision::RecordSatisfied
    }
}

pub struct SqliteStartupBootstrapResult {
    pub store: SqliteDiscoveryStore,
    pub applied_migrations: usize,
    pub deferred_migrations: Vec<String>,
}

impl SqliteDiscoveryStore {
    pub fn run_migrations(&mut self, migrations_dir: &Path) -> Result<usize> {
        if !migrations_dir.exists() {
            return Err(anyhow!(
                "migrations directory not found: {}",
                migrations_dir.display()
            ));
        }
        let mut files = read_migration_files(migrations_dir)?;
        files.sort();
        self.run_migrations_from_sorted_files(&files)
    }

    pub fn open_and_migrate_for_startup(
        path: &Path,
        migrations_dir: &Path,
        policy: &SqliteStartupPolicy,
        reporter: Option<&StartupStepProgressReporter>,
    ) -> Result<SqliteStartupBootstrapResult> {
        let sqlite_path = path.to_path_buf();
        let store = run_observed_startup_step(
            "sqlite_open_connection",
            policy.open_step,
            reporter,
            move || SqliteDiscoveryStore::open(&sqlite_path),
        )?;
        let migrations_dir = migrations_dir.to_path_buf();
        let (mut store, files) = run_observed_startup_step(
            "sqlite_migrations_scan",
            policy.migrations_scan_step,
            reporter,
            move || -> Result<(SqliteDiscoveryStore, Vec<PathBuf>)> {
                if !migrations_dir.exists() {
                    return Err(anyhow!(
                        "migrations directory not found: {}",
                        migrations_dir.display()
                    ));
                }
                let mut files = read_migration_files(&migrations_dir)?;
                files.sort();
                Ok((store, files))
            },
        )?;
        let (files, deferred_migrations) = store.split_startup_migration_files(files)?;
        report_deferred_startup_migrations(reporter, &deferred_migrations);
        let (store, applied_migrations) = run_observed_startup_step(
            "sqlite_migrations_apply",
            policy.migrations_apply_step,
            reporter,
            move || -> Result<(SqliteDiscoveryStore, usize)> {
                let applied = store.run_migrations_from_sorted_files(&files)?;
                Ok((store, applied))
            },
        )?;
        Ok(SqliteStartupBootstrapResult {
            store,
            applied_migrations,
            deferred_migrations,
        })
    }

    fn split_startup_migration_files(
        &self,
        files: Vec<PathBuf>,
    ) -> Result<(Vec<PathBuf>, Vec<String>)> {
        let mut blocking = Vec::new();
        let mut deferred = Vec::new();
        for path in files {
            let Some(version) = path.file_name().and_then(|name| name.to_str()) else {
                blocking.push(path);
                continue;
            };
            if STARTUP_DEFERRED_MIGRATIONS.contains(&version) {
                match self.startup_deferred_migration_decision(version)? {
                    StartupMigrationDecision::Apply => blocking.push(path),
                    StartupMigrationDecision::Defer => deferred.push(version.to_string()),
                    StartupMigrationDecision::RecordSatisfied => {
                        self.record_satisfied_deferred_migration(version)?;
                    }
                }
                continue;
            }
            blocking.push(path);
        }
        Ok((blocking, deferred))
    }

    fn startup_deferred_migration_decision(
        &self,
        version: &str,
    ) -> Result<StartupMigrationDecision> {
        let recorded = self.sqlite_migration_version_recorded(version)?;
        match version {
            "0003_token_quality_indexes.sql" => self
                .observed_swaps_read_indexes_migration_decision(
                    &[
                        "idx_observed_swaps_token_in_ts",
                        "idx_observed_swaps_token_out_ts",
                    ],
                    recorded,
                ),
            "0007_observed_swaps_time_scan_index.sql" => self
                .observed_swaps_read_indexes_migration_decision(
                    &["idx_observed_swaps_ts_slot_signature"],
                    recorded,
                ),
            "0008_shadow_perf_indexes.sql" => self.shadow_perf_indexes_migration_decision(recorded),
            "0039_observed_swaps_sol_leg_ts_index.sql" => {
                if observed_swaps_sol_leg_index_is_valid(self)? {
                    Ok(satisfied_deferred_migration_decision(recorded))
                } else if recorded || self.sqlite_table_has_rows("observed_swaps")? {
                    Ok(StartupMigrationDecision::Defer)
                } else {
                    self.drop_sqlite_indexes(&["idx_observed_swaps_sol_leg_ts_slot_signature"])?;
                    Ok(StartupMigrationDecision::Apply)
                }
            }
            "0040_observed_swaps_non_utc_ts_index.sql" => {
                if observed_swaps_non_utc_timestamp_index_is_valid(self)? {
                    Ok(satisfied_deferred_migration_decision(recorded))
                } else if recorded || self.sqlite_table_has_rows("observed_swaps")? {
                    Ok(StartupMigrationDecision::Defer)
                } else {
                    Ok(StartupMigrationDecision::Apply)
                }
            }
            "0043_observed_sol_leg_projection_covering_index.sql" => {
                if observed_sol_leg_projection_covering_index_is_valid(self)? {
                    Ok(satisfied_deferred_migration_decision(recorded))
                } else if recorded || self.sqlite_table_has_rows("observed_sol_leg_swaps")? {
                    Ok(StartupMigrationDecision::Defer)
                } else {
                    self.drop_sqlite_indexes(&[PROJECTION_COVERING_INDEX])?;
                    Ok(StartupMigrationDecision::Apply)
                }
            }
            _ => Ok(StartupMigrationDecision::Apply),
        }
    }

    fn observed_swaps_read_indexes_migration_decision(
        &self,
        index_names: &[&str],
        recorded: bool,
    ) -> Result<StartupMigrationDecision> {
        if !self.sqlite_table_exists("observed_swaps")? {
            return Ok(StartupMigrationDecision::Apply);
        }
        let all_valid = index_names
            .iter()
            .map(|index_name| observed_swaps_read_index_is_valid(self, index_name))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .all(|valid| valid);
        if all_valid {
            return Ok(satisfied_deferred_migration_decision(recorded));
        }
        if recorded || self.sqlite_table_has_rows("observed_swaps")? {
            return Ok(StartupMigrationDecision::Defer);
        }
        self.drop_sqlite_indexes(index_names)?;
        Ok(StartupMigrationDecision::Apply)
    }

    fn shadow_perf_indexes_migration_decision(
        &self,
        recorded: bool,
    ) -> Result<StartupMigrationDecision> {
        let observed_pair_decision = self.observed_swaps_read_indexes_migration_decision(
            &[
                "idx_observed_swaps_token_in_out_ts",
                "idx_observed_swaps_token_out_in_ts",
            ],
            recorded,
        )?;
        let shadow_decision = self.sqlite_index_migration_decision(
            "shadow_closed_trades",
            "idx_shadow_closed_trades_wallet_closed_ts",
            &["wallet_id", "closed_ts"],
            recorded,
        )?;
        if matches!(observed_pair_decision, StartupMigrationDecision::Defer)
            || matches!(shadow_decision, StartupMigrationDecision::Defer)
        {
            return Ok(StartupMigrationDecision::Defer);
        }
        if matches!(
            observed_pair_decision,
            StartupMigrationDecision::RecordSatisfied
        ) && matches!(shadow_decision, StartupMigrationDecision::RecordSatisfied)
        {
            return Ok(StartupMigrationDecision::RecordSatisfied);
        }
        if matches!(observed_pair_decision, StartupMigrationDecision::Apply)
            && matches!(shadow_decision, StartupMigrationDecision::Apply)
        {
            Ok(StartupMigrationDecision::Apply)
        } else {
            Ok(StartupMigrationDecision::Defer)
        }
    }

    fn sqlite_index_migration_decision(
        &self,
        table: &str,
        index_name: &str,
        expected_columns: &[&str],
        recorded: bool,
    ) -> Result<StartupMigrationDecision> {
        if !self.sqlite_table_exists(table)? {
            return Ok(StartupMigrationDecision::Apply);
        }
        if self.sqlite_index_is_valid(table, index_name, expected_columns)? {
            return Ok(satisfied_deferred_migration_decision(recorded));
        }
        if recorded || self.sqlite_table_has_rows(table)? {
            return Ok(StartupMigrationDecision::Defer);
        }
        self.drop_sqlite_indexes(&[index_name])?;
        Ok(StartupMigrationDecision::Apply)
    }

    fn run_migrations_from_sorted_files(&mut self, files: &[PathBuf]) -> Result<usize> {
        let tx = self
            .conn
            .transaction()
            .context("failed to open sqlite migration transaction")?;
        let mut applied = 0usize;
        for path in files {
            let version = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| anyhow!("invalid migration filename: {}", path.display()))?;
            let already_applied: Option<String> = tx
                .query_row(
                    "SELECT version FROM schema_migrations WHERE version = ?1",
                    params![version],
                    |row| row.get(0),
                )
                .optional()
                .with_context(|| format!("failed checking migration {version}"))?;
            if already_applied.is_some() {
                continue;
            }
            let sql = fs::read_to_string(path)
                .with_context(|| format!("failed reading migration file {}", path.display()))?;
            tx.execute_batch(&sql)
                .with_context(|| format!("failed applying migration {version}"))?;
            tx.execute(
                "INSERT INTO schema_migrations(version, applied_at) VALUES (?1, datetime('now'))",
                params![version],
            )
            .with_context(|| format!("failed recording migration {version}"))?;
            applied += 1;
            tracing::info!(version = version, "migration applied");
        }
        tx.commit().context("failed to commit migrations")?;
        Ok(applied)
    }
}

fn read_migration_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let entries = fs::read_dir(dir)
        .with_context(|| format!("failed to read migrations dir {}", dir.display()))?;
    let mut files = Vec::new();
    for entry in entries {
        let entry = entry.with_context(|| format!("failed to read entry in {}", dir.display()))?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("sql") {
            files.push(path);
        }
    }
    Ok(files)
}

fn report_deferred_startup_migrations(
    reporter: Option<&StartupStepProgressReporter>,
    deferred_versions: &[String],
) {
    report_startup_step_progress(
        reporter,
        "sqlite_migrations_deferred",
        StartupStepOutcome::Started,
        StdDuration::ZERO,
        None,
        None,
    );
    let (outcome, detail) = if deferred_versions.is_empty() {
        (
            StartupStepOutcome::Completed,
            Some("deferred_count=0".to_string()),
        )
    } else {
        (
            StartupStepOutcome::Skipped,
            Some(format!("deferred_versions={}", deferred_versions.join(","))),
        )
    };
    report_startup_step_progress(
        reporter,
        "sqlite_migrations_deferred",
        outcome,
        StdDuration::ZERO,
        None,
        detail,
    );
}
