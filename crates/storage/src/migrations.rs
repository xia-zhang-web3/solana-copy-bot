use super::{
    report_startup_step_progress, run_observed_startup_step, SqliteStartupBootstrapResult,
    SqliteStartupPolicy, SqliteStore, StartupStepOutcome, StartupStepProgressReporter,
};
use anyhow::{anyhow, Context, Result};
use rusqlite::{params, OptionalExtension};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration as StdDuration;

#[path = "migrations_index_guard.rs"]
mod migrations_index_guard;

const STARTUP_DEFERRED_MIGRATIONS: &[&str] = &[
    "0003_token_quality_indexes.sql",
    "0007_observed_swaps_time_scan_index.sql",
    "0008_shadow_perf_indexes.sql",
    "0039_observed_swaps_sol_leg_ts_index.sql",
    "0040_observed_swaps_non_utc_ts_index.sql",
];

const OBSERVED_SWAPS_SOL_LEG_PREDICATE: &str =
    "token_in = 'So11111111111111111111111111111111111111112'
       OR token_out = 'So11111111111111111111111111111111111111112'";

const OBSERVED_SWAPS_NON_UTC_TIMESTAMP_PREDICATE: &str = "NOT (
    (
        (length(ts) = 25 AND substr(ts, 20, 6) = '+00:00')
        OR (
            length(ts) BETWEEN 27 AND 35
            AND substr(ts, 20, 1) = '.'
            AND substr(ts, -6) = '+00:00'
            AND substr(ts, 21, length(ts) - 26) GLOB '[0-9]*'
            AND substr(ts, 21, length(ts) - 26) NOT GLOB '*[^0-9]*'
        )
    )
    AND substr(ts, 5, 1) = '-'
    AND substr(ts, 8, 1) = '-'
    AND substr(ts, 11, 1) = 'T'
    AND substr(ts, 14, 1) = ':'
    AND substr(ts, 17, 1) = ':'
    AND substr(ts, 1, 4) GLOB '[0-9][0-9][0-9][0-9]'
    AND substr(ts, 6, 2) GLOB '[0-9][0-9]'
    AND CAST(substr(ts, 6, 2) AS INTEGER) BETWEEN 1 AND 12
    AND substr(ts, 9, 2) GLOB '[0-9][0-9]'
    AND CAST(substr(ts, 9, 2) AS INTEGER) BETWEEN 1 AND 31
    AND substr(ts, 12, 2) GLOB '[0-9][0-9]'
    AND CAST(substr(ts, 12, 2) AS INTEGER) BETWEEN 0 AND 23
    AND substr(ts, 15, 2) GLOB '[0-9][0-9]'
    AND CAST(substr(ts, 15, 2) AS INTEGER) BETWEEN 0 AND 59
    AND substr(ts, 18, 2) GLOB '[0-9][0-9]'
    AND CAST(substr(ts, 18, 2) AS INTEGER) BETWEEN 0 AND 59
    AND julianday(ts) IS NOT NULL
    AND strftime('%Y-%m-%dT%H:%M:%S', ts) = substr(ts, 1, 19)
)";

#[derive(Clone, Copy)]
enum StartupDeferredMigrationDecision {
    Apply,
    Defer,
    RecordSatisfied,
}

impl SqliteStore {
    pub fn run_migrations(&mut self, migrations_dir: &Path) -> Result<usize> {
        if !migrations_dir.exists() {
            return Err(anyhow!(
                "migrations directory not found: {}",
                migrations_dir.display()
            ));
        }

        let mut files = self.read_migration_files(migrations_dir)?;
        files.sort();
        self.run_migrations_from_sorted_files(&files)
    }

    pub fn open_and_migrate_for_startup(
        path: &Path,
        migrations_dir: &Path,
        policy: &SqliteStartupPolicy,
        reporter: Option<&StartupStepProgressReporter>,
    ) -> Result<SqliteStartupBootstrapResult> {
        let store = Self::open_for_startup(path, policy, reporter)?;
        let migrations_dir = migrations_dir.to_path_buf();
        let (mut store, files) = run_observed_startup_step(
            "sqlite_migrations_scan",
            policy.migrations_scan_step,
            reporter,
            move || -> Result<(SqliteStore, Vec<PathBuf>)> {
                if !migrations_dir.exists() {
                    return Err(anyhow!(
                        "migrations directory not found: {}",
                        migrations_dir.display()
                    ));
                }

                let mut files = store.read_migration_files(&migrations_dir)?;
                files.sort();
                Ok((store, files))
            },
        )?;
        let (files, deferred_migrations) = store.split_startup_migration_files(files)?;
        Self::report_deferred_startup_migrations(reporter, &deferred_migrations);
        let (store, applied_migrations) = run_observed_startup_step(
            "sqlite_migrations_apply",
            policy.migrations_apply_step,
            reporter,
            move || -> Result<(SqliteStore, usize)> {
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
                let recorded = self.sqlite_migration_version_recorded(version)?;
                match self.startup_deferred_migration_decision(version, recorded)? {
                    StartupDeferredMigrationDecision::Apply => {
                        blocking.push(path);
                    }
                    StartupDeferredMigrationDecision::Defer => {
                        deferred.push(version.to_string());
                    }
                    StartupDeferredMigrationDecision::RecordSatisfied => {
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
        recorded: bool,
    ) -> Result<StartupDeferredMigrationDecision> {
        match version {
            "0003_token_quality_indexes.sql" => self.startup_observed_swaps_indexes_decision(
                &[
                    (
                        "idx_observed_swaps_token_in_ts",
                        &["token_in", "ts"][..],
                        false,
                        None,
                    ),
                    (
                        "idx_observed_swaps_token_out_ts",
                        &["token_out", "ts"][..],
                        false,
                        None,
                    ),
                ],
                recorded,
            ),
            "0007_observed_swaps_time_scan_index.sql" => self
                .startup_observed_swaps_indexes_decision(
                    &[(
                        "idx_observed_swaps_ts_slot_signature",
                        &["ts", "slot", "signature"][..],
                        false,
                        None,
                    )],
                    recorded,
                ),
            "0008_shadow_perf_indexes.sql" => {
                let observed_decision = self.startup_observed_swaps_indexes_decision(
                    &[
                        (
                            "idx_observed_swaps_token_in_out_ts",
                            &["token_in", "token_out", "ts"][..],
                            false,
                            None,
                        ),
                        (
                            "idx_observed_swaps_token_out_in_ts",
                            &["token_out", "token_in", "ts"][..],
                            false,
                            None,
                        ),
                    ],
                    recorded,
                )?;
                let shadow_decision = self.startup_sqlite_indexes_decision(
                    "shadow_closed_trades",
                    &[(
                        "idx_shadow_closed_trades_wallet_closed_ts",
                        &["wallet_id", "closed_ts"][..],
                        false,
                        None,
                    )],
                    recorded,
                )?;
                match (observed_decision, shadow_decision) {
                    (
                        StartupDeferredMigrationDecision::RecordSatisfied,
                        StartupDeferredMigrationDecision::RecordSatisfied,
                    ) => Ok(StartupDeferredMigrationDecision::RecordSatisfied),
                    (
                        StartupDeferredMigrationDecision::Apply,
                        StartupDeferredMigrationDecision::Apply,
                    ) => Ok(StartupDeferredMigrationDecision::Apply),
                    _ => Ok(StartupDeferredMigrationDecision::Defer),
                }
            }
            "0039_observed_swaps_sol_leg_ts_index.sql" => self
                .startup_observed_swaps_indexes_decision(
                    &[(
                        "idx_observed_swaps_sol_leg_ts_slot_signature",
                        &["ts", "slot", "signature"][..],
                        true,
                        Some(OBSERVED_SWAPS_SOL_LEG_PREDICATE),
                    )],
                    recorded,
                ),
            "0040_observed_swaps_non_utc_ts_index.sql" => self
                .startup_observed_swaps_indexes_decision(
                    &[(
                        "idx_observed_swaps_non_utc_ts",
                        &["ts"][..],
                        true,
                        Some(OBSERVED_SWAPS_NON_UTC_TIMESTAMP_PREDICATE),
                    )],
                    recorded,
                ),
            _ => Ok(StartupDeferredMigrationDecision::Defer),
        }
    }

    fn startup_observed_swaps_indexes_decision(
        &self,
        indexes: &[(&str, &[&str], bool, Option<&str>)],
        recorded: bool,
    ) -> Result<StartupDeferredMigrationDecision> {
        self.startup_sqlite_indexes_decision("observed_swaps", indexes, recorded)
    }

    fn startup_sqlite_indexes_decision(
        &self,
        table_name: &str,
        indexes: &[(&str, &[&str], bool, Option<&str>)],
        recorded: bool,
    ) -> Result<StartupDeferredMigrationDecision> {
        if !self.sqlite_table_exists(table_name)? {
            return Ok(StartupDeferredMigrationDecision::Apply);
        }
        if self.startup_sqlite_indexes_satisfied(table_name, indexes)? {
            Ok(StartupDeferredMigrationDecision::RecordSatisfied)
        } else if recorded || self.sqlite_table_has_rows(table_name)? {
            Ok(StartupDeferredMigrationDecision::Defer)
        } else {
            self.drop_sqlite_indexes(indexes.iter().map(|(name, _, _, _)| *name))?;
            Ok(StartupDeferredMigrationDecision::Apply)
        }
    }

    fn startup_sqlite_indexes_satisfied(
        &self,
        table_name: &str,
        indexes: &[(&str, &[&str], bool, Option<&str>)],
    ) -> Result<bool> {
        if !self.sqlite_table_exists(table_name)? {
            return Ok(false);
        }
        for (index_name, columns, partial, predicate) in indexes {
            if !self.sqlite_index_valid(table_name, index_name, columns, *partial, *predicate)? {
                return Ok(false);
            }
        }
        Ok(true)
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

    fn record_satisfied_deferred_migration(&self, version: &str) -> Result<()> {
        self.conn
            .execute(
                "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?1, datetime('now'))",
                params![version],
            )
            .with_context(|| format!("failed recording satisfied deferred migration {version}"))?;
        tracing::info!(version = version, "deferred migration already satisfied");
        Ok(())
    }

    fn read_migration_files(&self, dir: &Path) -> Result<Vec<PathBuf>> {
        let entries = fs::read_dir(dir)
            .with_context(|| format!("failed to read migrations dir {}", dir.display()))?;
        let mut files = Vec::new();

        for entry in entries {
            let entry =
                entry.with_context(|| format!("failed to read entry in {}", dir.display()))?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("sql") {
                files.push(path);
            }
        }

        Ok(files)
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
                .with_context(|| format!("failed checking migration {}", version))?;

            if already_applied.is_some() {
                continue;
            }

            let sql = fs::read_to_string(path)
                .with_context(|| format!("failed reading migration file {}", path.display()))?;
            tx.execute_batch(&sql)
                .with_context(|| format!("failed applying migration {}", version))?;
            tx.execute(
                "INSERT INTO schema_migrations(version, applied_at) VALUES (?1, datetime('now'))",
                params![version],
            )
            .with_context(|| format!("failed recording migration {}", version))?;

            applied += 1;
            tracing::info!(version = version, "migration applied");
        }

        tx.commit().context("failed to commit migrations")?;
        Ok(applied)
    }
}
