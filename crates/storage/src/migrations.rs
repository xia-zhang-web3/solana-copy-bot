use super::{
    report_startup_step_progress, run_observed_startup_step, SqliteStartupBootstrapResult,
    SqliteStartupPolicy, SqliteStore, StartupStepOutcome, StartupStepProgressReporter,
};
use anyhow::{anyhow, Context, Result};
use rusqlite::{params, OptionalExtension};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration as StdDuration;

const STARTUP_DEFERRED_MIGRATIONS: &[&str] = &["0039_observed_swaps_sol_leg_ts_index.sql"];

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
            if STARTUP_DEFERRED_MIGRATIONS.contains(&version)
                && self.startup_deferred_migration_still_pending(version)?
            {
                deferred.push(version.to_string());
            } else {
                blocking.push(path);
            }
        }

        Ok((blocking, deferred))
    }

    fn startup_deferred_migration_still_pending(&self, version: &str) -> Result<bool> {
        if self.sqlite_migration_version_recorded(version)? {
            return Ok(false);
        }
        match version {
            "0039_observed_swaps_sol_leg_ts_index.sql" => Ok(!self
                .startup_sqlite_index_exists("idx_observed_swaps_sol_leg_ts_slot_signature")?),
            _ => Ok(true),
        }
    }

    fn sqlite_migration_version_recorded(&self, version: &str) -> Result<bool> {
        Ok(self
            .conn
            .query_row(
                "SELECT 1
                 FROM schema_migrations
                 WHERE version = ?1
                 LIMIT 1",
                params![version],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .with_context(|| format!("failed checking schema_migrations for {version}"))?
            .is_some())
    }

    fn startup_sqlite_index_exists(&self, index_name: &str) -> Result<bool> {
        Ok(self
            .conn
            .query_row(
                "SELECT 1
                 FROM sqlite_master
                 WHERE type = 'index'
                   AND name = ?1
                 LIMIT 1",
                params![index_name],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .with_context(|| format!("failed checking sqlite index presence for {index_name}"))?
            .is_some())
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
