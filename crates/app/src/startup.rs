use anyhow::{Context, Result};
use copybot_storage::{
    report_startup_step_progress, SqliteStartupPolicy, SqliteStore, StartupStepOutcome,
    StartupStepProgress, StartupStepProgressReporter, StartupStepRuntimePolicy,
    StartupStepTimeoutBehavior, SQLITE_DEFAULT_WAL_AUTOCHECKPOINT_PAGES,
    SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_THRESHOLD_BYTES,
};
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant as StdInstant};
use tracing::{info, warn};

const STARTUP_STEP_LOG_INTERVAL: StdDuration = StdDuration::from_secs(5);
const STARTUP_REQUIRED_STEP_TIMEOUT: StdDuration = StdDuration::from_secs(120);
pub(crate) const STARTUP_SQLITE_AUX_STEP_TIMEOUT: StdDuration = StdDuration::from_secs(30);
pub(crate) const STARTUP_LARGE_WAL_CHECKPOINT_TIMEOUT: StdDuration =
    StdDuration::from_secs(15 * 60);
pub(crate) const STARTUP_WAL_CHECKPOINT_DEFER_REASON: &str = "deferred_off_startup_critical_path";

pub(crate) fn startup_step_policy(timeout: StdDuration) -> StartupStepRuntimePolicy {
    StartupStepRuntimePolicy::new(STARTUP_STEP_LOG_INTERVAL, Some(timeout))
        .with_timeout_behavior(StartupStepTimeoutBehavior::AbortProcess)
}

pub(crate) fn sqlite_startup_policy() -> SqliteStartupPolicy {
    SqliteStartupPolicy {
        open_step: startup_step_policy(STARTUP_REQUIRED_STEP_TIMEOUT),
        pragma_step: startup_step_policy(STARTUP_SQLITE_AUX_STEP_TIMEOUT),
        large_wal_checkpoint_step: startup_step_policy(STARTUP_LARGE_WAL_CHECKPOINT_TIMEOUT),
        schema_bootstrap_step: startup_step_policy(STARTUP_SQLITE_AUX_STEP_TIMEOUT),
        migrations_scan_step: startup_step_policy(STARTUP_SQLITE_AUX_STEP_TIMEOUT),
        migrations_apply_step: startup_step_policy(STARTUP_REQUIRED_STEP_TIMEOUT),
        large_wal_checkpoint_threshold_bytes: SQLITE_STARTUP_LARGE_WAL_CHECKPOINT_THRESHOLD_BYTES,
    }
}

pub(crate) fn build_startup_progress_reporter() -> StartupStepProgressReporter {
    Arc::new(|event| log_startup_progress_event(&event))
}

fn log_startup_progress_event(event: &StartupStepProgress) {
    match event.outcome {
        StartupStepOutcome::Started | StartupStepOutcome::Completed => {
            info!(
                startup_stage = event.stage,
                startup_stage_outcome = event.outcome.as_str(),
                startup_stage_elapsed_ms = event.elapsed_ms,
                startup_stage_budget_ms = event.budget_ms,
                detail = event.detail.as_deref(),
                "startup progress"
            );
        }
        StartupStepOutcome::Waiting | StartupStepOutcome::Skipped => {
            warn!(
                startup_stage = event.stage,
                startup_stage_outcome = event.outcome.as_str(),
                startup_stage_elapsed_ms = event.elapsed_ms,
                startup_stage_budget_ms = event.budget_ms,
                detail = event.detail.as_deref(),
                "startup progress"
            );
        }
        StartupStepOutcome::Failed | StartupStepOutcome::TimedOut => {
            tracing::error!(
                startup_stage = event.stage,
                startup_stage_outcome = event.outcome.as_str(),
                startup_stage_elapsed_ms = event.elapsed_ms,
                startup_stage_budget_ms = event.budget_ms,
                detail = event.detail.as_deref(),
                "startup progress"
            );
        }
    }
}

pub(crate) fn emit_inline_startup_progress(
    reporter: &StartupStepProgressReporter,
    stage: &'static str,
    outcome: StartupStepOutcome,
    started_at: StdInstant,
    budget: Option<StdDuration>,
    detail: Option<String>,
) {
    report_startup_step_progress(
        Some(reporter),
        stage,
        outcome,
        started_at.elapsed(),
        budget,
        detail,
    );
}

pub(crate) fn run_inline_startup_step<T, F>(
    reporter: &StartupStepProgressReporter,
    stage: &'static str,
    budget: Option<StdDuration>,
    operation: F,
) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    let started_at = StdInstant::now();
    emit_inline_startup_progress(
        reporter,
        stage,
        StartupStepOutcome::Started,
        started_at,
        budget,
        None,
    );
    match operation() {
        Ok(value) => {
            emit_inline_startup_progress(
                reporter,
                stage,
                StartupStepOutcome::Completed,
                started_at,
                budget,
                None,
            );
            Ok(value)
        }
        Err(error) => {
            emit_inline_startup_progress(
                reporter,
                stage,
                StartupStepOutcome::Failed,
                started_at,
                budget,
                Some(format!("{error:#}")),
            );
            Err(error).with_context(|| format!("startup step {stage} failed"))
        }
    }
}

pub(crate) fn skip_inline_startup_step(
    reporter: &StartupStepProgressReporter,
    stage: &'static str,
    detail: impl Into<String>,
) {
    let started_at = StdInstant::now();
    emit_inline_startup_progress(
        reporter,
        stage,
        StartupStepOutcome::Started,
        started_at,
        None,
        None,
    );
    emit_inline_startup_progress(
        reporter,
        stage,
        StartupStepOutcome::Skipped,
        started_at,
        None,
        Some(detail.into()),
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StartupWalCheckpointOutcome {
    Deferred,
}

pub(crate) fn perform_startup_wal_checkpoint(
    reporter: &StartupStepProgressReporter,
) -> StartupWalCheckpointOutcome {
    skip_inline_startup_step(
        reporter,
        "startup_sqlite_wal_checkpoint",
        STARTUP_WAL_CHECKPOINT_DEFER_REASON,
    );
    StartupWalCheckpointOutcome::Deferred
}

pub(crate) fn defer_implicit_startup_sqlite_wal_autocheckpoint(
    store: SqliteStore,
) -> Result<(SqliteStore, i64)> {
    let restore_pages = store
        .wal_autocheckpoint_pages()
        .context("failed to inspect sqlite wal_autocheckpoint before startup-critical writes")?;
    store.set_wal_autocheckpoint_pages(0).context(
        "failed to defer implicit sqlite wal autocheckpoint for startup-critical writes",
    )?;
    Ok((store, restore_pages))
}

pub(crate) fn restore_implicit_startup_sqlite_wal_autocheckpoint(
    store: SqliteStore,
    restore_pages: i64,
) -> Result<SqliteStore> {
    let restore_pages = if restore_pages < 0 {
        SQLITE_DEFAULT_WAL_AUTOCHECKPOINT_PAGES
    } else {
        restore_pages
    };
    store
        .set_wal_autocheckpoint_pages(restore_pages)
        .with_context(|| {
            format!(
                "failed to restore sqlite wal_autocheckpoint={} after startup-critical writes",
                restore_pages
            )
        })?;
    Ok(store)
}
