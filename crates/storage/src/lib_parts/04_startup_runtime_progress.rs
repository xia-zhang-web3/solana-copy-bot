use super::*;

pub fn sqlite_contention_snapshot() -> SqliteContentionSnapshot {
    SqliteContentionSnapshot {
        write_retry_total: SQLITE_WRITE_RETRY_TOTAL.load(Ordering::Relaxed),
        busy_error_total: SQLITE_BUSY_ERROR_TOTAL.load(Ordering::Relaxed),
    }
}

pub(super) fn startup_step_elapsed_ms(elapsed: StdDuration) -> u64 {
    elapsed.as_millis().min(u64::MAX as u128) as u64
}

pub fn log_startup_step_progress(progress: &StartupStepProgress) {
    match progress.outcome {
        StartupStepOutcome::Started | StartupStepOutcome::Completed => {
            tracing::info!(
                startup_stage = progress.stage,
                startup_stage_outcome = progress.outcome.as_str(),
                startup_stage_elapsed_ms = progress.elapsed_ms,
                startup_stage_budget_ms = progress.budget_ms,
                detail = progress.detail.as_deref(),
                "startup stage progress"
            );
        }
        StartupStepOutcome::Waiting | StartupStepOutcome::Skipped => {
            tracing::warn!(
                startup_stage = progress.stage,
                startup_stage_outcome = progress.outcome.as_str(),
                startup_stage_elapsed_ms = progress.elapsed_ms,
                startup_stage_budget_ms = progress.budget_ms,
                detail = progress.detail.as_deref(),
                "startup stage progress"
            );
        }
        StartupStepOutcome::Failed | StartupStepOutcome::TimedOut => {
            tracing::error!(
                startup_stage = progress.stage,
                startup_stage_outcome = progress.outcome.as_str(),
                startup_stage_elapsed_ms = progress.elapsed_ms,
                startup_stage_budget_ms = progress.budget_ms,
                detail = progress.detail.as_deref(),
                "startup stage progress"
            );
        }
    }
}

pub fn startup_step_progress_tracing_reporter() -> StartupStepProgressReporter {
    Arc::new(|progress| log_startup_step_progress(&progress))
}
