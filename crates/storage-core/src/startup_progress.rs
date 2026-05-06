use anyhow::{anyhow, Context, Result};
use std::sync::mpsc;
use std::thread;
use std::time::Duration as StdDuration;

use crate::types::{
    StartupStepOutcome, StartupStepProgress, StartupStepProgressReporter, StartupStepRuntimePolicy,
    StartupStepTimeout, StartupStepTimeoutBehavior,
};

fn startup_step_elapsed_ms(elapsed: StdDuration) -> u64 {
    elapsed.as_millis().min(u64::MAX as u128) as u64
}

pub fn report_startup_step_progress(
    reporter: Option<&StartupStepProgressReporter>,
    stage: &'static str,
    outcome: StartupStepOutcome,
    elapsed: StdDuration,
    budget: Option<StdDuration>,
    detail: Option<String>,
) {
    if let Some(reporter) = reporter {
        reporter(StartupStepProgress {
            stage,
            outcome,
            elapsed_ms: startup_step_elapsed_ms(elapsed),
            budget_ms: budget.map(startup_step_elapsed_ms),
            detail,
        });
    }
}

pub fn run_observed_startup_step<T, F>(
    stage: &'static str,
    policy: StartupStepRuntimePolicy,
    reporter: Option<&StartupStepProgressReporter>,
    operation: F,
) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    run_observed_startup_step_with_completion_detail(stage, policy, reporter, operation, |_| None)
}

pub fn run_observed_startup_step_with_completion_detail<T, F, D>(
    stage: &'static str,
    policy: StartupStepRuntimePolicy,
    reporter: Option<&StartupStepProgressReporter>,
    operation: F,
    completion_detail: D,
) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
    D: Fn(&T) -> Option<String>,
{
    report_startup_step_progress(
        reporter,
        stage,
        StartupStepOutcome::Started,
        StdDuration::ZERO,
        policy.timeout,
        None,
    );

    let started_at = std::time::Instant::now();
    let (tx, rx) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let _ = tx.send(operation());
    });

    let wait_slice = if policy.wait_log_interval.is_zero() {
        StdDuration::from_millis(100)
    } else {
        policy.wait_log_interval
    };

    loop {
        let elapsed = started_at.elapsed();
        if let Some(timeout) = policy.timeout {
            if elapsed >= timeout {
                let timeout_error = StartupStepTimeout {
                    stage,
                    elapsed_ms: startup_step_elapsed_ms(elapsed),
                    budget_ms: startup_step_elapsed_ms(timeout),
                };
                report_startup_step_progress(
                    reporter,
                    stage,
                    StartupStepOutcome::TimedOut,
                    elapsed,
                    Some(timeout),
                    Some(format!(
                        "timeout_behavior={}",
                        policy.timeout_behavior.as_str()
                    )),
                );
                match policy.timeout_behavior {
                    StartupStepTimeoutBehavior::ReturnError => {
                        return Err(timeout_error.into());
                    }
                    StartupStepTimeoutBehavior::Panic => panic!("{timeout_error}"),
                    StartupStepTimeoutBehavior::AbortProcess => {
                        tracing::error!(
                            startup_stage = stage,
                            startup_stage_elapsed_ms = timeout_error.elapsed_ms,
                            startup_stage_budget_ms = timeout_error.budget_ms,
                            "startup timeout reached; aborting process because the blocked startup step is not cancellable in-process"
                        );
                        std::process::abort();
                    }
                }
            }
        }

        let wait_for = match policy.timeout {
            Some(timeout) => wait_slice.min(timeout.saturating_sub(elapsed)),
            None => wait_slice,
        };
        let wait_for = if wait_for.is_zero() {
            StdDuration::from_millis(1)
        } else {
            wait_for
        };

        match rx.recv_timeout(wait_for) {
            Ok(result) => {
                let elapsed = started_at.elapsed();
                match result {
                    Ok(value) => {
                        let detail = completion_detail(&value);
                        report_startup_step_progress(
                            reporter,
                            stage,
                            StartupStepOutcome::Completed,
                            elapsed,
                            policy.timeout,
                            detail,
                        );
                        return Ok(value);
                    }
                    Err(error) => {
                        report_startup_step_progress(
                            reporter,
                            stage,
                            StartupStepOutcome::Failed,
                            elapsed,
                            policy.timeout,
                            Some(format!("{error:#}")),
                        );
                        return Err(error).with_context(|| format!("startup step {stage} failed"));
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                report_startup_step_progress(
                    reporter,
                    stage,
                    StartupStepOutcome::Waiting,
                    started_at.elapsed(),
                    policy.timeout,
                    None,
                );
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let elapsed = started_at.elapsed();
                report_startup_step_progress(
                    reporter,
                    stage,
                    StartupStepOutcome::Failed,
                    elapsed,
                    policy.timeout,
                    Some("startup worker thread disconnected".to_string()),
                );
                return Err(anyhow!("startup step worker thread disconnected"))
                    .with_context(|| format!("startup step {stage} failed"));
            }
        }
    }
}
