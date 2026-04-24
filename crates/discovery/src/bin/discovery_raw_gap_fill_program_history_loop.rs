use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread::sleep;
use std::time::Duration;

const USAGE: &str = "usage: discovery_raw_gap_fill_program_history_loop --gap-fill-bin <path> --progress-path <path> --max-attempts <n> --sleep-seconds <n> [--min-free-gb <n>] [--disk-guard-path <path>] [--json] -- <discovery_raw_gap_fill_program_history args...>";
const DEFAULT_MIN_FREE_GB: u64 = 100;
const BYTES_PER_GB: u64 = 1024 * 1024 * 1024;

fn main() {
    let Some(config) = (match parse_args() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error:#}");
            std::process::exit(1);
        }
    }) else {
        println!("{USAGE}");
        return;
    };

    let json = config.json;
    let result = run_loop_with_hooks(
        &config,
        run_child_process,
        free_disk_bytes,
        sleep,
        |summary| {
            if json {
                println!(
                    "{}",
                    serde_json::to_string(summary)
                        .context("failed serializing loop attempt summary")?
                );
            } else {
                println!("{}", render_human(summary));
            }
            Ok(())
        },
    );

    match result {
        Ok(exit_code) => std::process::exit(exit_code),
        Err(error) => {
            eprintln!("{error:#}");
            std::process::exit(1);
        }
    }
}

#[derive(Debug, Clone)]
struct Config {
    gap_fill_bin: PathBuf,
    progress_path: PathBuf,
    max_attempts: usize,
    sleep_seconds: u64,
    min_free_gb: u64,
    disk_guard_path: PathBuf,
    json: bool,
    child_args: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct LoopAttemptSummary {
    loop_attempt_index: usize,
    child_exit_status: Option<i32>,
    progress_attempt_number: Option<usize>,
    verdict: Option<String>,
    reason: Option<String>,
    attempt_frontier_advanced_slots: Option<usize>,
    next_batch_start_slot: Option<u64>,
    gap_fill_covered_through_ts_utc: Option<String>,
    staged_rows: Option<usize>,
    replayable_output: Option<bool>,
    stop_reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct GapFillProgress {
    attempt_number: Option<usize>,
    verdict: Option<String>,
    reason: Option<String>,
    #[serde(default)]
    current_phase: Option<String>,
    attempt_frontier_advanced_slots: Option<usize>,
    next_batch_start_slot: Option<u64>,
    gap_fill_covered_through_cursor: Option<ProgressCursor>,
    staged_rows: Option<usize>,
    replayable_output: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
struct ProgressCursor {
    ts_utc: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChildAttemptResult {
    exit_status: Option<i32>,
    success: bool,
    stdout: String,
}

struct LoopLock {
    path: PathBuf,
    _file: File,
}

impl Drop for LoopLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut gap_fill_bin: Option<PathBuf> = None;
    let mut progress_path: Option<PathBuf> = None;
    let mut max_attempts: Option<usize> = None;
    let mut sleep_seconds: Option<u64> = None;
    let mut min_free_gb = DEFAULT_MIN_FREE_GB;
    let mut disk_guard_path: Option<PathBuf> = None;
    let mut json = false;
    let mut child_args = Vec::new();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => return Ok(None),
            "--gap-fill-bin" => {
                gap_fill_bin = Some(PathBuf::from(parse_string_arg(
                    "--gap-fill-bin",
                    args.next(),
                )?))
            }
            "--progress-path" => {
                progress_path = Some(PathBuf::from(parse_string_arg(
                    "--progress-path",
                    args.next(),
                )?))
            }
            "--max-attempts" => {
                max_attempts = Some(parse_usize_arg("--max-attempts", args.next())?)
            }
            "--sleep-seconds" => {
                sleep_seconds = Some(parse_u64_arg("--sleep-seconds", args.next())?)
            }
            "--min-free-gb" => min_free_gb = parse_u64_arg("--min-free-gb", args.next())?,
            "--disk-guard-path" => {
                disk_guard_path = Some(PathBuf::from(parse_string_arg(
                    "--disk-guard-path",
                    args.next(),
                )?))
            }
            "--json" => json = true,
            "--" => {
                child_args.extend(args);
                break;
            }
            other => bail!("unknown argument before -- separator: {other}\n{USAGE}"),
        }
    }

    let gap_fill_bin = gap_fill_bin.ok_or_else(|| anyhow!("missing --gap-fill-bin\n{USAGE}"))?;
    let progress_path = progress_path.ok_or_else(|| anyhow!("missing --progress-path\n{USAGE}"))?;
    let max_attempts = max_attempts.ok_or_else(|| anyhow!("missing --max-attempts\n{USAGE}"))?;
    let sleep_seconds = sleep_seconds.ok_or_else(|| anyhow!("missing --sleep-seconds\n{USAGE}"))?;
    if max_attempts == 0 {
        bail!("--max-attempts must be greater than zero");
    }
    let disk_guard_path = disk_guard_path.unwrap_or_else(|| {
        progress_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
    });

    Ok(Some(Config {
        gap_fill_bin,
        progress_path,
        max_attempts,
        sleep_seconds,
        min_free_gb,
        disk_guard_path,
        json,
        child_args,
    }))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    value.ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn parse_usize_arg(flag: &str, value: Option<String>) -> Result<usize> {
    parse_string_arg(flag, value)?
        .parse::<usize>()
        .with_context(|| format!("invalid integer for {flag}"))
}

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    parse_string_arg(flag, value)?
        .parse::<u64>()
        .with_context(|| format!("invalid integer for {flag}"))
}

fn run_loop_with_hooks<R, D, S, E>(
    config: &Config,
    mut run_child: R,
    mut disk_free_bytes: D,
    mut sleep_fn: S,
    mut emit: E,
) -> Result<i32>
where
    R: FnMut(&Path, &[String]) -> Result<ChildAttemptResult>,
    D: FnMut(&Path) -> Result<u64>,
    S: FnMut(Duration),
    E: FnMut(&LoopAttemptSummary) -> Result<()>,
{
    let _lock = acquire_loop_lock(&config.progress_path)?;
    for loop_attempt_index in 1..=config.max_attempts {
        let existing_progress = match load_progress_if_exists(&config.progress_path) {
            Ok(progress) => progress,
            Err(error) => {
                let summary = summary_from_progress(
                    loop_attempt_index,
                    None,
                    None,
                    Some(progress_operator_error_stop_reason(&error)),
                );
                emit(&summary)?;
                return Ok(1);
            }
        };
        if existing_progress
            .as_ref()
            .and_then(|progress| progress.replayable_output)
            == Some(true)
        {
            let summary = summary_from_progress(
                loop_attempt_index,
                None,
                existing_progress.as_ref(),
                Some("program_history_gap_fill_loop_completed_replayable_output".to_string()),
            );
            emit(&summary)?;
            return Ok(0);
        }

        let free_bytes = match disk_free_bytes(&config.disk_guard_path) {
            Ok(free_bytes) => free_bytes,
            Err(error) => {
                let summary = summary_from_progress(
                    loop_attempt_index,
                    None,
                    existing_progress.as_ref(),
                    Some(format!(
                        "program_history_gap_fill_loop_unproven_operator_error_disk_guard_check_failed:path={}:{}",
                        config.disk_guard_path.display(),
                        error
                    )),
                );
                emit(&summary)?;
                return Ok(1);
            }
        };
        let min_free_bytes = config.min_free_gb.saturating_mul(BYTES_PER_GB);
        if free_bytes < min_free_bytes {
            let summary = summary_from_progress(
                loop_attempt_index,
                None,
                existing_progress.as_ref(),
                Some(format!(
                    "program_history_gap_fill_loop_disk_guard_free_space_below_threshold:free_gb={}:min_free_gb={}:path={}",
                    free_bytes / BYTES_PER_GB,
                    config.min_free_gb,
                    config.disk_guard_path.display()
                )),
            );
            emit(&summary)?;
            return Ok(1);
        }

        let child = match run_child(&config.gap_fill_bin, &config.child_args) {
            Ok(child) => child,
            Err(error) => {
                let summary = summary_from_progress(
                    loop_attempt_index,
                    None,
                    existing_progress.as_ref(),
                    Some(format!(
                        "program_history_gap_fill_loop_unproven_operator_error_child_launch_failed:bin={}:{}",
                        config.gap_fill_bin.display(),
                        error
                    )),
                );
                emit(&summary)?;
                return Ok(1);
            }
        };
        if !child.success {
            let progress = load_progress_if_exists(&config.progress_path)
                .ok()
                .flatten();
            let summary = summary_from_progress(
                loop_attempt_index,
                Some(child.exit_status),
                progress.as_ref(),
                Some("program_history_gap_fill_loop_child_nonzero_exit".to_string()),
            );
            emit(&summary)?;
            return Ok(1);
        }

        let progress = match load_progress_required_or_stdout(&config.progress_path, &child.stdout)
        {
            Ok(progress) => progress,
            Err(error) => {
                let summary = summary_from_progress(
                    loop_attempt_index,
                    Some(child.exit_status),
                    None,
                    Some(progress_operator_error_stop_reason(&error)),
                );
                emit(&summary)?;
                return Ok(1);
            }
        };

        let stop_reason = if progress.replayable_output == Some(true) {
            Some("program_history_gap_fill_loop_completed_replayable_output".to_string())
        } else if !is_continuable_incomplete_progress(&progress) {
            Some("program_history_gap_fill_loop_non_replayable_terminal_verdict".to_string())
        } else if loop_attempt_index == config.max_attempts {
            Some("program_history_gap_fill_loop_max_attempts_exhausted".to_string())
        } else {
            None
        };
        let summary = summary_from_progress(
            loop_attempt_index,
            Some(child.exit_status),
            Some(&progress),
            stop_reason,
        );
        emit(&summary)?;

        if progress.replayable_output == Some(true) {
            return Ok(0);
        }
        if !is_continuable_incomplete_progress(&progress) {
            return Ok(1);
        }
        if loop_attempt_index == config.max_attempts {
            return Ok(1);
        }
        if config.sleep_seconds > 0 {
            sleep_fn(Duration::from_secs(config.sleep_seconds));
        }
    }

    Ok(1)
}

fn run_child_process(bin: &Path, args: &[String]) -> Result<ChildAttemptResult> {
    let output = Command::new(bin)
        .args(args)
        .output()
        .with_context(|| format!("failed launching gap-fill child {}", bin.display()))?;
    Ok(ChildAttemptResult {
        exit_status: output.status.code(),
        success: output.status.success(),
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
    })
}

fn acquire_loop_lock(progress_path: &Path) -> Result<LoopLock> {
    let lock_path = lock_path_for_progress(progress_path);
    let file = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lock_path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            bail!(
                "program_history_gap_fill_loop_lock_exists:path={}",
                lock_path.display()
            )
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "program_history_gap_fill_loop_lock_create_failed:path={}",
                    lock_path.display()
                )
            });
        }
    };
    Ok(LoopLock {
        path: lock_path,
        _file: file,
    })
}

fn lock_path_for_progress(progress_path: &Path) -> PathBuf {
    let mut lock_name = progress_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("progress")
        .to_string();
    lock_name.push_str(".loop.lock");
    progress_path.with_file_name(lock_name)
}

fn load_progress_if_exists(path: &Path) -> Result<Option<GapFillProgress>> {
    if !path.exists() {
        return Ok(None);
    }
    load_progress_required(path).map(Some)
}

fn load_progress_required(path: &Path) -> Result<GapFillProgress> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed reading progress json {}", path.display()))?;
    let progress = serde_json::from_str::<GapFillProgress>(&raw)
        .with_context(|| format!("failed parsing progress json {}", path.display()))?;
    validate_progress_control_fields(progress)
}

fn load_progress_required_or_stdout(path: &Path, stdout: &str) -> Result<GapFillProgress> {
    if path.exists() {
        return load_progress_required(path);
    }
    if stdout.trim().is_empty() {
        bail!(
            "progress json {} does not exist and child stdout was empty",
            path.display()
        );
    }
    let progress = serde_json::from_str::<GapFillProgress>(stdout).with_context(|| {
        format!(
            "failed parsing child stdout as progress json after {} was absent",
            path.display()
        )
    })?;
    validate_progress_control_fields(progress)
}

fn validate_progress_control_fields(progress: GapFillProgress) -> Result<GapFillProgress> {
    if progress.verdict.is_none() {
        bail!("missing required progress control field verdict");
    }
    if progress.current_phase.is_none() {
        bail!("missing required progress control field current_phase");
    }
    if progress.replayable_output.is_none() {
        bail!("missing required progress control field replayable_output");
    }
    Ok(progress)
}

fn progress_operator_error_stop_reason(error: &anyhow::Error) -> String {
    let error_text = format!("{error:#}");
    if error_text.contains("missing required progress control field") {
        format!(
            "program_history_gap_fill_loop_unproven_operator_error_progress_control_field_missing:{error_text}"
        )
    } else {
        format!(
            "program_history_gap_fill_loop_unproven_operator_error_progress_json_parse_failed:{error_text}"
        )
    }
}

fn is_continuable_incomplete_progress(progress: &GapFillProgress) -> bool {
    if progress.replayable_output != Some(false) {
        return false;
    }
    let Some(verdict) = progress.verdict.as_deref() else {
        return false;
    };
    matches!(
        verdict,
        "not_proven_due_to_retryable_provider_failure"
            | "not_proven_due_to_cost_budget"
            | "not_proven_due_to_attempt_budget"
            | "not_proven_due_to_provider_throttling"
    )
}

fn summary_from_progress(
    loop_attempt_index: usize,
    child_exit_status: Option<Option<i32>>,
    progress: Option<&GapFillProgress>,
    stop_reason: Option<String>,
) -> LoopAttemptSummary {
    LoopAttemptSummary {
        loop_attempt_index,
        child_exit_status: child_exit_status.flatten(),
        progress_attempt_number: progress.and_then(|progress| progress.attempt_number),
        verdict: progress.and_then(|progress| progress.verdict.clone()),
        reason: progress.and_then(|progress| progress.reason.clone()),
        attempt_frontier_advanced_slots: progress
            .and_then(|progress| progress.attempt_frontier_advanced_slots),
        next_batch_start_slot: progress.and_then(|progress| progress.next_batch_start_slot),
        gap_fill_covered_through_ts_utc: progress
            .and_then(|progress| progress.gap_fill_covered_through_cursor.as_ref())
            .and_then(|cursor| cursor.ts_utc.clone()),
        staged_rows: progress.and_then(|progress| progress.staged_rows),
        replayable_output: progress.and_then(|progress| progress.replayable_output),
        stop_reason,
    }
}

fn free_disk_bytes(path: &Path) -> Result<u64> {
    let output = Command::new("df")
        .arg("-Pk")
        .arg(path)
        .output()
        .with_context(|| format!("failed running df for {}", path.display()))?;
    if !output.status.success() {
        bail!(
            "df failed for {}: {}",
            path.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let stdout = String::from_utf8(output.stdout).context("df output was not utf-8")?;
    let line = stdout
        .lines()
        .skip(1)
        .next()
        .ok_or_else(|| anyhow!("df output did not include a data row"))?;
    let available_kb = line
        .split_whitespace()
        .nth(3)
        .ok_or_else(|| anyhow!("df output missing Available column"))?
        .parse::<u64>()
        .context("df Available column was not an integer")?;
    Ok(available_kb.saturating_mul(1024))
}

fn render_human(summary: &LoopAttemptSummary) -> String {
    [
        "event=discovery_raw_gap_fill_program_history_loop_attempt".to_string(),
        format!("loop_attempt_index={}", summary.loop_attempt_index),
        format!(
            "child_exit_status={}",
            summary
                .child_exit_status
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "progress_attempt_number={}",
            summary
                .progress_attempt_number
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("verdict={}", summary.verdict.as_deref().unwrap_or("null")),
        format!("reason={}", summary.reason.as_deref().unwrap_or("null")),
        format!(
            "attempt_frontier_advanced_slots={}",
            summary
                .attempt_frontier_advanced_slots
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "next_batch_start_slot={}",
            summary
                .next_batch_start_slot
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "gap_fill_covered_through_ts_utc={}",
            summary
                .gap_fill_covered_through_ts_utc
                .as_deref()
                .unwrap_or("null")
        ),
        format!(
            "staged_rows={}",
            summary
                .staged_rows
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "replayable_output={}",
            summary
                .replayable_output
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "stop_reason={}",
            summary.stop_reason.as_deref().unwrap_or("null")
        ),
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};
    use tempfile::tempdir;

    fn config(progress_path: PathBuf) -> Config {
        let disk_guard_path = progress_path.parent().unwrap().to_path_buf();
        Config {
            gap_fill_bin: PathBuf::from("/bin/false"),
            progress_path,
            max_attempts: 3,
            sleep_seconds: 1,
            min_free_gb: 1,
            disk_guard_path,
            json: true,
            child_args: vec!["--example-child-arg".to_string()],
        }
    }

    fn write_progress(path: &Path, body: &str) {
        fs::write(path, body).expect("write progress json");
    }

    fn incomplete_retryable_json(attempt_number: usize) -> String {
        format!(
            r#"{{
  "attempt_number": {attempt_number},
  "verdict": "not_proven_due_to_retryable_provider_failure",
  "reason": "program_history_gap_fill_retryable_block_fetch_http_408",
  "current_phase": "awaiting_next_attempt",
  "attempt_frontier_advanced_slots": 7,
  "next_batch_start_slot": 1105,
  "gap_fill_covered_through_cursor": {{ "ts_utc": "2026-04-18T17:00:00Z" }},
  "staged_rows": 42,
  "replayable_output": false
}}"#
        )
    }

    fn complete_json(attempt_number: usize) -> String {
        format!(
            r#"{{
  "attempt_number": {attempt_number},
  "verdict": "complete_but_insufficient_for_healthy_restore",
  "reason": "program_history_gap_fill_completed_but_still_missing_required_raw_window",
  "current_phase": "complete",
  "attempt_frontier_advanced_slots": 3,
  "next_batch_start_slot": null,
  "gap_fill_covered_through_cursor": {{ "ts_utc": "2026-04-23T15:59:39Z" }},
  "staged_rows": 55,
  "replayable_output": true
}}"#
        )
    }

    fn incomplete_cost_budget_json(attempt_number: usize) -> String {
        format!(
            r#"{{
  "attempt_number": {attempt_number},
  "verdict": "not_proven_due_to_cost_budget",
  "reason": "program_history_gap_fill_block_fetch_budget_exhausted",
  "current_phase": "block_fetch_budget_exhausted",
  "attempt_frontier_advanced_slots": 5,
  "next_batch_start_slot": 1112,
  "gap_fill_covered_through_cursor": {{ "ts_utc": "2026-04-18T17:01:00Z" }},
  "staged_rows": 44,
  "replayable_output": false
}}"#
        )
    }

    fn incomplete_retryable_missing_replayable_output_json() -> String {
        r#"{
  "attempt_number": 1,
  "verdict": "not_proven_due_to_retryable_provider_failure",
  "reason": "program_history_gap_fill_retryable_block_fetch_http_408",
  "current_phase": "awaiting_next_attempt",
  "attempt_frontier_advanced_slots": 7,
  "next_batch_start_slot": 1105,
  "staged_rows": 42
}"#
        .to_string()
    }

    fn incomplete_retryable_missing_verdict_json() -> String {
        r#"{
  "attempt_number": 1,
  "reason": "program_history_gap_fill_retryable_block_fetch_http_408",
  "current_phase": "awaiting_next_attempt",
  "attempt_frontier_advanced_slots": 7,
  "next_batch_start_slot": 1105,
  "staged_rows": 42,
  "replayable_output": false
}"#
        .to_string()
    }

    #[test]
    fn lock_file_prevents_concurrent_loop_stage1() {
        let temp = tempdir().expect("tempdir");
        let progress_path = temp.path().join("manual_exact.progress.json");
        let lock_path = lock_path_for_progress(&progress_path);
        File::create(&lock_path).expect("create lock");
        let launched = Cell::new(0usize);
        let summaries = RefCell::new(Vec::new());

        let error = run_loop_with_hooks(
            &config(progress_path),
            |_bin, _args| {
                launched.set(launched.get() + 1);
                Ok(ChildAttemptResult {
                    exit_status: Some(0),
                    success: true,
                    stdout: String::new(),
                })
            },
            |_path| Ok(2 * BYTES_PER_GB),
            |_duration| {},
            |summary| {
                summaries.borrow_mut().push(summary.clone());
                Ok(())
            },
        )
        .expect_err("existing lock must reject the loop");

        assert_eq!(launched.get(), 0);
        assert!(summaries.borrow().is_empty());
        assert!(error
            .to_string()
            .contains("program_history_gap_fill_loop_lock_exists"));
    }

    #[test]
    fn child_non_zero_exit_stops_loop_with_explicit_non_green_reason_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("manual_exact.progress.json");
        let launched = Cell::new(0usize);
        let summaries = RefCell::new(Vec::new());

        let exit_code = run_loop_with_hooks(
            &config(progress_path),
            |_bin, _args| {
                launched.set(launched.get() + 1);
                Ok(ChildAttemptResult {
                    exit_status: Some(42),
                    success: false,
                    stdout: String::new(),
                })
            },
            |_path| Ok(2 * BYTES_PER_GB),
            |_duration| {},
            |summary| {
                summaries.borrow_mut().push(summary.clone());
                Ok(())
            },
        )?;

        assert_eq!(exit_code, 1);
        assert_eq!(launched.get(), 1);
        let summaries = summaries.borrow();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].child_exit_status, Some(42));
        assert_eq!(
            summaries[0].stop_reason.as_deref(),
            Some("program_history_gap_fill_loop_child_nonzero_exit")
        );
        assert_eq!(summaries[0].replayable_output, None);
        Ok(())
    }

    #[test]
    fn replayable_output_true_stops_loop_as_completed_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("manual_exact.progress.json");
        let launched = Cell::new(0usize);
        let summaries = RefCell::new(Vec::new());

        let exit_code = run_loop_with_hooks(
            &config(progress_path.clone()),
            |_bin, _args| {
                launched.set(launched.get() + 1);
                write_progress(&progress_path, &complete_json(1));
                Ok(ChildAttemptResult {
                    exit_status: Some(0),
                    success: true,
                    stdout: String::new(),
                })
            },
            |_path| Ok(2 * BYTES_PER_GB),
            |_duration| {},
            |summary| {
                summaries.borrow_mut().push(summary.clone());
                Ok(())
            },
        )?;

        assert_eq!(exit_code, 0);
        assert_eq!(launched.get(), 1);
        let summaries = summaries.borrow();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].replayable_output, Some(true));
        assert_eq!(summaries[0].progress_attempt_number, Some(1));
        assert_eq!(
            summaries[0].gap_fill_covered_through_ts_utc.as_deref(),
            Some("2026-04-23T15:59:39Z")
        );
        assert_eq!(
            summaries[0].stop_reason.as_deref(),
            Some("program_history_gap_fill_loop_completed_replayable_output")
        );
        Ok(())
    }

    #[test]
    fn incomplete_retryable_and_cost_budget_progress_continue_to_next_attempt_stage1() -> Result<()>
    {
        let temp = tempdir()?;
        let progress_path = temp.path().join("manual_exact.progress.json");
        let launched = Cell::new(0usize);
        let slept = Cell::new(0usize);
        let summaries = RefCell::new(Vec::new());

        let exit_code = run_loop_with_hooks(
            &config(progress_path.clone()),
            |_bin, _args| {
                let attempt = launched.get() + 1;
                launched.set(attempt);
                match attempt {
                    1 => write_progress(&progress_path, &incomplete_retryable_json(1)),
                    2 => write_progress(&progress_path, &incomplete_cost_budget_json(2)),
                    _ => write_progress(&progress_path, &complete_json(3)),
                }
                Ok(ChildAttemptResult {
                    exit_status: Some(0),
                    success: true,
                    stdout: String::new(),
                })
            },
            |_path| Ok(2 * BYTES_PER_GB),
            |_duration| {
                slept.set(slept.get() + 1);
            },
            |summary| {
                summaries.borrow_mut().push(summary.clone());
                Ok(())
            },
        )?;

        assert_eq!(exit_code, 0);
        assert_eq!(launched.get(), 3);
        assert_eq!(slept.get(), 2);
        let summaries = summaries.borrow();
        assert_eq!(summaries.len(), 3);
        assert_eq!(summaries[0].replayable_output, Some(false));
        assert_eq!(summaries[0].stop_reason, None);
        assert_eq!(
            summaries[0].verdict.as_deref(),
            Some("not_proven_due_to_retryable_provider_failure")
        );
        assert_eq!(summaries[1].replayable_output, Some(false));
        assert_eq!(summaries[1].stop_reason, None);
        assert_eq!(
            summaries[1].verdict.as_deref(),
            Some("not_proven_due_to_cost_budget")
        );
        assert_eq!(summaries[2].replayable_output, Some(true));
        assert_eq!(
            summaries[2].stop_reason.as_deref(),
            Some("program_history_gap_fill_loop_completed_replayable_output")
        );
        Ok(())
    }

    #[test]
    fn valid_progress_missing_replayable_output_stops_before_continuing_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("manual_exact.progress.json");
        write_progress(
            &progress_path,
            &incomplete_retryable_missing_replayable_output_json(),
        );
        let launched = Cell::new(0usize);
        let summaries = RefCell::new(Vec::new());

        let exit_code = run_loop_with_hooks(
            &config(progress_path),
            |_bin, _args| {
                launched.set(launched.get() + 1);
                Ok(ChildAttemptResult {
                    exit_status: Some(0),
                    success: true,
                    stdout: String::new(),
                })
            },
            |_path| Ok(2 * BYTES_PER_GB),
            |_duration| {},
            |summary| {
                summaries.borrow_mut().push(summary.clone());
                Ok(())
            },
        )?;

        assert_eq!(exit_code, 1);
        assert_eq!(launched.get(), 0);
        let summaries = summaries.borrow();
        assert_eq!(summaries.len(), 1);
        let stop_reason = summaries[0].stop_reason.as_deref().unwrap_or_default();
        assert!(stop_reason.contains(
            "program_history_gap_fill_loop_unproven_operator_error_progress_control_field_missing"
        ));
        assert!(stop_reason.contains("replayable_output"));
        Ok(())
    }

    #[test]
    fn valid_progress_missing_verdict_stops_with_operator_error_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("manual_exact.progress.json");
        write_progress(&progress_path, &incomplete_retryable_missing_verdict_json());
        let launched = Cell::new(0usize);
        let summaries = RefCell::new(Vec::new());

        let exit_code = run_loop_with_hooks(
            &config(progress_path),
            |_bin, _args| {
                launched.set(launched.get() + 1);
                Ok(ChildAttemptResult {
                    exit_status: Some(0),
                    success: true,
                    stdout: String::new(),
                })
            },
            |_path| Ok(2 * BYTES_PER_GB),
            |_duration| {},
            |summary| {
                summaries.borrow_mut().push(summary.clone());
                Ok(())
            },
        )?;

        assert_eq!(exit_code, 1);
        assert_eq!(launched.get(), 0);
        let summaries = summaries.borrow();
        assert_eq!(summaries.len(), 1);
        let stop_reason = summaries[0].stop_reason.as_deref().unwrap_or_default();
        assert!(stop_reason.contains(
            "program_history_gap_fill_loop_unproven_operator_error_progress_control_field_missing"
        ));
        assert!(stop_reason.contains("verdict"));
        Ok(())
    }

    #[test]
    fn disk_guard_stops_before_launching_child_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("manual_exact.progress.json");
        let launched = Cell::new(0usize);
        let summaries = RefCell::new(Vec::new());

        let exit_code = run_loop_with_hooks(
            &config(progress_path),
            |_bin, _args| {
                launched.set(launched.get() + 1);
                Ok(ChildAttemptResult {
                    exit_status: Some(0),
                    success: true,
                    stdout: String::new(),
                })
            },
            |_path| Ok(512 * 1024 * 1024),
            |_duration| {},
            |summary| {
                summaries.borrow_mut().push(summary.clone());
                Ok(())
            },
        )?;

        assert_eq!(exit_code, 1);
        assert_eq!(launched.get(), 0);
        let summaries = summaries.borrow();
        assert_eq!(summaries.len(), 1);
        assert!(summaries[0]
            .stop_reason
            .as_deref()
            .unwrap_or_default()
            .contains("program_history_gap_fill_loop_disk_guard_free_space_below_threshold"));
        Ok(())
    }

    #[test]
    fn disk_guard_check_failure_emits_operator_error_summary_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("manual_exact.progress.json");
        let launched = Cell::new(0usize);
        let summaries = RefCell::new(Vec::new());

        let exit_code = run_loop_with_hooks(
            &config(progress_path),
            |_bin, _args| {
                launched.set(launched.get() + 1);
                Ok(ChildAttemptResult {
                    exit_status: Some(0),
                    success: true,
                    stdout: String::new(),
                })
            },
            |_path| Err(anyhow!("df unavailable")),
            |_duration| {},
            |summary| {
                summaries.borrow_mut().push(summary.clone());
                Ok(())
            },
        )?;

        assert_eq!(exit_code, 1);
        assert_eq!(launched.get(), 0);
        let summaries = summaries.borrow();
        assert_eq!(summaries.len(), 1);
        let stop_reason = summaries[0].stop_reason.as_deref().unwrap_or_default();
        assert!(stop_reason.contains(
            "program_history_gap_fill_loop_unproven_operator_error_disk_guard_check_failed"
        ));
        assert!(stop_reason.contains("df unavailable"));
        Ok(())
    }

    #[test]
    fn child_launch_failure_emits_operator_error_summary_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("manual_exact.progress.json");
        let launched = Cell::new(0usize);
        let summaries = RefCell::new(Vec::new());

        let exit_code = run_loop_with_hooks(
            &config(progress_path),
            |_bin, _args| {
                launched.set(launched.get() + 1);
                Err(anyhow!("spawn refused"))
            },
            |_path| Ok(2 * BYTES_PER_GB),
            |_duration| {},
            |summary| {
                summaries.borrow_mut().push(summary.clone());
                Ok(())
            },
        )?;

        assert_eq!(exit_code, 1);
        assert_eq!(launched.get(), 1);
        let summaries = summaries.borrow();
        assert_eq!(summaries.len(), 1);
        let stop_reason = summaries[0].stop_reason.as_deref().unwrap_or_default();
        assert!(stop_reason
            .contains("program_history_gap_fill_loop_unproven_operator_error_child_launch_failed"));
        assert!(stop_reason.contains("spawn refused"));
        Ok(())
    }

    #[test]
    fn progress_json_parse_failure_stops_with_operator_error_reason_stage1() -> Result<()> {
        let temp = tempdir()?;
        let progress_path = temp.path().join("manual_exact.progress.json");
        write_progress(&progress_path, "{not-json");
        let launched = Cell::new(0usize);
        let summaries = RefCell::new(Vec::new());

        let exit_code = run_loop_with_hooks(
            &config(progress_path),
            |_bin, _args| {
                launched.set(launched.get() + 1);
                Ok(ChildAttemptResult {
                    exit_status: Some(0),
                    success: true,
                    stdout: String::new(),
                })
            },
            |_path| Ok(2 * BYTES_PER_GB),
            |_duration| {},
            |summary| {
                summaries.borrow_mut().push(summary.clone());
                Ok(())
            },
        )?;

        assert_eq!(exit_code, 1);
        assert_eq!(launched.get(), 0);
        let summaries = summaries.borrow();
        assert_eq!(summaries.len(), 1);
        assert!(summaries[0]
            .stop_reason
            .as_deref()
            .unwrap_or_default()
            .contains(
                "program_history_gap_fill_loop_unproven_operator_error_progress_json_parse_failed"
            ));
        Ok(())
    }
}
