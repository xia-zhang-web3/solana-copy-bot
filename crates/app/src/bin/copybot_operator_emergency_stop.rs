use anyhow::{anyhow, bail, Context, Result};
use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

const DEFAULT_OPERATOR_EMERGENCY_STOP_PATH: &str = "state/operator_emergency_stop.flag";
const EMERGENCY_STOP_FILE_ENV: &str = "SOLANA_COPY_BOT_EMERGENCY_STOP_FILE";
const CLEAR_CONFIRMATION: &str = "CLEAR_OPERATOR_EMERGENCY_STOP";
const USAGE: &str = "usage:
  copybot_operator_emergency_stop --status [--path <path>] [--json]
  copybot_operator_emergency_stop --activate --reason <text> [--force] [--path <path>] [--json]
  copybot_operator_emergency_stop --clear --confirm-clear CLEAR_OPERATOR_EMERGENCY_STOP [--path <path>] [--json]";

fn main() {
    match parse_args().and_then(run) {
        Ok(report) => {
            if report.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report.output)
                        .expect("emergency-stop report should serialize")
                );
            } else {
                println!("{}", render_human(&report.output));
            }
            if report.success {
                std::process::exit(0);
            }
            std::process::exit(1);
        }
        Err(error) => {
            eprintln!("{error:#}");
            std::process::exit(1);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Status,
    Activate,
    Clear,
}

#[derive(Debug, Clone)]
struct Config {
    mode: Mode,
    path: PathBuf,
    reason: Option<String>,
    force: bool,
    confirm_clear: Option<String>,
    json: bool,
}

#[derive(Debug, Clone)]
struct RunReport {
    output: OperatorEmergencyStopOutput,
    json: bool,
    success: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct OperatorEmergencyStopOutput {
    event: &'static str,
    path: String,
    active: bool,
    changed: bool,
    reason: Option<String>,
    verdict: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FlagStatus {
    active: bool,
    reason: Option<String>,
    verdict: String,
    readable: bool,
}

fn parse_args() -> Result<Config> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Config>
where
    I: IntoIterator<Item = String>,
{
    let mut mode: Option<Mode> = None;
    let mut path: Option<PathBuf> = None;
    let mut reason: Option<String> = None;
    let mut confirm_clear: Option<String> = None;
    let mut force = false;
    let mut json = false;
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--status" => set_mode(&mut mode, Mode::Status)?,
            "--activate" => set_mode(&mut mode, Mode::Activate)?,
            "--clear" => set_mode(&mut mode, Mode::Clear)?,
            "--path" => path = Some(PathBuf::from(parse_string_arg("--path", args.next())?)),
            "--reason" => reason = Some(parse_string_arg("--reason", args.next())?),
            "--confirm-clear" => {
                confirm_clear = Some(parse_string_arg("--confirm-clear", args.next())?)
            }
            "--force" => force = true,
            "--json" => json = true,
            "--help" | "-h" => {
                bail!("{USAGE}");
            }
            other => bail!("unknown argument: {other}\n{USAGE}"),
        }
    }

    let mode = mode.ok_or_else(|| anyhow!("one mode is required\n{USAGE}"))?;
    if force && mode != Mode::Activate {
        bail!("--force is only valid with --activate");
    }
    if reason.is_some() && mode != Mode::Activate {
        bail!("--reason is only valid with --activate");
    }
    if confirm_clear.is_some() && mode != Mode::Clear {
        bail!("--confirm-clear is only valid with --clear");
    }
    let path = resolve_path(path)?;
    if mode == Mode::Activate && reason.as_deref().is_none_or(str::is_empty) {
        bail!("copybot_operator_emergency_stop_activate_missing_reason: --reason is required");
    }

    Ok(Config {
        mode,
        path,
        reason,
        force,
        confirm_clear,
        json,
    })
}

fn set_mode(mode: &mut Option<Mode>, next: Mode) -> Result<()> {
    if mode.replace(next).is_some() {
        bail!("exactly one mode is allowed\n{USAGE}");
    }
    Ok(())
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed)
}

fn resolve_path(override_path: Option<PathBuf>) -> Result<PathBuf> {
    let path = override_path
        .or_else(|| env::var(EMERGENCY_STOP_FILE_ENV).ok().map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from(DEFAULT_OPERATOR_EMERGENCY_STOP_PATH));
    if path.as_os_str().is_empty() {
        bail!("copybot_operator_emergency_stop_empty_path: path cannot be empty");
    }
    Ok(path)
}

fn run(config: Config) -> Result<RunReport> {
    let output = match config.mode {
        Mode::Status => status_output(&config.path),
        Mode::Activate => activate_output(
            &config.path,
            config.reason.as_deref().expect("validated reason"),
            config.force,
        )?,
        Mode::Clear => clear_output(&config.path, config.confirm_clear.as_deref())?,
    };
    let success = !matches!(
        output.verdict.as_str(),
        "activate_refused_different_reason"
            | "activate_refused_unreadable_active_flag"
            | "clear_refused_missing_confirmation"
    );
    Ok(RunReport {
        output,
        json: config.json,
        success,
    })
}

fn status_output(path: &Path) -> OperatorEmergencyStopOutput {
    let status = read_flag_status(path);
    OperatorEmergencyStopOutput {
        event: "copybot_operator_emergency_stop",
        path: path.display().to_string(),
        active: status.active,
        changed: false,
        reason: status.reason,
        verdict: status.verdict,
    }
}

fn activate_output(path: &Path, reason: &str, force: bool) -> Result<OperatorEmergencyStopOutput> {
    let reason = reason.trim();
    if reason.is_empty() {
        bail!("copybot_operator_emergency_stop_activate_missing_reason: --reason is required");
    }
    let status = read_flag_status(path);
    if status.active && status.readable && status.reason.as_deref() == Some(reason) {
        return Ok(OperatorEmergencyStopOutput {
            event: "copybot_operator_emergency_stop",
            path: path.display().to_string(),
            active: true,
            changed: false,
            reason: Some(reason.to_string()),
            verdict: "already_active_same_reason".to_string(),
        });
    }
    if status.active && !status.readable && !force {
        return Ok(OperatorEmergencyStopOutput {
            event: "copybot_operator_emergency_stop",
            path: path.display().to_string(),
            active: true,
            changed: false,
            reason: status.reason,
            verdict: "activate_refused_unreadable_active_flag".to_string(),
        });
    }
    if status.active && status.reason.as_deref() != Some(reason) && !force {
        return Ok(OperatorEmergencyStopOutput {
            event: "copybot_operator_emergency_stop",
            path: path.display().to_string(),
            active: true,
            changed: false,
            reason: status.reason,
            verdict: "activate_refused_different_reason".to_string(),
        });
    }

    write_flag_atomic(path, reason)?;
    Ok(OperatorEmergencyStopOutput {
        event: "copybot_operator_emergency_stop",
        path: path.display().to_string(),
        active: true,
        changed: true,
        reason: Some(reason.to_string()),
        verdict: if status.active {
            "activated_force_overwrote_reason".to_string()
        } else {
            "activated".to_string()
        },
    })
}

fn clear_output(path: &Path, confirm_clear: Option<&str>) -> Result<OperatorEmergencyStopOutput> {
    if confirm_clear != Some(CLEAR_CONFIRMATION) {
        return Ok(OperatorEmergencyStopOutput {
            event: "copybot_operator_emergency_stop",
            path: path.display().to_string(),
            active: read_flag_status(path).active,
            changed: false,
            reason: read_flag_status(path).reason,
            verdict: "clear_refused_missing_confirmation".to_string(),
        });
    }
    let existed = path.exists();
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| format!("failed removing {}", path.display()));
        }
    }
    Ok(OperatorEmergencyStopOutput {
        event: "copybot_operator_emergency_stop",
        path: path.display().to_string(),
        active: false,
        changed: existed,
        reason: None,
        verdict: if existed {
            "cleared".to_string()
        } else {
            "already_clear".to_string()
        },
    })
}

fn read_flag_status(path: &Path) -> FlagStatus {
    match fs::read_to_string(path) {
        Ok(content) => FlagStatus {
            active: true,
            reason: parse_operator_emergency_stop_reason(&content),
            verdict: "active".to_string(),
            readable: true,
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => FlagStatus {
            active: false,
            reason: None,
            verdict: "inactive_missing".to_string(),
            readable: true,
        },
        Err(error) => match fs::metadata(path) {
            Ok(_) => FlagStatus {
                active: true,
                reason: Some(format!("emergency stop file is unreadable: {error}")),
                verdict: "active_fail_closed_unreadable".to_string(),
                readable: false,
            },
            Err(_) => FlagStatus {
                active: false,
                reason: None,
                verdict: "inactive_missing".to_string(),
                readable: true,
            },
        },
    }
}

fn parse_operator_emergency_stop_reason(content: &str) -> Option<String> {
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        return Some(trimmed.to_string());
    }
    None
}

fn write_flag_atomic(path: &Path, reason: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed creating {}", parent.display()))?;
        }
    }
    let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
    let contents = format!(
        "{reason}\n# operator_emergency_stop_activated_at_utc={timestamp}\n# managed_by=copybot_operator_emergency_stop\n"
    );
    let tmp_path = temp_flag_path(path);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp_path)
        .with_context(|| format!("failed creating temp flag {}", tmp_path.display()))?;
    if let Err(error) = file.write_all(contents.as_bytes()) {
        let _ = fs::remove_file(&tmp_path);
        return Err(error).with_context(|| format!("failed writing {}", tmp_path.display()));
    }
    if let Err(error) = file.sync_all() {
        let _ = fs::remove_file(&tmp_path);
        return Err(error).with_context(|| format!("failed syncing {}", tmp_path.display()));
    }
    drop(file);
    if let Err(error) = fs::rename(&tmp_path, path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(error).with_context(|| {
            format!(
                "failed renaming temp flag {} to {}",
                tmp_path.display(),
                path.display()
            )
        });
    }
    Ok(())
}

fn temp_flag_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("operator_emergency_stop.flag");
    let tmp_name = format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    path.with_file_name(tmp_name)
}

fn render_human(output: &OperatorEmergencyStopOutput) -> String {
    [
        format!("event={}", output.event),
        format!("path={}", output.path),
        format!("active={}", output.active),
        format!("changed={}", output.changed),
        format!("reason={}", output.reason.as_deref().unwrap_or("null")),
        format!("verdict={}", output.verdict),
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(mode: Mode, path: PathBuf) -> Config {
        Config {
            mode,
            path,
            reason: None,
            force: false,
            confirm_clear: None,
            json: true,
        }
    }

    fn test_dir(name: &str) -> Result<PathBuf> {
        let path = env::temp_dir().join(format!(
            "copybot_operator_emergency_stop_{name}_{}_{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        fs::create_dir_all(&path)?;
        Ok(path)
    }

    #[test]
    fn missing_flag_status_reports_inactive_stage4() -> Result<()> {
        let temp = test_dir("missing_status")?;
        let path = temp.join("stop.flag");

        let report = run(config(Mode::Status, path))?.output;

        assert!(!report.active);
        assert!(!report.changed);
        assert_eq!(report.reason, None);
        assert_eq!(report.verdict, "inactive_missing");
        Ok(())
    }

    #[test]
    fn active_flag_status_parses_first_non_comment_reason_stage4() -> Result<()> {
        let temp = test_dir("active_status")?;
        let path = temp.join("stop.flag");
        fs::write(
            &path,
            "\n# generated by operator\n  stop buys during incident  \nsecond line\n",
        )?;

        let report = run(config(Mode::Status, path))?.output;

        assert!(report.active);
        assert_eq!(report.reason.as_deref(), Some("stop buys during incident"));
        assert_eq!(report.verdict, "active");
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn unreadable_existing_flag_status_is_active_fail_closed_stage4() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let temp = test_dir("unreadable_status")?;
        let path = temp.join("stop.flag");
        fs::write(&path, "incident\n")?;
        let original_permissions = fs::metadata(&path)?.permissions();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o000))?;

        let report = run(config(Mode::Status, path.clone()))?.output;

        fs::set_permissions(&path, original_permissions)?;
        assert!(report.active);
        assert_eq!(report.verdict, "active_fail_closed_unreadable");
        assert!(report
            .reason
            .as_deref()
            .unwrap_or_default()
            .contains("unreadable"));
        Ok(())
    }

    #[test]
    fn activate_writes_expected_reason_stage4() -> Result<()> {
        let temp = test_dir("activate_writes")?;
        let path = temp.join("nested").join("stop.flag");
        let mut cfg = config(Mode::Activate, path.clone());
        cfg.reason = Some("incident stop".to_string());

        let report = run(cfg)?.output;

        assert!(report.active);
        assert!(report.changed);
        assert_eq!(report.reason.as_deref(), Some("incident stop"));
        assert_eq!(
            parse_operator_emergency_stop_reason(&fs::read_to_string(path)?).as_deref(),
            Some("incident stop")
        );
        Ok(())
    }

    #[test]
    fn activate_same_reason_is_idempotent_stage4() -> Result<()> {
        let temp = test_dir("activate_idempotent")?;
        let path = temp.join("stop.flag");
        fs::write(&path, "incident stop\n# existing metadata\n")?;
        let mut cfg = config(Mode::Activate, path.clone());
        cfg.reason = Some("incident stop".to_string());

        let report = run(cfg)?.output;

        assert!(report.active);
        assert!(!report.changed);
        assert_eq!(report.reason.as_deref(), Some("incident stop"));
        assert_eq!(report.verdict, "already_active_same_reason");
        assert_eq!(
            fs::read_to_string(path)?,
            "incident stop\n# existing metadata\n"
        );
        Ok(())
    }

    #[test]
    fn activate_refuses_different_reason_without_force_stage4() -> Result<()> {
        let temp = test_dir("activate_refuses")?;
        let path = temp.join("stop.flag");
        fs::write(&path, "old incident\n")?;
        let mut cfg = config(Mode::Activate, path.clone());
        cfg.reason = Some("new incident".to_string());

        let report = run(cfg)?.output;

        assert!(report.active);
        assert!(!report.changed);
        assert_eq!(report.reason.as_deref(), Some("old incident"));
        assert_eq!(report.verdict, "activate_refused_different_reason");
        assert_eq!(fs::read_to_string(path)?, "old incident\n");
        Ok(())
    }

    #[test]
    fn activate_with_force_overwrites_different_reason_stage4() -> Result<()> {
        let temp = test_dir("activate_force")?;
        let path = temp.join("stop.flag");
        fs::write(&path, "old incident\n")?;
        let mut cfg = config(Mode::Activate, path.clone());
        cfg.reason = Some("new incident".to_string());
        cfg.force = true;

        let report = run(cfg)?.output;

        assert!(report.active);
        assert!(report.changed);
        assert_eq!(report.reason.as_deref(), Some("new incident"));
        assert_eq!(report.verdict, "activated_force_overwrote_reason");
        assert_eq!(
            parse_operator_emergency_stop_reason(&fs::read_to_string(path)?).as_deref(),
            Some("new incident")
        );
        Ok(())
    }

    #[test]
    fn clear_requires_exact_confirmation_stage4() -> Result<()> {
        let temp = test_dir("clear_requires_confirmation")?;
        let path = temp.join("stop.flag");
        fs::write(&path, "incident\n")?;
        let mut cfg = config(Mode::Clear, path.clone());
        cfg.confirm_clear = Some("wrong".to_string());

        let report = run(cfg)?.output;

        assert!(report.active);
        assert!(!report.changed);
        assert_eq!(report.verdict, "clear_refused_missing_confirmation");
        assert!(path.exists());
        Ok(())
    }

    #[test]
    fn clear_is_idempotent_when_missing_stage4() -> Result<()> {
        let temp = test_dir("clear_idempotent")?;
        let path = temp.join("stop.flag");
        let mut cfg = config(Mode::Clear, path);
        cfg.confirm_clear = Some(CLEAR_CONFIRMATION.to_string());

        let report = run(cfg)?.output;

        assert!(!report.active);
        assert!(!report.changed);
        assert_eq!(report.verdict, "already_clear");
        Ok(())
    }
}
