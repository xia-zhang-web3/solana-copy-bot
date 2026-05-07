use super::{
    io::write_flag_atomic, Config, FlagStatus, Mode, OperatorEmergencyStopOutput, RunReport,
    CLEAR_CONFIRMATION,
};
use anyhow::{bail, Context, Result};
use std::{fs, path::Path};

pub(super) fn run(config: Config) -> Result<RunReport> {
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

pub(super) fn parse_operator_emergency_stop_reason(content: &str) -> Option<String> {
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        return Some(trimmed.to_string());
    }
    None
}
