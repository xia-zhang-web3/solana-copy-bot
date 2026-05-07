use serde::Serialize;
use std::path::PathBuf;

const DEFAULT_OPERATOR_EMERGENCY_STOP_PATH: &str = "state/operator_emergency_stop.flag";
const EMERGENCY_STOP_FILE_ENV: &str = "SOLANA_COPY_BOT_EMERGENCY_STOP_FILE";
const CLEAR_CONFIRMATION: &str = "CLEAR_OPERATOR_EMERGENCY_STOP";
const USAGE: &str = "usage:
  copybot_operator_emergency_stop --status [--path <path>] [--json]
  copybot_operator_emergency_stop --activate --reason <text> [--force] [--path <path>] [--json]
  copybot_operator_emergency_stop --clear --confirm-clear CLEAR_OPERATOR_EMERGENCY_STOP [--path <path>] [--json]";

#[path = "actions.rs"]
mod actions;
#[path = "args.rs"]
mod args;
#[path = "io.rs"]
mod io;

use self::{actions::run, args::parse_args, io::render_human};

pub fn main_entry() {
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
pub(super) enum Mode {
    Status,
    Activate,
    Clear,
}

#[derive(Debug, Clone)]
pub(super) struct Config {
    pub(super) mode: Mode,
    pub(super) path: PathBuf,
    pub(super) reason: Option<String>,
    pub(super) force: bool,
    pub(super) confirm_clear: Option<String>,
    pub(super) json: bool,
}

#[derive(Debug, Clone)]
pub(super) struct RunReport {
    pub(super) output: OperatorEmergencyStopOutput,
    pub(super) json: bool,
    pub(super) success: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(super) struct OperatorEmergencyStopOutput {
    pub(super) event: &'static str,
    pub(super) path: String,
    pub(super) active: bool,
    pub(super) changed: bool,
    pub(super) reason: Option<String>,
    pub(super) verdict: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FlagStatus {
    pub(super) active: bool,
    pub(super) reason: Option<String>,
    pub(super) verdict: String,
    pub(super) readable: bool,
}

#[cfg(test)]
use self::actions::parse_operator_emergency_stop_reason;
#[cfg(test)]
use anyhow::Result;
#[cfg(test)]
use chrono::Utc;
#[cfg(test)]
use std::{env, fs};

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
