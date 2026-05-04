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

include!("args.rs");
include!("actions.rs");
include!("io.rs");

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
