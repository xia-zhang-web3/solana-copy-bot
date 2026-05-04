use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::env;
use std::path::PathBuf;

use super::wrapper_contract as live_service_control_wrapper_contract;

const USAGE: &str = "usage: copybot_live_service_control_wrapper [--json] [--backend-command <path-or-name>] [--timeout-ms <ms>] (--render-wrapper --output <path> | --install-wrapper --output <path> | --verify-wrapper --path <path>)";

pub fn main_entry() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let output = run(config)?;
    println!("{output}");
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    mode: Mode,
    output_path: Option<PathBuf>,
    wrapper_path: Option<PathBuf>,
    backend_command: String,
    timeout_ms: u64,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    RenderWrapper,
    InstallWrapper,
    VerifyWrapper,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum WrapperVerdict {
    TinyLiveServiceControlWrapperRendered,
    TinyLiveServiceControlWrapperVerifyOk,
    TinyLiveServiceControlWrapperVerifyInvalid,
    TinyLiveServiceControlWrapperInstallRefused,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WrapperReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: WrapperVerdict,
    reason: String,
    output_path: Option<String>,
    wrapper_path: Option<String>,
    wrapper_version: String,
    backend_command: Option<String>,
    timeout_ms: Option<u64>,
    supported_actions: Vec<String>,
    status_schema_version: String,
    executable: Option<bool>,
    exact_content_matches_expected: Option<bool>,
    mismatches: Vec<String>,
    explicit_statement: String,
}

include!("wrapper_args.rs");
include!("wrapper_reports.rs");
include!("wrapper_render.rs");

#[cfg(test)]
#[path = "wrapper_tests.rs"]
mod tests;
