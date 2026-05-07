use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use super::wrapper_contract as live_service_control_wrapper_contract;

const USAGE: &str = "usage: copybot_live_service_control_wrapper [--json] [--backend-command <path-or-name>] [--timeout-ms <ms>] (--render-wrapper --output <path> | --install-wrapper --output <path> | --verify-wrapper --path <path>)";

#[path = "wrapper_args.rs"]
mod args;
#[path = "wrapper_render.rs"]
mod render;
#[path = "wrapper_reports.rs"]
mod reports;

#[cfg(test)]
use self::args::parse_args_from;
use self::{args::parse_args, reports::run};

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
pub(super) struct Config {
    pub(super) mode: Mode,
    pub(super) output_path: Option<PathBuf>,
    pub(super) wrapper_path: Option<PathBuf>,
    pub(super) backend_command: String,
    pub(super) timeout_ms: u64,
    pub(super) json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Mode {
    RenderWrapper,
    InstallWrapper,
    VerifyWrapper,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum WrapperVerdict {
    TinyLiveServiceControlWrapperRendered,
    TinyLiveServiceControlWrapperVerifyOk,
    TinyLiveServiceControlWrapperVerifyInvalid,
    TinyLiveServiceControlWrapperInstallRefused,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct WrapperReport {
    pub(super) generated_at: DateTime<Utc>,
    pub(super) mode: String,
    pub(super) verdict: WrapperVerdict,
    pub(super) reason: String,
    pub(super) output_path: Option<String>,
    pub(super) wrapper_path: Option<String>,
    pub(super) wrapper_version: String,
    pub(super) backend_command: Option<String>,
    pub(super) timeout_ms: Option<u64>,
    pub(super) supported_actions: Vec<String>,
    pub(super) status_schema_version: String,
    pub(super) executable: Option<bool>,
    pub(super) exact_content_matches_expected: Option<bool>,
    pub(super) mismatches: Vec<String>,
    pub(super) explicit_statement: String,
}

#[cfg(test)]
#[path = "wrapper_tests.rs"]
mod tests;
