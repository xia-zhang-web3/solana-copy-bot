use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

pub const WRAPPER_VERSION: &str = "1";
pub const STATUS_SCHEMA_VERSION: &str = "1";
pub const DEFAULT_TIMEOUT_MS: u64 = 10_000;
pub const SUPPORTED_ACTIONS: [&str; 5] = [
    "status",
    "restart",
    "rollback-status",
    "activation",
    "rollback",
];
const METADATA_PREFIX: &str = "# copybot_live_service_control_wrapper_metadata=";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WrapperMetadata {
    pub version: String,
    pub backend_command: String,
    pub timeout_ms: u64,
    pub supported_actions: Vec<String>,
    pub status_schema_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WrapperVerificationSummary {
    pub wrapper_path: String,
    pub wrapper_version: Option<String>,
    pub backend_command: Option<String>,
    pub timeout_ms: Option<u64>,
    pub supported_actions: Vec<String>,
    pub status_schema_version: Option<String>,
    pub executable: Option<bool>,
    pub exact_content_matches_expected: bool,
    pub mismatches: Vec<String>,
}

pub fn render_wrapper_script(
    output_path: &Path,
    backend_command: &str,
    timeout_ms: u64,
) -> Result<()> {
    if output_path.exists() {
        bail!(
            "refusing to overwrite existing service-control wrapper {}",
            output_path.display()
        );
    }
    let contents = render_wrapper_script_contents(backend_command, timeout_ms)?;
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed creating parent directory for service-control wrapper {}",
                output_path.display()
            )
        })?;
    }
    fs::write(output_path, contents).with_context(|| {
        format!(
            "failed writing service-control wrapper {}",
            output_path.display()
        )
    })?;
    set_executable(output_path)?;
    Ok(())
}

include!("wrapper_contract_render.rs");
include!("wrapper_contract_verify.rs");
include!("wrapper_contract_helpers.rs");
