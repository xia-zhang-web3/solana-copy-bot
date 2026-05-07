use super::{
    helpers::executable_flag, render_wrapper_script_contents, WrapperMetadata,
    WrapperVerificationSummary, METADATA_PREFIX, STATUS_SCHEMA_VERSION, SUPPORTED_ACTIONS,
    WRAPPER_VERSION,
};
use anyhow::{anyhow, Context, Result};
use std::{fs, path::Path};

pub fn verify_wrapper(path: &Path) -> Result<WrapperVerificationSummary> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed reading wrapper {}", path.display()))?;
    let metadata = extract_wrapper_metadata(&contents)?;
    let expected_contents =
        render_wrapper_script_contents(&metadata.backend_command, metadata.timeout_ms)?;
    let exact_content_matches_expected = contents == expected_contents;
    let executable = executable_flag(path);
    let mut mismatches = Vec::new();
    if metadata.version != WRAPPER_VERSION {
        mismatches.push(format!(
            "wrapper version must be {}, found {}",
            WRAPPER_VERSION, metadata.version
        ));
    }
    let expected_actions: Vec<String> = SUPPORTED_ACTIONS
        .iter()
        .map(|value| value.to_string())
        .collect();
    if metadata.supported_actions != expected_actions {
        mismatches.push("wrapper supported_actions do not match the bounded contract".to_string());
    }
    if metadata.status_schema_version != STATUS_SCHEMA_VERSION {
        mismatches.push(format!(
            "wrapper status_schema_version must be {}, found {}",
            STATUS_SCHEMA_VERSION, metadata.status_schema_version
        ));
    }
    if !exact_content_matches_expected {
        mismatches
            .push("wrapper contents do not match the deterministic rendered contract".to_string());
    }
    if let Some(false) = executable {
        mismatches.push("wrapper is not executable".to_string());
    }
    Ok(WrapperVerificationSummary {
        wrapper_path: path.display().to_string(),
        wrapper_version: Some(metadata.version),
        backend_command: Some(metadata.backend_command),
        timeout_ms: Some(metadata.timeout_ms),
        supported_actions: metadata.supported_actions,
        status_schema_version: Some(metadata.status_schema_version),
        executable,
        exact_content_matches_expected,
        mismatches,
    })
}

pub fn extract_wrapper_metadata(contents: &str) -> Result<WrapperMetadata> {
    let metadata_line = contents
        .lines()
        .find(|line| line.starts_with(METADATA_PREFIX))
        .ok_or_else(|| anyhow!("wrapper metadata header is missing"))?;
    let raw = metadata_line
        .strip_prefix(METADATA_PREFIX)
        .ok_or_else(|| anyhow!("wrapper metadata header is malformed"))?;
    serde_json::from_str(raw).context("failed parsing wrapper metadata header")
}
