use super::{
    live_service_control_wrapper_contract, render::mode_name, render::render_human_report, Config,
    Mode, WrapperReport, WrapperVerdict,
};
use anyhow::Result;
use chrono::Utc;

pub(super) fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::RenderWrapper | Mode::InstallWrapper => render_report(&config),
        Mode::VerifyWrapper => verify_report(&config),
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_human_report(&report))
    }
}

fn render_report(config: &Config) -> WrapperReport {
    let output_path = config.output_path.as_ref().expect("checked output path");
    match live_service_control_wrapper_contract::render_wrapper_script(
        output_path,
        &config.backend_command,
        config.timeout_ms,
    ) {
        Ok(()) => WrapperReport {
            generated_at: Utc::now(),
            mode: mode_name(config.mode).to_string(),
            verdict: WrapperVerdict::TinyLiveServiceControlWrapperRendered,
            reason: format!(
                "repo-managed bounded service-control wrapper rendered to {}",
                output_path.display()
            ),
            output_path: Some(output_path.display().to_string()),
            wrapper_path: Some(output_path.display().to_string()),
            wrapper_version: live_service_control_wrapper_contract::WRAPPER_VERSION.to_string(),
            backend_command: Some(config.backend_command.clone()),
            timeout_ms: Some(config.timeout_ms),
            supported_actions: live_service_control_wrapper_contract::SUPPORTED_ACTIONS
                .iter()
                .map(|value| value.to_string())
                .collect(),
            status_schema_version:
                live_service_control_wrapper_contract::STATUS_SCHEMA_VERSION.to_string(),
            executable: Some(true),
            exact_content_matches_expected: Some(true),
            mismatches: Vec::new(),
            explicit_statement:
                "this wrapper contract only manages bounded service control for activation/cutover; it does not authorize production activation by itself"
                    .to_string(),
        },
        Err(error) => WrapperReport {
            generated_at: Utc::now(),
            mode: mode_name(config.mode).to_string(),
            verdict: WrapperVerdict::TinyLiveServiceControlWrapperInstallRefused,
            reason: error.to_string(),
            output_path: Some(output_path.display().to_string()),
            wrapper_path: Some(output_path.display().to_string()),
            wrapper_version: live_service_control_wrapper_contract::WRAPPER_VERSION.to_string(),
            backend_command: Some(config.backend_command.clone()),
            timeout_ms: Some(config.timeout_ms),
            supported_actions: live_service_control_wrapper_contract::SUPPORTED_ACTIONS
                .iter()
                .map(|value| value.to_string())
                .collect(),
            status_schema_version:
                live_service_control_wrapper_contract::STATUS_SCHEMA_VERSION.to_string(),
            executable: None,
            exact_content_matches_expected: None,
            mismatches: vec![error.to_string()],
            explicit_statement:
                "wrapper install/render refusal leaves the live-target contract unchanged and still does not authorize production activation"
                    .to_string(),
        },
    }
}

fn verify_report(config: &Config) -> WrapperReport {
    let wrapper_path = config.wrapper_path.as_ref().expect("checked wrapper path");
    match live_service_control_wrapper_contract::verify_wrapper(wrapper_path) {
        Ok(summary) if summary.mismatches.is_empty() => WrapperReport {
            generated_at: Utc::now(),
            mode: mode_name(config.mode).to_string(),
            verdict: WrapperVerdict::TinyLiveServiceControlWrapperVerifyOk,
            reason: format!(
                "wrapper {} matches the deterministic bounded service-control contract",
                wrapper_path.display()
            ),
            output_path: None,
            wrapper_path: Some(summary.wrapper_path),
            wrapper_version: summary
                .wrapper_version
                .unwrap_or_else(|| live_service_control_wrapper_contract::WRAPPER_VERSION.to_string()),
            backend_command: summary.backend_command,
            timeout_ms: summary.timeout_ms,
            supported_actions: summary.supported_actions,
            status_schema_version: summary.status_schema_version.unwrap_or_else(|| {
                live_service_control_wrapper_contract::STATUS_SCHEMA_VERSION.to_string()
            }),
            executable: summary.executable,
            exact_content_matches_expected: Some(summary.exact_content_matches_expected),
            mismatches: Vec::new(),
            explicit_statement:
                "wrapper verification only proves the bounded service-control contract and still does not authorize production activation"
                    .to_string(),
        },
        Ok(summary) => WrapperReport {
            generated_at: Utc::now(),
            mode: mode_name(config.mode).to_string(),
            verdict: WrapperVerdict::TinyLiveServiceControlWrapperVerifyInvalid,
            reason: format!(
                "wrapper {} does not match the deterministic bounded service-control contract",
                wrapper_path.display()
            ),
            output_path: None,
            wrapper_path: Some(summary.wrapper_path),
            wrapper_version: summary
                .wrapper_version
                .unwrap_or_else(|| live_service_control_wrapper_contract::WRAPPER_VERSION.to_string()),
            backend_command: summary.backend_command,
            timeout_ms: summary.timeout_ms,
            supported_actions: summary.supported_actions,
            status_schema_version: summary.status_schema_version.unwrap_or_else(|| {
                live_service_control_wrapper_contract::STATUS_SCHEMA_VERSION.to_string()
            }),
            executable: summary.executable,
            exact_content_matches_expected: Some(summary.exact_content_matches_expected),
            mismatches: summary.mismatches,
            explicit_statement:
                "invalid wrapper verification must be treated as non-authorizing and unsafe for live-target activation control"
                    .to_string(),
        },
        Err(error) => WrapperReport {
            generated_at: Utc::now(),
            mode: mode_name(config.mode).to_string(),
            verdict: WrapperVerdict::TinyLiveServiceControlWrapperVerifyInvalid,
            reason: error.to_string(),
            output_path: None,
            wrapper_path: Some(wrapper_path.display().to_string()),
            wrapper_version: live_service_control_wrapper_contract::WRAPPER_VERSION.to_string(),
            backend_command: None,
            timeout_ms: None,
            supported_actions: live_service_control_wrapper_contract::SUPPORTED_ACTIONS
                .iter()
                .map(|value| value.to_string())
                .collect(),
            status_schema_version:
                live_service_control_wrapper_contract::STATUS_SCHEMA_VERSION.to_string(),
            executable: None,
            exact_content_matches_expected: Some(false),
            mismatches: vec![error.to_string()],
            explicit_statement:
                "invalid wrapper verification must be treated as non-authorizing and unsafe for live-target activation control"
                    .to_string(),
        },
    }
}
