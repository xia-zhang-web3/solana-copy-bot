use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::env;
use std::path::PathBuf;

#[allow(dead_code)]
#[path = "../live_service_control_wrapper_contract.rs"]
mod live_service_control_wrapper_contract;
#[cfg(test)]
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_live_execute.rs"]
mod tiny_live_activation_live_execute_for_tests;

const USAGE: &str = "usage: copybot_live_service_control_wrapper [--json] [--backend-command <path-or-name>] [--timeout-ms <ms>] (--render-wrapper --output <path> | --install-wrapper --output <path> | --verify-wrapper --path <path>)";

fn main() -> Result<()> {
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

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut output_path: Option<PathBuf> = None;
    let mut wrapper_path: Option<PathBuf> = None;
    let mut backend_command = "systemctl".to_string();
    let mut timeout_ms = live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS;
    let mut json = false;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--output" => {
                output_path = Some(PathBuf::from(parse_string_arg("--output", args.next())?))
            }
            "--path" => {
                wrapper_path = Some(PathBuf::from(parse_string_arg("--path", args.next())?))
            }
            "--backend-command" => {
                backend_command = parse_string_arg("--backend-command", args.next())?
            }
            "--timeout-ms" => timeout_ms = parse_u64_arg("--timeout-ms", args.next())?,
            "--json" => json = true,
            "--render-wrapper" => set_mode(&mut mode, Mode::RenderWrapper, "--render-wrapper")?,
            "--install-wrapper" => set_mode(&mut mode, Mode::InstallWrapper, "--install-wrapper")?,
            "--verify-wrapper" => set_mode(&mut mode, Mode::VerifyWrapper, "--verify-wrapper")?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unrecognized argument: {other}"),
        }
    }

    let mode = mode.ok_or_else(|| anyhow!("missing required mode"))?;
    match mode {
        Mode::RenderWrapper | Mode::InstallWrapper => {
            if output_path.is_none() {
                bail!("missing required --output for wrapper render/install");
            }
        }
        Mode::VerifyWrapper => {
            if wrapper_path.is_none() {
                bail!("missing required --path for --verify-wrapper");
            }
        }
    }

    Ok(Some(Config {
        mode,
        output_path,
        wrapper_path,
        backend_command,
        timeout_ms,
        json,
    }))
}

fn run(config: Config) -> Result<String> {
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
                "this wrapper contract only manages bounded service control for tiny-live activation/cutover; it does not authorize production activation by itself"
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

fn render_human_report(report: &WrapperReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!(
            "verdict={}",
            serde_json::to_string(&report.verdict)
                .unwrap()
                .trim_matches('"')
        ),
        format!("reason={}", report.reason),
    ];
    if let Some(path) = &report.output_path {
        lines.push(format!("output_path={path}"));
    }
    if let Some(path) = &report.wrapper_path {
        lines.push(format!("wrapper_path={path}"));
    }
    lines.push(format!("wrapper_version={}", report.wrapper_version));
    if let Some(backend_command) = &report.backend_command {
        lines.push(format!("backend_command={backend_command}"));
    }
    if let Some(timeout_ms) = report.timeout_ms {
        lines.push(format!("timeout_ms={timeout_ms}"));
    }
    lines.push(format!(
        "supported_actions={}",
        report.supported_actions.join(",")
    ));
    lines.push(format!(
        "status_schema_version={}",
        report.status_schema_version
    ));
    if let Some(executable) = report.executable {
        lines.push(format!("executable={executable}"));
    }
    if let Some(exact) = report.exact_content_matches_expected {
        lines.push(format!("exact_content_matches_expected={exact}"));
    }
    if !report.mismatches.is_empty() {
        lines.push(format!("mismatches={}", report.mismatches.join(" | ")));
    }
    lines.push(format!("note={}", report.explicit_statement));
    lines.join("\n")
}

fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::RenderWrapper => "render_wrapper",
        Mode::InstallWrapper => "install_wrapper",
        Mode::VerifyWrapper => "verify_wrapper",
    }
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    match value {
        Some(value) if !value.trim().is_empty() => Ok(value),
        _ => bail!("missing value for {flag}"),
    }
}

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    parse_string_arg(flag, value)?
        .parse::<u64>()
        .map_err(|error| anyhow!("invalid value for {flag}: {error}"))
}

fn set_mode(mode: &mut Option<Mode>, value: Mode, flag: &str) -> Result<()> {
    if mode.replace(value).is_some() {
        bail!("multiple modes supplied; {flag} conflicts with an earlier mode");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs;
    use std::path::Path;
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn render_and_verify_wrapper_round_trip() {
        let fixture_dir = unique_test_dir("render_verify");
        let wrapper_path = fixture_dir.join("service-control-wrapper.sh");
        let backend_path = fixture_dir.join("fake-systemctl.sh");
        write_fake_backend(&backend_path);

        let output = run(parse_args_from([
            "--render-wrapper".to_string(),
            "--output".to_string(),
            wrapper_path.display().to_string(),
            "--backend-command".to_string(),
            backend_path.display().to_string(),
            "--json".to_string(),
        ])
        .unwrap()
        .unwrap())
        .unwrap();
        let report: WrapperReport = serde_json::from_str(&output).unwrap();
        assert_eq!(
            report.verdict,
            WrapperVerdict::TinyLiveServiceControlWrapperRendered
        );

        let verify_output = run(parse_args_from([
            "--verify-wrapper".to_string(),
            "--path".to_string(),
            wrapper_path.display().to_string(),
            "--json".to_string(),
        ])
        .unwrap()
        .unwrap())
        .unwrap();
        let verify: WrapperReport = serde_json::from_str(&verify_output).unwrap();
        assert_eq!(
            verify.verdict,
            WrapperVerdict::TinyLiveServiceControlWrapperVerifyOk
        );
    }

    #[test]
    fn verify_wrapper_rejects_tampered_content() {
        let fixture_dir = unique_test_dir("verify_tampered");
        let wrapper_path = fixture_dir.join("service-control-wrapper.sh");
        let backend_path = fixture_dir.join("fake-systemctl.sh");
        write_fake_backend(&backend_path);
        live_service_control_wrapper_contract::render_wrapper_script(
            &wrapper_path,
            &backend_path.display().to_string(),
            live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
        )
        .unwrap();
        let mut contents = fs::read_to_string(&wrapper_path).unwrap();
        contents.push_str("\n# tampered\n");
        fs::write(&wrapper_path, contents).unwrap();

        let verify_output = run(parse_args_from([
            "--verify-wrapper".to_string(),
            "--path".to_string(),
            wrapper_path.display().to_string(),
            "--json".to_string(),
        ])
        .unwrap()
        .unwrap())
        .unwrap();
        let verify: WrapperReport = serde_json::from_str(&verify_output).unwrap();
        assert_eq!(
            verify.verdict,
            WrapperVerdict::TinyLiveServiceControlWrapperVerifyInvalid
        );
        assert!(verify
            .mismatches
            .iter()
            .any(|value| value.contains("deterministic rendered contract")));
    }

    #[test]
    fn rendered_wrapper_reports_status_json_in_expected_schema() {
        let fixture_dir = unique_test_dir("status_schema");
        let wrapper_path = fixture_dir.join("service-control-wrapper.sh");
        let backend_path = fixture_dir.join("fake-systemctl.sh");
        let runtime_dir = fixture_dir.join("runtime");
        let target_config_path = fixture_dir.join("live.server.toml");
        let status_path = runtime_dir.join("status.json");
        write_fake_backend(&backend_path);
        live_service_control_wrapper_contract::render_wrapper_script(
            &wrapper_path,
            &backend_path.display().to_string(),
            live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
        )
        .unwrap();
        fs::create_dir_all(&runtime_dir).unwrap();
        fs::write(&target_config_path, "execution.enabled = true\n").unwrap();

        let status = invoke_wrapper(
            &wrapper_path,
            "status",
            "solana-copy-bot.service",
            &target_config_path,
            &runtime_dir,
            &status_path,
            "expected-fingerprint",
            true,
        );

        assert!(status.restart_successful);
        assert_eq!(status.action, "status");
        assert_eq!(status.service_name, "solana-copy-bot.service");
        assert_eq!(
            status.target_config_path,
            target_config_path.display().to_string()
        );
        assert_eq!(
            status.expected_config_fingerprint_sha256,
            "expected-fingerprint"
        );
        assert_eq!(
            status.observed_config_fingerprint_sha256,
            "expected-fingerprint"
        );
        assert!(status.expected_execution_enabled);
        assert!(status.observed_execution_enabled);
    }

    #[test]
    fn rendered_wrapper_performs_bounded_restart_against_fake_backend() {
        let fixture_dir = unique_test_dir("restart_backend");
        let wrapper_path = fixture_dir.join("service-control-wrapper.sh");
        let backend_path = fixture_dir.join("fake-systemctl.sh");
        let runtime_dir = fixture_dir.join("runtime");
        let target_config_path = fixture_dir.join("live.server.toml");
        let status_path = runtime_dir.join("status.json");
        write_fake_backend(&backend_path);
        live_service_control_wrapper_contract::render_wrapper_script(
            &wrapper_path,
            &backend_path.display().to_string(),
            live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
        )
        .unwrap();
        fs::create_dir_all(&runtime_dir).unwrap();
        fs::write(&target_config_path, "execution.enabled = true\n").unwrap();

        let status = invoke_wrapper(
            &wrapper_path,
            "activation",
            "solana-copy-bot.service",
            &target_config_path,
            &runtime_dir,
            &status_path,
            "expected-fingerprint",
            true,
        );

        let backend_log = fs::read_to_string(runtime_dir.join("backend.log")).unwrap();
        assert!(backend_log.contains("restart:solana-copy-bot.service:activation"));
        assert!(backend_log.contains("show:solana-copy-bot.service:activation"));
        assert!(status.restart_successful);
    }

    #[test]
    fn invalid_service_name_is_refused_without_status_artifact() {
        let fixture_dir = unique_test_dir("invalid_service");
        let wrapper_path = fixture_dir.join("service-control-wrapper.sh");
        let backend_path = fixture_dir.join("fake-systemctl.sh");
        let runtime_dir = fixture_dir.join("runtime");
        let target_config_path = fixture_dir.join("live.server.toml");
        let status_path = runtime_dir.join("status.json");
        write_fake_backend(&backend_path);
        live_service_control_wrapper_contract::render_wrapper_script(
            &wrapper_path,
            &backend_path.display().to_string(),
            live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
        )
        .unwrap();
        fs::create_dir_all(&runtime_dir).unwrap();
        fs::write(&target_config_path, "execution.enabled = true\n").unwrap();

        let output = Command::new(&wrapper_path)
            .arg("--action")
            .arg("status")
            .arg("--service-name")
            .arg("../escape")
            .arg("--target-config")
            .arg(&target_config_path)
            .arg("--runtime-dir")
            .arg(&runtime_dir)
            .arg("--status-path")
            .arg(&status_path)
            .arg("--expected-config-fingerprint")
            .arg("expected-fingerprint")
            .arg("--expected-execution-enabled")
            .arg("true")
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(!status_path.exists());
    }

    fn invoke_wrapper(
        wrapper_path: &Path,
        action: &str,
        service_name: &str,
        target_config_path: &Path,
        runtime_dir: &Path,
        status_path: &Path,
        expected_fingerprint: &str,
        expected_enabled: bool,
    ) -> tiny_live_activation_live_execute_for_tests::LiveServiceStatus {
        let output = Command::new(wrapper_path)
            .arg("--action")
            .arg(action)
            .arg("--service-name")
            .arg(service_name)
            .arg("--target-config")
            .arg(target_config_path)
            .arg("--runtime-dir")
            .arg(runtime_dir)
            .arg("--status-path")
            .arg(status_path)
            .arg("--expected-config-fingerprint")
            .arg(expected_fingerprint)
            .arg("--expected-execution-enabled")
            .arg(expected_enabled.to_string())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "wrapper failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let raw = fs::read_to_string(status_path).unwrap();
        serde_json::from_str::<tiny_live_activation_live_execute_for_tests::LiveServiceStatus>(&raw)
            .unwrap()
    }

    fn write_fake_backend(path: &Path) {
        let script = r#"#!/usr/bin/env bash
set -euo pipefail
cmd="${1:-}"
service="${2:-}"
runtime_dir="${COPYBOT_LIVE_SERVICE_CONTROL_RUNTIME_DIR:-}"
action="${COPYBOT_LIVE_SERVICE_CONTROL_ACTION:-}"
mkdir -p "$runtime_dir"
case "$cmd" in
  restart)
    if [[ "$action" == "activation" && -f "$runtime_dir/force_apply_exit_failure" ]]; then
      echo "forced activation restart failure" >&2
      exit 7
    fi
    if [[ "$action" == "rollback" && -f "$runtime_dir/force_rollback_exit_failure" ]]; then
      echo "forced rollback restart failure" >&2
      exit 8
    fi
    echo "restart:$service:$action" >> "$runtime_dir/backend.log"
    ;;
  show)
    echo "show:$service:$action" >> "$runtime_dir/backend.log"
    if [[ -f "$runtime_dir/force_status_failure" ]]; then
      echo "forced status failure" >&2
      exit 9
    fi
    if [[ -f "$runtime_dir/force_inactive_status" ]]; then
      printf 'inactive\ndead\n'
      exit 0
    fi
    printf 'active\nrunning\n'
    ;;
  *)
    echo "unexpected backend command $cmd" >&2
    exit 2
    ;;
esac
"#;
        fs::write(path, script).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(path).unwrap().permissions();
            perms.set_mode(0o755);
            fs::set_permissions(path, perms).unwrap();
        }
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = env::temp_dir().join(format!(
            "copybot_live_service_control_wrapper_{name}_{}_{}",
            std::process::id(),
            unique
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }
}
