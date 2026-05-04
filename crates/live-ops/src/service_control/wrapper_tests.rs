use super::*;
use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, serde::Deserialize)]
struct LiveServiceStatus {
    action: String,
    service_name: String,
    target_config_path: String,
    expected_config_fingerprint_sha256: String,
    observed_config_fingerprint_sha256: String,
    expected_execution_enabled: bool,
    observed_execution_enabled: bool,
    restart_successful: bool,
}

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
) -> LiveServiceStatus {
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
    serde_json::from_str::<LiveServiceStatus>(&raw).unwrap()
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
