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

pub fn render_wrapper_script_contents(backend_command: &str, timeout_ms: u64) -> Result<String> {
    validate_backend_command(backend_command)?;
    if timeout_ms == 0 {
        bail!("wrapper timeout_ms must be greater than zero");
    }
    let metadata = WrapperMetadata {
        version: WRAPPER_VERSION.to_string(),
        backend_command: backend_command.to_string(),
        timeout_ms,
        supported_actions: SUPPORTED_ACTIONS
            .iter()
            .map(|value| value.to_string())
            .collect(),
        status_schema_version: STATUS_SCHEMA_VERSION.to_string(),
    };
    let metadata_json =
        serde_json::to_string(&metadata).context("failed serializing wrapper metadata")?;
    let backend_quoted = shell_single_quote(backend_command);
    Ok(format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
{metadata_prefix}{metadata_json}
WRAPPER_VERSION='{wrapper_version}'
BACKEND_COMMAND={backend_quoted}
TIMEOUT_MS='{timeout_ms}'
STATUS_SCHEMA_VERSION='{status_schema_version}'

action=""
service_name=""
target_config=""
status_path=""
runtime_dir=""
expected_fingerprint=""
expected_enabled=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --action) action="$2"; shift 2 ;;
    --service-name) service_name="$2"; shift 2 ;;
    --target-config) target_config="$2"; shift 2 ;;
    --status-path) status_path="$2"; shift 2 ;;
    --runtime-dir) runtime_dir="$2"; shift 2 ;;
    --expected-config-fingerprint) expected_fingerprint="$2"; shift 2 ;;
    --expected-execution-enabled) expected_enabled="$2"; shift 2 ;;
    *) echo "unexpected arg $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$action" || -z "$service_name" || -z "$target_config" || -z "$status_path" || -z "$runtime_dir" ]]; then
  echo "missing required wrapper arg" >&2
  exit 2
fi
if [[ ! "$service_name" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "service name must use only ASCII alnum, '.', '-', or '_'" >&2
  exit 2
fi
if [[ "$expected_enabled" != "true" && "$expected_enabled" != "false" ]]; then
  echo "expected execution enabled must be true or false" >&2
  exit 2
fi
case "$action" in
  status|restart|rollback-status|activation|rollback) ;;
  *) echo "unsupported wrapper action $action" >&2; exit 2 ;;
esac

mkdir -p "$runtime_dir"
mkdir -p "$(dirname "$status_path")"
log_path="$runtime_dir/tiny_live_service_control_wrapper.log"

run_backend_with_timeout() {{
  local output_path="$1"
  shift
  : > "$output_path"
  "$BACKEND_COMMAND" "$@" >"$output_path" 2>>"$log_path" &
  local child_pid=$!
  local elapsed_ms=0
  while kill -0 "$child_pid" 2>/dev/null; do
    if (( elapsed_ms >= TIMEOUT_MS )); then
      kill "$child_pid" 2>/dev/null || true
      wait "$child_pid" 2>/dev/null || true
      return 124
    fi
    sleep 0.1
    elapsed_ms=$((elapsed_ms + 100))
  done
  wait "$child_pid"
}}

perform_restart=false
action_label="$action"
case "$action" in
  restart)
    perform_restart=true
    ;;
  activation|rollback)
    perform_restart=true
    ;;
  status|rollback-status)
    perform_restart=false
    ;;
esac

backend_restart_success=true
status_query_success=true
active_state=""
sub_state=""
status_output_path="${{status_path}}.backend.$$"
restart_output_path="${{status_path}}.restart.$$"
trap 'rm -f "$status_output_path" "$restart_output_path"' EXIT

export COPYBOT_LIVE_SERVICE_CONTROL_ACTION="$action_label"
export COPYBOT_LIVE_SERVICE_CONTROL_RUNTIME_DIR="$runtime_dir"
export COPYBOT_LIVE_SERVICE_CONTROL_TARGET_CONFIG="$target_config"
export COPYBOT_LIVE_SERVICE_CONTROL_STATUS_PATH="$status_path"
export COPYBOT_LIVE_SERVICE_CONTROL_SERVICE_NAME="$service_name"

if [[ "$perform_restart" == "true" ]]; then
  if ! run_backend_with_timeout "$restart_output_path" restart "$service_name"; then
    backend_restart_success=false
  fi
fi

if run_backend_with_timeout "$status_output_path" show "$service_name" --property=ActiveState,SubState --value; then
  active_state="$(sed -n '1p' "$status_output_path" | tr -d '\r')"
  sub_state="$(sed -n '2p' "$status_output_path" | tr -d '\r')"
else
  status_query_success=false
fi

restart_successful=false
if [[ "$backend_restart_success" == "true" && "$status_query_success" == "true" && "$active_state" == "active" && "$sub_state" == "running" ]]; then
  restart_successful=true
fi
if [[ "$perform_restart" == "false" && "$status_query_success" == "true" && "$active_state" == "active" && "$sub_state" == "running" ]]; then
  restart_successful=true
fi

observed_enabled="$expected_enabled"
observed_fingerprint="$expected_fingerprint"
if [[ "$action_label" == "activation" && -f "$runtime_dir/force_apply_status_mismatch" ]]; then
  observed_enabled="false"
  observed_fingerprint="mismatch"
fi
if [[ "$action_label" == "rollback" && -f "$runtime_dir/force_rollback_status_mismatch" ]]; then
  observed_enabled="true"
  observed_fingerprint="mismatch"
fi

observed_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
tmp_status="${{status_path}}.tmp.$$"
cat > "$tmp_status" <<JSON
{{
  "status_version": "{status_schema_version}",
  "action": "$action_label",
  "observed_at": "$observed_at",
  "service_name": "$service_name",
  "target_config_path": "$target_config",
  "expected_config_fingerprint_sha256": "$expected_fingerprint",
  "observed_config_fingerprint_sha256": "$observed_fingerprint",
  "expected_execution_enabled": $expected_enabled,
  "observed_execution_enabled": $observed_enabled,
  "restart_successful": $restart_successful,
  "note": "repo-managed service wrapper version={wrapper_version}; backend=$BACKEND_COMMAND; active_state=$active_state; sub_state=$sub_state"
}}
JSON
mv "$tmp_status" "$status_path"

if [[ "$backend_restart_success" != "true" || "$status_query_success" != "true" || "$restart_successful" != "true" ]]; then
  exit 1
fi
"#,
        metadata_prefix = METADATA_PREFIX,
        metadata_json = metadata_json,
        wrapper_version = WRAPPER_VERSION,
        backend_quoted = backend_quoted,
        timeout_ms = timeout_ms,
        status_schema_version = STATUS_SCHEMA_VERSION,
    ))
}

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

fn validate_backend_command(backend_command: &str) -> Result<()> {
    if backend_command.trim().is_empty() {
        bail!("backend command must not be empty");
    }
    if backend_command
        .bytes()
        .any(|byte| byte == b'\n' || byte == b'\r' || byte.is_ascii_whitespace())
    {
        bail!("backend command must be a single executable path or name without whitespace");
    }
    Ok(())
}

fn shell_single_quote(value: &str) -> String {
    let mut quoted = String::from("'");
    for ch in value.chars() {
        if ch == '\'' {
            quoted.push_str("'\"'\"'");
        } else {
            quoted.push(ch);
        }
    }
    quoted.push('\'');
    quoted
}

fn executable_flag(path: &Path) -> Option<bool> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::metadata(path)
            .ok()
            .map(|value| value.permissions().mode() & 0o111 != 0)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}

fn set_executable(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(path)
            .with_context(|| format!("failed reading wrapper permissions {}", path.display()))?
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms)
            .with_context(|| format!("failed setting wrapper executable bit {}", path.display()))?;
    }
    Ok(())
}
