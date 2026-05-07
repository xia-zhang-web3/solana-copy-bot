use super::{
    helpers::{shell_single_quote, validate_backend_command},
    WrapperMetadata, METADATA_PREFIX, STATUS_SCHEMA_VERSION, SUPPORTED_ACTIONS, WRAPPER_VERSION,
};
use anyhow::{bail, Context, Result};

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
log_path="$runtime_dir/bounded_service_control_wrapper.log"

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
