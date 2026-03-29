use chrono::{Duration, Utc};
use copybot_storage::{
    DiscoveryWalletFreshnessCaptureWrite, ExecutionDryRunRehearsalWrite, SqliteStore,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const EXECUTE_BIN: &str = env!("CARGO_BIN_EXE_copybot_tiny_live_activation_execute");
const PACKAGE_BIN: &str = env!("CARGO_BIN_EXE_copybot_tiny_live_activation_package");
const PACKAGE_DEPLOY_BIN: &str = env!("CARGO_BIN_EXE_copybot_tiny_live_activation_package_deploy");
const LAUNCH_PACKET_BIN: &str =
    env!("CARGO_BIN_EXE_copybot_tiny_live_activation_package_launch_packet");
const TURN_GREEN_BIN: &str = env!("CARGO_BIN_EXE_copybot_tiny_live_activation_package_turn_green");

#[test]
fn green_run_then_verify_stays_executable_now() {
    let fixture = fixture("tiny_live_turn_green_it_green", GateState::Green);
    let run = run_turn_green(&fixture);
    assert_eq!(
        run["verdict"].as_str(),
        Some("tiny_live_package_turn_green_executable_now"),
        "{run:#}"
    );
    assert_eq!(run["result"].as_str(), Some("executable_now"));
    assert_eq!(run["executable_now"].as_bool(), Some(true));

    let verify = verify_turn_green(&fixture);
    assert_eq!(
        verify["verdict"].as_str(),
        Some("tiny_live_package_turn_green_verify_ok"),
        "{verify:#}"
    );
    assert_eq!(verify["result"].as_str(), Some("executable_now"));
    assert_eq!(verify["executable_now"].as_bool(), Some(true));
}

#[test]
fn current_stage3_non_green_yields_refused_now() {
    let fixture = fixture("tiny_live_turn_green_it_stage3", GateState::Stage3Blocked);
    let run = run_turn_green(&fixture);
    assert_eq!(
        run["verdict"].as_str(),
        Some("tiny_live_package_turn_green_refused_now_by_stage3"),
        "{run:#}"
    );
    assert_eq!(run["result"].as_str(), Some("refused_now_by_stage3"));
    assert_eq!(run["executable_now"].as_bool(), Some(false));
}

#[test]
fn current_pre_activation_non_green_yields_refused_now() {
    let fixture = fixture(
        "tiny_live_turn_green_it_pre_gate",
        GateState::PreActivationBlocked,
    );
    let run = run_turn_green(&fixture);
    assert_eq!(
        run["verdict"].as_str(),
        Some("tiny_live_package_turn_green_refused_now_by_pre_activation_gate"),
        "{run:#}"
    );
    assert_eq!(
        run["result"].as_str(),
        Some("refused_now_by_pre_activation_gate")
    );
    assert_eq!(run["executable_now"].as_bool(), Some(false));
}

#[test]
fn tampered_launch_packet_controller_summary_yields_invalid_or_drifted_refusal() {
    let fixture = fixture("tiny_live_turn_green_it_tampered_packet", GateState::Green);
    let controller_path = fixture
        .launch_packet_session_dir
        .join("tiny_live_activation_package_launch_packet.controller.report.json");
    let mut controller: Value = load_json(&controller_path);
    controller["run_live_cutover_command_summary"] =
        Value::String("tampered live cutover command".to_string());
    write_json(&controller_path, &controller);

    let run = run_turn_green(&fixture);
    assert_eq!(
        run["verdict"].as_str(),
        Some("tiny_live_package_turn_green_refused_now_by_invalid_or_drifted_contract"),
        "{run:#}"
    );
    assert_eq!(
        run["result"].as_str(),
        Some("refused_now_by_invalid_or_drifted_contract")
    );
}

#[test]
fn verify_rejects_tampered_step_path() {
    let fixture = fixture("tiny_live_turn_green_it_verify_tampered", GateState::Green);
    let _ = run_turn_green(&fixture);
    let status_path = fixture
        .turn_green_session_dir
        .join("tiny_live_activation_package_turn_green.status.json");
    let mut status: Value = load_json(&status_path);
    let foreign = fixture
        .fixture_dir
        .join("foreign.launch-packet.report.json");
    fs::write(&foreign, "{}").unwrap();
    status["frozen_launch_packet_step"]["path"] = Value::String(foreign.display().to_string());
    write_json(&status_path, &status);

    let verify = verify_turn_green(&fixture);
    assert_eq!(
        verify["verdict"].as_str(),
        Some("tiny_live_package_turn_green_verify_invalid")
    );
}

#[derive(Clone, Copy)]
enum GateState {
    Green,
    Stage3Blocked,
    PreActivationBlocked,
}

struct Fixture {
    fixture_dir: PathBuf,
    launch_packet_session_dir: PathBuf,
    turn_green_session_dir: PathBuf,
    _rpc_server: MockHttpServer,
    _adapter_server: MockHttpServer,
}

fn fixture(label: &str, gate_state: GateState) -> Fixture {
    let fixture_dir = temp_dir(label);
    let install_root = fixture_dir.join("live-root");
    let target_config_path = install_root.join("etc/solana-copy-bot/live.server.toml");
    fs::create_dir_all(target_config_path.parent().unwrap()).unwrap();

    let db_path = fixture_dir.join("live.db");
    let rpc_server = MockHttpServer::spawn(
        8,
        serde_json::json!({"jsonrpc":"2.0","id":1,"result":123456u64}).to_string(),
    );
    let adapter_server = MockHttpServer::spawn(
        8,
        serde_json::json!({
            "status": "ok",
            "route": "jito",
            "contract_version": "v1",
            "detail": "simulated"
        })
        .to_string(),
    );
    let backend_path = fixture_dir.join("fake-live-backend.sh");
    write_fake_backend(&backend_path);

    let disabled_contents = dynamic_live_config_toml(
        &db_path,
        &rpc_server.url(""),
        &adapter_server.url("/submit"),
        false,
    );
    fs::write(&target_config_path, &disabled_contents).unwrap();
    seed_current_gate_state(&db_path, gate_state);

    let source_dir = fixture_dir.join("source");
    fs::create_dir_all(&source_dir).unwrap();
    let activation_config_source_path = source_dir.join("rendered.activation.toml");
    let rollback_config_source_path = source_dir.join("rendered.rollback.toml");
    write_rendered_artifact_pair(
        &target_config_path,
        &activation_config_source_path,
        &rollback_config_source_path,
        &db_path,
        &rpc_server.url(""),
        &adapter_server.url("/submit"),
    );

    let package_dir = fixture_dir.join("package");
    let package_export = run_bin_json(
        PACKAGE_BIN,
        &[
            "--install-root",
            &install_root.display().to_string(),
            "--target-service",
            "solana-copy-bot.service",
            "--backend-command",
            &backend_path.display().to_string(),
            "--activation-config-source",
            &activation_config_source_path.display().to_string(),
            "--rollback-config-source",
            &rollback_config_source_path.display().to_string(),
            "--output-dir",
            &package_dir.display().to_string(),
            "--export-package",
            "--json",
        ],
    );
    assert_eq!(
        package_export["verdict"].as_str(),
        Some("tiny_live_activation_package_exported"),
        "{package_export:#}"
    );
    assert!(
        package_dir.is_dir(),
        "package dir missing: {}",
        package_dir.display()
    );

    let package_deploy = run_bin_json(
        PACKAGE_DEPLOY_BIN,
        &[
            "--package-dir",
            &package_dir.display().to_string(),
            "--install-root",
            &install_root.display().to_string(),
            "--target-service",
            "solana-copy-bot.service",
            "--backend-command",
            &backend_path.display().to_string(),
            "--install-from-package",
            "--json",
        ],
    );
    assert_eq!(
        package_deploy["verdict"].as_str(),
        Some("tiny_live_package_deploy_install_completed"),
        "{package_deploy:#}"
    );

    let launch_packet_session_dir = fixture_dir.join("launch-packet-session");
    let launch_packet = run_bin_json(
        LAUNCH_PACKET_BIN,
        &[
            "--package-dir",
            &package_dir.display().to_string(),
            "--install-root",
            &install_root.display().to_string(),
            "--target-service",
            "solana-copy-bot.service",
            "--backend-command",
            &backend_path.display().to_string(),
            "--session-dir",
            &launch_packet_session_dir.display().to_string(),
            "--run-live-package-launch-packet",
            "--json",
        ],
    );
    assert!(
        matches!(
            launch_packet["verdict"].as_str(),
            Some("tiny_live_package_launch_packet_eligible_when_gate_turns_green")
                | Some("tiny_live_package_launch_packet_refused_by_stage3")
                | Some("tiny_live_package_launch_packet_refused_by_pre_activation_gate")
        ),
        "{launch_packet:#}"
    );
    assert!(
        launch_packet_session_dir
            .join("tiny_live_activation_package_launch_packet.status.json")
            .is_file(),
        "launch packet status missing under {}",
        launch_packet_session_dir.display()
    );

    Fixture {
        fixture_dir: fixture_dir.clone(),
        launch_packet_session_dir,
        turn_green_session_dir: fixture_dir.join("turn-green-session"),
        _rpc_server: rpc_server,
        _adapter_server: adapter_server,
    }
}

fn run_turn_green(fixture: &Fixture) -> Value {
    run_bin_json(
        TURN_GREEN_BIN,
        &[
            "--launch-packet-session-dir",
            &fixture.launch_packet_session_dir.display().to_string(),
            "--session-dir",
            &fixture.turn_green_session_dir.display().to_string(),
            "--run-live-package-turn-green",
            "--json",
        ],
    )
}

fn verify_turn_green(fixture: &Fixture) -> Value {
    run_bin_json(
        TURN_GREEN_BIN,
        &[
            "--launch-packet-session-dir",
            &fixture.launch_packet_session_dir.display().to_string(),
            "--session-dir",
            &fixture.turn_green_session_dir.display().to_string(),
            "--verify-live-package-turn-green",
            "--json",
        ],
    )
}

fn write_rendered_artifact_pair(
    target_config_path: &Path,
    activation_path: &Path,
    rollback_path: &Path,
    db_path: &Path,
    rpc_url: &str,
    adapter_url: &str,
) {
    let plan = run_bin_json(
        EXECUTE_BIN,
        &[
            "--config",
            &target_config_path.display().to_string(),
            "--plan",
            "--json",
        ],
    );
    let fingerprint_scope = plan["source_config_fingerprint_scope"]
        .as_str()
        .unwrap()
        .to_string();
    let fingerprint_sha256 = plan["source_config_fingerprint_sha256"]
        .as_str()
        .unwrap()
        .to_string();

    write_rendered_artifact(
        activation_path,
        "activation",
        dynamic_live_config_toml(db_path, rpc_url, adapter_url, true),
        target_config_path,
        &fingerprint_scope,
        &fingerprint_sha256,
        false,
        true,
    );
    write_rendered_artifact(
        rollback_path,
        "rollback",
        dynamic_live_config_toml(db_path, rpc_url, adapter_url, false),
        target_config_path,
        &fingerprint_scope,
        &fingerprint_sha256,
        false,
        false,
    );
}

fn write_rendered_artifact(
    path: &Path,
    render_kind: &str,
    contents: String,
    source_config_path: &Path,
    source_fingerprint_scope: &str,
    source_fingerprint_sha256: &str,
    source_execution_enabled: bool,
    target_execution_enabled: bool,
) {
    fs::write(path, &contents).unwrap();
    let rendered_sha = format!("{:x}", Sha256::digest(contents.as_bytes()));
    let metadata = serde_json::json!({
        "metadata_version": "1",
        "render_kind": render_kind,
        "generated_at": "2026-03-28T12:00:00Z",
        "input_config_path": source_config_path.display().to_string(),
        "output_config_path": path.display().to_string(),
        "source_config_fingerprint_scope": source_fingerprint_scope,
        "source_config_fingerprint_sha256": source_fingerprint_sha256,
        "expected_source_fingerprint_sha256": source_fingerprint_sha256,
        "rendered_config_sha256": rendered_sha,
        "pre_activation_gate_verdict": "pre_activation_gates_green",
        "pre_activation_gate_reason": "green",
        "activation_plan_verdict": "activation_plan_ready_when_stage_gate_allows",
        "activation_plan_reason": "ready",
        "activation_overlay_complete": true,
        "rollback_plan_complete": true,
        "service_restart_contract_complete": true,
        "field_expectations": [{
            "field": "execution.enabled",
            "source_value": source_execution_enabled,
            "target_value": target_execution_enabled,
            "reason": "test",
            "source": "test"
        }],
        "execution_untouched_by_batch": true,
        "activation_authorized": false,
        "not_authorized_summary": "test"
    });
    let metadata_path = path.with_file_name(format!(
        "{}.tiny_live_activation_execute.metadata.json",
        path.file_name().unwrap().to_string_lossy()
    ));
    write_json(&metadata_path, &metadata);
}

fn seed_current_gate_state(db_path: &Path, gate_state: GateState) {
    let store = SqliteStore::open(db_path).unwrap();
    let now = Utc::now();
    match gate_state {
        GateState::Green => {
            append_wallet_freshness_capture(
                &store,
                now - Duration::minutes(5),
                "validated_current",
                "fresh_current",
                true,
            );
            append_wallet_freshness_capture(
                &store,
                now - Duration::minutes(15),
                "validated_current",
                "fresh_current",
                true,
            );
            append_wallet_freshness_capture(
                &store,
                now - Duration::minutes(25),
                "validated_current",
                "fresh_current",
                true,
            );
            store
                .append_execution_dry_run_rehearsal(&rehearsal_write(
                    now - Duration::minutes(3),
                    "rehearsal_green",
                ))
                .unwrap();
            store
                .append_execution_dry_run_rehearsal(&rehearsal_write(
                    now - Duration::minutes(12),
                    "rehearsal_green_with_business_reject",
                ))
                .unwrap();
        }
        GateState::PreActivationBlocked => {
            append_wallet_freshness_capture(
                &store,
                now - Duration::minutes(5),
                "validated_current",
                "fresh_current",
                true,
            );
            append_wallet_freshness_capture(
                &store,
                now - Duration::minutes(15),
                "validated_current",
                "fresh_current",
                true,
            );
            append_wallet_freshness_capture(
                &store,
                now - Duration::minutes(25),
                "validated_current",
                "fresh_current",
                true,
            );
            store
                .append_execution_dry_run_rehearsal(&rehearsal_write(
                    now - Duration::days(2),
                    "stale_rehearsal",
                ))
                .unwrap();
        }
        GateState::Stage3Blocked => {
            append_wallet_freshness_capture(
                &store,
                now - Duration::minutes(5),
                "publication_drifting",
                "drifting_but_acceptable",
                false,
            );
            append_wallet_freshness_capture(
                &store,
                now - Duration::minutes(15),
                "publication_drifting",
                "drifting_but_acceptable",
                false,
            );
            append_wallet_freshness_capture(
                &store,
                now - Duration::minutes(25),
                "publication_drifting",
                "drifting_but_acceptable",
                false,
            );
        }
    }
}

fn append_wallet_freshness_capture(
    store: &SqliteStore,
    captured_at: chrono::DateTime<Utc>,
    history_verdict: &str,
    audit_verdict: &str,
    exact_match: bool,
) {
    store
        .append_discovery_wallet_freshness_capture(&DiscoveryWalletFreshnessCaptureWrite {
            captured_at,
            recent_cycles: 3,
            verdict: history_verdict.to_string(),
            reason: "seed".to_string(),
            publication_age_seconds: Some(60),
            raw_truth_sufficient: true,
            raw_truth_reason: "full_scoring_window_raw_truth_available".to_string(),
            shadow_signal_verdict: "shadow_signals_present_but_concentrated".to_string(),
            shadow_signal_reason: "seed-shadow".to_string(),
            published_wallet_ids: vec!["wallet-alpha".to_string(), "wallet-beta".to_string()],
            active_follow_wallet_ids: vec!["wallet-alpha".to_string(), "wallet-beta".to_string()],
            current_raw_top_wallet_ids: vec![
                "wallet-alpha".to_string(),
                "wallet-beta".to_string(),
            ],
            audit_json: serde_json::json!({
                "now": captured_at,
                "window_start": captured_at - Duration::days(5),
                "verdict": audit_verdict,
                "reason": "seed",
                "follow_top_n": 2,
                "publication_truth_available": true,
                "publication_runtime_mode": "healthy",
                "publication_recent_under_gate": true,
                "latest_publication_ts": captured_at,
                "publication_age_seconds": 60,
                "latest_publication_window_start": captured_at - Duration::days(5),
                "published_scoring_source": "raw_window_persisted_stream",
                "published_wallet_ids": ["wallet-alpha", "wallet-beta"],
                "active_follow_wallet_ids": ["wallet-alpha", "wallet-beta"],
                "current_raw_top_wallet_ids": ["wallet-alpha", "wallet-beta"],
                "published_vs_current_raw": {
                    "left_count": 2,
                    "right_count": 2,
                    "overlap_count": if exact_match { 2 } else { 1 },
                    "exact_match": exact_match,
                    "only_left": if exact_match { serde_json::json!([]) } else { serde_json::json!(["wallet-beta"]) },
                    "only_right": if exact_match { serde_json::json!([]) } else { serde_json::json!(["wallet-gamma"]) }
                },
                "active_follow_vs_current_raw": {
                    "left_count": 2,
                    "right_count": 2,
                    "overlap_count": if exact_match { 2 } else { 1 },
                    "exact_match": exact_match,
                    "only_left": if exact_match { serde_json::json!([]) } else { serde_json::json!(["wallet-beta"]) },
                    "only_right": if exact_match { serde_json::json!([]) } else { serde_json::json!(["wallet-gamma"]) }
                },
                "active_follow_vs_published": {
                    "left_count": 2,
                    "right_count": 2,
                    "overlap_count": 2,
                    "exact_match": true,
                    "only_left": [],
                    "only_right": []
                },
                "raw_truth": {
                    "sufficient": true,
                    "reason": "full_scoring_window_raw_truth_available",
                    "observed_swaps_loaded": 10,
                    "eligible_wallet_count": 2,
                    "top_wallet_count": 2,
                    "short_retention_configured": false,
                    "covered_since": captured_at - Duration::days(5),
                    "covered_through_cursor": {
                        "ts_utc": captured_at,
                        "slot": 1,
                        "signature": "sig"
                    },
                    "covered_through_lag_seconds": 30,
                    "tail_fresh_within_runtime_lag": true,
                    "runtime_freshness_lag_seconds": 600,
                    "total_observed_swaps_rows": 10
                },
                "rotation": {
                    "signal_available": true,
                    "reason": null,
                    "cycles_requested": 3,
                    "cycles_completed": 3,
                    "sample_interval_seconds": 600,
                    "overlap_with_previous_cycle": 2,
                    "entered_since_previous_cycle": if exact_match {
                        serde_json::json!(["wallet-gamma"])
                    } else {
                        serde_json::json!([])
                    },
                    "left_since_previous_cycle": [],
                    "stable_wallets_across_cycles": ["wallet-alpha", "wallet-beta"],
                    "unique_wallet_count_across_cycles": if exact_match { 3 } else { 2 },
                    "samples": []
                }
            })
            .to_string(),
            shadow_signal_json: serde_json::json!({
                "recent_window_start": captured_at - Duration::minutes(30),
                "recent_window_end": captured_at,
                "selected_wallet_ids": ["wallet-alpha", "wallet-beta"],
                "selected_wallet_count": 2,
                "selected_wallets_with_recent_raw_activity": 1,
                "selected_wallets_with_recent_shadow_signal": 1,
                "recent_raw_swap_count": 3,
                "recent_shadow_signal_count": 1,
                "recent_raw_activity_wallet_ids": ["wallet-alpha"],
                "recent_shadow_signal_wallet_ids": ["wallet-alpha"],
                "recent_raw_activity_by_wallet": [{
                    "wallet_id": "wallet-alpha",
                    "row_count": 3,
                    "latest_ts": captured_at
                }],
                "recent_shadow_signal_by_wallet": [{
                    "wallet_id": "wallet-alpha",
                    "row_count": 1,
                    "latest_ts": captured_at
                }],
                "raw_activity_top_wallet_share": 1.0,
                "shadow_signal_top_wallet_share": 1.0,
                "raw_activity_broadly_distributed": false,
                "shadow_signal_broadly_distributed": false,
                "verdict": "shadow_signals_present_but_concentrated",
                "reason": "seed-shadow"
            })
            .to_string(),
        })
        .unwrap();
}

fn rehearsal_write(
    rehearsed_at: chrono::DateTime<Utc>,
    label: &str,
) -> ExecutionDryRunRehearsalWrite {
    ExecutionDryRunRehearsalWrite {
        rehearsed_at,
        execution_mode: "adapter_submit_confirm".to_string(),
        execution_enabled: false,
        route: "jito".to_string(),
        token: "So11111111111111111111111111111111111111112".to_string(),
        notional_sol: 0.01,
        signer_pubkey_configured: true,
        config_valid: true,
        connectivity_valid: true,
        adapter_contract_valid: true,
        policy_contract_valid: true,
        route_contract_valid: true,
        ready_for_dry_run: true,
        would_be_admissible_for_later_tiny_live: true,
        rpc_preconditions_valid: true,
        rpc_slot: Some(123),
        rpc_blockhash: Some("abc".to_string()),
        rpc_signer_balance_lamports: Some(1),
        adapter_result_classification: label.to_string(),
        adapter_accepted: Some(true),
        adapter_detail: label.to_string(),
        policy_echo_present: true,
        route_echo_present: true,
        contract_version_echo_present: true,
        response_slippage_bps: None,
        response_tip_lamports: None,
        response_compute_unit_limit: None,
        response_compute_unit_price_micro_lamports: None,
        verdict: label.to_string(),
        reason: label.to_string(),
        blockers: Vec::new(),
        warnings: Vec::new(),
        rehearsal_json: "{}".to_string(),
    }
}

fn run_bin_json(bin: &str, args: &[&str]) -> Value {
    let output = Command::new(bin)
        .args(args)
        .output()
        .unwrap_or_else(|error| panic!("failed to run {bin}: {error}"));
    if !output.status.success() {
        panic!(
            "command failed: {bin} {}\nstdout:\n{}\nstderr:\n{}",
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "failed to parse json from {bin}: {error}\nstdout:\n{}",
            String::from_utf8_lossy(&output.stdout)
        )
    })
}

fn write_json(path: &Path, value: &Value) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
}

fn load_json(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

#[cfg(unix)]
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
    echo "restart:$service:$action" >> "$runtime_dir/backend.log"
    ;;
  show)
    echo "show:$service:$action" >> "$runtime_dir/backend.log"
    printf 'active\nrunning\n'
    ;;
  *)
    echo "unexpected backend command $cmd" >&2
    exit 2
    ;;
esac
"#;
    fs::write(path, script).unwrap();
    use std::os::unix::fs::PermissionsExt;
    let mut perms = fs::metadata(path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms).unwrap();
}

#[cfg(not(unix))]
fn write_fake_backend(path: &Path) {
    fs::write(path, "").unwrap();
}

fn dynamic_live_config_toml(
    db_path: &Path,
    rpc_url: &str,
    adapter_url: &str,
    execution_enabled: bool,
) -> String {
    let mut contents = sample_safe_config_toml(db_path)
        .replace("https://rpc.example", rpc_url)
        .replace("http://127.0.0.1:8080/submit", adapter_url);
    if execution_enabled {
        contents = contents.replacen("enabled = false", "enabled = true", 1);
    }
    contents
}

fn sample_safe_config_toml(db_path: &Path) -> String {
    format!(
        r#"
[system]
env = "prod-live"

[sqlite]
path = "{}"

[execution]
enabled = false
mode = "adapter_submit_confirm"
batch_size = 1
default_route = "jito"
rpc_http_url = "https://rpc.example"
submit_adapter_http_url = "http://127.0.0.1:8080/submit"
submit_adapter_contract_version = "v1"
submit_adapter_require_policy_echo = true
submit_adapter_auth_token = "adapter-token"
submit_allowed_routes = ["jito"]
submit_route_order = ["jito"]
pretrade_max_fee_overhead_bps = 800
pretrade_max_priority_fee_lamports = 1500
slippage_bps = 40.0
execution_signer_pubkey = "11111111111111111111111111111111"

[execution.submit_route_max_slippage_bps]
jito = 40.0

[execution.submit_route_tip_lamports]
jito = 10000

[execution.submit_route_compute_unit_limit]
jito = 300000

[execution.submit_route_compute_unit_price_micro_lamports]
jito = 1500

[shadow]
copy_notional_sol = 0.05

[risk]
max_position_sol = 0.05
max_concurrent_positions = 1
daily_loss_limit_pct = 0.75

[tiny_live_policy]
enabled = true
max_trade_notional_sol = 0.05
max_batch_size = 1
max_concurrent_positions = 1
max_daily_loss_limit_pct = 1.0
allowed_routes = ["jito"]
require_policy_echo = true
max_pretrade_fee_overhead_bps = 1000
max_pretrade_priority_fee_lamports = 2000

[tiny_live_policy.max_route_slippage_bps]
jito = 50.0

[tiny_live_policy.max_route_tip_lamports]
jito = 10000

[tiny_live_policy.max_route_compute_unit_limit]
jito = 300000

[tiny_live_policy.max_route_compute_unit_price_micro_lamports]
jito = 1500

[tiny_live_guardrails]
enabled = true
evaluation_window_seconds = 900
max_execution_error_rate_pct = 5.0
max_adapter_contract_failure_rate_pct = 1.0
max_policy_echo_mismatch_rate_pct = 1.0
max_fee_or_slippage_breach_rate_pct = 5.0
max_connectivity_degraded_window_seconds = 120
max_daily_realized_loss_sol = 0.05
max_consecutive_hard_failures = 3
"#,
        db_path.display()
    )
    .trim_start()
    .to_string()
}

fn temp_dir(label: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    loop {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "{label}_{}_{}_{}",
            std::process::id(),
            unique,
            counter
        ));
        match fs::create_dir(&path) {
            Ok(()) => return path,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => panic!("failed creating temp dir {}: {error}", path.display()),
        }
    }
}

struct MockHttpServer {
    addr: SocketAddr,
    _handle: std::thread::JoinHandle<()>,
}

impl MockHttpServer {
    fn spawn(max_requests: usize, body: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let body = Arc::new(body);
        let handle = std::thread::spawn(move || {
            for _ in 0..max_requests {
                let (mut stream, _) = listener.accept().unwrap();
                let mut buffer = [0u8; 8192];
                let _ = stream.read(&mut buffer);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream.write_all(response.as_bytes()).unwrap();
                stream.flush().unwrap();
            }
        });
        Self {
            addr,
            _handle: handle,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }
}
