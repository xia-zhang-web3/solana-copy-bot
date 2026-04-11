#![recursion_limit = "256"]

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const USAGE: &str = "usage: copybot_tiny_live_activation_package_provenance_certificate_latest --root <path> [--session-dir <path>] [--output <path>] [--json] (--plan-latest-provenance-certificate | --render-latest-provenance-certificate-script | --run-latest-provenance-certificate | --verify-latest-provenance-certificate)";
const PROVENANCE_CERTIFICATE_LATEST_SESSION_VERSION: &str = "1";
const PROVENANCE_CERTIFICATE_LATEST_STATUS_VERSION: &str = "1";
const ATTESTATION_SEAL_LATEST_BIN: &str =
    "copybot_tiny_live_activation_package_attestation_seal_latest";
const PROVENANCE_CERTIFICATE_BIN: &str =
    "copybot_tiny_live_activation_package_provenance_certificate";
const ATTESTATION_SEAL_LATEST_PLAN_READY: &str =
    "tiny_live_package_attestation_seal_latest_plan_ready";
const ATTESTATION_SEAL_LATEST_VERIFY_OK: &str =
    "tiny_live_package_attestation_seal_latest_verify_ok";
const PROVENANCE_CERTIFICATE_VERIFY_OK: &str = "tiny_live_package_provenance_certificate_verify_ok";
const TOP_ACCEPTED_PACKAGE_STEP: &str = "clerestory_certificate";
const PLANNING_SAFE_STATEMENT: &str =
    "this latest-provenance-certificate handoff binds the latest persisted immutable package chain to the accepted provenance-certificate contract, but it never overrides Stage 3 or pre-activation refusal truth, it stays archival and read-only, it never runs the frozen live cutover controller by itself, and activation_authorized remains false";

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
    root: Option<PathBuf>,
    session_dir: Option<PathBuf>,
    output_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanLatestProvenanceCertificate,
    RenderLatestProvenanceCertificateScript,
    RunLatestProvenanceCertificate,
    VerifyLatestProvenanceCertificate,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageProvenanceCertificateLatestVerdict {
    TinyLivePackageProvenanceCertificateLatestPlanReady,
    TinyLivePackageProvenanceCertificateLatestScriptRendered,
    TinyLivePackageProvenanceCertificateLatestRefusedByMissingLatestChain,
    TinyLivePackageProvenanceCertificateLatestRefusedByInvalidLatestChain,
    TinyLivePackageProvenanceCertificateLatestRefusedByDriftedProvenanceCertificateContract,
    TinyLivePackageProvenanceCertificateLatestRefusedNowByStage3,
    TinyLivePackageProvenanceCertificateLatestRefusedNowByPreActivationGate,
    TinyLivePackageProvenanceCertificateLatestReadyForManualExecutionWhenGateTurnsGreen,
    TinyLivePackageProvenanceCertificateLatestVerifyOk,
    TinyLivePackageProvenanceCertificateLatestVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageProvenanceCertificateLatestResult {
    RefusedByMissingLatestChain,
    RefusedByInvalidLatestChain,
    RefusedByDriftedProvenanceCertificateContract,
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    ReadyForManualExecutionWhenGateTurnsGreen,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageProvenanceCertificateLatestStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageProvenanceCertificateLatestSession {
    session_version: String,
    planned_at: DateTime<Utc>,
    root: String,
    session_dir: String,
    nested_attestation_seal_latest_session_dir: String,
    nested_provenance_certificate_session_dir: String,
    downstream_decision_packet_session_dir: String,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    historical_execute_frozen_session_dir: Option<String>,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    resolve_latest_attestation_seal_command_summary: String,
    run_latest_attestation_seal_command_summary: String,
    verify_latest_attestation_seal_command_summary: String,
    downstream_provenance_certificate_command_summary: String,
    verify_provenance_certificate_command_summary: String,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageProvenanceCertificateLatestStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    root: String,
    session_dir: String,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    historical_execute_frozen_session_dir: Option<String>,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    nested_attestation_seal_latest_session_dir: String,
    nested_provenance_certificate_session_dir: String,
    downstream_decision_packet_session_dir: String,
    result: LivePackageProvenanceCertificateLatestResult,
    reason: String,
    latest_attestation_seal_plan_verdict: Option<String>,
    latest_attestation_seal_verdict: Option<String>,
    provenance_certificate_verdict: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    latest_attestation_seal_plan_step: Option<PackageProvenanceCertificateLatestStepArtifact>,
    latest_attestation_seal_step: Option<PackageProvenanceCertificateLatestStepArtifact>,
    provenance_certificate_step: Option<PackageProvenanceCertificateLatestStepArtifact>,
    operator_next_action_summary: String,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageProvenanceCertificateLatestPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    report_path: PathBuf,
    latest_attestation_seal_plan_report_path: PathBuf,
    latest_attestation_seal_report_path: PathBuf,
    provenance_certificate_report_path: PathBuf,
    nested_attestation_seal_latest_session_dir: PathBuf,
    nested_provenance_certificate_session_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageProvenanceCertificateLatestReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageProvenanceCertificateLatestVerdict,
    reason: String,
    root: Option<String>,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    historical_execute_frozen_session_dir: Option<String>,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    session_dir: Option<String>,
    provenance_certificate_latest_session_path: Option<String>,
    provenance_certificate_latest_status_path: Option<String>,
    provenance_certificate_latest_report_path: Option<String>,
    latest_attestation_seal_plan_report_path: Option<String>,
    latest_attestation_seal_report_path: Option<String>,
    provenance_certificate_report_path: Option<String>,
    nested_attestation_seal_latest_session_dir: Option<String>,
    nested_provenance_certificate_session_dir: Option<String>,
    downstream_decision_packet_session_dir: Option<String>,
    latest_attestation_seal_plan_verdict: Option<String>,
    latest_attestation_seal_verdict: Option<String>,
    provenance_certificate_verdict: Option<String>,
    result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    resolve_latest_attestation_seal_command_summary: Option<String>,
    run_latest_attestation_seal_command_summary: Option<String>,
    verify_latest_attestation_seal_command_summary: Option<String>,
    downstream_provenance_certificate_command_summary: Option<String>,
    verify_provenance_certificate_command_summary: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    verification_mismatches: Vec<String>,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct LatestProvenanceCertificateSnapshot {
    latest_top_step_name: String,
    latest_top_session_dir: PathBuf,
    downstream_decision_packet_session_dir: PathBuf,
    historical_execute_frozen_session_dir: PathBuf,
    turn_green_session_dir: PathBuf,
    launch_packet_session_dir: PathBuf,
    package_dir: PathBuf,
    install_root: PathBuf,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    reviewed_frozen_live_cutover_controller_command_summary: String,
    clerestory_certificate_summary: String,
    clerestory_certificate_sha256: String,
    clerestory_certificate_algorithm: String,
}

#[derive(Debug, Clone)]
struct ResolvedLatestProvenanceCertificate {
    snapshot: LatestProvenanceCertificateSnapshot,
    latest_attestation_seal_plan_raw: Value,
    latest_attestation_seal_plan: AttestationSealLatestReportView,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolutionFailureKind {
    MissingLatestChain,
    InvalidLatestChain,
    DriftedProvenanceCertificateContract,
}

#[derive(Debug, Clone)]
struct LatestProvenanceCertificateResolutionFailure {
    kind: ResolutionFailureKind,
    reason: String,
    snapshot: Option<LatestProvenanceCertificateSnapshot>,
    latest_attestation_seal_plan_raw: Option<Value>,
    latest_attestation_seal_plan_verdict: Option<String>,
    latest_attestation_seal_plan_reason: Option<String>,
    latest_attestation_seal_plan_generated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
struct AttestationSealLatestReportView {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: String,
    reason: String,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    historical_execute_frozen_session_dir: Option<String>,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    session_dir: Option<String>,
    #[serde(alias = "downstream_decision_packet_session_dir")]
    downstream_decision_packet_session_dir: Option<String>,
    latest_execute_verdict: Option<String>,
    decision_packet_plan_verdict: Option<String>,
    result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    activation_authorized: bool,
    explicit_statement: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ProvenanceCertificateReportView {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: String,
    reason: String,
    attestation_seal_session_dir: String,
    release_capsule_session_dir: Option<String>,
    activation_ticket_session_dir: Option<String>,
    review_receipt_session_dir: Option<String>,
    handoff_bundle_session_dir: Option<String>,
    decision_packet_session_dir: Option<String>,
    execute_frozen_session_dir: Option<String>,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    session_dir: Option<String>,
    provenance_certificate_session_path: Option<String>,
    provenance_certificate_status_path: Option<String>,
    archived_attestation_seal_report_path: Option<String>,
    archived_release_capsule_digest_manifest_path: Option<String>,
    result: Option<String>,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    attestation_seal_step: Option<PackageProvenanceCertificateLatestStepArtifact>,
    verify_attestation_seal_command_summary: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    attestation_seal_summary: Option<String>,
    custody_record_summary: Option<String>,
    release_capsule_digest_manifest_sha256: Option<String>,
    release_capsule_digest_manifest_entry_count: Option<usize>,
    release_capsule_digest_algorithm: Option<String>,
    provenance_certificate_summary: Option<String>,
    chain_fingerprint_summary: Option<String>,
    chain_fingerprint_sha256: Option<String>,
    chain_fingerprint_algorithm: Option<String>,
    release_capsule_digest_manifest: Option<Value>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    verification_mismatches: Option<Vec<String>>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ProvenanceCertificateSessionView {
    attestation_seal_session_dir: String,
    release_capsule_session_dir: String,
    activation_ticket_session_dir: String,
    review_receipt_session_dir: String,
    handoff_bundle_session_dir: String,
    decision_packet_session_dir: String,
    execute_frozen_session_dir: String,
    turn_green_session_dir: String,
    launch_packet_session_dir: String,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: String,
    verify_attestation_seal_command_summary: String,
    reviewed_frozen_live_cutover_controller_command_summary: String,
    attestation_seal_summary: String,
    custody_record_summary: String,
    release_capsule_digest_manifest_sha256: String,
    release_capsule_digest_manifest_entry_count: usize,
    release_capsule_digest_algorithm: String,
    provenance_certificate_summary: String,
    chain_fingerprint_summary: String,
    chain_fingerprint_sha256: String,
    chain_fingerprint_algorithm: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ProvenanceCertificateStatusView {
    session_dir: String,
    attestation_seal_session_dir: String,
    result: String,
    reason: String,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    attestation_seal_step: Option<PackageProvenanceCertificateLatestStepArtifact>,
    attestation_seal_summary: String,
    custody_record_summary: String,
    release_capsule_digest_manifest_sha256: String,
    release_capsule_digest_manifest_entry_count: usize,
    release_capsule_digest_algorithm: String,
    provenance_certificate_summary: String,
    chain_fingerprint_summary: String,
    chain_fingerprint_sha256: String,
    chain_fingerprint_algorithm: String,
    explicit_statement: String,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut root = None;
    let mut session_dir = None;
    let mut output_path = None;
    let mut json = false;
    let mut mode = None;

    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--root" => root = Some(PathBuf::from(parse_string_arg("--root", iter.next())?)),
            "--session-dir" => {
                session_dir = Some(PathBuf::from(parse_string_arg(
                    "--session-dir",
                    iter.next(),
                )?))
            }
            "--output" => {
                output_path = Some(PathBuf::from(parse_string_arg("--output", iter.next())?))
            }
            "--json" => json = true,
            "--plan-latest-provenance-certificate" => set_mode(
                &mut mode,
                Mode::PlanLatestProvenanceCertificate,
                arg.as_str(),
            )?,
            "--render-latest-provenance-certificate-script" => set_mode(
                &mut mode,
                Mode::RenderLatestProvenanceCertificateScript,
                arg.as_str(),
            )?,
            "--run-latest-provenance-certificate" => set_mode(
                &mut mode,
                Mode::RunLatestProvenanceCertificate,
                arg.as_str(),
            )?,
            "--verify-latest-provenance-certificate" => set_mode(
                &mut mode,
                Mode::VerifyLatestProvenanceCertificate,
                arg.as_str(),
            )?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown arg {other}; {USAGE}"),
        }
    }

    let Some(mode) = mode else {
        return Ok(None);
    };
    if matches!(
        mode,
        Mode::PlanLatestProvenanceCertificate
            | Mode::RenderLatestProvenanceCertificateScript
            | Mode::RunLatestProvenanceCertificate
    ) && root.is_none()
    {
        bail!("missing --root");
    }
    if matches!(mode, Mode::RenderLatestProvenanceCertificateScript) && output_path.is_none() {
        bail!("missing --output for render mode");
    }
    if matches!(
        mode,
        Mode::RunLatestProvenanceCertificate | Mode::VerifyLatestProvenanceCertificate
    ) && session_dir.is_none()
    {
        bail!("missing --session-dir");
    }

    Ok(Some(Config {
        mode,
        root,
        session_dir,
        output_path,
        json,
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanLatestProvenanceCertificate => {
            plan_latest_provenance_certificate_report(&config)?
        }
        Mode::RenderLatestProvenanceCertificateScript => {
            render_latest_provenance_certificate_script_report(&config)?
        }
        Mode::RunLatestProvenanceCertificate => run_latest_provenance_certificate_report(&config)?,
        Mode::VerifyLatestProvenanceCertificate => {
            verify_latest_provenance_certificate_report(&config)?
        }
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_latest_provenance_certificate_report(
    config: &Config,
) -> Result<PackageProvenanceCertificateLatestReport> {
    let root = config
        .root
        .as_ref()
        .ok_or_else(|| anyhow!("missing --root"))?;
    let suggested_session_dir = config.session_dir.clone().unwrap_or_else(|| {
        root.join("tiny_live_activation_package_provenance_certificate_latest.session")
    });
    let paths = provenance_certificate_latest_paths(&suggested_session_dir);

    match resolve_latest_provenance_certificate(root, &paths) {
        Ok(resolved) => Ok(plan_report_from_resolution(
            "plan_latest_provenance_certificate",
            TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestPlanReady,
            format!(
                "latest immutable {} chain under {} resolves to exact latest-attestation-seal {} lineage, exact decision-packet confirmation anchor {}, and the accepted provenance-certificate contract remains current",
                resolved.snapshot.latest_top_step_name,
                root.display(),
                paths.nested_attestation_seal_latest_session_dir.display(),
                resolved.snapshot.downstream_decision_packet_session_dir.display(),
            ),
            root,
            &suggested_session_dir,
            &paths,
            &resolved,
        )),
        Err(failure) => Ok(report_from_resolution_failure(
            "plan_latest_provenance_certificate",
            root,
            &suggested_session_dir,
            &paths,
            &failure,
        )),
    }
}

fn render_latest_provenance_certificate_script_report(
    config: &Config,
) -> Result<PackageProvenanceCertificateLatestReport> {
    let root = config
        .root
        .as_ref()
        .ok_or_else(|| anyhow!("missing --root"))?;
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --output for render mode"))?;
    let suggested_session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| output_path.with_extension("session"));
    let paths = provenance_certificate_latest_paths(&suggested_session_dir);

    match resolve_latest_provenance_certificate(root, &paths) {
        Ok(resolved) => {
            write_script(
                output_path,
                &format!(
                    "#!/usr/bin/env bash\nset -euo pipefail\n\n{run_latest}\n{verify_latest}\n{run_handoff_bundle}\n{verify_handoff_bundle}\n",
                    run_latest = run_latest_attestation_seal_command_summary(
                        root,
                        &paths.nested_attestation_seal_latest_session_dir,
                    ),
                    verify_latest = verify_latest_attestation_seal_command_summary(
                        &paths.nested_attestation_seal_latest_session_dir,
                    ),
                    run_handoff_bundle = downstream_provenance_certificate_command_summary(
                        &resolved.snapshot,
                        &expected_downstream_attestation_seal_session_dir(&paths),
                        &paths.nested_provenance_certificate_session_dir,
                    ),
                    verify_handoff_bundle = verify_provenance_certificate_command_summary(
                        &resolved.snapshot,
                        &expected_downstream_attestation_seal_session_dir(&paths),
                        &paths.nested_provenance_certificate_session_dir,
                    ),
                ),
            )?;
            Ok(plan_report_from_resolution(
                "render_latest_provenance_certificate_script",
                TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestScriptRendered,
                format!(
                    "rendered latest-provenance-certificate handoff script to {} using the accepted latest-attestation-seal and provenance-certificate contracts",
                    output_path.display()
                ),
                root,
                &suggested_session_dir,
                &paths,
                &resolved,
            ))
        }
        Err(failure) => Ok(report_from_resolution_failure(
            "render_latest_provenance_certificate_script",
            root,
            &suggested_session_dir,
            &paths,
            &failure,
        )),
    }
}

fn run_latest_provenance_certificate_report(
    config: &Config,
) -> Result<PackageProvenanceCertificateLatestReport> {
    let root = config
        .root
        .as_ref()
        .ok_or_else(|| anyhow!("missing --root"))?;
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for run mode"))?;
    if session_dir.exists() {
        bail!(
            "refusing to overwrite existing latest-provenance-certificate session dir {}",
            session_dir.display()
        );
    }
    let paths = provenance_certificate_latest_paths(session_dir);
    create_clean_session_dir(session_dir, "latest-provenance-certificate")?;

    let resolution = resolve_latest_provenance_certificate(root, &paths);
    match resolution {
        Ok(resolved) => {
            run_latest_provenance_certificate_with_resolution(root, session_dir, &paths, &resolved)
        }
        Err(failure) => {
            run_latest_provenance_certificate_failure(root, session_dir, &paths, &failure)
        }
    }
}

fn verify_latest_provenance_certificate_report(
    config: &Config,
) -> Result<PackageProvenanceCertificateLatestReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = provenance_certificate_latest_paths(session_dir);

    let session: PackageProvenanceCertificateLatestSession = match load_json(&paths.session_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(
                verify_invalid_provenance_certificate_latest_report_without_session(
                    session_dir,
                    vec![format!(
                        "failed reading latest-provenance-certificate session artifact {}: {error}",
                        paths.session_path.display()
                    )],
                ),
            )
        }
    };
    let status: PackageProvenanceCertificateLatestStatus = match load_json(&paths.status_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(
                verify_invalid_provenance_certificate_latest_report_without_session(
                    session_dir,
                    vec![format!(
                        "failed reading latest-provenance-certificate status artifact {}: {error}",
                        paths.status_path.display()
                    )],
                ),
            )
        }
    };
    let stored_report: PackageProvenanceCertificateLatestReport =
        match load_json(&paths.report_path) {
            Ok(value) => value,
            Err(error) => {
                return Ok(
                    verify_invalid_provenance_certificate_latest_report_without_session(
                        session_dir,
                        vec![format!(
                    "failed reading latest-provenance-certificate report artifact {}: {error}",
                    paths.report_path.display()
                )],
                    ),
                )
            }
        };

    let mut mismatches = Vec::new();
    let root = PathBuf::from(&session.root);

    compare_string(
        &session.root,
        &status.root,
        "latest-provenance-certificate root consistency",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "latest-provenance-certificate session session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "latest-provenance-certificate status session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "latest-provenance-certificate report session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_attestation_seal_latest_session_dir,
        &paths
            .nested_attestation_seal_latest_session_dir
            .display()
            .to_string(),
        "latest-provenance-certificate session nested_attestation_seal_latest_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_attestation_seal_latest_session_dir,
        &paths
            .nested_attestation_seal_latest_session_dir
            .display()
            .to_string(),
        "latest-provenance-certificate status nested_attestation_seal_latest_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report
            .nested_attestation_seal_latest_session_dir
            .as_deref(),
        Some(
            paths
                .nested_attestation_seal_latest_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "latest-provenance-certificate report nested_attestation_seal_latest_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_provenance_certificate_session_dir,
        &paths
            .nested_provenance_certificate_session_dir
            .display()
            .to_string(),
        "latest-provenance-certificate session nested_provenance_certificate_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_provenance_certificate_session_dir,
        &paths
            .nested_provenance_certificate_session_dir
            .display()
            .to_string(),
        "latest-provenance-certificate status nested_provenance_certificate_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report
            .nested_provenance_certificate_session_dir
            .as_deref(),
        Some(
            paths
                .nested_provenance_certificate_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "latest-provenance-certificate report nested_provenance_certificate_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.downstream_decision_packet_session_dir,
        &expected_downstream_decision_packet_session_dir(&paths)
            .display()
            .to_string(),
        "latest-provenance-certificate session downstream_decision_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.downstream_decision_packet_session_dir,
        &expected_downstream_decision_packet_session_dir(&paths)
            .display()
            .to_string(),
        "latest-provenance-certificate status downstream_decision_packet_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report
            .downstream_decision_packet_session_dir
            .as_deref(),
        Some(
            expected_downstream_decision_packet_session_dir(&paths)
                .display()
                .to_string()
                .as_str(),
        ),
        "latest-provenance-certificate report downstream_decision_packet_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report
            .provenance_certificate_latest_session_path
            .as_deref(),
        Some(paths.session_path.display().to_string().as_str()),
        "latest-provenance-certificate report session path",
        &mut mismatches,
    );
    compare_option_string(
        stored_report
            .provenance_certificate_latest_status_path
            .as_deref(),
        Some(paths.status_path.display().to_string().as_str()),
        "latest-provenance-certificate report status path",
        &mut mismatches,
    );
    compare_option_string(
        stored_report
            .provenance_certificate_latest_report_path
            .as_deref(),
        Some(paths.report_path.display().to_string().as_str()),
        "latest-provenance-certificate report report path",
        &mut mismatches,
    );
    compare_string(
        &session.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "latest-provenance-certificate session explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &status.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "latest-provenance-certificate status explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &stored_report.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "latest-provenance-certificate report explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &stored_report.mode,
        "run_latest_provenance_certificate",
        "latest-provenance-certificate stored report mode",
        &mut mismatches,
    );
    compare_string(
        &serialize_enum(&stored_report.verdict),
        &serialize_enum(&verdict_for_result(status.result)),
        "latest-provenance-certificate stored report verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.result.as_deref(),
        Some(serialize_enum(&status.result).as_str()),
        "latest-provenance-certificate stored report result consistency",
        &mut mismatches,
    );
    compare_string(
        &stored_report.reason,
        &status.reason,
        "latest-provenance-certificate stored report reason consistency",
        &mut mismatches,
    );
    if stored_report.activation_authorized != status.activation_authorized {
        mismatches.push(format!(
            "latest-provenance-certificate stored report activation_authorized {} does not match status {}",
            stored_report.activation_authorized, status.activation_authorized
        ));
    }
    compare_option_string(
        stored_report
            .latest_attestation_seal_plan_verdict
            .as_deref(),
        status.latest_attestation_seal_plan_verdict.as_deref(),
        "stored report latest_attestation_seal_plan_verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.latest_attestation_seal_verdict.as_deref(),
        status.latest_attestation_seal_verdict.as_deref(),
        "stored report latest_attestation_seal_verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.provenance_certificate_verdict.as_deref(),
        status.provenance_certificate_verdict.as_deref(),
        "stored report attestation_seal_verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.current_pre_activation_gate_verdict.as_deref(),
        status.current_pre_activation_gate_verdict.as_deref(),
        "stored report current_pre_activation_gate_verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.current_pre_activation_gate_reason.as_deref(),
        status.current_pre_activation_gate_reason.as_deref(),
        "stored report current_pre_activation_gate_reason",
        &mut mismatches,
    );

    let resolution = resolve_latest_provenance_certificate(&root, &paths);
    match resolution {
        Ok(resolved) => {
            compare_session_against_snapshot(
                &session,
                &root,
                session_dir,
                &paths,
                &resolved.snapshot,
                &mut mismatches,
            );
            compare_report_against_snapshot(
                &stored_report,
                &root,
                session_dir,
                &paths,
                &resolved.snapshot,
                &mut mismatches,
            );

            let stored_latest_plan: AttestationSealLatestReportView = load_required_step_json(
                &status.latest_attestation_seal_plan_step,
                &paths.latest_attestation_seal_plan_report_path,
                "latest_attestation_seal_plan_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.latest_attestation_seal_plan_step.as_ref(),
                &paths.latest_attestation_seal_plan_report_path,
                "plan_latest_attestation_seal",
                &stored_latest_plan.verdict,
                &stored_latest_plan.reason,
                stored_latest_plan.generated_at,
                "stored latest_attestation_seal_plan_step",
                &mut mismatches,
            );
            compare_attestation_seal_latest_plan_against_snapshot(
                &stored_latest_plan,
                &resolved.snapshot,
                &paths.nested_attestation_seal_latest_session_dir,
                &mut mismatches,
            );
            compare_option_string(
                status.latest_attestation_seal_plan_verdict.as_deref(),
                Some(stored_latest_plan.verdict.as_str()),
                "stored status latest_attestation_seal_plan_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report
                    .latest_attestation_seal_plan_verdict
                    .as_deref(),
                Some(stored_latest_plan.verdict.as_str()),
                "stored report latest_attestation_seal_plan_verdict",
                &mut mismatches,
            );

            let stored_latest_run: AttestationSealLatestReportView = load_required_step_json(
                &status.latest_attestation_seal_step,
                &paths.latest_attestation_seal_report_path,
                "latest_attestation_seal_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.latest_attestation_seal_step.as_ref(),
                &paths.latest_attestation_seal_report_path,
                "run_latest_attestation_seal",
                &stored_latest_run.verdict,
                &stored_latest_run.reason,
                stored_latest_run.generated_at,
                "stored latest_attestation_seal_step",
                &mut mismatches,
            );
            compare_attestation_seal_latest_run_against_snapshot(
                &stored_latest_run,
                &resolved.snapshot,
                &paths.nested_attestation_seal_latest_session_dir,
                &mut mismatches,
            );
            compare_option_string(
                status.latest_attestation_seal_verdict.as_deref(),
                Some(stored_latest_run.verdict.as_str()),
                "stored status latest_attestation_seal_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.latest_attestation_seal_verdict.as_deref(),
                Some(stored_latest_run.verdict.as_str()),
                "stored report latest_attestation_seal_verdict",
                &mut mismatches,
            );

            let actual_latest_run: AttestationSealLatestReportView = match load_json(
                &nested_attestation_seal_latest_report_path(
                    &paths.nested_attestation_seal_latest_session_dir,
                ),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed loading actual nested latest-attestation-seal report under {}: {error}",
                        paths.nested_attestation_seal_latest_session_dir.display()
                    ));
                    fallback_attestation_seal_latest_copy()
                }
            };
            compare_attestation_seal_latest_copy_matches_actual(
                &stored_latest_run,
                &actual_latest_run,
                &mut mismatches,
            );

            let latest_verify_raw = match run_json_command(
                ATTESTATION_SEAL_LATEST_BIN,
                &latest_attestation_seal_verify_args(
                    &paths.nested_attestation_seal_latest_session_dir,
                ),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed verifying nested latest-attestation-seal session under {}: {error}",
                        paths.nested_attestation_seal_latest_session_dir.display()
                    ));
                    Value::Null
                }
            };
            if !latest_verify_raw.is_null() {
                let latest_verify: AttestationSealLatestReportView =
                    serde_json::from_value(latest_verify_raw)
                        .context("failed parsing nested latest-attestation-seal verify json")?;
                if latest_verify.verdict != ATTESTATION_SEAL_LATEST_VERIFY_OK {
                    mismatches.push(format!(
                        "nested latest-attestation-seal verification is non-green: {}",
                        latest_verify.reason
                    ));
                }
            }

            let stored_handoff_bundle: ProvenanceCertificateReportView = load_required_step_json(
                &status.provenance_certificate_step,
                &paths.provenance_certificate_report_path,
                "provenance_certificate_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.provenance_certificate_step.as_ref(),
                &paths.provenance_certificate_report_path,
                "run_live_package_provenance_certificate",
                &stored_handoff_bundle.verdict,
                &stored_handoff_bundle.reason,
                stored_handoff_bundle.generated_at,
                "stored provenance_certificate_step",
                &mut mismatches,
            );
            compare_provenance_certificate_run_against_snapshot(
                &stored_handoff_bundle,
                &resolved.snapshot,
                &expected_downstream_attestation_seal_session_dir(&paths),
                &paths.nested_provenance_certificate_session_dir,
                &mut mismatches,
            );
            compare_option_string(
                status.provenance_certificate_verdict.as_deref(),
                Some(stored_handoff_bundle.verdict.as_str()),
                "stored status provenance_certificate_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.provenance_certificate_verdict.as_deref(),
                Some(stored_handoff_bundle.verdict.as_str()),
                "stored report provenance_certificate_verdict",
                &mut mismatches,
            );

            let nested_handoff_bundle_session: ProvenanceCertificateSessionView =
                match load_json(&nested_provenance_certificate_session_path(
                    &paths.nested_provenance_certificate_session_dir,
                )) {
                    Ok(value) => value,
                    Err(error) => {
                        mismatches.push(format!(
                        "failed reading nested provenance-certificate session under {}: {error}",
                        paths.nested_provenance_certificate_session_dir.display()
                    ));
                        fallback_provenance_certificate_session(
                            &resolved.snapshot,
                            &paths.nested_provenance_certificate_session_dir,
                        )
                    }
                };
            let nested_handoff_bundle_status: ProvenanceCertificateStatusView =
                match load_json(&nested_provenance_certificate_status_path(
                    &paths.nested_provenance_certificate_session_dir,
                )) {
                    Ok(value) => value,
                    Err(error) => {
                        mismatches.push(format!(
                            "failed reading nested provenance-certificate status under {}: {error}",
                            paths.nested_provenance_certificate_session_dir.display()
                        ));
                        fallback_provenance_certificate_status(
                            &resolved.snapshot,
                            &paths.nested_provenance_certificate_session_dir,
                        )
                    }
                };
            compare_provenance_certificate_copy_against_nested_truth(
                &stored_handoff_bundle,
                &nested_handoff_bundle_session,
                &nested_handoff_bundle_status,
                &mut mismatches,
            );

            let handoff_bundle_verify_raw = match run_json_command(
                PROVENANCE_CERTIFICATE_BIN,
                &provenance_certificate_verify_args(
                    &resolved.snapshot,
                    &expected_downstream_attestation_seal_session_dir(&paths),
                    &paths.nested_provenance_certificate_session_dir,
                ),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed verifying nested provenance-certificate session under {}: {error}",
                        paths.nested_provenance_certificate_session_dir.display()
                    ));
                    Value::Null
                }
            };
            if !handoff_bundle_verify_raw.is_null() {
                let handoff_bundle_verify: ProvenanceCertificateReportView =
                    serde_json::from_value(handoff_bundle_verify_raw)
                        .context("failed parsing nested provenance-certificate verify json")?;
                compare_option_string(
                    stored_handoff_bundle
                        .reviewed_frozen_live_cutover_controller_command_summary
                        .as_deref(),
                    handoff_bundle_verify
                        .reviewed_frozen_live_cutover_controller_command_summary
                        .as_deref(),
                    "stored provenance-certificate copy reviewed_frozen_live_cutover_controller_command_summary",
                    &mut mismatches,
                );
                if handoff_bundle_verify.verdict != PROVENANCE_CERTIFICATE_VERIFY_OK {
                    mismatches.push(format!(
                        "nested provenance-certificate verification is non-green: {}",
                        handoff_bundle_verify.reason
                    ));
                }
            }

            let expected_result = provenance_certificate_latest_result_from_provenance_certificate(
                nested_handoff_bundle_status.result.as_str(),
            )
            .unwrap_or(
                LivePackageProvenanceCertificateLatestResult::RefusedByDriftedProvenanceCertificateContract,
            );
            if status.result != expected_result {
                mismatches.push(format!(
                    "stored latest-provenance-certificate result {} does not match nested provenance-certificate result {}",
                    serialize_enum(&status.result),
                    serialize_enum(&expected_result)
                ));
            }
            compare_string(
                &status.reason,
                &nested_handoff_bundle_status.reason,
                "stored status reason",
                &mut mismatches,
            );
            compare_string(
                &status.operator_next_action_summary,
                &operator_next_action_summary(expected_result),
                "stored status operator_next_action_summary",
                &mut mismatches,
            );
            if status.activation_authorized {
                mismatches.push(
                    "stored status activation_authorized must remain false for latest-provenance-certificate"
                        .to_string(),
                );
            }
            compare_option_string(
                status.current_pre_activation_gate_verdict.as_deref(),
                nested_handoff_bundle_status
                    .current_pre_activation_gate_verdict
                    .as_deref(),
                "stored status current_pre_activation_gate_verdict",
                &mut mismatches,
            );
            compare_option_string(
                status.current_pre_activation_gate_reason.as_deref(),
                nested_handoff_bundle_status
                    .current_pre_activation_gate_reason
                    .as_deref(),
                "stored status current_pre_activation_gate_reason",
                &mut mismatches,
            );
        }
        Err(failure) => {
            compare_session_against_failure(
                &session,
                &root,
                session_dir,
                &paths,
                &failure,
                &mut mismatches,
            );
            compare_report_against_failure(
                &stored_report,
                &root,
                session_dir,
                &paths,
                &failure,
                &mut mismatches,
            );
            if failure.latest_attestation_seal_plan_raw.is_some() {
                let stored_latest_plan: AttestationSealLatestReportView = load_required_step_json(
                    &status.latest_attestation_seal_plan_step,
                    &paths.latest_attestation_seal_plan_report_path,
                    "latest_attestation_seal_plan_step",
                    &mut mismatches,
                )?;
                compare_step_artifact_against_report_metadata(
                    status.latest_attestation_seal_plan_step.as_ref(),
                    &paths.latest_attestation_seal_plan_report_path,
                    "plan_latest_attestation_seal",
                    &stored_latest_plan.verdict,
                    &stored_latest_plan.reason,
                    stored_latest_plan.generated_at,
                    "stored latest_attestation_seal_plan_step",
                    &mut mismatches,
                );
                compare_option_string(
                    status.latest_attestation_seal_plan_verdict.as_deref(),
                    failure.latest_attestation_seal_plan_verdict.as_deref(),
                    "stored status latest_attestation_seal_plan_verdict on failure",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_report
                        .latest_attestation_seal_plan_verdict
                        .as_deref(),
                    failure.latest_attestation_seal_plan_verdict.as_deref(),
                    "stored report latest_attestation_seal_plan_verdict on failure",
                    &mut mismatches,
                );
            }
            compare_option_string(
                status.latest_attestation_seal_verdict.as_deref(),
                None,
                "stored status latest_attestation_seal_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.latest_attestation_seal_verdict.as_deref(),
                None,
                "stored report latest_attestation_seal_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                status.provenance_certificate_verdict.as_deref(),
                None,
                "stored status attestation_seal_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.provenance_certificate_verdict.as_deref(),
                None,
                "stored report attestation_seal_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                status.current_pre_activation_gate_verdict.as_deref(),
                None,
                "stored status current_pre_activation_gate_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.current_pre_activation_gate_verdict.as_deref(),
                None,
                "stored report current_pre_activation_gate_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                status.current_pre_activation_gate_reason.as_deref(),
                None,
                "stored status current_pre_activation_gate_reason on failure",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.current_pre_activation_gate_reason.as_deref(),
                None,
                "stored report current_pre_activation_gate_reason on failure",
                &mut mismatches,
            );
            compare_string(
                &status.reason,
                &failure.reason,
                "stored status failure reason",
                &mut mismatches,
            );
            compare_string(
                &stored_report.reason,
                &failure.reason,
                "stored report failure reason",
                &mut mismatches,
            );
            compare_string(
                &status.operator_next_action_summary,
                &operator_next_action_summary(result_from_failure_kind(failure.kind)),
                "stored status operator_next_action_summary on failure",
                &mut mismatches,
            );
            if status.activation_authorized {
                mismatches.push(
                    "stored status activation_authorized must be false on failure".to_string(),
                );
            }
            if stored_report.activation_authorized {
                mismatches.push(
                    "stored report activation_authorized must be false on failure".to_string(),
                );
            }
        }
    }

    let verdict = if mismatches.is_empty() {
        TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestVerifyOk
    } else {
        TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "latest-provenance-certificate session under {} is coherent with the current latest immutable chain and the accepted provenance-certificate contract",
            session_dir.display()
        )
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "latest-provenance-certificate verification failed".to_string())
    };

    Ok(PackageProvenanceCertificateLatestReport {
        generated_at: Utc::now(),
        mode: "verify_latest_provenance_certificate".to_string(),
        verdict,
        reason,
        root: Some(session.root.clone()),
        latest_top_step_name: status.latest_top_step_name.clone(),
        latest_top_session_dir: status.latest_top_session_dir.clone(),
        historical_execute_frozen_session_dir: status
            .historical_execute_frozen_session_dir
            .clone(),
        turn_green_session_dir: status.turn_green_session_dir.clone(),
        launch_packet_session_dir: status.launch_packet_session_dir.clone(),
        package_dir: session.package_dir.clone(),
        install_root: session.install_root.clone(),
        target_service_name: session.target_service_name.clone(),
        backend_command: session.backend_command.clone(),
        wrapper_timeout_ms: session.wrapper_timeout_ms,
        service_status_max_staleness_ms: session.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        provenance_certificate_latest_session_path: Some(paths.session_path.display().to_string()),
        provenance_certificate_latest_status_path: Some(paths.status_path.display().to_string()),
        provenance_certificate_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_attestation_seal_plan_report_path: Some(
            paths
                .latest_attestation_seal_plan_report_path
                .display()
                .to_string(),
        ),
        latest_attestation_seal_report_path: Some(
            paths.latest_attestation_seal_report_path.display().to_string(),
        ),
        provenance_certificate_report_path: Some(paths.provenance_certificate_report_path.display().to_string()),
        nested_attestation_seal_latest_session_dir: Some(
            paths
                .nested_attestation_seal_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_provenance_certificate_session_dir: Some(
            paths.nested_provenance_certificate_session_dir.display().to_string(),
        ),
        downstream_decision_packet_session_dir: Some(
            expected_downstream_decision_packet_session_dir(&paths)
                .display()
                .to_string(),
        ),
        latest_attestation_seal_plan_verdict: status.latest_attestation_seal_plan_verdict.clone(),
        latest_attestation_seal_verdict: status.latest_attestation_seal_verdict.clone(),
        provenance_certificate_verdict: status.provenance_certificate_verdict.clone(),
        result: Some(serialize_enum(&status.result)),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        resolve_latest_attestation_seal_command_summary: Some(
            session.resolve_latest_attestation_seal_command_summary.clone(),
        ),
        run_latest_attestation_seal_command_summary: Some(
            session.run_latest_attestation_seal_command_summary.clone(),
        ),
        verify_latest_attestation_seal_command_summary: Some(
            session.verify_latest_attestation_seal_command_summary.clone(),
        ),
        downstream_provenance_certificate_command_summary: Some(
            session.downstream_provenance_certificate_command_summary.clone(),
        ),
        verify_provenance_certificate_command_summary: Some(session.verify_provenance_certificate_command_summary.clone()),
        reviewed_frozen_live_cutover_controller_command_summary: session
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        clerestory_certificate_summary: session.clerestory_certificate_summary.clone(),
        clerestory_certificate_sha256: session.clerestory_certificate_sha256.clone(),
        clerestory_certificate_algorithm: session.clerestory_certificate_algorithm.clone(),
        verification_mismatches: mismatches,
        activation_authorized: false,
        explicit_statement:
            "this verification proves only whether the latest immutable chain still resolves cleanly into the accepted provenance-certificate contract; it never runs the frozen controller"
                .to_string(),
    })
}

fn run_latest_provenance_certificate_with_resolution(
    root: &Path,
    session_dir: &Path,
    paths: &PackageProvenanceCertificateLatestPaths,
    resolved: &ResolvedLatestProvenanceCertificate,
) -> Result<PackageProvenanceCertificateLatestReport> {
    let session = planned_session(root, session_dir, paths, Some(resolved), None);
    persist_json(&paths.session_path, &session)?;
    persist_json(
        &paths.latest_attestation_seal_plan_report_path,
        &resolved.latest_attestation_seal_plan_raw,
    )?;

    let latest_attestation_seal_plan_step = Some(step_artifact(
        &paths.latest_attestation_seal_plan_report_path,
        "plan_latest_attestation_seal",
        &resolved.latest_attestation_seal_plan.verdict,
        &resolved.latest_attestation_seal_plan.reason,
        resolved.latest_attestation_seal_plan.generated_at,
    ));

    let latest_attestation_seal_run_raw = match run_json_command(
        ATTESTATION_SEAL_LATEST_BIN,
        &latest_attestation_seal_run_args(root, &paths.nested_attestation_seal_latest_session_dir),
    ) {
        Ok(value) => value,
        Err(error) => {
            let status = refusal_status(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageProvenanceCertificateLatestResult::RefusedByDriftedProvenanceCertificateContract,
                format!(
                    "failed running nested latest-attestation-seal handoff under {}: {error}",
                    paths.nested_attestation_seal_latest_session_dir.display()
                ),
                latest_attestation_seal_plan_step,
                None,
                None,
            );
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_provenance_certificate",
                verdict_for_result(status.result),
                session_dir,
                paths,
                &session,
                &status,
                Vec::new(),
            );
            persist_json(&paths.report_path, &report)?;
            return Ok(report);
        }
    };
    persist_json(
        &paths.latest_attestation_seal_report_path,
        &latest_attestation_seal_run_raw,
    )?;
    let latest_attestation_seal_run: AttestationSealLatestReportView =
        serde_json::from_value(latest_attestation_seal_run_raw)
            .context("failed parsing nested latest-attestation-seal run json")?;
    let latest_attestation_seal_step = Some(step_artifact(
        &paths.latest_attestation_seal_report_path,
        "run_latest_attestation_seal",
        &latest_attestation_seal_run.verdict,
        &latest_attestation_seal_run.reason,
        latest_attestation_seal_run.generated_at,
    ));

    if let Some(terminal_failure) =
        latest_attestation_seal_terminal_failure(&latest_attestation_seal_run)
    {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            terminal_failure,
            latest_attestation_seal_run.reason.clone(),
            latest_attestation_seal_plan_step,
            latest_attestation_seal_step,
            None,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_provenance_certificate",
            verdict_for_result(status.result),
            session_dir,
            paths,
            &session,
            &status,
            Vec::new(),
        );
        persist_json(&paths.report_path, &report)?;
        return Ok(report);
    }

    let mut run_mismatches = Vec::new();
    compare_attestation_seal_latest_run_against_snapshot(
        &latest_attestation_seal_run,
        &resolved.snapshot,
        &paths.nested_attestation_seal_latest_session_dir,
        &mut run_mismatches,
    );
    if !run_mismatches.is_empty() {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageProvenanceCertificateLatestResult::RefusedByDriftedProvenanceCertificateContract,
            run_mismatches[0].clone(),
            latest_attestation_seal_plan_step,
            latest_attestation_seal_step,
            None,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_provenance_certificate",
            verdict_for_result(status.result),
            session_dir,
            paths,
            &session,
            &status,
            Vec::new(),
        );
        persist_json(&paths.report_path, &report)?;
        return Ok(report);
    }

    let latest_attestation_seal_verify: AttestationSealLatestReportView = match run_json_command(
        ATTESTATION_SEAL_LATEST_BIN,
        &latest_attestation_seal_verify_args(&paths.nested_attestation_seal_latest_session_dir),
    ) {
        Ok(value) => serde_json::from_value(value)
            .context("failed parsing nested latest-attestation-seal verify json")?,
        Err(error) => {
            let status = refusal_status(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageProvenanceCertificateLatestResult::RefusedByDriftedProvenanceCertificateContract,
                format!(
                    "failed verifying nested latest-attestation-seal handoff under {}: {error}",
                    paths.nested_attestation_seal_latest_session_dir.display()
                ),
                latest_attestation_seal_plan_step,
                latest_attestation_seal_step,
                None,
            );
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_provenance_certificate",
                verdict_for_result(status.result),
                session_dir,
                paths,
                &session,
                &status,
                Vec::new(),
            );
            persist_json(&paths.report_path, &report)?;
            return Ok(report);
        }
    };
    if latest_attestation_seal_verify.verdict != ATTESTATION_SEAL_LATEST_VERIFY_OK {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageProvenanceCertificateLatestResult::RefusedByDriftedProvenanceCertificateContract,
            latest_attestation_seal_verify.reason,
            latest_attestation_seal_plan_step,
            latest_attestation_seal_step,
            None,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_provenance_certificate",
            verdict_for_result(status.result),
            session_dir,
            paths,
            &session,
            &status,
            Vec::new(),
        );
        persist_json(&paths.report_path, &report)?;
        return Ok(report);
    }

    let release_capsule_run_raw = match run_json_command(
        PROVENANCE_CERTIFICATE_BIN,
        &provenance_certificate_run_args(
            &resolved.snapshot,
            &expected_downstream_attestation_seal_session_dir(paths),
            &paths.nested_provenance_certificate_session_dir,
        ),
    ) {
        Ok(value) => value,
        Err(error) => {
            let status = refusal_status(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageProvenanceCertificateLatestResult::RefusedByDriftedProvenanceCertificateContract,
                format!(
                    "failed running downstream provenance-certificate contract under {}: {error}",
                    paths.nested_provenance_certificate_session_dir.display()
                ),
                latest_attestation_seal_plan_step,
                latest_attestation_seal_step,
                None,
            );
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_provenance_certificate",
                verdict_for_result(status.result),
                session_dir,
                paths,
                &session,
                &status,
                Vec::new(),
            );
            persist_json(&paths.report_path, &report)?;
            return Ok(report);
        }
    };
    persist_json(
        &paths.provenance_certificate_report_path,
        &release_capsule_run_raw,
    )?;
    let attestation_seal_run: ProvenanceCertificateReportView =
        serde_json::from_value(release_capsule_run_raw)
            .context("failed parsing nested provenance-certificate run json")?;
    let provenance_certificate_step = Some(step_artifact(
        &paths.provenance_certificate_report_path,
        "run_live_package_provenance_certificate",
        &attestation_seal_run.verdict,
        &attestation_seal_run.reason,
        attestation_seal_run.generated_at,
    ));

    let mut attestation_seal_mismatches = Vec::new();
    compare_provenance_certificate_run_against_snapshot(
        &attestation_seal_run,
        &resolved.snapshot,
        &expected_downstream_attestation_seal_session_dir(paths),
        &paths.nested_provenance_certificate_session_dir,
        &mut attestation_seal_mismatches,
    );
    if !attestation_seal_mismatches.is_empty() {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageProvenanceCertificateLatestResult::RefusedByDriftedProvenanceCertificateContract,
            attestation_seal_mismatches[0].clone(),
            latest_attestation_seal_plan_step,
            latest_attestation_seal_step,
            provenance_certificate_step,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_provenance_certificate",
            verdict_for_result(status.result),
            session_dir,
            paths,
            &session,
            &status,
            Vec::new(),
        );
        persist_json(&paths.report_path, &report)?;
        return Ok(report);
    }

    let attestation_seal_verify: ProvenanceCertificateReportView = match run_json_command(
        PROVENANCE_CERTIFICATE_BIN,
        &provenance_certificate_verify_args(
            &resolved.snapshot,
            &expected_downstream_attestation_seal_session_dir(paths),
            &paths.nested_provenance_certificate_session_dir,
        ),
    ) {
        Ok(value) => serde_json::from_value(value)
            .context("failed parsing nested provenance-certificate verify json")?,
        Err(error) => {
            let status = refusal_status(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageProvenanceCertificateLatestResult::RefusedByDriftedProvenanceCertificateContract,
                format!(
                    "failed verifying downstream provenance-certificate contract under {}: {error}",
                    paths.nested_provenance_certificate_session_dir.display()
                ),
                latest_attestation_seal_plan_step,
                latest_attestation_seal_step,
                provenance_certificate_step,
            );
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_provenance_certificate",
                verdict_for_result(status.result),
                session_dir,
                paths,
                &session,
                &status,
                Vec::new(),
            );
            persist_json(&paths.report_path, &report)?;
            return Ok(report);
        }
    };
    if attestation_seal_verify.verdict != PROVENANCE_CERTIFICATE_VERIFY_OK {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageProvenanceCertificateLatestResult::RefusedByDriftedProvenanceCertificateContract,
            attestation_seal_verify.reason,
            latest_attestation_seal_plan_step,
            latest_attestation_seal_step,
            provenance_certificate_step,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_provenance_certificate",
            verdict_for_result(status.result),
            session_dir,
            paths,
            &session,
            &status,
            Vec::new(),
        );
        persist_json(&paths.report_path, &report)?;
        return Ok(report);
    }

    let result = provenance_certificate_latest_result_from_provenance_certificate(
        attestation_seal_run.result.as_deref().unwrap_or_default(),
    )
    .unwrap_or(
        LivePackageProvenanceCertificateLatestResult::RefusedByDriftedProvenanceCertificateContract,
    );
    let status = PackageProvenanceCertificateLatestStatus {
        status_version: PROVENANCE_CERTIFICATE_LATEST_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        root: root.display().to_string(),
        session_dir: session_dir.display().to_string(),
        latest_top_step_name: Some(resolved.snapshot.latest_top_step_name.clone()),
        latest_top_session_dir: Some(
            resolved
                .snapshot
                .latest_top_session_dir
                .display()
                .to_string(),
        ),
        historical_execute_frozen_session_dir: Some(
            resolved
                .snapshot
                .historical_execute_frozen_session_dir
                .display()
                .to_string(),
        ),
        turn_green_session_dir: Some(
            resolved
                .snapshot
                .turn_green_session_dir
                .display()
                .to_string(),
        ),
        launch_packet_session_dir: Some(
            resolved
                .snapshot
                .launch_packet_session_dir
                .display()
                .to_string(),
        ),
        nested_attestation_seal_latest_session_dir: paths
            .nested_attestation_seal_latest_session_dir
            .display()
            .to_string(),
        nested_provenance_certificate_session_dir: paths
            .nested_provenance_certificate_session_dir
            .display()
            .to_string(),
        downstream_decision_packet_session_dir: resolved
            .snapshot
            .downstream_decision_packet_session_dir
            .display()
            .to_string(),
        result,
        reason: attestation_seal_run.reason.clone(),
        latest_attestation_seal_plan_verdict: Some(
            resolved.latest_attestation_seal_plan.verdict.clone(),
        ),
        latest_attestation_seal_verdict: Some(latest_attestation_seal_run.verdict.clone()),
        provenance_certificate_verdict: Some(attestation_seal_run.verdict.clone()),
        current_pre_activation_gate_verdict: attestation_seal_run
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: attestation_seal_run
            .current_pre_activation_gate_reason
            .clone(),
        latest_attestation_seal_plan_step,
        latest_attestation_seal_step,
        provenance_certificate_step,
        operator_next_action_summary: operator_next_action_summary(result),
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    let report = report_from_status(
        "run_latest_provenance_certificate",
        verdict_for_result(status.result),
        session_dir,
        paths,
        &session,
        &status,
        Vec::new(),
    );
    persist_json(&paths.report_path, &report)?;
    Ok(report)
}

fn run_latest_provenance_certificate_failure(
    root: &Path,
    session_dir: &Path,
    paths: &PackageProvenanceCertificateLatestPaths,
    failure: &LatestProvenanceCertificateResolutionFailure,
) -> Result<PackageProvenanceCertificateLatestReport> {
    let session = planned_session(root, session_dir, paths, None, Some(failure));
    persist_json(&paths.session_path, &session)?;
    if let Some(value) = &failure.latest_attestation_seal_plan_raw {
        persist_json(&paths.latest_attestation_seal_plan_report_path, value)?;
    }
    let status = refusal_status_from_resolution_failure(root, session_dir, paths, failure);
    persist_json(&paths.status_path, &status)?;
    let report = report_from_status(
        "run_latest_provenance_certificate",
        verdict_from_failure_kind(failure.kind),
        session_dir,
        paths,
        &session,
        &status,
        Vec::new(),
    );
    persist_json(&paths.report_path, &report)?;
    Ok(report)
}

fn resolve_latest_provenance_certificate(
    root: &Path,
    paths: &PackageProvenanceCertificateLatestPaths,
) -> std::result::Result<
    ResolvedLatestProvenanceCertificate,
    LatestProvenanceCertificateResolutionFailure,
> {
    let latest_attestation_seal_plan_raw = match run_json_command(
        ATTESTATION_SEAL_LATEST_BIN,
        &latest_attestation_seal_plan_args(root, &paths.nested_attestation_seal_latest_session_dir),
    ) {
        Ok(value) => value,
        Err(error) => {
            return Err(LatestProvenanceCertificateResolutionFailure {
                kind: ResolutionFailureKind::MissingLatestChain,
                reason: format!(
                    "failed resolving latest immutable chain under {}: {error}",
                    root.display()
                ),
                snapshot: None,
                latest_attestation_seal_plan_raw: None,
                latest_attestation_seal_plan_verdict: None,
                latest_attestation_seal_plan_reason: None,
                latest_attestation_seal_plan_generated_at: None,
            });
        }
    };
    let latest_attestation_seal_plan: AttestationSealLatestReportView =
        serde_json::from_value(latest_attestation_seal_plan_raw.clone()).map_err(|error| {
            LatestProvenanceCertificateResolutionFailure {
                kind: ResolutionFailureKind::InvalidLatestChain,
                reason: format!(
                    "failed parsing latest-attestation-seal plan json for {}: {error}",
                    root.display()
                ),
                snapshot: None,
                latest_attestation_seal_plan_raw: Some(latest_attestation_seal_plan_raw.clone()),
                latest_attestation_seal_plan_verdict: None,
                latest_attestation_seal_plan_reason: None,
                latest_attestation_seal_plan_generated_at: None,
            }
        })?;

    let snapshot = parse_snapshot_from_attestation_seal_latest(&latest_attestation_seal_plan).ok();
    if latest_attestation_seal_plan.verdict != ATTESTATION_SEAL_LATEST_PLAN_READY {
        return Err(LatestProvenanceCertificateResolutionFailure {
            kind: map_attestation_seal_latest_failure_kind(&latest_attestation_seal_plan.verdict),
            reason: latest_attestation_seal_plan.reason.clone(),
            snapshot,
            latest_attestation_seal_plan_raw: Some(latest_attestation_seal_plan_raw),
            latest_attestation_seal_plan_verdict: Some(latest_attestation_seal_plan.verdict),
            latest_attestation_seal_plan_reason: Some(latest_attestation_seal_plan.reason),
            latest_attestation_seal_plan_generated_at: Some(
                latest_attestation_seal_plan.generated_at,
            ),
        });
    }

    let snapshot = parse_snapshot_from_attestation_seal_latest(&latest_attestation_seal_plan)
        .map_err(|error| LatestProvenanceCertificateResolutionFailure {
            kind: ResolutionFailureKind::InvalidLatestChain,
            reason: error.to_string(),
            snapshot: None,
            latest_attestation_seal_plan_raw: Some(latest_attestation_seal_plan_raw.clone()),
            latest_attestation_seal_plan_verdict: Some(
                latest_attestation_seal_plan.verdict.clone(),
            ),
            latest_attestation_seal_plan_reason: Some(latest_attestation_seal_plan.reason.clone()),
            latest_attestation_seal_plan_generated_at: Some(
                latest_attestation_seal_plan.generated_at,
            ),
        })?;

    let mut mismatches = Vec::new();
    compare_attestation_seal_latest_plan_against_snapshot(
        &latest_attestation_seal_plan,
        &snapshot,
        &paths.nested_attestation_seal_latest_session_dir,
        &mut mismatches,
    );
    if !mismatches.is_empty() {
        return Err(LatestProvenanceCertificateResolutionFailure {
            kind: ResolutionFailureKind::DriftedProvenanceCertificateContract,
            reason: mismatches[0].clone(),
            snapshot: Some(snapshot),
            latest_attestation_seal_plan_raw: Some(latest_attestation_seal_plan_raw),
            latest_attestation_seal_plan_verdict: Some(latest_attestation_seal_plan.verdict),
            latest_attestation_seal_plan_reason: Some(latest_attestation_seal_plan.reason),
            latest_attestation_seal_plan_generated_at: Some(
                latest_attestation_seal_plan.generated_at,
            ),
        });
    }

    Ok(ResolvedLatestProvenanceCertificate {
        snapshot,
        latest_attestation_seal_plan_raw,
        latest_attestation_seal_plan,
    })
}

fn parse_snapshot_from_attestation_seal_latest(
    view: &AttestationSealLatestReportView,
) -> Result<LatestProvenanceCertificateSnapshot> {
    let latest_top_step_name =
        required_option_string(view.latest_top_step_name.as_ref(), "latest_top_step_name")?;
    if latest_top_step_name != TOP_ACCEPTED_PACKAGE_STEP {
        bail!(
            "latest-attestation-seal plan top accepted step is {} instead of {}",
            latest_top_step_name,
            TOP_ACCEPTED_PACKAGE_STEP
        );
    }
    if view.activation_authorized {
        bail!(
            "latest-attestation-seal plan must remain planning-safe with activation_authorized=false"
        );
    }
    Ok(LatestProvenanceCertificateSnapshot {
        latest_top_step_name,
        latest_top_session_dir: PathBuf::from(required_option_string(
            view.latest_top_session_dir.as_ref(),
            "latest_top_session_dir",
        )?),
        downstream_decision_packet_session_dir: PathBuf::from(required_option_string(
            view.downstream_decision_packet_session_dir.as_ref(),
            "downstream_decision_packet_session_dir",
        )?),
        historical_execute_frozen_session_dir: PathBuf::from(required_option_string(
            view.historical_execute_frozen_session_dir.as_ref(),
            "historical_execute_frozen_session_dir",
        )?),
        turn_green_session_dir: PathBuf::from(required_option_string(
            view.turn_green_session_dir.as_ref(),
            "turn_green_session_dir",
        )?),
        launch_packet_session_dir: PathBuf::from(required_option_string(
            view.launch_packet_session_dir.as_ref(),
            "launch_packet_session_dir",
        )?),
        package_dir: PathBuf::from(required_option_string(
            view.package_dir.as_ref(),
            "package_dir",
        )?),
        install_root: PathBuf::from(required_option_string(
            view.install_root.as_ref(),
            "install_root",
        )?),
        target_service_name: required_option_string(
            view.target_service_name.as_ref(),
            "target_service_name",
        )?,
        backend_command: required_option_string(view.backend_command.as_ref(), "backend_command")?,
        wrapper_timeout_ms: view
            .wrapper_timeout_ms
            .ok_or_else(|| anyhow!("latest-attestation-seal plan is missing wrapper_timeout_ms"))?,
        service_status_max_staleness_ms: view.service_status_max_staleness_ms.ok_or_else(|| {
            anyhow!("latest-attestation-seal plan is missing service_status_max_staleness_ms")
        })?,
        reviewed_frozen_live_cutover_controller_command_summary: required_option_string(
            view.reviewed_frozen_live_cutover_controller_command_summary
                .as_ref(),
            "reviewed_frozen_live_cutover_controller_command_summary",
        )?,
        clerestory_certificate_summary: required_option_string(
            view.clerestory_certificate_summary.as_ref(),
            "clerestory_certificate_summary",
        )?,
        clerestory_certificate_sha256: required_option_string(
            view.clerestory_certificate_sha256.as_ref(),
            "clerestory_certificate_sha256",
        )?,
        clerestory_certificate_algorithm: required_option_string(
            view.clerestory_certificate_algorithm.as_ref(),
            "clerestory_certificate_algorithm",
        )?,
    })
}

fn plan_report_from_resolution(
    mode: &str,
    verdict: TinyLivePackageProvenanceCertificateLatestVerdict,
    reason: String,
    root: &Path,
    session_dir: &Path,
    paths: &PackageProvenanceCertificateLatestPaths,
    resolved: &ResolvedLatestProvenanceCertificate,
) -> PackageProvenanceCertificateLatestReport {
    PackageProvenanceCertificateLatestReport {
        generated_at: Utc::now(),
        mode: canonical_wrapper_mode(mode).to_string(),
        verdict,
        reason,
        root: Some(root.display().to_string()),
        latest_top_step_name: Some(resolved.snapshot.latest_top_step_name.clone()),
        latest_top_session_dir: Some(
            resolved
                .snapshot
                .latest_top_session_dir
                .display()
                .to_string(),
        ),
        historical_execute_frozen_session_dir: Some(
            resolved
                .snapshot
                .historical_execute_frozen_session_dir
                .display()
                .to_string(),
        ),
        turn_green_session_dir: Some(
            resolved
                .snapshot
                .turn_green_session_dir
                .display()
                .to_string(),
        ),
        launch_packet_session_dir: Some(
            resolved
                .snapshot
                .launch_packet_session_dir
                .display()
                .to_string(),
        ),
        package_dir: Some(resolved.snapshot.package_dir.display().to_string()),
        install_root: Some(resolved.snapshot.install_root.display().to_string()),
        target_service_name: Some(resolved.snapshot.target_service_name.clone()),
        backend_command: Some(resolved.snapshot.backend_command.clone()),
        wrapper_timeout_ms: Some(resolved.snapshot.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(resolved.snapshot.service_status_max_staleness_ms),
        session_dir: Some(session_dir.display().to_string()),
        provenance_certificate_latest_session_path: Some(paths.session_path.display().to_string()),
        provenance_certificate_latest_status_path: Some(paths.status_path.display().to_string()),
        provenance_certificate_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_attestation_seal_plan_report_path: Some(
            paths
                .latest_attestation_seal_plan_report_path
                .display()
                .to_string(),
        ),
        latest_attestation_seal_report_path: Some(
            paths
                .latest_attestation_seal_report_path
                .display()
                .to_string(),
        ),
        provenance_certificate_report_path: Some(
            paths
                .provenance_certificate_report_path
                .display()
                .to_string(),
        ),
        nested_attestation_seal_latest_session_dir: Some(
            paths
                .nested_attestation_seal_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_provenance_certificate_session_dir: Some(
            paths
                .nested_provenance_certificate_session_dir
                .display()
                .to_string(),
        ),
        downstream_decision_packet_session_dir: Some(
            resolved
                .snapshot
                .downstream_decision_packet_session_dir
                .display()
                .to_string(),
        ),
        latest_attestation_seal_plan_verdict: Some(
            resolved.latest_attestation_seal_plan.verdict.clone(),
        ),
        latest_attestation_seal_verdict: None,
        provenance_certificate_verdict: None,
        result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        resolve_latest_attestation_seal_command_summary: Some(
            resolve_latest_attestation_seal_command_summary(
                root,
                &paths.nested_attestation_seal_latest_session_dir,
            ),
        ),
        run_latest_attestation_seal_command_summary: Some(
            run_latest_attestation_seal_command_summary(
                root,
                &paths.nested_attestation_seal_latest_session_dir,
            ),
        ),
        verify_latest_attestation_seal_command_summary: Some(
            verify_latest_attestation_seal_command_summary(
                &paths.nested_attestation_seal_latest_session_dir,
            ),
        ),
        downstream_provenance_certificate_command_summary: Some(
            downstream_provenance_certificate_command_summary(
                &resolved.snapshot,
                &expected_downstream_attestation_seal_session_dir(paths),
                &paths.nested_provenance_certificate_session_dir,
            ),
        ),
        verify_provenance_certificate_command_summary: Some(
            verify_provenance_certificate_command_summary(
                &resolved.snapshot,
                &expected_downstream_attestation_seal_session_dir(paths),
                &paths.nested_provenance_certificate_session_dir,
            ),
        ),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            resolved
                .snapshot
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        clerestory_certificate_summary: Some(
            resolved.snapshot.clerestory_certificate_summary.clone(),
        ),
        clerestory_certificate_sha256: Some(
            resolved.snapshot.clerestory_certificate_sha256.clone(),
        ),
        clerestory_certificate_algorithm: Some(
            resolved.snapshot.clerestory_certificate_algorithm.clone(),
        ),
        verification_mismatches: Vec::new(),
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn report_from_resolution_failure(
    mode: &str,
    root: &Path,
    session_dir: &Path,
    paths: &PackageProvenanceCertificateLatestPaths,
    failure: &LatestProvenanceCertificateResolutionFailure,
) -> PackageProvenanceCertificateLatestReport {
    let snapshot = failure.snapshot.as_ref();
    PackageProvenanceCertificateLatestReport {
        generated_at: Utc::now(),
        mode: canonical_wrapper_mode(mode).to_string(),
        verdict: verdict_from_failure_kind(failure.kind),
        reason: failure.reason.clone(),
        root: Some(root.display().to_string()),
        latest_top_step_name: snapshot.map(|value| value.latest_top_step_name.clone()),
        latest_top_session_dir: snapshot
            .map(|value| value.latest_top_session_dir.display().to_string()),
        historical_execute_frozen_session_dir: snapshot.map(|value| {
            value
                .historical_execute_frozen_session_dir
                .display()
                .to_string()
        }),
        turn_green_session_dir: snapshot
            .map(|value| value.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: snapshot
            .map(|value| value.launch_packet_session_dir.display().to_string()),
        package_dir: snapshot.map(|value| value.package_dir.display().to_string()),
        install_root: snapshot.map(|value| value.install_root.display().to_string()),
        target_service_name: snapshot.map(|value| value.target_service_name.clone()),
        backend_command: snapshot.map(|value| value.backend_command.clone()),
        wrapper_timeout_ms: snapshot.map(|value| value.wrapper_timeout_ms),
        service_status_max_staleness_ms: snapshot
            .map(|value| value.service_status_max_staleness_ms),
        session_dir: Some(session_dir.display().to_string()),
        provenance_certificate_latest_session_path: Some(paths.session_path.display().to_string()),
        provenance_certificate_latest_status_path: Some(paths.status_path.display().to_string()),
        provenance_certificate_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_attestation_seal_plan_report_path: Some(
            paths
                .latest_attestation_seal_plan_report_path
                .display()
                .to_string(),
        ),
        latest_attestation_seal_report_path: Some(
            paths
                .latest_attestation_seal_report_path
                .display()
                .to_string(),
        ),
        provenance_certificate_report_path: Some(
            paths
                .provenance_certificate_report_path
                .display()
                .to_string(),
        ),
        nested_attestation_seal_latest_session_dir: Some(
            paths
                .nested_attestation_seal_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_provenance_certificate_session_dir: Some(
            paths
                .nested_provenance_certificate_session_dir
                .display()
                .to_string(),
        ),
        downstream_decision_packet_session_dir: snapshot.map(|value| {
            value
                .downstream_decision_packet_session_dir
                .display()
                .to_string()
        }),
        latest_attestation_seal_plan_verdict: failure.latest_attestation_seal_plan_verdict.clone(),
        latest_attestation_seal_verdict: None,
        provenance_certificate_verdict: None,
        result: Some(serialize_enum(&result_from_failure_kind(failure.kind))),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        resolve_latest_attestation_seal_command_summary: Some(
            resolve_latest_attestation_seal_command_summary(
                root,
                &paths.nested_attestation_seal_latest_session_dir,
            ),
        ),
        run_latest_attestation_seal_command_summary: Some(
            run_latest_attestation_seal_command_summary(
                root,
                &paths.nested_attestation_seal_latest_session_dir,
            ),
        ),
        verify_latest_attestation_seal_command_summary: Some(
            verify_latest_attestation_seal_command_summary(
                &paths.nested_attestation_seal_latest_session_dir,
            ),
        ),
        downstream_provenance_certificate_command_summary: snapshot.map(|value| {
            downstream_provenance_certificate_command_summary(
                value,
                &expected_downstream_attestation_seal_session_dir(paths),
                &paths.nested_provenance_certificate_session_dir,
            )
        }),
        verify_provenance_certificate_command_summary: snapshot.map(|value| {
            verify_provenance_certificate_command_summary(
                value,
                &expected_downstream_attestation_seal_session_dir(paths),
                &paths.nested_provenance_certificate_session_dir,
            )
        }),
        reviewed_frozen_live_cutover_controller_command_summary: snapshot.map(|value| {
            value
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone()
        }),
        clerestory_certificate_summary: snapshot
            .map(|value| value.clerestory_certificate_summary.clone()),
        clerestory_certificate_sha256: snapshot
            .map(|value| value.clerestory_certificate_sha256.clone()),
        clerestory_certificate_algorithm: snapshot
            .map(|value| value.clerestory_certificate_algorithm.clone()),
        verification_mismatches: Vec::new(),
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn planned_session(
    root: &Path,
    session_dir: &Path,
    paths: &PackageProvenanceCertificateLatestPaths,
    resolved: Option<&ResolvedLatestProvenanceCertificate>,
    failure: Option<&LatestProvenanceCertificateResolutionFailure>,
) -> PackageProvenanceCertificateLatestSession {
    let snapshot = resolved
        .map(|value| &value.snapshot)
        .or_else(|| failure.and_then(|value| value.snapshot.as_ref()));
    PackageProvenanceCertificateLatestSession {
        session_version: PROVENANCE_CERTIFICATE_LATEST_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        root: root.display().to_string(),
        session_dir: session_dir.display().to_string(),
        nested_attestation_seal_latest_session_dir: paths
            .nested_attestation_seal_latest_session_dir
            .display()
            .to_string(),
        nested_provenance_certificate_session_dir: paths
            .nested_provenance_certificate_session_dir
            .display()
            .to_string(),
        downstream_decision_packet_session_dir: snapshot
            .map(|value| {
                value
                    .downstream_decision_packet_session_dir
                    .display()
                    .to_string()
            })
            .unwrap_or_else(|| {
                expected_downstream_decision_packet_session_dir(paths)
                    .display()
                    .to_string()
            }),
        latest_top_step_name: snapshot.map(|value| value.latest_top_step_name.clone()),
        latest_top_session_dir: snapshot
            .map(|value| value.latest_top_session_dir.display().to_string()),
        historical_execute_frozen_session_dir: snapshot.map(|value| {
            value
                .historical_execute_frozen_session_dir
                .display()
                .to_string()
        }),
        turn_green_session_dir: snapshot
            .map(|value| value.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: snapshot
            .map(|value| value.launch_packet_session_dir.display().to_string()),
        package_dir: snapshot.map(|value| value.package_dir.display().to_string()),
        install_root: snapshot.map(|value| value.install_root.display().to_string()),
        target_service_name: snapshot.map(|value| value.target_service_name.clone()),
        backend_command: snapshot.map(|value| value.backend_command.clone()),
        wrapper_timeout_ms: snapshot.map(|value| value.wrapper_timeout_ms),
        service_status_max_staleness_ms: snapshot
            .map(|value| value.service_status_max_staleness_ms),
        resolve_latest_attestation_seal_command_summary:
            resolve_latest_attestation_seal_command_summary(
                root,
                &paths.nested_attestation_seal_latest_session_dir,
            ),
        run_latest_attestation_seal_command_summary: run_latest_attestation_seal_command_summary(
            root,
            &paths.nested_attestation_seal_latest_session_dir,
        ),
        verify_latest_attestation_seal_command_summary:
            verify_latest_attestation_seal_command_summary(
                &paths.nested_attestation_seal_latest_session_dir,
            ),
        downstream_provenance_certificate_command_summary: snapshot
            .map(|value| {
                downstream_provenance_certificate_command_summary(
                    value,
                    &expected_downstream_attestation_seal_session_dir(paths),
                    &paths.nested_provenance_certificate_session_dir,
                )
            })
            .unwrap_or_else(|| "<attestation-seal-run-unavailable>".to_string()),
        verify_provenance_certificate_command_summary: snapshot
            .map(|value| {
                verify_provenance_certificate_command_summary(
                    value,
                    &expected_downstream_attestation_seal_session_dir(paths),
                    &paths.nested_provenance_certificate_session_dir,
                )
            })
            .unwrap_or_else(|| "<attestation-seal-verify-unavailable>".to_string()),
        reviewed_frozen_live_cutover_controller_command_summary: snapshot.map(|value| {
            value
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone()
        }),
        clerestory_certificate_summary: snapshot
            .map(|value| value.clerestory_certificate_summary.clone()),
        clerestory_certificate_sha256: snapshot
            .map(|value| value.clerestory_certificate_sha256.clone()),
        clerestory_certificate_algorithm: snapshot
            .map(|value| value.clerestory_certificate_algorithm.clone()),
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn report_from_status(
    mode: &str,
    verdict: TinyLivePackageProvenanceCertificateLatestVerdict,
    session_dir: &Path,
    paths: &PackageProvenanceCertificateLatestPaths,
    session: &PackageProvenanceCertificateLatestSession,
    status: &PackageProvenanceCertificateLatestStatus,
    verification_mismatches: Vec<String>,
) -> PackageProvenanceCertificateLatestReport {
    PackageProvenanceCertificateLatestReport {
        generated_at: Utc::now(),
        mode: canonical_wrapper_mode(mode).to_string(),
        verdict,
        reason: status.reason.clone(),
        root: Some(session.root.clone()),
        latest_top_step_name: status.latest_top_step_name.clone(),
        latest_top_session_dir: status.latest_top_session_dir.clone(),
        historical_execute_frozen_session_dir: status.historical_execute_frozen_session_dir.clone(),
        turn_green_session_dir: status.turn_green_session_dir.clone(),
        launch_packet_session_dir: status.launch_packet_session_dir.clone(),
        package_dir: session.package_dir.clone(),
        install_root: session.install_root.clone(),
        target_service_name: session.target_service_name.clone(),
        backend_command: session.backend_command.clone(),
        wrapper_timeout_ms: session.wrapper_timeout_ms,
        service_status_max_staleness_ms: session.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        provenance_certificate_latest_session_path: Some(paths.session_path.display().to_string()),
        provenance_certificate_latest_status_path: Some(paths.status_path.display().to_string()),
        provenance_certificate_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_attestation_seal_plan_report_path: Some(
            paths
                .latest_attestation_seal_plan_report_path
                .display()
                .to_string(),
        ),
        latest_attestation_seal_report_path: Some(
            paths
                .latest_attestation_seal_report_path
                .display()
                .to_string(),
        ),
        provenance_certificate_report_path: Some(
            paths
                .provenance_certificate_report_path
                .display()
                .to_string(),
        ),
        nested_attestation_seal_latest_session_dir: Some(
            session.nested_attestation_seal_latest_session_dir.clone(),
        ),
        nested_provenance_certificate_session_dir: Some(
            session.nested_provenance_certificate_session_dir.clone(),
        ),
        downstream_decision_packet_session_dir: Some(
            session.downstream_decision_packet_session_dir.clone(),
        ),
        latest_attestation_seal_plan_verdict: status.latest_attestation_seal_plan_verdict.clone(),
        latest_attestation_seal_verdict: status.latest_attestation_seal_verdict.clone(),
        provenance_certificate_verdict: status.provenance_certificate_verdict.clone(),
        result: Some(serialize_enum(&status.result)),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        resolve_latest_attestation_seal_command_summary: Some(
            session
                .resolve_latest_attestation_seal_command_summary
                .clone(),
        ),
        run_latest_attestation_seal_command_summary: Some(
            session.run_latest_attestation_seal_command_summary.clone(),
        ),
        verify_latest_attestation_seal_command_summary: Some(
            session
                .verify_latest_attestation_seal_command_summary
                .clone(),
        ),
        downstream_provenance_certificate_command_summary: Some(
            session
                .downstream_provenance_certificate_command_summary
                .clone(),
        ),
        verify_provenance_certificate_command_summary: Some(
            session
                .verify_provenance_certificate_command_summary
                .clone(),
        ),
        reviewed_frozen_live_cutover_controller_command_summary: session
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        clerestory_certificate_summary: session.clerestory_certificate_summary.clone(),
        clerestory_certificate_sha256: session.clerestory_certificate_sha256.clone(),
        clerestory_certificate_algorithm: session.clerestory_certificate_algorithm.clone(),
        verification_mismatches,
        activation_authorized: status.activation_authorized,
        explicit_statement: session.explicit_statement.clone(),
    }
}

fn provenance_certificate_latest_paths(
    session_dir: &Path,
) -> PackageProvenanceCertificateLatestPaths {
    PackageProvenanceCertificateLatestPaths {
        session_path: session_dir
            .join("tiny_live_activation_package_provenance_certificate_latest.session.json"),
        status_path: session_dir
            .join("tiny_live_activation_package_provenance_certificate_latest.status.json"),
        report_path: session_dir
            .join("tiny_live_activation_package_provenance_certificate_latest.report.json"),
        latest_attestation_seal_plan_report_path: session_dir.join(
            "tiny_live_activation_package_provenance_certificate_latest.attestation_seal_latest_plan.report.json",
        ),
        latest_attestation_seal_report_path: session_dir.join(
            "tiny_live_activation_package_provenance_certificate_latest.attestation_seal_latest.report.json",
        ),
        provenance_certificate_report_path: session_dir.join(
            "tiny_live_activation_package_provenance_certificate_latest.provenance_certificate.report.json",
        ),
        nested_attestation_seal_latest_session_dir: session_dir
            .join("tiny_live_activation_package_provenance_certificate_latest.attestation_seal_latest_session"),
        nested_provenance_certificate_session_dir: session_dir.join(
            "tiny_live_activation_package_provenance_certificate_latest.provenance_certificate_session",
        ),
    }
}

fn expected_downstream_decision_packet_session_dir(
    paths: &PackageProvenanceCertificateLatestPaths,
) -> PathBuf {
    paths
        .nested_attestation_seal_latest_session_dir
        .join("tiny_live_activation_package_attestation_seal_latest.decision_packet_session")
}

fn expected_downstream_attestation_seal_session_dir(
    paths: &PackageProvenanceCertificateLatestPaths,
) -> PathBuf {
    paths
        .nested_attestation_seal_latest_session_dir
        .join("tiny_live_activation_package_attestation_seal_latest.attestation_seal_session")
}

fn expected_downstream_review_receipt_session_dir(
    paths: &PackageProvenanceCertificateLatestPaths,
) -> PathBuf {
    paths
        .nested_attestation_seal_latest_session_dir
        .join("tiny_live_activation_package_attestation_seal_latest.review_receipt_session")
}

fn latest_attestation_seal_plan_args(root: &Path, session_dir: &Path) -> Vec<String> {
    vec![
        "--root".to_string(),
        root.display().to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--plan-latest-attestation-seal".to_string(),
        "--json".to_string(),
    ]
}

fn latest_attestation_seal_run_args(root: &Path, session_dir: &Path) -> Vec<String> {
    vec![
        "--root".to_string(),
        root.display().to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--run-latest-attestation-seal".to_string(),
        "--json".to_string(),
    ]
}

fn latest_attestation_seal_verify_args(session_dir: &Path) -> Vec<String> {
    vec![
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--verify-latest-attestation-seal".to_string(),
        "--json".to_string(),
    ]
}

fn provenance_certificate_run_args(
    snapshot: &LatestProvenanceCertificateSnapshot,
    attestation_seal_session_dir: &Path,
    session_dir: &Path,
) -> Vec<String> {
    vec![
        "--attestation-seal-session-dir".to_string(),
        attestation_seal_session_dir.display().to_string(),
        "--confirm-decision-packet-session-dir".to_string(),
        snapshot
            .downstream_decision_packet_session_dir
            .display()
            .to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--run-live-package-provenance-certificate".to_string(),
        "--json".to_string(),
    ]
}

fn provenance_certificate_verify_args(
    snapshot: &LatestProvenanceCertificateSnapshot,
    attestation_seal_session_dir: &Path,
    session_dir: &Path,
) -> Vec<String> {
    vec![
        "--attestation-seal-session-dir".to_string(),
        attestation_seal_session_dir.display().to_string(),
        "--confirm-decision-packet-session-dir".to_string(),
        snapshot
            .downstream_decision_packet_session_dir
            .display()
            .to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--verify-live-package-provenance-certificate".to_string(),
        "--json".to_string(),
    ]
}

fn resolve_latest_attestation_seal_command_summary(root: &Path, session_dir: &Path) -> String {
    format!(
        "{ATTESTATION_SEAL_LATEST_BIN} --root {} --session-dir {} --plan-latest-attestation-seal --json",
        shell_escape_path(root),
        shell_escape_path(session_dir),
    )
}

fn run_latest_attestation_seal_command_summary(root: &Path, session_dir: &Path) -> String {
    format!(
        "{ATTESTATION_SEAL_LATEST_BIN} --root {} --session-dir {} --run-latest-attestation-seal --json",
        shell_escape_path(root),
        shell_escape_path(session_dir),
    )
}

fn verify_latest_attestation_seal_command_summary(session_dir: &Path) -> String {
    format!(
        "{ATTESTATION_SEAL_LATEST_BIN} --session-dir {} --verify-latest-attestation-seal --json",
        shell_escape_path(session_dir),
    )
}

fn downstream_provenance_certificate_command_summary(
    snapshot: &LatestProvenanceCertificateSnapshot,
    attestation_seal_session_dir: &Path,
    session_dir: &Path,
) -> String {
    format!(
        "{PROVENANCE_CERTIFICATE_BIN} --attestation-seal-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --run-live-package-provenance-certificate --json",
        shell_escape_path(attestation_seal_session_dir),
        shell_escape_path(&snapshot.downstream_decision_packet_session_dir),
        shell_escape_path(session_dir),
    )
}

fn verify_provenance_certificate_command_summary(
    snapshot: &LatestProvenanceCertificateSnapshot,
    attestation_seal_session_dir: &Path,
    session_dir: &Path,
) -> String {
    format!(
        "{PROVENANCE_CERTIFICATE_BIN} --attestation-seal-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --verify-live-package-provenance-certificate --json",
        shell_escape_path(attestation_seal_session_dir),
        shell_escape_path(&snapshot.downstream_decision_packet_session_dir),
        shell_escape_path(session_dir),
    )
}

fn nested_attestation_seal_latest_report_path(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_attestation_seal_latest.report.json")
}

fn nested_provenance_certificate_session_path(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_provenance_certificate.session.json")
}

fn nested_provenance_certificate_status_path(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_provenance_certificate.status.json")
}

fn set_mode(mode: &mut Option<Mode>, value: Mode, flag: &str) -> Result<()> {
    if mode.replace(value).is_some() {
        bail!("multiple modes specified; latest was {flag}");
    }
    Ok(())
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    value.ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn run_json_command(binary: &str, args: &[String]) -> Result<Value> {
    let output = Command::new(binary)
        .args(args)
        .output()
        .with_context(|| format!("failed spawning {binary}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("{binary} exited with {}: {}", output.status, stderr.trim());
    }
    serde_json::from_slice(&output.stdout)
        .with_context(|| format!("failed parsing JSON output from {binary}"))
}

fn shell_escape_path(path: &Path) -> String {
    shell_escape_string(&path.display().to_string())
}

fn shell_escape_string(value: &str) -> String {
    if value
        .chars()
        .all(|raw| raw.is_ascii_alphanumeric() || "/._-".contains(raw))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\"'\"'"))
    }
}

fn create_clean_session_dir(session_dir: &Path, label: &str) -> Result<()> {
    fs::create_dir_all(session_dir).with_context(|| {
        format!(
            "failed creating {label} session dir {}",
            session_dir.display()
        )
    })
}

fn write_script(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, contents).with_context(|| {
        format!(
            "failed writing latest-provenance-certificate script to {}",
            path.display()
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms)?;
    }
    Ok(())
}

fn persist_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(value)?)
        .with_context(|| format!("failed writing {}", path.display()))
}

fn load_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let bytes = fs::read(path).with_context(|| format!("failed reading {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("failed parsing {}", path.display()))
}

fn step_artifact(
    path: &Path,
    mode: &str,
    verdict: &str,
    reason: &str,
    generated_at: DateTime<Utc>,
) -> PackageProvenanceCertificateLatestStepArtifact {
    PackageProvenanceCertificateLatestStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn canonical_wrapper_mode(mode: &str) -> &str {
    match mode {
        "plan_latest_provenance_certificate" => "plan_latest_provenance_certificate",
        "render_latest_provenance_certificate_script" => {
            "render_latest_provenance_certificate_script"
        }
        "run_latest_provenance_certificate" => "run_latest_provenance_certificate",
        "verify_latest_provenance_certificate" => "verify_latest_provenance_certificate",
        other => other,
    }
}

fn load_required_step_json<T: for<'de> Deserialize<'de>>(
    step: &Option<PackageProvenanceCertificateLatestStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("{label} is missing from latest-provenance-certificate status"))?;
    compare_string(
        &step.path,
        &expected_path.display().to_string(),
        &format!("{label} path"),
        mismatches,
    );
    load_json(expected_path)
}

fn compare_string(actual: &str, expected: &str, label: &str, mismatches: &mut Vec<String>) {
    if actual != expected {
        mismatches.push(format!(
            "{label} {actual:?} does not match expected {expected:?}"
        ));
    }
}

fn compare_option_string(
    actual: Option<&str>,
    expected: Option<&str>,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    if actual != expected {
        mismatches.push(format!(
            "{label} {:?} does not match expected {:?}",
            actual, expected
        ));
    }
}

fn compare_option_u64(
    actual: Option<u64>,
    expected: Option<u64>,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    if actual != expected {
        mismatches.push(format!(
            "{label} {:?} does not match expected {:?}",
            actual, expected
        ));
    }
}

fn compare_option_usize(
    actual: Option<usize>,
    expected: Option<usize>,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    if actual != expected {
        mismatches.push(format!(
            "{label} {:?} does not match expected {:?}",
            actual, expected
        ));
    }
}

fn compare_step_artifact_against_report_metadata(
    step: Option<&PackageProvenanceCertificateLatestStepArtifact>,
    expected_path: &Path,
    expected_mode: &str,
    expected_verdict: &str,
    expected_reason: &str,
    expected_generated_at: DateTime<Utc>,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    let Some(step) = step else {
        mismatches.push(format!(
            "{label} is missing from latest-provenance-certificate status"
        ));
        return;
    };
    compare_string(
        &step.path,
        &expected_path.display().to_string(),
        &format!("{label} path"),
        mismatches,
    );
    compare_string(
        &step.mode,
        expected_mode,
        &format!("{label} mode"),
        mismatches,
    );
    compare_string(
        &step.verdict,
        expected_verdict,
        &format!("{label} verdict"),
        mismatches,
    );
    compare_string(
        &step.reason,
        expected_reason,
        &format!("{label} reason"),
        mismatches,
    );
    if step.generated_at != expected_generated_at {
        mismatches.push(format!(
            "{label} generated_at {:?} does not match expected {:?}",
            step.generated_at, expected_generated_at
        ));
    }
}

fn compare_attestation_seal_latest_plan_against_snapshot(
    stored: &AttestationSealLatestReportView,
    snapshot: &LatestProvenanceCertificateSnapshot,
    session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        Some(stored.verdict.as_str()),
        Some(ATTESTATION_SEAL_LATEST_PLAN_READY),
        "stored latest-attestation-seal plan verdict",
        mismatches,
    );
    compare_string(
        &stored.mode,
        "plan_latest_attestation_seal",
        "stored latest-attestation-seal plan mode",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_step_name.as_deref(),
        Some(snapshot.latest_top_step_name.as_str()),
        "stored latest-attestation-seal plan latest_top_step_name",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_session_dir.as_deref(),
        Some(
            snapshot
                .latest_top_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored latest-attestation-seal plan latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored latest-attestation-seal plan session_dir",
        mismatches,
    );
    compare_option_string(
        stored.downstream_decision_packet_session_dir.as_deref(),
        Some(
            snapshot
                .downstream_decision_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored latest-attestation-seal plan downstream_decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored latest-attestation-seal plan package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored latest-attestation-seal plan install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored latest-attestation-seal plan target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored latest-attestation-seal plan backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored latest-attestation-seal plan wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored latest-attestation-seal plan service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        stored
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        Some(
            snapshot
                .reviewed_frozen_live_cutover_controller_command_summary
                .as_str(),
        ),
        "stored latest-attestation-seal plan reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_summary.as_deref(),
        Some(snapshot.clerestory_certificate_summary.as_str()),
        "stored latest-attestation-seal plan clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_sha256.as_deref(),
        Some(snapshot.clerestory_certificate_sha256.as_str()),
        "stored latest-attestation-seal plan clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_algorithm.as_deref(),
        Some(snapshot.clerestory_certificate_algorithm.as_str()),
        "stored latest-attestation-seal plan clerestory_certificate_algorithm",
        mismatches,
    );
}

fn compare_attestation_seal_latest_run_against_snapshot(
    stored: &AttestationSealLatestReportView,
    snapshot: &LatestProvenanceCertificateSnapshot,
    session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.mode,
        "run_latest_attestation_seal",
        "stored latest-attestation-seal run mode",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_step_name.as_deref(),
        Some(snapshot.latest_top_step_name.as_str()),
        "stored latest-attestation-seal run latest_top_step_name",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_session_dir.as_deref(),
        Some(
            snapshot
                .latest_top_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored latest-attestation-seal run latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored latest-attestation-seal run session_dir",
        mismatches,
    );
    compare_option_string(
        stored.downstream_decision_packet_session_dir.as_deref(),
        Some(
            snapshot
                .downstream_decision_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored latest-attestation-seal run downstream_decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored latest-attestation-seal run package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored latest-attestation-seal run install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored latest-attestation-seal run target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored latest-attestation-seal run backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored latest-attestation-seal run wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored latest-attestation-seal run service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        stored
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        Some(
            snapshot
                .reviewed_frozen_live_cutover_controller_command_summary
                .as_str(),
        ),
        "stored latest-attestation-seal run reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_summary.as_deref(),
        Some(snapshot.clerestory_certificate_summary.as_str()),
        "stored latest-attestation-seal run clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_sha256.as_deref(),
        Some(snapshot.clerestory_certificate_sha256.as_str()),
        "stored latest-attestation-seal run clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_algorithm.as_deref(),
        Some(snapshot.clerestory_certificate_algorithm.as_str()),
        "stored latest-attestation-seal run clerestory_certificate_algorithm",
        mismatches,
    );
}

fn compare_attestation_seal_latest_copy_matches_actual(
    stored: &AttestationSealLatestReportView,
    actual: &AttestationSealLatestReportView,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        Some(stored.verdict.as_str()),
        Some(actual.verdict.as_str()),
        "stored latest-attestation-seal copy verdict",
        mismatches,
    );
    compare_option_string(
        Some(stored.reason.as_str()),
        Some(actual.reason.as_str()),
        "stored latest-attestation-seal copy reason",
        mismatches,
    );
    if stored.generated_at != actual.generated_at {
        mismatches.push(format!(
            "stored latest-attestation-seal copy generated_at {:?} does not match actual {:?}",
            stored.generated_at, actual.generated_at
        ));
    }
    compare_option_string(
        stored.latest_top_step_name.as_deref(),
        actual.latest_top_step_name.as_deref(),
        "stored latest-attestation-seal copy latest_top_step_name",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_session_dir.as_deref(),
        actual.latest_top_session_dir.as_deref(),
        "stored latest-attestation-seal copy latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        actual.session_dir.as_deref(),
        "stored latest-attestation-seal copy session_dir",
        mismatches,
    );
    compare_option_string(
        stored.downstream_decision_packet_session_dir.as_deref(),
        actual.downstream_decision_packet_session_dir.as_deref(),
        "stored latest-attestation-seal copy downstream_decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.result.as_deref(),
        actual.result.as_deref(),
        "stored latest-attestation-seal copy result",
        mismatches,
    );
    compare_option_string(
        stored.explicit_statement.as_deref(),
        actual.explicit_statement.as_deref(),
        "stored latest-attestation-seal copy explicit_statement",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_verdict.as_deref(),
        actual.current_pre_activation_gate_verdict.as_deref(),
        "stored latest-attestation-seal copy current_pre_activation_gate_verdict",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_reason.as_deref(),
        actual.current_pre_activation_gate_reason.as_deref(),
        "stored latest-attestation-seal copy current_pre_activation_gate_reason",
        mismatches,
    );
    compare_option_string(
        stored
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        actual
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        "stored latest-attestation-seal copy reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
}

fn compare_provenance_certificate_run_against_snapshot(
    stored: &ProvenanceCertificateReportView,
    snapshot: &LatestProvenanceCertificateSnapshot,
    attestation_seal_session_dir: &Path,
    session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.mode,
        "run_live_package_provenance_certificate",
        "stored provenance-certificate run mode",
        mismatches,
    );
    compare_string(
        &stored.attestation_seal_session_dir,
        &attestation_seal_session_dir.display().to_string(),
        "stored provenance-certificate run attestation_seal_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.decision_packet_session_dir.as_deref(),
        Some(
            snapshot
                .downstream_decision_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored provenance-certificate run decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.execute_frozen_session_dir.as_deref(),
        Some(
            snapshot
                .historical_execute_frozen_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored provenance-certificate run execute_frozen_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.turn_green_session_dir.as_deref(),
        Some(
            snapshot
                .turn_green_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored provenance-certificate run turn_green_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.launch_packet_session_dir.as_deref(),
        Some(
            snapshot
                .launch_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored provenance-certificate run launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored provenance-certificate run package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored provenance-certificate run install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored provenance-certificate run target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored provenance-certificate run backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored provenance-certificate run wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored provenance-certificate run service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored provenance-certificate run session_dir",
        mismatches,
    );
    compare_option_string(
        stored.provenance_certificate_session_path.as_deref(),
        Some(
            nested_provenance_certificate_session_path(session_dir)
                .display()
                .to_string()
                .as_str(),
        ),
        "stored provenance-certificate run provenance_certificate_session_path",
        mismatches,
    );
    compare_option_string(
        stored.provenance_certificate_status_path.as_deref(),
        Some(
            nested_provenance_certificate_status_path(session_dir)
                .display()
                .to_string()
                .as_str(),
        ),
        "stored provenance-certificate run provenance_certificate_status_path",
        mismatches,
    );
    compare_option_string(
        stored.archived_attestation_seal_report_path.as_deref(),
        Some(
            session_dir
                .join("tiny_live_activation_package_provenance_certificate.attestation_seal.report.json")
                .display()
                .to_string()
                .as_str(),
        ),
        "stored provenance-certificate run archived_attestation_seal_report_path",
        mismatches,
    );
    compare_option_string(
        stored.archived_release_capsule_digest_manifest_path.as_deref(),
        Some(
            session_dir
                .join(
                    "tiny_live_activation_package_provenance_certificate.release_capsule.digest_manifest.json",
                )
                .display()
                .to_string()
                .as_str(),
        ),
        "stored provenance-certificate run archived_release_capsule_digest_manifest_path",
        mismatches,
    );
    compare_option_string(
        stored.verify_attestation_seal_command_summary.as_deref(),
        Some(
            provenance_certificate_verify_attestation_seal_command_summary(
                attestation_seal_session_dir,
            )
            .as_str(),
        ),
        "stored provenance-certificate run verify_attestation_seal_command_summary",
        mismatches,
    );
    compare_option_string(
        stored
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        Some(
            snapshot
                .reviewed_frozen_live_cutover_controller_command_summary
                .as_str(),
        ),
        "stored provenance-certificate run reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
}

fn compare_provenance_certificate_copy_against_nested_truth(
    stored: &ProvenanceCertificateReportView,
    nested_session: &ProvenanceCertificateSessionView,
    nested_status: &ProvenanceCertificateStatusView,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.attestation_seal_session_dir,
        &nested_session.attestation_seal_session_dir,
        "stored provenance-certificate copy attestation_seal_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.release_capsule_session_dir.as_deref(),
        Some(nested_session.release_capsule_session_dir.as_str()),
        "stored provenance-certificate copy release_capsule_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.activation_ticket_session_dir.as_deref(),
        Some(nested_session.activation_ticket_session_dir.as_str()),
        "stored provenance-certificate copy activation_ticket_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.review_receipt_session_dir.as_deref(),
        Some(nested_session.review_receipt_session_dir.as_str()),
        "stored provenance-certificate copy review_receipt_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.handoff_bundle_session_dir.as_deref(),
        Some(nested_session.handoff_bundle_session_dir.as_str()),
        "stored provenance-certificate copy handoff_bundle_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.decision_packet_session_dir.as_deref(),
        Some(nested_session.decision_packet_session_dir.as_str()),
        "stored provenance-certificate copy decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.execute_frozen_session_dir.as_deref(),
        Some(nested_session.execute_frozen_session_dir.as_str()),
        "stored provenance-certificate copy execute_frozen_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.turn_green_session_dir.as_deref(),
        Some(nested_session.turn_green_session_dir.as_str()),
        "stored provenance-certificate copy turn_green_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.launch_packet_session_dir.as_deref(),
        Some(nested_session.launch_packet_session_dir.as_str()),
        "stored provenance-certificate copy launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(nested_session.session_dir.as_str()),
        "stored provenance-certificate copy session_dir",
        mismatches,
    );
    compare_option_string(
        stored.result.as_deref(),
        Some(nested_status.result.as_str()),
        "stored provenance-certificate copy result",
        mismatches,
    );
    compare_option_string(
        Some(stored.verdict.as_str()),
        provenance_certificate_run_verdict_for_result(&nested_status.result),
        "stored provenance-certificate copy verdict",
        mismatches,
    );
    compare_option_string(
        stored.handoff_bundle_result.as_deref(),
        nested_status.handoff_bundle_result.as_deref(),
        "stored provenance-certificate copy handoff_bundle_result",
        mismatches,
    );
    compare_option_string(
        stored.decision_packet_result.as_deref(),
        nested_status.decision_packet_result.as_deref(),
        "stored provenance-certificate copy decision_packet_result",
        mismatches,
    );
    compare_option_string(
        stored.execute_frozen_result.as_deref(),
        nested_status.execute_frozen_result.as_deref(),
        "stored provenance-certificate copy execute_frozen_result",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_verdict.as_deref(),
        nested_status.current_pre_activation_gate_verdict.as_deref(),
        "stored provenance-certificate copy current_pre_activation_gate_verdict",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_reason.as_deref(),
        nested_status.current_pre_activation_gate_reason.as_deref(),
        "stored provenance-certificate copy current_pre_activation_gate_reason",
        mismatches,
    );
    compare_option_string(
        stored.verify_attestation_seal_command_summary.as_deref(),
        Some(
            nested_session
                .verify_attestation_seal_command_summary
                .as_str(),
        ),
        "stored provenance-certificate copy verify_attestation_seal_command_summary",
        mismatches,
    );
    compare_option_string(
        stored
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        Some(
            nested_session
                .reviewed_frozen_live_cutover_controller_command_summary
                .as_str(),
        ),
        "stored provenance-certificate copy reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        stored.attestation_seal_summary.as_deref(),
        Some(nested_status.attestation_seal_summary.as_str()),
        "stored provenance-certificate copy attestation_seal_summary",
        mismatches,
    );
    compare_option_string(
        stored.custody_record_summary.as_deref(),
        Some(nested_status.custody_record_summary.as_str()),
        "stored provenance-certificate copy custody_record_summary",
        mismatches,
    );
    compare_option_string(
        stored.release_capsule_digest_manifest_sha256.as_deref(),
        Some(
            nested_status
                .release_capsule_digest_manifest_sha256
                .as_str(),
        ),
        "stored provenance-certificate copy release_capsule_digest_manifest_sha256",
        mismatches,
    );
    compare_option_usize(
        stored.release_capsule_digest_manifest_entry_count,
        Some(nested_status.release_capsule_digest_manifest_entry_count),
        "stored provenance-certificate copy release_capsule_digest_manifest_entry_count",
        mismatches,
    );
    compare_option_string(
        stored.release_capsule_digest_algorithm.as_deref(),
        Some(nested_status.release_capsule_digest_algorithm.as_str()),
        "stored provenance-certificate copy release_capsule_digest_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.provenance_certificate_summary.as_deref(),
        Some(nested_status.provenance_certificate_summary.as_str()),
        "stored provenance-certificate copy provenance_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.chain_fingerprint_summary.as_deref(),
        Some(nested_status.chain_fingerprint_summary.as_str()),
        "stored provenance-certificate copy chain_fingerprint_summary",
        mismatches,
    );
    compare_option_string(
        stored.chain_fingerprint_sha256.as_deref(),
        Some(nested_status.chain_fingerprint_sha256.as_str()),
        "stored provenance-certificate copy chain_fingerprint_sha256",
        mismatches,
    );
    compare_option_string(
        stored.chain_fingerprint_algorithm.as_deref(),
        Some(nested_status.chain_fingerprint_algorithm.as_str()),
        "stored provenance-certificate copy chain_fingerprint_algorithm",
        mismatches,
    );
    compare_string(
        &stored.reason,
        &nested_status.reason,
        "stored provenance-certificate copy reason",
        mismatches,
    );
    compare_string(
        &stored.explicit_statement,
        &nested_status.explicit_statement,
        "stored provenance-certificate copy explicit_statement",
        mismatches,
    );
}

fn compare_session_against_snapshot(
    session: &PackageProvenanceCertificateLatestSession,
    root: &Path,
    session_dir: &Path,
    paths: &PackageProvenanceCertificateLatestPaths,
    snapshot: &LatestProvenanceCertificateSnapshot,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &session.resolve_latest_attestation_seal_command_summary,
        &resolve_latest_attestation_seal_command_summary(
            root,
            &paths.nested_attestation_seal_latest_session_dir,
        ),
        "stored session resolve_latest_attestation_seal_command_summary",
        mismatches,
    );
    compare_string(
        &session.run_latest_attestation_seal_command_summary,
        &run_latest_attestation_seal_command_summary(
            root,
            &paths.nested_attestation_seal_latest_session_dir,
        ),
        "stored session run_latest_attestation_seal_command_summary",
        mismatches,
    );
    compare_string(
        &session.verify_latest_attestation_seal_command_summary,
        &verify_latest_attestation_seal_command_summary(
            &paths.nested_attestation_seal_latest_session_dir,
        ),
        "stored session verify_latest_attestation_seal_command_summary",
        mismatches,
    );
    compare_string(
        &session.downstream_provenance_certificate_command_summary,
        &downstream_provenance_certificate_command_summary(
            snapshot,
            &expected_downstream_attestation_seal_session_dir(paths),
            &paths.nested_provenance_certificate_session_dir,
        ),
        "stored session downstream_provenance_certificate_command_summary",
        mismatches,
    );
    compare_string(
        &session.verify_provenance_certificate_command_summary,
        &verify_provenance_certificate_command_summary(
            snapshot,
            &expected_downstream_attestation_seal_session_dir(paths),
            &paths.nested_provenance_certificate_session_dir,
        ),
        "stored session verify_attestation_seal_command_summary",
        mismatches,
    );
    compare_option_string(
        session.latest_top_step_name.as_deref(),
        Some(snapshot.latest_top_step_name.as_str()),
        "stored session latest_top_step_name",
        mismatches,
    );
    compare_option_string(
        session.latest_top_session_dir.as_deref(),
        Some(
            snapshot
                .latest_top_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored session latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        session.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored session package_dir",
        mismatches,
    );
    compare_option_string(
        session.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored session install_root",
        mismatches,
    );
    compare_option_string(
        session.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored session target_service_name",
        mismatches,
    );
    compare_option_string(
        session.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored session backend_command",
        mismatches,
    );
    compare_option_u64(
        session.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored session wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        session.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored session service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        session
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        Some(
            snapshot
                .reviewed_frozen_live_cutover_controller_command_summary
                .as_str(),
        ),
        "stored session reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        session.clerestory_certificate_summary.as_deref(),
        Some(snapshot.clerestory_certificate_summary.as_str()),
        "stored session clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        session.clerestory_certificate_sha256.as_deref(),
        Some(snapshot.clerestory_certificate_sha256.as_str()),
        "stored session clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        session.clerestory_certificate_algorithm.as_deref(),
        Some(snapshot.clerestory_certificate_algorithm.as_str()),
        "stored session clerestory_certificate_algorithm",
        mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "stored session session_dir",
        mismatches,
    );
    compare_string(
        &session.nested_attestation_seal_latest_session_dir,
        &paths
            .nested_attestation_seal_latest_session_dir
            .display()
            .to_string(),
        "stored session nested_attestation_seal_latest_session_dir",
        mismatches,
    );
    compare_string(
        &session.nested_provenance_certificate_session_dir,
        &paths
            .nested_provenance_certificate_session_dir
            .display()
            .to_string(),
        "stored session nested_provenance_certificate_session_dir",
        mismatches,
    );
    compare_string(
        &session.downstream_decision_packet_session_dir,
        &snapshot
            .downstream_decision_packet_session_dir
            .display()
            .to_string(),
        "stored session downstream_decision_packet_session_dir",
        mismatches,
    );
    compare_string(
        &session.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "stored session explicit_statement",
        mismatches,
    );
}

fn compare_report_against_snapshot(
    report: &PackageProvenanceCertificateLatestReport,
    root: &Path,
    session_dir: &Path,
    paths: &PackageProvenanceCertificateLatestPaths,
    snapshot: &LatestProvenanceCertificateSnapshot,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        report.root.as_deref(),
        Some(root.display().to_string().as_str()),
        "stored report root",
        mismatches,
    );
    compare_option_string(
        report.latest_top_step_name.as_deref(),
        Some(snapshot.latest_top_step_name.as_str()),
        "stored report latest_top_step_name",
        mismatches,
    );
    compare_option_string(
        report.latest_top_session_dir.as_deref(),
        Some(
            snapshot
                .latest_top_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        report.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored report package_dir",
        mismatches,
    );
    compare_option_string(
        report.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored report install_root",
        mismatches,
    );
    compare_option_string(
        report.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored report target_service_name",
        mismatches,
    );
    compare_option_string(
        report.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored report backend_command",
        mismatches,
    );
    compare_option_u64(
        report.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored report wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        report.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored report service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        report
            .resolve_latest_attestation_seal_command_summary
            .as_deref(),
        Some(
            resolve_latest_attestation_seal_command_summary(
                root,
                &paths.nested_attestation_seal_latest_session_dir,
            )
            .as_str(),
        ),
        "stored report resolve_latest_attestation_seal_command_summary",
        mismatches,
    );
    compare_option_string(
        report
            .run_latest_attestation_seal_command_summary
            .as_deref(),
        Some(
            run_latest_attestation_seal_command_summary(
                root,
                &paths.nested_attestation_seal_latest_session_dir,
            )
            .as_str(),
        ),
        "stored report run_latest_attestation_seal_command_summary",
        mismatches,
    );
    compare_option_string(
        report
            .verify_latest_attestation_seal_command_summary
            .as_deref(),
        Some(
            verify_latest_attestation_seal_command_summary(
                &paths.nested_attestation_seal_latest_session_dir,
            )
            .as_str(),
        ),
        "stored report verify_latest_attestation_seal_command_summary",
        mismatches,
    );
    compare_option_string(
        report
            .downstream_provenance_certificate_command_summary
            .as_deref(),
        Some(
            downstream_provenance_certificate_command_summary(
                snapshot,
                &expected_downstream_attestation_seal_session_dir(paths),
                &paths.nested_provenance_certificate_session_dir,
            )
            .as_str(),
        ),
        "stored report downstream_provenance_certificate_command_summary",
        mismatches,
    );
    compare_option_string(
        report
            .verify_provenance_certificate_command_summary
            .as_deref(),
        Some(
            verify_provenance_certificate_command_summary(
                snapshot,
                &expected_downstream_attestation_seal_session_dir(paths),
                &paths.nested_provenance_certificate_session_dir,
            )
            .as_str(),
        ),
        "stored report verify_attestation_seal_command_summary",
        mismatches,
    );
    compare_option_string(
        report
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        Some(
            snapshot
                .reviewed_frozen_live_cutover_controller_command_summary
                .as_str(),
        ),
        "stored report reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        report.clerestory_certificate_summary.as_deref(),
        Some(snapshot.clerestory_certificate_summary.as_str()),
        "stored report clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        report.clerestory_certificate_sha256.as_deref(),
        Some(snapshot.clerestory_certificate_sha256.as_str()),
        "stored report clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        report.clerestory_certificate_algorithm.as_deref(),
        Some(snapshot.clerestory_certificate_algorithm.as_str()),
        "stored report clerestory_certificate_algorithm",
        mismatches,
    );
    compare_option_string(
        report.provenance_certificate_latest_session_path.as_deref(),
        Some(paths.session_path.display().to_string().as_str()),
        "stored report provenance_certificate_latest_session_path",
        mismatches,
    );
    compare_option_string(
        report.provenance_certificate_latest_status_path.as_deref(),
        Some(paths.status_path.display().to_string().as_str()),
        "stored report provenance_certificate_latest_status_path",
        mismatches,
    );
    compare_option_string(
        report.provenance_certificate_latest_report_path.as_deref(),
        Some(paths.report_path.display().to_string().as_str()),
        "stored report provenance_certificate_latest_report_path",
        mismatches,
    );
    compare_option_string(
        report.latest_attestation_seal_plan_report_path.as_deref(),
        Some(
            paths
                .latest_attestation_seal_plan_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report latest_attestation_seal_plan_report_path",
        mismatches,
    );
    compare_option_string(
        report.latest_attestation_seal_report_path.as_deref(),
        Some(
            paths
                .latest_attestation_seal_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report latest_attestation_seal_report_path",
        mismatches,
    );
    compare_option_string(
        report.provenance_certificate_report_path.as_deref(),
        Some(
            paths
                .provenance_certificate_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report provenance_certificate_report_path",
        mismatches,
    );
    compare_option_string(
        report.nested_attestation_seal_latest_session_dir.as_deref(),
        Some(
            paths
                .nested_attestation_seal_latest_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report nested_attestation_seal_latest_session_dir",
        mismatches,
    );
    compare_option_string(
        report.nested_provenance_certificate_session_dir.as_deref(),
        Some(
            paths
                .nested_provenance_certificate_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report nested_provenance_certificate_session_dir",
        mismatches,
    );
    compare_option_string(
        report.downstream_decision_packet_session_dir.as_deref(),
        Some(
            snapshot
                .downstream_decision_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report downstream_decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        report.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored report session_dir",
        mismatches,
    );
    compare_string(
        &report.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "stored report explicit_statement",
        mismatches,
    );
}

fn compare_session_against_failure(
    session: &PackageProvenanceCertificateLatestSession,
    root: &Path,
    session_dir: &Path,
    paths: &PackageProvenanceCertificateLatestPaths,
    failure: &LatestProvenanceCertificateResolutionFailure,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &session.root,
        &root.display().to_string(),
        "stored session root on failure",
        mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "stored session session_dir on failure",
        mismatches,
    );
    compare_string(
        &session.nested_attestation_seal_latest_session_dir,
        &paths
            .nested_attestation_seal_latest_session_dir
            .display()
            .to_string(),
        "stored session nested_attestation_seal_latest_session_dir on failure",
        mismatches,
    );
    compare_string(
        &session.nested_provenance_certificate_session_dir,
        &paths
            .nested_provenance_certificate_session_dir
            .display()
            .to_string(),
        "stored session nested_provenance_certificate_session_dir on failure",
        mismatches,
    );
    if let Some(snapshot) = failure.snapshot.as_ref() {
        compare_session_against_snapshot(session, root, session_dir, paths, snapshot, mismatches);
    }
}

fn compare_report_against_failure(
    report: &PackageProvenanceCertificateLatestReport,
    root: &Path,
    session_dir: &Path,
    paths: &PackageProvenanceCertificateLatestPaths,
    failure: &LatestProvenanceCertificateResolutionFailure,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        report.root.as_deref(),
        Some(root.display().to_string().as_str()),
        "stored report root on failure",
        mismatches,
    );
    compare_option_string(
        report.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored report session_dir on failure",
        mismatches,
    );
    compare_option_string(
        report.result.as_deref(),
        Some(serialize_enum(&result_from_failure_kind(failure.kind)).as_str()),
        "stored report result on failure",
        mismatches,
    );
    compare_string(
        &report.mode,
        "run_latest_provenance_certificate",
        "stored report mode on failure",
        mismatches,
    );
    compare_string(
        &serialize_enum(&report.verdict),
        &serialize_enum(&verdict_from_failure_kind(failure.kind)),
        "stored report verdict on failure",
        mismatches,
    );
    compare_string(
        &report.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "stored report explicit_statement on failure",
        mismatches,
    );
    if let Some(snapshot) = failure.snapshot.as_ref() {
        compare_report_against_snapshot(report, root, session_dir, paths, snapshot, mismatches);
    }
}

fn required_option_string(value: Option<&String>, label: &str) -> Result<String> {
    value
        .filter(|entry| !entry.trim().is_empty())
        .cloned()
        .ok_or_else(|| anyhow!("missing required {label}"))
}

fn map_attestation_seal_latest_failure_kind(verdict: &str) -> ResolutionFailureKind {
    match verdict {
        "tiny_live_package_attestation_seal_latest_refused_by_missing_latest_chain" => {
            ResolutionFailureKind::MissingLatestChain
        }
        "tiny_live_package_attestation_seal_latest_refused_by_drifted_attestation_seal_contract" => {
            ResolutionFailureKind::DriftedProvenanceCertificateContract
        }
        _ => ResolutionFailureKind::InvalidLatestChain,
    }
}

fn latest_attestation_seal_terminal_failure(
    report: &AttestationSealLatestReportView,
) -> Option<LivePackageProvenanceCertificateLatestResult> {
    match report.result.as_deref() {
        Some("refused_by_missing_latest_chain") => {
            Some(LivePackageProvenanceCertificateLatestResult::RefusedByMissingLatestChain)
        }
        Some("refused_by_invalid_latest_chain") => {
            Some(LivePackageProvenanceCertificateLatestResult::RefusedByInvalidLatestChain)
        }
        Some("refused_by_drifted_attestation_seal_contract") => {
            Some(
                LivePackageProvenanceCertificateLatestResult::RefusedByDriftedProvenanceCertificateContract,
            )
        }
        _ => None,
    }
}

fn provenance_certificate_latest_result_from_provenance_certificate(
    result: &str,
) -> Option<LivePackageProvenanceCertificateLatestResult> {
    match result {
        "ready_for_manual_execution_when_gate_turns_green" => {
            Some(LivePackageProvenanceCertificateLatestResult::ReadyForManualExecutionWhenGateTurnsGreen)
        }
        "refused_now_by_stage3" => Some(LivePackageProvenanceCertificateLatestResult::RefusedNowByStage3),
        "refused_now_by_pre_activation_gate" => {
            Some(LivePackageProvenanceCertificateLatestResult::RefusedNowByPreActivationGate)
        }
        "refused_now_by_invalid_or_drifted_contract" => {
            Some(LivePackageProvenanceCertificateLatestResult::RefusedByDriftedProvenanceCertificateContract)
        }
        _ => None,
    }
}

fn provenance_certificate_run_verdict_for_result(result: &str) -> Option<&'static str> {
    match result {
        "ready_for_manual_execution_when_gate_turns_green" => Some(
            "tiny_live_package_provenance_certificate_ready_for_manual_execution_when_gate_turns_green",
        ),
        "refused_now_by_stage3" => {
            Some("tiny_live_package_provenance_certificate_refused_now_by_stage3")
        }
        "refused_now_by_pre_activation_gate" => {
            Some("tiny_live_package_provenance_certificate_refused_now_by_pre_activation_gate")
        }
        "refused_now_by_invalid_or_drifted_contract" => {
            Some("tiny_live_package_provenance_certificate_refused_now_by_invalid_or_drifted_contract")
        }
        _ => None,
    }
}

fn result_from_failure_kind(
    kind: ResolutionFailureKind,
) -> LivePackageProvenanceCertificateLatestResult {
    match kind {
        ResolutionFailureKind::MissingLatestChain => {
            LivePackageProvenanceCertificateLatestResult::RefusedByMissingLatestChain
        }
        ResolutionFailureKind::InvalidLatestChain => {
            LivePackageProvenanceCertificateLatestResult::RefusedByInvalidLatestChain
        }
        ResolutionFailureKind::DriftedProvenanceCertificateContract => {
            LivePackageProvenanceCertificateLatestResult::RefusedByDriftedProvenanceCertificateContract
        }
    }
}

fn verdict_for_result(
    result: LivePackageProvenanceCertificateLatestResult,
) -> TinyLivePackageProvenanceCertificateLatestVerdict {
    match result {
        LivePackageProvenanceCertificateLatestResult::RefusedByMissingLatestChain => {
            TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestRefusedByMissingLatestChain
        }
        LivePackageProvenanceCertificateLatestResult::RefusedByInvalidLatestChain => {
            TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestRefusedByInvalidLatestChain
        }
        LivePackageProvenanceCertificateLatestResult::RefusedByDriftedProvenanceCertificateContract => {
            TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestRefusedByDriftedProvenanceCertificateContract
        }
        LivePackageProvenanceCertificateLatestResult::RefusedNowByStage3 => {
            TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestRefusedNowByStage3
        }
        LivePackageProvenanceCertificateLatestResult::RefusedNowByPreActivationGate => {
            TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestRefusedNowByPreActivationGate
        }
        LivePackageProvenanceCertificateLatestResult::ReadyForManualExecutionWhenGateTurnsGreen => {
            TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestReadyForManualExecutionWhenGateTurnsGreen
        }
    }
}

fn verdict_from_failure_kind(
    kind: ResolutionFailureKind,
) -> TinyLivePackageProvenanceCertificateLatestVerdict {
    verdict_for_result(result_from_failure_kind(kind))
}

fn operator_next_action_summary(result: LivePackageProvenanceCertificateLatestResult) -> String {
    match result {
        LivePackageProvenanceCertificateLatestResult::RefusedByMissingLatestChain => {
            "no immutable clerestory chain is available yet; do not manually hunt attestation-seal or provenance-certificate lineage by hand".to_string()
        }
        LivePackageProvenanceCertificateLatestResult::RefusedByInvalidLatestChain => {
            "the latest immutable chain is incomplete or below the accepted clerestory layer; mint a fresh coherent clerestory chain instead of bypassing the accepted latest-attestation-seal / provenance-certificate path".to_string()
        }
        LivePackageProvenanceCertificateLatestResult::RefusedByDriftedProvenanceCertificateContract => {
            "the latest chain no longer resolves cleanly into the accepted latest-attestation-seal / provenance-certificate contract; repair that drift instead of bypassing the bounded handoff path".to_string()
        }
        LivePackageProvenanceCertificateLatestResult::RefusedNowByStage3 => {
            "the frozen controller stays refused now by Stage 3; rerun this latest-provenance-certificate refresh after current Stage 3 truth turns green".to_string()
        }
        LivePackageProvenanceCertificateLatestResult::RefusedNowByPreActivationGate => {
            "the frozen controller stays refused now by the pre-activation gate; rerun this latest-provenance-certificate refresh after the current pre-activation gate turns green".to_string()
        }
        LivePackageProvenanceCertificateLatestResult::ReadyForManualExecutionWhenGateTurnsGreen => {
            "the latest immutable chain now resolves into the accepted provenance-certificate contract and one bounded latest provenance-certificate handoff is ready for manual execution when gate truth turns green; this wrapper still did not run or authorize the frozen controller".to_string()
        }
    }
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(Value::String(raw)) => raw,
        Ok(other) => other.to_string(),
        Err(_) => "<serialization-error>".to_string(),
    }
}

fn provenance_certificate_verify_attestation_seal_command_summary(
    attestation_seal_session_dir: &Path,
) -> String {
    format!(
        "verify immutable attestation-seal session under {} before freezing the final provenance certificate",
        attestation_seal_session_dir.display()
    )
}

fn refusal_status(
    root: &Path,
    session_dir: &Path,
    paths: &PackageProvenanceCertificateLatestPaths,
    resolved: Option<&ResolvedLatestProvenanceCertificate>,
    result: LivePackageProvenanceCertificateLatestResult,
    reason: String,
    latest_attestation_seal_plan_step: Option<PackageProvenanceCertificateLatestStepArtifact>,
    latest_attestation_seal_step: Option<PackageProvenanceCertificateLatestStepArtifact>,
    provenance_certificate_step: Option<PackageProvenanceCertificateLatestStepArtifact>,
) -> PackageProvenanceCertificateLatestStatus {
    let snapshot = resolved.map(|value| &value.snapshot);
    PackageProvenanceCertificateLatestStatus {
        status_version: PROVENANCE_CERTIFICATE_LATEST_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        root: root.display().to_string(),
        session_dir: session_dir.display().to_string(),
        latest_top_step_name: snapshot.map(|value| value.latest_top_step_name.clone()),
        latest_top_session_dir: snapshot
            .map(|value| value.latest_top_session_dir.display().to_string()),
        historical_execute_frozen_session_dir: snapshot.map(|value| {
            value
                .historical_execute_frozen_session_dir
                .display()
                .to_string()
        }),
        turn_green_session_dir: snapshot
            .map(|value| value.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: snapshot
            .map(|value| value.launch_packet_session_dir.display().to_string()),
        nested_attestation_seal_latest_session_dir: paths
            .nested_attestation_seal_latest_session_dir
            .display()
            .to_string(),
        nested_provenance_certificate_session_dir: paths
            .nested_provenance_certificate_session_dir
            .display()
            .to_string(),
        downstream_decision_packet_session_dir: snapshot
            .map(|value| {
                value
                    .downstream_decision_packet_session_dir
                    .display()
                    .to_string()
            })
            .unwrap_or_else(|| {
                expected_downstream_decision_packet_session_dir(paths)
                    .display()
                    .to_string()
            }),
        result,
        reason,
        latest_attestation_seal_plan_verdict: resolved
            .map(|value| value.latest_attestation_seal_plan.verdict.clone()),
        latest_attestation_seal_verdict: latest_attestation_seal_step
            .as_ref()
            .map(|value| value.verdict.clone()),
        provenance_certificate_verdict: provenance_certificate_step
            .as_ref()
            .map(|value| value.verdict.clone()),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        latest_attestation_seal_plan_step,
        latest_attestation_seal_step,
        provenance_certificate_step,
        operator_next_action_summary: operator_next_action_summary(result),
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn refusal_status_from_resolution_failure(
    root: &Path,
    session_dir: &Path,
    paths: &PackageProvenanceCertificateLatestPaths,
    failure: &LatestProvenanceCertificateResolutionFailure,
) -> PackageProvenanceCertificateLatestStatus {
    PackageProvenanceCertificateLatestStatus {
        status_version: PROVENANCE_CERTIFICATE_LATEST_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        root: root.display().to_string(),
        session_dir: session_dir.display().to_string(),
        latest_top_step_name: failure
            .snapshot
            .as_ref()
            .map(|value| value.latest_top_step_name.clone()),
        latest_top_session_dir: failure
            .snapshot
            .as_ref()
            .map(|value| value.latest_top_session_dir.display().to_string()),
        historical_execute_frozen_session_dir: failure.snapshot.as_ref().map(|value| {
            value
                .historical_execute_frozen_session_dir
                .display()
                .to_string()
        }),
        turn_green_session_dir: failure
            .snapshot
            .as_ref()
            .map(|value| value.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: failure
            .snapshot
            .as_ref()
            .map(|value| value.launch_packet_session_dir.display().to_string()),
        nested_attestation_seal_latest_session_dir: paths
            .nested_attestation_seal_latest_session_dir
            .display()
            .to_string(),
        nested_provenance_certificate_session_dir: paths
            .nested_provenance_certificate_session_dir
            .display()
            .to_string(),
        downstream_decision_packet_session_dir: failure
            .snapshot
            .as_ref()
            .map(|value| {
                value
                    .downstream_decision_packet_session_dir
                    .display()
                    .to_string()
            })
            .unwrap_or_else(|| {
                expected_downstream_decision_packet_session_dir(paths)
                    .display()
                    .to_string()
            }),
        result: result_from_failure_kind(failure.kind),
        reason: failure.reason.clone(),
        latest_attestation_seal_plan_verdict: failure.latest_attestation_seal_plan_verdict.clone(),
        latest_attestation_seal_verdict: None,
        provenance_certificate_verdict: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        latest_attestation_seal_plan_step: failure.latest_attestation_seal_plan_raw.as_ref().map(
            |_| {
                step_artifact(
                    &paths.latest_attestation_seal_plan_report_path,
                    "plan_latest_attestation_seal",
                    failure
                        .latest_attestation_seal_plan_verdict
                        .as_deref()
                        .unwrap_or("<missing>"),
                    failure
                        .latest_attestation_seal_plan_reason
                        .as_deref()
                        .unwrap_or("<missing>"),
                    failure
                        .latest_attestation_seal_plan_generated_at
                        .unwrap_or_else(Utc::now),
                )
            },
        ),
        latest_attestation_seal_step: None,
        provenance_certificate_step: None,
        operator_next_action_summary: operator_next_action_summary(result_from_failure_kind(
            failure.kind,
        )),
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn verify_invalid_provenance_certificate_latest_report_without_session(
    session_dir: &Path,
    verification_mismatches: Vec<String>,
) -> PackageProvenanceCertificateLatestReport {
    let reason = verification_mismatches
        .first()
        .cloned()
        .unwrap_or_else(|| "latest-provenance-certificate verification failed".to_string());
    PackageProvenanceCertificateLatestReport {
        generated_at: Utc::now(),
        mode: "verify_latest_provenance_certificate".to_string(),
        verdict: TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestVerifyInvalid,
        reason,
        root: None,
        latest_top_step_name: None,
        latest_top_session_dir: None,
        historical_execute_frozen_session_dir: None,
        turn_green_session_dir: None,
        launch_packet_session_dir: None,
        package_dir: None,
        install_root: None,
        target_service_name: None,
        backend_command: None,
        wrapper_timeout_ms: None,
        service_status_max_staleness_ms: None,
        session_dir: Some(session_dir.display().to_string()),
        provenance_certificate_latest_session_path: Some(
            provenance_certificate_latest_paths(session_dir)
                .session_path
                .display()
                .to_string(),
        ),
        provenance_certificate_latest_status_path: Some(
            provenance_certificate_latest_paths(session_dir)
                .status_path
                .display()
                .to_string(),
        ),
        provenance_certificate_latest_report_path: Some(
            provenance_certificate_latest_paths(session_dir)
                .report_path
                .display()
                .to_string(),
        ),
        latest_attestation_seal_plan_report_path: None,
        latest_attestation_seal_report_path: None,
        provenance_certificate_report_path: None,
        nested_attestation_seal_latest_session_dir: None,
        nested_provenance_certificate_session_dir: None,
        downstream_decision_packet_session_dir: None,
        latest_attestation_seal_plan_verdict: None,
        latest_attestation_seal_verdict: None,
        provenance_certificate_verdict: None,
        result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        resolve_latest_attestation_seal_command_summary: None,
        run_latest_attestation_seal_command_summary: None,
        verify_latest_attestation_seal_command_summary: None,
        downstream_provenance_certificate_command_summary: None,
        verify_provenance_certificate_command_summary: None,
        reviewed_frozen_live_cutover_controller_command_summary: None,
        clerestory_certificate_summary: None,
        clerestory_certificate_sha256: None,
        clerestory_certificate_algorithm: None,
        verification_mismatches,
        activation_authorized: false,
        explicit_statement:
            "this verification proves only whether the latest immutable chain still resolves cleanly into the accepted provenance-certificate contract; it never runs the frozen controller"
                .to_string(),
    }
}

fn fallback_attestation_seal_latest_copy() -> AttestationSealLatestReportView {
    AttestationSealLatestReportView {
        generated_at: Utc::now(),
        mode: "<missing>".to_string(),
        verdict: "<missing>".to_string(),
        reason: "<missing>".to_string(),
        latest_top_step_name: None,
        latest_top_session_dir: None,
        historical_execute_frozen_session_dir: None,
        turn_green_session_dir: None,
        launch_packet_session_dir: None,
        package_dir: None,
        install_root: None,
        target_service_name: None,
        backend_command: None,
        wrapper_timeout_ms: None,
        service_status_max_staleness_ms: None,
        session_dir: None,
        downstream_decision_packet_session_dir: None,
        latest_execute_verdict: None,
        decision_packet_plan_verdict: None,
        result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        reviewed_frozen_live_cutover_controller_command_summary: None,
        clerestory_certificate_summary: None,
        clerestory_certificate_sha256: None,
        clerestory_certificate_algorithm: None,
        activation_authorized: false,
        explicit_statement: None,
    }
}

fn fallback_provenance_certificate_session(
    snapshot: &LatestProvenanceCertificateSnapshot,
    session_dir: &Path,
) -> ProvenanceCertificateSessionView {
    ProvenanceCertificateSessionView {
        attestation_seal_session_dir: "<missing>".to_string(),
        release_capsule_session_dir: "<missing>".to_string(),
        activation_ticket_session_dir: "<missing>".to_string(),
        review_receipt_session_dir: "<missing>".to_string(),
        handoff_bundle_session_dir: "<missing>".to_string(),
        decision_packet_session_dir: snapshot
            .downstream_decision_packet_session_dir
            .display()
            .to_string(),
        execute_frozen_session_dir: snapshot
            .historical_execute_frozen_session_dir
            .display()
            .to_string(),
        turn_green_session_dir: snapshot.turn_green_session_dir.display().to_string(),
        launch_packet_session_dir: snapshot.launch_packet_session_dir.display().to_string(),
        package_dir: snapshot.package_dir.display().to_string(),
        install_root: snapshot.install_root.display().to_string(),
        target_service_name: snapshot.target_service_name.clone(),
        backend_command: snapshot.backend_command.clone(),
        wrapper_timeout_ms: snapshot.wrapper_timeout_ms,
        service_status_max_staleness_ms: snapshot.service_status_max_staleness_ms,
        session_dir: session_dir.display().to_string(),
        verify_attestation_seal_command_summary:
            provenance_certificate_verify_attestation_seal_command_summary(Path::new("<missing>")),
        reviewed_frozen_live_cutover_controller_command_summary: snapshot
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        attestation_seal_summary:
            "Attestation seal outcome: fallback session is synthetic and must not verify green."
                .to_string(),
        custody_record_summary:
            "Custody record identity: fallback session is synthetic and must not verify green."
                .to_string(),
        release_capsule_digest_manifest_sha256: "<missing>".to_string(),
        release_capsule_digest_manifest_entry_count: 0,
        release_capsule_digest_algorithm: "sha256".to_string(),
        provenance_certificate_summary:
            "Provenance certificate outcome: fallback session is synthetic and must not verify green."
                .to_string(),
        chain_fingerprint_summary:
            "Chain fingerprint identity: fallback session is synthetic and must not verify green."
                .to_string(),
        chain_fingerprint_sha256: "<missing>".to_string(),
        chain_fingerprint_algorithm: "sha256".to_string(),
        explicit_statement:
            "the verified attestation-seal session is the primary input here; this immutable provenance certificate freezes one final canonical chain fingerprint over the exact reviewed frozen live cutover contract without enabling production execution"
                .to_string(),
    }
}

fn fallback_provenance_certificate_status(
    _snapshot: &LatestProvenanceCertificateSnapshot,
    session_dir: &Path,
) -> ProvenanceCertificateStatusView {
    ProvenanceCertificateStatusView {
        session_dir: session_dir.display().to_string(),
        attestation_seal_session_dir: "<missing>".to_string(),
        result: "refused_now_by_invalid_or_drifted_contract".to_string(),
        reason: "<missing>".to_string(),
        handoff_bundle_result: None,
        decision_packet_result: None,
        execute_frozen_result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        attestation_seal_step: None,
        attestation_seal_summary:
            "Attestation seal outcome: fallback status is synthetic and must not verify green."
                .to_string(),
        custody_record_summary:
            "Custody record identity: fallback status is synthetic and must not verify green."
                .to_string(),
        release_capsule_digest_manifest_sha256: "<missing>".to_string(),
        release_capsule_digest_manifest_entry_count: 0,
        release_capsule_digest_algorithm: "sha256".to_string(),
        provenance_certificate_summary:
            "Provenance certificate outcome: fallback status is synthetic and must not verify green."
                .to_string(),
        chain_fingerprint_summary:
            "Chain fingerprint identity: fallback status is synthetic and must not verify green."
                .to_string(),
        chain_fingerprint_sha256: "<missing>".to_string(),
        chain_fingerprint_algorithm: "sha256".to_string(),
        explicit_statement:
            "this provenance certificate is archival and read-only; it never executes or enables the frozen controller"
                .to_string(),
    }
}

fn render_report_lines(report: &PackageProvenanceCertificateLatestReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
    ];
    if let Some(value) = &report.root {
        lines.push(format!("root={value}"));
    }
    if let Some(value) = &report.latest_top_step_name {
        lines.push(format!("latest_top_step_name={value}"));
    }
    if let Some(value) = &report.latest_top_session_dir {
        lines.push(format!("latest_top_session_dir={value}"));
    }
    if let Some(value) = &report.session_dir {
        lines.push(format!("session_dir={value}"));
    }
    if let Some(value) = &report.nested_attestation_seal_latest_session_dir {
        lines.push(format!(
            "nested_attestation_seal_latest_session_dir={value}"
        ));
    }
    if let Some(value) = &report.downstream_decision_packet_session_dir {
        lines.push(format!("downstream_decision_packet_session_dir={value}"));
    }
    if let Some(value) = &report.nested_provenance_certificate_session_dir {
        lines.push(format!("nested_provenance_certificate_session_dir={value}"));
    }
    if let Some(value) = &report.result {
        lines.push(format!("result={value}"));
    }
    if let Some(value) = &report.downstream_provenance_certificate_command_summary {
        lines.push(format!(
            "downstream_provenance_certificate_command_summary={value}"
        ));
    }
    if let Some(value) = &report.reviewed_frozen_live_cutover_controller_command_summary {
        lines.push(format!(
            "reviewed_frozen_live_cutover_controller_command_summary={value}"
        ));
    }
    lines.push(format!(
        "activation_authorized={}",
        report.activation_authorized
    ));
    lines.push(format!("explicit_statement={}", report.explicit_statement));
    if !report.verification_mismatches.is_empty() {
        lines.push("verification_mismatches:".to_string());
        lines.extend(
            report
                .verification_mismatches
                .iter()
                .map(|entry| format!("  - {entry}")),
        );
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::ffi::OsString;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    struct FakeAttestationSealLatestFixture {
        temp_dir: PathBuf,
        root: PathBuf,
        wrapper_session_dir: PathBuf,
        output_script_path: PathBuf,
        bin_dir: PathBuf,
        latest_top_session_dir: PathBuf,
        historical_execute_frozen_session_dir: PathBuf,
        turn_green_session_dir: PathBuf,
        launch_packet_session_dir: PathBuf,
        package_dir: PathBuf,
        install_root: PathBuf,
        backend_command: PathBuf,
        attestation_seal_latest_plan_json_path: PathBuf,
        attestation_seal_latest_run_json_path: PathBuf,
        attestation_seal_latest_verify_json_path: PathBuf,
        handoff_bundle_run_json_path: PathBuf,
        handoff_bundle_verify_json_path: PathBuf,
        handoff_bundle_session_json_path: PathBuf,
        handoff_bundle_status_json_path: PathBuf,
        frozen_controller_command: String,
    }

    impl FakeAttestationSealLatestFixture {
        fn plan_config(&self) -> Config {
            Config {
                mode: Mode::PlanLatestProvenanceCertificate,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn render_config(&self) -> Config {
            Config {
                mode: Mode::RenderLatestProvenanceCertificateScript,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: Some(self.output_script_path.clone()),
                json: true,
            }
        }

        fn run_config(&self) -> Config {
            Config {
                mode: Mode::RunLatestProvenanceCertificate,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn verify_config(&self) -> Config {
            Config {
                mode: Mode::VerifyLatestProvenanceCertificate,
                root: None,
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }
    }

    #[test]
    fn plan_mode_resolves_latest_valid_clerestory_chain_to_exact_expected_nested_latest_attestation_seal_lineage(
    ) {
        let fixture = fake_attestation_seal_latest_fixture(
            "attestation_seal_latest_plan_ok",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = plan_latest_provenance_certificate_report(&fixture.plan_config()).unwrap();
        let paths = provenance_certificate_latest_paths(&fixture.wrapper_session_dir);
        assert_eq!(
            report.verdict,
            TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestPlanReady
        );
        assert_eq!(
            report.latest_top_step_name.as_deref(),
            Some("clerestory_certificate")
        );
        assert_eq!(
            report.nested_attestation_seal_latest_session_dir.as_deref(),
            Some(
                paths
                    .nested_attestation_seal_latest_session_dir
                    .display()
                    .to_string()
                    .as_str()
            )
        );
        assert_eq!(
            report.downstream_decision_packet_session_dir.as_deref(),
            Some(
                expected_downstream_decision_packet_session_dir(&paths)
                    .display()
                    .to_string()
                    .as_str()
            )
        );
        assert!(report
            .downstream_provenance_certificate_command_summary
            .as_deref()
            .unwrap()
            .contains(PROVENANCE_CERTIFICATE_BIN));
    }

    #[test]
    fn render_mode_emits_exact_downstream_attestation_seal_contract_not_second_controller_path() {
        let fixture = fake_attestation_seal_latest_fixture(
            "attestation_seal_latest_render_ok",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report =
            render_latest_provenance_certificate_script_report(&fixture.render_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestScriptRendered
        );
        let script = fs::read_to_string(&fixture.output_script_path).unwrap();
        assert!(script.contains(ATTESTATION_SEAL_LATEST_BIN));
        assert!(script.contains(PROVENANCE_CERTIFICATE_BIN));
        assert!(script.contains("--attestation-seal-session-dir"));
        assert!(!script.contains("--activation-ticket-session-dir"));
        assert!(script.contains("--run-live-package-provenance-certificate"));
        assert!(script.contains("--verify-live-package-provenance-certificate"));
        assert!(!script.contains("copybot_tiny_live_activation_package_live_cutover"));
    }

    #[test]
    fn run_mode_refuses_when_latest_chain_missing_invalid_or_below_clerestory() {
        for (label, verdict, reason, expected) in [
            (
                "attestation_seal_latest_missing",
                "tiny_live_package_attestation_seal_latest_refused_by_missing_latest_chain",
                "no persisted immutable chain exists",
                TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestRefusedByMissingLatestChain,
            ),
            (
                "attestation_seal_latest_invalid",
                "tiny_live_package_attestation_seal_latest_refused_by_invalid_latest_chain",
                "latest immutable chain is incomplete",
                TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestRefusedByInvalidLatestChain,
            ),
            (
                "attestation_seal_latest_below",
                "tiny_live_package_attestation_seal_latest_refused_by_invalid_latest_chain",
                "latest persisted top chain is choir_receipt, which is below clerestory_certificate",
                TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestRefusedByInvalidLatestChain,
            ),
        ] {
            let fixture = fake_attestation_seal_latest_fixture(
                label,
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_signoff",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let mut plan: Value = load_json(&fixture.attestation_seal_latest_plan_json_path).unwrap();
            plan["verdict"] = json!(verdict);
            plan["reason"] = json!(reason);
            if label.ends_with("below") {
                plan["latest_top_step_name"] = json!("choir_receipt");
            }
            persist_json(&fixture.attestation_seal_latest_plan_json_path, &plan).unwrap();

            let report = run_latest_provenance_certificate_report(&fixture.run_config()).unwrap();
            assert_eq!(report.verdict, expected);
        }
    }

    #[test]
    fn plan_mode_maps_real_latest_attestation_seal_drift_verdict_to_local_drift_refusal() {
        let fixture = fake_attestation_seal_latest_fixture(
            "attestation_seal_latest_plan_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let mut plan: Value = load_json(&fixture.attestation_seal_latest_plan_json_path).unwrap();
        plan["verdict"] =
            json!("tiny_live_package_attestation_seal_latest_refused_by_drifted_attestation_seal_contract");
        plan["reason"] = json!("real accepted latest-attestation-seal drift");
        persist_json(&fixture.attestation_seal_latest_plan_json_path, &plan).unwrap();

        let report = run_latest_provenance_certificate_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestRefusedByDriftedProvenanceCertificateContract
        );
        assert_eq!(
            report.result.as_deref(),
            Some("refused_by_drifted_provenance_certificate_contract")
        );
    }

    #[test]
    fn run_mode_takes_terminal_drift_refusal_when_nested_latest_attestation_seal_reports_real_drift(
    ) {
        let fixture = fake_attestation_seal_latest_fixture(
            "attestation_seal_latest_run_drift",
            "refused_by_drifted_attestation_seal_contract",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = run_latest_provenance_certificate_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestRefusedByDriftedProvenanceCertificateContract
        );
        assert_eq!(
            report.result.as_deref(),
            Some("refused_by_drifted_provenance_certificate_contract")
        );
        assert_eq!(report.provenance_certificate_verdict.as_deref(), None);
    }

    #[test]
    fn run_mode_refuses_when_nested_release_capsule_lineage_drifts_from_latest_immutable_chain() {
        let fixture = fake_attestation_seal_latest_fixture(
            "attestation_seal_latest_handoff_bundle_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let mut run: Value = load_json(&fixture.attestation_seal_latest_run_json_path).unwrap();
        run["package_dir"] = json!("/tampered/package");
        persist_json(&fixture.attestation_seal_latest_run_json_path, &run).unwrap();

        let report = run_latest_provenance_certificate_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestRefusedByDriftedProvenanceCertificateContract
        );
        assert!(report
            .reason
            .contains("stored latest-attestation-seal run package_dir"));
    }

    #[test]
    fn verify_mode_fails_when_persisted_latest_attestation_seal_artifacts_are_tampered_or_missing_nested_lineage_truth(
    ) {
        {
            let fixture = fake_attestation_seal_latest_fixture(
                "attestation_seal_latest_verify_tampered_copy",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_signoff",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_provenance_certificate_report(&fixture.run_config()).unwrap();

            let paths = provenance_certificate_latest_paths(&fixture.wrapper_session_dir);
            let mut report: Value = load_json(&paths.provenance_certificate_report_path).unwrap();
            report["reason"] = json!("tampered stored provenance-certificate copy reason");
            persist_json(&paths.provenance_certificate_report_path, &report).unwrap();

            let verify =
                verify_latest_provenance_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestVerifyInvalid
            );
            assert!(verify
                .verification_mismatches
                .iter()
                .any(|entry| entry.contains("stored provenance-certificate copy reason")));
        }

        {
            let fixture = fake_attestation_seal_latest_fixture(
                "attestation_seal_latest_verify_missing_nested_truth",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_signoff",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_provenance_certificate_report(&fixture.run_config()).unwrap();

            let paths = provenance_certificate_latest_paths(&fixture.wrapper_session_dir);
            fs::remove_file(nested_attestation_seal_latest_report_path(
                &paths.nested_attestation_seal_latest_session_dir,
            ))
            .unwrap();

            let verify =
                verify_latest_provenance_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestVerifyInvalid
            );
            assert!(verify.verification_mismatches.iter().any(|entry| {
                entry.contains("tiny_live_activation_package_attestation_seal_latest.report.json")
            }));
        }
    }

    #[test]
    fn verify_fails_when_copied_nested_latest_attestation_seal_explicit_statement_drifts() {
        let fixture = fake_attestation_seal_latest_fixture(
            "attestation_seal_latest_verify_launch_packet_explicit_statement_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_provenance_certificate_report(&fixture.run_config()).unwrap();

        let paths = provenance_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.latest_attestation_seal_report_path).unwrap();
        report["explicit_statement"] =
            json!("tampered nested latest release capsule explicit statement");
        persist_json(&paths.latest_attestation_seal_report_path, &report).unwrap();

        let verify = verify_latest_provenance_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored latest-attestation-seal copy explicit_statement")
        }));
    }

    #[test]
    fn verify_fails_when_copied_nested_attestation_seal_decision_packet_result_drifts() {
        let fixture = fake_attestation_seal_latest_fixture(
            "attestation_seal_latest_verify_decision_packet_result_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_provenance_certificate_report(&fixture.run_config()).unwrap();

        let paths = provenance_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.provenance_certificate_report_path).unwrap();
        report["decision_packet_result"] = json!("tampered_decision_packet_result");
        persist_json(&paths.provenance_certificate_report_path, &report).unwrap();

        let verify = verify_latest_provenance_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored provenance-certificate copy decision_packet_result")
        }));
    }

    #[test]
    fn verify_fails_when_downstream_decision_packet_confirmation_anchor_drifts() {
        let fixture = fake_attestation_seal_latest_fixture(
            "attestation_seal_latest_verify_decision_packet_anchor_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_provenance_certificate_report(&fixture.run_config()).unwrap();

        let paths = provenance_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.provenance_certificate_report_path).unwrap();
        report["decision_packet_session_dir"] = json!("/tampered/decision-packet-session");
        persist_json(&paths.provenance_certificate_report_path, &report).unwrap();

        let verify = verify_latest_provenance_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored provenance-certificate copy decision_packet_session_dir")
        }));
    }

    #[test]
    fn verify_fails_when_copied_nested_attestation_seal_execute_frozen_result_drifts() {
        let fixture = fake_attestation_seal_latest_fixture(
            "attestation_seal_latest_verify_current_authorization_result_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_provenance_certificate_report(&fixture.run_config()).unwrap();

        let paths = provenance_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.provenance_certificate_report_path).unwrap();
        report["execute_frozen_result"] = json!("tampered_execute_frozen_result");
        persist_json(&paths.provenance_certificate_report_path, &report).unwrap();

        let verify = verify_latest_provenance_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored provenance-certificate copy execute_frozen_result")
        }));
    }

    #[test]
    fn verify_fails_when_copied_nested_attestation_seal_verdict_drifts() {
        let fixture = fake_attestation_seal_latest_fixture(
            "attestation_seal_latest_verify_release_capsule_verdict_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_provenance_certificate_report(&fixture.run_config()).unwrap();

        let paths = provenance_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.provenance_certificate_report_path).unwrap();
        report["verdict"] = json!("tampered_release_capsule_verdict");
        persist_json(&paths.provenance_certificate_report_path, &report).unwrap();

        let verify = verify_latest_provenance_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| entry.contains("stored provenance-certificate copy verdict")));
    }

    #[test]
    fn verify_fails_when_copied_nested_attestation_seal_current_live_cutover_controller_summary_drifts(
    ) {
        let fixture = fake_attestation_seal_latest_fixture(
            "attestation_seal_latest_verify_current_live_cutover_summary_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_provenance_certificate_report(&fixture.run_config()).unwrap();

        let paths = provenance_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.provenance_certificate_report_path).unwrap();
        report["reviewed_frozen_live_cutover_controller_command_summary"] =
            json!("tampered live cutover controller summary");
        persist_json(&paths.provenance_certificate_report_path, &report).unwrap();

        let verify = verify_latest_provenance_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains(
                "stored provenance-certificate copy reviewed_frozen_live_cutover_controller_command_summary",
            )
        }));
    }

    #[test]
    fn all_modes_preserve_stage3_and_pre_activation_refusal_semantics_and_do_not_imply_activation_authorization_by_themselves(
    ) {
        {
            let fixture = fake_attestation_seal_latest_fixture(
                "attestation_seal_latest_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let plan = plan_latest_provenance_certificate_report(&fixture.plan_config()).unwrap();
            assert_eq!(
                plan.verdict,
                TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestPlanReady
            );
            assert!(!plan.activation_authorized);

            let run = run_latest_provenance_certificate_report(&fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestRefusedNowByStage3
            );
            assert_eq!(run.result.as_deref(), Some("refused_now_by_stage3"));
            assert!(!run.activation_authorized);

            let verify =
                verify_latest_provenance_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestVerifyOk,
                "{:#?}",
                verify.verification_mismatches
            );
            assert!(!verify.activation_authorized);
        }

        {
            let fixture = fake_attestation_seal_latest_fixture(
                "attestation_seal_latest_pre_activation",
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let run = run_latest_provenance_certificate_report(&fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestRefusedNowByPreActivationGate
            );
            assert_eq!(
                run.result.as_deref(),
                Some("refused_now_by_pre_activation_gate")
            );
            assert!(!run.activation_authorized);

            let verify =
                verify_latest_provenance_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestVerifyOk,
                "{:#?}",
                verify.verification_mismatches
            );
            assert!(!verify.activation_authorized);
        }

        {
            let fixture = fake_attestation_seal_latest_fixture(
                "attestation_seal_latest_green",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_signoff",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let run = run_latest_provenance_certificate_report(&fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageProvenanceCertificateLatestVerdict::TinyLivePackageProvenanceCertificateLatestReadyForManualExecutionWhenGateTurnsGreen
            );
            assert_eq!(
                run.result.as_deref(),
                Some("ready_for_manual_execution_when_gate_turns_green")
            );
            assert!(!run.activation_authorized);
            assert!(run
                .explicit_statement
                .contains("never runs the frozen live cutover controller"));
        }
    }

    fn fake_attestation_seal_latest_fixture(
        label: &str,
        attestation_seal_latest_run_result: &str,
        handoff_bundle_result: &str,
    ) -> FakeAttestationSealLatestFixture {
        let temp_dir = temp_dir(label);
        let root = temp_dir.join("root");
        let wrapper_session_dir = temp_dir.join("attestation-seal-latest-session");
        let output_script_path = temp_dir.join("attestation-seal-latest.sh");
        let bin_dir = temp_dir.join("bin");
        let latest_top_session_dir = temp_dir.join("latest-clerestory-session");
        let historical_execute_frozen_session_dir =
            temp_dir.join("historical-execute-frozen-session");
        let turn_green_session_dir = temp_dir.join("historical-turn-green-session");
        let launch_packet_session_dir = temp_dir.join("historical-launch-packet-session");
        let package_dir = temp_dir.join("package");
        let install_root = temp_dir.join("install-root");
        let backend_command = temp_dir.join("backend");
        let attestation_seal_latest_plan_json_path =
            temp_dir.join("attestation_seal_latest.plan.json");
        let attestation_seal_latest_run_json_path =
            temp_dir.join("attestation_seal_latest.run.json");
        let attestation_seal_latest_verify_json_path =
            temp_dir.join("attestation_seal_latest.verify.json");
        let handoff_bundle_run_json_path = temp_dir.join("handoff_bundle.run.json");
        let handoff_bundle_verify_json_path = temp_dir.join("handoff_bundle.verify.json");
        let handoff_bundle_session_json_path = temp_dir.join("handoff_bundle.session.json");
        let handoff_bundle_status_json_path = temp_dir.join("handoff_bundle.status.json");
        let frozen_controller_command =
            "copybot_tiny_live_activation_package_live_cutover --package-dir /fake/pkg".to_string();

        for path in [
            &root,
            &bin_dir,
            &latest_top_session_dir,
            &historical_execute_frozen_session_dir,
            &turn_green_session_dir,
            &launch_packet_session_dir,
            &package_dir,
            &install_root,
        ] {
            fs::create_dir_all(path).unwrap();
        }
        fs::write(&backend_command, "#!/bin/sh\n").unwrap();

        let fixture = FakeAttestationSealLatestFixture {
            temp_dir,
            root,
            wrapper_session_dir,
            output_script_path,
            bin_dir,
            latest_top_session_dir,
            historical_execute_frozen_session_dir,
            turn_green_session_dir,
            launch_packet_session_dir,
            package_dir,
            install_root,
            backend_command,
            attestation_seal_latest_plan_json_path,
            attestation_seal_latest_run_json_path,
            attestation_seal_latest_verify_json_path,
            handoff_bundle_run_json_path,
            handoff_bundle_verify_json_path,
            handoff_bundle_session_json_path,
            handoff_bundle_status_json_path,
            frozen_controller_command,
        };

        persist_json(
            &fixture.attestation_seal_latest_plan_json_path,
            &default_attestation_seal_latest_plan_report(&fixture),
        )
        .unwrap();
        persist_json(
            &fixture.attestation_seal_latest_run_json_path,
            &default_attestation_seal_latest_run_report(
                &fixture,
                attestation_seal_latest_run_result,
            ),
        )
        .unwrap();
        persist_json(
            &fixture.attestation_seal_latest_verify_json_path,
            &default_attestation_seal_latest_verify_report(
                &fixture,
                attestation_seal_latest_run_result,
            ),
        )
        .unwrap();
        persist_json(
            &fixture.handoff_bundle_session_json_path,
            &default_attestation_seal_session(&fixture),
        )
        .unwrap();
        persist_json(
            &fixture.handoff_bundle_status_json_path,
            &default_attestation_seal_status(&fixture, handoff_bundle_result),
        )
        .unwrap();
        persist_json(
            &fixture.handoff_bundle_run_json_path,
            &default_attestation_seal_run_report(&fixture, handoff_bundle_result),
        )
        .unwrap();
        persist_json(
            &fixture.handoff_bundle_verify_json_path,
            &default_attestation_seal_verify_report(&fixture, handoff_bundle_result),
        )
        .unwrap();

        let paths = provenance_certificate_latest_paths(&fixture.wrapper_session_dir);
        write_fake_attestation_seal_latest_command(
            &fixture.bin_dir.join(ATTESTATION_SEAL_LATEST_BIN),
            &fixture.attestation_seal_latest_plan_json_path,
            &fixture.attestation_seal_latest_run_json_path,
            &fixture.attestation_seal_latest_verify_json_path,
            &fixture.root,
            &paths.nested_attestation_seal_latest_session_dir,
        );
        write_fake_release_capsule_command(
            &fixture.bin_dir.join(PROVENANCE_CERTIFICATE_BIN),
            &fixture.handoff_bundle_run_json_path,
            &fixture.handoff_bundle_verify_json_path,
            &fixture.handoff_bundle_session_json_path,
            &fixture.handoff_bundle_status_json_path,
            &expected_downstream_attestation_seal_session_dir(&paths),
            &expected_downstream_decision_packet_session_dir(&paths),
            &paths.nested_provenance_certificate_session_dir,
        );

        fixture
    }

    fn default_attestation_seal_latest_plan_report(
        fixture: &FakeAttestationSealLatestFixture,
    ) -> Value {
        let paths = provenance_certificate_latest_paths(&fixture.wrapper_session_dir);
        json!({
            "generated_at": Utc::now(),
            "mode": "plan_latest_attestation_seal",
            "verdict": ATTESTATION_SEAL_LATEST_PLAN_READY,
            "reason": "latest immutable chain resolves cleanly into accepted latest-attestation-seal contract",
            "latest_top_step_name": "clerestory_certificate",
            "latest_top_session_dir": fixture.latest_top_session_dir.display().to_string(),
            "historical_execute_frozen_session_dir": fixture.historical_execute_frozen_session_dir.display().to_string(),
            "turn_green_session_dir": fixture.turn_green_session_dir.display().to_string(),
            "launch_packet_session_dir": fixture.launch_packet_session_dir.display().to_string(),
            "package_dir": fixture.package_dir.display().to_string(),
            "install_root": fixture.install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": fixture.backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "session_dir": paths.nested_attestation_seal_latest_session_dir.display().to_string(),
            "downstream_decision_packet_session_dir": expected_downstream_decision_packet_session_dir(&paths).display().to_string(),
            "latest_execute_verdict": "tiny_live_package_execute_latest_plan_ready",
            "decision_packet_plan_verdict": "tiny_live_package_decision_packet_plan_ready",
            "reviewed_frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "clerestory_certificate_summary": "latest clerestory summary",
            "clerestory_certificate_sha256": "latest-clerestory-sha",
            "clerestory_certificate_algorithm": "sha256",
            "activation_authorized": false,
            "explicit_statement": "latest release capsule wrapper plan"
        })
    }

    fn canonical_attestation_seal_latest_result(result: &str) -> &str {
        match result {
            "runnable_when_gate_truth_turns_green" => {
                "ready_for_manual_execution_when_gate_turns_green"
            }
            other => other,
        }
    }

    fn canonical_release_capsule_result(result: &str) -> &str {
        match result {
            "ready_for_manual_go_live_signoff" => {
                "ready_for_manual_execution_when_gate_turns_green"
            }
            other => other,
        }
    }

    fn default_attestation_seal_latest_run_report(
        fixture: &FakeAttestationSealLatestFixture,
        result: &str,
    ) -> Value {
        let result = canonical_attestation_seal_latest_result(result);
        let paths = provenance_certificate_latest_paths(&fixture.wrapper_session_dir);
        let (verdict, gate_verdict, gate_reason) = match result {
            "ready_for_manual_execution_when_gate_turns_green" => (
                "tiny_live_package_attestation_seal_latest_ready_for_manual_execution_when_gate_turns_green",
                Value::Null,
                Value::Null,
            ),
            "refused_now_by_stage3" => (
                "tiny_live_package_attestation_seal_latest_refused_now_by_stage3",
                json!("blocked"),
                json!("stage3 blocked"),
            ),
            "refused_now_by_pre_activation_gate" => (
                "tiny_live_package_attestation_seal_latest_refused_now_by_pre_activation_gate",
                json!("blocked"),
                json!("pre gate blocked"),
            ),
            "refused_by_missing_latest_chain" => (
                "tiny_live_package_attestation_seal_latest_refused_by_missing_latest_chain",
                Value::Null,
                Value::Null,
            ),
            "refused_by_invalid_latest_chain" => (
                "tiny_live_package_attestation_seal_latest_refused_by_invalid_latest_chain",
                Value::Null,
                Value::Null,
            ),
            _ => (
                "tiny_live_package_attestation_seal_latest_refused_by_drifted_attestation_seal_contract",
                Value::Null,
                Value::Null,
            ),
        };
        json!({
            "generated_at": Utc::now(),
            "mode": "run_latest_attestation_seal",
            "verdict": verdict,
            "reason": format!("latest release capsule {result}"),
            "latest_top_step_name": "clerestory_certificate",
            "latest_top_session_dir": fixture.latest_top_session_dir.display().to_string(),
            "historical_execute_frozen_session_dir": fixture.historical_execute_frozen_session_dir.display().to_string(),
            "turn_green_session_dir": fixture.turn_green_session_dir.display().to_string(),
            "launch_packet_session_dir": fixture.launch_packet_session_dir.display().to_string(),
            "package_dir": fixture.package_dir.display().to_string(),
            "install_root": fixture.install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": fixture.backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "session_dir": paths.nested_attestation_seal_latest_session_dir.display().to_string(),
            "downstream_decision_packet_session_dir": expected_downstream_decision_packet_session_dir(&paths).display().to_string(),
            "latest_execute_verdict": "tiny_live_package_execute_latest_plan_ready",
            "decision_packet_plan_verdict": "tiny_live_package_decision_packet_plan_ready",
            "result": result,
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason,
            "reviewed_frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "clerestory_certificate_summary": "latest clerestory summary",
            "clerestory_certificate_sha256": "latest-clerestory-sha",
            "clerestory_certificate_algorithm": "sha256",
            "activation_authorized": false,
            "explicit_statement": "latest release capsule wrapper run"
        })
    }

    fn default_attestation_seal_latest_verify_report(
        fixture: &FakeAttestationSealLatestFixture,
        result: &str,
    ) -> Value {
        let mut report = default_attestation_seal_latest_run_report(fixture, result);
        report["mode"] = json!("verify_latest_attestation_seal");
        report["verdict"] = json!(ATTESTATION_SEAL_LATEST_VERIFY_OK);
        report["reason"] = json!("verify ok");
        report
    }

    fn fake_release_capsule_audit_manifest_summary() -> &'static str {
        "Hash-locked audit manifest: sha256 digests over 9 archival members; any content drift invalidates this release capsule"
    }

    fn fake_release_capsule_digest_manifest_sha256() -> &'static str {
        "attestation-seal-manifest-sha256"
    }

    fn fake_release_capsule_summary(result: &str, frozen_controller_command: &str) -> String {
        match result {
            "ready_for_manual_execution_when_gate_turns_green" => format!(
                "Release capsule outcome: this immutable release record is ready for manual execution when gate truth turns green; the exact reviewed frozen controller command remains: {frozen_controller_command}"
            ),
            "refused_now_by_stage3" => {
                "Release capsule outcome: the immutable reviewed chain remains refused now by Stage 3; archive this capsule as tamper-evident refusal evidence only and do not run the frozen controller.".to_string()
            }
            "refused_now_by_pre_activation_gate" => {
                "Release capsule outcome: the immutable reviewed chain remains refused now by the pre-activation gate; archive this capsule as tamper-evident refusal evidence only and do not run the frozen controller.".to_string()
            }
            _ => {
                "Release capsule outcome: the immutable reviewed chain is invalid or drifted; archive this capsule for audit only and mint a new coherent warrant chain before any later execution record.".to_string()
            }
        }
    }

    fn fake_attestation_reason(result: &str) -> &'static str {
        match result {
            "ready_for_manual_execution_when_gate_turns_green" => {
                "the immutable release capsule is coherent and this attestation seal is ready for manual execution when gate truth turns green"
            }
            "refused_now_by_stage3" => {
                "the immutable release capsule remains refused now by Stage 3, so this attestation seal remains an explicit refusal record"
            }
            "refused_now_by_pre_activation_gate" => {
                "the immutable release capsule remains refused now by the pre-activation gate, so this attestation seal remains an explicit refusal record"
            }
            _ => {
                "the verified attestation-seal chain is invalid, drifted, or non-runnable, so this attestation seal remains refused"
            }
        }
    }

    fn fake_attestation_seal_summary(result: &str, frozen_controller_command: &str) -> String {
        match result {
            "ready_for_manual_execution_when_gate_turns_green" => format!(
                "Attestation seal outcome: this immutable custody record is ready for manual execution when gate truth turns green; the exact reviewed frozen controller command remains: {frozen_controller_command}"
            ),
            "refused_now_by_stage3" => {
                "Attestation seal outcome: the immutable reviewed chain remains refused now by Stage 3; archive this final seal as custody evidence only and do not run the frozen controller.".to_string()
            }
            "refused_now_by_pre_activation_gate" => {
                "Attestation seal outcome: the immutable reviewed chain remains refused now by the pre-activation gate; archive this final seal as custody evidence only and do not run the frozen controller.".to_string()
            }
            _ => {
                "Attestation seal outcome: the immutable reviewed chain is invalid or drifted; archive this final seal for audit only and mint a new coherent custody record before any later execution record.".to_string()
            }
        }
    }

    fn fake_custody_record_summary() -> String {
        format!(
            "Custody record identity: nested attestation-seal digest manifest uses sha256 across 9 archival members with manifest sha256 {}; any digest drift invalidates this attestation seal",
            fake_release_capsule_digest_manifest_sha256()
        )
    }

    fn default_attestation_seal_session(fixture: &FakeAttestationSealLatestFixture) -> Value {
        let paths = provenance_certificate_latest_paths(&fixture.wrapper_session_dir);
        let release_capsule_session_dir = expected_downstream_attestation_seal_session_dir(&paths);
        let activation_ticket_session_dir = fixture
            .temp_dir
            .join("downstream-attestation-seal-activation-ticket-session");
        let review_receipt_session_dir = expected_downstream_review_receipt_session_dir(&paths);
        let handoff_bundle_session_dir = fixture
            .temp_dir
            .join("downstream-attestation-seal-handoff-bundle-session");
        let snapshot = LatestProvenanceCertificateSnapshot {
            latest_top_step_name: "clerestory_certificate".to_string(),
            latest_top_session_dir: fixture.latest_top_session_dir.clone(),
            downstream_decision_packet_session_dir: expected_downstream_decision_packet_session_dir(
                &paths,
            ),
            historical_execute_frozen_session_dir: fixture
                .historical_execute_frozen_session_dir
                .clone(),
            turn_green_session_dir: fixture.turn_green_session_dir.clone(),
            launch_packet_session_dir: fixture.launch_packet_session_dir.clone(),
            package_dir: fixture.package_dir.clone(),
            install_root: fixture.install_root.clone(),
            target_service_name: "solana-copy-bot.service".to_string(),
            backend_command: fixture.backend_command.display().to_string(),
            wrapper_timeout_ms: 1200,
            service_status_max_staleness_ms: 4500,
            reviewed_frozen_live_cutover_controller_command_summary: fixture
                .frozen_controller_command
                .clone(),
            clerestory_certificate_summary: "latest clerestory summary".to_string(),
            clerestory_certificate_sha256: "latest-clerestory-sha".to_string(),
            clerestory_certificate_algorithm: "sha256".to_string(),
        };
        json!({
            "session_version": "1",
            "planned_at": Utc::now(),
            "attestation_seal_session_dir": release_capsule_session_dir.display().to_string(),
            "release_capsule_session_dir": release_capsule_session_dir.display().to_string(),
            "activation_ticket_session_dir": activation_ticket_session_dir.display().to_string(),
            "review_receipt_session_dir": review_receipt_session_dir.display().to_string(),
            "handoff_bundle_session_dir": handoff_bundle_session_dir.display().to_string(),
            "decision_packet_session_dir": snapshot.downstream_decision_packet_session_dir.display().to_string(),
            "execute_frozen_session_dir": snapshot.historical_execute_frozen_session_dir.display().to_string(),
            "turn_green_session_dir": snapshot.turn_green_session_dir.display().to_string(),
            "launch_packet_session_dir": snapshot.launch_packet_session_dir.display().to_string(),
            "package_dir": snapshot.package_dir.display().to_string(),
            "install_root": snapshot.install_root.display().to_string(),
            "target_service_name": snapshot.target_service_name,
            "backend_command": snapshot.backend_command,
            "wrapper_timeout_ms": snapshot.wrapper_timeout_ms,
            "service_status_max_staleness_ms": snapshot.service_status_max_staleness_ms,
            "session_dir": provenance_certificate_latest_paths(&fixture.wrapper_session_dir).nested_provenance_certificate_session_dir.display().to_string(),
            "verify_attestation_seal_command_summary": provenance_certificate_verify_attestation_seal_command_summary(&release_capsule_session_dir),
            "reviewed_frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "attestation_seal_summary": fake_attestation_seal_summary(
                "ready_for_manual_execution_when_gate_turns_green",
                &fixture.frozen_controller_command
            ),
            "custody_record_summary": fake_custody_record_summary(),
            "release_capsule_digest_manifest_sha256": fake_release_capsule_digest_manifest_sha256(),
            "release_capsule_digest_manifest_entry_count": 9,
            "release_capsule_digest_algorithm": "sha256",
            "provenance_certificate_summary": "Provenance certificate outcome: fixture session is ready for manual execution when gate truth turns green.",
            "chain_fingerprint_summary": "Chain fingerprint identity: fixture provenance fingerprint over nested release-capsule digest manifest truth.",
            "chain_fingerprint_sha256": "fixture-provenance-fingerprint-sha256",
            "chain_fingerprint_algorithm": "sha256",
            "explicit_statement": "the verified attestation-seal session is the primary input here; this immutable provenance certificate freezes one final canonical chain fingerprint over the exact reviewed frozen live cutover contract without enabling production execution"
        })
    }

    fn default_attestation_seal_status(
        fixture: &FakeAttestationSealLatestFixture,
        result: &str,
    ) -> Value {
        let result = canonical_release_capsule_result(result);
        let paths = provenance_certificate_latest_paths(&fixture.wrapper_session_dir);
        let (reason, gate_verdict, gate_reason) = match result {
            "ready_for_manual_execution_when_gate_turns_green" => {
                (fake_attestation_reason(result), json!("green"), json!("ok"))
            }
            "refused_now_by_stage3" => (
                fake_attestation_reason(result),
                json!("blocked"),
                json!("stage3 blocked"),
            ),
            "refused_now_by_pre_activation_gate" => (
                fake_attestation_reason(result),
                json!("blocked"),
                json!("pre gate blocked"),
            ),
            _ => (fake_attestation_reason(result), Value::Null, Value::Null),
        };
        json!({
            "status_version": "1",
            "updated_at": Utc::now(),
            "session_dir": paths.nested_provenance_certificate_session_dir.display().to_string(),
            "attestation_seal_session_dir": expected_downstream_attestation_seal_session_dir(&paths).display().to_string(),
            "result": result,
            "reason": reason,
            "handoff_bundle_result": if result == "ready_for_manual_execution_when_gate_turns_green" { json!("ready_for_manual_go_live_review") } else if result == "refused_now_by_stage3" { json!("refused_now_by_stage3") } else { json!("refused_now_by_pre_activation_gate") },
            "decision_packet_result": if result == "ready_for_manual_execution_when_gate_turns_green" { json!("runnable_when_gate_truth_turns_green") } else if result == "refused_now_by_stage3" { json!("refused_now_by_stage3") } else { json!("refused_now_by_pre_activation_gate") },
            "execute_frozen_result": "completed_keep_running",
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason,
            "attestation_seal_step": Value::Null,
            "attestation_seal_summary": fake_attestation_seal_summary(result, &fixture.frozen_controller_command),
            "custody_record_summary": fake_custody_record_summary(),
            "release_capsule_digest_manifest_sha256": fake_release_capsule_digest_manifest_sha256(),
            "release_capsule_digest_manifest_entry_count": 9,
            "release_capsule_digest_algorithm": "sha256",
            "provenance_certificate_summary": "Provenance certificate outcome: fixture status mirrors native provenance result.",
            "chain_fingerprint_summary": "Chain fingerprint identity: fixture status provenance fingerprint.",
            "chain_fingerprint_sha256": "fixture-provenance-fingerprint-sha256",
            "chain_fingerprint_algorithm": "sha256",
            "explicit_statement": "this provenance certificate is archival and read-only; it never executes or enables the frozen controller"
        })
    }

    fn default_attestation_seal_run_report(
        fixture: &FakeAttestationSealLatestFixture,
        result: &str,
    ) -> Value {
        let result = canonical_release_capsule_result(result);
        let paths = provenance_certificate_latest_paths(&fixture.wrapper_session_dir);
        let status = default_attestation_seal_status(fixture, result);
        let provenance_verdict = match result {
            "ready_for_manual_execution_when_gate_turns_green" => {
                "tiny_live_package_provenance_certificate_ready_for_manual_execution_when_gate_turns_green"
            }
            "refused_now_by_stage3" => {
                "tiny_live_package_provenance_certificate_refused_now_by_stage3"
            }
            "refused_now_by_pre_activation_gate" => {
                "tiny_live_package_provenance_certificate_refused_now_by_pre_activation_gate"
            }
            _ => "tiny_live_package_provenance_certificate_refused_now_by_invalid_or_drifted_contract",
        };
        json!({
            "generated_at": Utc::now(),
            "mode": "run_live_package_provenance_certificate",
            "verdict": provenance_verdict,
            "reason": status["reason"],
            "attestation_seal_session_dir": expected_downstream_attestation_seal_session_dir(&paths).display().to_string(),
            "release_capsule_session_dir": expected_downstream_attestation_seal_session_dir(&paths).display().to_string(),
            "activation_ticket_session_dir": fixture.temp_dir.join("downstream-attestation-seal-activation-ticket-session").display().to_string(),
            "review_receipt_session_dir": expected_downstream_review_receipt_session_dir(&paths).display().to_string(),
            "handoff_bundle_session_dir": fixture.temp_dir.join("downstream-attestation-seal-handoff-bundle-session").display().to_string(),
            "decision_packet_session_dir": expected_downstream_decision_packet_session_dir(&paths).display().to_string(),
            "execute_frozen_session_dir": fixture.historical_execute_frozen_session_dir.display().to_string(),
            "turn_green_session_dir": fixture.turn_green_session_dir.display().to_string(),
            "launch_packet_session_dir": fixture.launch_packet_session_dir.display().to_string(),
            "package_dir": fixture.package_dir.display().to_string(),
            "install_root": fixture.install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": fixture.backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "session_dir": paths.nested_provenance_certificate_session_dir.display().to_string(),
            "provenance_certificate_session_path": nested_provenance_certificate_session_path(&paths.nested_provenance_certificate_session_dir).display().to_string(),
            "provenance_certificate_status_path": nested_provenance_certificate_status_path(&paths.nested_provenance_certificate_session_dir).display().to_string(),
            "archived_attestation_seal_report_path": paths.nested_provenance_certificate_session_dir.join("tiny_live_activation_package_provenance_certificate.attestation_seal.report.json").display().to_string(),
            "archived_release_capsule_digest_manifest_path": paths.nested_provenance_certificate_session_dir.join("tiny_live_activation_package_provenance_certificate.release_capsule.digest_manifest.json").display().to_string(),
            "result": result,
            "handoff_bundle_result": status["handoff_bundle_result"],
            "decision_packet_result": status["decision_packet_result"],
            "execute_frozen_result": status["execute_frozen_result"],
            "attestation_seal_step": Value::Null,
            "verify_attestation_seal_command_summary": provenance_certificate_verify_attestation_seal_command_summary(&expected_downstream_attestation_seal_session_dir(&paths)),
            "reviewed_frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "attestation_seal_summary": status["attestation_seal_summary"],
            "custody_record_summary": status["custody_record_summary"],
            "release_capsule_digest_manifest_sha256": status["release_capsule_digest_manifest_sha256"],
            "release_capsule_digest_manifest_entry_count": status["release_capsule_digest_manifest_entry_count"],
            "release_capsule_digest_algorithm": status["release_capsule_digest_algorithm"],
            "provenance_certificate_summary": status["provenance_certificate_summary"],
            "chain_fingerprint_summary": status["chain_fingerprint_summary"],
            "chain_fingerprint_sha256": status["chain_fingerprint_sha256"],
            "chain_fingerprint_algorithm": status["chain_fingerprint_algorithm"],
            "current_pre_activation_gate_verdict": status["current_pre_activation_gate_verdict"],
            "current_pre_activation_gate_reason": status["current_pre_activation_gate_reason"],
            "explicit_statement": status["explicit_statement"]
        })
    }

    fn default_attestation_seal_verify_report(
        fixture: &FakeAttestationSealLatestFixture,
        result: &str,
    ) -> Value {
        let mut report = default_attestation_seal_run_report(fixture, result);
        report["mode"] = json!("verify_live_package_provenance_certificate");
        report["verdict"] = json!(PROVENANCE_CERTIFICATE_VERIFY_OK);
        report["reason"] = json!("verify ok");
        report["explicit_statement"] = json!("this verification binds one immutable provenance certificate to verified attestation-seal truth, the exact reviewed frozen controller summary, and the canonical nested archival chain fingerprint");
        report
    }

    fn write_fake_attestation_seal_latest_command(
        path: &Path,
        plan_json_path: &Path,
        run_json_path: &Path,
        verify_json_path: &Path,
        root: &Path,
        session_dir: &Path,
    ) {
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected latest-attestation-seal session-dir' >&2; exit 1;;\nesac\nmode=''\ncase \" $* \" in\n  *\" --plan-latest-attestation-seal \"*) mode='plan';;\n  *\" --run-latest-attestation-seal \"*) mode='run';;\n  *\" --verify-latest-attestation-seal \"*) mode='verify';;\n  *) echo 'unexpected latest-attestation-seal mode' >&2; exit 1;;\nesac\ncase \"$mode\" in\n  plan|run)\n    case \" $* \" in\n      *\" --root {} \"*) : ;;\n      *) echo 'unexpected root' >&2; exit 1;;\n    esac\n    ;;\nesac\ncase \"$mode\" in\n  plan) cat '{}';;\n  run) mkdir -p '{}'; cp '{}' '{}/tiny_live_activation_package_attestation_seal_latest.report.json'; cat '{}';;\n  verify) cat '{}';;\n  *) exit 1;;\nesac\n",
            session_dir.display(),
            root.display(),
            plan_json_path.display(),
            session_dir.display(),
            run_json_path.display(),
            session_dir.display(),
            run_json_path.display(),
            verify_json_path.display(),
        );
        fs::write(path, script).unwrap();
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).unwrap();
    }

    fn write_fake_release_capsule_command(
        path: &Path,
        run_json_path: &Path,
        verify_json_path: &Path,
        session_json_path: &Path,
        status_json_path: &Path,
        release_capsule_session_dir: &Path,
        decision_packet_session_dir: &Path,
        session_dir: &Path,
    ) {
        let nested_session_path = nested_provenance_certificate_session_path(session_dir);
        let nested_status_path = nested_provenance_certificate_status_path(session_dir);
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --attestation-seal-session-dir {} \"*) : ;;\n  *) echo 'unexpected provenance-certificate attestation-seal-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --confirm-decision-packet-session-dir {} \"*) : ;;\n  *) echo 'unexpected provenance-certificate confirm-decision-packet-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected provenance-certificate session-dir' >&2; exit 1;;\nesac\nmode=''\ncase \" $* \" in\n  *\" --run-live-package-provenance-certificate \"*) mode='run';;\n  *\" --verify-live-package-provenance-certificate \"*) mode='verify';;\n  *) echo 'unexpected provenance-certificate mode' >&2; exit 1;;\nesac\ncase \"$mode\" in\n  run) mkdir -p '{}'; cp '{}' '{}'; cp '{}' '{}'; cat '{}';;\n  verify) cat '{}';;\n  *) exit 1;;\nesac\n",
            release_capsule_session_dir.display(),
            decision_packet_session_dir.display(),
            session_dir.display(),
            session_dir.display(),
            session_json_path.display(),
            nested_session_path.display(),
            status_json_path.display(),
            nested_status_path.display(),
            run_json_path.display(),
            verify_json_path.display(),
        );
        fs::write(path, script).unwrap();
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).unwrap();
    }

    struct PathGuard {
        _lock: MutexGuard<'static, ()>,
        original_path: Option<OsString>,
    }

    impl PathGuard {
        fn install(bin_dir: &Path) -> Self {
            let lock = env_lock()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let original_path = std::env::var_os("PATH");
            let mut new_path = OsString::from(bin_dir.as_os_str());
            if let Some(existing) = &original_path {
                new_path.push(":");
                new_path.push(existing);
            }
            std::env::set_var("PATH", &new_path);
            Self {
                _lock: lock,
                original_path,
            }
        }
    }

    impl Drop for PathGuard {
        fn drop(&mut self) {
            match &self.original_path {
                Some(value) => std::env::set_var("PATH", value),
                None => std::env::remove_var("PATH"),
            }
        }
    }

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_nanos(0))
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{label}.{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }
}
