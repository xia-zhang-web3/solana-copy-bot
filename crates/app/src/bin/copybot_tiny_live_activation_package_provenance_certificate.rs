use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_app::tiny_live_activation::install_target_managed_surface::validate_turn_green_session_dir as validate_managed_surface_session_dir;
use copybot_app::tiny_live_activation::package_attestation_seal_provenance_certificate as attestation_seal_provenance_certificate;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: copybot_tiny_live_activation_package_provenance_certificate --attestation-seal-session-dir <path> [--confirm-decision-packet-session-dir <path>] [--session-dir <path>] [--output <path>] [--json] (--plan-live-package-provenance-certificate | --render-live-package-provenance-certificate | --run-live-package-provenance-certificate | --verify-live-package-provenance-certificate)";
const PROVENANCE_CERTIFICATE_SESSION_VERSION: &str = "1";
const PROVENANCE_CERTIFICATE_STATUS_VERSION: &str = "1";
const PROVENANCE_CERTIFICATE_CHAIN_FINGERPRINT_ALGORITHM: &str = "sha256";
const PROVENANCE_CERTIFICATE_SESSION_EXPLICIT_STATEMENT: &str =
    "the verified attestation-seal session is the primary input here; this immutable provenance certificate freezes one final canonical chain fingerprint over the exact reviewed frozen live cutover contract without enabling production execution";
const PROVENANCE_CERTIFICATE_STATUS_EXPLICIT_STATEMENT: &str =
    "this provenance certificate is archival and read-only; it never executes or enables the frozen controller";
const PROVENANCE_CERTIFICATE_VERIFY_EXPLICIT_STATEMENT: &str =
    "this verification binds one immutable provenance certificate to verified attestation-seal truth, the exact reviewed frozen controller summary, and the canonical nested archival chain fingerprint";

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
    attestation_seal_session_dir: PathBuf,
    confirmed_decision_packet_session_dir: Option<PathBuf>,
    session_dir: Option<PathBuf>,
    output_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanLivePackageProvenanceCertificate,
    RenderLivePackageProvenanceCertificate,
    RunLivePackageProvenanceCertificate,
    VerifyLivePackageProvenanceCertificate,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageProvenanceCertificateVerdict {
    TinyLivePackageProvenanceCertificatePlanReady,
    TinyLivePackageProvenanceCertificateRendered,
    TinyLivePackageProvenanceCertificateRefusedNowByStage3,
    TinyLivePackageProvenanceCertificateRefusedNowByPreActivationGate,
    TinyLivePackageProvenanceCertificateRefusedNowByInvalidOrDriftedContract,
    TinyLivePackageProvenanceCertificateReadyForManualExecutionWhenGateTurnsGreen,
    TinyLivePackageProvenanceCertificateVerifyOk,
    TinyLivePackageProvenanceCertificateVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageProvenanceCertificateResult {
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    RefusedNowByInvalidOrDriftedContract,
    ReadyForManualExecutionWhenGateTurnsGreen,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageProvenanceCertificateStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageProvenanceCertificateSession {
    session_version: String,
    planned_at: DateTime<Utc>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageProvenanceCertificateStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    attestation_seal_session_dir: String,
    result: LivePackageProvenanceCertificateResult,
    reason: String,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    attestation_seal_step: Option<PackageProvenanceCertificateStepArtifact>,
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

#[derive(Debug, Clone)]
struct PackageProvenanceCertificatePaths {
    session_path: PathBuf,
    status_path: PathBuf,
    attestation_seal_report_path: PathBuf,
    release_capsule_digest_manifest_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageProvenanceCertificateReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageProvenanceCertificateVerdict,
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
    attestation_seal_step: Option<PackageProvenanceCertificateStepArtifact>,
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
    release_capsule_digest_manifest:
        Option<attestation_seal_provenance_certificate::ReleaseCapsuleDigestManifest>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    verification_mismatches: Vec<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct NestedAttestationSealReportView {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: String,
    reason: String,
}

#[derive(Debug, Serialize)]
struct ChainFingerprintPayload<'a> {
    result: &'a str,
    handoff_bundle_result: Option<&'a str>,
    decision_packet_result: Option<&'a str>,
    execute_frozen_result: Option<&'a str>,
    current_pre_activation_gate_verdict: Option<&'a str>,
    current_pre_activation_gate_reason: Option<&'a str>,
    reviewed_frozen_live_cutover_controller_command_summary: &'a str,
    release_capsule_digest_manifest_sha256: &'a str,
    release_capsule_digest_manifest_entry_count: usize,
    release_capsule_digest_algorithm: &'a str,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut attestation_seal_session_dir: Option<PathBuf> = None;
    let mut confirmed_decision_packet_session_dir: Option<PathBuf> = None;
    let mut session_dir: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut json = false;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--attestation-seal-session-dir" => {
                attestation_seal_session_dir = Some(PathBuf::from(parse_string_arg(
                    "--attestation-seal-session-dir",
                    args.next(),
                )?))
            }
            "--confirm-decision-packet-session-dir" => {
                confirmed_decision_packet_session_dir = Some(PathBuf::from(parse_string_arg(
                    "--confirm-decision-packet-session-dir",
                    args.next(),
                )?))
            }
            "--session-dir" => {
                session_dir = Some(PathBuf::from(parse_string_arg(
                    "--session-dir",
                    args.next(),
                )?))
            }
            "--output" => {
                output_path = Some(PathBuf::from(parse_string_arg("--output", args.next())?))
            }
            "--json" => json = true,
            "--plan-live-package-provenance-certificate" => set_mode(
                &mut mode,
                Mode::PlanLivePackageProvenanceCertificate,
                arg.as_str(),
            )?,
            "--render-live-package-provenance-certificate" => set_mode(
                &mut mode,
                Mode::RenderLivePackageProvenanceCertificate,
                arg.as_str(),
            )?,
            "--run-live-package-provenance-certificate" => set_mode(
                &mut mode,
                Mode::RunLivePackageProvenanceCertificate,
                arg.as_str(),
            )?,
            "--verify-live-package-provenance-certificate" => set_mode(
                &mut mode,
                Mode::VerifyLivePackageProvenanceCertificate,
                arg.as_str(),
            )?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let Some(mode) = mode else {
        return Ok(None);
    };
    if matches!(mode, Mode::RenderLivePackageProvenanceCertificate) && output_path.is_none() {
        bail!("missing --output for --render-live-package-provenance-certificate");
    }
    if matches!(
        mode,
        Mode::RunLivePackageProvenanceCertificate | Mode::VerifyLivePackageProvenanceCertificate
    ) && session_dir.is_none()
    {
        bail!("missing --session-dir for run/verify mode");
    }
    if matches!(
        mode,
        Mode::RunLivePackageProvenanceCertificate | Mode::VerifyLivePackageProvenanceCertificate
    ) && confirmed_decision_packet_session_dir.is_none()
    {
        bail!("missing --confirm-decision-packet-session-dir for run/verify mode");
    }
    let attestation_seal_session_dir = attestation_seal_session_dir
        .ok_or_else(|| anyhow!("missing --attestation-seal-session-dir"))?;
    Ok(Some(Config {
        mode,
        attestation_seal_session_dir,
        confirmed_decision_packet_session_dir,
        session_dir,
        output_path,
        json,
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanLivePackageProvenanceCertificate => {
            plan_live_package_provenance_certificate_report(&config)?
        }
        Mode::RenderLivePackageProvenanceCertificate => {
            render_live_package_provenance_certificate_report(&config)?
        }
        Mode::RunLivePackageProvenanceCertificate => {
            run_live_package_provenance_certificate_report(&config)?
        }
        Mode::VerifyLivePackageProvenanceCertificate => {
            verify_live_package_provenance_certificate_report(&config)?
        }
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_live_package_provenance_certificate_report(
    config: &Config,
) -> Result<PackageProvenanceCertificateReport> {
    let (attestation_seal, release_capsule_digest_manifest, classification) =
        match config.confirmed_decision_packet_session_dir.as_deref() {
            Some(confirmed_decision_packet_session_dir) => {
                match attestation_seal_provenance_certificate::verify_live_package_attestation_seal_for_provenance_certificate(
                    &config.attestation_seal_session_dir,
                    confirmed_decision_packet_session_dir,
                ) {
                    Ok(verified) => {
                        let classification = classify_verified_attestation_seal(&verified);
                        (
                            verified.contract,
                            Some(verified.release_capsule_digest_manifest),
                            classification,
                        )
                    }
                    Err(_) => {
                        let raw_contract = attestation_seal_provenance_certificate::load_live_package_attestation_seal_contract_for_provenance_certificate(
                            &config.attestation_seal_session_dir,
                        )?;
                        let classification =
                            classify_provenance_contract(&raw_contract, false);
                        (raw_contract, None, classification)
                    }
                }
            }
            None => {
                let raw_contract = attestation_seal_provenance_certificate::load_live_package_attestation_seal_contract_for_provenance_certificate(
                    &config.attestation_seal_session_dir,
                )?;
                let classification = classify_provenance_contract(&raw_contract, false);
                (raw_contract, None, classification)
            }
        };
    let session_dir = config.session_dir.clone().unwrap_or_else(|| {
        config
            .attestation_seal_session_dir
            .join("provenance-certificate")
    });
    let manifest_sha256 = match release_capsule_digest_manifest.as_ref() {
        Some(value) => canonical_sha256_hex(value)?,
        None => attestation_seal
            .release_capsule_digest_manifest_sha256
            .clone(),
    };
    let manifest_entry_count = release_capsule_digest_manifest
        .as_ref()
        .map(|value| value.entries.len())
        .unwrap_or(attestation_seal.release_capsule_digest_manifest_entry_count);
    let manifest_algorithm = release_capsule_digest_manifest
        .as_ref()
        .map(|value| value.digest_algorithm.clone())
        .unwrap_or_else(|| attestation_seal.release_capsule_digest_algorithm.clone());
    let result = serialize_enum(&classification.0);
    let chain_fingerprint_sha256 = compute_chain_fingerprint_sha256(
        &result,
        attestation_seal.handoff_bundle_result.as_deref(),
        attestation_seal.decision_packet_result.as_deref(),
        attestation_seal.execute_frozen_result.as_deref(),
        attestation_seal
            .current_pre_activation_gate_verdict
            .as_deref(),
        attestation_seal
            .current_pre_activation_gate_reason
            .as_deref(),
        &attestation_seal.reviewed_frozen_live_cutover_controller_command_summary,
        &manifest_sha256,
        manifest_entry_count,
        &manifest_algorithm,
    )?;

    Ok(PackageProvenanceCertificateReport {
        generated_at: Utc::now(),
        mode: "plan_live_package_provenance_certificate".to_string(),
        verdict:
            TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificatePlanReady,
        reason: format!(
            "attestation-seal-native provenance certificate is explicit for verified attestation-seal session {}; this command stays archival and read-only while freezing one final canonical chain fingerprint",
            config.attestation_seal_session_dir.display()
        ),
        attestation_seal_session_dir: config.attestation_seal_session_dir.display().to_string(),
        release_capsule_session_dir: Some(
            attestation_seal.release_capsule_session_dir.display().to_string(),
        ),
        activation_ticket_session_dir: Some(
            attestation_seal
                .activation_ticket_session_dir
                .display()
                .to_string(),
        ),
        review_receipt_session_dir: Some(
            attestation_seal
                .review_receipt_session_dir
                .display()
                .to_string(),
        ),
        handoff_bundle_session_dir: Some(
            attestation_seal
                .handoff_bundle_session_dir
                .display()
                .to_string(),
        ),
        decision_packet_session_dir: Some(
            attestation_seal
                .decision_packet_session_dir
                .display()
                .to_string(),
        ),
        execute_frozen_session_dir: Some(
            attestation_seal
                .execute_frozen_session_dir
                .display()
                .to_string(),
        ),
        turn_green_session_dir: Some(
            attestation_seal.turn_green_session_dir.display().to_string(),
        ),
        launch_packet_session_dir: Some(
            attestation_seal.launch_packet_session_dir.display().to_string(),
        ),
        package_dir: Some(attestation_seal.package_dir.display().to_string()),
        install_root: Some(attestation_seal.install_root.display().to_string()),
        target_service_name: Some(attestation_seal.target_service_name.clone()),
        backend_command: Some(attestation_seal.backend_command.clone()),
        wrapper_timeout_ms: Some(attestation_seal.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(
            attestation_seal.service_status_max_staleness_ms,
        ),
        session_dir: Some(session_dir.display().to_string()),
        provenance_certificate_session_path: None,
        provenance_certificate_status_path: None,
        archived_attestation_seal_report_path: None,
        archived_release_capsule_digest_manifest_path: None,
        result: Some(result.clone()),
        handoff_bundle_result: attestation_seal.handoff_bundle_result.clone(),
        decision_packet_result: attestation_seal.decision_packet_result.clone(),
        execute_frozen_result: attestation_seal.execute_frozen_result.clone(),
        attestation_seal_step: None,
        verify_attestation_seal_command_summary: Some(verify_attestation_seal_command_summary(
            &config.attestation_seal_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            attestation_seal
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        attestation_seal_summary: Some(attestation_seal.attestation_seal_summary.clone()),
        custody_record_summary: Some(attestation_seal.custody_record_summary.clone()),
        release_capsule_digest_manifest_sha256: Some(manifest_sha256.clone()),
        release_capsule_digest_manifest_entry_count: Some(manifest_entry_count),
        release_capsule_digest_algorithm: Some(manifest_algorithm.clone()),
        provenance_certificate_summary: Some(provenance_certificate_summary(
            classification.0,
            &attestation_seal.reviewed_frozen_live_cutover_controller_command_summary,
        )),
        chain_fingerprint_summary: Some(chain_fingerprint_summary(
            PROVENANCE_CERTIFICATE_CHAIN_FINGERPRINT_ALGORITHM,
            &chain_fingerprint_sha256,
            &manifest_sha256,
            manifest_entry_count,
        )),
        chain_fingerprint_sha256: Some(chain_fingerprint_sha256),
        chain_fingerprint_algorithm: Some(
            PROVENANCE_CERTIFICATE_CHAIN_FINGERPRINT_ALGORITHM.to_string(),
        ),
        release_capsule_digest_manifest,
        current_pre_activation_gate_verdict: attestation_seal.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: attestation_seal.current_pre_activation_gate_reason,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this final provenance certificate freezes verified attestation-seal truth and one immutable canonical chain fingerprint without enabling production execution"
                .to_string(),
    })
}

fn render_live_package_provenance_certificate_report(
    config: &Config,
) -> Result<PackageProvenanceCertificateReport> {
    let plan = plan_live_package_provenance_certificate_report(config)?;
    let confirmed_decision_packet_session_dir = PathBuf::from(
        plan.decision_packet_session_dir
            .as_ref()
            .ok_or_else(|| anyhow!("plan is missing decision_packet_session_dir for render"))?,
    );
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --output for render mode"))?;
    let session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| output_path.with_extension("session"));
    let script = format!(
        "#!/usr/bin/env bash\nset -euo pipefail\n\ncopybot_tiny_live_activation_package_provenance_certificate --attestation-seal-session-dir {} --plan-live-package-provenance-certificate\ncopybot_tiny_live_activation_package_provenance_certificate --attestation-seal-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --run-live-package-provenance-certificate\ncopybot_tiny_live_activation_package_provenance_certificate --attestation-seal-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --verify-live-package-provenance-certificate\n",
        shell_escape_path(&config.attestation_seal_session_dir),
        shell_escape_path(&config.attestation_seal_session_dir),
        shell_escape_path(&confirmed_decision_packet_session_dir),
        shell_escape_path(&session_dir),
        shell_escape_path(&config.attestation_seal_session_dir),
        shell_escape_path(&confirmed_decision_packet_session_dir),
        shell_escape_path(&session_dir),
    );
    write_script(output_path, &script)?;
    Ok(PackageProvenanceCertificateReport {
        generated_at: Utc::now(),
        mode: "render_live_package_provenance_certificate".to_string(),
        verdict:
            TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificateRendered,
        reason: format!(
            "rendered attestation-seal-native provenance-certificate script to {}",
            output_path.display()
        ),
        attestation_seal_session_dir: plan.attestation_seal_session_dir,
        release_capsule_session_dir: plan.release_capsule_session_dir,
        activation_ticket_session_dir: plan.activation_ticket_session_dir,
        review_receipt_session_dir: plan.review_receipt_session_dir,
        handoff_bundle_session_dir: plan.handoff_bundle_session_dir,
        decision_packet_session_dir: plan.decision_packet_session_dir,
        execute_frozen_session_dir: plan.execute_frozen_session_dir,
        turn_green_session_dir: plan.turn_green_session_dir,
        launch_packet_session_dir: plan.launch_packet_session_dir,
        package_dir: plan.package_dir,
        install_root: plan.install_root,
        target_service_name: plan.target_service_name,
        backend_command: plan.backend_command,
        wrapper_timeout_ms: plan.wrapper_timeout_ms,
        service_status_max_staleness_ms: plan.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        provenance_certificate_session_path: None,
        provenance_certificate_status_path: None,
        archived_attestation_seal_report_path: None,
        archived_release_capsule_digest_manifest_path: None,
        result: plan.result,
        handoff_bundle_result: plan.handoff_bundle_result,
        decision_packet_result: plan.decision_packet_result,
        execute_frozen_result: plan.execute_frozen_result,
        attestation_seal_step: None,
        verify_attestation_seal_command_summary: plan.verify_attestation_seal_command_summary,
        reviewed_frozen_live_cutover_controller_command_summary: plan
            .reviewed_frozen_live_cutover_controller_command_summary,
        attestation_seal_summary: plan.attestation_seal_summary,
        custody_record_summary: plan.custody_record_summary,
        release_capsule_digest_manifest_sha256: plan.release_capsule_digest_manifest_sha256,
        release_capsule_digest_manifest_entry_count:
            plan.release_capsule_digest_manifest_entry_count,
        release_capsule_digest_algorithm: plan.release_capsule_digest_algorithm,
        provenance_certificate_summary: plan.provenance_certificate_summary,
        chain_fingerprint_summary: plan.chain_fingerprint_summary,
        chain_fingerprint_sha256: plan.chain_fingerprint_sha256,
        chain_fingerprint_algorithm: plan.chain_fingerprint_algorithm,
        release_capsule_digest_manifest: None,
        current_pre_activation_gate_verdict: plan.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: plan.current_pre_activation_gate_reason,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this rendered script accepts the verified attestation-seal session as the primary input and stays archival and read-only with respect to the real target"
                .to_string(),
    })
}

fn run_live_package_provenance_certificate_report(
    config: &Config,
) -> Result<PackageProvenanceCertificateReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for run mode"))?;
    let verified_attestation_seal = match attestation_seal_provenance_certificate::verify_live_package_attestation_seal_for_provenance_certificate(
        &config.attestation_seal_session_dir,
        config
            .confirmed_decision_packet_session_dir
            .as_deref()
            .ok_or_else(|| anyhow!("missing --confirm-decision-packet-session-dir for run mode"))?,
    ) {
        Ok(value) => value,
        Err(error) => {
            return Ok(failure_report_without_contract(
                config,
                TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificateRefusedNowByInvalidOrDriftedContract,
                format!(
                    "failed to verify attestation-seal session under {}: {error}",
                    config.attestation_seal_session_dir.display()
                ),
            ))
        }
    };

    validate_managed_surface_session_dir(
        &verified_attestation_seal.contract.install_root,
        session_dir,
    )
    .with_context(|| {
        format!(
            "provenance-certificate session dir {} overlaps the managed live target surface derived from verified attestation-seal install_root {}",
            session_dir.display(),
            verified_attestation_seal.contract.install_root.display()
        )
    })?;
    ensure_clean_provenance_certificate_session_dir(session_dir)?;
    let paths = provenance_certificate_paths(session_dir);
    let classification = classify_verified_attestation_seal(&verified_attestation_seal);
    let manifest_sha256 =
        canonical_sha256_hex(&verified_attestation_seal.release_capsule_digest_manifest)?;
    let manifest_entry_count = verified_attestation_seal
        .release_capsule_digest_manifest
        .entries
        .len();
    let chain_fingerprint_sha256 = compute_chain_fingerprint_sha256(
        &serialize_enum(&classification.0),
        verified_attestation_seal
            .contract
            .handoff_bundle_result
            .as_deref(),
        verified_attestation_seal
            .contract
            .decision_packet_result
            .as_deref(),
        verified_attestation_seal
            .contract
            .execute_frozen_result
            .as_deref(),
        verified_attestation_seal
            .contract
            .current_pre_activation_gate_verdict
            .as_deref(),
        verified_attestation_seal
            .contract
            .current_pre_activation_gate_reason
            .as_deref(),
        &verified_attestation_seal
            .contract
            .reviewed_frozen_live_cutover_controller_command_summary,
        &manifest_sha256,
        manifest_entry_count,
        &verified_attestation_seal
            .release_capsule_digest_manifest
            .digest_algorithm,
    )?;
    let session = planned_session(
        config,
        session_dir,
        &verified_attestation_seal.contract,
        classification.0,
        &manifest_sha256,
        manifest_entry_count,
        &chain_fingerprint_sha256,
    );
    persist_json(&paths.session_path, &session)?;
    persist_json(
        &paths.attestation_seal_report_path,
        &verified_attestation_seal.report_json,
    )?;
    persist_json(
        &paths.release_capsule_digest_manifest_path,
        &verified_attestation_seal.release_capsule_digest_manifest,
    )?;
    let attestation_seal_step = Some(step_artifact(
        &paths.attestation_seal_report_path,
        "verify_live_package_attestation_seal",
        &verified_attestation_seal.verdict,
        &verified_attestation_seal.reason,
        verified_attestation_seal.generated_at,
    ));
    let status = PackageProvenanceCertificateStatus {
        status_version: PROVENANCE_CERTIFICATE_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        attestation_seal_session_dir: config.attestation_seal_session_dir.display().to_string(),
        result: classification.0,
        reason: classification.2.clone(),
        handoff_bundle_result: verified_attestation_seal
            .contract
            .handoff_bundle_result
            .clone(),
        decision_packet_result: verified_attestation_seal
            .contract
            .decision_packet_result
            .clone(),
        execute_frozen_result: verified_attestation_seal
            .contract
            .execute_frozen_result
            .clone(),
        current_pre_activation_gate_verdict: verified_attestation_seal
            .contract
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: verified_attestation_seal
            .contract
            .current_pre_activation_gate_reason
            .clone(),
        attestation_seal_step: attestation_seal_step.clone(),
        attestation_seal_summary: verified_attestation_seal
            .contract
            .attestation_seal_summary
            .clone(),
        custody_record_summary: verified_attestation_seal
            .contract
            .custody_record_summary
            .clone(),
        release_capsule_digest_manifest_sha256: manifest_sha256.clone(),
        release_capsule_digest_manifest_entry_count: manifest_entry_count,
        release_capsule_digest_algorithm: verified_attestation_seal
            .release_capsule_digest_manifest
            .digest_algorithm
            .clone(),
        provenance_certificate_summary: provenance_certificate_summary(
            classification.0,
            &verified_attestation_seal
                .contract
                .reviewed_frozen_live_cutover_controller_command_summary,
        ),
        chain_fingerprint_summary: chain_fingerprint_summary(
            PROVENANCE_CERTIFICATE_CHAIN_FINGERPRINT_ALGORITHM,
            &chain_fingerprint_sha256,
            &manifest_sha256,
            manifest_entry_count,
        ),
        chain_fingerprint_sha256: chain_fingerprint_sha256.clone(),
        chain_fingerprint_algorithm: PROVENANCE_CERTIFICATE_CHAIN_FINGERPRINT_ALGORITHM.to_string(),
        explicit_statement: PROVENANCE_CERTIFICATE_STATUS_EXPLICIT_STATEMENT.to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    Ok(report_from_status(
        config,
        session_dir,
        &paths,
        &verified_attestation_seal.contract,
        classification.1,
        &status,
        verified_attestation_seal.release_capsule_digest_manifest,
    ))
}

fn verify_live_package_provenance_certificate_report(
    config: &Config,
) -> Result<PackageProvenanceCertificateReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = provenance_certificate_paths(session_dir);
    let session: PackageProvenanceCertificateSession = load_json(&paths.session_path)?;
    let status: PackageProvenanceCertificateStatus = load_json(&paths.status_path)?;
    let mut mismatches = Vec::new();

    compare_string(
        &session.attestation_seal_session_dir,
        &config.attestation_seal_session_dir.display().to_string(),
        "provenance-certificate attestation_seal_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "provenance-certificate session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "provenance-certificate status session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.attestation_seal_session_dir,
        &config.attestation_seal_session_dir.display().to_string(),
        "provenance-certificate status attestation_seal_session_dir",
        &mut mismatches,
    );

    let verified_attestation_seal = match attestation_seal_provenance_certificate::verify_live_package_attestation_seal_for_provenance_certificate(
        &config.attestation_seal_session_dir,
        config
            .confirmed_decision_packet_session_dir
            .as_deref()
            .ok_or_else(|| anyhow!("missing --confirm-decision-packet-session-dir for verify mode"))?,
    ) {
        Ok(value) => value,
        Err(error) => {
            let raw_contract = attestation_seal_provenance_certificate::load_live_package_attestation_seal_contract_for_provenance_certificate(
                &config.attestation_seal_session_dir,
            )?;
            return Ok(failure_report_for_verify(
                config,
                session_dir,
                &raw_contract,
                vec![format!(
                    "failed to verify attestation-seal session under {}: {error}",
                    config.attestation_seal_session_dir.display()
                )],
            ));
        }
    };

    let stored_attestation_seal_report: serde_json::Value = load_required_step_json(
        &status.attestation_seal_step,
        &paths.attestation_seal_report_path,
        "attestation_seal_step",
        &mut mismatches,
    )?;
    let stored_attestation_seal_report_view: NestedAttestationSealReportView =
        serde_json::from_value(stored_attestation_seal_report.clone()).with_context(|| {
            format!(
                "failed parsing archived nested attestation-seal report {}",
                paths.attestation_seal_report_path.display()
            )
        })?;
    let verified_attestation_seal_report_view: NestedAttestationSealReportView =
        serde_json::from_value(verified_attestation_seal.report_json.clone()).with_context(
            || {
                format!(
                    "failed parsing freshly verified nested attestation-seal report for {}",
                    config.attestation_seal_session_dir.display()
                )
            },
        )?;
    compare_json_ignoring_generated_at(
        &stored_attestation_seal_report,
        &verified_attestation_seal.report_json,
        "provenance-certificate archived attestation-seal report json",
        &mut mismatches,
    );
    compare_string(
        &stored_attestation_seal_report_view.mode,
        &verified_attestation_seal_report_view.mode,
        "provenance-certificate archived attestation-seal report mode",
        &mut mismatches,
    );
    compare_string(
        &stored_attestation_seal_report_view.verdict,
        &verified_attestation_seal_report_view.verdict,
        "provenance-certificate archived attestation-seal report verdict",
        &mut mismatches,
    );
    compare_string(
        &stored_attestation_seal_report_view.reason,
        &verified_attestation_seal_report_view.reason,
        "provenance-certificate archived attestation-seal report reason",
        &mut mismatches,
    );
    if stored_attestation_seal_report_view.generated_at
        != verified_attestation_seal_report_view.generated_at
    {
        mismatches.push(format!(
            "provenance-certificate archived attestation-seal report generated_at {:?} does not match freshly verified nested truth {:?}",
            stored_attestation_seal_report_view.generated_at,
            verified_attestation_seal_report_view.generated_at
        ));
    }
    let expected_attestation_seal_step = step_artifact(
        &paths.attestation_seal_report_path,
        &verified_attestation_seal_report_view.mode,
        &verified_attestation_seal_report_view.verdict,
        &verified_attestation_seal_report_view.reason,
        verified_attestation_seal_report_view.generated_at,
    );
    compare_step_artifact(
        status.attestation_seal_step.as_ref(),
        &expected_attestation_seal_step,
        "provenance-certificate attestation_seal_step",
        &mut mismatches,
    );
    if verified_attestation_seal.verdict != "tiny_live_package_attestation_seal_verify_ok" {
        mismatches.push(format!(
            "nested attestation-seal verification is non-green: {}",
            verified_attestation_seal.reason
        ));
    }

    let stored_manifest_bytes = fs::read(&paths.release_capsule_digest_manifest_path)
        .with_context(|| {
            format!(
                "failed reading {}",
                paths.release_capsule_digest_manifest_path.display()
            )
        })?;
    let stored_manifest: attestation_seal_provenance_certificate::ReleaseCapsuleDigestManifest =
        serde_json::from_slice(&stored_manifest_bytes).with_context(|| {
            format!(
                "failed parsing {}",
                paths.release_capsule_digest_manifest_path.display()
            )
        })?;
    if stored_manifest != verified_attestation_seal.release_capsule_digest_manifest {
        mismatches.push(
            "provenance-certificate archived release-capsule digest manifest does not match freshly verified nested truth"
                .to_string(),
        );
    }
    let expected_manifest_sha256 =
        canonical_sha256_hex(&verified_attestation_seal.release_capsule_digest_manifest)?;
    let stored_manifest_sha256 = sha256_hex_bytes(&stored_manifest_bytes);
    if stored_manifest_sha256 != expected_manifest_sha256 {
        mismatches.push(format!(
            "provenance-certificate archived release-capsule digest manifest sha256 {stored_manifest_sha256} does not match expected {expected_manifest_sha256}"
        ));
    }
    let expected_manifest_entry_count = verified_attestation_seal
        .release_capsule_digest_manifest
        .entries
        .len();

    compare_string(
        &session.release_capsule_session_dir,
        &verified_attestation_seal
            .contract
            .release_capsule_session_dir
            .display()
            .to_string(),
        "provenance-certificate release_capsule_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.activation_ticket_session_dir,
        &verified_attestation_seal
            .contract
            .activation_ticket_session_dir
            .display()
            .to_string(),
        "provenance-certificate activation_ticket_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.review_receipt_session_dir,
        &verified_attestation_seal
            .contract
            .review_receipt_session_dir
            .display()
            .to_string(),
        "provenance-certificate review_receipt_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.handoff_bundle_session_dir,
        &verified_attestation_seal
            .contract
            .handoff_bundle_session_dir
            .display()
            .to_string(),
        "provenance-certificate handoff_bundle_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.decision_packet_session_dir,
        &verified_attestation_seal
            .contract
            .decision_packet_session_dir
            .display()
            .to_string(),
        "provenance-certificate decision_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.execute_frozen_session_dir,
        &verified_attestation_seal
            .contract
            .execute_frozen_session_dir
            .display()
            .to_string(),
        "provenance-certificate execute_frozen_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.turn_green_session_dir,
        &verified_attestation_seal
            .contract
            .turn_green_session_dir
            .display()
            .to_string(),
        "provenance-certificate turn_green_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.launch_packet_session_dir,
        &verified_attestation_seal
            .contract
            .launch_packet_session_dir
            .display()
            .to_string(),
        "provenance-certificate launch_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.package_dir,
        &verified_attestation_seal
            .contract
            .package_dir
            .display()
            .to_string(),
        "provenance-certificate package_dir",
        &mut mismatches,
    );
    compare_string(
        &session.install_root,
        &verified_attestation_seal
            .contract
            .install_root
            .display()
            .to_string(),
        "provenance-certificate install_root",
        &mut mismatches,
    );
    compare_string(
        &session.target_service_name,
        &verified_attestation_seal.contract.target_service_name,
        "provenance-certificate target_service_name",
        &mut mismatches,
    );
    compare_string(
        &session.backend_command,
        &verified_attestation_seal.contract.backend_command,
        "provenance-certificate backend_command",
        &mut mismatches,
    );
    compare_u64(
        session.wrapper_timeout_ms,
        verified_attestation_seal.contract.wrapper_timeout_ms,
        "provenance-certificate wrapper_timeout_ms",
        &mut mismatches,
    );
    compare_u64(
        session.service_status_max_staleness_ms,
        verified_attestation_seal
            .contract
            .service_status_max_staleness_ms,
        "provenance-certificate service_status_max_staleness_ms",
        &mut mismatches,
    );

    let classification = classify_verified_attestation_seal(&verified_attestation_seal);
    let expected_result = serialize_enum(&classification.0);
    let expected_provenance_certificate_summary = provenance_certificate_summary(
        classification.0,
        &verified_attestation_seal
            .contract
            .reviewed_frozen_live_cutover_controller_command_summary,
    );
    let expected_chain_fingerprint_sha256 = compute_chain_fingerprint_sha256(
        &expected_result,
        verified_attestation_seal
            .contract
            .handoff_bundle_result
            .as_deref(),
        verified_attestation_seal
            .contract
            .decision_packet_result
            .as_deref(),
        verified_attestation_seal
            .contract
            .execute_frozen_result
            .as_deref(),
        verified_attestation_seal
            .contract
            .current_pre_activation_gate_verdict
            .as_deref(),
        verified_attestation_seal
            .contract
            .current_pre_activation_gate_reason
            .as_deref(),
        &verified_attestation_seal
            .contract
            .reviewed_frozen_live_cutover_controller_command_summary,
        &expected_manifest_sha256,
        expected_manifest_entry_count,
        &verified_attestation_seal
            .release_capsule_digest_manifest
            .digest_algorithm,
    )?;
    let expected_chain_fingerprint_summary = chain_fingerprint_summary(
        PROVENANCE_CERTIFICATE_CHAIN_FINGERPRINT_ALGORITHM,
        &expected_chain_fingerprint_sha256,
        &expected_manifest_sha256,
        expected_manifest_entry_count,
    );

    compare_string(
        &session.verify_attestation_seal_command_summary,
        &verify_attestation_seal_command_summary(&config.attestation_seal_session_dir),
        "provenance-certificate session verify_attestation_seal_command_summary",
        &mut mismatches,
    );
    compare_string(
        &session.reviewed_frozen_live_cutover_controller_command_summary,
        &verified_attestation_seal
            .contract
            .reviewed_frozen_live_cutover_controller_command_summary,
        "provenance-certificate session reviewed_frozen_live_cutover_controller_command_summary",
        &mut mismatches,
    );
    compare_string(
        &session.attestation_seal_summary,
        &verified_attestation_seal.contract.attestation_seal_summary,
        "provenance-certificate session attestation_seal_summary",
        &mut mismatches,
    );
    compare_string(
        &session.custody_record_summary,
        &verified_attestation_seal.contract.custody_record_summary,
        "provenance-certificate session custody_record_summary",
        &mut mismatches,
    );
    compare_string(
        &session.release_capsule_digest_manifest_sha256,
        &expected_manifest_sha256,
        "provenance-certificate session release_capsule_digest_manifest_sha256",
        &mut mismatches,
    );
    compare_usize(
        session.release_capsule_digest_manifest_entry_count,
        expected_manifest_entry_count,
        "provenance-certificate session release_capsule_digest_manifest_entry_count",
        &mut mismatches,
    );
    compare_string(
        &session.release_capsule_digest_algorithm,
        &verified_attestation_seal
            .release_capsule_digest_manifest
            .digest_algorithm,
        "provenance-certificate session release_capsule_digest_algorithm",
        &mut mismatches,
    );
    compare_string(
        &session.provenance_certificate_summary,
        &expected_provenance_certificate_summary,
        "provenance-certificate session provenance_certificate_summary",
        &mut mismatches,
    );
    compare_string(
        &session.chain_fingerprint_summary,
        &expected_chain_fingerprint_summary,
        "provenance-certificate session chain_fingerprint_summary",
        &mut mismatches,
    );
    compare_string(
        &session.chain_fingerprint_sha256,
        &expected_chain_fingerprint_sha256,
        "provenance-certificate session chain_fingerprint_sha256",
        &mut mismatches,
    );
    compare_string(
        &session.chain_fingerprint_algorithm,
        PROVENANCE_CERTIFICATE_CHAIN_FINGERPRINT_ALGORITHM,
        "provenance-certificate session chain_fingerprint_algorithm",
        &mut mismatches,
    );

    compare_string(
        &serialize_enum(&status.result),
        &expected_result,
        "provenance-certificate status result",
        &mut mismatches,
    );
    compare_optional_string(
        status.handoff_bundle_result.as_deref(),
        verified_attestation_seal
            .contract
            .handoff_bundle_result
            .as_deref(),
        "provenance-certificate status handoff_bundle_result",
        &mut mismatches,
    );
    compare_optional_string(
        status.decision_packet_result.as_deref(),
        verified_attestation_seal
            .contract
            .decision_packet_result
            .as_deref(),
        "provenance-certificate status decision_packet_result",
        &mut mismatches,
    );
    compare_optional_string(
        status.execute_frozen_result.as_deref(),
        verified_attestation_seal
            .contract
            .execute_frozen_result
            .as_deref(),
        "provenance-certificate status execute_frozen_result",
        &mut mismatches,
    );
    compare_optional_string(
        status.current_pre_activation_gate_verdict.as_deref(),
        verified_attestation_seal
            .contract
            .current_pre_activation_gate_verdict
            .as_deref(),
        "provenance-certificate status current_pre_activation_gate_verdict",
        &mut mismatches,
    );
    compare_optional_string(
        status.current_pre_activation_gate_reason.as_deref(),
        verified_attestation_seal
            .contract
            .current_pre_activation_gate_reason
            .as_deref(),
        "provenance-certificate status current_pre_activation_gate_reason",
        &mut mismatches,
    );
    compare_string(
        &status.attestation_seal_summary,
        &verified_attestation_seal.contract.attestation_seal_summary,
        "provenance-certificate status attestation_seal_summary",
        &mut mismatches,
    );
    compare_string(
        &status.custody_record_summary,
        &verified_attestation_seal.contract.custody_record_summary,
        "provenance-certificate status custody_record_summary",
        &mut mismatches,
    );
    compare_string(
        &status.release_capsule_digest_manifest_sha256,
        &expected_manifest_sha256,
        "provenance-certificate status release_capsule_digest_manifest_sha256",
        &mut mismatches,
    );
    compare_usize(
        status.release_capsule_digest_manifest_entry_count,
        expected_manifest_entry_count,
        "provenance-certificate status release_capsule_digest_manifest_entry_count",
        &mut mismatches,
    );
    compare_string(
        &status.release_capsule_digest_algorithm,
        &verified_attestation_seal
            .release_capsule_digest_manifest
            .digest_algorithm,
        "provenance-certificate status release_capsule_digest_algorithm",
        &mut mismatches,
    );
    compare_string(
        &status.provenance_certificate_summary,
        &expected_provenance_certificate_summary,
        "provenance-certificate status provenance_certificate_summary",
        &mut mismatches,
    );
    compare_string(
        &status.chain_fingerprint_summary,
        &expected_chain_fingerprint_summary,
        "provenance-certificate status chain_fingerprint_summary",
        &mut mismatches,
    );
    compare_string(
        &status.chain_fingerprint_sha256,
        &expected_chain_fingerprint_sha256,
        "provenance-certificate status chain_fingerprint_sha256",
        &mut mismatches,
    );
    compare_string(
        &status.chain_fingerprint_algorithm,
        PROVENANCE_CERTIFICATE_CHAIN_FINGERPRINT_ALGORITHM,
        "provenance-certificate status chain_fingerprint_algorithm",
        &mut mismatches,
    );

    if !mismatches.is_empty() {
        return Ok(failure_report_for_verify(
            config,
            session_dir,
            &verified_attestation_seal.contract,
            mismatches,
        ));
    }

    Ok(PackageProvenanceCertificateReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_provenance_certificate".to_string(),
        verdict:
            TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificateVerifyOk,
        reason: format!(
            "attestation-seal-native provenance certificate under {} is coherent and correctly binds one canonical chain fingerprint to verified attestation-seal truth",
            session_dir.display()
        ),
        attestation_seal_session_dir: config.attestation_seal_session_dir.display().to_string(),
        release_capsule_session_dir: Some(
            verified_attestation_seal
                .contract
                .release_capsule_session_dir
                .display()
                .to_string(),
        ),
        activation_ticket_session_dir: Some(
            verified_attestation_seal
                .contract
                .activation_ticket_session_dir
                .display()
                .to_string(),
        ),
        review_receipt_session_dir: Some(
            verified_attestation_seal
                .contract
                .review_receipt_session_dir
                .display()
                .to_string(),
        ),
        handoff_bundle_session_dir: Some(
            verified_attestation_seal
                .contract
                .handoff_bundle_session_dir
                .display()
                .to_string(),
        ),
        decision_packet_session_dir: Some(
            verified_attestation_seal
                .contract
                .decision_packet_session_dir
                .display()
                .to_string(),
        ),
        execute_frozen_session_dir: Some(
            verified_attestation_seal
                .contract
                .execute_frozen_session_dir
                .display()
                .to_string(),
        ),
        turn_green_session_dir: Some(
            verified_attestation_seal
                .contract
                .turn_green_session_dir
                .display()
                .to_string(),
        ),
        launch_packet_session_dir: Some(
            verified_attestation_seal
                .contract
                .launch_packet_session_dir
                .display()
                .to_string(),
        ),
        package_dir: Some(verified_attestation_seal.contract.package_dir.display().to_string()),
        install_root: Some(
            verified_attestation_seal
                .contract
                .install_root
                .display()
                .to_string(),
        ),
        target_service_name: Some(
            verified_attestation_seal
                .contract
                .target_service_name
                .clone(),
        ),
        backend_command: Some(verified_attestation_seal.contract.backend_command.clone()),
        wrapper_timeout_ms: Some(verified_attestation_seal.contract.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(
            verified_attestation_seal
                .contract
                .service_status_max_staleness_ms,
        ),
        session_dir: Some(session_dir.display().to_string()),
        provenance_certificate_session_path: Some(paths.session_path.display().to_string()),
        provenance_certificate_status_path: Some(paths.status_path.display().to_string()),
        archived_attestation_seal_report_path: Some(
            paths.attestation_seal_report_path.display().to_string(),
        ),
        archived_release_capsule_digest_manifest_path: Some(
            paths.release_capsule_digest_manifest_path
                .display()
                .to_string(),
        ),
        result: Some(expected_result),
        handoff_bundle_result: verified_attestation_seal.contract.handoff_bundle_result.clone(),
        decision_packet_result: verified_attestation_seal.contract.decision_packet_result.clone(),
        execute_frozen_result: verified_attestation_seal.contract.execute_frozen_result.clone(),
        attestation_seal_step: Some(expected_attestation_seal_step),
        verify_attestation_seal_command_summary: Some(verify_attestation_seal_command_summary(
            &config.attestation_seal_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            verified_attestation_seal
                .contract
                .reviewed_frozen_live_cutover_controller_command_summary,
        ),
        attestation_seal_summary: Some(
            verified_attestation_seal.contract.attestation_seal_summary.clone(),
        ),
        custody_record_summary: Some(
            verified_attestation_seal.contract.custody_record_summary.clone(),
        ),
        release_capsule_digest_manifest_sha256: Some(expected_manifest_sha256.clone()),
        release_capsule_digest_manifest_entry_count: Some(expected_manifest_entry_count),
        release_capsule_digest_algorithm: Some(
            verified_attestation_seal
                .release_capsule_digest_manifest
                .digest_algorithm
                .clone(),
        ),
        provenance_certificate_summary: Some(expected_provenance_certificate_summary),
        chain_fingerprint_summary: Some(expected_chain_fingerprint_summary),
        chain_fingerprint_sha256: Some(expected_chain_fingerprint_sha256),
        chain_fingerprint_algorithm: Some(
            PROVENANCE_CERTIFICATE_CHAIN_FINGERPRINT_ALGORITHM.to_string(),
        ),
        release_capsule_digest_manifest: Some(
            verified_attestation_seal.release_capsule_digest_manifest,
        ),
        current_pre_activation_gate_verdict: verified_attestation_seal
            .contract
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: verified_attestation_seal
            .contract
            .current_pre_activation_gate_reason
            .clone(),
        verification_mismatches: mismatches,
        explicit_statement: PROVENANCE_CERTIFICATE_VERIFY_EXPLICIT_STATEMENT.to_string(),
    })
}

fn verify_attestation_seal_command_summary(attestation_seal_session_dir: &Path) -> String {
    format!(
        "verify immutable attestation-seal session under {} before freezing the final provenance certificate",
        attestation_seal_session_dir.display()
    )
}

fn classify_provenance_contract(
    contract: &attestation_seal_provenance_certificate::LivePackageAttestationSealContractView,
    assume_verified: bool,
) -> (
    LivePackageProvenanceCertificateResult,
    TinyLivePackageProvenanceCertificateVerdict,
    String,
) {
    if !assume_verified {
        return (
            LivePackageProvenanceCertificateResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificateRefusedNowByInvalidOrDriftedContract,
            "the attestation-seal chain is not verified, so the provenance certificate remains refused".to_string(),
        );
    }
    match contract.result.as_deref() {
        Some("refused_now_by_stage3") => (
            LivePackageProvenanceCertificateResult::RefusedNowByStage3,
            TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificateRefusedNowByStage3,
            "the immutable attestation seal remains refused now by Stage 3, so this provenance certificate remains an explicit refusal record".to_string(),
        ),
        Some("refused_now_by_pre_activation_gate") => (
            LivePackageProvenanceCertificateResult::RefusedNowByPreActivationGate,
            TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificateRefusedNowByPreActivationGate,
            "the immutable attestation seal remains refused now by the pre-activation gate, so this provenance certificate remains an explicit refusal record".to_string(),
        ),
        Some("ready_for_manual_execution_when_gate_turns_green") => (
            LivePackageProvenanceCertificateResult::ReadyForManualExecutionWhenGateTurnsGreen,
            TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificateReadyForManualExecutionWhenGateTurnsGreen,
            "the immutable attestation seal is coherent and this provenance certificate is ready for manual execution when gate truth turns green".to_string(),
        ),
        _ => (
            LivePackageProvenanceCertificateResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificateRefusedNowByInvalidOrDriftedContract,
            "the verified attestation-seal chain is invalid, drifted, or non-runnable, so this provenance certificate remains refused".to_string(),
        ),
    }
}

fn classify_verified_attestation_seal(
    verified: &attestation_seal_provenance_certificate::VerifiedLivePackageAttestationSealProvenanceCertificateStep,
) -> (
    LivePackageProvenanceCertificateResult,
    TinyLivePackageProvenanceCertificateVerdict,
    String,
) {
    if verified.verdict != "tiny_live_package_attestation_seal_verify_ok" {
        return (
            LivePackageProvenanceCertificateResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificateRefusedNowByInvalidOrDriftedContract,
            "the immutable attestation seal did not verify cleanly, so this provenance certificate remains refused".to_string(),
        );
    }
    classify_provenance_contract(&verified.contract, true)
}

fn provenance_certificate_summary(
    result: LivePackageProvenanceCertificateResult,
    reviewed_frozen_live_cutover_controller_command_summary: &str,
) -> String {
    match result {
        LivePackageProvenanceCertificateResult::RefusedNowByStage3 => {
            "Provenance certificate outcome: the immutable reviewed chain remains refused now by Stage 3; archive this final certificate as provenance evidence only and do not run the frozen controller.".to_string()
        }
        LivePackageProvenanceCertificateResult::RefusedNowByPreActivationGate => {
            "Provenance certificate outcome: the immutable reviewed chain remains refused now by the pre-activation gate; archive this final certificate as provenance evidence only and do not run the frozen controller.".to_string()
        }
        LivePackageProvenanceCertificateResult::RefusedNowByInvalidOrDriftedContract => {
            "Provenance certificate outcome: the immutable reviewed chain is invalid or drifted; archive this final certificate for audit only and mint a new coherent provenance record before any later execution record.".to_string()
        }
        LivePackageProvenanceCertificateResult::ReadyForManualExecutionWhenGateTurnsGreen => format!(
            "Provenance certificate outcome: this immutable chain fingerprint is ready for manual execution when gate truth turns green; the exact reviewed frozen controller command remains: {reviewed_frozen_live_cutover_controller_command_summary}"
        ),
    }
}

fn chain_fingerprint_summary(
    algorithm: &str,
    chain_fingerprint_sha256: &str,
    manifest_sha256: &str,
    manifest_entry_count: usize,
) -> String {
    format!(
        "Chain fingerprint identity: {algorithm} {chain_fingerprint_sha256} over the verified refusal-vs-ready classification, the reviewed frozen controller summary, and the nested release-capsule digest manifest sha256 {manifest_sha256} across {manifest_entry_count} archival members; any identity drift invalidates this provenance certificate"
    )
}

fn provenance_certificate_paths(session_dir: &Path) -> PackageProvenanceCertificatePaths {
    PackageProvenanceCertificatePaths {
        session_path: session_dir
            .join("tiny_live_activation_package_provenance_certificate.session.json"),
        status_path: session_dir
            .join("tiny_live_activation_package_provenance_certificate.status.json"),
        attestation_seal_report_path: session_dir.join(
            "tiny_live_activation_package_provenance_certificate.attestation_seal.report.json",
        ),
        release_capsule_digest_manifest_path: session_dir.join(
            "tiny_live_activation_package_provenance_certificate.release_capsule.digest_manifest.json",
        ),
    }
}

fn ensure_clean_provenance_certificate_session_dir(session_dir: &Path) -> Result<()> {
    if session_dir.exists() {
        bail!(
            "refusing to overwrite existing provenance-certificate session dir {}",
            session_dir.display()
        );
    }
    fs::create_dir_all(session_dir).with_context(|| {
        format!(
            "failed creating provenance-certificate session dir {}",
            session_dir.display()
        )
    })
}

fn planned_session(
    config: &Config,
    session_dir: &Path,
    contract: &attestation_seal_provenance_certificate::LivePackageAttestationSealContractView,
    result: LivePackageProvenanceCertificateResult,
    manifest_sha256: &str,
    manifest_entry_count: usize,
    chain_fingerprint_sha256: &str,
) -> PackageProvenanceCertificateSession {
    PackageProvenanceCertificateSession {
        session_version: PROVENANCE_CERTIFICATE_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        attestation_seal_session_dir: config.attestation_seal_session_dir.display().to_string(),
        release_capsule_session_dir: contract.release_capsule_session_dir.display().to_string(),
        activation_ticket_session_dir: contract.activation_ticket_session_dir.display().to_string(),
        review_receipt_session_dir: contract.review_receipt_session_dir.display().to_string(),
        handoff_bundle_session_dir: contract.handoff_bundle_session_dir.display().to_string(),
        decision_packet_session_dir: contract.decision_packet_session_dir.display().to_string(),
        execute_frozen_session_dir: contract.execute_frozen_session_dir.display().to_string(),
        turn_green_session_dir: contract.turn_green_session_dir.display().to_string(),
        launch_packet_session_dir: contract.launch_packet_session_dir.display().to_string(),
        package_dir: contract.package_dir.display().to_string(),
        install_root: contract.install_root.display().to_string(),
        target_service_name: contract.target_service_name.clone(),
        backend_command: contract.backend_command.clone(),
        wrapper_timeout_ms: contract.wrapper_timeout_ms,
        service_status_max_staleness_ms: contract.service_status_max_staleness_ms,
        session_dir: session_dir.display().to_string(),
        verify_attestation_seal_command_summary: verify_attestation_seal_command_summary(
            &config.attestation_seal_session_dir,
        ),
        reviewed_frozen_live_cutover_controller_command_summary: contract
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        attestation_seal_summary: contract.attestation_seal_summary.clone(),
        custody_record_summary: contract.custody_record_summary.clone(),
        release_capsule_digest_manifest_sha256: manifest_sha256.to_string(),
        release_capsule_digest_manifest_entry_count: manifest_entry_count,
        release_capsule_digest_algorithm: contract.release_capsule_digest_algorithm.clone(),
        provenance_certificate_summary: provenance_certificate_summary(
            result,
            &contract.reviewed_frozen_live_cutover_controller_command_summary,
        ),
        chain_fingerprint_summary: chain_fingerprint_summary(
            PROVENANCE_CERTIFICATE_CHAIN_FINGERPRINT_ALGORITHM,
            chain_fingerprint_sha256,
            manifest_sha256,
            manifest_entry_count,
        ),
        chain_fingerprint_sha256: chain_fingerprint_sha256.to_string(),
        chain_fingerprint_algorithm: PROVENANCE_CERTIFICATE_CHAIN_FINGERPRINT_ALGORITHM.to_string(),
        explicit_statement: PROVENANCE_CERTIFICATE_SESSION_EXPLICIT_STATEMENT.to_string(),
    }
}

fn report_from_status(
    config: &Config,
    session_dir: &Path,
    paths: &PackageProvenanceCertificatePaths,
    contract: &attestation_seal_provenance_certificate::LivePackageAttestationSealContractView,
    verdict: TinyLivePackageProvenanceCertificateVerdict,
    status: &PackageProvenanceCertificateStatus,
    release_capsule_digest_manifest:
        attestation_seal_provenance_certificate::ReleaseCapsuleDigestManifest,
) -> PackageProvenanceCertificateReport {
    PackageProvenanceCertificateReport {
        generated_at: Utc::now(),
        mode: "run_live_package_provenance_certificate".to_string(),
        verdict,
        reason: status.reason.clone(),
        attestation_seal_session_dir: config.attestation_seal_session_dir.display().to_string(),
        release_capsule_session_dir: Some(
            contract.release_capsule_session_dir.display().to_string(),
        ),
        activation_ticket_session_dir: Some(
            contract.activation_ticket_session_dir.display().to_string(),
        ),
        review_receipt_session_dir: Some(contract.review_receipt_session_dir.display().to_string()),
        handoff_bundle_session_dir: Some(contract.handoff_bundle_session_dir.display().to_string()),
        decision_packet_session_dir: Some(
            contract.decision_packet_session_dir.display().to_string(),
        ),
        execute_frozen_session_dir: Some(contract.execute_frozen_session_dir.display().to_string()),
        turn_green_session_dir: Some(contract.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: Some(contract.launch_packet_session_dir.display().to_string()),
        package_dir: Some(contract.package_dir.display().to_string()),
        install_root: Some(contract.install_root.display().to_string()),
        target_service_name: Some(contract.target_service_name.clone()),
        backend_command: Some(contract.backend_command.clone()),
        wrapper_timeout_ms: Some(contract.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(contract.service_status_max_staleness_ms),
        session_dir: Some(session_dir.display().to_string()),
        provenance_certificate_session_path: Some(paths.session_path.display().to_string()),
        provenance_certificate_status_path: Some(paths.status_path.display().to_string()),
        archived_attestation_seal_report_path: Some(
            paths.attestation_seal_report_path.display().to_string(),
        ),
        archived_release_capsule_digest_manifest_path: Some(
            paths
                .release_capsule_digest_manifest_path
                .display()
                .to_string(),
        ),
        result: Some(serialize_enum(&status.result)),
        handoff_bundle_result: status.handoff_bundle_result.clone(),
        decision_packet_result: status.decision_packet_result.clone(),
        execute_frozen_result: status.execute_frozen_result.clone(),
        attestation_seal_step: status.attestation_seal_step.clone(),
        verify_attestation_seal_command_summary: Some(verify_attestation_seal_command_summary(
            &config.attestation_seal_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            contract
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        attestation_seal_summary: Some(status.attestation_seal_summary.clone()),
        custody_record_summary: Some(status.custody_record_summary.clone()),
        release_capsule_digest_manifest_sha256: Some(
            status.release_capsule_digest_manifest_sha256.clone(),
        ),
        release_capsule_digest_manifest_entry_count: Some(
            status.release_capsule_digest_manifest_entry_count,
        ),
        release_capsule_digest_algorithm: Some(status.release_capsule_digest_algorithm.clone()),
        provenance_certificate_summary: Some(status.provenance_certificate_summary.clone()),
        chain_fingerprint_summary: Some(status.chain_fingerprint_summary.clone()),
        chain_fingerprint_sha256: Some(status.chain_fingerprint_sha256.clone()),
        chain_fingerprint_algorithm: Some(status.chain_fingerprint_algorithm.clone()),
        release_capsule_digest_manifest: Some(release_capsule_digest_manifest),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        verification_mismatches: Vec::new(),
        explicit_statement: status.explicit_statement.clone(),
    }
}

fn failure_report_without_contract(
    config: &Config,
    verdict: TinyLivePackageProvenanceCertificateVerdict,
    reason: String,
) -> PackageProvenanceCertificateReport {
    PackageProvenanceCertificateReport {
        generated_at: Utc::now(),
        mode: match config.mode {
            Mode::PlanLivePackageProvenanceCertificate => {
                "plan_live_package_provenance_certificate".to_string()
            }
            Mode::RenderLivePackageProvenanceCertificate => {
                "render_live_package_provenance_certificate".to_string()
            }
            Mode::RunLivePackageProvenanceCertificate => {
                "run_live_package_provenance_certificate".to_string()
            }
            Mode::VerifyLivePackageProvenanceCertificate => {
                "verify_live_package_provenance_certificate".to_string()
            }
        },
        verdict,
        reason,
        attestation_seal_session_dir: config.attestation_seal_session_dir.display().to_string(),
        release_capsule_session_dir: None,
        activation_ticket_session_dir: None,
        review_receipt_session_dir: None,
        handoff_bundle_session_dir: None,
        decision_packet_session_dir: None,
        execute_frozen_session_dir: None,
        turn_green_session_dir: None,
        launch_packet_session_dir: None,
        package_dir: None,
        install_root: None,
        target_service_name: None,
        backend_command: None,
        wrapper_timeout_ms: None,
        service_status_max_staleness_ms: None,
        session_dir: config
            .session_dir
            .as_ref()
            .map(|value| value.display().to_string()),
        provenance_certificate_session_path: None,
        provenance_certificate_status_path: None,
        archived_attestation_seal_report_path: None,
        archived_release_capsule_digest_manifest_path: None,
        result: Some("refused_now_by_invalid_or_drifted_contract".to_string()),
        handoff_bundle_result: None,
        decision_packet_result: None,
        execute_frozen_result: None,
        attestation_seal_step: None,
        verify_attestation_seal_command_summary: Some(verify_attestation_seal_command_summary(
            &config.attestation_seal_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: None,
        attestation_seal_summary: None,
        custody_record_summary: None,
        release_capsule_digest_manifest_sha256: None,
        release_capsule_digest_manifest_entry_count: None,
        release_capsule_digest_algorithm: None,
        provenance_certificate_summary: None,
        chain_fingerprint_summary: None,
        chain_fingerprint_sha256: None,
        chain_fingerprint_algorithm: None,
        release_capsule_digest_manifest: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this immutable provenance certificate refused before persisting because the attestation-seal artifact could not be verified coherently"
                .to_string(),
    }
}

fn failure_report_for_verify(
    config: &Config,
    session_dir: &Path,
    contract: &attestation_seal_provenance_certificate::LivePackageAttestationSealContractView,
    mismatches: Vec<String>,
) -> PackageProvenanceCertificateReport {
    let paths = provenance_certificate_paths(session_dir);
    let reason = mismatches.first().cloned().unwrap_or_else(|| {
        "attestation-seal-native provenance certificate verification failed".to_string()
    });
    PackageProvenanceCertificateReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_provenance_certificate".to_string(),
        verdict:
            TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificateVerifyInvalid,
        reason,
        attestation_seal_session_dir: config.attestation_seal_session_dir.display().to_string(),
        release_capsule_session_dir: Some(contract.release_capsule_session_dir.display().to_string()),
        activation_ticket_session_dir: Some(contract.activation_ticket_session_dir.display().to_string()),
        review_receipt_session_dir: Some(contract.review_receipt_session_dir.display().to_string()),
        handoff_bundle_session_dir: Some(contract.handoff_bundle_session_dir.display().to_string()),
        decision_packet_session_dir: Some(contract.decision_packet_session_dir.display().to_string()),
        execute_frozen_session_dir: Some(contract.execute_frozen_session_dir.display().to_string()),
        turn_green_session_dir: Some(contract.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: Some(contract.launch_packet_session_dir.display().to_string()),
        package_dir: Some(contract.package_dir.display().to_string()),
        install_root: Some(contract.install_root.display().to_string()),
        target_service_name: Some(contract.target_service_name.clone()),
        backend_command: Some(contract.backend_command.clone()),
        wrapper_timeout_ms: Some(contract.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(contract.service_status_max_staleness_ms),
        session_dir: Some(session_dir.display().to_string()),
        provenance_certificate_session_path: Some(paths.session_path.display().to_string()),
        provenance_certificate_status_path: Some(paths.status_path.display().to_string()),
        archived_attestation_seal_report_path: Some(
            paths.attestation_seal_report_path.display().to_string(),
        ),
        archived_release_capsule_digest_manifest_path: Some(
            paths.release_capsule_digest_manifest_path
                .display()
                .to_string(),
        ),
        result: None,
        handoff_bundle_result: contract.handoff_bundle_result.clone(),
        decision_packet_result: contract.decision_packet_result.clone(),
        execute_frozen_result: contract.execute_frozen_result.clone(),
        attestation_seal_step: None,
        verify_attestation_seal_command_summary: Some(verify_attestation_seal_command_summary(
            &config.attestation_seal_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            contract
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        attestation_seal_summary: Some(contract.attestation_seal_summary.clone()),
        custody_record_summary: Some(contract.custody_record_summary.clone()),
        release_capsule_digest_manifest_sha256: Some(
            contract.release_capsule_digest_manifest_sha256.clone(),
        ),
        release_capsule_digest_manifest_entry_count: Some(
            contract.release_capsule_digest_manifest_entry_count,
        ),
        release_capsule_digest_algorithm: Some(
            contract.release_capsule_digest_algorithm.clone(),
        ),
        provenance_certificate_summary: None,
        chain_fingerprint_summary: None,
        chain_fingerprint_sha256: None,
        chain_fingerprint_algorithm: Some(
            PROVENANCE_CERTIFICATE_CHAIN_FINGERPRINT_ALGORITHM.to_string(),
        ),
        release_capsule_digest_manifest: None,
        current_pre_activation_gate_verdict: contract.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: contract.current_pre_activation_gate_reason.clone(),
        verification_mismatches: mismatches,
        explicit_statement:
            "this verification failure means the immutable provenance certificate no longer proves the exact reviewed frozen controller context or the canonical nested chain identity"
                .to_string(),
    }
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    value.ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn set_mode(mode: &mut Option<Mode>, value: Mode, flag: &str) -> Result<()> {
    if mode.replace(value).is_some() {
        bail!("duplicate mode flag provided: {flag}");
    }
    Ok(())
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "<serialization-error>".to_string())
}

fn persist_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(value)?;
    fs::write(path, bytes).with_context(|| format!("failed writing {}", path.display()))
}

fn load_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let bytes = fs::read(path).with_context(|| format!("failed reading {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("failed parsing {}", path.display()))
}

fn load_required_step_json(
    step: &Option<PackageProvenanceCertificateStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<serde_json::Value> {
    let Some(step) = step else {
        mismatches.push(format!(
            "{label} is missing from provenance-certificate status"
        ));
        return load_json(expected_path);
    };
    compare_string(
        &step.path,
        &expected_path.display().to_string(),
        &format!("{label} path"),
        mismatches,
    );
    load_json(expected_path)
}

fn step_artifact(
    path: &Path,
    mode: &str,
    verdict: &str,
    reason: &str,
    generated_at: DateTime<Utc>,
) -> PackageProvenanceCertificateStepArtifact {
    PackageProvenanceCertificateStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn compare_string(actual: &str, expected: &str, label: &str, mismatches: &mut Vec<String>) {
    if actual != expected {
        mismatches.push(format!(
            "{label} {actual:?} does not match expected {expected:?}"
        ));
    }
}

fn compare_optional_string(
    actual: Option<&str>,
    expected: Option<&str>,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    if actual != expected {
        mismatches.push(format!(
            "{label} {actual:?} does not match expected {expected:?}"
        ));
    }
}

fn compare_u64(actual: u64, expected: u64, label: &str, mismatches: &mut Vec<String>) {
    if actual != expected {
        mismatches.push(format!(
            "{label} {actual} does not match expected {expected}"
        ));
    }
}

fn compare_usize(actual: usize, expected: usize, label: &str, mismatches: &mut Vec<String>) {
    if actual != expected {
        mismatches.push(format!(
            "{label} {actual} does not match expected {expected}"
        ));
    }
}

fn compare_json_ignoring_generated_at(
    actual: &serde_json::Value,
    expected: &serde_json::Value,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    let normalized_actual = normalized_report_json_without_generated_at(actual.clone());
    let normalized_expected = normalized_report_json_without_generated_at(expected.clone());
    if normalized_actual != normalized_expected {
        mismatches.push(format!(
            "{label} does not match freshly verified nested truth"
        ));
    }
}

fn normalized_report_json_without_generated_at(mut value: serde_json::Value) -> serde_json::Value {
    if let Some(object) = value.as_object_mut() {
        object.remove("generated_at");
    }
    value
}

fn compare_step_artifact(
    actual: Option<&PackageProvenanceCertificateStepArtifact>,
    expected: &PackageProvenanceCertificateStepArtifact,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    let Some(actual) = actual else {
        mismatches.push(format!(
            "{label} is missing from provenance-certificate status"
        ));
        return;
    };
    compare_string(
        &actual.path,
        &expected.path,
        &format!("{label} path"),
        mismatches,
    );
    compare_string(
        &actual.mode,
        &expected.mode,
        &format!("{label} mode"),
        mismatches,
    );
    compare_string(
        &actual.verdict,
        &expected.verdict,
        &format!("{label} verdict"),
        mismatches,
    );
    compare_string(
        &actual.reason,
        &expected.reason,
        &format!("{label} reason"),
        mismatches,
    );
    if actual.generated_at != expected.generated_at {
        mismatches.push(format!(
            "{label} generated_at {:?} does not match expected {:?}",
            actual.generated_at, expected.generated_at
        ));
    }
}

fn compute_chain_fingerprint_sha256(
    result: &str,
    handoff_bundle_result: Option<&str>,
    decision_packet_result: Option<&str>,
    execute_frozen_result: Option<&str>,
    current_pre_activation_gate_verdict: Option<&str>,
    current_pre_activation_gate_reason: Option<&str>,
    reviewed_frozen_live_cutover_controller_command_summary: &str,
    release_capsule_digest_manifest_sha256: &str,
    release_capsule_digest_manifest_entry_count: usize,
    release_capsule_digest_algorithm: &str,
) -> Result<String> {
    canonical_sha256_hex(&ChainFingerprintPayload {
        result,
        handoff_bundle_result,
        decision_packet_result,
        execute_frozen_result,
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        reviewed_frozen_live_cutover_controller_command_summary,
        release_capsule_digest_manifest_sha256,
        release_capsule_digest_manifest_entry_count,
        release_capsule_digest_algorithm,
    })
}

fn canonical_sha256_hex<T: Serialize>(value: &T) -> Result<String> {
    Ok(sha256_hex_bytes(&serde_json::to_vec_pretty(value)?))
}

fn sha256_hex_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("{digest:x}")
}

fn write_script(path: &Path, contents: &str) -> Result<()> {
    fs::write(path, contents).with_context(|| {
        format!(
            "failed writing provenance-certificate script to {}",
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

fn shell_escape_path(path: &Path) -> String {
    let value = path.display().to_string();
    if value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || b"/._-".contains(&byte))
    {
        value
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

fn render_report_lines(report: &PackageProvenanceCertificateReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "attestation_seal_session_dir={}",
            report.attestation_seal_session_dir
        ),
    ];
    if let Some(value) = &report.result {
        lines.push(format!("result={value}"));
    }
    if let Some(value) = &report.provenance_certificate_summary {
        lines.push(format!("provenance_certificate_summary={value}"));
    }
    if let Some(value) = &report.chain_fingerprint_summary {
        lines.push(format!("chain_fingerprint_summary={value}"));
    }
    if let Some(value) = &report.chain_fingerprint_sha256 {
        lines.push(format!("chain_fingerprint_sha256={value}"));
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
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn run_ready_for_manual_execution_when_gate_turns_green_then_verify_stays_green() {
        let fixture = fake_command_fixture(
            "provenance_certificate_ready",
            attestation_seal_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_provenance_certificate_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificateReadyForManualExecutionWhenGateTurnsGreen,
            "{run:#?}"
        );
        assert_eq!(
            run.result.as_deref(),
            Some("ready_for_manual_execution_when_gate_turns_green")
        );

        let verify =
            verify_live_package_provenance_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificateVerifyOk,
            "{:#?}",
            verify.verification_mismatches
        );
    }

    #[test]
    fn stage3_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "provenance_certificate_stage3",
            attestation_seal_verify_report(
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "blocked",
                "stage3 blocked",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_provenance_certificate_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificateRefusedNowByStage3
        );
    }

    #[test]
    fn pre_activation_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "provenance_certificate_pre_activation",
            attestation_seal_verify_report(
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
                "blocked",
                "pre-activation blocked",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_provenance_certificate_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificateRefusedNowByPreActivationGate
        );
    }

    #[test]
    fn drifted_attestation_seal_contract_is_refused() {
        let fixture = fake_command_fixture(
            "provenance_certificate_drifted",
            attestation_seal_verify_report(
                "refused_now_by_invalid_or_drifted_contract",
                "refused_now_by_invalid_or_drifted_contract",
                "refused_now_by_invalid_or_drifted_contract",
                "refused_now_by_invalid_or_drifted_contract",
                "green",
                "contract drifted",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_provenance_certificate_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificateRefusedNowByInvalidOrDriftedContract
        );
    }

    #[test]
    fn run_refuses_managed_surface_overlap_before_writing_any_artifacts() {
        let fixture = fake_command_fixture(
            "provenance_certificate_overlap",
            attestation_seal_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let managed_surface =
            copybot_app::tiny_live_activation::install_target_managed_surface::derive_install_target_managed_surface_paths(
                &fixture.install_root,
            )
            .unwrap();
        let unsafe_session_dir = managed_surface
            .session_dir
            .join("provenance-certificate-session");
        let config = Config {
            mode: Mode::RunLivePackageProvenanceCertificate,
            attestation_seal_session_dir: fixture.attestation_seal_session_dir.clone(),
            confirmed_decision_packet_session_dir: Some(
                fixture.decision_packet_session_dir.clone(),
            ),
            session_dir: Some(unsafe_session_dir.clone()),
            output_path: None,
            json: true,
        };

        let error = run_live_package_provenance_certificate_report(&config).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("overlaps the managed live target surface"),
            "{error:#}"
        );
        let paths = provenance_certificate_paths(&unsafe_session_dir);
        assert!(!paths.session_path.exists());
        assert!(!paths.status_path.exists());
        assert!(!paths.release_capsule_digest_manifest_path.exists());
    }

    #[test]
    fn verify_rejects_tampered_attestation_seal_step_path() {
        let fixture = fake_command_fixture(
            "provenance_certificate_tampered_step",
            attestation_seal_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_provenance_certificate_report(&fixture.run_config()).unwrap();
        let paths = provenance_certificate_paths(&fixture.provenance_certificate_session_dir);
        let mut status: PackageProvenanceCertificateStatus = load_json(&paths.status_path).unwrap();
        let foreign = fixture
            .fixture_dir
            .join("foreign.attestation-seal.report.json");
        fs::write(&foreign, "{}").unwrap();
        status.attestation_seal_step.as_mut().unwrap().path = foreign.display().to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify =
            verify_live_package_provenance_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificateVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_provenance_certificate_text() {
        let fixture = fake_command_fixture(
            "provenance_certificate_tampered_text",
            attestation_seal_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_provenance_certificate_report(&fixture.run_config()).unwrap();
        let paths = provenance_certificate_paths(&fixture.provenance_certificate_session_dir);
        let mut session: PackageProvenanceCertificateSession =
            load_json(&paths.session_path).unwrap();
        session.provenance_certificate_summary = "tampered summary".to_string();
        persist_json(&paths.session_path, &session).unwrap();

        let verify =
            verify_live_package_provenance_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificateVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_status_result() {
        let fixture = fake_command_fixture(
            "provenance_certificate_tampered_status_result",
            attestation_seal_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_provenance_certificate_report(&fixture.run_config()).unwrap();
        let paths = provenance_certificate_paths(&fixture.provenance_certificate_session_dir);
        let mut status: PackageProvenanceCertificateStatus = load_json(&paths.status_path).unwrap();
        status.result = LivePackageProvenanceCertificateResult::RefusedNowByStage3;
        persist_json(&paths.status_path, &status).unwrap();

        let verify =
            verify_live_package_provenance_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificateVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_status_gate_fields() {
        let fixture = fake_command_fixture(
            "provenance_certificate_tampered_status_gate",
            attestation_seal_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_provenance_certificate_report(&fixture.run_config()).unwrap();
        let paths = provenance_certificate_paths(&fixture.provenance_certificate_session_dir);
        let mut status: PackageProvenanceCertificateStatus = load_json(&paths.status_path).unwrap();
        status.current_pre_activation_gate_verdict = Some("tampered-gate".to_string());
        status.current_pre_activation_gate_reason = Some("tampered gate reason".to_string());
        persist_json(&paths.status_path, &status).unwrap();

        let verify =
            verify_live_package_provenance_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificateVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_status_handoff_bundle_result() {
        let fixture = fake_command_fixture(
            "provenance_certificate_tampered_status_handoff_bundle",
            attestation_seal_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_provenance_certificate_report(&fixture.run_config()).unwrap();
        let paths = provenance_certificate_paths(&fixture.provenance_certificate_session_dir);
        let mut status: PackageProvenanceCertificateStatus = load_json(&paths.status_path).unwrap();
        status.handoff_bundle_result = Some("tampered-handoff-bundle-result".to_string());
        persist_json(&paths.status_path, &status).unwrap();

        let verify =
            verify_live_package_provenance_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificateVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_nested_attestation_seal_report_content() {
        let fixture = fake_command_fixture(
            "provenance_certificate_tampered_nested_report",
            attestation_seal_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_provenance_certificate_report(&fixture.run_config()).unwrap();
        let paths = provenance_certificate_paths(&fixture.provenance_certificate_session_dir);
        let mut report: serde_json::Value = load_json(&paths.attestation_seal_report_path).unwrap();
        report.as_object_mut().unwrap().insert(
            "reason".to_string(),
            json!("tampered nested attestation seal reason"),
        );
        persist_json(&paths.attestation_seal_report_path, &report).unwrap();

        let verify =
            verify_live_package_provenance_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificateVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_retimed_nested_attestation_seal_report_and_step_generated_at() {
        let fixture = fake_command_fixture(
            "provenance_certificate_retimed_nested_report",
            attestation_seal_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_provenance_certificate_report(&fixture.run_config()).unwrap();
        let paths = provenance_certificate_paths(&fixture.provenance_certificate_session_dir);
        let mut report: serde_json::Value = load_json(&paths.attestation_seal_report_path).unwrap();
        let archived_generated_at =
            DateTime::parse_from_rfc3339(report["generated_at"].as_str().unwrap())
                .unwrap()
                .with_timezone(&Utc);
        let forged_generated_at = archived_generated_at + chrono::Duration::seconds(61);
        report.as_object_mut().unwrap().insert(
            "generated_at".to_string(),
            json!(forged_generated_at.to_rfc3339()),
        );
        persist_json(&paths.attestation_seal_report_path, &report).unwrap();

        let mut status: PackageProvenanceCertificateStatus = load_json(&paths.status_path).unwrap();
        status.attestation_seal_step.as_mut().unwrap().generated_at = forged_generated_at;
        persist_json(&paths.status_path, &status).unwrap();

        let verify =
            verify_live_package_provenance_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificateVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_chain_fingerprint_identity_fields() {
        let fixture = fake_command_fixture(
            "provenance_certificate_tampered_chain_fingerprint",
            attestation_seal_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_provenance_certificate_report(&fixture.run_config()).unwrap();
        let paths = provenance_certificate_paths(&fixture.provenance_certificate_session_dir);
        let mut status: PackageProvenanceCertificateStatus = load_json(&paths.status_path).unwrap();
        status.chain_fingerprint_sha256 = "deadbeef".to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify =
            verify_live_package_provenance_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageProvenanceCertificateVerdict::TinyLivePackageProvenanceCertificateVerifyInvalid
        );
    }

    struct FakeCommandFixture {
        fixture_dir: PathBuf,
        bin_dir: PathBuf,
        attestation_seal_session_dir: PathBuf,
        provenance_certificate_session_dir: PathBuf,
        decision_packet_session_dir: PathBuf,
        install_root: PathBuf,
    }

    impl FakeCommandFixture {
        fn run_config(&self) -> Config {
            Config {
                mode: Mode::RunLivePackageProvenanceCertificate,
                attestation_seal_session_dir: self.attestation_seal_session_dir.clone(),
                confirmed_decision_packet_session_dir: Some(
                    self.decision_packet_session_dir.clone(),
                ),
                session_dir: Some(self.provenance_certificate_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn verify_config(&self) -> Config {
            Config {
                mode: Mode::VerifyLivePackageProvenanceCertificate,
                attestation_seal_session_dir: self.attestation_seal_session_dir.clone(),
                confirmed_decision_packet_session_dir: Some(
                    self.decision_packet_session_dir.clone(),
                ),
                session_dir: Some(self.provenance_certificate_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }
    }

    fn fake_command_fixture(label: &str, verify_report: serde_json::Value) -> FakeCommandFixture {
        let fixture_dir = temp_dir(label);
        let bin_dir = fixture_dir.join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        let attestation_seal_session_dir = fixture_dir.join("attestation-seal-session");
        let provenance_certificate_session_dir = fixture_dir.join("provenance-certificate-session");
        let release_capsule_session_dir = fixture_dir.join("release-capsule-session");
        let activation_ticket_session_dir = fixture_dir.join("activation-ticket-session");
        let review_receipt_session_dir = fixture_dir.join("review-receipt-session");
        let handoff_bundle_session_dir = fixture_dir.join("handoff-bundle-session");
        let decision_packet_session_dir = fixture_dir.join("decision-packet-session");
        let execute_frozen_session_dir = fixture_dir.join("execute-frozen-session");
        let turn_green_session_dir = fixture_dir.join("turn-green-session");
        let launch_packet_session_dir = fixture_dir.join("launch-packet-session");
        let package_dir = fixture_dir.join("package");
        let install_root = fixture_dir.join("live-root");
        let backend_command = fixture_dir.join("fake-backend");
        fs::create_dir_all(&attestation_seal_session_dir).unwrap();
        fs::create_dir_all(&release_capsule_session_dir).unwrap();
        fs::create_dir_all(&activation_ticket_session_dir).unwrap();
        fs::create_dir_all(&review_receipt_session_dir).unwrap();
        fs::create_dir_all(&handoff_bundle_session_dir).unwrap();
        fs::create_dir_all(&decision_packet_session_dir).unwrap();
        fs::create_dir_all(&execute_frozen_session_dir).unwrap();
        fs::create_dir_all(&turn_green_session_dir).unwrap();
        fs::create_dir_all(&launch_packet_session_dir).unwrap();
        fs::create_dir_all(&package_dir).unwrap();
        fs::create_dir_all(&install_root).unwrap();

        persist_json(
            &attestation_seal_session_dir
                .join("tiny_live_activation_package_attestation_seal.session.json"),
            &json!({
                "release_capsule_session_dir": release_capsule_session_dir.display().to_string(),
                "activation_ticket_session_dir": activation_ticket_session_dir.display().to_string(),
                "review_receipt_session_dir": review_receipt_session_dir.display().to_string(),
                "handoff_bundle_session_dir": handoff_bundle_session_dir.display().to_string(),
                "decision_packet_session_dir": decision_packet_session_dir.display().to_string(),
                "execute_frozen_session_dir": execute_frozen_session_dir.display().to_string(),
                "turn_green_session_dir": turn_green_session_dir.display().to_string(),
                "launch_packet_session_dir": launch_packet_session_dir.display().to_string(),
                "package_dir": package_dir.display().to_string(),
                "install_root": install_root.display().to_string(),
                "target_service_name": "solana-copy-bot.service",
                "backend_command": backend_command.display().to_string(),
                "wrapper_timeout_ms": 1200,
                "service_status_max_staleness_ms": 4500,
                "verify_release_capsule_command_summary": format!(
                    "verify immutable release-capsule session under {} before freezing the final attestation seal",
                    release_capsule_session_dir.display()
                ),
                "reviewed_frozen_live_cutover_controller_command_summary": format!(
                    "copybot_tiny_live_activation_package_live_cutover --package-dir {} --install-root {} --target-service solana-copy-bot.service --backend-command {} --wrapper-timeout-ms 1200 --service-status-max-staleness-ms 4500 --session-dir {} --run-live-package-cutover",
                    shell_escape_path(&package_dir),
                    shell_escape_path(&install_root),
                    shell_escape_path(&backend_command),
                    shell_escape_path(&fixture_dir.join("frozen-live-cutover-session")),
                ),
                "release_capsule_summary": "Release capsule summary.",
                "release_capsule_audit_manifest_summary": "Hash-locked audit manifest: sha256 digests over 3 archival members; any content drift invalidates this release capsule",
                "release_capsule_digest_manifest_sha256": "placeholder-manifest-sha",
                "release_capsule_digest_manifest_entry_count": 3,
                "release_capsule_digest_algorithm": "sha256",
                "attestation_seal_summary": "Attestation seal summary.",
                "custody_record_summary": "Custody record identity.",
                "explicit_statement": "attestation-seal statement"
            }),
        )
        .unwrap();
        persist_json(
            &attestation_seal_session_dir
                .join("tiny_live_activation_package_attestation_seal.status.json"),
            &json!({
                "result": verify_report.get("result").and_then(|value| value.as_str()).unwrap(),
                "handoff_bundle_result": verify_report.get("handoff_bundle_result").and_then(|value| value.as_str()).unwrap(),
                "decision_packet_result": verify_report.get("decision_packet_result").and_then(|value| value.as_str()).unwrap(),
                "execute_frozen_result": verify_report.get("execute_frozen_result").and_then(|value| value.as_str()).unwrap(),
                "current_pre_activation_gate_verdict": verify_report.get("current_pre_activation_gate_verdict").and_then(|value| value.as_str()),
                "current_pre_activation_gate_reason": verify_report.get("current_pre_activation_gate_reason").and_then(|value| value.as_str()),
                "release_capsule_summary": "Release capsule summary.",
                "release_capsule_audit_manifest_summary": "Hash-locked audit manifest: sha256 digests over 3 archival members; any content drift invalidates this release capsule",
                "attestation_seal_summary": "Attestation seal summary.",
                "custody_record_summary": "Custody record identity.",
                "release_capsule_digest_manifest_sha256": "placeholder-manifest-sha",
                "release_capsule_digest_manifest_entry_count": 3,
                "release_capsule_digest_algorithm": "sha256",
                "explicit_statement": "attestation-seal status statement"
            }),
        )
        .unwrap();
        persist_json(
            &attestation_seal_session_dir
                .join("tiny_live_activation_package_attestation_seal.release_capsule.report.json"),
            &json!({
                "decision_packet_session_dir": decision_packet_session_dir.display().to_string()
            }),
        )
        .unwrap();
        let manifest = json!({
            "digest_algorithm": "sha256",
            "entries": [
                {
                    "label": "release_capsule_session",
                    "path": release_capsule_session_dir.join("tiny_live_activation_package_release_capsule.session.json").display().to_string(),
                    "sha256": "1111"
                },
                {
                    "label": "release_capsule_status",
                    "path": release_capsule_session_dir.join("tiny_live_activation_package_release_capsule.status.json").display().to_string(),
                    "sha256": "2222"
                },
                {
                    "label": "nested_activation_ticket_session",
                    "path": activation_ticket_session_dir.join("tiny_live_activation_package_activation_ticket.session.json").display().to_string(),
                    "sha256": "3333"
                }
            ]
        });
        let manifest_sha256 = canonical_sha256_hex(&manifest).unwrap();
        persist_json(
            &attestation_seal_session_dir.join(
                "tiny_live_activation_package_attestation_seal.release_capsule.digest_manifest.json",
            ),
            &manifest,
        )
        .unwrap();

        let mut session: serde_json::Value = load_json(
            &attestation_seal_session_dir
                .join("tiny_live_activation_package_attestation_seal.session.json"),
        )
        .unwrap();
        session.as_object_mut().unwrap().insert(
            "release_capsule_digest_manifest_sha256".to_string(),
            json!(manifest_sha256.clone()),
        );
        persist_json(
            &attestation_seal_session_dir
                .join("tiny_live_activation_package_attestation_seal.session.json"),
            &session,
        )
        .unwrap();
        let mut status: serde_json::Value = load_json(
            &attestation_seal_session_dir
                .join("tiny_live_activation_package_attestation_seal.status.json"),
        )
        .unwrap();
        status.as_object_mut().unwrap().insert(
            "release_capsule_digest_manifest_sha256".to_string(),
            json!(manifest_sha256),
        );
        persist_json(
            &attestation_seal_session_dir
                .join("tiny_live_activation_package_attestation_seal.status.json"),
            &status,
        )
        .unwrap();

        let mut verify_report = verify_report;
        bind_attestation_seal_report_to_fixture(
            &mut verify_report,
            &attestation_seal_session_dir,
            &release_capsule_session_dir,
            &activation_ticket_session_dir,
            &review_receipt_session_dir,
            &handoff_bundle_session_dir,
            &decision_packet_session_dir,
            &execute_frozen_session_dir,
            &turn_green_session_dir,
            &launch_packet_session_dir,
            &package_dir,
            &install_root,
            &backend_command,
            manifest,
        );
        let verify_path = fixture_dir.join("attestation-seal.verify.json");
        persist_json(&verify_path, &verify_report).unwrap();
        write_fake_attestation_seal_verify_command(
            &bin_dir.join("copybot_tiny_live_activation_package_attestation_seal"),
            &verify_path,
            &release_capsule_session_dir,
            &decision_packet_session_dir,
            &attestation_seal_session_dir,
        );

        FakeCommandFixture {
            fixture_dir,
            bin_dir,
            attestation_seal_session_dir,
            provenance_certificate_session_dir,
            decision_packet_session_dir,
            install_root,
        }
    }

    fn bind_attestation_seal_report_to_fixture(
        verify_report: &mut serde_json::Value,
        attestation_seal_session_dir: &Path,
        release_capsule_session_dir: &Path,
        activation_ticket_session_dir: &Path,
        review_receipt_session_dir: &Path,
        handoff_bundle_session_dir: &Path,
        decision_packet_session_dir: &Path,
        execute_frozen_session_dir: &Path,
        turn_green_session_dir: &Path,
        launch_packet_session_dir: &Path,
        package_dir: &Path,
        install_root: &Path,
        backend_command: &Path,
        release_capsule_digest_manifest: serde_json::Value,
    ) {
        let manifest_sha256 = canonical_sha256_hex(&release_capsule_digest_manifest).unwrap();
        let manifest_entry_count = release_capsule_digest_manifest["entries"]
            .as_array()
            .unwrap()
            .len();
        let object = verify_report.as_object_mut().unwrap();
        object.insert(
            "release_capsule_session_dir".to_string(),
            json!(release_capsule_session_dir.display().to_string()),
        );
        object.insert(
            "activation_ticket_session_dir".to_string(),
            json!(activation_ticket_session_dir.display().to_string()),
        );
        object.insert(
            "review_receipt_session_dir".to_string(),
            json!(review_receipt_session_dir.display().to_string()),
        );
        object.insert(
            "handoff_bundle_session_dir".to_string(),
            json!(handoff_bundle_session_dir.display().to_string()),
        );
        object.insert(
            "decision_packet_session_dir".to_string(),
            json!(decision_packet_session_dir.display().to_string()),
        );
        object.insert(
            "execute_frozen_session_dir".to_string(),
            json!(execute_frozen_session_dir.display().to_string()),
        );
        object.insert(
            "turn_green_session_dir".to_string(),
            json!(turn_green_session_dir.display().to_string()),
        );
        object.insert(
            "launch_packet_session_dir".to_string(),
            json!(launch_packet_session_dir.display().to_string()),
        );
        object.insert(
            "package_dir".to_string(),
            json!(package_dir.display().to_string()),
        );
        object.insert(
            "install_root".to_string(),
            json!(install_root.display().to_string()),
        );
        object.insert(
            "target_service_name".to_string(),
            json!("solana-copy-bot.service"),
        );
        object.insert(
            "backend_command".to_string(),
            json!(backend_command.display().to_string()),
        );
        object.insert("wrapper_timeout_ms".to_string(), json!(1200_u64));
        object.insert(
            "service_status_max_staleness_ms".to_string(),
            json!(4500_u64),
        );
        object.insert(
            "verify_release_capsule_command_summary".to_string(),
            json!(format!(
                "verify immutable release-capsule session under {} before freezing the final attestation seal",
                release_capsule_session_dir.display()
            )),
        );
        object.insert(
            "reviewed_frozen_live_cutover_controller_command_summary".to_string(),
            json!(format!(
                "copybot_tiny_live_activation_package_live_cutover --package-dir {} --install-root {} --target-service solana-copy-bot.service --backend-command {} --wrapper-timeout-ms 1200 --service-status-max-staleness-ms 4500 --session-dir {} --run-live-package-cutover",
                shell_escape_path(package_dir),
                shell_escape_path(install_root),
                shell_escape_path(backend_command),
                shell_escape_path(&attestation_seal_session_dir.join("frozen-live-cutover-session")),
            )),
        );
        object.insert(
            "attestation_seal_summary".to_string(),
            json!("Attestation seal summary."),
        );
        object.insert(
            "custody_record_summary".to_string(),
            json!("Custody record identity."),
        );
        object.insert(
            "release_capsule_summary".to_string(),
            json!("Release capsule summary."),
        );
        object.insert(
            "release_capsule_audit_manifest_summary".to_string(),
            json!("Hash-locked audit manifest: sha256 digests over 3 archival members; any content drift invalidates this release capsule"),
        );
        object.insert(
            "release_capsule_digest_manifest_sha256".to_string(),
            json!(manifest_sha256),
        );
        object.insert(
            "release_capsule_digest_manifest_entry_count".to_string(),
            json!(manifest_entry_count),
        );
        object.insert(
            "release_capsule_digest_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "release_capsule_digest_manifest".to_string(),
            release_capsule_digest_manifest,
        );
        object.insert(
            "explicit_statement".to_string(),
            json!("attestation-seal verify statement"),
        );
    }

    fn attestation_seal_verify_report(
        result: &str,
        handoff_bundle_result: &str,
        decision_packet_result: &str,
        execute_frozen_result: &str,
        gate_verdict: &str,
        gate_reason: &str,
    ) -> serde_json::Value {
        let attestation_seal_result =
            if result == "ready_for_manual_execution_when_gate_turns_green" {
                "ready_for_manual_execution_when_gate_turns_green"
            } else {
                result
            };
        json!({
            "generated_at": Utc::now(),
            "mode": "verify_live_package_attestation_seal",
            "verdict": "tiny_live_package_attestation_seal_verify_ok",
            "reason": "attestation-seal chain is coherent",
            "result": attestation_seal_result,
            "handoff_bundle_result": handoff_bundle_result,
            "decision_packet_result": decision_packet_result,
            "execute_frozen_result": execute_frozen_result,
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason,
        })
    }

    fn write_fake_attestation_seal_verify_command(
        path: &Path,
        verify_path: &Path,
        expected_release_capsule_session_dir: &Path,
        expected_decision_packet_session_dir: &Path,
        attestation_seal_session_dir: &Path,
    ) {
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --release-capsule-session-dir {} \"*) : ;;\n  *) echo 'unexpected release-capsule-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --confirm-decision-packet-session-dir {} \"*) : ;;\n  *) echo 'unexpected decision-packet-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected attestation-seal session dir' >&2; exit 1;;\nesac\ncat '{}'\n",
            shell_escape_path(expected_release_capsule_session_dir),
            shell_escape_path(expected_decision_packet_session_dir),
            shell_escape_path(attestation_seal_session_dir),
            verify_path.display(),
        );
        fs::write(path, script).unwrap();
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).unwrap();
    }

    struct PathGuard {
        original_path: OsString,
        _lock: MutexGuard<'static, ()>,
    }

    impl PathGuard {
        fn install(bin_dir: &Path) -> Self {
            static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
            let lock = LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
            let original_path = env::var_os("PATH").unwrap_or_default();
            let mut parts = vec![bin_dir.as_os_str().to_os_string()];
            parts.extend(env::split_paths(&original_path).map(|path| path.into_os_string()));
            let joined = env::join_paths(parts.iter()).unwrap();
            env::set_var("PATH", &joined);
            Self {
                original_path,
                _lock: lock,
            }
        }
    }

    impl Drop for PathGuard {
        fn drop(&mut self) {
            env::set_var("PATH", &self.original_path);
        }
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{label}.{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }
}
