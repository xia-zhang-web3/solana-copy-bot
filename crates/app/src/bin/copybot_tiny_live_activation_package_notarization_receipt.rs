use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_app::tiny_live_activation::install_target_managed_surface::validate_turn_green_session_dir as validate_managed_surface_session_dir;
use copybot_app::tiny_live_activation::package_provenance_certificate_notarization_receipt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: copybot_tiny_live_activation_package_notarization_receipt --provenance-certificate-session-dir <path> [--confirm-decision-packet-session-dir <path>] [--session-dir <path>] [--output <path>] [--json] (--plan-live-package-notarization-receipt | --render-live-package-notarization-receipt | --run-live-package-notarization-receipt | --verify-live-package-notarization-receipt)";
const NOTARIZATION_RECEIPT_SESSION_VERSION: &str = "1";
const NOTARIZATION_RECEIPT_STATUS_VERSION: &str = "1";
const NOTARIZATION_RECEIPT_LEDGER_SEAL_ALGORITHM: &str = "sha256";
const NOTARIZATION_RECEIPT_SESSION_EXPLICIT_STATEMENT: &str =
    "the verified provenance-certificate session is the primary input here; this immutable notarization receipt freezes one final operator-facing ledger seal over the canonical chain fingerprint without enabling production execution";
const NOTARIZATION_RECEIPT_STATUS_EXPLICIT_STATEMENT: &str =
    "this notarization receipt is archival and read-only; it never executes or enables the frozen controller";
const NOTARIZATION_RECEIPT_VERIFY_EXPLICIT_STATEMENT: &str =
    "this verification binds one immutable notarization receipt to verified provenance-certificate truth, the exact reviewed frozen controller summary, and one final ledger-seal identity over the canonical nested archival chain fingerprint";

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
    provenance_certificate_session_dir: PathBuf,
    confirmed_decision_packet_session_dir: Option<PathBuf>,
    session_dir: Option<PathBuf>,
    output_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanLivePackageNotarizationReceipt,
    RenderLivePackageNotarizationReceipt,
    RunLivePackageNotarizationReceipt,
    VerifyLivePackageNotarizationReceipt,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageNotarizationReceiptVerdict {
    TinyLivePackageNotarizationReceiptPlanReady,
    TinyLivePackageNotarizationReceiptRendered,
    TinyLivePackageNotarizationReceiptRefusedNowByStage3,
    TinyLivePackageNotarizationReceiptRefusedNowByPreActivationGate,
    TinyLivePackageNotarizationReceiptRefusedNowByInvalidOrDriftedContract,
    TinyLivePackageNotarizationReceiptReadyForManualExecutionWhenGateTurnsGreen,
    TinyLivePackageNotarizationReceiptVerifyOk,
    TinyLivePackageNotarizationReceiptVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageNotarizationReceiptResult {
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    RefusedNowByInvalidOrDriftedContract,
    ReadyForManualExecutionWhenGateTurnsGreen,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageNotarizationReceiptStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageNotarizationReceiptSession {
    session_version: String,
    planned_at: DateTime<Utc>,
    provenance_certificate_session_dir: String,
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
    verify_provenance_certificate_command_summary: String,
    reviewed_frozen_live_cutover_controller_command_summary: String,
    provenance_certificate_summary: String,
    chain_fingerprint_summary: String,
    chain_fingerprint_sha256: String,
    chain_fingerprint_algorithm: String,
    release_capsule_digest_manifest_sha256: String,
    release_capsule_digest_manifest_entry_count: usize,
    release_capsule_digest_algorithm: String,
    notarization_receipt_summary: String,
    ledger_seal_summary: String,
    ledger_seal_sha256: String,
    ledger_seal_algorithm: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageNotarizationReceiptStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    provenance_certificate_session_dir: String,
    result: LivePackageNotarizationReceiptResult,
    reason: String,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    provenance_certificate_step: Option<PackageNotarizationReceiptStepArtifact>,
    provenance_certificate_summary: String,
    chain_fingerprint_summary: String,
    chain_fingerprint_sha256: String,
    chain_fingerprint_algorithm: String,
    release_capsule_digest_manifest_sha256: String,
    release_capsule_digest_manifest_entry_count: usize,
    release_capsule_digest_algorithm: String,
    notarization_receipt_summary: String,
    ledger_seal_summary: String,
    ledger_seal_sha256: String,
    ledger_seal_algorithm: String,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageNotarizationReceiptPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    provenance_certificate_report_path: PathBuf,
    release_capsule_digest_manifest_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageNotarizationReceiptReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageNotarizationReceiptVerdict,
    reason: String,
    provenance_certificate_session_dir: String,
    attestation_seal_session_dir: Option<String>,
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
    notarization_receipt_session_path: Option<String>,
    notarization_receipt_status_path: Option<String>,
    archived_provenance_certificate_report_path: Option<String>,
    archived_release_capsule_digest_manifest_path: Option<String>,
    result: Option<String>,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    provenance_certificate_step: Option<PackageNotarizationReceiptStepArtifact>,
    verify_provenance_certificate_command_summary: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    provenance_certificate_summary: Option<String>,
    chain_fingerprint_summary: Option<String>,
    chain_fingerprint_sha256: Option<String>,
    chain_fingerprint_algorithm: Option<String>,
    release_capsule_digest_manifest_sha256: Option<String>,
    release_capsule_digest_manifest_entry_count: Option<usize>,
    release_capsule_digest_algorithm: Option<String>,
    notarization_receipt_summary: Option<String>,
    ledger_seal_summary: Option<String>,
    ledger_seal_sha256: Option<String>,
    ledger_seal_algorithm: Option<String>,
    release_capsule_digest_manifest:
        Option<package_provenance_certificate_notarization_receipt::ReleaseCapsuleDigestManifest>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    verification_mismatches: Vec<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct NestedProvenanceCertificateReportView {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: String,
    reason: String,
}

#[derive(Debug, Serialize)]
struct LedgerSealPayload<'a> {
    result: &'a str,
    handoff_bundle_result: Option<&'a str>,
    decision_packet_result: Option<&'a str>,
    execute_frozen_result: Option<&'a str>,
    current_pre_activation_gate_verdict: Option<&'a str>,
    current_pre_activation_gate_reason: Option<&'a str>,
    reviewed_frozen_live_cutover_controller_command_summary: &'a str,
    chain_fingerprint_sha256: &'a str,
    chain_fingerprint_algorithm: &'a str,
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
    let mut provenance_certificate_session_dir: Option<PathBuf> = None;
    let mut confirmed_decision_packet_session_dir: Option<PathBuf> = None;
    let mut session_dir: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut json = false;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--provenance-certificate-session-dir" => {
                provenance_certificate_session_dir = Some(PathBuf::from(parse_string_arg(
                    "--provenance-certificate-session-dir",
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
            "--plan-live-package-notarization-receipt" => set_mode(
                &mut mode,
                Mode::PlanLivePackageNotarizationReceipt,
                arg.as_str(),
            )?,
            "--render-live-package-notarization-receipt" => set_mode(
                &mut mode,
                Mode::RenderLivePackageNotarizationReceipt,
                arg.as_str(),
            )?,
            "--run-live-package-notarization-receipt" => set_mode(
                &mut mode,
                Mode::RunLivePackageNotarizationReceipt,
                arg.as_str(),
            )?,
            "--verify-live-package-notarization-receipt" => set_mode(
                &mut mode,
                Mode::VerifyLivePackageNotarizationReceipt,
                arg.as_str(),
            )?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let Some(mode) = mode else {
        return Ok(None);
    };
    if matches!(mode, Mode::RenderLivePackageNotarizationReceipt) && output_path.is_none() {
        bail!("missing --output for --render-live-package-notarization-receipt");
    }
    if matches!(
        mode,
        Mode::RunLivePackageNotarizationReceipt | Mode::VerifyLivePackageNotarizationReceipt
    ) && session_dir.is_none()
    {
        bail!("missing --session-dir for run/verify mode");
    }
    if matches!(
        mode,
        Mode::RunLivePackageNotarizationReceipt | Mode::VerifyLivePackageNotarizationReceipt
    ) && confirmed_decision_packet_session_dir.is_none()
    {
        bail!("missing --confirm-decision-packet-session-dir for run/verify mode");
    }
    let provenance_certificate_session_dir = provenance_certificate_session_dir
        .ok_or_else(|| anyhow!("missing --provenance-certificate-session-dir"))?;
    Ok(Some(Config {
        mode,
        provenance_certificate_session_dir,
        confirmed_decision_packet_session_dir,
        session_dir,
        output_path,
        json,
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanLivePackageNotarizationReceipt => {
            plan_live_package_notarization_receipt_report(&config)?
        }
        Mode::RenderLivePackageNotarizationReceipt => {
            render_live_package_notarization_receipt_report(&config)?
        }
        Mode::RunLivePackageNotarizationReceipt => {
            run_live_package_notarization_receipt_report(&config)?
        }
        Mode::VerifyLivePackageNotarizationReceipt => {
            verify_live_package_notarization_receipt_report(&config)?
        }
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_live_package_notarization_receipt_report(
    config: &Config,
) -> Result<PackageNotarizationReceiptReport> {
    let (provenance_certificate, release_capsule_digest_manifest, classification) =
        match config.confirmed_decision_packet_session_dir.as_deref() {
            Some(confirmed_decision_packet_session_dir) => {
                match package_provenance_certificate_notarization_receipt::verify_live_package_provenance_certificate_for_notarization_receipt(
                    &config.provenance_certificate_session_dir,
                    confirmed_decision_packet_session_dir,
                ) {
                    Ok(verified) => {
                        let classification =
                            classify_verified_provenance_certificate(&verified);
                        (
                            verified.contract,
                            Some(verified.release_capsule_digest_manifest),
                            classification,
                        )
                    }
                    Err(_) => {
                        let raw_contract = package_provenance_certificate_notarization_receipt::load_live_package_provenance_certificate_contract_for_notarization_receipt(
                            &config.provenance_certificate_session_dir,
                        )?;
                        let classification =
                            classify_notarization_contract(&raw_contract, false);
                        (raw_contract, None, classification)
                    }
                }
            }
            None => {
                let raw_contract = package_provenance_certificate_notarization_receipt::load_live_package_provenance_certificate_contract_for_notarization_receipt(
                    &config.provenance_certificate_session_dir,
                )?;
                let classification = classify_notarization_contract(&raw_contract, false);
                (raw_contract, None, classification)
            }
        };
    let session_dir = config.session_dir.clone().unwrap_or_else(|| {
        config
            .provenance_certificate_session_dir
            .join("notarization-receipt")
    });
    let manifest_sha256 = match release_capsule_digest_manifest.as_ref() {
        Some(value) => canonical_sha256_hex(value)?,
        None => provenance_certificate
            .release_capsule_digest_manifest_sha256
            .clone(),
    };
    let manifest_entry_count = release_capsule_digest_manifest
        .as_ref()
        .map(|value| value.entries.len())
        .unwrap_or(provenance_certificate.release_capsule_digest_manifest_entry_count);
    let manifest_algorithm = release_capsule_digest_manifest
        .as_ref()
        .map(|value| value.digest_algorithm.clone())
        .unwrap_or_else(|| {
            provenance_certificate
                .release_capsule_digest_algorithm
                .clone()
        });
    let result = serialize_enum(&classification.0);
    let ledger_seal_sha256 = compute_ledger_seal_sha256(
        &result,
        provenance_certificate.handoff_bundle_result.as_deref(),
        provenance_certificate.decision_packet_result.as_deref(),
        provenance_certificate.execute_frozen_result.as_deref(),
        provenance_certificate
            .current_pre_activation_gate_verdict
            .as_deref(),
        provenance_certificate
            .current_pre_activation_gate_reason
            .as_deref(),
        &provenance_certificate.reviewed_frozen_live_cutover_controller_command_summary,
        &provenance_certificate.chain_fingerprint_sha256,
        &provenance_certificate.chain_fingerprint_algorithm,
        &manifest_sha256,
        manifest_entry_count,
        &manifest_algorithm,
    )?;

    Ok(PackageNotarizationReceiptReport {
        generated_at: Utc::now(),
        mode: "plan_live_package_notarization_receipt".to_string(),
        verdict:
            TinyLivePackageNotarizationReceiptVerdict::TinyLivePackageNotarizationReceiptPlanReady,
        reason: format!(
            "provenance-certificate-native notarization receipt is explicit for verified provenance-certificate session {}; this command stays archival and read-only while freezing one final ledger-style seal over the canonical chain fingerprint",
            config.provenance_certificate_session_dir.display()
        ),
        provenance_certificate_session_dir: config
            .provenance_certificate_session_dir
            .display()
            .to_string(),
        attestation_seal_session_dir: Some(
            provenance_certificate
                .attestation_seal_session_dir
                .display()
                .to_string(),
        ),
        release_capsule_session_dir: Some(
            provenance_certificate
                .release_capsule_session_dir
                .display()
                .to_string(),
        ),
        activation_ticket_session_dir: Some(
            provenance_certificate
                .activation_ticket_session_dir
                .display()
                .to_string(),
        ),
        review_receipt_session_dir: Some(
            provenance_certificate
                .review_receipt_session_dir
                .display()
                .to_string(),
        ),
        handoff_bundle_session_dir: Some(
            provenance_certificate
                .handoff_bundle_session_dir
                .display()
                .to_string(),
        ),
        decision_packet_session_dir: Some(
            provenance_certificate
                .decision_packet_session_dir
                .display()
                .to_string(),
        ),
        execute_frozen_session_dir: Some(
            provenance_certificate
                .execute_frozen_session_dir
                .display()
                .to_string(),
        ),
        turn_green_session_dir: Some(
            provenance_certificate.turn_green_session_dir.display().to_string(),
        ),
        launch_packet_session_dir: Some(
            provenance_certificate
                .launch_packet_session_dir
                .display()
                .to_string(),
        ),
        package_dir: Some(provenance_certificate.package_dir.display().to_string()),
        install_root: Some(provenance_certificate.install_root.display().to_string()),
        target_service_name: Some(provenance_certificate.target_service_name.clone()),
        backend_command: Some(provenance_certificate.backend_command.clone()),
        wrapper_timeout_ms: Some(provenance_certificate.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(
            provenance_certificate.service_status_max_staleness_ms,
        ),
        session_dir: Some(session_dir.display().to_string()),
        notarization_receipt_session_path: None,
        notarization_receipt_status_path: None,
        archived_provenance_certificate_report_path: None,
        archived_release_capsule_digest_manifest_path: None,
        result: Some(result),
        handoff_bundle_result: provenance_certificate.handoff_bundle_result.clone(),
        decision_packet_result: provenance_certificate.decision_packet_result.clone(),
        execute_frozen_result: provenance_certificate.execute_frozen_result.clone(),
        provenance_certificate_step: None,
        verify_provenance_certificate_command_summary: Some(
            verify_provenance_certificate_command_summary(
                &config.provenance_certificate_session_dir,
            ),
        ),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            provenance_certificate
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        provenance_certificate_summary: Some(
            provenance_certificate.provenance_certificate_summary.clone(),
        ),
        chain_fingerprint_summary: Some(
            provenance_certificate.chain_fingerprint_summary.clone(),
        ),
        chain_fingerprint_sha256: Some(
            provenance_certificate.chain_fingerprint_sha256.clone(),
        ),
        chain_fingerprint_algorithm: Some(
            provenance_certificate.chain_fingerprint_algorithm.clone(),
        ),
        release_capsule_digest_manifest_sha256: Some(manifest_sha256.clone()),
        release_capsule_digest_manifest_entry_count: Some(manifest_entry_count),
        release_capsule_digest_algorithm: Some(manifest_algorithm.clone()),
        notarization_receipt_summary: Some(notarization_receipt_summary(
            classification.0,
            &provenance_certificate
                .reviewed_frozen_live_cutover_controller_command_summary,
        )),
        ledger_seal_summary: Some(ledger_seal_summary(
            NOTARIZATION_RECEIPT_LEDGER_SEAL_ALGORITHM,
            &ledger_seal_sha256,
            &provenance_certificate.chain_fingerprint_sha256,
        )),
        ledger_seal_sha256: Some(ledger_seal_sha256),
        ledger_seal_algorithm: Some(
            NOTARIZATION_RECEIPT_LEDGER_SEAL_ALGORITHM.to_string(),
        ),
        release_capsule_digest_manifest,
        current_pre_activation_gate_verdict: provenance_certificate
            .current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: provenance_certificate
            .current_pre_activation_gate_reason,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this final notarization receipt freezes verified provenance-certificate truth, the canonical chain fingerprint, and one immutable ledger seal without enabling production execution"
                .to_string(),
    })
}

fn render_live_package_notarization_receipt_report(
    config: &Config,
) -> Result<PackageNotarizationReceiptReport> {
    let plan = plan_live_package_notarization_receipt_report(config)?;
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
        "#!/usr/bin/env bash\nset -euo pipefail\n\ncopybot_tiny_live_activation_package_notarization_receipt --provenance-certificate-session-dir {} --plan-live-package-notarization-receipt\ncopybot_tiny_live_activation_package_notarization_receipt --provenance-certificate-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --run-live-package-notarization-receipt\ncopybot_tiny_live_activation_package_notarization_receipt --provenance-certificate-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --verify-live-package-notarization-receipt\n",
        shell_escape_path(&config.provenance_certificate_session_dir),
        shell_escape_path(&config.provenance_certificate_session_dir),
        shell_escape_path(&confirmed_decision_packet_session_dir),
        shell_escape_path(&session_dir),
        shell_escape_path(&config.provenance_certificate_session_dir),
        shell_escape_path(&confirmed_decision_packet_session_dir),
        shell_escape_path(&session_dir),
    );
    write_script(output_path, &script)?;
    Ok(PackageNotarizationReceiptReport {
        generated_at: Utc::now(),
        mode: "render_live_package_notarization_receipt".to_string(),
        verdict:
            TinyLivePackageNotarizationReceiptVerdict::TinyLivePackageNotarizationReceiptRendered,
        reason: format!(
            "rendered provenance-certificate-native notarization-receipt script to {}",
            output_path.display()
        ),
        provenance_certificate_session_dir: plan.provenance_certificate_session_dir,
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
        notarization_receipt_session_path: None,
        notarization_receipt_status_path: None,
        archived_provenance_certificate_report_path: None,
        archived_release_capsule_digest_manifest_path: None,
        result: plan.result,
        handoff_bundle_result: plan.handoff_bundle_result,
        decision_packet_result: plan.decision_packet_result,
        execute_frozen_result: plan.execute_frozen_result,
        provenance_certificate_step: None,
        verify_provenance_certificate_command_summary: plan
            .verify_provenance_certificate_command_summary,
        reviewed_frozen_live_cutover_controller_command_summary: plan
            .reviewed_frozen_live_cutover_controller_command_summary,
        provenance_certificate_summary: plan.provenance_certificate_summary,
        chain_fingerprint_summary: plan.chain_fingerprint_summary,
        chain_fingerprint_sha256: plan.chain_fingerprint_sha256,
        chain_fingerprint_algorithm: plan.chain_fingerprint_algorithm,
        release_capsule_digest_manifest_sha256: plan.release_capsule_digest_manifest_sha256,
        release_capsule_digest_manifest_entry_count: plan
            .release_capsule_digest_manifest_entry_count,
        release_capsule_digest_algorithm: plan.release_capsule_digest_algorithm,
        notarization_receipt_summary: plan.notarization_receipt_summary,
        ledger_seal_summary: plan.ledger_seal_summary,
        ledger_seal_sha256: plan.ledger_seal_sha256,
        ledger_seal_algorithm: plan.ledger_seal_algorithm,
        release_capsule_digest_manifest: None,
        current_pre_activation_gate_verdict: plan.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: plan.current_pre_activation_gate_reason,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this rendered script accepts the verified provenance-certificate session as the primary input and stays archival and read-only with respect to the real target"
                .to_string(),
    })
}

fn run_live_package_notarization_receipt_report(
    config: &Config,
) -> Result<PackageNotarizationReceiptReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for run mode"))?;
    let verified_provenance_certificate = match package_provenance_certificate_notarization_receipt::verify_live_package_provenance_certificate_for_notarization_receipt(
        &config.provenance_certificate_session_dir,
        config
            .confirmed_decision_packet_session_dir
            .as_deref()
            .ok_or_else(|| anyhow!("missing --confirm-decision-packet-session-dir for run mode"))?,
    ) {
        Ok(value) => value,
        Err(error) => {
            return Ok(failure_report_without_contract(
                config,
                TinyLivePackageNotarizationReceiptVerdict::TinyLivePackageNotarizationReceiptRefusedNowByInvalidOrDriftedContract,
                format!(
                    "failed to verify provenance-certificate session under {}: {error}",
                    config.provenance_certificate_session_dir.display()
                ),
            ))
        }
    };

    validate_managed_surface_session_dir(
        &verified_provenance_certificate.contract.install_root,
        session_dir,
    )
    .with_context(|| {
        format!(
            "notarization-receipt session dir {} overlaps the managed live target surface derived from verified provenance-certificate install_root {}",
            session_dir.display(),
            verified_provenance_certificate.contract.install_root.display()
        )
    })?;
    ensure_clean_notarization_receipt_session_dir(session_dir)?;
    let paths = notarization_receipt_paths(session_dir);
    let classification = classify_verified_provenance_certificate(&verified_provenance_certificate);
    let manifest_sha256 =
        canonical_sha256_hex(&verified_provenance_certificate.release_capsule_digest_manifest)?;
    let manifest_entry_count = verified_provenance_certificate
        .release_capsule_digest_manifest
        .entries
        .len();
    let ledger_seal_sha256 = compute_ledger_seal_sha256(
        &serialize_enum(&classification.0),
        verified_provenance_certificate
            .contract
            .handoff_bundle_result
            .as_deref(),
        verified_provenance_certificate
            .contract
            .decision_packet_result
            .as_deref(),
        verified_provenance_certificate
            .contract
            .execute_frozen_result
            .as_deref(),
        verified_provenance_certificate
            .contract
            .current_pre_activation_gate_verdict
            .as_deref(),
        verified_provenance_certificate
            .contract
            .current_pre_activation_gate_reason
            .as_deref(),
        &verified_provenance_certificate
            .contract
            .reviewed_frozen_live_cutover_controller_command_summary,
        &verified_provenance_certificate
            .contract
            .chain_fingerprint_sha256,
        &verified_provenance_certificate
            .contract
            .chain_fingerprint_algorithm,
        &manifest_sha256,
        manifest_entry_count,
        &verified_provenance_certificate
            .release_capsule_digest_manifest
            .digest_algorithm,
    )?;
    let session = planned_session(
        config,
        session_dir,
        &verified_provenance_certificate.contract,
        classification.0,
        &manifest_sha256,
        manifest_entry_count,
        &ledger_seal_sha256,
    );
    persist_json(&paths.session_path, &session)?;
    persist_json(
        &paths.provenance_certificate_report_path,
        &verified_provenance_certificate.report_json,
    )?;
    persist_json(
        &paths.release_capsule_digest_manifest_path,
        &verified_provenance_certificate.release_capsule_digest_manifest,
    )?;
    let provenance_certificate_step = Some(step_artifact(
        &paths.provenance_certificate_report_path,
        "verify_live_package_provenance_certificate",
        &verified_provenance_certificate.verdict,
        &verified_provenance_certificate.reason,
        verified_provenance_certificate.generated_at,
    ));
    let status = PackageNotarizationReceiptStatus {
        status_version: NOTARIZATION_RECEIPT_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        provenance_certificate_session_dir: config
            .provenance_certificate_session_dir
            .display()
            .to_string(),
        result: classification.0,
        reason: classification.2.clone(),
        handoff_bundle_result: verified_provenance_certificate
            .contract
            .handoff_bundle_result
            .clone(),
        decision_packet_result: verified_provenance_certificate
            .contract
            .decision_packet_result
            .clone(),
        execute_frozen_result: verified_provenance_certificate
            .contract
            .execute_frozen_result
            .clone(),
        current_pre_activation_gate_verdict: verified_provenance_certificate
            .contract
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: verified_provenance_certificate
            .contract
            .current_pre_activation_gate_reason
            .clone(),
        provenance_certificate_step: provenance_certificate_step.clone(),
        provenance_certificate_summary: verified_provenance_certificate
            .contract
            .provenance_certificate_summary
            .clone(),
        chain_fingerprint_summary: verified_provenance_certificate
            .contract
            .chain_fingerprint_summary
            .clone(),
        chain_fingerprint_sha256: verified_provenance_certificate
            .contract
            .chain_fingerprint_sha256
            .clone(),
        chain_fingerprint_algorithm: verified_provenance_certificate
            .contract
            .chain_fingerprint_algorithm
            .clone(),
        release_capsule_digest_manifest_sha256: manifest_sha256.clone(),
        release_capsule_digest_manifest_entry_count: manifest_entry_count,
        release_capsule_digest_algorithm: verified_provenance_certificate
            .release_capsule_digest_manifest
            .digest_algorithm
            .clone(),
        notarization_receipt_summary: notarization_receipt_summary(
            classification.0,
            &verified_provenance_certificate
                .contract
                .reviewed_frozen_live_cutover_controller_command_summary,
        ),
        ledger_seal_summary: ledger_seal_summary(
            NOTARIZATION_RECEIPT_LEDGER_SEAL_ALGORITHM,
            &ledger_seal_sha256,
            &verified_provenance_certificate
                .contract
                .chain_fingerprint_sha256,
        ),
        ledger_seal_sha256: ledger_seal_sha256.clone(),
        ledger_seal_algorithm: NOTARIZATION_RECEIPT_LEDGER_SEAL_ALGORITHM.to_string(),
        explicit_statement: NOTARIZATION_RECEIPT_STATUS_EXPLICIT_STATEMENT.to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    Ok(report_from_status(
        config,
        session_dir,
        &paths,
        &verified_provenance_certificate.contract,
        classification.1,
        &status,
        verified_provenance_certificate.release_capsule_digest_manifest,
    ))
}

fn verify_live_package_notarization_receipt_report(
    config: &Config,
) -> Result<PackageNotarizationReceiptReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = notarization_receipt_paths(session_dir);
    let session: PackageNotarizationReceiptSession = load_json(&paths.session_path)?;
    let status: PackageNotarizationReceiptStatus = load_json(&paths.status_path)?;
    let mut mismatches = Vec::new();

    compare_string(
        &session.provenance_certificate_session_dir,
        &config
            .provenance_certificate_session_dir
            .display()
            .to_string(),
        "notarization-receipt provenance_certificate_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "notarization-receipt session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "notarization-receipt status session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.provenance_certificate_session_dir,
        &config
            .provenance_certificate_session_dir
            .display()
            .to_string(),
        "notarization-receipt status provenance_certificate_session_dir",
        &mut mismatches,
    );

    let verified_provenance_certificate = match package_provenance_certificate_notarization_receipt::verify_live_package_provenance_certificate_for_notarization_receipt(
        &config.provenance_certificate_session_dir,
        config
            .confirmed_decision_packet_session_dir
            .as_deref()
            .ok_or_else(|| anyhow!("missing --confirm-decision-packet-session-dir for verify mode"))?,
    ) {
        Ok(value) => value,
        Err(error) => {
            let raw_contract = package_provenance_certificate_notarization_receipt::load_live_package_provenance_certificate_contract_for_notarization_receipt(
                &config.provenance_certificate_session_dir,
            )?;
            return Ok(failure_report_for_verify(
                config,
                session_dir,
                &raw_contract,
                vec![format!(
                    "failed to verify provenance-certificate session under {}: {error}",
                    config.provenance_certificate_session_dir.display()
                )],
            ));
        }
    };

    let stored_provenance_certificate_report: serde_json::Value = load_required_step_json(
        &status.provenance_certificate_step,
        &paths.provenance_certificate_report_path,
        "provenance_certificate_step",
        &mut mismatches,
    )?;
    let stored_provenance_certificate_report_view: NestedProvenanceCertificateReportView =
        serde_json::from_value(stored_provenance_certificate_report.clone()).with_context(
            || {
                format!(
                    "failed parsing archived nested provenance-certificate report {}",
                    paths.provenance_certificate_report_path.display()
                )
            },
        )?;
    let verified_provenance_certificate_report_view: NestedProvenanceCertificateReportView =
        serde_json::from_value(verified_provenance_certificate.report_json.clone()).with_context(
            || {
                format!(
                    "failed parsing freshly verified nested provenance-certificate report for {}",
                    config.provenance_certificate_session_dir.display()
                )
            },
        )?;
    compare_json_ignoring_generated_at(
        &stored_provenance_certificate_report,
        &verified_provenance_certificate.report_json,
        "notarization-receipt archived provenance-certificate report json",
        &mut mismatches,
    );
    compare_string(
        &stored_provenance_certificate_report_view.mode,
        &verified_provenance_certificate_report_view.mode,
        "notarization-receipt archived provenance-certificate report mode",
        &mut mismatches,
    );
    compare_string(
        &stored_provenance_certificate_report_view.verdict,
        &verified_provenance_certificate_report_view.verdict,
        "notarization-receipt archived provenance-certificate report verdict",
        &mut mismatches,
    );
    compare_string(
        &stored_provenance_certificate_report_view.reason,
        &verified_provenance_certificate_report_view.reason,
        "notarization-receipt archived provenance-certificate report reason",
        &mut mismatches,
    );
    if stored_provenance_certificate_report_view.generated_at
        != verified_provenance_certificate_report_view.generated_at
    {
        mismatches.push(format!(
            "notarization-receipt archived provenance-certificate report generated_at {:?} does not match freshly verified nested truth {:?}",
            stored_provenance_certificate_report_view.generated_at,
            verified_provenance_certificate_report_view.generated_at
        ));
    }
    let expected_provenance_certificate_step = step_artifact(
        &paths.provenance_certificate_report_path,
        &verified_provenance_certificate_report_view.mode,
        &verified_provenance_certificate_report_view.verdict,
        &verified_provenance_certificate_report_view.reason,
        verified_provenance_certificate_report_view.generated_at,
    );
    compare_step_artifact(
        status.provenance_certificate_step.as_ref(),
        &expected_provenance_certificate_step,
        "notarization-receipt provenance_certificate_step",
        &mut mismatches,
    );
    if verified_provenance_certificate.verdict
        != "tiny_live_package_provenance_certificate_verify_ok"
    {
        mismatches.push(format!(
            "nested provenance-certificate verification is non-green: {}",
            verified_provenance_certificate.reason
        ));
    }

    let stored_manifest_bytes = fs::read(&paths.release_capsule_digest_manifest_path)
        .with_context(|| {
            format!(
                "failed reading {}",
                paths.release_capsule_digest_manifest_path.display()
            )
        })?;
    let stored_manifest: package_provenance_certificate_notarization_receipt::ReleaseCapsuleDigestManifest =
        serde_json::from_slice(&stored_manifest_bytes).with_context(|| {
            format!(
                "failed parsing {}",
                paths.release_capsule_digest_manifest_path.display()
            )
        })?;
    if stored_manifest != verified_provenance_certificate.release_capsule_digest_manifest {
        mismatches.push(
            "notarization-receipt archived release-capsule digest manifest does not match freshly verified nested truth"
                .to_string(),
        );
    }
    let expected_manifest_sha256 =
        canonical_sha256_hex(&verified_provenance_certificate.release_capsule_digest_manifest)?;
    let stored_manifest_sha256 = sha256_hex_bytes(&stored_manifest_bytes);
    if stored_manifest_sha256 != expected_manifest_sha256 {
        mismatches.push(format!(
            "notarization-receipt archived release-capsule digest manifest sha256 {stored_manifest_sha256} does not match expected {expected_manifest_sha256}"
        ));
    }
    let expected_manifest_entry_count = verified_provenance_certificate
        .release_capsule_digest_manifest
        .entries
        .len();

    compare_string(
        &session.attestation_seal_session_dir,
        &verified_provenance_certificate
            .contract
            .attestation_seal_session_dir
            .display()
            .to_string(),
        "notarization-receipt attestation_seal_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.release_capsule_session_dir,
        &verified_provenance_certificate
            .contract
            .release_capsule_session_dir
            .display()
            .to_string(),
        "notarization-receipt release_capsule_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.activation_ticket_session_dir,
        &verified_provenance_certificate
            .contract
            .activation_ticket_session_dir
            .display()
            .to_string(),
        "notarization-receipt activation_ticket_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.review_receipt_session_dir,
        &verified_provenance_certificate
            .contract
            .review_receipt_session_dir
            .display()
            .to_string(),
        "notarization-receipt review_receipt_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.handoff_bundle_session_dir,
        &verified_provenance_certificate
            .contract
            .handoff_bundle_session_dir
            .display()
            .to_string(),
        "notarization-receipt handoff_bundle_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.decision_packet_session_dir,
        &verified_provenance_certificate
            .contract
            .decision_packet_session_dir
            .display()
            .to_string(),
        "notarization-receipt decision_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.execute_frozen_session_dir,
        &verified_provenance_certificate
            .contract
            .execute_frozen_session_dir
            .display()
            .to_string(),
        "notarization-receipt execute_frozen_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.turn_green_session_dir,
        &verified_provenance_certificate
            .contract
            .turn_green_session_dir
            .display()
            .to_string(),
        "notarization-receipt turn_green_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.launch_packet_session_dir,
        &verified_provenance_certificate
            .contract
            .launch_packet_session_dir
            .display()
            .to_string(),
        "notarization-receipt launch_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.package_dir,
        &verified_provenance_certificate
            .contract
            .package_dir
            .display()
            .to_string(),
        "notarization-receipt package_dir",
        &mut mismatches,
    );
    compare_string(
        &session.install_root,
        &verified_provenance_certificate
            .contract
            .install_root
            .display()
            .to_string(),
        "notarization-receipt install_root",
        &mut mismatches,
    );
    compare_string(
        &session.target_service_name,
        &verified_provenance_certificate.contract.target_service_name,
        "notarization-receipt target_service_name",
        &mut mismatches,
    );
    compare_string(
        &session.backend_command,
        &verified_provenance_certificate.contract.backend_command,
        "notarization-receipt backend_command",
        &mut mismatches,
    );
    compare_u64(
        session.wrapper_timeout_ms,
        verified_provenance_certificate.contract.wrapper_timeout_ms,
        "notarization-receipt wrapper_timeout_ms",
        &mut mismatches,
    );
    compare_u64(
        session.service_status_max_staleness_ms,
        verified_provenance_certificate
            .contract
            .service_status_max_staleness_ms,
        "notarization-receipt service_status_max_staleness_ms",
        &mut mismatches,
    );

    let classification = classify_verified_provenance_certificate(&verified_provenance_certificate);
    let expected_result = serialize_enum(&classification.0);
    let expected_notarization_receipt_summary = notarization_receipt_summary(
        classification.0,
        &verified_provenance_certificate
            .contract
            .reviewed_frozen_live_cutover_controller_command_summary,
    );
    let expected_ledger_seal_sha256 = compute_ledger_seal_sha256(
        &expected_result,
        verified_provenance_certificate
            .contract
            .handoff_bundle_result
            .as_deref(),
        verified_provenance_certificate
            .contract
            .decision_packet_result
            .as_deref(),
        verified_provenance_certificate
            .contract
            .execute_frozen_result
            .as_deref(),
        verified_provenance_certificate
            .contract
            .current_pre_activation_gate_verdict
            .as_deref(),
        verified_provenance_certificate
            .contract
            .current_pre_activation_gate_reason
            .as_deref(),
        &verified_provenance_certificate
            .contract
            .reviewed_frozen_live_cutover_controller_command_summary,
        &verified_provenance_certificate
            .contract
            .chain_fingerprint_sha256,
        &verified_provenance_certificate
            .contract
            .chain_fingerprint_algorithm,
        &expected_manifest_sha256,
        expected_manifest_entry_count,
        &verified_provenance_certificate
            .release_capsule_digest_manifest
            .digest_algorithm,
    )?;
    let expected_ledger_seal_summary = ledger_seal_summary(
        NOTARIZATION_RECEIPT_LEDGER_SEAL_ALGORITHM,
        &expected_ledger_seal_sha256,
        &verified_provenance_certificate
            .contract
            .chain_fingerprint_sha256,
    );

    compare_string(
        &session.verify_provenance_certificate_command_summary,
        &verify_provenance_certificate_command_summary(&config.provenance_certificate_session_dir),
        "notarization-receipt session verify_provenance_certificate_command_summary",
        &mut mismatches,
    );
    compare_string(
        &session.reviewed_frozen_live_cutover_controller_command_summary,
        &verified_provenance_certificate
            .contract
            .reviewed_frozen_live_cutover_controller_command_summary,
        "notarization-receipt session reviewed_frozen_live_cutover_controller_command_summary",
        &mut mismatches,
    );
    compare_string(
        &session.provenance_certificate_summary,
        &verified_provenance_certificate
            .contract
            .provenance_certificate_summary,
        "notarization-receipt session provenance_certificate_summary",
        &mut mismatches,
    );
    compare_string(
        &session.chain_fingerprint_summary,
        &verified_provenance_certificate
            .contract
            .chain_fingerprint_summary,
        "notarization-receipt session chain_fingerprint_summary",
        &mut mismatches,
    );
    compare_string(
        &session.chain_fingerprint_sha256,
        &verified_provenance_certificate
            .contract
            .chain_fingerprint_sha256,
        "notarization-receipt session chain_fingerprint_sha256",
        &mut mismatches,
    );
    compare_string(
        &session.chain_fingerprint_algorithm,
        &verified_provenance_certificate
            .contract
            .chain_fingerprint_algorithm,
        "notarization-receipt session chain_fingerprint_algorithm",
        &mut mismatches,
    );
    compare_string(
        &session.release_capsule_digest_manifest_sha256,
        &expected_manifest_sha256,
        "notarization-receipt session release_capsule_digest_manifest_sha256",
        &mut mismatches,
    );
    compare_usize(
        session.release_capsule_digest_manifest_entry_count,
        expected_manifest_entry_count,
        "notarization-receipt session release_capsule_digest_manifest_entry_count",
        &mut mismatches,
    );
    compare_string(
        &session.release_capsule_digest_algorithm,
        &verified_provenance_certificate
            .release_capsule_digest_manifest
            .digest_algorithm,
        "notarization-receipt session release_capsule_digest_algorithm",
        &mut mismatches,
    );
    compare_string(
        &session.notarization_receipt_summary,
        &expected_notarization_receipt_summary,
        "notarization-receipt session notarization_receipt_summary",
        &mut mismatches,
    );
    compare_string(
        &session.ledger_seal_summary,
        &expected_ledger_seal_summary,
        "notarization-receipt session ledger_seal_summary",
        &mut mismatches,
    );
    compare_string(
        &session.ledger_seal_sha256,
        &expected_ledger_seal_sha256,
        "notarization-receipt session ledger_seal_sha256",
        &mut mismatches,
    );
    compare_string(
        &session.ledger_seal_algorithm,
        NOTARIZATION_RECEIPT_LEDGER_SEAL_ALGORITHM,
        "notarization-receipt session ledger_seal_algorithm",
        &mut mismatches,
    );

    compare_string(
        &serialize_enum(&status.result),
        &expected_result,
        "notarization-receipt status result",
        &mut mismatches,
    );
    compare_optional_string(
        status.handoff_bundle_result.as_deref(),
        verified_provenance_certificate
            .contract
            .handoff_bundle_result
            .as_deref(),
        "notarization-receipt status handoff_bundle_result",
        &mut mismatches,
    );
    compare_optional_string(
        status.decision_packet_result.as_deref(),
        verified_provenance_certificate
            .contract
            .decision_packet_result
            .as_deref(),
        "notarization-receipt status decision_packet_result",
        &mut mismatches,
    );
    compare_optional_string(
        status.execute_frozen_result.as_deref(),
        verified_provenance_certificate
            .contract
            .execute_frozen_result
            .as_deref(),
        "notarization-receipt status execute_frozen_result",
        &mut mismatches,
    );
    compare_optional_string(
        status.current_pre_activation_gate_verdict.as_deref(),
        verified_provenance_certificate
            .contract
            .current_pre_activation_gate_verdict
            .as_deref(),
        "notarization-receipt status current_pre_activation_gate_verdict",
        &mut mismatches,
    );
    compare_optional_string(
        status.current_pre_activation_gate_reason.as_deref(),
        verified_provenance_certificate
            .contract
            .current_pre_activation_gate_reason
            .as_deref(),
        "notarization-receipt status current_pre_activation_gate_reason",
        &mut mismatches,
    );
    compare_string(
        &status.provenance_certificate_summary,
        &verified_provenance_certificate
            .contract
            .provenance_certificate_summary,
        "notarization-receipt status provenance_certificate_summary",
        &mut mismatches,
    );
    compare_string(
        &status.chain_fingerprint_summary,
        &verified_provenance_certificate
            .contract
            .chain_fingerprint_summary,
        "notarization-receipt status chain_fingerprint_summary",
        &mut mismatches,
    );
    compare_string(
        &status.chain_fingerprint_sha256,
        &verified_provenance_certificate
            .contract
            .chain_fingerprint_sha256,
        "notarization-receipt status chain_fingerprint_sha256",
        &mut mismatches,
    );
    compare_string(
        &status.chain_fingerprint_algorithm,
        &verified_provenance_certificate
            .contract
            .chain_fingerprint_algorithm,
        "notarization-receipt status chain_fingerprint_algorithm",
        &mut mismatches,
    );
    compare_string(
        &status.release_capsule_digest_manifest_sha256,
        &expected_manifest_sha256,
        "notarization-receipt status release_capsule_digest_manifest_sha256",
        &mut mismatches,
    );
    compare_usize(
        status.release_capsule_digest_manifest_entry_count,
        expected_manifest_entry_count,
        "notarization-receipt status release_capsule_digest_manifest_entry_count",
        &mut mismatches,
    );
    compare_string(
        &status.release_capsule_digest_algorithm,
        &verified_provenance_certificate
            .release_capsule_digest_manifest
            .digest_algorithm,
        "notarization-receipt status release_capsule_digest_algorithm",
        &mut mismatches,
    );
    compare_string(
        &status.notarization_receipt_summary,
        &expected_notarization_receipt_summary,
        "notarization-receipt status notarization_receipt_summary",
        &mut mismatches,
    );
    compare_string(
        &status.ledger_seal_summary,
        &expected_ledger_seal_summary,
        "notarization-receipt status ledger_seal_summary",
        &mut mismatches,
    );
    compare_string(
        &status.ledger_seal_sha256,
        &expected_ledger_seal_sha256,
        "notarization-receipt status ledger_seal_sha256",
        &mut mismatches,
    );
    compare_string(
        &status.ledger_seal_algorithm,
        NOTARIZATION_RECEIPT_LEDGER_SEAL_ALGORITHM,
        "notarization-receipt status ledger_seal_algorithm",
        &mut mismatches,
    );

    if !mismatches.is_empty() {
        return Ok(failure_report_for_verify(
            config,
            session_dir,
            &verified_provenance_certificate.contract,
            mismatches,
        ));
    }

    Ok(PackageNotarizationReceiptReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_notarization_receipt".to_string(),
        verdict:
            TinyLivePackageNotarizationReceiptVerdict::TinyLivePackageNotarizationReceiptVerifyOk,
        reason: format!(
            "provenance-certificate-native notarization receipt under {} is coherent and correctly binds one final ledger seal to verified provenance-certificate truth",
            session_dir.display()
        ),
        provenance_certificate_session_dir: config
            .provenance_certificate_session_dir
            .display()
            .to_string(),
        attestation_seal_session_dir: Some(
            verified_provenance_certificate
                .contract
                .attestation_seal_session_dir
                .display()
                .to_string(),
        ),
        release_capsule_session_dir: Some(
            verified_provenance_certificate
                .contract
                .release_capsule_session_dir
                .display()
                .to_string(),
        ),
        activation_ticket_session_dir: Some(
            verified_provenance_certificate
                .contract
                .activation_ticket_session_dir
                .display()
                .to_string(),
        ),
        review_receipt_session_dir: Some(
            verified_provenance_certificate
                .contract
                .review_receipt_session_dir
                .display()
                .to_string(),
        ),
        handoff_bundle_session_dir: Some(
            verified_provenance_certificate
                .contract
                .handoff_bundle_session_dir
                .display()
                .to_string(),
        ),
        decision_packet_session_dir: Some(
            verified_provenance_certificate
                .contract
                .decision_packet_session_dir
                .display()
                .to_string(),
        ),
        execute_frozen_session_dir: Some(
            verified_provenance_certificate
                .contract
                .execute_frozen_session_dir
                .display()
                .to_string(),
        ),
        turn_green_session_dir: Some(
            verified_provenance_certificate
                .contract
                .turn_green_session_dir
                .display()
                .to_string(),
        ),
        launch_packet_session_dir: Some(
            verified_provenance_certificate
                .contract
                .launch_packet_session_dir
                .display()
                .to_string(),
        ),
        package_dir: Some(
            verified_provenance_certificate
                .contract
                .package_dir
                .display()
                .to_string(),
        ),
        install_root: Some(
            verified_provenance_certificate
                .contract
                .install_root
                .display()
                .to_string(),
        ),
        target_service_name: Some(
            verified_provenance_certificate
                .contract
                .target_service_name
                .clone(),
        ),
        backend_command: Some(
            verified_provenance_certificate.contract.backend_command.clone(),
        ),
        wrapper_timeout_ms: Some(
            verified_provenance_certificate.contract.wrapper_timeout_ms,
        ),
        service_status_max_staleness_ms: Some(
            verified_provenance_certificate
                .contract
                .service_status_max_staleness_ms,
        ),
        session_dir: Some(session_dir.display().to_string()),
        notarization_receipt_session_path: Some(paths.session_path.display().to_string()),
        notarization_receipt_status_path: Some(paths.status_path.display().to_string()),
        archived_provenance_certificate_report_path: Some(
            paths.provenance_certificate_report_path.display().to_string(),
        ),
        archived_release_capsule_digest_manifest_path: Some(
            paths.release_capsule_digest_manifest_path
                .display()
                .to_string(),
        ),
        result: Some(expected_result),
        handoff_bundle_result: verified_provenance_certificate
            .contract
            .handoff_bundle_result
            .clone(),
        decision_packet_result: verified_provenance_certificate
            .contract
            .decision_packet_result
            .clone(),
        execute_frozen_result: verified_provenance_certificate
            .contract
            .execute_frozen_result
            .clone(),
        provenance_certificate_step: Some(expected_provenance_certificate_step),
        verify_provenance_certificate_command_summary: Some(
            verify_provenance_certificate_command_summary(
                &config.provenance_certificate_session_dir,
            ),
        ),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            verified_provenance_certificate
                .contract
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        provenance_certificate_summary: Some(
            verified_provenance_certificate
                .contract
                .provenance_certificate_summary
                .clone(),
        ),
        chain_fingerprint_summary: Some(
            verified_provenance_certificate
                .contract
                .chain_fingerprint_summary
                .clone(),
        ),
        chain_fingerprint_sha256: Some(
            verified_provenance_certificate
                .contract
                .chain_fingerprint_sha256
                .clone(),
        ),
        chain_fingerprint_algorithm: Some(
            verified_provenance_certificate
                .contract
                .chain_fingerprint_algorithm
                .clone(),
        ),
        release_capsule_digest_manifest_sha256: Some(expected_manifest_sha256),
        release_capsule_digest_manifest_entry_count: Some(expected_manifest_entry_count),
        release_capsule_digest_algorithm: Some(
            verified_provenance_certificate
                .release_capsule_digest_manifest
                .digest_algorithm
                .clone(),
        ),
        notarization_receipt_summary: Some(expected_notarization_receipt_summary),
        ledger_seal_summary: Some(expected_ledger_seal_summary),
        ledger_seal_sha256: Some(expected_ledger_seal_sha256),
        ledger_seal_algorithm: Some(
            NOTARIZATION_RECEIPT_LEDGER_SEAL_ALGORITHM.to_string(),
        ),
        release_capsule_digest_manifest: Some(
            verified_provenance_certificate.release_capsule_digest_manifest,
        ),
        current_pre_activation_gate_verdict: verified_provenance_certificate
            .contract
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: verified_provenance_certificate
            .contract
            .current_pre_activation_gate_reason
            .clone(),
        verification_mismatches: mismatches,
        explicit_statement: NOTARIZATION_RECEIPT_VERIFY_EXPLICIT_STATEMENT.to_string(),
    })
}

fn verify_provenance_certificate_command_summary(
    provenance_certificate_session_dir: &Path,
) -> String {
    format!(
        "verify immutable provenance-certificate session under {} before freezing the final notarization receipt",
        provenance_certificate_session_dir.display()
    )
}

fn classify_notarization_contract(
    contract: &package_provenance_certificate_notarization_receipt::LivePackageProvenanceCertificateContractView,
    assume_verified: bool,
) -> (
    LivePackageNotarizationReceiptResult,
    TinyLivePackageNotarizationReceiptVerdict,
    String,
) {
    if !assume_verified {
        return (
            LivePackageNotarizationReceiptResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageNotarizationReceiptVerdict::TinyLivePackageNotarizationReceiptRefusedNowByInvalidOrDriftedContract,
            "the provenance-certificate chain is not verified, so this notarization receipt remains refused"
                .to_string(),
        );
    }
    match contract.result.as_deref() {
        Some("refused_now_by_stage3") => (
            LivePackageNotarizationReceiptResult::RefusedNowByStage3,
            TinyLivePackageNotarizationReceiptVerdict::TinyLivePackageNotarizationReceiptRefusedNowByStage3,
            "the immutable provenance certificate remains refused now by Stage 3, so this notarization receipt remains an explicit refusal record"
                .to_string(),
        ),
        Some("refused_now_by_pre_activation_gate") => (
            LivePackageNotarizationReceiptResult::RefusedNowByPreActivationGate,
            TinyLivePackageNotarizationReceiptVerdict::TinyLivePackageNotarizationReceiptRefusedNowByPreActivationGate,
            "the immutable provenance certificate remains refused now by the pre-activation gate, so this notarization receipt remains an explicit refusal record"
                .to_string(),
        ),
        Some("ready_for_manual_execution_when_gate_turns_green") => (
            LivePackageNotarizationReceiptResult::ReadyForManualExecutionWhenGateTurnsGreen,
            TinyLivePackageNotarizationReceiptVerdict::TinyLivePackageNotarizationReceiptReadyForManualExecutionWhenGateTurnsGreen,
            "the immutable provenance certificate is coherent and this notarization receipt is ready for manual execution when gate truth turns green"
                .to_string(),
        ),
        _ => (
            LivePackageNotarizationReceiptResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageNotarizationReceiptVerdict::TinyLivePackageNotarizationReceiptRefusedNowByInvalidOrDriftedContract,
            "the verified provenance-certificate chain is invalid, drifted, or non-runnable, so this notarization receipt remains refused"
                .to_string(),
        ),
    }
}

fn classify_verified_provenance_certificate(
    verified: &package_provenance_certificate_notarization_receipt::VerifiedLivePackageProvenanceCertificateNotarizationReceiptStep,
) -> (
    LivePackageNotarizationReceiptResult,
    TinyLivePackageNotarizationReceiptVerdict,
    String,
) {
    if verified.verdict != "tiny_live_package_provenance_certificate_verify_ok" {
        return (
            LivePackageNotarizationReceiptResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageNotarizationReceiptVerdict::TinyLivePackageNotarizationReceiptRefusedNowByInvalidOrDriftedContract,
            "the immutable provenance certificate did not verify cleanly, so this notarization receipt remains refused"
                .to_string(),
        );
    }
    classify_notarization_contract(&verified.contract, true)
}

fn notarization_receipt_summary(
    result: LivePackageNotarizationReceiptResult,
    reviewed_frozen_live_cutover_controller_command_summary: &str,
) -> String {
    match result {
        LivePackageNotarizationReceiptResult::RefusedNowByStage3 => {
            "Notarization receipt outcome: the immutable reviewed chain remains refused now by Stage 3; archive this final ledger-style receipt as refusal evidence only and do not run the frozen controller.".to_string()
        }
        LivePackageNotarizationReceiptResult::RefusedNowByPreActivationGate => {
            "Notarization receipt outcome: the immutable reviewed chain remains refused now by the pre-activation gate; archive this final ledger-style receipt as refusal evidence only and do not run the frozen controller.".to_string()
        }
        LivePackageNotarizationReceiptResult::RefusedNowByInvalidOrDriftedContract => {
            "Notarization receipt outcome: the immutable reviewed chain is invalid or drifted; archive this final ledger-style receipt for audit only and mint a new coherent receipt before any later execution record.".to_string()
        }
        LivePackageNotarizationReceiptResult::ReadyForManualExecutionWhenGateTurnsGreen => format!(
            "Notarization receipt outcome: this immutable ledger-style receipt is ready for manual execution when gate truth turns green; the exact reviewed frozen controller command remains: {reviewed_frozen_live_cutover_controller_command_summary}"
        ),
    }
}

fn ledger_seal_summary(
    algorithm: &str,
    ledger_seal_sha256: &str,
    chain_fingerprint_sha256: &str,
) -> String {
    format!(
        "Ledger seal identity: {algorithm} {ledger_seal_sha256} over the verified refusal-vs-ready classification, the reviewed frozen controller summary, and the canonical chain fingerprint {chain_fingerprint_sha256}; any identity drift invalidates this notarization receipt"
    )
}

fn notarization_receipt_paths(session_dir: &Path) -> PackageNotarizationReceiptPaths {
    PackageNotarizationReceiptPaths {
        session_path: session_dir
            .join("tiny_live_activation_package_notarization_receipt.session.json"),
        status_path: session_dir
            .join("tiny_live_activation_package_notarization_receipt.status.json"),
        provenance_certificate_report_path: session_dir.join(
            "tiny_live_activation_package_notarization_receipt.provenance_certificate.report.json",
        ),
        release_capsule_digest_manifest_path: session_dir.join(
            "tiny_live_activation_package_notarization_receipt.release_capsule.digest_manifest.json",
        ),
    }
}

fn ensure_clean_notarization_receipt_session_dir(session_dir: &Path) -> Result<()> {
    if session_dir.exists() {
        bail!(
            "refusing to overwrite existing notarization-receipt session dir {}",
            session_dir.display()
        );
    }
    fs::create_dir_all(session_dir).with_context(|| {
        format!(
            "failed creating notarization-receipt session dir {}",
            session_dir.display()
        )
    })
}

fn planned_session(
    config: &Config,
    session_dir: &Path,
    contract: &package_provenance_certificate_notarization_receipt::LivePackageProvenanceCertificateContractView,
    result: LivePackageNotarizationReceiptResult,
    manifest_sha256: &str,
    manifest_entry_count: usize,
    ledger_seal_sha256: &str,
) -> PackageNotarizationReceiptSession {
    PackageNotarizationReceiptSession {
        session_version: NOTARIZATION_RECEIPT_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        provenance_certificate_session_dir: config
            .provenance_certificate_session_dir
            .display()
            .to_string(),
        attestation_seal_session_dir: contract.attestation_seal_session_dir.display().to_string(),
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
        verify_provenance_certificate_command_summary:
            verify_provenance_certificate_command_summary(
                &config.provenance_certificate_session_dir,
            ),
        reviewed_frozen_live_cutover_controller_command_summary: contract
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        provenance_certificate_summary: contract.provenance_certificate_summary.clone(),
        chain_fingerprint_summary: contract.chain_fingerprint_summary.clone(),
        chain_fingerprint_sha256: contract.chain_fingerprint_sha256.clone(),
        chain_fingerprint_algorithm: contract.chain_fingerprint_algorithm.clone(),
        release_capsule_digest_manifest_sha256: manifest_sha256.to_string(),
        release_capsule_digest_manifest_entry_count: manifest_entry_count,
        release_capsule_digest_algorithm: contract.release_capsule_digest_algorithm.clone(),
        notarization_receipt_summary: notarization_receipt_summary(
            result,
            &contract.reviewed_frozen_live_cutover_controller_command_summary,
        ),
        ledger_seal_summary: ledger_seal_summary(
            NOTARIZATION_RECEIPT_LEDGER_SEAL_ALGORITHM,
            ledger_seal_sha256,
            &contract.chain_fingerprint_sha256,
        ),
        ledger_seal_sha256: ledger_seal_sha256.to_string(),
        ledger_seal_algorithm: NOTARIZATION_RECEIPT_LEDGER_SEAL_ALGORITHM.to_string(),
        explicit_statement: NOTARIZATION_RECEIPT_SESSION_EXPLICIT_STATEMENT.to_string(),
    }
}

fn report_from_status(
    config: &Config,
    session_dir: &Path,
    paths: &PackageNotarizationReceiptPaths,
    contract: &package_provenance_certificate_notarization_receipt::LivePackageProvenanceCertificateContractView,
    verdict: TinyLivePackageNotarizationReceiptVerdict,
    status: &PackageNotarizationReceiptStatus,
    release_capsule_digest_manifest:
        package_provenance_certificate_notarization_receipt::ReleaseCapsuleDigestManifest,
) -> PackageNotarizationReceiptReport {
    PackageNotarizationReceiptReport {
        generated_at: Utc::now(),
        mode: "run_live_package_notarization_receipt".to_string(),
        verdict,
        reason: status.reason.clone(),
        provenance_certificate_session_dir: config
            .provenance_certificate_session_dir
            .display()
            .to_string(),
        attestation_seal_session_dir: Some(
            contract.attestation_seal_session_dir.display().to_string(),
        ),
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
        notarization_receipt_session_path: Some(paths.session_path.display().to_string()),
        notarization_receipt_status_path: Some(paths.status_path.display().to_string()),
        archived_provenance_certificate_report_path: Some(
            paths
                .provenance_certificate_report_path
                .display()
                .to_string(),
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
        provenance_certificate_step: status.provenance_certificate_step.clone(),
        verify_provenance_certificate_command_summary: Some(
            verify_provenance_certificate_command_summary(
                &config.provenance_certificate_session_dir,
            ),
        ),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            contract
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        provenance_certificate_summary: Some(status.provenance_certificate_summary.clone()),
        chain_fingerprint_summary: Some(status.chain_fingerprint_summary.clone()),
        chain_fingerprint_sha256: Some(status.chain_fingerprint_sha256.clone()),
        chain_fingerprint_algorithm: Some(status.chain_fingerprint_algorithm.clone()),
        release_capsule_digest_manifest_sha256: Some(
            status.release_capsule_digest_manifest_sha256.clone(),
        ),
        release_capsule_digest_manifest_entry_count: Some(
            status.release_capsule_digest_manifest_entry_count,
        ),
        release_capsule_digest_algorithm: Some(status.release_capsule_digest_algorithm.clone()),
        notarization_receipt_summary: Some(status.notarization_receipt_summary.clone()),
        ledger_seal_summary: Some(status.ledger_seal_summary.clone()),
        ledger_seal_sha256: Some(status.ledger_seal_sha256.clone()),
        ledger_seal_algorithm: Some(status.ledger_seal_algorithm.clone()),
        release_capsule_digest_manifest: Some(release_capsule_digest_manifest),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        verification_mismatches: Vec::new(),
        explicit_statement: status.explicit_statement.clone(),
    }
}

fn failure_report_without_contract(
    config: &Config,
    verdict: TinyLivePackageNotarizationReceiptVerdict,
    reason: String,
) -> PackageNotarizationReceiptReport {
    PackageNotarizationReceiptReport {
        generated_at: Utc::now(),
        mode: match config.mode {
            Mode::PlanLivePackageNotarizationReceipt => {
                "plan_live_package_notarization_receipt".to_string()
            }
            Mode::RenderLivePackageNotarizationReceipt => {
                "render_live_package_notarization_receipt".to_string()
            }
            Mode::RunLivePackageNotarizationReceipt => {
                "run_live_package_notarization_receipt".to_string()
            }
            Mode::VerifyLivePackageNotarizationReceipt => {
                "verify_live_package_notarization_receipt".to_string()
            }
        },
        verdict,
        reason,
        provenance_certificate_session_dir: config
            .provenance_certificate_session_dir
            .display()
            .to_string(),
        attestation_seal_session_dir: None,
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
        notarization_receipt_session_path: None,
        notarization_receipt_status_path: None,
        archived_provenance_certificate_report_path: None,
        archived_release_capsule_digest_manifest_path: None,
        result: Some("refused_now_by_invalid_or_drifted_contract".to_string()),
        handoff_bundle_result: None,
        decision_packet_result: None,
        execute_frozen_result: None,
        provenance_certificate_step: None,
        verify_provenance_certificate_command_summary: Some(
            verify_provenance_certificate_command_summary(
                &config.provenance_certificate_session_dir,
            ),
        ),
        reviewed_frozen_live_cutover_controller_command_summary: None,
        provenance_certificate_summary: None,
        chain_fingerprint_summary: None,
        chain_fingerprint_sha256: None,
        chain_fingerprint_algorithm: None,
        release_capsule_digest_manifest_sha256: None,
        release_capsule_digest_manifest_entry_count: None,
        release_capsule_digest_algorithm: None,
        notarization_receipt_summary: None,
        ledger_seal_summary: None,
        ledger_seal_sha256: None,
        ledger_seal_algorithm: None,
        release_capsule_digest_manifest: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this immutable notarization receipt refused before persisting because the provenance-certificate artifact could not be verified coherently"
                .to_string(),
    }
}

fn failure_report_for_verify(
    config: &Config,
    session_dir: &Path,
    contract: &package_provenance_certificate_notarization_receipt::LivePackageProvenanceCertificateContractView,
    mismatches: Vec<String>,
) -> PackageNotarizationReceiptReport {
    let paths = notarization_receipt_paths(session_dir);
    let reason = mismatches.first().cloned().unwrap_or_else(|| {
        "provenance-certificate-native notarization receipt verification failed".to_string()
    });
    PackageNotarizationReceiptReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_notarization_receipt".to_string(),
        verdict:
            TinyLivePackageNotarizationReceiptVerdict::TinyLivePackageNotarizationReceiptVerifyInvalid,
        reason,
        provenance_certificate_session_dir: config
            .provenance_certificate_session_dir
            .display()
            .to_string(),
        attestation_seal_session_dir: Some(contract.attestation_seal_session_dir.display().to_string()),
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
        notarization_receipt_session_path: Some(paths.session_path.display().to_string()),
        notarization_receipt_status_path: Some(paths.status_path.display().to_string()),
        archived_provenance_certificate_report_path: Some(
            paths.provenance_certificate_report_path.display().to_string(),
        ),
        archived_release_capsule_digest_manifest_path: Some(
            paths.release_capsule_digest_manifest_path.display().to_string(),
        ),
        result: None,
        handoff_bundle_result: contract.handoff_bundle_result.clone(),
        decision_packet_result: contract.decision_packet_result.clone(),
        execute_frozen_result: contract.execute_frozen_result.clone(),
        provenance_certificate_step: None,
        verify_provenance_certificate_command_summary: Some(
            verify_provenance_certificate_command_summary(
                &config.provenance_certificate_session_dir,
            ),
        ),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            contract
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        provenance_certificate_summary: Some(contract.provenance_certificate_summary.clone()),
        chain_fingerprint_summary: Some(contract.chain_fingerprint_summary.clone()),
        chain_fingerprint_sha256: Some(contract.chain_fingerprint_sha256.clone()),
        chain_fingerprint_algorithm: Some(contract.chain_fingerprint_algorithm.clone()),
        release_capsule_digest_manifest_sha256: Some(
            contract.release_capsule_digest_manifest_sha256.clone(),
        ),
        release_capsule_digest_manifest_entry_count: Some(
            contract.release_capsule_digest_manifest_entry_count,
        ),
        release_capsule_digest_algorithm: Some(
            contract.release_capsule_digest_algorithm.clone(),
        ),
        notarization_receipt_summary: None,
        ledger_seal_summary: None,
        ledger_seal_sha256: None,
        ledger_seal_algorithm: Some(
            NOTARIZATION_RECEIPT_LEDGER_SEAL_ALGORITHM.to_string(),
        ),
        release_capsule_digest_manifest: None,
        current_pre_activation_gate_verdict: contract.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: contract.current_pre_activation_gate_reason.clone(),
        verification_mismatches: mismatches,
        explicit_statement:
            "this verification failure means the immutable notarization receipt no longer proves the exact reviewed frozen controller context or the canonical nested chain identity"
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
    step: &Option<PackageNotarizationReceiptStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<serde_json::Value> {
    let Some(step) = step else {
        mismatches.push(format!(
            "{label} is missing from notarization-receipt status"
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
) -> PackageNotarizationReceiptStepArtifact {
    PackageNotarizationReceiptStepArtifact {
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
    actual: Option<&PackageNotarizationReceiptStepArtifact>,
    expected: &PackageNotarizationReceiptStepArtifact,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    let Some(actual) = actual else {
        mismatches.push(format!(
            "{label} is missing from notarization-receipt status"
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

fn compute_ledger_seal_sha256(
    result: &str,
    handoff_bundle_result: Option<&str>,
    decision_packet_result: Option<&str>,
    execute_frozen_result: Option<&str>,
    current_pre_activation_gate_verdict: Option<&str>,
    current_pre_activation_gate_reason: Option<&str>,
    reviewed_frozen_live_cutover_controller_command_summary: &str,
    chain_fingerprint_sha256: &str,
    chain_fingerprint_algorithm: &str,
    release_capsule_digest_manifest_sha256: &str,
    release_capsule_digest_manifest_entry_count: usize,
    release_capsule_digest_algorithm: &str,
) -> Result<String> {
    canonical_sha256_hex(&LedgerSealPayload {
        result,
        handoff_bundle_result,
        decision_packet_result,
        execute_frozen_result,
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        reviewed_frozen_live_cutover_controller_command_summary,
        chain_fingerprint_sha256,
        chain_fingerprint_algorithm,
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
            "failed writing notarization-receipt script to {}",
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

fn render_report_lines(report: &PackageNotarizationReceiptReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "provenance_certificate_session_dir={}",
            report.provenance_certificate_session_dir
        ),
    ];
    if let Some(value) = &report.result {
        lines.push(format!("result={value}"));
    }
    if let Some(value) = &report.notarization_receipt_summary {
        lines.push(format!("notarization_receipt_summary={value}"));
    }
    if let Some(value) = &report.ledger_seal_summary {
        lines.push(format!("ledger_seal_summary={value}"));
    }
    if let Some(value) = &report.ledger_seal_sha256 {
        lines.push(format!("ledger_seal_sha256={value}"));
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
            "notarization_receipt_ready",
            provenance_certificate_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_notarization_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageNotarizationReceiptVerdict::TinyLivePackageNotarizationReceiptReadyForManualExecutionWhenGateTurnsGreen,
            "{run:#?}"
        );

        let verify =
            verify_live_package_notarization_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageNotarizationReceiptVerdict::TinyLivePackageNotarizationReceiptVerifyOk,
            "{:#?}",
            verify.verification_mismatches
        );
    }

    #[test]
    fn stage3_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "notarization_receipt_stage3",
            provenance_certificate_verify_report(
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "blocked",
                "stage3 blocked",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_notarization_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageNotarizationReceiptVerdict::TinyLivePackageNotarizationReceiptRefusedNowByStage3
        );
    }

    #[test]
    fn pre_activation_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "notarization_receipt_pre_activation",
            provenance_certificate_verify_report(
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
                "blocked",
                "pre-activation blocked",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_notarization_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageNotarizationReceiptVerdict::TinyLivePackageNotarizationReceiptRefusedNowByPreActivationGate
        );
    }

    #[test]
    fn drifted_provenance_certificate_contract_is_refused() {
        let fixture = fake_command_fixture(
            "notarization_receipt_drifted",
            provenance_certificate_verify_report(
                "refused_now_by_invalid_or_drifted_contract",
                "refused_now_by_invalid_or_drifted_contract",
                "refused_now_by_invalid_or_drifted_contract",
                "refused_now_by_invalid_or_drifted_contract",
                "green",
                "contract drifted",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_notarization_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageNotarizationReceiptVerdict::TinyLivePackageNotarizationReceiptRefusedNowByInvalidOrDriftedContract
        );
    }

    #[test]
    fn run_refuses_managed_surface_overlap_before_writing_any_artifacts() {
        let fixture = fake_command_fixture(
            "notarization_receipt_overlap",
            provenance_certificate_verify_report(
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
            .join("notarization-receipt-session");
        let config = Config {
            mode: Mode::RunLivePackageNotarizationReceipt,
            provenance_certificate_session_dir: fixture.provenance_certificate_session_dir.clone(),
            confirmed_decision_packet_session_dir: Some(
                fixture.decision_packet_session_dir.clone(),
            ),
            session_dir: Some(unsafe_session_dir.clone()),
            output_path: None,
            json: true,
        };

        let error = run_live_package_notarization_receipt_report(&config).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("overlaps the managed live target surface"),
            "{error:#}"
        );
        let paths = notarization_receipt_paths(&unsafe_session_dir);
        assert!(!paths.session_path.exists());
        assert!(!paths.status_path.exists());
        assert!(!paths.release_capsule_digest_manifest_path.exists());
    }

    #[test]
    fn verify_rejects_tampered_provenance_certificate_step_path() {
        let fixture = fake_command_fixture(
            "notarization_receipt_tampered_step",
            provenance_certificate_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_notarization_receipt_report(&fixture.run_config()).unwrap();
        let paths = notarization_receipt_paths(&fixture.notarization_receipt_session_dir);
        let mut status: PackageNotarizationReceiptStatus = load_json(&paths.status_path).unwrap();
        let foreign = fixture
            .fixture_dir
            .join("foreign.provenance-certificate.report.json");
        fs::write(&foreign, "{}").unwrap();
        status.provenance_certificate_step.as_mut().unwrap().path = foreign.display().to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify =
            verify_live_package_notarization_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageNotarizationReceiptVerdict::TinyLivePackageNotarizationReceiptVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_notarization_receipt_text() {
        let fixture = fake_command_fixture(
            "notarization_receipt_tampered_text",
            provenance_certificate_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_notarization_receipt_report(&fixture.run_config()).unwrap();
        let paths = notarization_receipt_paths(&fixture.notarization_receipt_session_dir);
        let mut session: PackageNotarizationReceiptSession =
            load_json(&paths.session_path).unwrap();
        session.notarization_receipt_summary = "tampered summary".to_string();
        persist_json(&paths.session_path, &session).unwrap();

        let verify =
            verify_live_package_notarization_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageNotarizationReceiptVerdict::TinyLivePackageNotarizationReceiptVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_status_result() {
        let fixture = fake_command_fixture(
            "notarization_receipt_tampered_status_result",
            provenance_certificate_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_notarization_receipt_report(&fixture.run_config()).unwrap();
        let paths = notarization_receipt_paths(&fixture.notarization_receipt_session_dir);
        let mut status: PackageNotarizationReceiptStatus = load_json(&paths.status_path).unwrap();
        status.result = LivePackageNotarizationReceiptResult::RefusedNowByStage3;
        persist_json(&paths.status_path, &status).unwrap();

        let verify =
            verify_live_package_notarization_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageNotarizationReceiptVerdict::TinyLivePackageNotarizationReceiptVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_status_gate_fields() {
        let fixture = fake_command_fixture(
            "notarization_receipt_tampered_status_gate",
            provenance_certificate_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_notarization_receipt_report(&fixture.run_config()).unwrap();
        let paths = notarization_receipt_paths(&fixture.notarization_receipt_session_dir);
        let mut status: PackageNotarizationReceiptStatus = load_json(&paths.status_path).unwrap();
        status.current_pre_activation_gate_verdict = Some("tampered-gate".to_string());
        status.current_pre_activation_gate_reason = Some("tampered gate reason".to_string());
        persist_json(&paths.status_path, &status).unwrap();

        let verify =
            verify_live_package_notarization_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageNotarizationReceiptVerdict::TinyLivePackageNotarizationReceiptVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_nested_provenance_certificate_report_content() {
        let fixture = fake_command_fixture(
            "notarization_receipt_tampered_nested_report",
            provenance_certificate_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_notarization_receipt_report(&fixture.run_config()).unwrap();
        let paths = notarization_receipt_paths(&fixture.notarization_receipt_session_dir);
        let mut report: serde_json::Value =
            load_json(&paths.provenance_certificate_report_path).unwrap();
        report.as_object_mut().unwrap().insert(
            "reason".to_string(),
            json!("tampered nested provenance-certificate reason"),
        );
        persist_json(&paths.provenance_certificate_report_path, &report).unwrap();

        let verify =
            verify_live_package_notarization_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageNotarizationReceiptVerdict::TinyLivePackageNotarizationReceiptVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_retimed_nested_provenance_certificate_report_and_step_generated_at() {
        let fixture = fake_command_fixture(
            "notarization_receipt_retimed_nested_report",
            provenance_certificate_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_notarization_receipt_report(&fixture.run_config()).unwrap();
        let paths = notarization_receipt_paths(&fixture.notarization_receipt_session_dir);
        let mut report: serde_json::Value =
            load_json(&paths.provenance_certificate_report_path).unwrap();
        let archived_generated_at =
            DateTime::parse_from_rfc3339(report["generated_at"].as_str().unwrap())
                .unwrap()
                .with_timezone(&Utc);
        let forged_generated_at = archived_generated_at + chrono::Duration::seconds(61);
        report.as_object_mut().unwrap().insert(
            "generated_at".to_string(),
            json!(forged_generated_at.to_rfc3339()),
        );
        persist_json(&paths.provenance_certificate_report_path, &report).unwrap();

        let mut status: PackageNotarizationReceiptStatus = load_json(&paths.status_path).unwrap();
        status
            .provenance_certificate_step
            .as_mut()
            .unwrap()
            .generated_at = forged_generated_at;
        persist_json(&paths.status_path, &status).unwrap();

        let verify =
            verify_live_package_notarization_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageNotarizationReceiptVerdict::TinyLivePackageNotarizationReceiptVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_chain_fingerprint_identity_fields() {
        let fixture = fake_command_fixture(
            "notarization_receipt_tampered_chain_fingerprint",
            provenance_certificate_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_notarization_receipt_report(&fixture.run_config()).unwrap();
        let paths = notarization_receipt_paths(&fixture.notarization_receipt_session_dir);
        let mut status: PackageNotarizationReceiptStatus = load_json(&paths.status_path).unwrap();
        status.chain_fingerprint_sha256 = "deadbeef".to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify =
            verify_live_package_notarization_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageNotarizationReceiptVerdict::TinyLivePackageNotarizationReceiptVerifyInvalid
        );
    }

    struct FakeCommandFixture {
        fixture_dir: PathBuf,
        bin_dir: PathBuf,
        provenance_certificate_session_dir: PathBuf,
        notarization_receipt_session_dir: PathBuf,
        decision_packet_session_dir: PathBuf,
        install_root: PathBuf,
    }

    impl FakeCommandFixture {
        fn run_config(&self) -> Config {
            Config {
                mode: Mode::RunLivePackageNotarizationReceipt,
                provenance_certificate_session_dir: self.provenance_certificate_session_dir.clone(),
                confirmed_decision_packet_session_dir: Some(
                    self.decision_packet_session_dir.clone(),
                ),
                session_dir: Some(self.notarization_receipt_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn verify_config(&self) -> Config {
            Config {
                mode: Mode::VerifyLivePackageNotarizationReceipt,
                provenance_certificate_session_dir: self.provenance_certificate_session_dir.clone(),
                confirmed_decision_packet_session_dir: Some(
                    self.decision_packet_session_dir.clone(),
                ),
                session_dir: Some(self.notarization_receipt_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }
    }

    fn fake_command_fixture(label: &str, verify_report: serde_json::Value) -> FakeCommandFixture {
        let fixture_dir = temp_dir(label);
        let bin_dir = fixture_dir.join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        let provenance_certificate_session_dir = fixture_dir.join("provenance-certificate-session");
        let notarization_receipt_session_dir = fixture_dir.join("notarization-receipt-session");
        let attestation_seal_session_dir = fixture_dir.join("attestation-seal-session");
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
        for path in [
            &provenance_certificate_session_dir,
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
        ] {
            fs::create_dir_all(path).unwrap();
        }

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
        let chain_fingerprint_sha256 = "canonical-chain-sha256".to_string();

        persist_json(
            &provenance_certificate_session_dir
                .join("tiny_live_activation_package_provenance_certificate.session.json"),
            &json!({
                "attestation_seal_session_dir": attestation_seal_session_dir.display().to_string(),
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
                "verify_attestation_seal_command_summary": format!(
                    "verify immutable attestation-seal session under {} before freezing the final provenance certificate",
                    attestation_seal_session_dir.display()
                ),
                "reviewed_frozen_live_cutover_controller_command_summary": format!(
                    "copybot_tiny_live_activation_package_live_cutover --package-dir {} --install-root {} --target-service solana-copy-bot.service --backend-command {} --wrapper-timeout-ms 1200 --service-status-max-staleness-ms 4500 --session-dir {} --run-live-package-cutover",
                    shell_escape_path(&package_dir),
                    shell_escape_path(&install_root),
                    shell_escape_path(&backend_command),
                    shell_escape_path(&fixture_dir.join("frozen-live-cutover-session")),
                ),
                "provenance_certificate_summary": "Provenance certificate summary.",
                "chain_fingerprint_summary": "Canonical chain fingerprint summary.",
                "chain_fingerprint_sha256": chain_fingerprint_sha256,
                "chain_fingerprint_algorithm": "sha256",
                "release_capsule_digest_manifest_sha256": manifest_sha256,
                "release_capsule_digest_manifest_entry_count": 3,
                "release_capsule_digest_algorithm": "sha256",
                "explicit_statement": "provenance-certificate statement"
            }),
        )
        .unwrap();
        persist_json(
            &provenance_certificate_session_dir
                .join("tiny_live_activation_package_provenance_certificate.status.json"),
            &json!({
                "result": verify_report.get("result").and_then(|value| value.as_str()).unwrap(),
                "handoff_bundle_result": verify_report.get("handoff_bundle_result").and_then(|value| value.as_str()).unwrap(),
                "decision_packet_result": verify_report.get("decision_packet_result").and_then(|value| value.as_str()).unwrap(),
                "execute_frozen_result": verify_report.get("execute_frozen_result").and_then(|value| value.as_str()).unwrap(),
                "current_pre_activation_gate_verdict": verify_report.get("current_pre_activation_gate_verdict").and_then(|value| value.as_str()),
                "current_pre_activation_gate_reason": verify_report.get("current_pre_activation_gate_reason").and_then(|value| value.as_str()),
                "provenance_certificate_summary": "Provenance certificate summary.",
                "chain_fingerprint_summary": "Canonical chain fingerprint summary.",
                "chain_fingerprint_sha256": "canonical-chain-sha256",
                "chain_fingerprint_algorithm": "sha256",
                "release_capsule_digest_manifest_sha256": manifest_sha256,
                "release_capsule_digest_manifest_entry_count": 3,
                "release_capsule_digest_algorithm": "sha256",
                "explicit_statement": "provenance-certificate status statement"
            }),
        )
        .unwrap();
        persist_json(
            &provenance_certificate_session_dir.join(
                "tiny_live_activation_package_provenance_certificate.attestation_seal.report.json",
            ),
            &json!({
                "decision_packet_session_dir": decision_packet_session_dir.display().to_string()
            }),
        )
        .unwrap();
        persist_json(
            &provenance_certificate_session_dir
                .join("tiny_live_activation_package_provenance_certificate.release_capsule.digest_manifest.json"),
            &manifest,
        )
        .unwrap();

        let mut verify_report = verify_report;
        bind_provenance_certificate_report_to_fixture(
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
        let verify_path = fixture_dir.join("provenance-certificate.verify.json");
        persist_json(&verify_path, &verify_report).unwrap();
        write_fake_provenance_certificate_verify_command(
            &bin_dir.join("copybot_tiny_live_activation_package_provenance_certificate"),
            &verify_path,
            &attestation_seal_session_dir,
            &decision_packet_session_dir,
            &provenance_certificate_session_dir,
        );

        FakeCommandFixture {
            fixture_dir,
            bin_dir,
            provenance_certificate_session_dir,
            notarization_receipt_session_dir,
            decision_packet_session_dir,
            install_root,
        }
    }

    fn bind_provenance_certificate_report_to_fixture(
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
            "attestation_seal_session_dir".to_string(),
            json!(attestation_seal_session_dir.display().to_string()),
        );
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
            "verify_attestation_seal_command_summary".to_string(),
            json!(format!(
                "verify immutable attestation-seal session under {} before freezing the final provenance certificate",
                attestation_seal_session_dir.display()
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
            "provenance_certificate_summary".to_string(),
            json!("Provenance certificate summary."),
        );
        object.insert(
            "chain_fingerprint_summary".to_string(),
            json!("Canonical chain fingerprint summary."),
        );
        object.insert(
            "chain_fingerprint_sha256".to_string(),
            json!("canonical-chain-sha256"),
        );
        object.insert("chain_fingerprint_algorithm".to_string(), json!("sha256"));
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
            json!("provenance-certificate verify statement"),
        );
    }

    fn provenance_certificate_verify_report(
        result: &str,
        handoff_bundle_result: &str,
        decision_packet_result: &str,
        execute_frozen_result: &str,
        gate_verdict: &str,
        gate_reason: &str,
    ) -> serde_json::Value {
        json!({
            "generated_at": Utc::now(),
            "mode": "verify_live_package_provenance_certificate",
            "verdict": "tiny_live_package_provenance_certificate_verify_ok",
            "reason": "provenance-certificate chain is coherent",
            "result": result,
            "handoff_bundle_result": handoff_bundle_result,
            "decision_packet_result": decision_packet_result,
            "execute_frozen_result": execute_frozen_result,
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason,
        })
    }

    fn write_fake_provenance_certificate_verify_command(
        path: &Path,
        verify_path: &Path,
        expected_attestation_seal_session_dir: &Path,
        expected_decision_packet_session_dir: &Path,
        provenance_certificate_session_dir: &Path,
    ) {
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --attestation-seal-session-dir {} \"*) : ;;\n  *) echo 'unexpected attestation-seal-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --confirm-decision-packet-session-dir {} \"*) : ;;\n  *) echo 'unexpected decision-packet-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected provenance-certificate session dir' >&2; exit 1;;\nesac\ncat '{}'\n",
            shell_escape_path(expected_attestation_seal_session_dir),
            shell_escape_path(expected_decision_packet_session_dir),
            shell_escape_path(provenance_certificate_session_dir),
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
