use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_app::tiny_live_activation::install_target_managed_surface::validate_turn_green_session_dir as validate_managed_surface_session_dir;
use copybot_app::tiny_live_activation::package_closure_certificate_finality_receipt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: copybot_tiny_live_activation_package_finality_receipt --closure-certificate-session-dir <path> [--confirm-decision-packet-session-dir <path>] [--session-dir <path>] [--output <path>] [--json] (--plan-live-package-finality-receipt | --render-live-package-finality-receipt | --run-live-package-finality-receipt | --verify-live-package-finality-receipt)";
const FINALITY_RECEIPT_SESSION_VERSION: &str = "1";
const FINALITY_RECEIPT_STATUS_VERSION: &str = "1";
const FINALITY_RECEIPT_ALGORITHM: &str = "sha256";
const FINALITY_RECEIPT_SESSION_EXPLICIT_STATEMENT: &str =
    "the verified closure-certificate session is the primary input here; this immutable finality receipt freezes one final end-state identity over the fully closed chain without enabling production execution";
const FINALITY_RECEIPT_STATUS_EXPLICIT_STATEMENT: &str =
    "this finality receipt is archival and read-only; it never executes or enables the frozen controller";
const FINALITY_RECEIPT_VERIFY_EXPLICIT_STATEMENT: &str =
    "this verification binds one immutable finality receipt to verified closure-certificate truth, the exact reviewed frozen controller summary, the canonical chain fingerprint, the ledger seal, the registry entry, the filing-certificate identity, the archive-receipt identity, and the closure-certificate identity";

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
    closure_certificate_session_dir: PathBuf,
    confirmed_decision_packet_session_dir: Option<PathBuf>,
    session_dir: Option<PathBuf>,
    output_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanLivePackageFinalityReceipt,
    RenderLivePackageFinalityReceipt,
    RunLivePackageFinalityReceipt,
    VerifyLivePackageFinalityReceipt,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageFinalityReceiptVerdict {
    TinyLivePackageFinalityReceiptPlanReady,
    TinyLivePackageFinalityReceiptRendered,
    TinyLivePackageFinalityReceiptRefusedNowByStage3,
    TinyLivePackageFinalityReceiptRefusedNowByPreActivationGate,
    TinyLivePackageFinalityReceiptRefusedNowByInvalidOrDriftedContract,
    TinyLivePackageFinalityReceiptReadyForManualExecutionWhenGateTurnsGreen,
    TinyLivePackageFinalityReceiptVerifyOk,
    TinyLivePackageFinalityReceiptVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageFinalityReceiptResult {
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    RefusedNowByInvalidOrDriftedContract,
    ReadyForManualExecutionWhenGateTurnsGreen,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PackageFinalityReceiptStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PackageFinalityReceiptSession {
    session_version: String,
    planned_at: DateTime<Utc>,
    closure_certificate_session_dir: String,
    registry_entry_session_dir: String,
    notarization_receipt_session_dir: String,
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
    verify_closure_certificate_command_summary: String,
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
    registry_entry_summary: String,
    registry_entry_sha256: String,
    registry_entry_algorithm: String,
    filing_certificate_summary: String,
    filing_certificate_sha256: String,
    filing_certificate_algorithm: String,
    archive_receipt_summary: String,
    archive_receipt_sha256: String,
    archive_receipt_algorithm: String,
    closure_certificate_summary: String,
    closure_certificate_sha256: String,
    closure_certificate_algorithm: String,
    finality_receipt_summary: String,
    finality_receipt_sha256: String,
    finality_receipt_algorithm: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PackageFinalityReceiptStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    closure_certificate_session_dir: String,
    result: LivePackageFinalityReceiptResult,
    reason: String,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    closure_certificate_step: Option<PackageFinalityReceiptStepArtifact>,
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
    registry_entry_summary: String,
    registry_entry_sha256: String,
    registry_entry_algorithm: String,
    filing_certificate_summary: String,
    filing_certificate_sha256: String,
    filing_certificate_algorithm: String,
    archive_receipt_summary: String,
    archive_receipt_sha256: String,
    archive_receipt_algorithm: String,
    closure_certificate_summary: String,
    closure_certificate_sha256: String,
    closure_certificate_algorithm: String,
    finality_receipt_summary: String,
    finality_receipt_sha256: String,
    finality_receipt_algorithm: String,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageFinalityReceiptPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    closure_certificate_report_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageFinalityReceiptReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageFinalityReceiptVerdict,
    reason: String,
    closure_certificate_session_dir: String,
    registry_entry_session_dir: Option<String>,
    notarization_receipt_session_dir: Option<String>,
    provenance_certificate_session_dir: Option<String>,
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
    finality_receipt_session_path: Option<String>,
    finality_receipt_status_path: Option<String>,
    archived_closure_certificate_report_path: Option<String>,
    result: Option<String>,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    closure_certificate_step: Option<PackageFinalityReceiptStepArtifact>,
    verify_closure_certificate_command_summary: Option<String>,
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
    registry_entry_summary: Option<String>,
    registry_entry_sha256: Option<String>,
    registry_entry_algorithm: Option<String>,
    filing_certificate_summary: Option<String>,
    filing_certificate_sha256: Option<String>,
    filing_certificate_algorithm: Option<String>,
    archive_receipt_summary: Option<String>,
    archive_receipt_sha256: Option<String>,
    archive_receipt_algorithm: Option<String>,
    closure_certificate_summary: Option<String>,
    closure_certificate_sha256: Option<String>,
    closure_certificate_algorithm: Option<String>,
    finality_receipt_summary: Option<String>,
    finality_receipt_sha256: Option<String>,
    finality_receipt_algorithm: Option<String>,
    verification_mismatches: Vec<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct NestedClosureCertificateReportView {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: String,
    reason: String,
}

#[derive(Debug, Serialize)]
struct FinalityReceiptPayload<'a> {
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
    ledger_seal_sha256: &'a str,
    ledger_seal_algorithm: &'a str,
    registry_entry_sha256: &'a str,
    registry_entry_algorithm: &'a str,
    filing_certificate_sha256: &'a str,
    filing_certificate_algorithm: &'a str,
    archive_receipt_sha256: &'a str,
    archive_receipt_algorithm: &'a str,
    closure_certificate_sha256: &'a str,
    closure_certificate_algorithm: &'a str,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut closure_certificate_session_dir: Option<PathBuf> = None;
    let mut confirmed_decision_packet_session_dir: Option<PathBuf> = None;
    let mut session_dir: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut json = false;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--closure-certificate-session-dir" => {
                closure_certificate_session_dir = Some(PathBuf::from(parse_string_arg(
                    "--closure-certificate-session-dir",
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
            "--plan-live-package-finality-receipt" => set_mode(
                &mut mode,
                Mode::PlanLivePackageFinalityReceipt,
                arg.as_str(),
            )?,
            "--render-live-package-finality-receipt" => set_mode(
                &mut mode,
                Mode::RenderLivePackageFinalityReceipt,
                arg.as_str(),
            )?,
            "--run-live-package-finality-receipt" => {
                set_mode(&mut mode, Mode::RunLivePackageFinalityReceipt, arg.as_str())?
            }
            "--verify-live-package-finality-receipt" => set_mode(
                &mut mode,
                Mode::VerifyLivePackageFinalityReceipt,
                arg.as_str(),
            )?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let Some(mode) = mode else {
        return Ok(None);
    };
    if matches!(mode, Mode::RenderLivePackageFinalityReceipt) && output_path.is_none() {
        bail!("missing --output for --render-live-package-finality-receipt");
    }
    if matches!(
        mode,
        Mode::RunLivePackageFinalityReceipt | Mode::VerifyLivePackageFinalityReceipt
    ) && session_dir.is_none()
    {
        bail!("missing --session-dir for run/verify mode");
    }
    if matches!(
        mode,
        Mode::RunLivePackageFinalityReceipt | Mode::VerifyLivePackageFinalityReceipt
    ) && confirmed_decision_packet_session_dir.is_none()
    {
        bail!("missing --confirm-decision-packet-session-dir for run/verify mode");
    }
    let closure_certificate_session_dir = closure_certificate_session_dir
        .ok_or_else(|| anyhow!("missing --closure-certificate-session-dir"))?;
    Ok(Some(Config {
        mode,
        closure_certificate_session_dir,
        confirmed_decision_packet_session_dir,
        session_dir,
        output_path,
        json,
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanLivePackageFinalityReceipt => plan_live_package_finality_receipt_report(&config)?,
        Mode::RenderLivePackageFinalityReceipt => {
            render_live_package_finality_receipt_report(&config)?
        }
        Mode::RunLivePackageFinalityReceipt => run_live_package_finality_receipt_report(&config)?,
        Mode::VerifyLivePackageFinalityReceipt => {
            verify_live_package_finality_receipt_report(&config)?
        }
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_live_package_finality_receipt_report(
    config: &Config,
) -> Result<PackageFinalityReceiptReport> {
    let (closure_certificate, classification) =
        match config.confirmed_decision_packet_session_dir.as_deref() {
            Some(confirmed_decision_packet_session_dir) => {
                match package_closure_certificate_finality_receipt::verify_live_package_closure_certificate_for_finality_receipt(
                    &config.closure_certificate_session_dir,
                    confirmed_decision_packet_session_dir,
                ) {
                    Ok(verified) => {
                        let classification = classify_verified_closure_certificate(&verified);
                        (verified.contract, classification)
                    }
                    Err(_) => {
                        let raw_contract = package_closure_certificate_finality_receipt::load_live_package_closure_certificate_contract_for_finality_receipt(
                            &config.closure_certificate_session_dir,
                        )?;
                        let classification = classify_archive_contract(&raw_contract, false);
                        (raw_contract, classification)
                    }
                }
            }
            None => {
                let raw_contract = package_closure_certificate_finality_receipt::load_live_package_closure_certificate_contract_for_finality_receipt(
                    &config.closure_certificate_session_dir,
                )?;
                let classification = classify_archive_contract(&raw_contract, false);
                (raw_contract, classification)
            }
        };
    let session_dir = config.session_dir.clone().unwrap_or_else(|| {
        config
            .closure_certificate_session_dir
            .join("finality-receipt")
    });
    let result = serialize_enum(&classification.0);
    let finality_receipt_sha256 = compute_finality_receipt_sha256(
        &result,
        closure_certificate.handoff_bundle_result.as_deref(),
        closure_certificate.decision_packet_result.as_deref(),
        closure_certificate.execute_frozen_result.as_deref(),
        closure_certificate
            .current_pre_activation_gate_verdict
            .as_deref(),
        closure_certificate
            .current_pre_activation_gate_reason
            .as_deref(),
        &closure_certificate.reviewed_frozen_live_cutover_controller_command_summary,
        &closure_certificate.chain_fingerprint_sha256,
        &closure_certificate.chain_fingerprint_algorithm,
        &closure_certificate.release_capsule_digest_manifest_sha256,
        closure_certificate.release_capsule_digest_manifest_entry_count,
        &closure_certificate.release_capsule_digest_algorithm,
        &closure_certificate.ledger_seal_sha256,
        &closure_certificate.ledger_seal_algorithm,
        &closure_certificate.registry_entry_sha256,
        &closure_certificate.registry_entry_algorithm,
        &closure_certificate.filing_certificate_sha256,
        &closure_certificate.filing_certificate_algorithm,
        &closure_certificate.archive_receipt_sha256,
        &closure_certificate.archive_receipt_algorithm,
        &closure_certificate.closure_certificate_sha256,
        &closure_certificate.closure_certificate_algorithm,
    )?;

    Ok(PackageFinalityReceiptReport {
        generated_at: Utc::now(),
        mode: "plan_live_package_finality_receipt".to_string(),
        verdict: TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptPlanReady,
        reason: format!(
            "closure-certificate-native finality receipt is explicit for verified closure-certificate session {}; this command stays archival and read-only while freezing one final end-state identity over the fully closed chain",
            config.closure_certificate_session_dir.display()
        ),
        closure_certificate_session_dir: config.closure_certificate_session_dir.display().to_string(),
        registry_entry_session_dir: Some(
            closure_certificate.registry_entry_session_dir.display().to_string(),
        ),
        notarization_receipt_session_dir: Some(
            closure_certificate
                .notarization_receipt_session_dir
                .display()
                .to_string(),
        ),
        provenance_certificate_session_dir: Some(
            closure_certificate
                .provenance_certificate_session_dir
                .display()
                .to_string(),
        ),
        attestation_seal_session_dir: Some(
            closure_certificate
                .attestation_seal_session_dir
                .display()
                .to_string(),
        ),
        release_capsule_session_dir: Some(
            closure_certificate
                .release_capsule_session_dir
                .display()
                .to_string(),
        ),
        activation_ticket_session_dir: Some(
            closure_certificate
                .activation_ticket_session_dir
                .display()
                .to_string(),
        ),
        review_receipt_session_dir: Some(
            closure_certificate.review_receipt_session_dir.display().to_string(),
        ),
        handoff_bundle_session_dir: Some(
            closure_certificate.handoff_bundle_session_dir.display().to_string(),
        ),
        decision_packet_session_dir: Some(
            closure_certificate
                .decision_packet_session_dir
                .display()
                .to_string(),
        ),
        execute_frozen_session_dir: Some(
            closure_certificate
                .execute_frozen_session_dir
                .display()
                .to_string(),
        ),
        turn_green_session_dir: Some(
            closure_certificate.turn_green_session_dir.display().to_string(),
        ),
        launch_packet_session_dir: Some(
            closure_certificate.launch_packet_session_dir.display().to_string(),
        ),
        package_dir: Some(closure_certificate.package_dir.display().to_string()),
        install_root: Some(closure_certificate.install_root.display().to_string()),
        target_service_name: Some(closure_certificate.target_service_name.clone()),
        backend_command: Some(closure_certificate.backend_command.clone()),
        wrapper_timeout_ms: Some(closure_certificate.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(closure_certificate.service_status_max_staleness_ms),
        session_dir: Some(session_dir.display().to_string()),
        finality_receipt_session_path: None,
        finality_receipt_status_path: None,
        archived_closure_certificate_report_path: None,
        result: Some(result),
        handoff_bundle_result: closure_certificate.handoff_bundle_result.clone(),
        decision_packet_result: closure_certificate.decision_packet_result.clone(),
        execute_frozen_result: closure_certificate.execute_frozen_result.clone(),
        current_pre_activation_gate_verdict: closure_certificate
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: closure_certificate
            .current_pre_activation_gate_reason
            .clone(),
        closure_certificate_step: None,
        verify_closure_certificate_command_summary: Some(verify_closure_certificate_command_summary(
            &config.closure_certificate_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            closure_certificate
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        provenance_certificate_summary: Some(closure_certificate.provenance_certificate_summary.clone()),
        chain_fingerprint_summary: Some(closure_certificate.chain_fingerprint_summary.clone()),
        chain_fingerprint_sha256: Some(closure_certificate.chain_fingerprint_sha256.clone()),
        chain_fingerprint_algorithm: Some(closure_certificate.chain_fingerprint_algorithm.clone()),
        release_capsule_digest_manifest_sha256: Some(
            closure_certificate.release_capsule_digest_manifest_sha256.clone(),
        ),
        release_capsule_digest_manifest_entry_count: Some(
            closure_certificate.release_capsule_digest_manifest_entry_count,
        ),
        release_capsule_digest_algorithm: Some(
            closure_certificate.release_capsule_digest_algorithm.clone(),
        ),
        notarization_receipt_summary: Some(closure_certificate.notarization_receipt_summary.clone()),
        ledger_seal_summary: Some(closure_certificate.ledger_seal_summary.clone()),
        ledger_seal_sha256: Some(closure_certificate.ledger_seal_sha256.clone()),
        ledger_seal_algorithm: Some(closure_certificate.ledger_seal_algorithm.clone()),
        registry_entry_summary: Some(closure_certificate.registry_entry_summary.clone()),
        registry_entry_sha256: Some(closure_certificate.registry_entry_sha256.clone()),
        registry_entry_algorithm: Some(closure_certificate.registry_entry_algorithm.clone()),
        filing_certificate_summary: Some(closure_certificate.filing_certificate_summary.clone()),
        filing_certificate_sha256: Some(closure_certificate.filing_certificate_sha256.clone()),
        filing_certificate_algorithm: Some(closure_certificate.filing_certificate_algorithm.clone()),
        archive_receipt_summary: Some(closure_certificate.archive_receipt_summary.clone()),
        archive_receipt_sha256: Some(closure_certificate.archive_receipt_sha256.clone()),
        archive_receipt_algorithm: Some(closure_certificate.archive_receipt_algorithm.clone()),
        closure_certificate_summary: Some(closure_certificate.closure_certificate_summary.clone()),
        closure_certificate_sha256: Some(closure_certificate.closure_certificate_sha256.clone()),
        closure_certificate_algorithm: Some(closure_certificate.closure_certificate_algorithm.clone()),
        finality_receipt_summary: Some(finality_receipt_summary(
            classification.0,
            &closure_certificate.reviewed_frozen_live_cutover_controller_command_summary,
        )),
        finality_receipt_sha256: Some(finality_receipt_sha256),
        finality_receipt_algorithm: Some(FINALITY_RECEIPT_ALGORITHM.to_string()),
        verification_mismatches: Vec::new(),
        explicit_statement: FINALITY_RECEIPT_SESSION_EXPLICIT_STATEMENT.to_string(),
    })
}

fn render_live_package_finality_receipt_report(
    config: &Config,
) -> Result<PackageFinalityReceiptReport> {
    let plan = plan_live_package_finality_receipt_report(config)?;
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --output for render mode"))?;
    let session_dir = PathBuf::from(
        plan.session_dir
            .clone()
            .ok_or_else(|| anyhow!("plan is missing session_dir for render"))?,
    );
    let confirmed_decision_packet_session_dir = PathBuf::from(
        plan.decision_packet_session_dir
            .clone()
            .ok_or_else(|| anyhow!("plan is missing decision_packet_session_dir for render"))?,
    );
    let script = format!(
        "#!/bin/sh\nset -eu\ncopybot_tiny_live_activation_package_finality_receipt --closure-certificate-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --run-live-package-finality-receipt --json\ncopybot_tiny_live_activation_package_finality_receipt --closure-certificate-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --verify-live-package-finality-receipt --json\n",
        shell_escape_path(&config.closure_certificate_session_dir),
        shell_escape_path(&confirmed_decision_packet_session_dir),
        shell_escape_path(&session_dir),
        shell_escape_path(&config.closure_certificate_session_dir),
        shell_escape_path(&confirmed_decision_packet_session_dir),
        shell_escape_path(&session_dir),
    );
    write_script(output_path, &script)?;
    Ok(PackageFinalityReceiptReport {
        generated_at: Utc::now(),
        mode: "render_live_package_finality_receipt".to_string(),
        verdict: TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptRendered,
        reason: format!(
            "rendered finality-receipt run/verify script to {}; the primary input remains the immutable closure-certificate session and run/verify still require the decision-packet confirmation anchor",
            output_path.display()
        ),
        explicit_statement: FINALITY_RECEIPT_SESSION_EXPLICIT_STATEMENT.to_string(),
        ..plan
    })
}

fn run_live_package_finality_receipt_report(
    config: &Config,
) -> Result<PackageFinalityReceiptReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for run mode"))?;
    let verified_closure_certificate = match package_closure_certificate_finality_receipt::verify_live_package_closure_certificate_for_finality_receipt(
        &config.closure_certificate_session_dir,
        config
            .confirmed_decision_packet_session_dir
            .as_deref()
            .ok_or_else(|| anyhow!("missing --confirm-decision-packet-session-dir for run mode"))?,
    ) {
        Ok(value) => value,
        Err(error) => {
            return Ok(failure_report_without_contract(
                config,
                TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptRefusedNowByInvalidOrDriftedContract,
                format!(
                    "failed to verify closure-certificate session under {}: {error}",
                    config.closure_certificate_session_dir.display()
                ),
            ))
        }
    };

    validate_managed_surface_session_dir(
        &verified_closure_certificate.contract.install_root,
        session_dir,
    )
    .with_context(|| {
        format!(
            "finality-receipt session dir {} overlaps the managed live target surface derived from verified closure-certificate install_root {}",
            session_dir.display(),
            verified_closure_certificate.contract.install_root.display()
        )
    })?;
    ensure_clean_finality_receipt_session_dir(session_dir)?;
    let paths = finality_receipt_paths(session_dir);
    let classification = classify_verified_closure_certificate(&verified_closure_certificate);
    let result = classification.0;
    let finality_receipt_sha256 = compute_finality_receipt_sha256(
        &serialize_enum(&result),
        verified_closure_certificate
            .contract
            .handoff_bundle_result
            .as_deref(),
        verified_closure_certificate
            .contract
            .decision_packet_result
            .as_deref(),
        verified_closure_certificate
            .contract
            .execute_frozen_result
            .as_deref(),
        verified_closure_certificate
            .contract
            .current_pre_activation_gate_verdict
            .as_deref(),
        verified_closure_certificate
            .contract
            .current_pre_activation_gate_reason
            .as_deref(),
        &verified_closure_certificate
            .contract
            .reviewed_frozen_live_cutover_controller_command_summary,
        &verified_closure_certificate
            .contract
            .chain_fingerprint_sha256,
        &verified_closure_certificate
            .contract
            .chain_fingerprint_algorithm,
        &verified_closure_certificate
            .contract
            .release_capsule_digest_manifest_sha256,
        verified_closure_certificate
            .contract
            .release_capsule_digest_manifest_entry_count,
        &verified_closure_certificate
            .contract
            .release_capsule_digest_algorithm,
        &verified_closure_certificate.contract.ledger_seal_sha256,
        &verified_closure_certificate.contract.ledger_seal_algorithm,
        &verified_closure_certificate.contract.registry_entry_sha256,
        &verified_closure_certificate
            .contract
            .registry_entry_algorithm,
        &verified_closure_certificate
            .contract
            .filing_certificate_sha256,
        &verified_closure_certificate
            .contract
            .filing_certificate_algorithm,
        &verified_closure_certificate.contract.archive_receipt_sha256,
        &verified_closure_certificate
            .contract
            .archive_receipt_algorithm,
        &verified_closure_certificate
            .contract
            .closure_certificate_sha256,
        &verified_closure_certificate
            .contract
            .closure_certificate_algorithm,
    )?;
    let session = planned_session(
        config,
        session_dir,
        &verified_closure_certificate.contract,
        result,
        &finality_receipt_sha256,
    );
    persist_json(&paths.session_path, &session)?;
    persist_json(
        &paths.closure_certificate_report_path,
        &verified_closure_certificate.report_json,
    )?;
    let closure_certificate_step = Some(step_artifact(
        &paths.closure_certificate_report_path,
        "verify_live_package_closure_certificate",
        &verified_closure_certificate.verdict,
        &verified_closure_certificate.reason,
        verified_closure_certificate.generated_at,
    ));
    let status = PackageFinalityReceiptStatus {
        status_version: FINALITY_RECEIPT_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        closure_certificate_session_dir: config
            .closure_certificate_session_dir
            .display()
            .to_string(),
        result,
        reason: classification.2.clone(),
        handoff_bundle_result: verified_closure_certificate
            .contract
            .handoff_bundle_result
            .clone(),
        decision_packet_result: verified_closure_certificate
            .contract
            .decision_packet_result
            .clone(),
        execute_frozen_result: verified_closure_certificate
            .contract
            .execute_frozen_result
            .clone(),
        current_pre_activation_gate_verdict: verified_closure_certificate
            .contract
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: verified_closure_certificate
            .contract
            .current_pre_activation_gate_reason
            .clone(),
        closure_certificate_step: closure_certificate_step.clone(),
        provenance_certificate_summary: verified_closure_certificate
            .contract
            .provenance_certificate_summary
            .clone(),
        chain_fingerprint_summary: verified_closure_certificate
            .contract
            .chain_fingerprint_summary
            .clone(),
        chain_fingerprint_sha256: verified_closure_certificate
            .contract
            .chain_fingerprint_sha256
            .clone(),
        chain_fingerprint_algorithm: verified_closure_certificate
            .contract
            .chain_fingerprint_algorithm
            .clone(),
        release_capsule_digest_manifest_sha256: verified_closure_certificate
            .contract
            .release_capsule_digest_manifest_sha256
            .clone(),
        release_capsule_digest_manifest_entry_count: verified_closure_certificate
            .contract
            .release_capsule_digest_manifest_entry_count,
        release_capsule_digest_algorithm: verified_closure_certificate
            .contract
            .release_capsule_digest_algorithm
            .clone(),
        notarization_receipt_summary: verified_closure_certificate
            .contract
            .notarization_receipt_summary
            .clone(),
        ledger_seal_summary: verified_closure_certificate
            .contract
            .ledger_seal_summary
            .clone(),
        ledger_seal_sha256: verified_closure_certificate
            .contract
            .ledger_seal_sha256
            .clone(),
        ledger_seal_algorithm: verified_closure_certificate
            .contract
            .ledger_seal_algorithm
            .clone(),
        registry_entry_summary: verified_closure_certificate
            .contract
            .registry_entry_summary
            .clone(),
        registry_entry_sha256: verified_closure_certificate
            .contract
            .registry_entry_sha256
            .clone(),
        registry_entry_algorithm: verified_closure_certificate
            .contract
            .registry_entry_algorithm
            .clone(),
        filing_certificate_summary: verified_closure_certificate
            .contract
            .filing_certificate_summary
            .clone(),
        filing_certificate_sha256: verified_closure_certificate
            .contract
            .filing_certificate_sha256
            .clone(),
        filing_certificate_algorithm: verified_closure_certificate
            .contract
            .filing_certificate_algorithm
            .clone(),
        archive_receipt_summary: verified_closure_certificate
            .contract
            .archive_receipt_summary
            .clone(),
        archive_receipt_sha256: verified_closure_certificate
            .contract
            .archive_receipt_sha256
            .clone(),
        archive_receipt_algorithm: verified_closure_certificate
            .contract
            .archive_receipt_algorithm
            .clone(),
        closure_certificate_summary: verified_closure_certificate
            .contract
            .closure_certificate_summary
            .clone(),
        closure_certificate_sha256: verified_closure_certificate
            .contract
            .closure_certificate_sha256
            .clone(),
        closure_certificate_algorithm: verified_closure_certificate
            .contract
            .closure_certificate_algorithm
            .clone(),
        finality_receipt_summary: finality_receipt_summary(
            result,
            &verified_closure_certificate
                .contract
                .reviewed_frozen_live_cutover_controller_command_summary,
        ),
        finality_receipt_sha256: finality_receipt_sha256.clone(),
        finality_receipt_algorithm: FINALITY_RECEIPT_ALGORITHM.to_string(),
        explicit_statement: FINALITY_RECEIPT_STATUS_EXPLICIT_STATEMENT.to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    Ok(report_from_status(
        config,
        session_dir,
        &paths,
        &verified_closure_certificate.contract,
        classification.1,
        &status,
    ))
}

fn verify_live_package_finality_receipt_report(
    config: &Config,
) -> Result<PackageFinalityReceiptReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = finality_receipt_paths(session_dir);
    let session: PackageFinalityReceiptSession = load_json(&paths.session_path)?;
    let status: PackageFinalityReceiptStatus = load_json(&paths.status_path)?;
    let mut mismatches = Vec::new();

    compare_string(
        &session.closure_certificate_session_dir,
        &config.closure_certificate_session_dir.display().to_string(),
        "finality-receipt closure_certificate_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "finality-receipt session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "finality-receipt status session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.closure_certificate_session_dir,
        &config.closure_certificate_session_dir.display().to_string(),
        "finality-receipt status closure_certificate_session_dir",
        &mut mismatches,
    );

    let verified_closure_certificate = match package_closure_certificate_finality_receipt::verify_live_package_closure_certificate_for_finality_receipt(
        &config.closure_certificate_session_dir,
        config
            .confirmed_decision_packet_session_dir
            .as_deref()
            .ok_or_else(|| anyhow!("missing --confirm-decision-packet-session-dir for verify mode"))?,
    ) {
        Ok(value) => value,
        Err(error) => {
            let raw_contract = package_closure_certificate_finality_receipt::load_live_package_closure_certificate_contract_for_finality_receipt(
                &config.closure_certificate_session_dir,
            )?;
            return Ok(failure_report_for_verify(
                config,
                session_dir,
                &raw_contract,
                vec![format!(
                    "failed to verify closure-certificate session under {}: {error}",
                    config.closure_certificate_session_dir.display()
                )],
            ));
        }
    };

    let stored_closure_certificate_report: serde_json::Value = load_required_step_json(
        &status.closure_certificate_step,
        &paths.closure_certificate_report_path,
        "closure_certificate_step",
        &mut mismatches,
    )?;
    let stored_closure_certificate_report_view: NestedClosureCertificateReportView =
        serde_json::from_value(stored_closure_certificate_report.clone()).with_context(|| {
            format!(
                "failed parsing archived nested closure-certificate report {}",
                paths.closure_certificate_report_path.display()
            )
        })?;
    let verified_closure_certificate_report_view: NestedClosureCertificateReportView =
        serde_json::from_value(verified_closure_certificate.report_json.clone()).with_context(
            || {
                format!(
                    "failed parsing freshly verified nested closure-certificate report for {}",
                    config.closure_certificate_session_dir.display()
                )
            },
        )?;
    compare_json_ignoring_generated_at(
        &stored_closure_certificate_report,
        &verified_closure_certificate.report_json,
        "finality-receipt archived closure-certificate report json",
        &mut mismatches,
    );
    compare_string(
        &stored_closure_certificate_report_view.mode,
        &verified_closure_certificate_report_view.mode,
        "finality-receipt archived closure-certificate report mode",
        &mut mismatches,
    );
    compare_string(
        &stored_closure_certificate_report_view.verdict,
        &verified_closure_certificate_report_view.verdict,
        "finality-receipt archived closure-certificate report verdict",
        &mut mismatches,
    );
    compare_string(
        &stored_closure_certificate_report_view.reason,
        &verified_closure_certificate_report_view.reason,
        "finality-receipt archived closure-certificate report reason",
        &mut mismatches,
    );
    if stored_closure_certificate_report_view.generated_at
        != verified_closure_certificate_report_view.generated_at
    {
        mismatches.push(format!(
            "finality-receipt archived closure-certificate report generated_at {:?} does not match freshly verified nested truth {:?}",
            stored_closure_certificate_report_view.generated_at,
            verified_closure_certificate_report_view.generated_at
        ));
    }
    let expected_closure_certificate_step = step_artifact(
        &paths.closure_certificate_report_path,
        &verified_closure_certificate_report_view.mode,
        &verified_closure_certificate_report_view.verdict,
        &verified_closure_certificate_report_view.reason,
        verified_closure_certificate_report_view.generated_at,
    );
    compare_step_artifact(
        status.closure_certificate_step.as_ref(),
        &expected_closure_certificate_step,
        "finality-receipt closure_certificate_step",
        &mut mismatches,
    );
    if verified_closure_certificate.verdict != "tiny_live_package_closure_certificate_verify_ok" {
        mismatches.push(format!(
            "nested closure-certificate verification is non-green: {}",
            verified_closure_certificate.reason
        ));
    }

    let classification = classify_verified_closure_certificate(&verified_closure_certificate);
    let expected_finality_receipt_sha256 = compute_finality_receipt_sha256(
        &serialize_enum(&classification.0),
        verified_closure_certificate
            .contract
            .handoff_bundle_result
            .as_deref(),
        verified_closure_certificate
            .contract
            .decision_packet_result
            .as_deref(),
        verified_closure_certificate
            .contract
            .execute_frozen_result
            .as_deref(),
        verified_closure_certificate
            .contract
            .current_pre_activation_gate_verdict
            .as_deref(),
        verified_closure_certificate
            .contract
            .current_pre_activation_gate_reason
            .as_deref(),
        &verified_closure_certificate
            .contract
            .reviewed_frozen_live_cutover_controller_command_summary,
        &verified_closure_certificate
            .contract
            .chain_fingerprint_sha256,
        &verified_closure_certificate
            .contract
            .chain_fingerprint_algorithm,
        &verified_closure_certificate
            .contract
            .release_capsule_digest_manifest_sha256,
        verified_closure_certificate
            .contract
            .release_capsule_digest_manifest_entry_count,
        &verified_closure_certificate
            .contract
            .release_capsule_digest_algorithm,
        &verified_closure_certificate.contract.ledger_seal_sha256,
        &verified_closure_certificate.contract.ledger_seal_algorithm,
        &verified_closure_certificate.contract.registry_entry_sha256,
        &verified_closure_certificate
            .contract
            .registry_entry_algorithm,
        &verified_closure_certificate
            .contract
            .filing_certificate_sha256,
        &verified_closure_certificate
            .contract
            .filing_certificate_algorithm,
        &verified_closure_certificate.contract.archive_receipt_sha256,
        &verified_closure_certificate
            .contract
            .archive_receipt_algorithm,
        &verified_closure_certificate
            .contract
            .closure_certificate_sha256,
        &verified_closure_certificate
            .contract
            .closure_certificate_algorithm,
    )?;

    let expected_session = expected_session_from_verified(
        config,
        session_dir,
        session.planned_at,
        &verified_closure_certificate.contract,
        classification.0,
        &expected_finality_receipt_sha256,
    );
    if session != expected_session {
        mismatches.push(
            "finality-receipt session contract does not match freshly verified closure-certificate truth"
                .to_string(),
        );
    }
    let expected_status = expected_status_from_verified(
        config,
        session_dir,
        status.updated_at,
        &verified_closure_certificate.contract,
        classification,
        expected_closure_certificate_step.clone(),
        &expected_finality_receipt_sha256,
    );
    if status != expected_status {
        mismatches.push(
            "finality-receipt status contract does not match freshly verified closure-certificate truth"
                .to_string(),
        );
    }

    if !mismatches.is_empty() {
        return Ok(failure_report_for_verify(
            config,
            session_dir,
            &verified_closure_certificate.contract,
            mismatches,
        ));
    }

    Ok(report_from_status(
        config,
        session_dir,
        &paths,
        &verified_closure_certificate.contract,
        TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptVerifyOk,
        &expected_status,
    ))
}

fn verify_closure_certificate_command_summary(closure_certificate_session_dir: &Path) -> String {
    format!(
        "verify immutable closure-certificate session under {} before freezing the final finality receipt",
        closure_certificate_session_dir.display()
    )
}

fn classify_archive_contract(
    contract: &package_closure_certificate_finality_receipt::LivePackageClosureCertificateContractView,
    assume_verified: bool,
) -> (
    LivePackageFinalityReceiptResult,
    TinyLivePackageFinalityReceiptVerdict,
    String,
) {
    if !assume_verified {
        return (
            LivePackageFinalityReceiptResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptRefusedNowByInvalidOrDriftedContract,
            "the closure-certificate chain is not verified, so this finality receipt remains refused"
                .to_string(),
        );
    }
    match contract.result.as_deref() {
        Some("refused_now_by_stage3") => (
            LivePackageFinalityReceiptResult::RefusedNowByStage3,
            TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptRefusedNowByStage3,
            "the immutable closure certificate remains refused now by Stage 3, so this finality receipt remains an explicit refusal record"
                .to_string(),
        ),
        Some("refused_now_by_pre_activation_gate") => (
            LivePackageFinalityReceiptResult::RefusedNowByPreActivationGate,
            TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptRefusedNowByPreActivationGate,
            "the immutable closure certificate remains refused now by the pre-activation gate, so this finality receipt remains an explicit refusal record"
                .to_string(),
        ),
        Some("ready_for_manual_execution_when_gate_turns_green") => (
            LivePackageFinalityReceiptResult::ReadyForManualExecutionWhenGateTurnsGreen,
            TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptReadyForManualExecutionWhenGateTurnsGreen,
            "the immutable closure certificate is coherent and this finality receipt is ready for manual execution when gate truth turns green"
                .to_string(),
        ),
        _ => (
            LivePackageFinalityReceiptResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptRefusedNowByInvalidOrDriftedContract,
            "the verified closure-certificate chain is invalid, drifted, or non-runnable, so this finality receipt remains refused"
                .to_string(),
        ),
    }
}

fn classify_verified_closure_certificate(
    verified: &package_closure_certificate_finality_receipt::VerifiedLivePackageClosureCertificateFinalityReceiptStep,
) -> (
    LivePackageFinalityReceiptResult,
    TinyLivePackageFinalityReceiptVerdict,
    String,
) {
    if verified.verdict != "tiny_live_package_closure_certificate_verify_ok" {
        return (
            LivePackageFinalityReceiptResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptRefusedNowByInvalidOrDriftedContract,
            "the immutable closure certificate did not verify cleanly, so this finality receipt remains refused"
                .to_string(),
        );
    }
    classify_archive_contract(&verified.contract, true)
}

fn finality_receipt_summary(
    result: LivePackageFinalityReceiptResult,
    reviewed_frozen_live_cutover_controller_command_summary: &str,
) -> String {
    match result {
        LivePackageFinalityReceiptResult::RefusedNowByStage3 => {
            "Finality receipt outcome: the immutable closed chain remains refused now by Stage 3; archive this final end-state seal as refusal evidence only and do not run the frozen controller.".to_string()
        }
        LivePackageFinalityReceiptResult::RefusedNowByPreActivationGate => {
            "Finality receipt outcome: the immutable closed chain remains refused now by the pre-activation gate; archive this final end-state seal as refusal evidence only and do not run the frozen controller.".to_string()
        }
        LivePackageFinalityReceiptResult::RefusedNowByInvalidOrDriftedContract => {
            "Finality receipt outcome: the immutable closed chain is invalid or drifted; archive this final end-state seal for audit only and mint a new coherent certificate before any later execution record.".to_string()
        }
        LivePackageFinalityReceiptResult::ReadyForManualExecutionWhenGateTurnsGreen => format!(
            "Finality receipt outcome: this immutable end-state seal is ready for manual execution when gate truth turns green; the exact reviewed frozen controller command remains: {reviewed_frozen_live_cutover_controller_command_summary}"
        ),
    }
}

fn finality_receipt_paths(session_dir: &Path) -> PackageFinalityReceiptPaths {
    PackageFinalityReceiptPaths {
        session_path: session_dir
            .join("tiny_live_activation_package_finality_receipt.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_finality_receipt.status.json"),
        closure_certificate_report_path: session_dir
            .join("tiny_live_activation_package_finality_receipt.closure_certificate.report.json"),
    }
}

fn ensure_clean_finality_receipt_session_dir(session_dir: &Path) -> Result<()> {
    if session_dir.exists() {
        bail!(
            "refusing to overwrite existing finality-receipt session dir {}",
            session_dir.display()
        );
    }
    fs::create_dir_all(session_dir).with_context(|| {
        format!(
            "failed creating finality-receipt session dir {}",
            session_dir.display()
        )
    })
}

fn planned_session(
    config: &Config,
    session_dir: &Path,
    contract: &package_closure_certificate_finality_receipt::LivePackageClosureCertificateContractView,
    result: LivePackageFinalityReceiptResult,
    finality_receipt_sha256: &str,
) -> PackageFinalityReceiptSession {
    PackageFinalityReceiptSession {
        session_version: FINALITY_RECEIPT_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        closure_certificate_session_dir: config
            .closure_certificate_session_dir
            .display()
            .to_string(),
        registry_entry_session_dir: contract.registry_entry_session_dir.display().to_string(),
        notarization_receipt_session_dir: contract
            .notarization_receipt_session_dir
            .display()
            .to_string(),
        provenance_certificate_session_dir: contract
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
        verify_closure_certificate_command_summary: verify_closure_certificate_command_summary(
            &config.closure_certificate_session_dir,
        ),
        reviewed_frozen_live_cutover_controller_command_summary: contract
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        provenance_certificate_summary: contract.provenance_certificate_summary.clone(),
        chain_fingerprint_summary: contract.chain_fingerprint_summary.clone(),
        chain_fingerprint_sha256: contract.chain_fingerprint_sha256.clone(),
        chain_fingerprint_algorithm: contract.chain_fingerprint_algorithm.clone(),
        release_capsule_digest_manifest_sha256: contract
            .release_capsule_digest_manifest_sha256
            .clone(),
        release_capsule_digest_manifest_entry_count: contract
            .release_capsule_digest_manifest_entry_count,
        release_capsule_digest_algorithm: contract.release_capsule_digest_algorithm.clone(),
        notarization_receipt_summary: contract.notarization_receipt_summary.clone(),
        ledger_seal_summary: contract.ledger_seal_summary.clone(),
        ledger_seal_sha256: contract.ledger_seal_sha256.clone(),
        ledger_seal_algorithm: contract.ledger_seal_algorithm.clone(),
        registry_entry_summary: contract.registry_entry_summary.clone(),
        registry_entry_sha256: contract.registry_entry_sha256.clone(),
        registry_entry_algorithm: contract.registry_entry_algorithm.clone(),
        filing_certificate_summary: contract.filing_certificate_summary.clone(),
        filing_certificate_sha256: contract.filing_certificate_sha256.clone(),
        filing_certificate_algorithm: contract.filing_certificate_algorithm.clone(),
        archive_receipt_summary: contract.archive_receipt_summary.clone(),
        archive_receipt_sha256: contract.archive_receipt_sha256.clone(),
        archive_receipt_algorithm: contract.archive_receipt_algorithm.clone(),
        closure_certificate_summary: contract.closure_certificate_summary.clone(),
        closure_certificate_sha256: contract.closure_certificate_sha256.clone(),
        closure_certificate_algorithm: contract.closure_certificate_algorithm.clone(),
        finality_receipt_summary: finality_receipt_summary(
            result,
            &contract.reviewed_frozen_live_cutover_controller_command_summary,
        ),
        finality_receipt_sha256: finality_receipt_sha256.to_string(),
        finality_receipt_algorithm: FINALITY_RECEIPT_ALGORITHM.to_string(),
        explicit_statement: FINALITY_RECEIPT_SESSION_EXPLICIT_STATEMENT.to_string(),
    }
}

fn expected_session_from_verified(
    config: &Config,
    session_dir: &Path,
    planned_at: DateTime<Utc>,
    contract: &package_closure_certificate_finality_receipt::LivePackageClosureCertificateContractView,
    result: LivePackageFinalityReceiptResult,
    finality_receipt_sha256: &str,
) -> PackageFinalityReceiptSession {
    PackageFinalityReceiptSession {
        planned_at,
        ..planned_session(
            config,
            session_dir,
            contract,
            result,
            finality_receipt_sha256,
        )
    }
}

fn expected_status_from_verified(
    config: &Config,
    session_dir: &Path,
    updated_at: DateTime<Utc>,
    contract: &package_closure_certificate_finality_receipt::LivePackageClosureCertificateContractView,
    classification: (
        LivePackageFinalityReceiptResult,
        TinyLivePackageFinalityReceiptVerdict,
        String,
    ),
    closure_certificate_step: PackageFinalityReceiptStepArtifact,
    finality_receipt_sha256: &str,
) -> PackageFinalityReceiptStatus {
    PackageFinalityReceiptStatus {
        status_version: FINALITY_RECEIPT_STATUS_VERSION.to_string(),
        updated_at,
        session_dir: session_dir.display().to_string(),
        closure_certificate_session_dir: config
            .closure_certificate_session_dir
            .display()
            .to_string(),
        result: classification.0,
        reason: classification.2,
        handoff_bundle_result: contract.handoff_bundle_result.clone(),
        decision_packet_result: contract.decision_packet_result.clone(),
        execute_frozen_result: contract.execute_frozen_result.clone(),
        current_pre_activation_gate_verdict: contract.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: contract.current_pre_activation_gate_reason.clone(),
        closure_certificate_step: Some(closure_certificate_step),
        provenance_certificate_summary: contract.provenance_certificate_summary.clone(),
        chain_fingerprint_summary: contract.chain_fingerprint_summary.clone(),
        chain_fingerprint_sha256: contract.chain_fingerprint_sha256.clone(),
        chain_fingerprint_algorithm: contract.chain_fingerprint_algorithm.clone(),
        release_capsule_digest_manifest_sha256: contract
            .release_capsule_digest_manifest_sha256
            .clone(),
        release_capsule_digest_manifest_entry_count: contract
            .release_capsule_digest_manifest_entry_count,
        release_capsule_digest_algorithm: contract.release_capsule_digest_algorithm.clone(),
        notarization_receipt_summary: contract.notarization_receipt_summary.clone(),
        ledger_seal_summary: contract.ledger_seal_summary.clone(),
        ledger_seal_sha256: contract.ledger_seal_sha256.clone(),
        ledger_seal_algorithm: contract.ledger_seal_algorithm.clone(),
        registry_entry_summary: contract.registry_entry_summary.clone(),
        registry_entry_sha256: contract.registry_entry_sha256.clone(),
        registry_entry_algorithm: contract.registry_entry_algorithm.clone(),
        filing_certificate_summary: contract.filing_certificate_summary.clone(),
        filing_certificate_sha256: contract.filing_certificate_sha256.clone(),
        filing_certificate_algorithm: contract.filing_certificate_algorithm.clone(),
        archive_receipt_summary: contract.archive_receipt_summary.clone(),
        archive_receipt_sha256: contract.archive_receipt_sha256.clone(),
        archive_receipt_algorithm: contract.archive_receipt_algorithm.clone(),
        closure_certificate_summary: contract.closure_certificate_summary.clone(),
        closure_certificate_sha256: contract.closure_certificate_sha256.clone(),
        closure_certificate_algorithm: contract.closure_certificate_algorithm.clone(),
        finality_receipt_summary: finality_receipt_summary(
            classification.0,
            &contract.reviewed_frozen_live_cutover_controller_command_summary,
        ),
        finality_receipt_sha256: finality_receipt_sha256.to_string(),
        finality_receipt_algorithm: FINALITY_RECEIPT_ALGORITHM.to_string(),
        explicit_statement: FINALITY_RECEIPT_STATUS_EXPLICIT_STATEMENT.to_string(),
    }
}

fn report_from_status(
    config: &Config,
    session_dir: &Path,
    paths: &PackageFinalityReceiptPaths,
    contract: &package_closure_certificate_finality_receipt::LivePackageClosureCertificateContractView,
    verdict: TinyLivePackageFinalityReceiptVerdict,
    status: &PackageFinalityReceiptStatus,
) -> PackageFinalityReceiptReport {
    PackageFinalityReceiptReport {
        generated_at: Utc::now(),
        mode: if verdict
            == TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptVerifyOk
        {
            "verify_live_package_finality_receipt".to_string()
        } else {
            "run_live_package_finality_receipt".to_string()
        },
        verdict,
        reason: status.reason.clone(),
        closure_certificate_session_dir: config
            .closure_certificate_session_dir
            .display()
            .to_string(),
        registry_entry_session_dir: Some(contract.registry_entry_session_dir.display().to_string()),
        notarization_receipt_session_dir: Some(
            contract
                .notarization_receipt_session_dir
                .display()
                .to_string(),
        ),
        provenance_certificate_session_dir: Some(
            contract
                .provenance_certificate_session_dir
                .display()
                .to_string(),
        ),
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
        finality_receipt_session_path: Some(paths.session_path.display().to_string()),
        finality_receipt_status_path: Some(paths.status_path.display().to_string()),
        archived_closure_certificate_report_path: Some(
            paths.closure_certificate_report_path.display().to_string(),
        ),
        result: Some(serialize_enum(&status.result)),
        handoff_bundle_result: status.handoff_bundle_result.clone(),
        decision_packet_result: status.decision_packet_result.clone(),
        execute_frozen_result: status.execute_frozen_result.clone(),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        closure_certificate_step: status.closure_certificate_step.clone(),
        verify_closure_certificate_command_summary: Some(
            verify_closure_certificate_command_summary(&config.closure_certificate_session_dir),
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
        registry_entry_summary: Some(status.registry_entry_summary.clone()),
        registry_entry_sha256: Some(status.registry_entry_sha256.clone()),
        registry_entry_algorithm: Some(status.registry_entry_algorithm.clone()),
        filing_certificate_summary: Some(status.filing_certificate_summary.clone()),
        filing_certificate_sha256: Some(status.filing_certificate_sha256.clone()),
        filing_certificate_algorithm: Some(status.filing_certificate_algorithm.clone()),
        archive_receipt_summary: Some(status.archive_receipt_summary.clone()),
        archive_receipt_sha256: Some(status.archive_receipt_sha256.clone()),
        archive_receipt_algorithm: Some(status.archive_receipt_algorithm.clone()),
        closure_certificate_summary: Some(status.closure_certificate_summary.clone()),
        closure_certificate_sha256: Some(status.closure_certificate_sha256.clone()),
        closure_certificate_algorithm: Some(status.closure_certificate_algorithm.clone()),
        finality_receipt_summary: Some(status.finality_receipt_summary.clone()),
        finality_receipt_sha256: Some(status.finality_receipt_sha256.clone()),
        finality_receipt_algorithm: Some(status.finality_receipt_algorithm.clone()),
        verification_mismatches: Vec::new(),
        explicit_statement: if verdict
            == TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptVerifyOk
        {
            FINALITY_RECEIPT_VERIFY_EXPLICIT_STATEMENT.to_string()
        } else {
            status.explicit_statement.clone()
        },
    }
}

fn failure_report_without_contract(
    config: &Config,
    verdict: TinyLivePackageFinalityReceiptVerdict,
    reason: String,
) -> PackageFinalityReceiptReport {
    PackageFinalityReceiptReport {
        generated_at: Utc::now(),
        mode: match config.mode {
            Mode::PlanLivePackageFinalityReceipt => "plan_live_package_finality_receipt".to_string(),
            Mode::RenderLivePackageFinalityReceipt => {
                "render_live_package_finality_receipt".to_string()
            }
            Mode::RunLivePackageFinalityReceipt => "run_live_package_finality_receipt".to_string(),
            Mode::VerifyLivePackageFinalityReceipt => {
                "verify_live_package_finality_receipt".to_string()
            }
        },
        verdict,
        reason,
        closure_certificate_session_dir: config
            .closure_certificate_session_dir
            .display()
            .to_string(),
        registry_entry_session_dir: None,
        notarization_receipt_session_dir: None,
        provenance_certificate_session_dir: None,
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
        finality_receipt_session_path: None,
        finality_receipt_status_path: None,
        archived_closure_certificate_report_path: None,
        result: Some("refused_now_by_invalid_or_drifted_contract".to_string()),
        handoff_bundle_result: None,
        decision_packet_result: None,
        execute_frozen_result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        closure_certificate_step: None,
        verify_closure_certificate_command_summary: Some(verify_closure_certificate_command_summary(
            &config.closure_certificate_session_dir,
        )),
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
        registry_entry_summary: None,
        registry_entry_sha256: None,
        registry_entry_algorithm: None,
        filing_certificate_summary: None,
        filing_certificate_sha256: None,
        filing_certificate_algorithm: None,
        archive_receipt_summary: None,
        archive_receipt_sha256: None,
        archive_receipt_algorithm: None,
        closure_certificate_summary: None,
        closure_certificate_sha256: None,
        closure_certificate_algorithm: None,
        finality_receipt_summary: None,
        finality_receipt_sha256: None,
        finality_receipt_algorithm: Some(FINALITY_RECEIPT_ALGORITHM.to_string()),
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this immutable finality receipt refused before persisting because the closure-certificate artifact could not be verified coherently"
                .to_string(),
    }
}

fn failure_report_for_verify(
    config: &Config,
    session_dir: &Path,
    contract: &package_closure_certificate_finality_receipt::LivePackageClosureCertificateContractView,
    mismatches: Vec<String>,
) -> PackageFinalityReceiptReport {
    let paths = finality_receipt_paths(session_dir);
    let reason = mismatches.first().cloned().unwrap_or_else(|| {
        "closure-certificate-native finality receipt verification failed".to_string()
    });
    PackageFinalityReceiptReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_finality_receipt".to_string(),
        verdict: TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptVerifyInvalid,
        reason,
        closure_certificate_session_dir: config
            .closure_certificate_session_dir
            .display()
            .to_string(),
        registry_entry_session_dir: Some(contract.registry_entry_session_dir.display().to_string()),
        notarization_receipt_session_dir: Some(
            contract
                .notarization_receipt_session_dir
                .display()
                .to_string(),
        ),
        provenance_certificate_session_dir: Some(
            contract
                .provenance_certificate_session_dir
                .display()
                .to_string(),
        ),
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
        finality_receipt_session_path: Some(paths.session_path.display().to_string()),
        finality_receipt_status_path: Some(paths.status_path.display().to_string()),
        archived_closure_certificate_report_path: Some(
            paths.closure_certificate_report_path.display().to_string(),
        ),
        result: None,
        handoff_bundle_result: contract.handoff_bundle_result.clone(),
        decision_packet_result: contract.decision_packet_result.clone(),
        execute_frozen_result: contract.execute_frozen_result.clone(),
        current_pre_activation_gate_verdict: contract.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: contract.current_pre_activation_gate_reason.clone(),
        closure_certificate_step: None,
        verify_closure_certificate_command_summary: Some(
            verify_closure_certificate_command_summary(&config.closure_certificate_session_dir),
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
        release_capsule_digest_algorithm: Some(contract.release_capsule_digest_algorithm.clone()),
        notarization_receipt_summary: Some(contract.notarization_receipt_summary.clone()),
        ledger_seal_summary: Some(contract.ledger_seal_summary.clone()),
        ledger_seal_sha256: Some(contract.ledger_seal_sha256.clone()),
        ledger_seal_algorithm: Some(contract.ledger_seal_algorithm.clone()),
        registry_entry_summary: Some(contract.registry_entry_summary.clone()),
        registry_entry_sha256: Some(contract.registry_entry_sha256.clone()),
        registry_entry_algorithm: Some(contract.registry_entry_algorithm.clone()),
        filing_certificate_summary: Some(contract.filing_certificate_summary.clone()),
        filing_certificate_sha256: Some(contract.filing_certificate_sha256.clone()),
        filing_certificate_algorithm: Some(contract.filing_certificate_algorithm.clone()),
        archive_receipt_summary: Some(contract.archive_receipt_summary.clone()),
        archive_receipt_sha256: Some(contract.archive_receipt_sha256.clone()),
        archive_receipt_algorithm: Some(contract.archive_receipt_algorithm.clone()),
        closure_certificate_summary: Some(contract.closure_certificate_summary.clone()),
        closure_certificate_sha256: Some(contract.closure_certificate_sha256.clone()),
        closure_certificate_algorithm: Some(contract.closure_certificate_algorithm.clone()),
        finality_receipt_summary: None,
        finality_receipt_sha256: None,
        finality_receipt_algorithm: Some(FINALITY_RECEIPT_ALGORITHM.to_string()),
        verification_mismatches: mismatches,
        explicit_statement: FINALITY_RECEIPT_VERIFY_EXPLICIT_STATEMENT.to_string(),
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
    step: &Option<PackageFinalityReceiptStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<serde_json::Value> {
    let Some(step) = step else {
        mismatches.push(format!("{label} is missing from finality-receipt status"));
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
) -> PackageFinalityReceiptStepArtifact {
    PackageFinalityReceiptStepArtifact {
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
    actual: Option<&PackageFinalityReceiptStepArtifact>,
    expected: &PackageFinalityReceiptStepArtifact,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    let Some(actual) = actual else {
        mismatches.push(format!("{label} is missing from finality-receipt status"));
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

fn compute_finality_receipt_sha256(
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
    ledger_seal_sha256: &str,
    ledger_seal_algorithm: &str,
    registry_entry_sha256: &str,
    registry_entry_algorithm: &str,
    filing_certificate_sha256: &str,
    filing_certificate_algorithm: &str,
    archive_receipt_sha256: &str,
    archive_receipt_algorithm: &str,
    closure_certificate_sha256: &str,
    closure_certificate_algorithm: &str,
) -> Result<String> {
    canonical_sha256_hex(&FinalityReceiptPayload {
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
        ledger_seal_sha256,
        ledger_seal_algorithm,
        registry_entry_sha256,
        registry_entry_algorithm,
        filing_certificate_sha256,
        filing_certificate_algorithm,
        archive_receipt_sha256,
        archive_receipt_algorithm,
        closure_certificate_sha256,
        closure_certificate_algorithm,
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
            "failed writing finality-receipt script to {}",
            path.display()
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(path)?.permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions)?;
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

fn render_report_lines(report: &PackageFinalityReceiptReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "closure_certificate_session_dir={}",
            report.closure_certificate_session_dir
        ),
    ];
    if let Some(value) = &report.result {
        lines.push(format!("result={value}"));
    }
    if let Some(value) = &report.finality_receipt_sha256 {
        lines.push(format!("finality_receipt_sha256={value}"));
    }
    if let Some(value) = &report.filing_certificate_sha256 {
        lines.push(format!("filing_certificate_sha256={value}"));
    }
    if let Some(value) = &report.archive_receipt_sha256 {
        lines.push(format!("archive_receipt_sha256={value}"));
    }
    if let Some(value) = &report.closure_certificate_sha256 {
        lines.push(format!("closure_certificate_sha256={value}"));
    }
    if let Some(value) = &report.registry_entry_sha256 {
        lines.push(format!("registry_entry_sha256={value}"));
    }
    if let Some(value) = &report.ledger_seal_sha256 {
        lines.push(format!("ledger_seal_sha256={value}"));
    }
    if let Some(value) = &report.chain_fingerprint_sha256 {
        lines.push(format!("chain_fingerprint_sha256={value}"));
    }
    if let Some(value) = &report.session_dir {
        lines.push(format!("session_dir={value}"));
    }
    if !report.verification_mismatches.is_empty() {
        lines.push(format!(
            "verification_mismatches={}",
            report.verification_mismatches.join(" | ")
        ));
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Map, Value};
    use std::ffi::OsString;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn run_ready_for_manual_execution_when_gate_turns_green_then_verify_stays_green() {
        let fixture = fake_command_fixture(
            "finality_receipt_ready",
            closure_certificate_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let run = run_live_package_finality_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptReadyForManualExecutionWhenGateTurnsGreen
        );
        let verify = verify_live_package_finality_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptVerifyOk
        );
    }

    #[test]
    fn stage3_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "finality_receipt_stage3_refusal",
            closure_certificate_verify_report(
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "red",
                "stage3 blocked",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let run = run_live_package_finality_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptRefusedNowByStage3
        );
    }

    #[test]
    fn pre_activation_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "finality_receipt_gate_refusal",
            closure_certificate_verify_report(
                "refused_now_by_pre_activation_gate",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "refused_now_by_pre_activation_gate",
                "red",
                "pre-activation blocked",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let run = run_live_package_finality_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptRefusedNowByPreActivationGate
        );
    }

    #[test]
    fn drifted_closure_certificate_contract_is_refused() {
        let fixture = fake_command_fixture(
            "finality_receipt_drifted_filing",
            json!({
                "generated_at": Utc::now(),
                "mode": "verify_live_package_closure_certificate",
                "verdict": "tiny_live_package_closure_certificate_verify_invalid",
                "reason": "drifted",
                "result": "refused_now_by_invalid_or_drifted_contract",
                "handoff_bundle_result": "ready_for_manual_go_live_review",
                "decision_packet_result": "runnable_when_gate_truth_turns_green",
                "execute_frozen_result": "ready_for_manual_execution_when_gate_turns_green",
                "current_pre_activation_gate_verdict": "red",
                "current_pre_activation_gate_reason": "drifted"
            }),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let run = run_live_package_finality_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptRefusedNowByInvalidOrDriftedContract
        );
    }

    #[test]
    fn run_refuses_managed_surface_overlap_before_writing_any_artifacts() {
        let fixture = fake_command_fixture(
            "finality_receipt_managed_overlap",
            closure_certificate_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let managed_paths = copybot_app::tiny_live_activation::install_target_managed_surface::derive_install_target_managed_surface_paths(
            &fixture.install_root,
        )
        .unwrap();
        let overlap_session_dir = managed_paths.runtime_dir.join("finality-receipt-session");
        fs::create_dir_all(overlap_session_dir.parent().unwrap()).unwrap();

        let config = Config {
            session_dir: Some(overlap_session_dir.clone()),
            ..fixture.run_config()
        };
        let error = run_live_package_finality_receipt_report(&config).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("overlaps the managed live target surface"),
            "{error:#}"
        );
        let paths = finality_receipt_paths(&overlap_session_dir);
        assert!(!paths.session_path.exists());
        assert!(!paths.status_path.exists());
    }

    #[test]
    fn verify_rejects_tampered_closure_certificate_step_path() {
        let fixture = fake_command_fixture(
            "finality_receipt_tampered_step_path",
            closure_certificate_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_live_package_finality_receipt_report(&fixture.run_config()).unwrap();
        let paths = finality_receipt_paths(&fixture.finality_receipt_session_dir);
        let mut status: PackageFinalityReceiptStatus = load_json(&paths.status_path).unwrap();
        status.closure_certificate_step.as_mut().unwrap().path =
            "/tmp/foreign.closure-certificate.report.json".to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_finality_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_finality_receipt_text() {
        let fixture = fake_command_fixture(
            "finality_receipt_tampered_text",
            closure_certificate_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_live_package_finality_receipt_report(&fixture.run_config()).unwrap();
        let paths = finality_receipt_paths(&fixture.finality_receipt_session_dir);
        let mut session: PackageFinalityReceiptSession = load_json(&paths.session_path).unwrap();
        session.finality_receipt_summary = "tampered finality summary".to_string();
        persist_json(&paths.session_path, &session).unwrap();

        let verify = verify_live_package_finality_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_status_result() {
        let fixture = fake_command_fixture(
            "finality_receipt_tampered_result",
            closure_certificate_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_live_package_finality_receipt_report(&fixture.run_config()).unwrap();
        let paths = finality_receipt_paths(&fixture.finality_receipt_session_dir);
        let mut status: PackageFinalityReceiptStatus = load_json(&paths.status_path).unwrap();
        status.result = LivePackageFinalityReceiptResult::RefusedNowByStage3;
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_finality_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_status_gate_fields() {
        let fixture = fake_command_fixture(
            "finality_receipt_tampered_gate_fields",
            closure_certificate_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_live_package_finality_receipt_report(&fixture.run_config()).unwrap();
        let paths = finality_receipt_paths(&fixture.finality_receipt_session_dir);
        let mut status: PackageFinalityReceiptStatus = load_json(&paths.status_path).unwrap();
        status.current_pre_activation_gate_verdict = Some("red".to_string());
        status.current_pre_activation_gate_reason = Some("tampered".to_string());
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_finality_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_nested_closure_certificate_report_content() {
        let fixture = fake_command_fixture(
            "finality_receipt_tampered_nested_report",
            closure_certificate_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_live_package_finality_receipt_report(&fixture.run_config()).unwrap();
        let paths = finality_receipt_paths(&fixture.finality_receipt_session_dir);
        let mut report: serde_json::Value =
            load_json(&paths.closure_certificate_report_path).unwrap();
        report.as_object_mut().unwrap().insert(
            "reason".to_string(),
            json!("tampered nested closure-certificate reason"),
        );
        persist_json(&paths.closure_certificate_report_path, &report).unwrap();

        let verify = verify_live_package_finality_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_retimed_nested_closure_certificate_report_and_step_generated_at() {
        let fixture = fake_command_fixture(
            "finality_receipt_retimed_nested_report",
            closure_certificate_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_live_package_finality_receipt_report(&fixture.run_config()).unwrap();
        let paths = finality_receipt_paths(&fixture.finality_receipt_session_dir);
        let mut report: serde_json::Value =
            load_json(&paths.closure_certificate_report_path).unwrap();
        let archived_generated_at =
            DateTime::parse_from_rfc3339(report["generated_at"].as_str().unwrap())
                .unwrap()
                .with_timezone(&Utc);
        let forged_generated_at = archived_generated_at + chrono::Duration::seconds(61);
        report.as_object_mut().unwrap().insert(
            "generated_at".to_string(),
            json!(forged_generated_at.to_rfc3339()),
        );
        persist_json(&paths.closure_certificate_report_path, &report).unwrap();

        let mut status: PackageFinalityReceiptStatus = load_json(&paths.status_path).unwrap();
        status
            .closure_certificate_step
            .as_mut()
            .unwrap()
            .generated_at = forged_generated_at;
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_finality_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_chain_ledger_registry_filing_archive_or_closure_identity_fields() {
        {
            let fixture = fake_command_fixture(
                "finality_receipt_tampered_chain",
                closure_certificate_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_finality_receipt_report(&fixture.run_config()).unwrap();
            let paths = finality_receipt_paths(&fixture.finality_receipt_session_dir);
            let mut status: PackageFinalityReceiptStatus = load_json(&paths.status_path).unwrap();
            status.chain_fingerprint_sha256 = "tampered-chain".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_finality_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "finality_receipt_tampered_ledger",
                closure_certificate_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_finality_receipt_report(&fixture.run_config()).unwrap();
            let paths = finality_receipt_paths(&fixture.finality_receipt_session_dir);
            let mut status: PackageFinalityReceiptStatus = load_json(&paths.status_path).unwrap();
            status.ledger_seal_sha256 = "tampered-ledger".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_finality_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "finality_receipt_tampered_registry",
                closure_certificate_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_finality_receipt_report(&fixture.run_config()).unwrap();
            let paths = finality_receipt_paths(&fixture.finality_receipt_session_dir);
            let mut status: PackageFinalityReceiptStatus = load_json(&paths.status_path).unwrap();
            status.registry_entry_sha256 = "tampered-registry".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_finality_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "finality_receipt_tampered_filing",
                closure_certificate_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_finality_receipt_report(&fixture.run_config()).unwrap();
            let paths = finality_receipt_paths(&fixture.finality_receipt_session_dir);
            let mut status: PackageFinalityReceiptStatus = load_json(&paths.status_path).unwrap();
            status.filing_certificate_sha256 = "tampered-filing".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_finality_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "finality_receipt_tampered_archive",
                closure_certificate_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_finality_receipt_report(&fixture.run_config()).unwrap();
            let paths = finality_receipt_paths(&fixture.finality_receipt_session_dir);
            let mut status: PackageFinalityReceiptStatus = load_json(&paths.status_path).unwrap();
            status.archive_receipt_sha256 = "tampered-archive".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_finality_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "finality_receipt_tampered_closure_identity",
                closure_certificate_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_finality_receipt_report(&fixture.run_config()).unwrap();
            let paths = finality_receipt_paths(&fixture.finality_receipt_session_dir);
            let mut status: PackageFinalityReceiptStatus = load_json(&paths.status_path).unwrap();
            status.closure_certificate_sha256 = "tampered-closure".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_finality_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "finality_receipt_tampered_finality_identity",
                closure_certificate_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_finality_receipt_report(&fixture.run_config()).unwrap();
            let paths = finality_receipt_paths(&fixture.finality_receipt_session_dir);
            let mut status: PackageFinalityReceiptStatus = load_json(&paths.status_path).unwrap();
            status.finality_receipt_sha256 = "tampered-finality".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_finality_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageFinalityReceiptVerdict::TinyLivePackageFinalityReceiptVerifyInvalid
            );
        }
    }

    struct FakeCommandFixture {
        bin_dir: PathBuf,
        closure_certificate_session_dir: PathBuf,
        finality_receipt_session_dir: PathBuf,
        decision_packet_session_dir: PathBuf,
        install_root: PathBuf,
    }

    impl FakeCommandFixture {
        fn run_config(&self) -> Config {
            Config {
                mode: Mode::RunLivePackageFinalityReceipt,
                closure_certificate_session_dir: self.closure_certificate_session_dir.clone(),
                confirmed_decision_packet_session_dir: Some(
                    self.decision_packet_session_dir.clone(),
                ),
                session_dir: Some(self.finality_receipt_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn verify_config(&self) -> Config {
            Config {
                mode: Mode::VerifyLivePackageFinalityReceipt,
                closure_certificate_session_dir: self.closure_certificate_session_dir.clone(),
                confirmed_decision_packet_session_dir: Some(
                    self.decision_packet_session_dir.clone(),
                ),
                session_dir: Some(self.finality_receipt_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }
    }

    fn fake_command_fixture(label: &str, verify_report: serde_json::Value) -> FakeCommandFixture {
        let fixture_dir = temp_dir(label);
        let bin_dir = fixture_dir.join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        let finality_receipt_session_dir = fixture_dir.join("finality-receipt-session");
        let closure_certificate_session_dir = fixture_dir.join("closure-certificate-session");
        let archive_receipt_session_dir = fixture_dir.join("archive-receipt-session");
        let registry_entry_session_dir = fixture_dir.join("registry-entry-session");
        let notarization_receipt_session_dir = fixture_dir.join("notarization-receipt-session");
        let provenance_certificate_session_dir = fixture_dir.join("provenance-certificate-session");
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
            &closure_certificate_session_dir,
            &archive_receipt_session_dir,
            &registry_entry_session_dir,
            &notarization_receipt_session_dir,
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

        persist_json(
            &closure_certificate_session_dir
                .join("tiny_live_activation_package_closure_certificate.session.json"),
            &stored_closure_certificate_session_fixture(
                &archive_receipt_session_dir,
                &registry_entry_session_dir,
                &notarization_receipt_session_dir,
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
                &backend_command,
                &fixture_dir.join("frozen-live-cutover-session"),
            ),
        )
        .unwrap();
        persist_json(
            &closure_certificate_session_dir
                .join("tiny_live_activation_package_closure_certificate.status.json"),
            &stored_closure_certificate_status_fixture(&verify_report),
        )
        .unwrap();
        persist_json(
            &closure_certificate_session_dir.join(
                "tiny_live_activation_package_closure_certificate.archive_receipt.report.json",
            ),
            &json!({
                "decision_packet_session_dir": decision_packet_session_dir.display().to_string()
            }),
        )
        .unwrap();

        let mut verify_report = verify_report;
        bind_closure_certificate_report_to_fixture(
            &mut verify_report,
            &archive_receipt_session_dir,
            &registry_entry_session_dir,
            &notarization_receipt_session_dir,
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
            &backend_command,
        );
        let verify_path = fixture_dir.join("closure-certificate.verify.json");
        persist_json(&verify_path, &verify_report).unwrap();
        write_fake_closure_certificate_verify_command(
            &bin_dir.join("copybot_tiny_live_activation_package_closure_certificate"),
            &verify_path,
            &archive_receipt_session_dir,
            &decision_packet_session_dir,
            &closure_certificate_session_dir,
        );

        FakeCommandFixture {
            bin_dir,
            closure_certificate_session_dir,
            finality_receipt_session_dir,
            decision_packet_session_dir,
            install_root,
        }
    }

    fn bind_closure_certificate_report_to_fixture(
        verify_report: &mut serde_json::Value,
        archive_receipt_session_dir: &Path,
        registry_entry_session_dir: &Path,
        notarization_receipt_session_dir: &Path,
        provenance_certificate_session_dir: &Path,
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
    ) {
        let object = verify_report.as_object_mut().unwrap();
        object.insert(
            "archive_receipt_session_dir".to_string(),
            json!(archive_receipt_session_dir.display().to_string()),
        );
        object.insert(
            "registry_entry_session_dir".to_string(),
            json!(registry_entry_session_dir.display().to_string()),
        );
        object.insert(
            "notarization_receipt_session_dir".to_string(),
            json!(notarization_receipt_session_dir.display().to_string()),
        );
        object.insert(
            "provenance_certificate_session_dir".to_string(),
            json!(provenance_certificate_session_dir.display().to_string()),
        );
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
            "verify_archive_receipt_command_summary".to_string(),
            json!(format!(
                "verify immutable archive-receipt session under {} before freezing the final closure certificate",
                archive_receipt_session_dir.display()
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
            json!("release-capsule-manifest-sha256"),
        );
        object.insert(
            "release_capsule_digest_manifest_entry_count".to_string(),
            json!(3),
        );
        object.insert(
            "release_capsule_digest_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "notarization_receipt_summary".to_string(),
            json!("Notarization receipt summary."),
        );
        object.insert(
            "ledger_seal_summary".to_string(),
            json!("Ledger seal summary."),
        );
        object.insert(
            "ledger_seal_sha256".to_string(),
            json!("ledger-seal-sha256"),
        );
        object.insert("ledger_seal_algorithm".to_string(), json!("sha256"));
        object.insert(
            "registry_entry_summary".to_string(),
            json!("Registry entry summary."),
        );
        object.insert(
            "registry_entry_sha256".to_string(),
            json!("registry-entry-sha256"),
        );
        object.insert("registry_entry_algorithm".to_string(), json!("sha256"));
        object.insert(
            "filing_certificate_summary".to_string(),
            json!("Filing certificate summary."),
        );
        object.insert(
            "filing_certificate_sha256".to_string(),
            json!("filing-certificate-sha256"),
        );
        object.insert("filing_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "archive_receipt_summary".to_string(),
            json!("Archive receipt summary."),
        );
        object.insert(
            "archive_receipt_sha256".to_string(),
            json!("archive-receipt-sha256"),
        );
        object.insert("archive_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "closure_certificate_summary".to_string(),
            json!("Closure certificate summary."),
        );
        object.insert(
            "closure_certificate_sha256".to_string(),
            json!("closure-certificate-sha256"),
        );
        object.insert("closure_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "explicit_statement".to_string(),
            json!("closure-certificate verify statement"),
        );
    }

    fn closure_certificate_verify_report(
        result: &str,
        handoff_bundle_result: &str,
        decision_packet_result: &str,
        execute_frozen_result: &str,
        gate_verdict: &str,
        gate_reason: &str,
    ) -> serde_json::Value {
        json!({
            "generated_at": Utc::now(),
            "mode": "verify_live_package_closure_certificate",
            "verdict": "tiny_live_package_closure_certificate_verify_ok",
            "reason": "closure-certificate chain is coherent",
            "result": result,
            "handoff_bundle_result": handoff_bundle_result,
            "decision_packet_result": decision_packet_result,
            "execute_frozen_result": execute_frozen_result,
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason
        })
    }

    fn write_fake_closure_certificate_verify_command(
        path: &Path,
        verify_path: &Path,
        expected_archive_receipt_session_dir: &Path,
        expected_decision_packet_session_dir: &Path,
        closure_certificate_session_dir: &Path,
    ) {
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --archive-receipt-session-dir {} \"*) : ;;\n  *) echo 'unexpected archive-receipt-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --confirm-decision-packet-session-dir {} \"*) : ;;\n  *) echo 'unexpected decision-packet-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected closure-certificate session dir' >&2; exit 1;;\nesac\ncat '{}'\n",
            shell_escape_path(expected_archive_receipt_session_dir),
            shell_escape_path(expected_decision_packet_session_dir),
            shell_escape_path(closure_certificate_session_dir),
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
            let lock = LOCK
                .get_or_init(|| Mutex::new(()))
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
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

    fn stored_closure_certificate_session_fixture(
        archive_receipt_session_dir: &Path,
        registry_entry_session_dir: &Path,
        notarization_receipt_session_dir: &Path,
        provenance_certificate_session_dir: &Path,
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
        frozen_live_cutover_session_dir: &Path,
    ) -> Value {
        let mut object = Map::new();
        object.insert(
            "archive_receipt_session_dir".to_string(),
            json!(archive_receipt_session_dir.display().to_string()),
        );
        object.insert(
            "registry_entry_session_dir".to_string(),
            json!(registry_entry_session_dir.display().to_string()),
        );
        object.insert(
            "notarization_receipt_session_dir".to_string(),
            json!(notarization_receipt_session_dir.display().to_string()),
        );
        object.insert(
            "provenance_certificate_session_dir".to_string(),
            json!(provenance_certificate_session_dir.display().to_string()),
        );
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
        object.insert("wrapper_timeout_ms".to_string(), json!(1200));
        object.insert("service_status_max_staleness_ms".to_string(), json!(4500));
        object.insert(
            "verify_archive_receipt_command_summary".to_string(),
            json!(format!(
                "verify immutable archive-receipt session under {} before freezing the final closure certificate",
                archive_receipt_session_dir.display()
            )),
        );
        object.insert(
            "reviewed_frozen_live_cutover_controller_command_summary".to_string(),
            json!(format!(
                "copybot_tiny_live_activation_package_live_cutover --package-dir {} --install-root {} --target-service solana-copy-bot.service --backend-command {} --wrapper-timeout-ms 1200 --service-status-max-staleness-ms 4500 --session-dir {} --run-live-package-cutover",
                shell_escape_path(package_dir),
                shell_escape_path(install_root),
                shell_escape_path(backend_command),
                shell_escape_path(frozen_live_cutover_session_dir),
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
            json!("release-capsule-manifest-sha256"),
        );
        object.insert(
            "release_capsule_digest_manifest_entry_count".to_string(),
            json!(3),
        );
        object.insert(
            "release_capsule_digest_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "notarization_receipt_summary".to_string(),
            json!("Notarization receipt summary."),
        );
        object.insert(
            "ledger_seal_summary".to_string(),
            json!("Ledger seal summary."),
        );
        object.insert(
            "ledger_seal_sha256".to_string(),
            json!("ledger-seal-sha256"),
        );
        object.insert("ledger_seal_algorithm".to_string(), json!("sha256"));
        object.insert(
            "registry_entry_summary".to_string(),
            json!("Registry entry summary."),
        );
        object.insert(
            "registry_entry_sha256".to_string(),
            json!("registry-entry-sha256"),
        );
        object.insert("registry_entry_algorithm".to_string(), json!("sha256"));
        object.insert(
            "filing_certificate_summary".to_string(),
            json!("Filing certificate summary."),
        );
        object.insert(
            "filing_certificate_sha256".to_string(),
            json!("filing-certificate-sha256"),
        );
        object.insert("filing_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "archive_receipt_summary".to_string(),
            json!("Archive receipt summary."),
        );
        object.insert(
            "archive_receipt_sha256".to_string(),
            json!("archive-receipt-sha256"),
        );
        object.insert("archive_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "closure_certificate_summary".to_string(),
            json!("Closure certificate summary."),
        );
        object.insert(
            "closure_certificate_sha256".to_string(),
            json!("closure-certificate-sha256"),
        );
        object.insert("closure_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "explicit_statement".to_string(),
            json!("closure-certificate statement"),
        );
        Value::Object(object)
    }

    fn stored_closure_certificate_status_fixture(verify_report: &Value) -> Value {
        let mut object = Map::new();
        object.insert(
            "result".to_string(),
            json!(verify_report
                .get("result")
                .and_then(|value| value.as_str())
                .unwrap()),
        );
        object.insert(
            "handoff_bundle_result".to_string(),
            json!(verify_report
                .get("handoff_bundle_result")
                .and_then(|value| value.as_str())),
        );
        object.insert(
            "decision_packet_result".to_string(),
            json!(verify_report
                .get("decision_packet_result")
                .and_then(|value| value.as_str())),
        );
        object.insert(
            "execute_frozen_result".to_string(),
            json!(verify_report
                .get("execute_frozen_result")
                .and_then(|value| value.as_str())),
        );
        object.insert(
            "current_pre_activation_gate_verdict".to_string(),
            json!(verify_report
                .get("current_pre_activation_gate_verdict")
                .and_then(|value| value.as_str())),
        );
        object.insert(
            "current_pre_activation_gate_reason".to_string(),
            json!(verify_report
                .get("current_pre_activation_gate_reason")
                .and_then(|value| value.as_str())),
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
            json!("release-capsule-manifest-sha256"),
        );
        object.insert(
            "release_capsule_digest_manifest_entry_count".to_string(),
            json!(3),
        );
        object.insert(
            "release_capsule_digest_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "notarization_receipt_summary".to_string(),
            json!("Notarization receipt summary."),
        );
        object.insert(
            "ledger_seal_summary".to_string(),
            json!("Ledger seal summary."),
        );
        object.insert(
            "ledger_seal_sha256".to_string(),
            json!("ledger-seal-sha256"),
        );
        object.insert("ledger_seal_algorithm".to_string(), json!("sha256"));
        object.insert(
            "registry_entry_summary".to_string(),
            json!("Registry entry summary."),
        );
        object.insert(
            "registry_entry_sha256".to_string(),
            json!("registry-entry-sha256"),
        );
        object.insert("registry_entry_algorithm".to_string(), json!("sha256"));
        object.insert(
            "filing_certificate_summary".to_string(),
            json!("Filing certificate summary."),
        );
        object.insert(
            "filing_certificate_sha256".to_string(),
            json!("filing-certificate-sha256"),
        );
        object.insert("filing_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "archive_receipt_summary".to_string(),
            json!("Archive receipt summary."),
        );
        object.insert(
            "archive_receipt_sha256".to_string(),
            json!("archive-receipt-sha256"),
        );
        object.insert("archive_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "closure_certificate_summary".to_string(),
            json!("Closure certificate summary."),
        );
        object.insert(
            "closure_certificate_sha256".to_string(),
            json!("closure-certificate-sha256"),
        );
        object.insert("closure_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "explicit_statement".to_string(),
            json!("closure-certificate status statement"),
        );
        Value::Object(object)
    }
}
