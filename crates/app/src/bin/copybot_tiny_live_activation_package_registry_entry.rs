use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_app::tiny_live_activation::install_target_managed_surface::validate_turn_green_session_dir as validate_managed_surface_session_dir;
use copybot_app::tiny_live_activation::package_notarization_receipt_registry_entry;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: copybot_tiny_live_activation_package_registry_entry --notarization-receipt-session-dir <path> [--confirm-decision-packet-session-dir <path>] [--session-dir <path>] [--output <path>] [--json] (--plan-live-package-registry-entry | --render-live-package-registry-entry | --run-live-package-registry-entry | --verify-live-package-registry-entry)";
const REGISTRY_ENTRY_SESSION_VERSION: &str = "1";
const REGISTRY_ENTRY_STATUS_VERSION: &str = "1";
const REGISTRY_ENTRY_ALGORITHM: &str = "sha256";
const REGISTRY_ENTRY_SESSION_EXPLICIT_STATEMENT: &str =
    "the verified notarization-receipt session is the primary input here; this immutable registry entry freezes one final top-level docket-style identity over the notarized archival chain without enabling production execution";
const REGISTRY_ENTRY_STATUS_EXPLICIT_STATEMENT: &str =
    "this registry entry is archival and read-only; it never executes or enables the frozen controller";
const REGISTRY_ENTRY_VERIFY_EXPLICIT_STATEMENT: &str =
    "this verification binds one immutable registry entry to verified notarization-receipt truth, the exact reviewed frozen controller summary, the canonical chain fingerprint, and the top-level ledger-seal identity";

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
    notarization_receipt_session_dir: PathBuf,
    confirmed_decision_packet_session_dir: Option<PathBuf>,
    session_dir: Option<PathBuf>,
    output_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanLivePackageRegistryEntry,
    RenderLivePackageRegistryEntry,
    RunLivePackageRegistryEntry,
    VerifyLivePackageRegistryEntry,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageRegistryEntryVerdict {
    TinyLivePackageRegistryEntryPlanReady,
    TinyLivePackageRegistryEntryRendered,
    TinyLivePackageRegistryEntryRefusedNowByStage3,
    TinyLivePackageRegistryEntryRefusedNowByPreActivationGate,
    TinyLivePackageRegistryEntryRefusedNowByInvalidOrDriftedContract,
    TinyLivePackageRegistryEntryReadyForManualExecutionWhenGateTurnsGreen,
    TinyLivePackageRegistryEntryVerifyOk,
    TinyLivePackageRegistryEntryVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageRegistryEntryResult {
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    RefusedNowByInvalidOrDriftedContract,
    ReadyForManualExecutionWhenGateTurnsGreen,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PackageRegistryEntryStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PackageRegistryEntrySession {
    session_version: String,
    planned_at: DateTime<Utc>,
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
    verify_notarization_receipt_command_summary: String,
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
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PackageRegistryEntryStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    notarization_receipt_session_dir: String,
    result: LivePackageRegistryEntryResult,
    reason: String,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    notarization_receipt_step: Option<PackageRegistryEntryStepArtifact>,
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
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageRegistryEntryPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    notarization_receipt_report_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageRegistryEntryReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageRegistryEntryVerdict,
    reason: String,
    notarization_receipt_session_dir: String,
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
    registry_entry_session_path: Option<String>,
    registry_entry_status_path: Option<String>,
    archived_notarization_receipt_report_path: Option<String>,
    result: Option<String>,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    notarization_receipt_step: Option<PackageRegistryEntryStepArtifact>,
    verify_notarization_receipt_command_summary: Option<String>,
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
    verification_mismatches: Vec<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct NestedNotarizationReceiptReportView {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: String,
    reason: String,
}

#[derive(Debug, Serialize)]
struct RegistryEntryPayload<'a> {
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
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut notarization_receipt_session_dir: Option<PathBuf> = None;
    let mut confirmed_decision_packet_session_dir: Option<PathBuf> = None;
    let mut session_dir: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut json = false;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--notarization-receipt-session-dir" => {
                notarization_receipt_session_dir = Some(PathBuf::from(parse_string_arg(
                    "--notarization-receipt-session-dir",
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
            "--plan-live-package-registry-entry" => {
                set_mode(&mut mode, Mode::PlanLivePackageRegistryEntry, arg.as_str())?
            }
            "--render-live-package-registry-entry" => set_mode(
                &mut mode,
                Mode::RenderLivePackageRegistryEntry,
                arg.as_str(),
            )?,
            "--run-live-package-registry-entry" => {
                set_mode(&mut mode, Mode::RunLivePackageRegistryEntry, arg.as_str())?
            }
            "--verify-live-package-registry-entry" => set_mode(
                &mut mode,
                Mode::VerifyLivePackageRegistryEntry,
                arg.as_str(),
            )?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let Some(mode) = mode else {
        return Ok(None);
    };
    if matches!(mode, Mode::RenderLivePackageRegistryEntry) && output_path.is_none() {
        bail!("missing --output for --render-live-package-registry-entry");
    }
    if matches!(
        mode,
        Mode::RunLivePackageRegistryEntry | Mode::VerifyLivePackageRegistryEntry
    ) && session_dir.is_none()
    {
        bail!("missing --session-dir for run/verify mode");
    }
    if matches!(
        mode,
        Mode::RunLivePackageRegistryEntry | Mode::VerifyLivePackageRegistryEntry
    ) && confirmed_decision_packet_session_dir.is_none()
    {
        bail!("missing --confirm-decision-packet-session-dir for run/verify mode");
    }
    let notarization_receipt_session_dir = notarization_receipt_session_dir
        .ok_or_else(|| anyhow!("missing --notarization-receipt-session-dir"))?;
    Ok(Some(Config {
        mode,
        notarization_receipt_session_dir,
        confirmed_decision_packet_session_dir,
        session_dir,
        output_path,
        json,
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanLivePackageRegistryEntry => plan_live_package_registry_entry_report(&config)?,
        Mode::RenderLivePackageRegistryEntry => render_live_package_registry_entry_report(&config)?,
        Mode::RunLivePackageRegistryEntry => run_live_package_registry_entry_report(&config)?,
        Mode::VerifyLivePackageRegistryEntry => verify_live_package_registry_entry_report(&config)?,
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_live_package_registry_entry_report(config: &Config) -> Result<PackageRegistryEntryReport> {
    let (notarization_receipt, classification) =
        match config.confirmed_decision_packet_session_dir.as_deref() {
            Some(confirmed_decision_packet_session_dir) => {
                match package_notarization_receipt_registry_entry::verify_live_package_notarization_receipt_for_registry_entry(
                    &config.notarization_receipt_session_dir,
                    confirmed_decision_packet_session_dir,
                ) {
                    Ok(verified) => {
                        let classification = classify_verified_notarization_receipt(&verified);
                        (verified.contract, classification)
                    }
                    Err(_) => {
                        let raw_contract = package_notarization_receipt_registry_entry::load_live_package_notarization_receipt_contract_for_registry_entry(
                            &config.notarization_receipt_session_dir,
                        )?;
                        let classification = classify_registry_contract(&raw_contract, false);
                        (raw_contract, classification)
                    }
                }
            }
            None => {
                let raw_contract = package_notarization_receipt_registry_entry::load_live_package_notarization_receipt_contract_for_registry_entry(
                    &config.notarization_receipt_session_dir,
                )?;
                let classification = classify_registry_contract(&raw_contract, false);
                (raw_contract, classification)
            }
        };
    let session_dir = config.session_dir.clone().unwrap_or_else(|| {
        config
            .notarization_receipt_session_dir
            .join("registry-entry")
    });
    let result = serialize_enum(&classification.0);
    let registry_entry_sha256 = compute_registry_entry_sha256(
        &result,
        notarization_receipt.handoff_bundle_result.as_deref(),
        notarization_receipt.decision_packet_result.as_deref(),
        notarization_receipt.execute_frozen_result.as_deref(),
        notarization_receipt
            .current_pre_activation_gate_verdict
            .as_deref(),
        notarization_receipt
            .current_pre_activation_gate_reason
            .as_deref(),
        &notarization_receipt.reviewed_frozen_live_cutover_controller_command_summary,
        &notarization_receipt.chain_fingerprint_sha256,
        &notarization_receipt.chain_fingerprint_algorithm,
        &notarization_receipt.release_capsule_digest_manifest_sha256,
        notarization_receipt.release_capsule_digest_manifest_entry_count,
        &notarization_receipt.release_capsule_digest_algorithm,
        &notarization_receipt.ledger_seal_sha256,
        &notarization_receipt.ledger_seal_algorithm,
    )?;

    Ok(PackageRegistryEntryReport {
        generated_at: Utc::now(),
        mode: "plan_live_package_registry_entry".to_string(),
        verdict: TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryPlanReady,
        reason: format!(
            "notarization-receipt-native registry entry is explicit for verified notarization-receipt session {}; this command stays archival and read-only while freezing one final registry-style identity over the sealed chain",
            config.notarization_receipt_session_dir.display()
        ),
        notarization_receipt_session_dir: config
            .notarization_receipt_session_dir
            .display()
            .to_string(),
        provenance_certificate_session_dir: Some(
            notarization_receipt
                .provenance_certificate_session_dir
                .display()
                .to_string(),
        ),
        attestation_seal_session_dir: Some(
            notarization_receipt
                .attestation_seal_session_dir
                .display()
                .to_string(),
        ),
        release_capsule_session_dir: Some(
            notarization_receipt
                .release_capsule_session_dir
                .display()
                .to_string(),
        ),
        activation_ticket_session_dir: Some(
            notarization_receipt
                .activation_ticket_session_dir
                .display()
                .to_string(),
        ),
        review_receipt_session_dir: Some(
            notarization_receipt
                .review_receipt_session_dir
                .display()
                .to_string(),
        ),
        handoff_bundle_session_dir: Some(
            notarization_receipt
                .handoff_bundle_session_dir
                .display()
                .to_string(),
        ),
        decision_packet_session_dir: Some(
            notarization_receipt
                .decision_packet_session_dir
                .display()
                .to_string(),
        ),
        execute_frozen_session_dir: Some(
            notarization_receipt
                .execute_frozen_session_dir
                .display()
                .to_string(),
        ),
        turn_green_session_dir: Some(
            notarization_receipt.turn_green_session_dir.display().to_string(),
        ),
        launch_packet_session_dir: Some(
            notarization_receipt
                .launch_packet_session_dir
                .display()
                .to_string(),
        ),
        package_dir: Some(notarization_receipt.package_dir.display().to_string()),
        install_root: Some(notarization_receipt.install_root.display().to_string()),
        target_service_name: Some(notarization_receipt.target_service_name.clone()),
        backend_command: Some(notarization_receipt.backend_command.clone()),
        wrapper_timeout_ms: Some(notarization_receipt.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(
            notarization_receipt.service_status_max_staleness_ms,
        ),
        session_dir: Some(session_dir.display().to_string()),
        registry_entry_session_path: None,
        registry_entry_status_path: None,
        archived_notarization_receipt_report_path: None,
        result: Some(result),
        handoff_bundle_result: notarization_receipt.handoff_bundle_result.clone(),
        decision_packet_result: notarization_receipt.decision_packet_result.clone(),
        execute_frozen_result: notarization_receipt.execute_frozen_result.clone(),
        current_pre_activation_gate_verdict: notarization_receipt
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: notarization_receipt
            .current_pre_activation_gate_reason
            .clone(),
        notarization_receipt_step: None,
        verify_notarization_receipt_command_summary: Some(
            verify_notarization_receipt_command_summary(&config.notarization_receipt_session_dir),
        ),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            notarization_receipt
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        provenance_certificate_summary: Some(
            notarization_receipt.provenance_certificate_summary.clone(),
        ),
        chain_fingerprint_summary: Some(notarization_receipt.chain_fingerprint_summary.clone()),
        chain_fingerprint_sha256: Some(notarization_receipt.chain_fingerprint_sha256.clone()),
        chain_fingerprint_algorithm: Some(
            notarization_receipt.chain_fingerprint_algorithm.clone(),
        ),
        release_capsule_digest_manifest_sha256: Some(
            notarization_receipt
                .release_capsule_digest_manifest_sha256
                .clone(),
        ),
        release_capsule_digest_manifest_entry_count: Some(
            notarization_receipt.release_capsule_digest_manifest_entry_count,
        ),
        release_capsule_digest_algorithm: Some(
            notarization_receipt.release_capsule_digest_algorithm.clone(),
        ),
        notarization_receipt_summary: Some(notarization_receipt.notarization_receipt_summary.clone()),
        ledger_seal_summary: Some(notarization_receipt.ledger_seal_summary.clone()),
        ledger_seal_sha256: Some(notarization_receipt.ledger_seal_sha256.clone()),
        ledger_seal_algorithm: Some(notarization_receipt.ledger_seal_algorithm.clone()),
        registry_entry_summary: Some(registry_entry_summary(
            classification.0,
            &notarization_receipt.reviewed_frozen_live_cutover_controller_command_summary,
        )),
        registry_entry_sha256: Some(registry_entry_sha256),
        registry_entry_algorithm: Some(REGISTRY_ENTRY_ALGORITHM.to_string()),
        verification_mismatches: Vec::new(),
        explicit_statement: REGISTRY_ENTRY_SESSION_EXPLICIT_STATEMENT.to_string(),
    })
}

fn render_live_package_registry_entry_report(
    config: &Config,
) -> Result<PackageRegistryEntryReport> {
    let plan = plan_live_package_registry_entry_report(config)?;
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
        "#!/bin/sh\nset -eu\ncopybot_tiny_live_activation_package_registry_entry --notarization-receipt-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --run-live-package-registry-entry --json\ncopybot_tiny_live_activation_package_registry_entry --notarization-receipt-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --verify-live-package-registry-entry --json\n",
        shell_escape_path(&config.notarization_receipt_session_dir),
        shell_escape_path(&confirmed_decision_packet_session_dir),
        shell_escape_path(&session_dir),
        shell_escape_path(&config.notarization_receipt_session_dir),
        shell_escape_path(&confirmed_decision_packet_session_dir),
        shell_escape_path(&session_dir),
    );
    write_script(output_path, &script)?;
    Ok(PackageRegistryEntryReport {
        generated_at: Utc::now(),
        mode: "render_live_package_registry_entry".to_string(),
        verdict: TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryRendered,
        reason: format!(
            "rendered registry-entry run/verify script to {}; the primary input remains the immutable notarization-receipt session and run/verify still require the decision-packet confirmation anchor",
            output_path.display()
        ),
        explicit_statement: REGISTRY_ENTRY_SESSION_EXPLICIT_STATEMENT.to_string(),
        ..plan
    })
}

fn run_live_package_registry_entry_report(config: &Config) -> Result<PackageRegistryEntryReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for run mode"))?;
    let verified_notarization_receipt = match package_notarization_receipt_registry_entry::verify_live_package_notarization_receipt_for_registry_entry(
        &config.notarization_receipt_session_dir,
        config
            .confirmed_decision_packet_session_dir
            .as_deref()
            .ok_or_else(|| anyhow!("missing --confirm-decision-packet-session-dir for run mode"))?,
    ) {
        Ok(value) => value,
        Err(error) => {
            return Ok(failure_report_without_contract(
                config,
                TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryRefusedNowByInvalidOrDriftedContract,
                format!(
                    "failed to verify notarization-receipt session under {}: {error}",
                    config.notarization_receipt_session_dir.display()
                ),
            ))
        }
    };

    validate_managed_surface_session_dir(&verified_notarization_receipt.contract.install_root, session_dir)
        .with_context(|| {
            format!(
                "registry-entry session dir {} overlaps the managed live target surface derived from verified notarization-receipt install_root {}",
                session_dir.display(),
                verified_notarization_receipt.contract.install_root.display()
            )
        })?;
    ensure_clean_registry_entry_session_dir(session_dir)?;
    let paths = registry_entry_paths(session_dir);
    let classification = classify_verified_notarization_receipt(&verified_notarization_receipt);
    let result = classification.0;
    let registry_entry_sha256 = compute_registry_entry_sha256(
        &serialize_enum(&result),
        verified_notarization_receipt
            .contract
            .handoff_bundle_result
            .as_deref(),
        verified_notarization_receipt
            .contract
            .decision_packet_result
            .as_deref(),
        verified_notarization_receipt
            .contract
            .execute_frozen_result
            .as_deref(),
        verified_notarization_receipt
            .contract
            .current_pre_activation_gate_verdict
            .as_deref(),
        verified_notarization_receipt
            .contract
            .current_pre_activation_gate_reason
            .as_deref(),
        &verified_notarization_receipt
            .contract
            .reviewed_frozen_live_cutover_controller_command_summary,
        &verified_notarization_receipt
            .contract
            .chain_fingerprint_sha256,
        &verified_notarization_receipt
            .contract
            .chain_fingerprint_algorithm,
        &verified_notarization_receipt
            .contract
            .release_capsule_digest_manifest_sha256,
        verified_notarization_receipt
            .contract
            .release_capsule_digest_manifest_entry_count,
        &verified_notarization_receipt
            .contract
            .release_capsule_digest_algorithm,
        &verified_notarization_receipt.contract.ledger_seal_sha256,
        &verified_notarization_receipt.contract.ledger_seal_algorithm,
    )?;
    let session = planned_session(
        config,
        session_dir,
        &verified_notarization_receipt.contract,
        result,
        &registry_entry_sha256,
    );
    persist_json(&paths.session_path, &session)?;
    persist_json(
        &paths.notarization_receipt_report_path,
        &verified_notarization_receipt.report_json,
    )?;
    let notarization_receipt_step = Some(step_artifact(
        &paths.notarization_receipt_report_path,
        "verify_live_package_notarization_receipt",
        &verified_notarization_receipt.verdict,
        &verified_notarization_receipt.reason,
        verified_notarization_receipt.generated_at,
    ));
    let status = PackageRegistryEntryStatus {
        status_version: REGISTRY_ENTRY_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        notarization_receipt_session_dir: config
            .notarization_receipt_session_dir
            .display()
            .to_string(),
        result,
        reason: classification.2.clone(),
        handoff_bundle_result: verified_notarization_receipt
            .contract
            .handoff_bundle_result
            .clone(),
        decision_packet_result: verified_notarization_receipt
            .contract
            .decision_packet_result
            .clone(),
        execute_frozen_result: verified_notarization_receipt
            .contract
            .execute_frozen_result
            .clone(),
        current_pre_activation_gate_verdict: verified_notarization_receipt
            .contract
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: verified_notarization_receipt
            .contract
            .current_pre_activation_gate_reason
            .clone(),
        notarization_receipt_step: notarization_receipt_step.clone(),
        provenance_certificate_summary: verified_notarization_receipt
            .contract
            .provenance_certificate_summary
            .clone(),
        chain_fingerprint_summary: verified_notarization_receipt
            .contract
            .chain_fingerprint_summary
            .clone(),
        chain_fingerprint_sha256: verified_notarization_receipt
            .contract
            .chain_fingerprint_sha256
            .clone(),
        chain_fingerprint_algorithm: verified_notarization_receipt
            .contract
            .chain_fingerprint_algorithm
            .clone(),
        release_capsule_digest_manifest_sha256: verified_notarization_receipt
            .contract
            .release_capsule_digest_manifest_sha256
            .clone(),
        release_capsule_digest_manifest_entry_count: verified_notarization_receipt
            .contract
            .release_capsule_digest_manifest_entry_count,
        release_capsule_digest_algorithm: verified_notarization_receipt
            .contract
            .release_capsule_digest_algorithm
            .clone(),
        notarization_receipt_summary: verified_notarization_receipt
            .contract
            .notarization_receipt_summary
            .clone(),
        ledger_seal_summary: verified_notarization_receipt
            .contract
            .ledger_seal_summary
            .clone(),
        ledger_seal_sha256: verified_notarization_receipt
            .contract
            .ledger_seal_sha256
            .clone(),
        ledger_seal_algorithm: verified_notarization_receipt
            .contract
            .ledger_seal_algorithm
            .clone(),
        registry_entry_summary: registry_entry_summary(
            result,
            &verified_notarization_receipt
                .contract
                .reviewed_frozen_live_cutover_controller_command_summary,
        ),
        registry_entry_sha256: registry_entry_sha256.clone(),
        registry_entry_algorithm: REGISTRY_ENTRY_ALGORITHM.to_string(),
        explicit_statement: REGISTRY_ENTRY_STATUS_EXPLICIT_STATEMENT.to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    Ok(report_from_status(
        config,
        session_dir,
        &paths,
        &verified_notarization_receipt.contract,
        classification.1,
        &status,
    ))
}

fn verify_live_package_registry_entry_report(
    config: &Config,
) -> Result<PackageRegistryEntryReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = registry_entry_paths(session_dir);
    let session: PackageRegistryEntrySession = load_json(&paths.session_path)?;
    let status: PackageRegistryEntryStatus = load_json(&paths.status_path)?;
    let mut mismatches = Vec::new();

    compare_string(
        &session.notarization_receipt_session_dir,
        &config
            .notarization_receipt_session_dir
            .display()
            .to_string(),
        "registry-entry notarization_receipt_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "registry-entry session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "registry-entry status session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.notarization_receipt_session_dir,
        &config
            .notarization_receipt_session_dir
            .display()
            .to_string(),
        "registry-entry status notarization_receipt_session_dir",
        &mut mismatches,
    );

    let verified_notarization_receipt = match package_notarization_receipt_registry_entry::verify_live_package_notarization_receipt_for_registry_entry(
        &config.notarization_receipt_session_dir,
        config
            .confirmed_decision_packet_session_dir
            .as_deref()
            .ok_or_else(|| anyhow!("missing --confirm-decision-packet-session-dir for verify mode"))?,
    ) {
        Ok(value) => value,
        Err(error) => {
            let raw_contract =
                package_notarization_receipt_registry_entry::load_live_package_notarization_receipt_contract_for_registry_entry(
                    &config.notarization_receipt_session_dir,
                )?;
            return Ok(failure_report_for_verify(
                config,
                session_dir,
                &raw_contract,
                vec![format!(
                    "failed to verify notarization-receipt session under {}: {error}",
                    config.notarization_receipt_session_dir.display()
                )],
            ));
        }
    };

    let stored_notarization_receipt_report: serde_json::Value = load_required_step_json(
        &status.notarization_receipt_step,
        &paths.notarization_receipt_report_path,
        "notarization_receipt_step",
        &mut mismatches,
    )?;
    let stored_notarization_receipt_report_view: NestedNotarizationReceiptReportView =
        serde_json::from_value(stored_notarization_receipt_report.clone()).with_context(|| {
            format!(
                "failed parsing archived nested notarization-receipt report {}",
                paths.notarization_receipt_report_path.display()
            )
        })?;
    let verified_notarization_receipt_report_view: NestedNotarizationReceiptReportView =
        serde_json::from_value(verified_notarization_receipt.report_json.clone()).with_context(
            || {
                format!(
                    "failed parsing freshly verified nested notarization-receipt report for {}",
                    config.notarization_receipt_session_dir.display()
                )
            },
        )?;
    compare_json_ignoring_generated_at(
        &stored_notarization_receipt_report,
        &verified_notarization_receipt.report_json,
        "registry-entry archived notarization-receipt report json",
        &mut mismatches,
    );
    compare_string(
        &stored_notarization_receipt_report_view.mode,
        &verified_notarization_receipt_report_view.mode,
        "registry-entry archived notarization-receipt report mode",
        &mut mismatches,
    );
    compare_string(
        &stored_notarization_receipt_report_view.verdict,
        &verified_notarization_receipt_report_view.verdict,
        "registry-entry archived notarization-receipt report verdict",
        &mut mismatches,
    );
    compare_string(
        &stored_notarization_receipt_report_view.reason,
        &verified_notarization_receipt_report_view.reason,
        "registry-entry archived notarization-receipt report reason",
        &mut mismatches,
    );
    if stored_notarization_receipt_report_view.generated_at
        != verified_notarization_receipt_report_view.generated_at
    {
        mismatches.push(format!(
            "registry-entry archived notarization-receipt report generated_at {:?} does not match freshly verified nested truth {:?}",
            stored_notarization_receipt_report_view.generated_at,
            verified_notarization_receipt_report_view.generated_at
        ));
    }
    let expected_notarization_receipt_step = step_artifact(
        &paths.notarization_receipt_report_path,
        &verified_notarization_receipt_report_view.mode,
        &verified_notarization_receipt_report_view.verdict,
        &verified_notarization_receipt_report_view.reason,
        verified_notarization_receipt_report_view.generated_at,
    );
    compare_step_artifact(
        status.notarization_receipt_step.as_ref(),
        &expected_notarization_receipt_step,
        "registry-entry notarization_receipt_step",
        &mut mismatches,
    );
    if verified_notarization_receipt.verdict != "tiny_live_package_notarization_receipt_verify_ok" {
        mismatches.push(format!(
            "nested notarization-receipt verification is non-green: {}",
            verified_notarization_receipt.reason
        ));
    }

    let classification = classify_verified_notarization_receipt(&verified_notarization_receipt);
    let expected_registry_entry_sha256 = compute_registry_entry_sha256(
        &serialize_enum(&classification.0),
        verified_notarization_receipt
            .contract
            .handoff_bundle_result
            .as_deref(),
        verified_notarization_receipt
            .contract
            .decision_packet_result
            .as_deref(),
        verified_notarization_receipt
            .contract
            .execute_frozen_result
            .as_deref(),
        verified_notarization_receipt
            .contract
            .current_pre_activation_gate_verdict
            .as_deref(),
        verified_notarization_receipt
            .contract
            .current_pre_activation_gate_reason
            .as_deref(),
        &verified_notarization_receipt
            .contract
            .reviewed_frozen_live_cutover_controller_command_summary,
        &verified_notarization_receipt
            .contract
            .chain_fingerprint_sha256,
        &verified_notarization_receipt
            .contract
            .chain_fingerprint_algorithm,
        &verified_notarization_receipt
            .contract
            .release_capsule_digest_manifest_sha256,
        verified_notarization_receipt
            .contract
            .release_capsule_digest_manifest_entry_count,
        &verified_notarization_receipt
            .contract
            .release_capsule_digest_algorithm,
        &verified_notarization_receipt.contract.ledger_seal_sha256,
        &verified_notarization_receipt.contract.ledger_seal_algorithm,
    )?;

    let expected_session = expected_session_from_verified(
        config,
        session_dir,
        session.planned_at,
        &verified_notarization_receipt.contract,
        classification.0,
        &expected_registry_entry_sha256,
    );
    if session != expected_session {
        mismatches.push(
            "registry-entry session contract does not match freshly verified notarization-receipt truth"
                .to_string(),
        );
    }
    let expected_status = expected_status_from_verified(
        config,
        session_dir,
        status.updated_at,
        &verified_notarization_receipt.contract,
        classification,
        expected_notarization_receipt_step.clone(),
        &expected_registry_entry_sha256,
    );
    if status != expected_status {
        mismatches.push(
            "registry-entry status contract does not match freshly verified notarization-receipt truth"
                .to_string(),
        );
    }

    if !mismatches.is_empty() {
        return Ok(failure_report_for_verify(
            config,
            session_dir,
            &verified_notarization_receipt.contract,
            mismatches,
        ));
    }

    Ok(report_from_status(
        config,
        session_dir,
        &paths,
        &verified_notarization_receipt.contract,
        TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryVerifyOk,
        &expected_status,
    ))
}

fn verify_notarization_receipt_command_summary(notarization_receipt_session_dir: &Path) -> String {
    format!(
        "verify immutable notarization-receipt session under {} before freezing the final registry entry",
        notarization_receipt_session_dir.display()
    )
}

fn classify_registry_contract(
    contract: &package_notarization_receipt_registry_entry::LivePackageNotarizationReceiptContractView,
    assume_verified: bool,
) -> (
    LivePackageRegistryEntryResult,
    TinyLivePackageRegistryEntryVerdict,
    String,
) {
    if !assume_verified {
        return (
            LivePackageRegistryEntryResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryRefusedNowByInvalidOrDriftedContract,
            "the notarization-receipt chain is not verified, so this registry entry remains refused"
                .to_string(),
        );
    }
    match contract.result.as_deref() {
        Some("refused_now_by_stage3") => (
            LivePackageRegistryEntryResult::RefusedNowByStage3,
            TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryRefusedNowByStage3,
            "the immutable notarization receipt remains refused now by Stage 3, so this registry entry remains an explicit refusal record"
                .to_string(),
        ),
        Some("refused_now_by_pre_activation_gate") => (
            LivePackageRegistryEntryResult::RefusedNowByPreActivationGate,
            TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryRefusedNowByPreActivationGate,
            "the immutable notarization receipt remains refused now by the pre-activation gate, so this registry entry remains an explicit refusal record"
                .to_string(),
        ),
        Some("ready_for_manual_execution_when_gate_turns_green") => (
            LivePackageRegistryEntryResult::ReadyForManualExecutionWhenGateTurnsGreen,
            TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryReadyForManualExecutionWhenGateTurnsGreen,
            "the immutable notarization receipt is coherent and this registry entry is ready for manual execution when gate truth turns green"
                .to_string(),
        ),
        _ => (
            LivePackageRegistryEntryResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryRefusedNowByInvalidOrDriftedContract,
            "the verified notarization-receipt chain is invalid, drifted, or non-runnable, so this registry entry remains refused"
                .to_string(),
        ),
    }
}

fn classify_verified_notarization_receipt(
    verified: &package_notarization_receipt_registry_entry::VerifiedLivePackageNotarizationReceiptRegistryEntryStep,
) -> (
    LivePackageRegistryEntryResult,
    TinyLivePackageRegistryEntryVerdict,
    String,
) {
    if verified.verdict != "tiny_live_package_notarization_receipt_verify_ok" {
        return (
            LivePackageRegistryEntryResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryRefusedNowByInvalidOrDriftedContract,
            "the immutable notarization receipt did not verify cleanly, so this registry entry remains refused"
                .to_string(),
        );
    }
    classify_registry_contract(&verified.contract, true)
}

fn registry_entry_summary(
    result: LivePackageRegistryEntryResult,
    reviewed_frozen_live_cutover_controller_command_summary: &str,
) -> String {
    match result {
        LivePackageRegistryEntryResult::RefusedNowByStage3 => {
            "Registry entry outcome: the immutable sealed chain remains refused now by Stage 3; archive this final registry record as refusal evidence only and do not run the frozen controller.".to_string()
        }
        LivePackageRegistryEntryResult::RefusedNowByPreActivationGate => {
            "Registry entry outcome: the immutable sealed chain remains refused now by the pre-activation gate; archive this final registry record as refusal evidence only and do not run the frozen controller.".to_string()
        }
        LivePackageRegistryEntryResult::RefusedNowByInvalidOrDriftedContract => {
            "Registry entry outcome: the immutable sealed chain is invalid or drifted; archive this final registry record for audit only and mint a new coherent entry before any later execution record.".to_string()
        }
        LivePackageRegistryEntryResult::ReadyForManualExecutionWhenGateTurnsGreen => format!(
            "Registry entry outcome: this immutable registry-style record is ready for manual execution when gate truth turns green; the exact reviewed frozen controller command remains: {reviewed_frozen_live_cutover_controller_command_summary}"
        ),
    }
}

fn registry_entry_paths(session_dir: &Path) -> PackageRegistryEntryPaths {
    PackageRegistryEntryPaths {
        session_path: session_dir.join("tiny_live_activation_package_registry_entry.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_registry_entry.status.json"),
        notarization_receipt_report_path: session_dir
            .join("tiny_live_activation_package_registry_entry.notarization_receipt.report.json"),
    }
}

fn ensure_clean_registry_entry_session_dir(session_dir: &Path) -> Result<()> {
    if session_dir.exists() {
        bail!(
            "refusing to overwrite existing registry-entry session dir {}",
            session_dir.display()
        );
    }
    fs::create_dir_all(session_dir).with_context(|| {
        format!(
            "failed creating registry-entry session dir {}",
            session_dir.display()
        )
    })
}

fn planned_session(
    config: &Config,
    session_dir: &Path,
    contract: &package_notarization_receipt_registry_entry::LivePackageNotarizationReceiptContractView,
    result: LivePackageRegistryEntryResult,
    registry_entry_sha256: &str,
) -> PackageRegistryEntrySession {
    PackageRegistryEntrySession {
        session_version: REGISTRY_ENTRY_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        notarization_receipt_session_dir: config
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
        verify_notarization_receipt_command_summary: verify_notarization_receipt_command_summary(
            &config.notarization_receipt_session_dir,
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
        registry_entry_summary: registry_entry_summary(
            result,
            &contract.reviewed_frozen_live_cutover_controller_command_summary,
        ),
        registry_entry_sha256: registry_entry_sha256.to_string(),
        registry_entry_algorithm: REGISTRY_ENTRY_ALGORITHM.to_string(),
        explicit_statement: REGISTRY_ENTRY_SESSION_EXPLICIT_STATEMENT.to_string(),
    }
}

fn expected_session_from_verified(
    config: &Config,
    session_dir: &Path,
    planned_at: DateTime<Utc>,
    contract: &package_notarization_receipt_registry_entry::LivePackageNotarizationReceiptContractView,
    result: LivePackageRegistryEntryResult,
    registry_entry_sha256: &str,
) -> PackageRegistryEntrySession {
    PackageRegistryEntrySession {
        planned_at,
        ..planned_session(config, session_dir, contract, result, registry_entry_sha256)
    }
}

fn expected_status_from_verified(
    config: &Config,
    session_dir: &Path,
    updated_at: DateTime<Utc>,
    contract: &package_notarization_receipt_registry_entry::LivePackageNotarizationReceiptContractView,
    classification: (
        LivePackageRegistryEntryResult,
        TinyLivePackageRegistryEntryVerdict,
        String,
    ),
    notarization_receipt_step: PackageRegistryEntryStepArtifact,
    registry_entry_sha256: &str,
) -> PackageRegistryEntryStatus {
    PackageRegistryEntryStatus {
        status_version: REGISTRY_ENTRY_STATUS_VERSION.to_string(),
        updated_at,
        session_dir: session_dir.display().to_string(),
        notarization_receipt_session_dir: config
            .notarization_receipt_session_dir
            .display()
            .to_string(),
        result: classification.0,
        reason: classification.2,
        handoff_bundle_result: contract.handoff_bundle_result.clone(),
        decision_packet_result: contract.decision_packet_result.clone(),
        execute_frozen_result: contract.execute_frozen_result.clone(),
        current_pre_activation_gate_verdict: contract.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: contract.current_pre_activation_gate_reason.clone(),
        notarization_receipt_step: Some(notarization_receipt_step),
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
        registry_entry_summary: registry_entry_summary(
            classification.0,
            &contract.reviewed_frozen_live_cutover_controller_command_summary,
        ),
        registry_entry_sha256: registry_entry_sha256.to_string(),
        registry_entry_algorithm: REGISTRY_ENTRY_ALGORITHM.to_string(),
        explicit_statement: REGISTRY_ENTRY_STATUS_EXPLICIT_STATEMENT.to_string(),
    }
}

fn report_from_status(
    config: &Config,
    session_dir: &Path,
    paths: &PackageRegistryEntryPaths,
    contract: &package_notarization_receipt_registry_entry::LivePackageNotarizationReceiptContractView,
    verdict: TinyLivePackageRegistryEntryVerdict,
    status: &PackageRegistryEntryStatus,
) -> PackageRegistryEntryReport {
    PackageRegistryEntryReport {
        generated_at: Utc::now(),
        mode: if verdict
            == TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryVerifyOk
        {
            "verify_live_package_registry_entry".to_string()
        } else {
            "run_live_package_registry_entry".to_string()
        },
        verdict,
        reason: status.reason.clone(),
        notarization_receipt_session_dir: config
            .notarization_receipt_session_dir
            .display()
            .to_string(),
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
        registry_entry_session_path: Some(paths.session_path.display().to_string()),
        registry_entry_status_path: Some(paths.status_path.display().to_string()),
        archived_notarization_receipt_report_path: Some(
            paths.notarization_receipt_report_path.display().to_string(),
        ),
        result: Some(serialize_enum(&status.result)),
        handoff_bundle_result: status.handoff_bundle_result.clone(),
        decision_packet_result: status.decision_packet_result.clone(),
        execute_frozen_result: status.execute_frozen_result.clone(),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        notarization_receipt_step: status.notarization_receipt_step.clone(),
        verify_notarization_receipt_command_summary: Some(
            verify_notarization_receipt_command_summary(&config.notarization_receipt_session_dir),
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
        verification_mismatches: Vec::new(),
        explicit_statement: if verdict
            == TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryVerifyOk
        {
            REGISTRY_ENTRY_VERIFY_EXPLICIT_STATEMENT.to_string()
        } else {
            status.explicit_statement.clone()
        },
    }
}

fn failure_report_without_contract(
    config: &Config,
    verdict: TinyLivePackageRegistryEntryVerdict,
    reason: String,
) -> PackageRegistryEntryReport {
    PackageRegistryEntryReport {
        generated_at: Utc::now(),
        mode: match config.mode {
            Mode::PlanLivePackageRegistryEntry => "plan_live_package_registry_entry".to_string(),
            Mode::RenderLivePackageRegistryEntry => {
                "render_live_package_registry_entry".to_string()
            }
            Mode::RunLivePackageRegistryEntry => "run_live_package_registry_entry".to_string(),
            Mode::VerifyLivePackageRegistryEntry => {
                "verify_live_package_registry_entry".to_string()
            }
        },
        verdict,
        reason,
        notarization_receipt_session_dir: config
            .notarization_receipt_session_dir
            .display()
            .to_string(),
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
        registry_entry_session_path: None,
        registry_entry_status_path: None,
        archived_notarization_receipt_report_path: None,
        result: Some("refused_now_by_invalid_or_drifted_contract".to_string()),
        handoff_bundle_result: None,
        decision_packet_result: None,
        execute_frozen_result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        notarization_receipt_step: None,
        verify_notarization_receipt_command_summary: Some(
            verify_notarization_receipt_command_summary(&config.notarization_receipt_session_dir),
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
        registry_entry_summary: None,
        registry_entry_sha256: None,
        registry_entry_algorithm: Some(REGISTRY_ENTRY_ALGORITHM.to_string()),
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this immutable registry entry refused before persisting because the notarization-receipt artifact could not be verified coherently"
                .to_string(),
    }
}

fn failure_report_for_verify(
    config: &Config,
    session_dir: &Path,
    contract: &package_notarization_receipt_registry_entry::LivePackageNotarizationReceiptContractView,
    mismatches: Vec<String>,
) -> PackageRegistryEntryReport {
    let paths = registry_entry_paths(session_dir);
    let reason = mismatches.first().cloned().unwrap_or_else(|| {
        "notarization-receipt-native registry entry verification failed".to_string()
    });
    PackageRegistryEntryReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_registry_entry".to_string(),
        verdict: TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryVerifyInvalid,
        reason,
        notarization_receipt_session_dir: config
            .notarization_receipt_session_dir
            .display()
            .to_string(),
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
        registry_entry_session_path: Some(paths.session_path.display().to_string()),
        registry_entry_status_path: Some(paths.status_path.display().to_string()),
        archived_notarization_receipt_report_path: Some(
            paths.notarization_receipt_report_path.display().to_string(),
        ),
        result: None,
        handoff_bundle_result: contract.handoff_bundle_result.clone(),
        decision_packet_result: contract.decision_packet_result.clone(),
        execute_frozen_result: contract.execute_frozen_result.clone(),
        current_pre_activation_gate_verdict: contract.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: contract.current_pre_activation_gate_reason.clone(),
        notarization_receipt_step: None,
        verify_notarization_receipt_command_summary: Some(
            verify_notarization_receipt_command_summary(&config.notarization_receipt_session_dir),
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
        registry_entry_summary: None,
        registry_entry_sha256: None,
        registry_entry_algorithm: Some(REGISTRY_ENTRY_ALGORITHM.to_string()),
        verification_mismatches: mismatches,
        explicit_statement: REGISTRY_ENTRY_VERIFY_EXPLICIT_STATEMENT.to_string(),
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
    step: &Option<PackageRegistryEntryStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<serde_json::Value> {
    let Some(step) = step else {
        mismatches.push(format!("{label} is missing from registry-entry status"));
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
) -> PackageRegistryEntryStepArtifact {
    PackageRegistryEntryStepArtifact {
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
    actual: Option<&PackageRegistryEntryStepArtifact>,
    expected: &PackageRegistryEntryStepArtifact,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    let Some(actual) = actual else {
        mismatches.push(format!("{label} is missing from registry-entry status"));
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

fn compute_registry_entry_sha256(
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
) -> Result<String> {
    canonical_sha256_hex(&RegistryEntryPayload {
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
    fs::write(path, contents)
        .with_context(|| format!("failed writing registry-entry script to {}", path.display()))?;
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

fn render_report_lines(report: &PackageRegistryEntryReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "notarization_receipt_session_dir={}",
            report.notarization_receipt_session_dir
        ),
    ];
    if let Some(value) = &report.result {
        lines.push(format!("result={value}"));
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
    use serde_json::json;
    use std::ffi::OsString;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn run_ready_for_manual_execution_when_gate_turns_green_then_verify_stays_green() {
        let fixture = fake_command_fixture(
            "registry_entry_ready",
            notarization_receipt_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_registry_entry_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryReadyForManualExecutionWhenGateTurnsGreen
        );

        let verify = verify_live_package_registry_entry_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryVerifyOk
        );
    }

    #[test]
    fn stage3_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "registry_entry_stage3_refusal",
            notarization_receipt_verify_report(
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "red",
                "stage3 blocked",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_registry_entry_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryRefusedNowByStage3
        );
    }

    #[test]
    fn pre_activation_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "registry_entry_gate_refusal",
            notarization_receipt_verify_report(
                "refused_now_by_pre_activation_gate",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "refused_now_by_pre_activation_gate",
                "red",
                "pre-activation blocked",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_registry_entry_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryRefusedNowByPreActivationGate
        );
    }

    #[test]
    fn drifted_notarization_receipt_contract_is_refused() {
        let fixture = fake_command_fixture(
            "registry_entry_drifted_notarization",
            json!({
                "generated_at": Utc::now(),
                "mode": "verify_live_package_notarization_receipt",
                "verdict": "tiny_live_package_notarization_receipt_verify_invalid",
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

        let run = run_live_package_registry_entry_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryRefusedNowByInvalidOrDriftedContract
        );
    }

    #[test]
    fn run_refuses_managed_surface_overlap_before_writing_any_artifacts() {
        let fixture = fake_command_fixture(
            "registry_entry_managed_overlap",
            notarization_receipt_verify_report(
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
        let overlap_session_dir = managed_paths.runtime_dir.join("registry-entry-session");
        fs::create_dir_all(overlap_session_dir.parent().unwrap()).unwrap();

        let config = Config {
            session_dir: Some(overlap_session_dir.clone()),
            ..fixture.run_config()
        };
        let error = run_live_package_registry_entry_report(&config).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("overlaps the managed live target surface"),
            "{error:#}"
        );
        let paths = registry_entry_paths(&overlap_session_dir);
        assert!(!paths.session_path.exists());
        assert!(!paths.status_path.exists());
    }

    #[test]
    fn verify_rejects_tampered_notarization_receipt_step_path() {
        let fixture = fake_command_fixture(
            "registry_entry_tampered_step_path",
            notarization_receipt_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_live_package_registry_entry_report(&fixture.run_config()).unwrap();
        let paths = registry_entry_paths(&fixture.registry_entry_session_dir);
        let mut status: PackageRegistryEntryStatus = load_json(&paths.status_path).unwrap();
        status.notarization_receipt_step.as_mut().unwrap().path =
            "/tmp/foreign.notarization.report.json".to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_registry_entry_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_registry_entry_text() {
        let fixture = fake_command_fixture(
            "registry_entry_tampered_text",
            notarization_receipt_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_live_package_registry_entry_report(&fixture.run_config()).unwrap();
        let paths = registry_entry_paths(&fixture.registry_entry_session_dir);
        let mut session: PackageRegistryEntrySession = load_json(&paths.session_path).unwrap();
        session.registry_entry_summary = "tampered registry summary".to_string();
        persist_json(&paths.session_path, &session).unwrap();

        let verify = verify_live_package_registry_entry_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_status_result() {
        let fixture = fake_command_fixture(
            "registry_entry_tampered_result",
            notarization_receipt_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_live_package_registry_entry_report(&fixture.run_config()).unwrap();
        let paths = registry_entry_paths(&fixture.registry_entry_session_dir);
        let mut status: PackageRegistryEntryStatus = load_json(&paths.status_path).unwrap();
        status.result = LivePackageRegistryEntryResult::RefusedNowByStage3;
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_registry_entry_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_status_gate_fields() {
        let fixture = fake_command_fixture(
            "registry_entry_tampered_gate_fields",
            notarization_receipt_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_live_package_registry_entry_report(&fixture.run_config()).unwrap();
        let paths = registry_entry_paths(&fixture.registry_entry_session_dir);
        let mut status: PackageRegistryEntryStatus = load_json(&paths.status_path).unwrap();
        status.current_pre_activation_gate_verdict = Some("red".to_string());
        status.current_pre_activation_gate_reason = Some("tampered".to_string());
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_registry_entry_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_nested_notarization_receipt_report_content() {
        let fixture = fake_command_fixture(
            "registry_entry_tampered_nested_report",
            notarization_receipt_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_live_package_registry_entry_report(&fixture.run_config()).unwrap();
        let paths = registry_entry_paths(&fixture.registry_entry_session_dir);
        let mut report: serde_json::Value =
            load_json(&paths.notarization_receipt_report_path).unwrap();
        report.as_object_mut().unwrap().insert(
            "reason".to_string(),
            json!("tampered nested notarization-receipt reason"),
        );
        persist_json(&paths.notarization_receipt_report_path, &report).unwrap();

        let verify = verify_live_package_registry_entry_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_retimed_nested_notarization_receipt_report_and_step_generated_at() {
        let fixture = fake_command_fixture(
            "registry_entry_retimed_nested_report",
            notarization_receipt_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_live_package_registry_entry_report(&fixture.run_config()).unwrap();
        let paths = registry_entry_paths(&fixture.registry_entry_session_dir);
        let mut report: serde_json::Value =
            load_json(&paths.notarization_receipt_report_path).unwrap();
        let archived_generated_at =
            DateTime::parse_from_rfc3339(report["generated_at"].as_str().unwrap())
                .unwrap()
                .with_timezone(&Utc);
        let forged_generated_at = archived_generated_at + chrono::Duration::seconds(61);
        report.as_object_mut().unwrap().insert(
            "generated_at".to_string(),
            json!(forged_generated_at.to_rfc3339()),
        );
        persist_json(&paths.notarization_receipt_report_path, &report).unwrap();

        let mut status: PackageRegistryEntryStatus = load_json(&paths.status_path).unwrap();
        status
            .notarization_receipt_step
            .as_mut()
            .unwrap()
            .generated_at = forged_generated_at;
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_registry_entry_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_chain_ledger_or_registry_identity_fields() {
        {
            let chain_fixture = fake_command_fixture(
                "registry_entry_tampered_chain_identity",
                notarization_receipt_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&chain_fixture.bin_dir);
            run_live_package_registry_entry_report(&chain_fixture.run_config()).unwrap();
            let paths = registry_entry_paths(&chain_fixture.registry_entry_session_dir);
            let mut status: PackageRegistryEntryStatus = load_json(&paths.status_path).unwrap();
            status.chain_fingerprint_sha256 = "tampered-chain".to_string();
            persist_json(&paths.status_path, &status).unwrap();

            let verify =
                verify_live_package_registry_entry_report(&chain_fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryVerifyInvalid
            );
        }

        {
            let ledger_fixture = fake_command_fixture(
                "registry_entry_tampered_ledger_identity",
                notarization_receipt_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&ledger_fixture.bin_dir);
            run_live_package_registry_entry_report(&ledger_fixture.run_config()).unwrap();
            let paths = registry_entry_paths(&ledger_fixture.registry_entry_session_dir);
            let mut status: PackageRegistryEntryStatus = load_json(&paths.status_path).unwrap();
            status.ledger_seal_sha256 = "tampered-ledger".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_registry_entry_report(&ledger_fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryVerifyInvalid
            );
        }

        {
            let registry_fixture = fake_command_fixture(
                "registry_entry_tampered_registry_identity",
                notarization_receipt_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&registry_fixture.bin_dir);
            run_live_package_registry_entry_report(&registry_fixture.run_config()).unwrap();
            let paths = registry_entry_paths(&registry_fixture.registry_entry_session_dir);
            let mut status: PackageRegistryEntryStatus = load_json(&paths.status_path).unwrap();
            status.registry_entry_sha256 = "tampered-registry".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_registry_entry_report(&registry_fixture.verify_config())
                    .unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageRegistryEntryVerdict::TinyLivePackageRegistryEntryVerifyInvalid
            );
        }
    }

    struct FakeCommandFixture {
        bin_dir: PathBuf,
        notarization_receipt_session_dir: PathBuf,
        registry_entry_session_dir: PathBuf,
        decision_packet_session_dir: PathBuf,
        install_root: PathBuf,
    }

    impl FakeCommandFixture {
        fn run_config(&self) -> Config {
            Config {
                mode: Mode::RunLivePackageRegistryEntry,
                notarization_receipt_session_dir: self.notarization_receipt_session_dir.clone(),
                confirmed_decision_packet_session_dir: Some(
                    self.decision_packet_session_dir.clone(),
                ),
                session_dir: Some(self.registry_entry_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn verify_config(&self) -> Config {
            Config {
                mode: Mode::VerifyLivePackageRegistryEntry,
                notarization_receipt_session_dir: self.notarization_receipt_session_dir.clone(),
                confirmed_decision_packet_session_dir: Some(
                    self.decision_packet_session_dir.clone(),
                ),
                session_dir: Some(self.registry_entry_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }
    }

    fn fake_command_fixture(label: &str, verify_report: serde_json::Value) -> FakeCommandFixture {
        let fixture_dir = temp_dir(label);
        let bin_dir = fixture_dir.join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
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
            &notarization_receipt_session_dir
                .join("tiny_live_activation_package_notarization_receipt.session.json"),
            &json!({
                "provenance_certificate_session_dir": provenance_certificate_session_dir.display().to_string(),
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
                "verify_provenance_certificate_command_summary": format!(
                    "verify immutable provenance-certificate session under {} before freezing the final notarization receipt",
                    provenance_certificate_session_dir.display()
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
                "chain_fingerprint_sha256": "canonical-chain-sha256",
                "chain_fingerprint_algorithm": "sha256",
                "release_capsule_digest_manifest_sha256": "release-capsule-manifest-sha256",
                "release_capsule_digest_manifest_entry_count": 3,
                "release_capsule_digest_algorithm": "sha256",
                "notarization_receipt_summary": "Notarization receipt summary.",
                "ledger_seal_summary": "Ledger seal summary.",
                "ledger_seal_sha256": "ledger-seal-sha256",
                "ledger_seal_algorithm": "sha256",
                "explicit_statement": "notarization-receipt statement"
            }),
        )
        .unwrap();
        persist_json(
            &notarization_receipt_session_dir
                .join("tiny_live_activation_package_notarization_receipt.status.json"),
            &json!({
                "result": verify_report.get("result").and_then(|value| value.as_str()).unwrap(),
                "handoff_bundle_result": verify_report.get("handoff_bundle_result").and_then(|value| value.as_str()),
                "decision_packet_result": verify_report.get("decision_packet_result").and_then(|value| value.as_str()),
                "execute_frozen_result": verify_report.get("execute_frozen_result").and_then(|value| value.as_str()),
                "current_pre_activation_gate_verdict": verify_report.get("current_pre_activation_gate_verdict").and_then(|value| value.as_str()),
                "current_pre_activation_gate_reason": verify_report.get("current_pre_activation_gate_reason").and_then(|value| value.as_str()),
                "provenance_certificate_summary": "Provenance certificate summary.",
                "chain_fingerprint_summary": "Canonical chain fingerprint summary.",
                "chain_fingerprint_sha256": "canonical-chain-sha256",
                "chain_fingerprint_algorithm": "sha256",
                "release_capsule_digest_manifest_sha256": "release-capsule-manifest-sha256",
                "release_capsule_digest_manifest_entry_count": 3,
                "release_capsule_digest_algorithm": "sha256",
                "notarization_receipt_summary": "Notarization receipt summary.",
                "ledger_seal_summary": "Ledger seal summary.",
                "ledger_seal_sha256": "ledger-seal-sha256",
                "ledger_seal_algorithm": "sha256",
                "explicit_statement": "notarization-receipt status statement"
            }),
        )
        .unwrap();
        persist_json(
            &notarization_receipt_session_dir.join(
                "tiny_live_activation_package_notarization_receipt.provenance_certificate.report.json",
            ),
            &json!({
                "decision_packet_session_dir": decision_packet_session_dir.display().to_string()
            }),
        )
        .unwrap();

        let mut verify_report = verify_report;
        bind_notarization_receipt_report_to_fixture(
            &mut verify_report,
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
        let verify_path = fixture_dir.join("notarization-receipt.verify.json");
        persist_json(&verify_path, &verify_report).unwrap();
        write_fake_notarization_receipt_verify_command(
            &bin_dir.join("copybot_tiny_live_activation_package_notarization_receipt"),
            &verify_path,
            &provenance_certificate_session_dir,
            &decision_packet_session_dir,
            &notarization_receipt_session_dir,
        );

        FakeCommandFixture {
            bin_dir,
            notarization_receipt_session_dir,
            registry_entry_session_dir,
            decision_packet_session_dir,
            install_root,
        }
    }

    fn bind_notarization_receipt_report_to_fixture(
        verify_report: &mut serde_json::Value,
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
            "verify_provenance_certificate_command_summary".to_string(),
            json!(format!(
                "verify immutable provenance-certificate session under {} before freezing the final notarization receipt",
                provenance_certificate_session_dir.display()
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
            "explicit_statement".to_string(),
            json!("notarization-receipt verify statement"),
        );
    }

    fn notarization_receipt_verify_report(
        result: &str,
        handoff_bundle_result: &str,
        decision_packet_result: &str,
        execute_frozen_result: &str,
        gate_verdict: &str,
        gate_reason: &str,
    ) -> serde_json::Value {
        json!({
            "generated_at": Utc::now(),
            "mode": "verify_live_package_notarization_receipt",
            "verdict": "tiny_live_package_notarization_receipt_verify_ok",
            "reason": "notarization-receipt chain is coherent",
            "result": result,
            "handoff_bundle_result": handoff_bundle_result,
            "decision_packet_result": decision_packet_result,
            "execute_frozen_result": execute_frozen_result,
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason
        })
    }

    fn write_fake_notarization_receipt_verify_command(
        path: &Path,
        verify_path: &Path,
        expected_provenance_certificate_session_dir: &Path,
        expected_decision_packet_session_dir: &Path,
        notarization_receipt_session_dir: &Path,
    ) {
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --provenance-certificate-session-dir {} \"*) : ;;\n  *) echo 'unexpected provenance-certificate-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --confirm-decision-packet-session-dir {} \"*) : ;;\n  *) echo 'unexpected decision-packet-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected notarization-receipt session dir' >&2; exit 1;;\nesac\ncat '{}'\n",
            shell_escape_path(expected_provenance_certificate_session_dir),
            shell_escape_path(expected_decision_packet_session_dir),
            shell_escape_path(notarization_receipt_session_dir),
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
}
