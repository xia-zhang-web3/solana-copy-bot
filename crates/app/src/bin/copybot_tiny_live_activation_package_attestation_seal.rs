use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_app::tiny_live_activation::install_target_managed_surface::validate_turn_green_session_dir as validate_managed_surface_session_dir;
use copybot_app::tiny_live_activation::package_release_capsule_attestation_seal as release_capsule_attestation_seal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: copybot_tiny_live_activation_package_attestation_seal --release-capsule-session-dir <path> [--confirm-decision-packet-session-dir <path>] [--session-dir <path>] [--output <path>] [--json] (--plan-live-package-attestation-seal | --render-live-package-attestation-seal | --run-live-package-attestation-seal | --verify-live-package-attestation-seal)";
const ATTESTATION_SEAL_SESSION_VERSION: &str = "1";
const ATTESTATION_SEAL_STATUS_VERSION: &str = "1";
const ATTESTATION_SEAL_SESSION_EXPLICIT_STATEMENT: &str =
    "the verified release-capsule session is the primary input here; this immutable attestation seal freezes one final custody record over the exact reviewed frozen live cutover contract without enabling production execution";
const ATTESTATION_SEAL_STATUS_EXPLICIT_STATEMENT: &str =
    "this attestation seal is archival and read-only; it never executes or enables the frozen controller";
const ATTESTATION_SEAL_VERIFY_EXPLICIT_STATEMENT: &str =
    "this verification binds one immutable attestation seal to verified release-capsule truth and the exact nested digest-manifest identity";

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
    release_capsule_session_dir: PathBuf,
    confirmed_decision_packet_session_dir: Option<PathBuf>,
    session_dir: Option<PathBuf>,
    output_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanLivePackageAttestationSeal,
    RenderLivePackageAttestationSeal,
    RunLivePackageAttestationSeal,
    VerifyLivePackageAttestationSeal,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageAttestationSealVerdict {
    TinyLivePackageAttestationSealPlanReady,
    TinyLivePackageAttestationSealRendered,
    TinyLivePackageAttestationSealRefusedNowByStage3,
    TinyLivePackageAttestationSealRefusedNowByPreActivationGate,
    TinyLivePackageAttestationSealRefusedNowByInvalidOrDriftedContract,
    TinyLivePackageAttestationSealReadyForManualExecutionWhenGateTurnsGreen,
    TinyLivePackageAttestationSealVerifyOk,
    TinyLivePackageAttestationSealVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageAttestationSealResult {
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    RefusedNowByInvalidOrDriftedContract,
    ReadyForManualExecutionWhenGateTurnsGreen,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageAttestationSealStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageAttestationSealSession {
    session_version: String,
    planned_at: DateTime<Utc>,
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
    verify_release_capsule_command_summary: String,
    reviewed_frozen_live_cutover_controller_command_summary: String,
    release_capsule_summary: String,
    release_capsule_audit_manifest_summary: String,
    release_capsule_digest_manifest_sha256: String,
    release_capsule_digest_manifest_entry_count: usize,
    release_capsule_digest_algorithm: String,
    attestation_seal_summary: String,
    custody_record_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageAttestationSealStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    release_capsule_session_dir: String,
    result: LivePackageAttestationSealResult,
    reason: String,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    release_capsule_step: Option<PackageAttestationSealStepArtifact>,
    release_capsule_summary: String,
    release_capsule_audit_manifest_summary: String,
    release_capsule_digest_manifest_sha256: String,
    release_capsule_digest_manifest_entry_count: usize,
    release_capsule_digest_algorithm: String,
    attestation_seal_summary: String,
    custody_record_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageAttestationSealPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    release_capsule_report_path: PathBuf,
    release_capsule_digest_manifest_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageAttestationSealReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageAttestationSealVerdict,
    reason: String,
    release_capsule_session_dir: String,
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
    attestation_seal_session_path: Option<String>,
    attestation_seal_status_path: Option<String>,
    archived_release_capsule_report_path: Option<String>,
    archived_release_capsule_digest_manifest_path: Option<String>,
    result: Option<String>,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    release_capsule_step: Option<PackageAttestationSealStepArtifact>,
    verify_release_capsule_command_summary: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    release_capsule_summary: Option<String>,
    release_capsule_audit_manifest_summary: Option<String>,
    release_capsule_digest_manifest_sha256: Option<String>,
    release_capsule_digest_manifest_entry_count: Option<usize>,
    release_capsule_digest_algorithm: Option<String>,
    attestation_seal_summary: Option<String>,
    custody_record_summary: Option<String>,
    release_capsule_digest_manifest:
        Option<release_capsule_attestation_seal::ReleaseCapsuleDigestManifest>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    verification_mismatches: Vec<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct NestedReleaseCapsuleReportView {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: String,
    reason: String,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut release_capsule_session_dir: Option<PathBuf> = None;
    let mut confirmed_decision_packet_session_dir: Option<PathBuf> = None;
    let mut session_dir: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut json = false;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--release-capsule-session-dir" => {
                release_capsule_session_dir = Some(PathBuf::from(parse_string_arg(
                    "--release-capsule-session-dir",
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
            "--plan-live-package-attestation-seal" => set_mode(
                &mut mode,
                Mode::PlanLivePackageAttestationSeal,
                arg.as_str(),
            )?,
            "--render-live-package-attestation-seal" => set_mode(
                &mut mode,
                Mode::RenderLivePackageAttestationSeal,
                arg.as_str(),
            )?,
            "--run-live-package-attestation-seal" => {
                set_mode(&mut mode, Mode::RunLivePackageAttestationSeal, arg.as_str())?
            }
            "--verify-live-package-attestation-seal" => set_mode(
                &mut mode,
                Mode::VerifyLivePackageAttestationSeal,
                arg.as_str(),
            )?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let Some(mode) = mode else {
        return Ok(None);
    };
    if matches!(mode, Mode::RenderLivePackageAttestationSeal) && output_path.is_none() {
        bail!("missing --output for --render-live-package-attestation-seal");
    }
    if matches!(
        mode,
        Mode::RunLivePackageAttestationSeal | Mode::VerifyLivePackageAttestationSeal
    ) && session_dir.is_none()
    {
        bail!("missing --session-dir for run/verify mode");
    }
    if matches!(
        mode,
        Mode::RunLivePackageAttestationSeal | Mode::VerifyLivePackageAttestationSeal
    ) && confirmed_decision_packet_session_dir.is_none()
    {
        bail!("missing --confirm-decision-packet-session-dir for run/verify mode");
    }
    let release_capsule_session_dir = release_capsule_session_dir
        .ok_or_else(|| anyhow!("missing --release-capsule-session-dir"))?;
    Ok(Some(Config {
        mode,
        release_capsule_session_dir,
        confirmed_decision_packet_session_dir,
        session_dir,
        output_path,
        json,
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanLivePackageAttestationSeal => plan_live_package_attestation_seal_report(&config)?,
        Mode::RenderLivePackageAttestationSeal => {
            render_live_package_attestation_seal_report(&config)?
        }
        Mode::RunLivePackageAttestationSeal => run_live_package_attestation_seal_report(&config)?,
        Mode::VerifyLivePackageAttestationSeal => {
            verify_live_package_attestation_seal_report(&config)?
        }
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_live_package_attestation_seal_report(
    config: &Config,
) -> Result<PackageAttestationSealReport> {
    let (release_capsule, digest_manifest, classification) =
        match config.confirmed_decision_packet_session_dir.as_deref() {
            Some(confirmed_decision_packet_session_dir) => match release_capsule_attestation_seal::verify_live_package_release_capsule_for_attestation_seal(
                &config.release_capsule_session_dir,
                confirmed_decision_packet_session_dir,
            ) {
                Ok(verified) => {
                    let classification = classify_verified_release_capsule(&verified);
                    (verified.contract, Some(verified.digest_manifest), classification)
                }
                Err(_) => {
                    let raw_contract = release_capsule_attestation_seal::load_live_package_release_capsule_contract_for_attestation_seal(
                        &config.release_capsule_session_dir,
                    )?;
                    let classification = classify_attestation_contract(&raw_contract, false);
                    (raw_contract, None, classification)
                }
            },
            None => {
                let raw_contract = release_capsule_attestation_seal::load_live_package_release_capsule_contract_for_attestation_seal(
                    &config.release_capsule_session_dir,
                )?;
                let classification = classify_attestation_contract(&raw_contract, false);
                (raw_contract, None, classification)
            }
        };
    let session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| config.release_capsule_session_dir.join("attestation-seal"));
    let manifest_sha256 = match digest_manifest.as_ref() {
        Some(value) => canonical_sha256_hex(value)?,
        None => "<pending-verified-release-capsule-digest-manifest>".to_string(),
    };
    let member_count = digest_manifest
        .as_ref()
        .map(|value| value.entries.len())
        .unwrap_or(0);
    let digest_algorithm = digest_manifest
        .as_ref()
        .map(|value| value.digest_algorithm.clone())
        .unwrap_or_else(|| release_capsule.digest_algorithm.clone());

    Ok(PackageAttestationSealReport {
        generated_at: Utc::now(),
        mode: "plan_live_package_attestation_seal".to_string(),
        verdict: TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealPlanReady,
        reason: format!(
            "release-capsule-native attestation seal is explicit for verified release-capsule session {}; this command stays archival and read-only while freezing one final custody record",
            config.release_capsule_session_dir.display()
        ),
        release_capsule_session_dir: config.release_capsule_session_dir.display().to_string(),
        activation_ticket_session_dir: Some(
            release_capsule
                .activation_ticket_session_dir
                .display()
                .to_string(),
        ),
        review_receipt_session_dir: Some(
            release_capsule
                .review_receipt_session_dir
                .display()
                .to_string(),
        ),
        handoff_bundle_session_dir: Some(
            release_capsule
                .handoff_bundle_session_dir
                .display()
                .to_string(),
        ),
        decision_packet_session_dir: Some(
            release_capsule
                .decision_packet_session_dir
                .display()
                .to_string(),
        ),
        execute_frozen_session_dir: Some(
            release_capsule
                .execute_frozen_session_dir
                .display()
                .to_string(),
        ),
        turn_green_session_dir: Some(
            release_capsule.turn_green_session_dir.display().to_string(),
        ),
        launch_packet_session_dir: Some(
            release_capsule.launch_packet_session_dir.display().to_string(),
        ),
        package_dir: Some(release_capsule.package_dir.display().to_string()),
        install_root: Some(release_capsule.install_root.display().to_string()),
        target_service_name: Some(release_capsule.target_service_name.clone()),
        backend_command: Some(release_capsule.backend_command.clone()),
        wrapper_timeout_ms: Some(release_capsule.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(
            release_capsule.service_status_max_staleness_ms,
        ),
        session_dir: Some(session_dir.display().to_string()),
        attestation_seal_session_path: None,
        attestation_seal_status_path: None,
        archived_release_capsule_report_path: None,
        archived_release_capsule_digest_manifest_path: None,
        result: Some(serialize_enum(&classification.0)),
        handoff_bundle_result: release_capsule.handoff_bundle_result.clone(),
        decision_packet_result: release_capsule.decision_packet_result.clone(),
        execute_frozen_result: release_capsule.execute_frozen_result.clone(),
        release_capsule_step: None,
        verify_release_capsule_command_summary: Some(verify_release_capsule_command_summary(
            &config.release_capsule_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            release_capsule
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        release_capsule_summary: Some(release_capsule.release_capsule_summary.clone()),
        release_capsule_audit_manifest_summary: Some(
            release_capsule.audit_manifest_summary.clone(),
        ),
        release_capsule_digest_manifest_sha256: Some(manifest_sha256.clone()),
        release_capsule_digest_manifest_entry_count: Some(member_count),
        release_capsule_digest_algorithm: Some(digest_algorithm.clone()),
        attestation_seal_summary: Some(attestation_seal_summary(
            classification.0,
            &release_capsule
                .reviewed_frozen_live_cutover_controller_command_summary,
        )),
        custody_record_summary: Some(custody_record_summary(
            &digest_algorithm,
            member_count,
            &manifest_sha256,
        )),
        release_capsule_digest_manifest: digest_manifest,
        current_pre_activation_gate_verdict: release_capsule.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: release_capsule.current_pre_activation_gate_reason,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this final attestation seal freezes verified release-capsule truth and one immutable custody record without enabling production execution"
                .to_string(),
    })
}

fn render_live_package_attestation_seal_report(
    config: &Config,
) -> Result<PackageAttestationSealReport> {
    let plan = plan_live_package_attestation_seal_report(config)?;
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
        "#!/usr/bin/env bash\nset -euo pipefail\n\ncopybot_tiny_live_activation_package_attestation_seal --release-capsule-session-dir {} --plan-live-package-attestation-seal\ncopybot_tiny_live_activation_package_attestation_seal --release-capsule-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --run-live-package-attestation-seal\ncopybot_tiny_live_activation_package_attestation_seal --release-capsule-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --verify-live-package-attestation-seal\n",
        shell_escape_path(&config.release_capsule_session_dir),
        shell_escape_path(&config.release_capsule_session_dir),
        shell_escape_path(&confirmed_decision_packet_session_dir),
        shell_escape_path(&session_dir),
        shell_escape_path(&config.release_capsule_session_dir),
        shell_escape_path(&confirmed_decision_packet_session_dir),
        shell_escape_path(&session_dir),
    );
    write_script(output_path, &script)?;
    Ok(PackageAttestationSealReport {
        generated_at: Utc::now(),
        mode: "render_live_package_attestation_seal".to_string(),
        verdict: TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealRendered,
        reason: format!(
            "rendered release-capsule-native attestation-seal script to {}",
            output_path.display()
        ),
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
        attestation_seal_session_path: None,
        attestation_seal_status_path: None,
        archived_release_capsule_report_path: None,
        archived_release_capsule_digest_manifest_path: None,
        result: plan.result,
        handoff_bundle_result: plan.handoff_bundle_result,
        decision_packet_result: plan.decision_packet_result,
        execute_frozen_result: plan.execute_frozen_result,
        release_capsule_step: None,
        verify_release_capsule_command_summary: plan.verify_release_capsule_command_summary,
        reviewed_frozen_live_cutover_controller_command_summary: plan
            .reviewed_frozen_live_cutover_controller_command_summary,
        release_capsule_summary: plan.release_capsule_summary,
        release_capsule_audit_manifest_summary: plan.release_capsule_audit_manifest_summary,
        release_capsule_digest_manifest_sha256: plan.release_capsule_digest_manifest_sha256,
        release_capsule_digest_manifest_entry_count:
            plan.release_capsule_digest_manifest_entry_count,
        release_capsule_digest_algorithm: plan.release_capsule_digest_algorithm,
        attestation_seal_summary: plan.attestation_seal_summary,
        custody_record_summary: plan.custody_record_summary,
        release_capsule_digest_manifest: None,
        current_pre_activation_gate_verdict: plan.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: plan.current_pre_activation_gate_reason,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this rendered script accepts the verified release-capsule session as the primary input and stays archival and read-only with respect to the real target"
                .to_string(),
    })
}

fn run_live_package_attestation_seal_report(
    config: &Config,
) -> Result<PackageAttestationSealReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for run mode"))?;
    let verified_release_capsule = match release_capsule_attestation_seal::verify_live_package_release_capsule_for_attestation_seal(
        &config.release_capsule_session_dir,
        config
            .confirmed_decision_packet_session_dir
            .as_deref()
            .ok_or_else(|| anyhow!("missing --confirm-decision-packet-session-dir for run mode"))?,
    ) {
        Ok(value) => value,
        Err(error) => {
            return Ok(failure_report_without_contract(
                config,
                TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealRefusedNowByInvalidOrDriftedContract,
                format!(
                    "failed to verify release-capsule session under {}: {error}",
                    config.release_capsule_session_dir.display()
                ),
            ))
        }
    };

    validate_managed_surface_session_dir(&verified_release_capsule.contract.install_root, session_dir)
        .with_context(|| {
            format!(
                "attestation-seal session dir {} overlaps the managed live target surface derived from verified release-capsule install_root {}",
                session_dir.display(),
                verified_release_capsule.contract.install_root.display()
            )
        })?;
    ensure_clean_attestation_seal_session_dir(session_dir)?;
    let paths = attestation_seal_paths(session_dir);
    let classification = classify_verified_release_capsule(&verified_release_capsule);
    let manifest_sha256 = canonical_sha256_hex(&verified_release_capsule.digest_manifest)?;
    let manifest_entry_count = verified_release_capsule.digest_manifest.entries.len();
    let session = planned_session(
        config,
        session_dir,
        &verified_release_capsule.contract,
        classification.0,
        &manifest_sha256,
        manifest_entry_count,
    );
    persist_json(&paths.session_path, &session)?;
    persist_json(
        &paths.release_capsule_report_path,
        &verified_release_capsule.report_json,
    )?;
    persist_json(
        &paths.release_capsule_digest_manifest_path,
        &verified_release_capsule.digest_manifest,
    )?;
    let release_capsule_step = Some(step_artifact(
        &paths.release_capsule_report_path,
        "verify_live_package_release_capsule",
        &verified_release_capsule.verdict,
        &verified_release_capsule.reason,
        verified_release_capsule.generated_at,
    ));
    let status = PackageAttestationSealStatus {
        status_version: ATTESTATION_SEAL_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        release_capsule_session_dir: config.release_capsule_session_dir.display().to_string(),
        result: classification.0,
        reason: classification.2.clone(),
        handoff_bundle_result: verified_release_capsule
            .contract
            .handoff_bundle_result
            .clone(),
        decision_packet_result: verified_release_capsule
            .contract
            .decision_packet_result
            .clone(),
        execute_frozen_result: verified_release_capsule
            .contract
            .execute_frozen_result
            .clone(),
        current_pre_activation_gate_verdict: verified_release_capsule
            .contract
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: verified_release_capsule
            .contract
            .current_pre_activation_gate_reason
            .clone(),
        release_capsule_step: release_capsule_step.clone(),
        release_capsule_summary: verified_release_capsule
            .contract
            .release_capsule_summary
            .clone(),
        release_capsule_audit_manifest_summary: verified_release_capsule
            .contract
            .audit_manifest_summary
            .clone(),
        release_capsule_digest_manifest_sha256: manifest_sha256.clone(),
        release_capsule_digest_manifest_entry_count: manifest_entry_count,
        release_capsule_digest_algorithm: verified_release_capsule
            .digest_manifest
            .digest_algorithm
            .clone(),
        attestation_seal_summary: attestation_seal_summary(
            classification.0,
            &verified_release_capsule
                .contract
                .reviewed_frozen_live_cutover_controller_command_summary,
        ),
        custody_record_summary: custody_record_summary(
            &verified_release_capsule.digest_manifest.digest_algorithm,
            manifest_entry_count,
            &manifest_sha256,
        ),
        explicit_statement: ATTESTATION_SEAL_STATUS_EXPLICIT_STATEMENT.to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    Ok(report_from_status(
        config,
        session_dir,
        &paths,
        &verified_release_capsule.contract,
        classification.1,
        &status,
        verified_release_capsule.digest_manifest,
    ))
}

fn verify_live_package_attestation_seal_report(
    config: &Config,
) -> Result<PackageAttestationSealReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = attestation_seal_paths(session_dir);
    let session: PackageAttestationSealSession = load_json(&paths.session_path)?;
    let status: PackageAttestationSealStatus = load_json(&paths.status_path)?;
    let mut mismatches = Vec::new();

    compare_string(
        &session.release_capsule_session_dir,
        &config.release_capsule_session_dir.display().to_string(),
        "attestation-seal release_capsule_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "attestation-seal session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "attestation-seal status session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.release_capsule_session_dir,
        &config.release_capsule_session_dir.display().to_string(),
        "attestation-seal status release_capsule_session_dir",
        &mut mismatches,
    );

    let verified_release_capsule = match release_capsule_attestation_seal::verify_live_package_release_capsule_for_attestation_seal(
        &config.release_capsule_session_dir,
        config
            .confirmed_decision_packet_session_dir
            .as_deref()
            .ok_or_else(|| anyhow!("missing --confirm-decision-packet-session-dir for verify mode"))?,
    ) {
        Ok(value) => value,
        Err(error) => {
            let raw_contract = release_capsule_attestation_seal::load_live_package_release_capsule_contract_for_attestation_seal(
                &config.release_capsule_session_dir,
            )?;
            return Ok(failure_report_for_verify(
                config,
                session_dir,
                &raw_contract,
                vec![format!(
                    "failed to verify release-capsule session under {}: {error}",
                    config.release_capsule_session_dir.display()
                )],
            ));
        }
    };

    let stored_release_capsule_report: serde_json::Value = load_required_step_json(
        &status.release_capsule_step,
        &paths.release_capsule_report_path,
        "release_capsule_step",
        &mut mismatches,
    )?;
    let stored_release_capsule_report_view: NestedReleaseCapsuleReportView =
        serde_json::from_value(stored_release_capsule_report.clone()).with_context(|| {
            format!(
                "failed parsing archived nested release-capsule report {}",
                paths.release_capsule_report_path.display()
            )
        })?;
    let verified_release_capsule_report_view: NestedReleaseCapsuleReportView =
        serde_json::from_value(verified_release_capsule.report_json.clone()).with_context(
            || {
                format!(
                    "failed parsing freshly verified nested release-capsule report for {}",
                    config.release_capsule_session_dir.display()
                )
            },
        )?;
    compare_json_ignoring_generated_at(
        &stored_release_capsule_report,
        &verified_release_capsule.report_json,
        "attestation-seal archived release-capsule report json",
        &mut mismatches,
    );
    compare_string(
        &stored_release_capsule_report_view.mode,
        &verified_release_capsule_report_view.mode,
        "attestation-seal archived release-capsule report mode",
        &mut mismatches,
    );
    compare_string(
        &stored_release_capsule_report_view.verdict,
        &verified_release_capsule_report_view.verdict,
        "attestation-seal archived release-capsule report verdict",
        &mut mismatches,
    );
    compare_string(
        &stored_release_capsule_report_view.reason,
        &verified_release_capsule_report_view.reason,
        "attestation-seal archived release-capsule report reason",
        &mut mismatches,
    );
    if stored_release_capsule_report_view.generated_at
        != verified_release_capsule_report_view.generated_at
    {
        mismatches.push(format!(
            "attestation-seal archived release-capsule report generated_at {:?} does not match freshly verified nested truth {:?}",
            stored_release_capsule_report_view.generated_at,
            verified_release_capsule_report_view.generated_at
        ));
    }
    let expected_release_capsule_step = step_artifact(
        &paths.release_capsule_report_path,
        &verified_release_capsule_report_view.mode,
        &verified_release_capsule_report_view.verdict,
        &verified_release_capsule_report_view.reason,
        verified_release_capsule_report_view.generated_at,
    );
    compare_step_artifact(
        status.release_capsule_step.as_ref(),
        &expected_release_capsule_step,
        "attestation-seal release_capsule_step",
        &mut mismatches,
    );
    if verified_release_capsule.verdict != "tiny_live_package_release_capsule_verify_ok" {
        mismatches.push(format!(
            "nested release-capsule verification is non-green: {}",
            verified_release_capsule.reason
        ));
    }

    let stored_manifest_bytes = fs::read(&paths.release_capsule_digest_manifest_path)
        .with_context(|| {
            format!(
                "failed reading {}",
                paths.release_capsule_digest_manifest_path.display()
            )
        })?;
    let stored_manifest: release_capsule_attestation_seal::ReleaseCapsuleDigestManifest =
        serde_json::from_slice(&stored_manifest_bytes).with_context(|| {
            format!(
                "failed parsing {}",
                paths.release_capsule_digest_manifest_path.display()
            )
        })?;
    if stored_manifest != verified_release_capsule.digest_manifest {
        mismatches.push(
            "attestation-seal archived release-capsule digest manifest does not match freshly verified nested truth"
                .to_string(),
        );
    }
    let expected_manifest_sha256 = canonical_sha256_hex(&verified_release_capsule.digest_manifest)?;
    let stored_manifest_sha256 = sha256_hex_bytes(&stored_manifest_bytes);
    if stored_manifest_sha256 != expected_manifest_sha256 {
        mismatches.push(format!(
            "attestation-seal archived release-capsule digest manifest sha256 {stored_manifest_sha256} does not match expected {expected_manifest_sha256}"
        ));
    }
    let expected_manifest_entry_count = verified_release_capsule.digest_manifest.entries.len();

    compare_string(
        &session.activation_ticket_session_dir,
        &verified_release_capsule
            .contract
            .activation_ticket_session_dir
            .display()
            .to_string(),
        "attestation-seal activation_ticket_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.review_receipt_session_dir,
        &verified_release_capsule
            .contract
            .review_receipt_session_dir
            .display()
            .to_string(),
        "attestation-seal review_receipt_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.handoff_bundle_session_dir,
        &verified_release_capsule
            .contract
            .handoff_bundle_session_dir
            .display()
            .to_string(),
        "attestation-seal handoff_bundle_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.decision_packet_session_dir,
        &verified_release_capsule
            .contract
            .decision_packet_session_dir
            .display()
            .to_string(),
        "attestation-seal decision_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.execute_frozen_session_dir,
        &verified_release_capsule
            .contract
            .execute_frozen_session_dir
            .display()
            .to_string(),
        "attestation-seal execute_frozen_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.turn_green_session_dir,
        &verified_release_capsule
            .contract
            .turn_green_session_dir
            .display()
            .to_string(),
        "attestation-seal turn_green_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.launch_packet_session_dir,
        &verified_release_capsule
            .contract
            .launch_packet_session_dir
            .display()
            .to_string(),
        "attestation-seal launch_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.package_dir,
        &verified_release_capsule
            .contract
            .package_dir
            .display()
            .to_string(),
        "attestation-seal package_dir",
        &mut mismatches,
    );
    compare_string(
        &session.install_root,
        &verified_release_capsule
            .contract
            .install_root
            .display()
            .to_string(),
        "attestation-seal install_root",
        &mut mismatches,
    );
    compare_string(
        &session.target_service_name,
        &verified_release_capsule.contract.target_service_name,
        "attestation-seal target_service_name",
        &mut mismatches,
    );
    compare_string(
        &session.backend_command,
        &verified_release_capsule.contract.backend_command,
        "attestation-seal backend_command",
        &mut mismatches,
    );
    compare_u64(
        session.wrapper_timeout_ms,
        verified_release_capsule.contract.wrapper_timeout_ms,
        "attestation-seal wrapper_timeout_ms",
        &mut mismatches,
    );
    compare_u64(
        session.service_status_max_staleness_ms,
        verified_release_capsule
            .contract
            .service_status_max_staleness_ms,
        "attestation-seal service_status_max_staleness_ms",
        &mut mismatches,
    );

    let classification = classify_verified_release_capsule(&verified_release_capsule);
    let expected_attestation_seal_summary = attestation_seal_summary(
        classification.0,
        &verified_release_capsule
            .contract
            .reviewed_frozen_live_cutover_controller_command_summary,
    );
    let expected_custody_record_summary = custody_record_summary(
        &verified_release_capsule.digest_manifest.digest_algorithm,
        expected_manifest_entry_count,
        &expected_manifest_sha256,
    );
    let expected_result = serialize_enum(&classification.0);

    compare_string(
        &session.verify_release_capsule_command_summary,
        &verify_release_capsule_command_summary(&config.release_capsule_session_dir),
        "attestation-seal session verify_release_capsule_command_summary",
        &mut mismatches,
    );
    compare_string(
        &session.reviewed_frozen_live_cutover_controller_command_summary,
        &verified_release_capsule
            .contract
            .reviewed_frozen_live_cutover_controller_command_summary,
        "attestation-seal session reviewed_frozen_live_cutover_controller_command_summary",
        &mut mismatches,
    );
    compare_string(
        &session.release_capsule_summary,
        &verified_release_capsule.contract.release_capsule_summary,
        "attestation-seal session release_capsule_summary",
        &mut mismatches,
    );
    compare_string(
        &status.release_capsule_summary,
        &verified_release_capsule.contract.release_capsule_summary,
        "attestation-seal status release_capsule_summary",
        &mut mismatches,
    );
    compare_string(
        &session.release_capsule_audit_manifest_summary,
        &verified_release_capsule.contract.audit_manifest_summary,
        "attestation-seal session release_capsule_audit_manifest_summary",
        &mut mismatches,
    );
    compare_string(
        &status.release_capsule_audit_manifest_summary,
        &verified_release_capsule.contract.audit_manifest_summary,
        "attestation-seal status release_capsule_audit_manifest_summary",
        &mut mismatches,
    );
    compare_string(
        &session.release_capsule_digest_manifest_sha256,
        &expected_manifest_sha256,
        "attestation-seal session release_capsule_digest_manifest_sha256",
        &mut mismatches,
    );
    compare_string(
        &status.release_capsule_digest_manifest_sha256,
        &expected_manifest_sha256,
        "attestation-seal status release_capsule_digest_manifest_sha256",
        &mut mismatches,
    );
    compare_usize(
        session.release_capsule_digest_manifest_entry_count,
        expected_manifest_entry_count,
        "attestation-seal session release_capsule_digest_manifest_entry_count",
        &mut mismatches,
    );
    compare_usize(
        status.release_capsule_digest_manifest_entry_count,
        expected_manifest_entry_count,
        "attestation-seal status release_capsule_digest_manifest_entry_count",
        &mut mismatches,
    );
    compare_string(
        &session.release_capsule_digest_algorithm,
        &verified_release_capsule.digest_manifest.digest_algorithm,
        "attestation-seal session release_capsule_digest_algorithm",
        &mut mismatches,
    );
    compare_string(
        &status.release_capsule_digest_algorithm,
        &verified_release_capsule.digest_manifest.digest_algorithm,
        "attestation-seal status release_capsule_digest_algorithm",
        &mut mismatches,
    );
    compare_string(
        &session.attestation_seal_summary,
        &expected_attestation_seal_summary,
        "attestation-seal session attestation_seal_summary",
        &mut mismatches,
    );
    compare_string(
        &status.attestation_seal_summary,
        &expected_attestation_seal_summary,
        "attestation-seal status attestation_seal_summary",
        &mut mismatches,
    );
    compare_string(
        &session.custody_record_summary,
        &expected_custody_record_summary,
        "attestation-seal session custody_record_summary",
        &mut mismatches,
    );
    compare_string(
        &status.custody_record_summary,
        &expected_custody_record_summary,
        "attestation-seal status custody_record_summary",
        &mut mismatches,
    );
    compare_string(
        &serialize_enum(&status.result),
        &expected_result,
        "attestation-seal status result",
        &mut mismatches,
    );
    compare_optional_string(
        status.handoff_bundle_result.as_deref(),
        verified_release_capsule
            .contract
            .handoff_bundle_result
            .as_deref(),
        "attestation-seal status handoff_bundle_result",
        &mut mismatches,
    );
    compare_optional_string(
        status.decision_packet_result.as_deref(),
        verified_release_capsule
            .contract
            .decision_packet_result
            .as_deref(),
        "attestation-seal status decision_packet_result",
        &mut mismatches,
    );
    compare_optional_string(
        status.execute_frozen_result.as_deref(),
        verified_release_capsule
            .contract
            .execute_frozen_result
            .as_deref(),
        "attestation-seal status execute_frozen_result",
        &mut mismatches,
    );
    compare_optional_string(
        status.current_pre_activation_gate_verdict.as_deref(),
        verified_release_capsule
            .contract
            .current_pre_activation_gate_verdict
            .as_deref(),
        "attestation-seal status current_pre_activation_gate_verdict",
        &mut mismatches,
    );
    compare_optional_string(
        status.current_pre_activation_gate_reason.as_deref(),
        verified_release_capsule
            .contract
            .current_pre_activation_gate_reason
            .as_deref(),
        "attestation-seal status current_pre_activation_gate_reason",
        &mut mismatches,
    );

    if !mismatches.is_empty() {
        return Ok(failure_report_for_verify(
            config,
            session_dir,
            &verified_release_capsule.contract,
            mismatches,
        ));
    }

    Ok(PackageAttestationSealReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_attestation_seal".to_string(),
        verdict: TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealVerifyOk,
        reason: format!(
            "release-capsule-native attestation seal under {} is coherent and correctly binds the final custody record to verified release-capsule truth",
            session_dir.display()
        ),
        release_capsule_session_dir: config.release_capsule_session_dir.display().to_string(),
        activation_ticket_session_dir: Some(
            verified_release_capsule
                .contract
                .activation_ticket_session_dir
                .display()
                .to_string(),
        ),
        review_receipt_session_dir: Some(
            verified_release_capsule
                .contract
                .review_receipt_session_dir
                .display()
                .to_string(),
        ),
        handoff_bundle_session_dir: Some(
            verified_release_capsule
                .contract
                .handoff_bundle_session_dir
                .display()
                .to_string(),
        ),
        decision_packet_session_dir: Some(
            verified_release_capsule
                .contract
                .decision_packet_session_dir
                .display()
                .to_string(),
        ),
        execute_frozen_session_dir: Some(
            verified_release_capsule
                .contract
                .execute_frozen_session_dir
                .display()
                .to_string(),
        ),
        turn_green_session_dir: Some(
            verified_release_capsule
                .contract
                .turn_green_session_dir
                .display()
                .to_string(),
        ),
        launch_packet_session_dir: Some(
            verified_release_capsule
                .contract
                .launch_packet_session_dir
                .display()
                .to_string(),
        ),
        package_dir: Some(verified_release_capsule.contract.package_dir.display().to_string()),
        install_root: Some(
            verified_release_capsule
                .contract
                .install_root
                .display()
                .to_string(),
        ),
        target_service_name: Some(
            verified_release_capsule
                .contract
                .target_service_name
                .clone(),
        ),
        backend_command: Some(verified_release_capsule.contract.backend_command.clone()),
        wrapper_timeout_ms: Some(verified_release_capsule.contract.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(
            verified_release_capsule
                .contract
                .service_status_max_staleness_ms,
        ),
        session_dir: Some(session_dir.display().to_string()),
        attestation_seal_session_path: Some(paths.session_path.display().to_string()),
        attestation_seal_status_path: Some(paths.status_path.display().to_string()),
        archived_release_capsule_report_path: Some(
            paths.release_capsule_report_path.display().to_string(),
        ),
        archived_release_capsule_digest_manifest_path: Some(
            paths.release_capsule_digest_manifest_path
                .display()
                .to_string(),
        ),
        result: Some(expected_result),
        handoff_bundle_result: verified_release_capsule.contract.handoff_bundle_result.clone(),
        decision_packet_result: verified_release_capsule.contract.decision_packet_result.clone(),
        execute_frozen_result: verified_release_capsule.contract.execute_frozen_result.clone(),
        release_capsule_step: Some(expected_release_capsule_step),
        verify_release_capsule_command_summary: Some(verify_release_capsule_command_summary(
            &config.release_capsule_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            verified_release_capsule
                .contract
                .reviewed_frozen_live_cutover_controller_command_summary,
        ),
        release_capsule_summary: Some(
            verified_release_capsule.contract.release_capsule_summary.clone(),
        ),
        release_capsule_audit_manifest_summary: Some(
            verified_release_capsule.contract.audit_manifest_summary.clone(),
        ),
        release_capsule_digest_manifest_sha256: Some(expected_manifest_sha256),
        release_capsule_digest_manifest_entry_count: Some(expected_manifest_entry_count),
        release_capsule_digest_algorithm: Some(
            verified_release_capsule.digest_manifest.digest_algorithm.clone(),
        ),
        attestation_seal_summary: Some(expected_attestation_seal_summary),
        custody_record_summary: Some(expected_custody_record_summary),
        release_capsule_digest_manifest: Some(verified_release_capsule.digest_manifest),
        current_pre_activation_gate_verdict: verified_release_capsule
            .contract
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: verified_release_capsule
            .contract
            .current_pre_activation_gate_reason
            .clone(),
        verification_mismatches: mismatches,
        explicit_statement: ATTESTATION_SEAL_VERIFY_EXPLICIT_STATEMENT.to_string(),
    })
}

fn verify_release_capsule_command_summary(release_capsule_session_dir: &Path) -> String {
    format!(
        "verify immutable release-capsule session under {} before freezing the final attestation seal",
        release_capsule_session_dir.display()
    )
}

fn classify_attestation_contract(
    contract: &release_capsule_attestation_seal::LivePackageReleaseCapsuleContractView,
    assume_verified: bool,
) -> (
    LivePackageAttestationSealResult,
    TinyLivePackageAttestationSealVerdict,
    String,
) {
    if !assume_verified {
        return (
            LivePackageAttestationSealResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealRefusedNowByInvalidOrDriftedContract,
            "the release-capsule chain is not verified, so the attestation seal remains refused".to_string(),
        );
    }
    match contract.result.as_deref() {
        Some("refused_now_by_stage3") => (
            LivePackageAttestationSealResult::RefusedNowByStage3,
            TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealRefusedNowByStage3,
            "the immutable release capsule remains refused now by Stage 3, so this attestation seal remains an explicit refusal record".to_string(),
        ),
        Some("refused_now_by_pre_activation_gate") => (
            LivePackageAttestationSealResult::RefusedNowByPreActivationGate,
            TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealRefusedNowByPreActivationGate,
            "the immutable release capsule remains refused now by the pre-activation gate, so this attestation seal remains an explicit refusal record".to_string(),
        ),
        Some("ready_for_manual_execution_when_gate_turns_green") => (
            LivePackageAttestationSealResult::ReadyForManualExecutionWhenGateTurnsGreen,
            TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealReadyForManualExecutionWhenGateTurnsGreen,
            "the immutable release capsule is coherent and this attestation seal is ready for manual execution when gate truth turns green".to_string(),
        ),
        _ => (
            LivePackageAttestationSealResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealRefusedNowByInvalidOrDriftedContract,
            "the verified release-capsule chain is invalid, drifted, or non-runnable, so this attestation seal remains refused".to_string(),
        ),
    }
}

fn classify_verified_release_capsule(
    verified: &release_capsule_attestation_seal::VerifiedLivePackageReleaseCapsuleAttestationSealStep,
) -> (
    LivePackageAttestationSealResult,
    TinyLivePackageAttestationSealVerdict,
    String,
) {
    if verified.verdict != "tiny_live_package_release_capsule_verify_ok" {
        return (
            LivePackageAttestationSealResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealRefusedNowByInvalidOrDriftedContract,
            "the immutable release capsule did not verify cleanly, so this attestation seal remains refused".to_string(),
        );
    }
    classify_attestation_contract(&verified.contract, true)
}

fn attestation_seal_summary(
    result: LivePackageAttestationSealResult,
    reviewed_frozen_live_cutover_controller_command_summary: &str,
) -> String {
    match result {
        LivePackageAttestationSealResult::RefusedNowByStage3 => {
            "Attestation seal outcome: the immutable reviewed chain remains refused now by Stage 3; archive this final seal as custody evidence only and do not run the frozen controller.".to_string()
        }
        LivePackageAttestationSealResult::RefusedNowByPreActivationGate => {
            "Attestation seal outcome: the immutable reviewed chain remains refused now by the pre-activation gate; archive this final seal as custody evidence only and do not run the frozen controller.".to_string()
        }
        LivePackageAttestationSealResult::RefusedNowByInvalidOrDriftedContract => {
            "Attestation seal outcome: the immutable reviewed chain is invalid or drifted; archive this final seal for audit only and mint a new coherent custody record before any later execution record.".to_string()
        }
        LivePackageAttestationSealResult::ReadyForManualExecutionWhenGateTurnsGreen => format!(
            "Attestation seal outcome: this immutable custody record is ready for manual execution when gate truth turns green; the exact reviewed frozen controller command remains: {reviewed_frozen_live_cutover_controller_command_summary}"
        ),
    }
}

fn custody_record_summary(
    digest_algorithm: &str,
    member_count: usize,
    manifest_sha256: &str,
) -> String {
    format!(
        "Custody record identity: nested release-capsule digest manifest uses {digest_algorithm} across {member_count} archival members with manifest sha256 {manifest_sha256}; any digest drift invalidates this attestation seal"
    )
}

fn attestation_seal_paths(session_dir: &Path) -> PackageAttestationSealPaths {
    PackageAttestationSealPaths {
        session_path: session_dir
            .join("tiny_live_activation_package_attestation_seal.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_attestation_seal.status.json"),
        release_capsule_report_path: session_dir
            .join("tiny_live_activation_package_attestation_seal.release_capsule.report.json"),
        release_capsule_digest_manifest_path: session_dir.join(
            "tiny_live_activation_package_attestation_seal.release_capsule.digest_manifest.json",
        ),
    }
}

fn ensure_clean_attestation_seal_session_dir(session_dir: &Path) -> Result<()> {
    if session_dir.exists() {
        bail!(
            "refusing to overwrite existing attestation-seal session dir {}",
            session_dir.display()
        );
    }
    fs::create_dir_all(session_dir).with_context(|| {
        format!(
            "failed creating attestation-seal session dir {}",
            session_dir.display()
        )
    })
}

fn planned_session(
    config: &Config,
    session_dir: &Path,
    contract: &release_capsule_attestation_seal::LivePackageReleaseCapsuleContractView,
    result: LivePackageAttestationSealResult,
    manifest_sha256: &str,
    manifest_entry_count: usize,
) -> PackageAttestationSealSession {
    PackageAttestationSealSession {
        session_version: ATTESTATION_SEAL_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        release_capsule_session_dir: config.release_capsule_session_dir.display().to_string(),
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
        verify_release_capsule_command_summary: verify_release_capsule_command_summary(
            &config.release_capsule_session_dir,
        ),
        reviewed_frozen_live_cutover_controller_command_summary: contract
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        release_capsule_summary: contract.release_capsule_summary.clone(),
        release_capsule_audit_manifest_summary: contract.audit_manifest_summary.clone(),
        release_capsule_digest_manifest_sha256: manifest_sha256.to_string(),
        release_capsule_digest_manifest_entry_count: manifest_entry_count,
        release_capsule_digest_algorithm: contract.digest_algorithm.clone(),
        attestation_seal_summary: attestation_seal_summary(
            result,
            &contract.reviewed_frozen_live_cutover_controller_command_summary,
        ),
        custody_record_summary: custody_record_summary(
            &contract.digest_algorithm,
            manifest_entry_count,
            manifest_sha256,
        ),
        explicit_statement: ATTESTATION_SEAL_SESSION_EXPLICIT_STATEMENT.to_string(),
    }
}

fn report_from_status(
    config: &Config,
    session_dir: &Path,
    paths: &PackageAttestationSealPaths,
    contract: &release_capsule_attestation_seal::LivePackageReleaseCapsuleContractView,
    verdict: TinyLivePackageAttestationSealVerdict,
    status: &PackageAttestationSealStatus,
    digest_manifest: release_capsule_attestation_seal::ReleaseCapsuleDigestManifest,
) -> PackageAttestationSealReport {
    PackageAttestationSealReport {
        generated_at: Utc::now(),
        mode: "run_live_package_attestation_seal".to_string(),
        verdict,
        reason: status.reason.clone(),
        release_capsule_session_dir: config.release_capsule_session_dir.display().to_string(),
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
        attestation_seal_session_path: Some(paths.session_path.display().to_string()),
        attestation_seal_status_path: Some(paths.status_path.display().to_string()),
        archived_release_capsule_report_path: Some(
            paths.release_capsule_report_path.display().to_string(),
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
        release_capsule_step: status.release_capsule_step.clone(),
        verify_release_capsule_command_summary: Some(verify_release_capsule_command_summary(
            &config.release_capsule_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            contract
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        release_capsule_summary: Some(status.release_capsule_summary.clone()),
        release_capsule_audit_manifest_summary: Some(
            status.release_capsule_audit_manifest_summary.clone(),
        ),
        release_capsule_digest_manifest_sha256: Some(
            status.release_capsule_digest_manifest_sha256.clone(),
        ),
        release_capsule_digest_manifest_entry_count: Some(
            status.release_capsule_digest_manifest_entry_count,
        ),
        release_capsule_digest_algorithm: Some(status.release_capsule_digest_algorithm.clone()),
        attestation_seal_summary: Some(status.attestation_seal_summary.clone()),
        custody_record_summary: Some(status.custody_record_summary.clone()),
        release_capsule_digest_manifest: Some(digest_manifest),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        verification_mismatches: Vec::new(),
        explicit_statement: status.explicit_statement.clone(),
    }
}

fn failure_report_without_contract(
    config: &Config,
    verdict: TinyLivePackageAttestationSealVerdict,
    reason: String,
) -> PackageAttestationSealReport {
    PackageAttestationSealReport {
        generated_at: Utc::now(),
        mode: match config.mode {
            Mode::PlanLivePackageAttestationSeal => "plan_live_package_attestation_seal".to_string(),
            Mode::RenderLivePackageAttestationSeal => {
                "render_live_package_attestation_seal".to_string()
            }
            Mode::RunLivePackageAttestationSeal => "run_live_package_attestation_seal".to_string(),
            Mode::VerifyLivePackageAttestationSeal => {
                "verify_live_package_attestation_seal".to_string()
            }
        },
        verdict,
        reason,
        release_capsule_session_dir: config.release_capsule_session_dir.display().to_string(),
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
        attestation_seal_session_path: None,
        attestation_seal_status_path: None,
        archived_release_capsule_report_path: None,
        archived_release_capsule_digest_manifest_path: None,
        result: Some("refused_now_by_invalid_or_drifted_contract".to_string()),
        handoff_bundle_result: None,
        decision_packet_result: None,
        execute_frozen_result: None,
        release_capsule_step: None,
        verify_release_capsule_command_summary: Some(verify_release_capsule_command_summary(
            &config.release_capsule_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: None,
        release_capsule_summary: None,
        release_capsule_audit_manifest_summary: None,
        release_capsule_digest_manifest_sha256: None,
        release_capsule_digest_manifest_entry_count: None,
        release_capsule_digest_algorithm: None,
        attestation_seal_summary: None,
        custody_record_summary: None,
        release_capsule_digest_manifest: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this immutable attestation seal refused before persisting because the release-capsule artifact could not be verified coherently"
                .to_string(),
    }
}

fn failure_report_for_verify(
    config: &Config,
    session_dir: &Path,
    contract: &release_capsule_attestation_seal::LivePackageReleaseCapsuleContractView,
    mismatches: Vec<String>,
) -> PackageAttestationSealReport {
    let paths = attestation_seal_paths(session_dir);
    let reason = mismatches.first().cloned().unwrap_or_else(|| {
        "release-capsule-native attestation seal verification failed".to_string()
    });
    PackageAttestationSealReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_attestation_seal".to_string(),
        verdict: TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealVerifyInvalid,
        reason,
        release_capsule_session_dir: config.release_capsule_session_dir.display().to_string(),
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
        attestation_seal_session_path: Some(paths.session_path.display().to_string()),
        attestation_seal_status_path: Some(paths.status_path.display().to_string()),
        archived_release_capsule_report_path: Some(
            paths.release_capsule_report_path.display().to_string(),
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
        release_capsule_step: None,
        verify_release_capsule_command_summary: Some(verify_release_capsule_command_summary(
            &config.release_capsule_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            contract
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        release_capsule_summary: Some(contract.release_capsule_summary.clone()),
        release_capsule_audit_manifest_summary: Some(contract.audit_manifest_summary.clone()),
        release_capsule_digest_manifest_sha256: None,
        release_capsule_digest_manifest_entry_count: None,
        release_capsule_digest_algorithm: Some(contract.digest_algorithm.clone()),
        attestation_seal_summary: None,
        custody_record_summary: None,
        release_capsule_digest_manifest: None,
        current_pre_activation_gate_verdict: contract.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: contract.current_pre_activation_gate_reason.clone(),
        verification_mismatches: mismatches,
        explicit_statement:
            "this verification failure means the immutable attestation seal no longer proves the exact reviewed frozen controller context or the nested digest-manifest identity"
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
    step: &Option<PackageAttestationSealStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<serde_json::Value> {
    let Some(step) = step else {
        mismatches.push(format!("{label} is missing from attestation-seal status"));
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
) -> PackageAttestationSealStepArtifact {
    PackageAttestationSealStepArtifact {
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
    actual: Option<&PackageAttestationSealStepArtifact>,
    expected: &PackageAttestationSealStepArtifact,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    let Some(actual) = actual else {
        mismatches.push(format!("{label} is missing from attestation-seal status"));
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
            "failed writing attestation-seal script to {}",
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

fn render_report_lines(report: &PackageAttestationSealReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "release_capsule_session_dir={}",
            report.release_capsule_session_dir
        ),
    ];
    if let Some(value) = &report.result {
        lines.push(format!("result={value}"));
    }
    if let Some(value) = &report.attestation_seal_summary {
        lines.push(format!("attestation_seal_summary={value}"));
    }
    if let Some(value) = &report.custody_record_summary {
        lines.push(format!("custody_record_summary={value}"));
    }
    if let Some(value) = &report.release_capsule_digest_manifest_sha256 {
        lines.push(format!("release_capsule_digest_manifest_sha256={value}"));
    }
    if let Some(value) = report.release_capsule_digest_manifest_entry_count {
        lines.push(format!(
            "release_capsule_digest_manifest_entry_count={value}"
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
            "attestation_seal_ready",
            release_capsule_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_attestation_seal_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealReadyForManualExecutionWhenGateTurnsGreen
        );
        assert_eq!(
            run.result.as_deref(),
            Some("ready_for_manual_execution_when_gate_turns_green")
        );

        let verify = verify_live_package_attestation_seal_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealVerifyOk,
            "{:#?}",
            verify.verification_mismatches
        );
    }

    #[test]
    fn stage3_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "attestation_seal_stage3",
            release_capsule_verify_report(
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "blocked",
                "stage3 blocked",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_attestation_seal_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealRefusedNowByStage3
        );
    }

    #[test]
    fn pre_activation_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "attestation_seal_pre_activation",
            release_capsule_verify_report(
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
                "blocked",
                "pre-activation blocked",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_attestation_seal_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealRefusedNowByPreActivationGate
        );
    }

    #[test]
    fn drifted_release_capsule_contract_is_refused() {
        let fixture = fake_command_fixture(
            "attestation_seal_drifted",
            release_capsule_verify_report(
                "refused_now_by_invalid_or_drifted_contract",
                "refused_now_by_invalid_or_drifted_contract",
                "refused_now_by_invalid_or_drifted_contract",
                "refused_now_by_invalid_or_drifted_contract",
                "refused_now_by_invalid_or_drifted_contract",
                "green",
                "contract drifted",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_attestation_seal_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealRefusedNowByInvalidOrDriftedContract
        );
    }

    #[test]
    fn run_refuses_managed_surface_overlap_before_writing_any_artifacts() {
        let fixture = fake_command_fixture(
            "attestation_seal_overlap",
            release_capsule_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
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
        let unsafe_session_dir = managed_surface.session_dir.join("attestation-seal-session");
        let config = Config {
            mode: Mode::RunLivePackageAttestationSeal,
            release_capsule_session_dir: fixture.release_capsule_session_dir.clone(),
            confirmed_decision_packet_session_dir: Some(
                fixture.decision_packet_session_dir.clone(),
            ),
            session_dir: Some(unsafe_session_dir.clone()),
            output_path: None,
            json: true,
        };

        let error = run_live_package_attestation_seal_report(&config).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("overlaps the managed live target surface"),
            "{error:#}"
        );
        let paths = attestation_seal_paths(&unsafe_session_dir);
        assert!(!paths.session_path.exists());
        assert!(!paths.status_path.exists());
        assert!(!paths.release_capsule_digest_manifest_path.exists());
    }

    #[test]
    fn verify_rejects_tampered_release_capsule_step_path() {
        let fixture = fake_command_fixture(
            "attestation_seal_tampered_step",
            release_capsule_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_attestation_seal_report(&fixture.run_config()).unwrap();
        let paths = attestation_seal_paths(&fixture.attestation_seal_session_dir);
        let mut status: PackageAttestationSealStatus = load_json(&paths.status_path).unwrap();
        let foreign = fixture
            .fixture_dir
            .join("foreign.release-capsule.report.json");
        fs::write(&foreign, "{}").unwrap();
        status.release_capsule_step.as_mut().unwrap().path = foreign.display().to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_attestation_seal_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_attestation_seal_text() {
        let fixture = fake_command_fixture(
            "attestation_seal_tampered_text",
            release_capsule_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_attestation_seal_report(&fixture.run_config()).unwrap();
        let paths = attestation_seal_paths(&fixture.attestation_seal_session_dir);
        let mut session: PackageAttestationSealSession = load_json(&paths.session_path).unwrap();
        session.attestation_seal_summary = "tampered summary".to_string();
        persist_json(&paths.session_path, &session).unwrap();

        let verify = verify_live_package_attestation_seal_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_status_result() {
        let fixture = fake_command_fixture(
            "attestation_seal_tampered_status_result",
            release_capsule_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_attestation_seal_report(&fixture.run_config()).unwrap();
        let paths = attestation_seal_paths(&fixture.attestation_seal_session_dir);
        let mut status: PackageAttestationSealStatus = load_json(&paths.status_path).unwrap();
        status.result = LivePackageAttestationSealResult::RefusedNowByStage3;
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_attestation_seal_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_status_gate_fields() {
        let fixture = fake_command_fixture(
            "attestation_seal_tampered_status_gate",
            release_capsule_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_attestation_seal_report(&fixture.run_config()).unwrap();
        let paths = attestation_seal_paths(&fixture.attestation_seal_session_dir);
        let mut status: PackageAttestationSealStatus = load_json(&paths.status_path).unwrap();
        status.current_pre_activation_gate_verdict = Some("tampered-gate".to_string());
        status.current_pre_activation_gate_reason = Some("tampered gate reason".to_string());
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_attestation_seal_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_status_handoff_bundle_result() {
        let fixture = fake_command_fixture(
            "attestation_seal_tampered_status_handoff_bundle",
            release_capsule_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_attestation_seal_report(&fixture.run_config()).unwrap();
        let paths = attestation_seal_paths(&fixture.attestation_seal_session_dir);
        let mut status: PackageAttestationSealStatus = load_json(&paths.status_path).unwrap();
        status.handoff_bundle_result = Some("tampered-handoff-bundle-result".to_string());
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_attestation_seal_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_nested_release_capsule_report_content() {
        let fixture = fake_command_fixture(
            "attestation_seal_tampered_nested_report",
            release_capsule_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_attestation_seal_report(&fixture.run_config()).unwrap();
        let paths = attestation_seal_paths(&fixture.attestation_seal_session_dir);
        let mut report: serde_json::Value = load_json(&paths.release_capsule_report_path).unwrap();
        report.as_object_mut().unwrap().insert(
            "reason".to_string(),
            json!("tampered nested release capsule reason"),
        );
        persist_json(&paths.release_capsule_report_path, &report).unwrap();

        let verify = verify_live_package_attestation_seal_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_retimed_nested_release_capsule_report_and_step_generated_at() {
        let fixture = fake_command_fixture(
            "attestation_seal_retimed_nested_report",
            release_capsule_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_attestation_seal_report(&fixture.run_config()).unwrap();
        let paths = attestation_seal_paths(&fixture.attestation_seal_session_dir);
        let mut report: serde_json::Value = load_json(&paths.release_capsule_report_path).unwrap();
        let archived_generated_at =
            DateTime::parse_from_rfc3339(report["generated_at"].as_str().unwrap())
                .unwrap()
                .with_timezone(&Utc);
        let forged_generated_at = archived_generated_at + chrono::Duration::seconds(61);
        report.as_object_mut().unwrap().insert(
            "generated_at".to_string(),
            json!(forged_generated_at.to_rfc3339()),
        );
        persist_json(&paths.release_capsule_report_path, &report).unwrap();

        let mut status: PackageAttestationSealStatus = load_json(&paths.status_path).unwrap();
        status.release_capsule_step.as_mut().unwrap().generated_at = forged_generated_at;
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_attestation_seal_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_digest_member_identity() {
        let fixture = fake_command_fixture(
            "attestation_seal_tampered_digest_manifest",
            release_capsule_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_attestation_seal_report(&fixture.run_config()).unwrap();
        let paths = attestation_seal_paths(&fixture.attestation_seal_session_dir);
        let mut manifest: release_capsule_attestation_seal::ReleaseCapsuleDigestManifest =
            load_json(&paths.release_capsule_digest_manifest_path).unwrap();
        manifest.entries.pop();
        persist_json(&paths.release_capsule_digest_manifest_path, &manifest).unwrap();

        let verify = verify_live_package_attestation_seal_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageAttestationSealVerdict::TinyLivePackageAttestationSealVerifyInvalid
        );
    }

    struct FakeCommandFixture {
        fixture_dir: PathBuf,
        bin_dir: PathBuf,
        release_capsule_session_dir: PathBuf,
        attestation_seal_session_dir: PathBuf,
        decision_packet_session_dir: PathBuf,
        install_root: PathBuf,
    }

    impl FakeCommandFixture {
        fn run_config(&self) -> Config {
            Config {
                mode: Mode::RunLivePackageAttestationSeal,
                release_capsule_session_dir: self.release_capsule_session_dir.clone(),
                confirmed_decision_packet_session_dir: Some(
                    self.decision_packet_session_dir.clone(),
                ),
                session_dir: Some(self.attestation_seal_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn verify_config(&self) -> Config {
            Config {
                mode: Mode::VerifyLivePackageAttestationSeal,
                release_capsule_session_dir: self.release_capsule_session_dir.clone(),
                confirmed_decision_packet_session_dir: Some(
                    self.decision_packet_session_dir.clone(),
                ),
                session_dir: Some(self.attestation_seal_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }
    }

    fn fake_command_fixture(label: &str, verify_report: serde_json::Value) -> FakeCommandFixture {
        let fixture_dir = temp_dir(label);
        let bin_dir = fixture_dir.join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        let release_capsule_session_dir = fixture_dir.join("release-capsule-session");
        let attestation_seal_session_dir = fixture_dir.join("attestation-seal-session");
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
            &release_capsule_session_dir.join("tiny_live_activation_package_release_capsule.session.json"),
            &json!({
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
                "verify_activation_ticket_command_summary": format!(
                    "verify immutable activation-ticket session under {} before freezing the final hash-locked release capsule",
                    activation_ticket_session_dir.display()
                ),
                "reviewed_frozen_live_cutover_controller_command_summary": format!(
                    "copybot_tiny_live_activation_package_live_cutover --package-dir {} --install-root {} --target-service solana-copy-bot.service --backend-command {} --wrapper-timeout-ms 1200 --service-status-max-staleness-ms 4500 --session-dir {} --run-live-package-cutover",
                    shell_escape_path(&package_dir),
                    shell_escape_path(&install_root),
                    shell_escape_path(&backend_command),
                    shell_escape_path(&fixture_dir.join("frozen-live-cutover-session")),
                ),
                "release_capsule_summary": "Release capsule summary.",
                "audit_manifest_summary": "Hash-locked audit manifest: sha256 digests over 3 archival members; any content drift invalidates this release capsule",
                "digest_algorithm": "sha256",
                "explicit_statement": "release-capsule statement"
            }),
        )
        .unwrap();
        persist_json(
            &release_capsule_session_dir.join("tiny_live_activation_package_release_capsule.status.json"),
            &json!({
                "result": verify_report.get("result").and_then(|value| value.as_str()).unwrap(),
                "handoff_bundle_result": verify_report.get("handoff_bundle_result").and_then(|value| value.as_str()).unwrap(),
                "decision_packet_result": verify_report.get("decision_packet_result").and_then(|value| value.as_str()).unwrap(),
                "execute_frozen_result": verify_report.get("execute_frozen_result").and_then(|value| value.as_str()).unwrap(),
                "current_pre_activation_gate_verdict": verify_report.get("current_pre_activation_gate_verdict").and_then(|value| value.as_str()),
                "current_pre_activation_gate_reason": verify_report.get("current_pre_activation_gate_reason").and_then(|value| value.as_str()),
                "release_capsule_summary": "Release capsule summary.",
                "audit_manifest_summary": "Hash-locked audit manifest: sha256 digests over 3 archival members; any content drift invalidates this release capsule",
                "digest_algorithm": "sha256",
                "explicit_statement": "release-capsule status statement"
            }),
        )
        .unwrap();
        persist_json(
            &release_capsule_session_dir
                .join("tiny_live_activation_package_release_capsule.activation_ticket.report.json"),
            &json!({
                "decision_packet_session_dir": decision_packet_session_dir.display().to_string()
            }),
        )
        .unwrap();
        persist_json(
            &release_capsule_session_dir
                .join("tiny_live_activation_package_release_capsule.digest_manifest.json"),
            &json!({
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
            }),
        )
        .unwrap();

        let mut verify_report = verify_report;
        bind_release_capsule_report_to_fixture(
            &mut verify_report,
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
        let verify_path = fixture_dir.join("release-capsule.verify.json");
        persist_json(&verify_path, &verify_report).unwrap();
        write_fake_release_capsule_verify_command(
            &bin_dir.join("copybot_tiny_live_activation_package_release_capsule"),
            &verify_path,
            &activation_ticket_session_dir,
            &decision_packet_session_dir,
            &release_capsule_session_dir,
        );

        FakeCommandFixture {
            fixture_dir,
            bin_dir,
            release_capsule_session_dir,
            attestation_seal_session_dir,
            decision_packet_session_dir,
            install_root,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn bind_release_capsule_report_to_fixture(
        report: &mut serde_json::Value,
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
        let object = report
            .as_object_mut()
            .expect("release-capsule report fixture must be an object");
        object.insert(
            "mode".to_string(),
            json!("verify_live_package_release_capsule"),
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
            "session_dir".to_string(),
            json!(release_capsule_session_dir.display().to_string()),
        );
        object.insert(
            "verify_activation_ticket_command_summary".to_string(),
            json!(format!(
                "verify immutable activation-ticket session under {} before freezing the final hash-locked release capsule",
                activation_ticket_session_dir.display()
            )),
        );
        object.insert(
            "reviewed_frozen_live_cutover_controller_command_summary".to_string(),
            json!(format!(
                "copybot_tiny_live_activation_package_live_cutover --package-dir {} --install-root {} --target-service solana-copy-bot.service --backend-command {} --wrapper-timeout-ms 1200 --service-status-max-staleness-ms 4500 --session-dir {} --run-live-package-cutover",
                shell_escape_path(package_dir),
                shell_escape_path(install_root),
                shell_escape_path(backend_command),
                shell_escape_path(&review_receipt_session_dir.join("frozen-live-cutover-session")),
            )),
        );
        object.insert(
            "release_capsule_summary".to_string(),
            json!("Release capsule summary."),
        );
        object.insert(
            "audit_manifest_summary".to_string(),
            json!("Hash-locked audit manifest: sha256 digests over 3 archival members; any content drift invalidates this release capsule"),
        );
        object.insert("digest_algorithm".to_string(), json!("sha256"));
        object.insert(
            "digest_manifest".to_string(),
            json!({
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
            }),
        );
        object.insert(
            "explicit_statement".to_string(),
            json!("release-capsule verify statement"),
        );
    }

    fn release_capsule_verify_report(
        result: &str,
        handoff_bundle_result: &str,
        decision_packet_result: &str,
        execute_frozen_result: &str,
        activation_ticket_result: &str,
        gate_verdict: &str,
        gate_reason: &str,
    ) -> serde_json::Value {
        let release_capsule_result = if result == "ready_for_manual_execution_when_gate_turns_green"
        {
            "ready_for_manual_execution_when_gate_turns_green"
        } else {
            result
        };
        let _ = activation_ticket_result;
        json!({
            "generated_at": Utc::now(),
            "mode": "verify_live_package_release_capsule",
            "verdict": "tiny_live_package_release_capsule_verify_ok",
            "reason": "release-capsule chain is coherent",
            "result": release_capsule_result,
            "handoff_bundle_result": handoff_bundle_result,
            "decision_packet_result": decision_packet_result,
            "execute_frozen_result": execute_frozen_result,
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason,
        })
    }

    fn write_fake_release_capsule_verify_command(
        path: &Path,
        verify_path: &Path,
        expected_activation_ticket_session_dir: &Path,
        expected_decision_packet_session_dir: &Path,
        release_capsule_session_dir: &Path,
    ) {
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --activation-ticket-session-dir {} \"*) : ;;\n  *) echo 'unexpected activation-ticket-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --confirm-decision-packet-session-dir {} \"*) : ;;\n  *) echo 'unexpected decision-packet-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected release-capsule session dir' >&2; exit 1;;\nesac\ncat '{}'\n",
            shell_escape_path(expected_activation_ticket_session_dir),
            shell_escape_path(expected_decision_packet_session_dir),
            shell_escape_path(release_capsule_session_dir),
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
