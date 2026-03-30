use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_app::tiny_live_activation::install_target_managed_surface::validate_turn_green_session_dir as validate_managed_surface_session_dir;
use copybot_app::tiny_live_activation::package_activation_ticket_release_capsule as activation_ticket_release_capsule;
use copybot_app::tiny_live_activation::package_review_receipt_activation_ticket as review_receipt_activation_ticket;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: copybot_tiny_live_activation_package_release_capsule --activation-ticket-session-dir <path> [--confirm-decision-packet-session-dir <path>] [--session-dir <path>] [--output <path>] [--json] (--plan-live-package-release-capsule | --render-live-package-release-capsule | --run-live-package-release-capsule | --verify-live-package-release-capsule)";
const RELEASE_CAPSULE_SESSION_VERSION: &str = "1";
const RELEASE_CAPSULE_STATUS_VERSION: &str = "1";
const RELEASE_CAPSULE_DIGEST_ALGORITHM: &str = "sha256";
const RELEASE_CAPSULE_SESSION_EXPLICIT_STATEMENT: &str =
    "the verified activation-ticket session is the primary input here; this immutable release capsule freezes one final hash-locked audit manifest over the exact reviewed frozen live cutover contract without enabling production execution";
const RELEASE_CAPSULE_STATUS_EXPLICIT_STATEMENT: &str =
    "this release capsule is archival and read-only; it never executes or enables the frozen controller";
const RELEASE_CAPSULE_VERIFY_EXPLICIT_STATEMENT: &str =
    "this verification binds one immutable release capsule to verified activation-ticket truth and an exact hash-locked archival chain";

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
    activation_ticket_session_dir: PathBuf,
    confirmed_decision_packet_session_dir: Option<PathBuf>,
    session_dir: Option<PathBuf>,
    output_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanLivePackageReleaseCapsule,
    RenderLivePackageReleaseCapsule,
    RunLivePackageReleaseCapsule,
    VerifyLivePackageReleaseCapsule,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageReleaseCapsuleVerdict {
    TinyLivePackageReleaseCapsulePlanReady,
    TinyLivePackageReleaseCapsuleRendered,
    TinyLivePackageReleaseCapsuleRefusedNowByStage3,
    TinyLivePackageReleaseCapsuleRefusedNowByPreActivationGate,
    TinyLivePackageReleaseCapsuleRefusedNowByInvalidOrDriftedContract,
    TinyLivePackageReleaseCapsuleReadyForManualExecutionWhenGateTurnsGreen,
    TinyLivePackageReleaseCapsuleVerifyOk,
    TinyLivePackageReleaseCapsuleVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageReleaseCapsuleResult {
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    RefusedNowByInvalidOrDriftedContract,
    ReadyForManualExecutionWhenGateTurnsGreen,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageReleaseCapsuleStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ReleaseCapsuleDigestEntry {
    label: String,
    path: String,
    sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ReleaseCapsuleDigestManifest {
    digest_algorithm: String,
    entries: Vec<ReleaseCapsuleDigestEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageReleaseCapsuleSession {
    session_version: String,
    planned_at: DateTime<Utc>,
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
    verify_activation_ticket_command_summary: String,
    reviewed_frozen_live_cutover_controller_command_summary: String,
    release_capsule_summary: String,
    audit_manifest_summary: String,
    digest_algorithm: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageReleaseCapsuleStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    activation_ticket_session_dir: String,
    result: LivePackageReleaseCapsuleResult,
    reason: String,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    activation_ticket_step: Option<PackageReleaseCapsuleStepArtifact>,
    release_capsule_summary: String,
    audit_manifest_summary: String,
    digest_algorithm: String,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageReleaseCapsulePaths {
    session_path: PathBuf,
    status_path: PathBuf,
    activation_ticket_report_path: PathBuf,
    digest_manifest_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageReleaseCapsuleReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageReleaseCapsuleVerdict,
    reason: String,
    activation_ticket_session_dir: String,
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
    release_capsule_session_path: Option<String>,
    release_capsule_status_path: Option<String>,
    release_capsule_digest_manifest_path: Option<String>,
    result: Option<String>,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    activation_ticket_step: Option<PackageReleaseCapsuleStepArtifact>,
    verify_activation_ticket_command_summary: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    release_capsule_summary: Option<String>,
    audit_manifest_summary: Option<String>,
    digest_algorithm: Option<String>,
    digest_manifest: Option<ReleaseCapsuleDigestManifest>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    verification_mismatches: Vec<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct NestedActivationTicketReportView {
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
    let mut activation_ticket_session_dir: Option<PathBuf> = None;
    let mut confirmed_decision_packet_session_dir: Option<PathBuf> = None;
    let mut session_dir: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut json = false;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--activation-ticket-session-dir" => {
                activation_ticket_session_dir = Some(PathBuf::from(parse_string_arg(
                    "--activation-ticket-session-dir",
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
            "--plan-live-package-release-capsule" => {
                set_mode(&mut mode, Mode::PlanLivePackageReleaseCapsule, arg.as_str())?
            }
            "--render-live-package-release-capsule" => set_mode(
                &mut mode,
                Mode::RenderLivePackageReleaseCapsule,
                arg.as_str(),
            )?,
            "--run-live-package-release-capsule" => {
                set_mode(&mut mode, Mode::RunLivePackageReleaseCapsule, arg.as_str())?
            }
            "--verify-live-package-release-capsule" => set_mode(
                &mut mode,
                Mode::VerifyLivePackageReleaseCapsule,
                arg.as_str(),
            )?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let Some(mode) = mode else {
        return Ok(None);
    };
    if matches!(mode, Mode::RenderLivePackageReleaseCapsule) && output_path.is_none() {
        bail!("missing --output for --render-live-package-release-capsule");
    }
    if matches!(
        mode,
        Mode::RunLivePackageReleaseCapsule | Mode::VerifyLivePackageReleaseCapsule
    ) && session_dir.is_none()
    {
        bail!("missing --session-dir for run/verify mode");
    }
    if matches!(
        mode,
        Mode::RunLivePackageReleaseCapsule | Mode::VerifyLivePackageReleaseCapsule
    ) && confirmed_decision_packet_session_dir.is_none()
    {
        bail!("missing --confirm-decision-packet-session-dir for run/verify mode");
    }
    let activation_ticket_session_dir = activation_ticket_session_dir
        .ok_or_else(|| anyhow!("missing --activation-ticket-session-dir"))?;
    Ok(Some(Config {
        mode,
        activation_ticket_session_dir,
        confirmed_decision_packet_session_dir,
        session_dir,
        output_path,
        json,
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanLivePackageReleaseCapsule => plan_live_package_release_capsule_report(&config)?,
        Mode::RenderLivePackageReleaseCapsule => {
            render_live_package_release_capsule_report(&config)?
        }
        Mode::RunLivePackageReleaseCapsule => run_live_package_release_capsule_report(&config)?,
        Mode::VerifyLivePackageReleaseCapsule => {
            verify_live_package_release_capsule_report(&config)?
        }
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_live_package_release_capsule_report(
    config: &Config,
) -> Result<PackageReleaseCapsuleReport> {
    let (activation_ticket, classification) =
        match config.confirmed_decision_packet_session_dir.as_deref() {
            Some(confirmed_decision_packet_session_dir) => {
                match activation_ticket_release_capsule::verify_live_package_activation_ticket_for_release_capsule(
                    &config.activation_ticket_session_dir,
                    confirmed_decision_packet_session_dir,
                ) {
                    Ok(verified) => {
                        let classification = classify_verified_activation_ticket(&verified);
                        (verified.contract, classification)
                    }
                    Err(_) => {
                        let raw_contract =
                            activation_ticket_release_capsule::load_live_package_activation_ticket_contract_for_release_capsule(
                                &config.activation_ticket_session_dir,
                            )?;
                        let classification = classify_release_capsule_contract(&raw_contract, false);
                        (raw_contract, classification)
                    }
                }
            }
            None => {
                let raw_contract =
                    activation_ticket_release_capsule::load_live_package_activation_ticket_contract_for_release_capsule(
                        &config.activation_ticket_session_dir,
                    )?;
                let classification = classify_release_capsule_contract(&raw_contract, false);
                (raw_contract, classification)
            }
        };
    let session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| config.activation_ticket_session_dir.join("release-capsule"));
    let digest_manifest_summary = digest_manifest_summary(9);

    Ok(PackageReleaseCapsuleReport {
        generated_at: Utc::now(),
        mode: "plan_live_package_release_capsule".to_string(),
        verdict: TinyLivePackageReleaseCapsuleVerdict::TinyLivePackageReleaseCapsulePlanReady,
        reason: format!(
            "activation-ticket-native release capsule is explicit for verified activation-ticket session {}; this command stays archival and read-only while freezing one hash-locked audit manifest",
            config.activation_ticket_session_dir.display()
        ),
        activation_ticket_session_dir: config.activation_ticket_session_dir.display().to_string(),
        review_receipt_session_dir: Some(
            activation_ticket
                .review_receipt_session_dir
                .display()
                .to_string(),
        ),
        handoff_bundle_session_dir: Some(
            activation_ticket
                .handoff_bundle_session_dir
                .display()
                .to_string(),
        ),
        decision_packet_session_dir: Some(
            activation_ticket
                .decision_packet_session_dir
                .display()
                .to_string(),
        ),
        execute_frozen_session_dir: Some(
            activation_ticket
                .execute_frozen_session_dir
                .display()
                .to_string(),
        ),
        turn_green_session_dir: Some(
            activation_ticket.turn_green_session_dir.display().to_string(),
        ),
        launch_packet_session_dir: Some(
            activation_ticket
                .launch_packet_session_dir
                .display()
                .to_string(),
        ),
        package_dir: Some(activation_ticket.package_dir.display().to_string()),
        install_root: Some(activation_ticket.install_root.display().to_string()),
        target_service_name: Some(activation_ticket.target_service_name.clone()),
        backend_command: Some(activation_ticket.backend_command.clone()),
        wrapper_timeout_ms: Some(activation_ticket.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(
            activation_ticket.service_status_max_staleness_ms,
        ),
        session_dir: Some(session_dir.display().to_string()),
        release_capsule_session_path: None,
        release_capsule_status_path: None,
        release_capsule_digest_manifest_path: None,
        result: Some(serialize_enum(&classification.0)),
        handoff_bundle_result: activation_ticket.handoff_bundle_result.clone(),
        decision_packet_result: activation_ticket.decision_packet_result.clone(),
        execute_frozen_result: activation_ticket.execute_frozen_result.clone(),
        activation_ticket_step: None,
        verify_activation_ticket_command_summary: Some(verify_activation_ticket_command_summary(
            &config.activation_ticket_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            activation_ticket
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        release_capsule_summary: Some(release_capsule_summary(
            classification.0,
            &activation_ticket
                .reviewed_frozen_live_cutover_controller_command_summary,
        )),
        audit_manifest_summary: Some(digest_manifest_summary),
        digest_algorithm: Some(RELEASE_CAPSULE_DIGEST_ALGORITHM.to_string()),
        digest_manifest: None,
        current_pre_activation_gate_verdict: activation_ticket.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: activation_ticket.current_pre_activation_gate_reason,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this final release capsule freezes verified activation-ticket truth and one hash-locked audit manifest without enabling production execution"
                .to_string(),
    })
}

fn render_live_package_release_capsule_report(
    config: &Config,
) -> Result<PackageReleaseCapsuleReport> {
    let plan = plan_live_package_release_capsule_report(config)?;
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
        "#!/usr/bin/env bash\nset -euo pipefail\n\ncopybot_tiny_live_activation_package_release_capsule --activation-ticket-session-dir {} --plan-live-package-release-capsule\ncopybot_tiny_live_activation_package_release_capsule --activation-ticket-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --run-live-package-release-capsule\ncopybot_tiny_live_activation_package_release_capsule --activation-ticket-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --verify-live-package-release-capsule\n",
        shell_escape_path(&config.activation_ticket_session_dir),
        shell_escape_path(&config.activation_ticket_session_dir),
        shell_escape_path(&confirmed_decision_packet_session_dir),
        shell_escape_path(&session_dir),
        shell_escape_path(&config.activation_ticket_session_dir),
        shell_escape_path(&confirmed_decision_packet_session_dir),
        shell_escape_path(&session_dir),
    );
    write_script(output_path, &script)?;
    Ok(PackageReleaseCapsuleReport {
        generated_at: Utc::now(),
        mode: "render_live_package_release_capsule".to_string(),
        verdict: TinyLivePackageReleaseCapsuleVerdict::TinyLivePackageReleaseCapsuleRendered,
        reason: format!(
            "rendered activation-ticket-native release-capsule script to {}",
            output_path.display()
        ),
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
        release_capsule_session_path: None,
        release_capsule_status_path: None,
        release_capsule_digest_manifest_path: None,
        result: plan.result,
        handoff_bundle_result: plan.handoff_bundle_result,
        decision_packet_result: plan.decision_packet_result,
        execute_frozen_result: plan.execute_frozen_result,
        activation_ticket_step: None,
        verify_activation_ticket_command_summary: plan.verify_activation_ticket_command_summary,
        reviewed_frozen_live_cutover_controller_command_summary: plan
            .reviewed_frozen_live_cutover_controller_command_summary,
        release_capsule_summary: plan.release_capsule_summary,
        audit_manifest_summary: plan.audit_manifest_summary,
        digest_algorithm: plan.digest_algorithm,
        digest_manifest: None,
        current_pre_activation_gate_verdict: plan.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: plan.current_pre_activation_gate_reason,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this rendered script accepts the verified activation-ticket session as the primary input and stays archival and read-only with respect to the real target"
                .to_string(),
    })
}

fn run_live_package_release_capsule_report(config: &Config) -> Result<PackageReleaseCapsuleReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for run mode"))?;
    let verified_activation_ticket = match activation_ticket_release_capsule::verify_live_package_activation_ticket_for_release_capsule(
        &config.activation_ticket_session_dir,
        config
            .confirmed_decision_packet_session_dir
            .as_deref()
            .ok_or_else(|| anyhow!("missing --confirm-decision-packet-session-dir for run mode"))?,
    ) {
        Ok(value) => value,
        Err(error) => {
            return Ok(failure_report_without_contract(
                config,
                TinyLivePackageReleaseCapsuleVerdict::TinyLivePackageReleaseCapsuleRefusedNowByInvalidOrDriftedContract,
                format!(
                    "failed to verify activation-ticket session under {}: {error}",
                    config.activation_ticket_session_dir.display()
                ),
            ))
        }
    };

    validate_managed_surface_session_dir(&verified_activation_ticket.contract.install_root, session_dir)
        .with_context(|| {
            format!(
                "release-capsule session dir {} overlaps the managed live target surface derived from verified activation-ticket install_root {}",
                session_dir.display(),
                verified_activation_ticket.contract.install_root.display()
            )
        })?;
    ensure_clean_release_capsule_session_dir(session_dir)?;
    let paths = release_capsule_paths(session_dir);
    let classification = classify_verified_activation_ticket(&verified_activation_ticket);
    let session = planned_session(
        config,
        session_dir,
        &verified_activation_ticket.contract,
        classification.0,
    );
    persist_json(&paths.session_path, &session)?;
    persist_json(
        &paths.activation_ticket_report_path,
        &verified_activation_ticket.report_json,
    )?;
    let activation_ticket_step = Some(step_artifact(
        &paths.activation_ticket_report_path,
        "verify_live_package_activation_ticket",
        &verified_activation_ticket.verdict,
        &verified_activation_ticket.reason,
        verified_activation_ticket.generated_at,
    ));
    let status = PackageReleaseCapsuleStatus {
        status_version: RELEASE_CAPSULE_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        activation_ticket_session_dir: config.activation_ticket_session_dir.display().to_string(),
        result: classification.0,
        reason: classification.2.clone(),
        handoff_bundle_result: verified_activation_ticket
            .contract
            .handoff_bundle_result
            .clone(),
        decision_packet_result: verified_activation_ticket
            .contract
            .decision_packet_result
            .clone(),
        execute_frozen_result: verified_activation_ticket
            .contract
            .execute_frozen_result
            .clone(),
        current_pre_activation_gate_verdict: verified_activation_ticket
            .contract
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: verified_activation_ticket
            .contract
            .current_pre_activation_gate_reason
            .clone(),
        activation_ticket_step: activation_ticket_step.clone(),
        release_capsule_summary: release_capsule_summary(
            classification.0,
            &verified_activation_ticket
                .contract
                .reviewed_frozen_live_cutover_controller_command_summary,
        ),
        audit_manifest_summary: digest_manifest_summary(9),
        digest_algorithm: RELEASE_CAPSULE_DIGEST_ALGORITHM.to_string(),
        explicit_statement: RELEASE_CAPSULE_STATUS_EXPLICIT_STATEMENT.to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    let digest_manifest = build_digest_manifest(&paths, &verified_activation_ticket.contract)?;
    persist_json(&paths.digest_manifest_path, &digest_manifest)?;
    Ok(report_from_status(
        config,
        session_dir,
        &paths,
        &verified_activation_ticket.contract,
        classification.1,
        &status,
        digest_manifest,
    ))
}

fn verify_live_package_release_capsule_report(
    config: &Config,
) -> Result<PackageReleaseCapsuleReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = release_capsule_paths(session_dir);
    let session: PackageReleaseCapsuleSession = load_json(&paths.session_path)?;
    let status: PackageReleaseCapsuleStatus = load_json(&paths.status_path)?;
    let mut mismatches = Vec::new();

    compare_string(
        &session.activation_ticket_session_dir,
        &config.activation_ticket_session_dir.display().to_string(),
        "release-capsule activation_ticket_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "release-capsule session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "release-capsule status session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.activation_ticket_session_dir,
        &config.activation_ticket_session_dir.display().to_string(),
        "release-capsule status activation_ticket_session_dir",
        &mut mismatches,
    );

    let verified_activation_ticket =
        match activation_ticket_release_capsule::verify_live_package_activation_ticket_for_release_capsule(
            &config.activation_ticket_session_dir,
            config
                .confirmed_decision_packet_session_dir
                .as_deref()
                .ok_or_else(|| anyhow!("missing --confirm-decision-packet-session-dir for verify mode"))?,
        ) {
            Ok(value) => value,
            Err(error) => {
                let raw_contract =
                    activation_ticket_release_capsule::load_live_package_activation_ticket_contract_for_release_capsule(
                        &config.activation_ticket_session_dir,
                    )?;
                return Ok(failure_report_for_verify(
                    config,
                    session_dir,
                    &raw_contract,
                    vec![format!(
                        "failed to verify activation-ticket session under {}: {error}",
                        config.activation_ticket_session_dir.display()
                    )],
                ));
            }
        };

    let stored_activation_ticket_report: serde_json::Value = load_required_step_json(
        &status.activation_ticket_step,
        &paths.activation_ticket_report_path,
        "activation_ticket_step",
        &mut mismatches,
    )?;
    let stored_activation_ticket_report_view: NestedActivationTicketReportView =
        serde_json::from_value(stored_activation_ticket_report.clone()).with_context(|| {
            format!(
                "failed parsing archived nested activation-ticket report {}",
                paths.activation_ticket_report_path.display()
            )
        })?;
    let verified_activation_ticket_report_view: NestedActivationTicketReportView =
        serde_json::from_value(verified_activation_ticket.report_json.clone()).with_context(
            || {
                format!(
                    "failed parsing freshly verified nested activation-ticket report for {}",
                    config.activation_ticket_session_dir.display()
                )
            },
        )?;
    compare_json_ignoring_generated_at(
        &stored_activation_ticket_report,
        &verified_activation_ticket.report_json,
        "release-capsule archived activation-ticket report json",
        &mut mismatches,
    );
    compare_string(
        &stored_activation_ticket_report_view.mode,
        &verified_activation_ticket_report_view.mode,
        "release-capsule archived activation-ticket report mode",
        &mut mismatches,
    );
    compare_string(
        &stored_activation_ticket_report_view.verdict,
        &verified_activation_ticket_report_view.verdict,
        "release-capsule archived activation-ticket report verdict",
        &mut mismatches,
    );
    compare_string(
        &stored_activation_ticket_report_view.reason,
        &verified_activation_ticket_report_view.reason,
        "release-capsule archived activation-ticket report reason",
        &mut mismatches,
    );
    if stored_activation_ticket_report_view.generated_at
        != verified_activation_ticket_report_view.generated_at
    {
        mismatches.push(format!(
            "release-capsule archived activation-ticket report generated_at {:?} does not match freshly verified nested truth {:?}",
            stored_activation_ticket_report_view.generated_at,
            verified_activation_ticket_report_view.generated_at
        ));
    }
    let expected_activation_ticket_step = step_artifact(
        &paths.activation_ticket_report_path,
        &verified_activation_ticket_report_view.mode,
        &verified_activation_ticket_report_view.verdict,
        &verified_activation_ticket_report_view.reason,
        verified_activation_ticket_report_view.generated_at,
    );
    compare_step_artifact(
        status.activation_ticket_step.as_ref(),
        &expected_activation_ticket_step,
        "release-capsule activation_ticket_step",
        &mut mismatches,
    );
    if verified_activation_ticket.verdict != "tiny_live_package_activation_ticket_verify_ok" {
        mismatches.push(format!(
            "nested activation-ticket verification is non-green: {}",
            verified_activation_ticket.reason
        ));
    }

    compare_string(
        &session.review_receipt_session_dir,
        &verified_activation_ticket
            .contract
            .review_receipt_session_dir
            .display()
            .to_string(),
        "release-capsule review_receipt_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.handoff_bundle_session_dir,
        &verified_activation_ticket
            .contract
            .handoff_bundle_session_dir
            .display()
            .to_string(),
        "release-capsule handoff_bundle_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.decision_packet_session_dir,
        &verified_activation_ticket
            .contract
            .decision_packet_session_dir
            .display()
            .to_string(),
        "release-capsule decision_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.execute_frozen_session_dir,
        &verified_activation_ticket
            .contract
            .execute_frozen_session_dir
            .display()
            .to_string(),
        "release-capsule execute_frozen_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.turn_green_session_dir,
        &verified_activation_ticket
            .contract
            .turn_green_session_dir
            .display()
            .to_string(),
        "release-capsule turn_green_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.launch_packet_session_dir,
        &verified_activation_ticket
            .contract
            .launch_packet_session_dir
            .display()
            .to_string(),
        "release-capsule launch_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.package_dir,
        &verified_activation_ticket
            .contract
            .package_dir
            .display()
            .to_string(),
        "release-capsule package_dir",
        &mut mismatches,
    );
    compare_string(
        &session.install_root,
        &verified_activation_ticket
            .contract
            .install_root
            .display()
            .to_string(),
        "release-capsule install_root",
        &mut mismatches,
    );
    compare_string(
        &session.target_service_name,
        &verified_activation_ticket.contract.target_service_name,
        "release-capsule target_service_name",
        &mut mismatches,
    );
    compare_string(
        &session.backend_command,
        &verified_activation_ticket.contract.backend_command,
        "release-capsule backend_command",
        &mut mismatches,
    );
    if session.wrapper_timeout_ms != verified_activation_ticket.contract.wrapper_timeout_ms {
        mismatches.push(format!(
            "release-capsule wrapper_timeout_ms {} does not match verified {}",
            session.wrapper_timeout_ms, verified_activation_ticket.contract.wrapper_timeout_ms
        ));
    }
    if session.service_status_max_staleness_ms
        != verified_activation_ticket
            .contract
            .service_status_max_staleness_ms
    {
        mismatches.push(format!(
            "release-capsule service_status_max_staleness_ms {} does not match verified {}",
            session.service_status_max_staleness_ms,
            verified_activation_ticket
                .contract
                .service_status_max_staleness_ms
        ));
    }
    compare_string(
        &session.verify_activation_ticket_command_summary,
        &verify_activation_ticket_command_summary(&config.activation_ticket_session_dir),
        "release-capsule verify_activation_ticket_command_summary",
        &mut mismatches,
    );
    compare_string(
        &session.reviewed_frozen_live_cutover_controller_command_summary,
        &verified_activation_ticket
            .contract
            .reviewed_frozen_live_cutover_controller_command_summary,
        "release-capsule reviewed_frozen_live_cutover_controller_command_summary",
        &mut mismatches,
    );
    compare_string(
        &session.digest_algorithm,
        RELEASE_CAPSULE_DIGEST_ALGORITHM,
        "release-capsule session digest_algorithm",
        &mut mismatches,
    );
    compare_string(
        &status.digest_algorithm,
        RELEASE_CAPSULE_DIGEST_ALGORITHM,
        "release-capsule status digest_algorithm",
        &mut mismatches,
    );

    let classification = classify_verified_activation_ticket(&verified_activation_ticket);
    let expected_release_capsule_summary = release_capsule_summary(
        classification.0,
        &verified_activation_ticket
            .contract
            .reviewed_frozen_live_cutover_controller_command_summary,
    );
    let expected_audit_manifest_summary = digest_manifest_summary(9);
    compare_string(
        &session.release_capsule_summary,
        &expected_release_capsule_summary,
        "release-capsule session release_capsule_summary",
        &mut mismatches,
    );
    compare_string(
        &status.release_capsule_summary,
        &expected_release_capsule_summary,
        "release-capsule status release_capsule_summary",
        &mut mismatches,
    );
    compare_string(
        &session.audit_manifest_summary,
        &expected_audit_manifest_summary,
        "release-capsule session audit_manifest_summary",
        &mut mismatches,
    );
    compare_string(
        &status.audit_manifest_summary,
        &expected_audit_manifest_summary,
        "release-capsule status audit_manifest_summary",
        &mut mismatches,
    );
    compare_string(
        &session.explicit_statement,
        RELEASE_CAPSULE_SESSION_EXPLICIT_STATEMENT,
        "release-capsule session explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &status.explicit_statement,
        RELEASE_CAPSULE_STATUS_EXPLICIT_STATEMENT,
        "release-capsule status explicit_statement",
        &mut mismatches,
    );
    if status.result != classification.0 {
        mismatches.push(format!(
            "stored release-capsule result {} does not match verified {}",
            serialize_enum(&status.result),
            serialize_enum(&classification.0)
        ));
    }
    if status.handoff_bundle_result != verified_activation_ticket.contract.handoff_bundle_result {
        mismatches.push(format!(
            "stored handoff_bundle_result {:?} does not match verified {:?}",
            status.handoff_bundle_result, verified_activation_ticket.contract.handoff_bundle_result
        ));
    }
    if status.decision_packet_result != verified_activation_ticket.contract.decision_packet_result {
        mismatches.push(format!(
            "stored decision_packet_result {:?} does not match verified {:?}",
            status.decision_packet_result,
            verified_activation_ticket.contract.decision_packet_result
        ));
    }
    if status.execute_frozen_result != verified_activation_ticket.contract.execute_frozen_result {
        mismatches.push(format!(
            "stored execute_frozen_result {:?} does not match verified {:?}",
            status.execute_frozen_result, verified_activation_ticket.contract.execute_frozen_result
        ));
    }
    if status.current_pre_activation_gate_verdict
        != verified_activation_ticket
            .contract
            .current_pre_activation_gate_verdict
    {
        mismatches.push(format!(
            "stored current_pre_activation_gate_verdict {:?} does not match verified {:?}",
            status.current_pre_activation_gate_verdict,
            verified_activation_ticket
                .contract
                .current_pre_activation_gate_verdict
        ));
    }
    if status.current_pre_activation_gate_reason
        != verified_activation_ticket
            .contract
            .current_pre_activation_gate_reason
    {
        mismatches.push(format!(
            "stored current_pre_activation_gate_reason {:?} does not match verified {:?}",
            status.current_pre_activation_gate_reason,
            verified_activation_ticket
                .contract
                .current_pre_activation_gate_reason
        ));
    }

    let stored_manifest: ReleaseCapsuleDigestManifest = load_json(&paths.digest_manifest_path)?;
    let expected_manifest = build_digest_manifest(&paths, &verified_activation_ticket.contract)?;
    if stored_manifest != expected_manifest {
        mismatches.push(
            "release-capsule digest manifest does not match the expected archival member set or digests"
                .to_string(),
        );
    }

    let verdict = if mismatches.is_empty() {
        TinyLivePackageReleaseCapsuleVerdict::TinyLivePackageReleaseCapsuleVerifyOk
    } else {
        TinyLivePackageReleaseCapsuleVerdict::TinyLivePackageReleaseCapsuleVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "activation-ticket-native release capsule under {} is coherent and correctly binds the final hash-locked release record to verified activation-ticket truth",
            session_dir.display()
        )
    } else {
        mismatches.first().cloned().unwrap_or_else(|| {
            "activation-ticket-native release capsule verification failed".to_string()
        })
    };

    Ok(PackageReleaseCapsuleReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_release_capsule".to_string(),
        verdict,
        reason,
        activation_ticket_session_dir: config.activation_ticket_session_dir.display().to_string(),
        review_receipt_session_dir: Some(
            verified_activation_ticket
                .contract
                .review_receipt_session_dir
                .display()
                .to_string(),
        ),
        handoff_bundle_session_dir: Some(
            verified_activation_ticket
                .contract
                .handoff_bundle_session_dir
                .display()
                .to_string(),
        ),
        decision_packet_session_dir: Some(
            verified_activation_ticket
                .contract
                .decision_packet_session_dir
                .display()
                .to_string(),
        ),
        execute_frozen_session_dir: Some(
            verified_activation_ticket
                .contract
                .execute_frozen_session_dir
                .display()
                .to_string(),
        ),
        turn_green_session_dir: Some(
            verified_activation_ticket
                .contract
                .turn_green_session_dir
                .display()
                .to_string(),
        ),
        launch_packet_session_dir: Some(
            verified_activation_ticket
                .contract
                .launch_packet_session_dir
                .display()
                .to_string(),
        ),
        package_dir: Some(
            verified_activation_ticket
                .contract
                .package_dir
                .display()
                .to_string(),
        ),
        install_root: Some(
            verified_activation_ticket
                .contract
                .install_root
                .display()
                .to_string(),
        ),
        target_service_name: Some(
            verified_activation_ticket
                .contract
                .target_service_name
                .clone(),
        ),
        backend_command: Some(verified_activation_ticket.contract.backend_command.clone()),
        wrapper_timeout_ms: Some(verified_activation_ticket.contract.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(
            verified_activation_ticket
                .contract
                .service_status_max_staleness_ms,
        ),
        session_dir: Some(session_dir.display().to_string()),
        release_capsule_session_path: Some(paths.session_path.display().to_string()),
        release_capsule_status_path: Some(paths.status_path.display().to_string()),
        release_capsule_digest_manifest_path: Some(
            paths.digest_manifest_path.display().to_string(),
        ),
        result: Some(serialize_enum(&status.result)),
        handoff_bundle_result: status.handoff_bundle_result.clone(),
        decision_packet_result: status.decision_packet_result.clone(),
        execute_frozen_result: status.execute_frozen_result.clone(),
        activation_ticket_step: Some(expected_activation_ticket_step),
        verify_activation_ticket_command_summary: Some(verify_activation_ticket_command_summary(
            &config.activation_ticket_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            verified_activation_ticket
                .contract
                .reviewed_frozen_live_cutover_controller_command_summary,
        ),
        release_capsule_summary: Some(expected_release_capsule_summary),
        audit_manifest_summary: Some(expected_audit_manifest_summary),
        digest_algorithm: Some(RELEASE_CAPSULE_DIGEST_ALGORITHM.to_string()),
        digest_manifest: Some(expected_manifest),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        verification_mismatches: mismatches,
        explicit_statement: RELEASE_CAPSULE_VERIFY_EXPLICIT_STATEMENT.to_string(),
    })
}

fn verify_activation_ticket_command_summary(activation_ticket_session_dir: &Path) -> String {
    format!(
        "verify immutable activation-ticket session under {} before freezing the final hash-locked release capsule",
        activation_ticket_session_dir.display()
    )
}

fn classify_release_capsule_contract(
    contract: &activation_ticket_release_capsule::LivePackageActivationTicketContractView,
    assume_verified: bool,
) -> (
    LivePackageReleaseCapsuleResult,
    TinyLivePackageReleaseCapsuleVerdict,
    String,
) {
    if !assume_verified {
        return (
            LivePackageReleaseCapsuleResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageReleaseCapsuleVerdict::TinyLivePackageReleaseCapsuleRefusedNowByInvalidOrDriftedContract,
            "the activation-ticket chain is not verified, so the release capsule remains refused".to_string(),
        );
    }
    match contract.result.as_deref() {
        Some("refused_now_by_stage3") => (
            LivePackageReleaseCapsuleResult::RefusedNowByStage3,
            TinyLivePackageReleaseCapsuleVerdict::TinyLivePackageReleaseCapsuleRefusedNowByStage3,
            "the immutable activation ticket remains refused now by Stage 3, so the release capsule remains an explicit refusal record".to_string(),
        ),
        Some("refused_now_by_pre_activation_gate") => (
            LivePackageReleaseCapsuleResult::RefusedNowByPreActivationGate,
            TinyLivePackageReleaseCapsuleVerdict::TinyLivePackageReleaseCapsuleRefusedNowByPreActivationGate,
            "the immutable activation ticket remains refused now by the pre-activation gate, so the release capsule remains an explicit refusal record".to_string(),
        ),
        Some("ready_for_manual_execution_when_gate_turns_green") => (
            LivePackageReleaseCapsuleResult::ReadyForManualExecutionWhenGateTurnsGreen,
            TinyLivePackageReleaseCapsuleVerdict::TinyLivePackageReleaseCapsuleReadyForManualExecutionWhenGateTurnsGreen,
            "the immutable activation ticket is coherent and this release capsule is ready for manual execution when gate truth turns green".to_string(),
        ),
        _ => (
            LivePackageReleaseCapsuleResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageReleaseCapsuleVerdict::TinyLivePackageReleaseCapsuleRefusedNowByInvalidOrDriftedContract,
            "the verified activation-ticket chain is invalid, drifted, or non-runnable, so the release capsule remains refused".to_string(),
        ),
    }
}

fn classify_verified_activation_ticket(
    verified: &activation_ticket_release_capsule::VerifiedLivePackageActivationTicketReleaseCapsuleStep,
) -> (
    LivePackageReleaseCapsuleResult,
    TinyLivePackageReleaseCapsuleVerdict,
    String,
) {
    if verified.verdict != "tiny_live_package_activation_ticket_verify_ok" {
        return (
            LivePackageReleaseCapsuleResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageReleaseCapsuleVerdict::TinyLivePackageReleaseCapsuleRefusedNowByInvalidOrDriftedContract,
            format!(
                "nested activation-ticket verification is non-green: {}",
                verified.reason
            ),
        );
    }
    classify_release_capsule_contract(&verified.contract, true)
}

fn release_capsule_summary(
    result: LivePackageReleaseCapsuleResult,
    reviewed_frozen_live_cutover_controller_command_summary: &str,
) -> String {
    match result {
        LivePackageReleaseCapsuleResult::RefusedNowByStage3 => {
            "Release capsule outcome: the immutable reviewed chain remains refused now by Stage 3; archive this capsule as tamper-evident refusal evidence only and do not run the frozen controller.".to_string()
        }
        LivePackageReleaseCapsuleResult::RefusedNowByPreActivationGate => {
            "Release capsule outcome: the immutable reviewed chain remains refused now by the pre-activation gate; archive this capsule as tamper-evident refusal evidence only and do not run the frozen controller.".to_string()
        }
        LivePackageReleaseCapsuleResult::RefusedNowByInvalidOrDriftedContract => {
            "Release capsule outcome: the immutable reviewed chain is invalid or drifted; archive this capsule for audit only and mint a new coherent warrant chain before any later execution record.".to_string()
        }
        LivePackageReleaseCapsuleResult::ReadyForManualExecutionWhenGateTurnsGreen => format!(
            "Release capsule outcome: this immutable release record is ready for manual execution when gate truth turns green; the exact reviewed frozen controller command remains: {reviewed_frozen_live_cutover_controller_command_summary}"
        ),
    }
}

fn digest_manifest_summary(member_count: usize) -> String {
    format!(
        "Hash-locked audit manifest: {} digests over {member_count} archival members; any content drift invalidates this release capsule",
        RELEASE_CAPSULE_DIGEST_ALGORITHM
    )
}

fn release_capsule_paths(session_dir: &Path) -> PackageReleaseCapsulePaths {
    PackageReleaseCapsulePaths {
        session_path: session_dir.join("tiny_live_activation_package_release_capsule.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_release_capsule.status.json"),
        activation_ticket_report_path: session_dir
            .join("tiny_live_activation_package_release_capsule.activation_ticket.report.json"),
        digest_manifest_path: session_dir
            .join("tiny_live_activation_package_release_capsule.digest_manifest.json"),
    }
}

fn release_capsule_manifest_member_specs(
    paths: &PackageReleaseCapsulePaths,
    contract: &activation_ticket_release_capsule::LivePackageActivationTicketContractView,
) -> Vec<(String, PathBuf)> {
    let activation_ticket_paths =
        activation_ticket_release_capsule::activation_ticket_artifact_paths(
            &contract.activation_ticket_session_dir,
        );
    let review_receipt_paths = review_receipt_activation_ticket::review_receipt_artifact_paths(
        &contract.review_receipt_session_dir,
    );
    vec![
        (
            "release_capsule_session".to_string(),
            paths.session_path.clone(),
        ),
        (
            "release_capsule_status".to_string(),
            paths.status_path.clone(),
        ),
        (
            "release_capsule_activation_ticket_report".to_string(),
            paths.activation_ticket_report_path.clone(),
        ),
        (
            "nested_activation_ticket_session".to_string(),
            activation_ticket_paths.session_path,
        ),
        (
            "nested_activation_ticket_status".to_string(),
            activation_ticket_paths.status_path,
        ),
        (
            "nested_activation_ticket_review_receipt_report".to_string(),
            activation_ticket_paths.review_receipt_report_path,
        ),
        (
            "nested_review_receipt_session".to_string(),
            review_receipt_paths.session_path,
        ),
        (
            "nested_review_receipt_status".to_string(),
            review_receipt_paths.status_path,
        ),
        (
            "nested_review_receipt_handoff_bundle_report".to_string(),
            review_receipt_paths.handoff_bundle_report_path,
        ),
    ]
}

fn build_digest_manifest(
    paths: &PackageReleaseCapsulePaths,
    contract: &activation_ticket_release_capsule::LivePackageActivationTicketContractView,
) -> Result<ReleaseCapsuleDigestManifest> {
    let mut entries = Vec::new();
    for (label, path) in release_capsule_manifest_member_specs(paths, contract) {
        entries.push(ReleaseCapsuleDigestEntry {
            label,
            path: path.display().to_string(),
            sha256: compute_sha256_hex(&path)?,
        });
    }
    Ok(ReleaseCapsuleDigestManifest {
        digest_algorithm: RELEASE_CAPSULE_DIGEST_ALGORITHM.to_string(),
        entries,
    })
}

fn compute_sha256_hex(path: &Path) -> Result<String> {
    let bytes = fs::read(path).with_context(|| format!("failed reading {}", path.display()))?;
    let digest = Sha256::digest(bytes);
    Ok(format!("{digest:x}"))
}

fn ensure_clean_release_capsule_session_dir(session_dir: &Path) -> Result<()> {
    if session_dir.exists() {
        bail!(
            "refusing to overwrite existing release-capsule session dir {}",
            session_dir.display()
        );
    }
    fs::create_dir_all(session_dir).with_context(|| {
        format!(
            "failed creating release-capsule session dir {}",
            session_dir.display()
        )
    })
}

fn planned_session(
    config: &Config,
    session_dir: &Path,
    contract: &activation_ticket_release_capsule::LivePackageActivationTicketContractView,
    result: LivePackageReleaseCapsuleResult,
) -> PackageReleaseCapsuleSession {
    PackageReleaseCapsuleSession {
        session_version: RELEASE_CAPSULE_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        activation_ticket_session_dir: config.activation_ticket_session_dir.display().to_string(),
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
        verify_activation_ticket_command_summary: verify_activation_ticket_command_summary(
            &config.activation_ticket_session_dir,
        ),
        reviewed_frozen_live_cutover_controller_command_summary: contract
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        release_capsule_summary: release_capsule_summary(
            result,
            &contract.reviewed_frozen_live_cutover_controller_command_summary,
        ),
        audit_manifest_summary: digest_manifest_summary(9),
        digest_algorithm: RELEASE_CAPSULE_DIGEST_ALGORITHM.to_string(),
        explicit_statement: RELEASE_CAPSULE_SESSION_EXPLICIT_STATEMENT.to_string(),
    }
}

fn report_from_status(
    config: &Config,
    session_dir: &Path,
    paths: &PackageReleaseCapsulePaths,
    contract: &activation_ticket_release_capsule::LivePackageActivationTicketContractView,
    verdict: TinyLivePackageReleaseCapsuleVerdict,
    status: &PackageReleaseCapsuleStatus,
    digest_manifest: ReleaseCapsuleDigestManifest,
) -> PackageReleaseCapsuleReport {
    PackageReleaseCapsuleReport {
        generated_at: Utc::now(),
        mode: "run_live_package_release_capsule".to_string(),
        verdict,
        reason: status.reason.clone(),
        activation_ticket_session_dir: config.activation_ticket_session_dir.display().to_string(),
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
        release_capsule_session_path: Some(paths.session_path.display().to_string()),
        release_capsule_status_path: Some(paths.status_path.display().to_string()),
        release_capsule_digest_manifest_path: Some(
            paths.digest_manifest_path.display().to_string(),
        ),
        result: Some(serialize_enum(&status.result)),
        handoff_bundle_result: status.handoff_bundle_result.clone(),
        decision_packet_result: status.decision_packet_result.clone(),
        execute_frozen_result: status.execute_frozen_result.clone(),
        activation_ticket_step: status.activation_ticket_step.clone(),
        verify_activation_ticket_command_summary: Some(verify_activation_ticket_command_summary(
            &config.activation_ticket_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            contract
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        release_capsule_summary: Some(status.release_capsule_summary.clone()),
        audit_manifest_summary: Some(status.audit_manifest_summary.clone()),
        digest_algorithm: Some(status.digest_algorithm.clone()),
        digest_manifest: Some(digest_manifest),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        verification_mismatches: Vec::new(),
        explicit_statement: status.explicit_statement.clone(),
    }
}

fn failure_report_without_contract(
    config: &Config,
    verdict: TinyLivePackageReleaseCapsuleVerdict,
    reason: String,
) -> PackageReleaseCapsuleReport {
    PackageReleaseCapsuleReport {
        generated_at: Utc::now(),
        mode: match config.mode {
            Mode::PlanLivePackageReleaseCapsule => "plan_live_package_release_capsule".to_string(),
            Mode::RenderLivePackageReleaseCapsule => {
                "render_live_package_release_capsule".to_string()
            }
            Mode::RunLivePackageReleaseCapsule => "run_live_package_release_capsule".to_string(),
            Mode::VerifyLivePackageReleaseCapsule => {
                "verify_live_package_release_capsule".to_string()
            }
        },
        verdict,
        reason,
        activation_ticket_session_dir: config.activation_ticket_session_dir.display().to_string(),
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
        release_capsule_session_path: None,
        release_capsule_status_path: None,
        release_capsule_digest_manifest_path: None,
        result: Some("refused_now_by_invalid_or_drifted_contract".to_string()),
        handoff_bundle_result: None,
        decision_packet_result: None,
        execute_frozen_result: None,
        activation_ticket_step: None,
        verify_activation_ticket_command_summary: Some(verify_activation_ticket_command_summary(
            &config.activation_ticket_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: None,
        release_capsule_summary: None,
        audit_manifest_summary: None,
        digest_algorithm: Some(RELEASE_CAPSULE_DIGEST_ALGORITHM.to_string()),
        digest_manifest: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this immutable release capsule refused before persisting because the activation-ticket artifact could not be verified coherently"
                .to_string(),
    }
}

fn failure_report_for_verify(
    config: &Config,
    session_dir: &Path,
    contract: &activation_ticket_release_capsule::LivePackageActivationTicketContractView,
    mismatches: Vec<String>,
) -> PackageReleaseCapsuleReport {
    let paths = release_capsule_paths(session_dir);
    let reason = mismatches.first().cloned().unwrap_or_else(|| {
        "activation-ticket-native release capsule verification failed".to_string()
    });
    PackageReleaseCapsuleReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_release_capsule".to_string(),
        verdict: TinyLivePackageReleaseCapsuleVerdict::TinyLivePackageReleaseCapsuleVerifyInvalid,
        reason,
        activation_ticket_session_dir: config.activation_ticket_session_dir.display().to_string(),
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
        release_capsule_session_path: Some(paths.session_path.display().to_string()),
        release_capsule_status_path: Some(paths.status_path.display().to_string()),
        release_capsule_digest_manifest_path: Some(paths.digest_manifest_path.display().to_string()),
        result: None,
        handoff_bundle_result: contract.handoff_bundle_result.clone(),
        decision_packet_result: contract.decision_packet_result.clone(),
        execute_frozen_result: contract.execute_frozen_result.clone(),
        activation_ticket_step: None,
        verify_activation_ticket_command_summary: Some(verify_activation_ticket_command_summary(
            &config.activation_ticket_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            contract
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        release_capsule_summary: Some(release_capsule_summary(
            classify_release_capsule_contract(contract, false).0,
            &contract.reviewed_frozen_live_cutover_controller_command_summary,
        )),
        audit_manifest_summary: Some(digest_manifest_summary(9)),
        digest_algorithm: Some(RELEASE_CAPSULE_DIGEST_ALGORITHM.to_string()),
        digest_manifest: None,
        current_pre_activation_gate_verdict: contract.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: contract.current_pre_activation_gate_reason.clone(),
        verification_mismatches: mismatches,
        explicit_statement:
            "this verification failure means the immutable release capsule no longer proves the exact reviewed frozen controller context or archival digest chain"
                .to_string(),
    }
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    value.ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn set_mode(mode: &mut Option<Mode>, value: Mode, flag: &str) -> Result<()> {
    if mode.replace(value).is_some() {
        bail!("multiple modes specified; latest was {flag}");
    }
    Ok(())
}

fn shell_escape_path(path: &Path) -> String {
    let raw = path.display().to_string();
    if raw
        .chars()
        .all(|value| value.is_ascii_alphanumeric() || "/._-".contains(value))
    {
        raw
    } else {
        format!("'{}'", raw.replace('\'', "'\"'\"'"))
    }
}

fn write_script(path: &Path, contents: &str) -> Result<()> {
    fs::write(path, contents).with_context(|| {
        format!(
            "failed writing release-capsule script to {}",
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

fn step_artifact(
    path: &Path,
    mode: &str,
    verdict: &str,
    reason: &str,
    generated_at: DateTime<Utc>,
) -> PackageReleaseCapsuleStepArtifact {
    PackageReleaseCapsuleStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn load_required_step_json<T: for<'de> Deserialize<'de>>(
    step: &Option<PackageReleaseCapsuleStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("{label} is missing from release-capsule status"))?;
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
    actual: Option<&PackageReleaseCapsuleStepArtifact>,
    expected: &PackageReleaseCapsuleStepArtifact,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    let Some(actual) = actual else {
        mismatches.push(format!("{label} is missing from release-capsule status"));
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

fn serialize_enum<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(raw)) => raw,
        Ok(other) => other.to_string(),
        Err(_) => "<serialization-error>".to_string(),
    }
}

fn render_report_lines(report: &PackageReleaseCapsuleReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "activation_ticket_session_dir={}",
            report.activation_ticket_session_dir
        ),
    ];
    if let Some(value) = &report.package_dir {
        lines.push(format!("package_dir={value}"));
    }
    if let Some(value) = &report.install_root {
        lines.push(format!("install_root={value}"));
    }
    if let Some(value) = &report.target_service_name {
        lines.push(format!("target_service_name={value}"));
    }
    if let Some(value) = &report.result {
        lines.push(format!("result={value}"));
    }
    if let Some(value) = &report.release_capsule_summary {
        lines.push(format!("release_capsule_summary={value}"));
    }
    if let Some(value) = &report.audit_manifest_summary {
        lines.push(format!("audit_manifest_summary={value}"));
    }
    if !report.verification_mismatches.is_empty() {
        lines.push("verification_mismatches:".to_string());
        lines.extend(
            report
                .verification_mismatches
                .iter()
                .map(|entry| format!("  - {entry}")),
        );
    }
    lines.push(format!("explicit_statement={}", report.explicit_statement));
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
            "release_capsule_ready",
            activation_ticket_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_release_capsule_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageReleaseCapsuleVerdict::TinyLivePackageReleaseCapsuleReadyForManualExecutionWhenGateTurnsGreen
        );
        assert_eq!(
            run.result.as_deref(),
            Some("ready_for_manual_execution_when_gate_turns_green")
        );

        let verify = verify_live_package_release_capsule_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageReleaseCapsuleVerdict::TinyLivePackageReleaseCapsuleVerifyOk,
            "{:#?}",
            verify.verification_mismatches
        );
    }

    #[test]
    fn stage3_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "release_capsule_stage3",
            activation_ticket_verify_report(
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "blocked",
                "stage3 blocked",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_release_capsule_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageReleaseCapsuleVerdict::TinyLivePackageReleaseCapsuleRefusedNowByStage3
        );
    }

    #[test]
    fn pre_activation_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "release_capsule_pre_activation",
            activation_ticket_verify_report(
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
                "blocked",
                "pre-activation blocked",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_release_capsule_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageReleaseCapsuleVerdict::TinyLivePackageReleaseCapsuleRefusedNowByPreActivationGate
        );
    }

    #[test]
    fn drifted_activation_ticket_contract_is_refused() {
        let fixture = fake_command_fixture(
            "release_capsule_drifted",
            activation_ticket_verify_report(
                "refused_now_by_invalid_or_drifted_contract",
                "refused_now_by_invalid_or_drifted_contract",
                "refused_now_by_invalid_or_drifted_contract",
                "refused_now_by_invalid_or_drifted_contract",
                "green",
                "contract drifted",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_release_capsule_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageReleaseCapsuleVerdict::TinyLivePackageReleaseCapsuleRefusedNowByInvalidOrDriftedContract
        );
    }

    #[test]
    fn run_refuses_managed_surface_overlap_before_writing_any_artifacts() {
        let fixture = fake_command_fixture(
            "release_capsule_overlap",
            activation_ticket_verify_report(
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
        let unsafe_session_dir = managed_surface.session_dir.join("release-capsule-session");
        let config = Config {
            mode: Mode::RunLivePackageReleaseCapsule,
            activation_ticket_session_dir: fixture.activation_ticket_session_dir.clone(),
            confirmed_decision_packet_session_dir: Some(
                fixture.decision_packet_session_dir.clone(),
            ),
            session_dir: Some(unsafe_session_dir.clone()),
            output_path: None,
            json: true,
        };

        let error = run_live_package_release_capsule_report(&config).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("overlaps the managed live target surface"),
            "{error:#}"
        );
        let paths = release_capsule_paths(&unsafe_session_dir);
        assert!(!paths.session_path.exists());
        assert!(!paths.status_path.exists());
        assert!(!paths.digest_manifest_path.exists());
    }

    #[test]
    fn verify_rejects_tampered_activation_ticket_step_path() {
        let fixture = fake_command_fixture(
            "release_capsule_tampered_step",
            activation_ticket_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_release_capsule_report(&fixture.run_config()).unwrap();
        let paths = release_capsule_paths(&fixture.release_capsule_session_dir);
        let mut status: PackageReleaseCapsuleStatus = load_json(&paths.status_path).unwrap();
        let foreign = fixture
            .fixture_dir
            .join("foreign.activation-ticket.report.json");
        fs::write(&foreign, "{}").unwrap();
        status.activation_ticket_step.as_mut().unwrap().path = foreign.display().to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_release_capsule_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageReleaseCapsuleVerdict::TinyLivePackageReleaseCapsuleVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_release_capsule_text() {
        let fixture = fake_command_fixture(
            "release_capsule_tampered_text",
            activation_ticket_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_release_capsule_report(&fixture.run_config()).unwrap();
        let paths = release_capsule_paths(&fixture.release_capsule_session_dir);
        let mut session: PackageReleaseCapsuleSession = load_json(&paths.session_path).unwrap();
        session.release_capsule_summary = "tampered summary".to_string();
        persist_json(&paths.session_path, &session).unwrap();

        let verify = verify_live_package_release_capsule_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageReleaseCapsuleVerdict::TinyLivePackageReleaseCapsuleVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_nested_activation_ticket_report_content() {
        let fixture = fake_command_fixture(
            "release_capsule_tampered_nested_report",
            activation_ticket_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_release_capsule_report(&fixture.run_config()).unwrap();
        let paths = release_capsule_paths(&fixture.release_capsule_session_dir);
        let mut report: serde_json::Value =
            load_json(&paths.activation_ticket_report_path).unwrap();
        report.as_object_mut().unwrap().insert(
            "reason".to_string(),
            json!("tampered nested activation ticket reason"),
        );
        persist_json(&paths.activation_ticket_report_path, &report).unwrap();

        let verify = verify_live_package_release_capsule_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageReleaseCapsuleVerdict::TinyLivePackageReleaseCapsuleVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_retimed_nested_activation_ticket_report_and_step_generated_at() {
        let fixture = fake_command_fixture(
            "release_capsule_retimed_nested_report",
            activation_ticket_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_release_capsule_report(&fixture.run_config()).unwrap();
        let paths = release_capsule_paths(&fixture.release_capsule_session_dir);
        let mut report: serde_json::Value =
            load_json(&paths.activation_ticket_report_path).unwrap();
        let archived_generated_at =
            DateTime::parse_from_rfc3339(report["generated_at"].as_str().unwrap())
                .unwrap()
                .with_timezone(&Utc);
        let forged_generated_at = archived_generated_at + chrono::Duration::seconds(61);
        report.as_object_mut().unwrap().insert(
            "generated_at".to_string(),
            json!(forged_generated_at.to_rfc3339()),
        );
        persist_json(&paths.activation_ticket_report_path, &report).unwrap();

        let mut status: PackageReleaseCapsuleStatus = load_json(&paths.status_path).unwrap();
        status.activation_ticket_step.as_mut().unwrap().generated_at = forged_generated_at;
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_release_capsule_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageReleaseCapsuleVerdict::TinyLivePackageReleaseCapsuleVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_digest_manifest_member_set() {
        let fixture = fake_command_fixture(
            "release_capsule_tampered_manifest",
            activation_ticket_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_release_capsule_report(&fixture.run_config()).unwrap();
        let paths = release_capsule_paths(&fixture.release_capsule_session_dir);
        let mut manifest: ReleaseCapsuleDigestManifest =
            load_json(&paths.digest_manifest_path).unwrap();
        manifest.entries.pop();
        persist_json(&paths.digest_manifest_path, &manifest).unwrap();

        let verify = verify_live_package_release_capsule_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageReleaseCapsuleVerdict::TinyLivePackageReleaseCapsuleVerifyInvalid
        );
    }

    struct FakeCommandFixture {
        fixture_dir: PathBuf,
        bin_dir: PathBuf,
        activation_ticket_session_dir: PathBuf,
        release_capsule_session_dir: PathBuf,
        decision_packet_session_dir: PathBuf,
        install_root: PathBuf,
    }

    impl FakeCommandFixture {
        fn run_config(&self) -> Config {
            Config {
                mode: Mode::RunLivePackageReleaseCapsule,
                activation_ticket_session_dir: self.activation_ticket_session_dir.clone(),
                confirmed_decision_packet_session_dir: Some(
                    self.decision_packet_session_dir.clone(),
                ),
                session_dir: Some(self.release_capsule_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn verify_config(&self) -> Config {
            Config {
                mode: Mode::VerifyLivePackageReleaseCapsule,
                activation_ticket_session_dir: self.activation_ticket_session_dir.clone(),
                confirmed_decision_packet_session_dir: Some(
                    self.decision_packet_session_dir.clone(),
                ),
                session_dir: Some(self.release_capsule_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }
    }

    fn fake_command_fixture(label: &str, verify_report: serde_json::Value) -> FakeCommandFixture {
        let fixture_dir = temp_dir(label);
        let bin_dir = fixture_dir.join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        let activation_ticket_session_dir = fixture_dir.join("activation-ticket-session");
        fs::create_dir_all(&activation_ticket_session_dir).unwrap();
        let release_capsule_session_dir = fixture_dir.join("release-capsule-session");
        let review_receipt_session_dir = fixture_dir.join("review-receipt-session");
        let handoff_bundle_session_dir = fixture_dir.join("handoff-bundle-session");
        let decision_packet_session_dir = fixture_dir.join("decision-packet-session");
        let execute_frozen_session_dir = fixture_dir.join("execute-frozen-session");
        let turn_green_session_dir = fixture_dir.join("turn-green-session");
        let launch_packet_session_dir = fixture_dir.join("launch-packet-session");
        let package_dir = fixture_dir.join("package");
        let install_root = fixture_dir.join("live-root");
        let backend_command = fixture_dir.join("fake-backend");
        fs::create_dir_all(&review_receipt_session_dir).unwrap();
        fs::create_dir_all(&handoff_bundle_session_dir).unwrap();
        fs::create_dir_all(&decision_packet_session_dir).unwrap();
        fs::create_dir_all(&execute_frozen_session_dir).unwrap();
        fs::create_dir_all(&turn_green_session_dir).unwrap();
        fs::create_dir_all(&launch_packet_session_dir).unwrap();
        fs::create_dir_all(&package_dir).unwrap();
        fs::create_dir_all(&install_root).unwrap();

        persist_json(
            &review_receipt_session_dir.join("tiny_live_activation_package_review_receipt.session.json"),
            &json!({
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
                "verify_handoff_bundle_command_summary": "verify handoff bundle",
                "reviewed_frozen_live_cutover_controller_command_summary": format!(
                    "copybot_tiny_live_activation_package_live_cutover --package-dir {} --install-root {} --target-service solana-copy-bot.service --backend-command {} --wrapper-timeout-ms 1200 --service-status-max-staleness-ms 4500 --session-dir {} --run-live-package-cutover",
                    shell_escape_path(&package_dir),
                    shell_escape_path(&install_root),
                    shell_escape_path(&backend_command),
                    shell_escape_path(&fixture_dir.join("frozen-live-cutover-session")),
                ),
                "review_receipt_summary": "Review outcome: ready for signoff.",
                "checklist_acknowledgement_summary": "Checklist acknowledgement.",
                "runbook_acknowledgement_summary": "Runbook acknowledgement.",
                "explicit_statement": "review-receipt statement"
            }),
        )
        .unwrap();
        persist_json(
            &review_receipt_session_dir.join("tiny_live_activation_package_review_receipt.status.json"),
            &json!({
                "result": verify_report.get("result").and_then(|value| value.as_str()).unwrap(),
                "handoff_bundle_result": verify_report.get("handoff_bundle_result").and_then(|value| value.as_str()).unwrap(),
                "decision_packet_result": verify_report.get("decision_packet_result").and_then(|value| value.as_str()).unwrap(),
                "execute_frozen_result": verify_report.get("execute_frozen_result").and_then(|value| value.as_str()).unwrap(),
                "review_receipt_summary": "Review outcome: ready for signoff.",
                "checklist_acknowledgement_summary": "Checklist acknowledgement.",
                "runbook_acknowledgement_summary": "Runbook acknowledgement.",
                "explicit_statement": "review-receipt status statement"
            }),
        )
        .unwrap();
        persist_json(
            &review_receipt_session_dir
                .join("tiny_live_activation_package_review_receipt.handoff_bundle.report.json"),
            &json!({
                "decision_packet_session_dir": decision_packet_session_dir.display().to_string()
            }),
        )
        .unwrap();

        persist_json(
            &activation_ticket_session_dir.join("tiny_live_activation_package_activation_ticket.session.json"),
            &json!({
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
                "verify_review_receipt_command_summary": format!(
                    "verify immutable review-receipt session under {} before freezing the final activation ticket",
                    review_receipt_session_dir.display()
                ),
                "reviewed_frozen_live_cutover_controller_command_summary": format!(
                    "copybot_tiny_live_activation_package_live_cutover --package-dir {} --install-root {} --target-service solana-copy-bot.service --backend-command {} --wrapper-timeout-ms 1200 --service-status-max-staleness-ms 4500 --session-dir {} --run-live-package-cutover",
                    shell_escape_path(&package_dir),
                    shell_escape_path(&install_root),
                    shell_escape_path(&backend_command),
                    shell_escape_path(&fixture_dir.join("frozen-live-cutover-session")),
                ),
                "activation_ticket_summary": "Activation ticket summary.",
                "execution_warrant_summary": "Execution warrant summary.",
                "explicit_statement": "activation-ticket statement"
            }),
        )
        .unwrap();
        persist_json(
            &activation_ticket_session_dir.join("tiny_live_activation_package_activation_ticket.status.json"),
            &json!({
                "result": verify_report.get("result").and_then(|value| value.as_str()).unwrap(),
                "handoff_bundle_result": verify_report.get("handoff_bundle_result").and_then(|value| value.as_str()).unwrap(),
                "decision_packet_result": verify_report.get("decision_packet_result").and_then(|value| value.as_str()).unwrap(),
                "execute_frozen_result": verify_report.get("execute_frozen_result").and_then(|value| value.as_str()).unwrap(),
                "current_pre_activation_gate_verdict": verify_report.get("current_pre_activation_gate_verdict").and_then(|value| value.as_str()),
                "current_pre_activation_gate_reason": verify_report.get("current_pre_activation_gate_reason").and_then(|value| value.as_str()),
                "activation_ticket_summary": "Activation ticket summary.",
                "execution_warrant_summary": "Execution warrant summary.",
                "explicit_statement": "activation-ticket status statement"
            }),
        )
        .unwrap();
        persist_json(
            &activation_ticket_session_dir
                .join("tiny_live_activation_package_activation_ticket.review_receipt.report.json"),
            &json!({
                "decision_packet_session_dir": decision_packet_session_dir.display().to_string()
            }),
        )
        .unwrap();

        let mut verify_report = verify_report;
        bind_activation_ticket_report_to_fixture(
            &mut verify_report,
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
        let invalid_verify_report = invalid_activation_ticket_verify_report(&verify_report);
        let verify_path = fixture_dir.join("activation-ticket.verify.json");
        let invalid_verify_path = fixture_dir.join("activation-ticket.verify.invalid.json");
        persist_json(&verify_path, &verify_report).unwrap();
        persist_json(&invalid_verify_path, &invalid_verify_report).unwrap();
        write_fake_activation_ticket_verify_command(
            &bin_dir.join("copybot_tiny_live_activation_package_activation_ticket"),
            &verify_path,
            &invalid_verify_path,
            &review_receipt_session_dir,
            &decision_packet_session_dir,
            &activation_ticket_session_dir,
        );

        FakeCommandFixture {
            fixture_dir,
            bin_dir,
            activation_ticket_session_dir,
            release_capsule_session_dir,
            decision_packet_session_dir,
            install_root,
        }
    }

    fn bind_activation_ticket_report_to_fixture(
        report: &mut serde_json::Value,
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
            .expect("activation-ticket report fixture must be an object");
        object.insert(
            "mode".to_string(),
            json!("verify_live_package_activation_ticket"),
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
            "verify_review_receipt_command_summary".to_string(),
            json!(format!(
                "verify immutable review-receipt session under {} before freezing the final activation ticket",
                review_receipt_session_dir.display()
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
            "activation_ticket_summary".to_string(),
            json!("Activation ticket summary."),
        );
        object.insert(
            "execution_warrant_summary".to_string(),
            json!("Execution warrant summary."),
        );
        object.insert(
            "explicit_statement".to_string(),
            json!("activation-ticket verify statement"),
        );
    }

    fn activation_ticket_verify_report(
        result: &str,
        handoff_bundle_result: &str,
        decision_packet_result: &str,
        execute_frozen_result: &str,
        gate_verdict: &str,
        gate_reason: &str,
    ) -> serde_json::Value {
        json!({
            "generated_at": Utc::now(),
            "mode": "verify_live_package_activation_ticket",
            "verdict": "tiny_live_package_activation_ticket_verify_ok",
            "reason": "activation-ticket chain is coherent",
            "result": result,
            "handoff_bundle_result": handoff_bundle_result,
            "decision_packet_result": decision_packet_result,
            "execute_frozen_result": execute_frozen_result,
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason,
        })
    }

    fn invalid_activation_ticket_verify_report(
        success_report: &serde_json::Value,
    ) -> serde_json::Value {
        let mut report = success_report.clone();
        let object = report
            .as_object_mut()
            .expect("activation-ticket verify report fixture must be an object");
        object.insert(
            "verdict".to_string(),
            json!("tiny_live_package_activation_ticket_verify_invalid"),
        );
        object.insert(
            "reason".to_string(),
            json!("stored decision_packet_session_dir does not match the trusted activation-ticket contract"),
        );
        object.insert(
            "result".to_string(),
            json!("refused_now_by_invalid_or_drifted_contract"),
        );
        report
    }

    fn write_fake_activation_ticket_verify_command(
        path: &Path,
        verify_json_path: &Path,
        invalid_verify_path: &Path,
        expected_review_receipt_session_dir: &Path,
        expected_decision_packet_session_dir: &Path,
        activation_ticket_session_dir: &Path,
    ) {
        let expected_review_receipt_session_dir =
            expected_review_receipt_session_dir.display().to_string();
        let expected_decision_packet_session_dir =
            expected_decision_packet_session_dir.display().to_string();
        let activation_ticket_session_dir = activation_ticket_session_dir.display().to_string();
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --review-receipt-session-dir {expected_review_receipt_session_dir} \"*) : ;;\n  *) echo 'unexpected review-receipt-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --confirm-decision-packet-session-dir {expected_decision_packet_session_dir} \"*) : ;;\n  *) echo 'unexpected decision-packet-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {activation_ticket_session_dir} \"*) : ;;\n  *) echo 'unexpected activation-ticket session dir' >&2; exit 1;;\nesac\nsession_json='{activation_ticket_session_dir}/tiny_live_activation_package_activation_ticket.session.json'\nif grep -F '\"decision_packet_session_dir\": \"{expected_decision_packet_session_dir}\"' \"$session_json\" >/dev/null; then\n  cat '{}'\nelse\n  cat '{}'\nfi\n",
            verify_json_path.display(),
            invalid_verify_path.display()
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
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{label}.{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }
}
