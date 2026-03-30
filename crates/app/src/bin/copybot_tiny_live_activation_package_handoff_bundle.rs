use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_app::tiny_live_activation::install_target_managed_surface::validate_turn_green_session_dir as validate_managed_surface_session_dir;
use copybot_app::tiny_live_activation::package_decision_packet_handoff_bundle as decision_packet_handoff_bundle;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: copybot_tiny_live_activation_package_handoff_bundle --decision-packet-session-dir <path> [--session-dir <path>] [--output <path>] [--json] (--plan-live-package-handoff-bundle | --render-live-package-handoff-bundle | --run-live-package-handoff-bundle | --verify-live-package-handoff-bundle)";
const HANDOFF_BUNDLE_SESSION_VERSION: &str = "1";
const HANDOFF_BUNDLE_STATUS_VERSION: &str = "1";
const HANDOFF_BUNDLE_SESSION_EXPLICIT_STATEMENT: &str =
    "the verified decision-packet session is the only handoff input here; this archival bundle freezes one immutable go-live dossier over the exact frozen live cutover contract without enabling production execution";
const HANDOFF_BUNDLE_STATUS_EXPLICIT_STATEMENT: &str =
    "this handoff bundle is read-only and archival; it never executes the frozen controller itself";
const HANDOFF_BUNDLE_VERIFY_EXPLICIT_STATEMENT: &str =
    "this verification binds one immutable handoff bundle to verified decision-packet truth and exact nested frozen execution evidence";

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
    decision_packet_session_dir: PathBuf,
    session_dir: Option<PathBuf>,
    output_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanLivePackageHandoffBundle,
    RenderLivePackageHandoffBundle,
    RunLivePackageHandoffBundle,
    VerifyLivePackageHandoffBundle,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageHandoffBundleVerdict {
    TinyLivePackageHandoffBundlePlanReady,
    TinyLivePackageHandoffBundleRendered,
    TinyLivePackageHandoffBundleRefusedNowByStage3,
    TinyLivePackageHandoffBundleRefusedNowByPreActivationGate,
    TinyLivePackageHandoffBundleRefusedNowByInvalidOrDriftedContract,
    TinyLivePackageHandoffBundleReadyForManualGoLiveReview,
    TinyLivePackageHandoffBundleVerifyOk,
    TinyLivePackageHandoffBundleVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageHandoffBundleResult {
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    RefusedNowByInvalidOrDriftedContract,
    ReadyForManualGoLiveReview,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PackageHandoffBundleManifestEntry {
    label: String,
    path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageHandoffBundleStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageHandoffBundleSession {
    session_version: String,
    planned_at: DateTime<Utc>,
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
    verify_decision_packet_command_summary: String,
    frozen_live_cutover_controller_command_summary: String,
    operator_checklist_summary: String,
    operator_runbook_summary: String,
    handoff_bundle_summary: String,
    manifest: Vec<PackageHandoffBundleManifestEntry>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageHandoffBundleStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    decision_packet_session_dir: String,
    result: LivePackageHandoffBundleResult,
    reason: String,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    decision_packet_step: Option<PackageHandoffBundleStepArtifact>,
    operator_checklist_summary: String,
    operator_runbook_summary: String,
    handoff_bundle_summary: String,
    manifest: Vec<PackageHandoffBundleManifestEntry>,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageHandoffBundlePaths {
    session_path: PathBuf,
    status_path: PathBuf,
    decision_packet_report_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageHandoffBundleReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageHandoffBundleVerdict,
    reason: String,
    decision_packet_session_dir: String,
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
    handoff_bundle_session_path: Option<String>,
    handoff_bundle_status_path: Option<String>,
    result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    decision_packet_step: Option<PackageHandoffBundleStepArtifact>,
    verify_decision_packet_command_summary: Option<String>,
    frozen_live_cutover_controller_command_summary: Option<String>,
    operator_checklist_summary: Option<String>,
    operator_runbook_summary: Option<String>,
    handoff_bundle_summary: Option<String>,
    manifest: Vec<PackageHandoffBundleManifestEntry>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    verification_mismatches: Vec<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct NestedDecisionPacketReportView {
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
    let mut decision_packet_session_dir: Option<PathBuf> = None;
    let mut session_dir: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut json = false;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--decision-packet-session-dir" => {
                decision_packet_session_dir = Some(PathBuf::from(parse_string_arg(
                    "--decision-packet-session-dir",
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
            "--plan-live-package-handoff-bundle" => {
                set_mode(&mut mode, Mode::PlanLivePackageHandoffBundle, arg.as_str())?
            }
            "--render-live-package-handoff-bundle" => set_mode(
                &mut mode,
                Mode::RenderLivePackageHandoffBundle,
                arg.as_str(),
            )?,
            "--run-live-package-handoff-bundle" => {
                set_mode(&mut mode, Mode::RunLivePackageHandoffBundle, arg.as_str())?
            }
            "--verify-live-package-handoff-bundle" => set_mode(
                &mut mode,
                Mode::VerifyLivePackageHandoffBundle,
                arg.as_str(),
            )?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let Some(mode) = mode else {
        return Ok(None);
    };
    if matches!(mode, Mode::RenderLivePackageHandoffBundle) && output_path.is_none() {
        bail!("missing --output for --render-live-package-handoff-bundle");
    }
    if matches!(
        mode,
        Mode::RunLivePackageHandoffBundle | Mode::VerifyLivePackageHandoffBundle
    ) && session_dir.is_none()
    {
        bail!("missing --session-dir for run/verify mode");
    }
    let decision_packet_session_dir = decision_packet_session_dir
        .ok_or_else(|| anyhow!("missing --decision-packet-session-dir"))?;
    Ok(Some(Config {
        mode,
        decision_packet_session_dir,
        session_dir,
        output_path,
        json,
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanLivePackageHandoffBundle => plan_live_package_handoff_bundle_report(&config)?,
        Mode::RenderLivePackageHandoffBundle => render_live_package_handoff_bundle_report(&config)?,
        Mode::RunLivePackageHandoffBundle => run_live_package_handoff_bundle_report(&config)?,
        Mode::VerifyLivePackageHandoffBundle => verify_live_package_handoff_bundle_report(&config)?,
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_live_package_handoff_bundle_report(config: &Config) -> Result<PackageHandoffBundleReport> {
    let (decision_packet, classification) =
        match decision_packet_handoff_bundle::verify_live_package_decision_packet_for_handoff_bundle(
            &config.decision_packet_session_dir,
        ) {
            Ok(verified) => {
                let classification = classify_verified_decision_packet(&verified);
                (verified.contract, classification)
            }
            Err(_) => {
                let raw_contract =
                decision_packet_handoff_bundle::load_live_package_decision_packet_contract_for_handoff_bundle(
                    &config.decision_packet_session_dir,
                )?;
                let classification = classify_handoff_bundle_contract(&raw_contract, false);
                (raw_contract, classification)
            }
        };

    let session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| config.decision_packet_session_dir.join("handoff-bundle"));
    let paths = handoff_bundle_paths(&session_dir);
    let manifest = expected_manifest(&paths, &decision_packet);

    Ok(PackageHandoffBundleReport {
        generated_at: Utc::now(),
        mode: "plan_live_package_handoff_bundle".to_string(),
        verdict: TinyLivePackageHandoffBundleVerdict::TinyLivePackageHandoffBundlePlanReady,
        reason: format!(
            "decision-packet-native go-live handoff bundle is explicit for verified decision-packet session {}; this command stays archival and read-only while freezing one final immutable dossier",
            config.decision_packet_session_dir.display()
        ),
        decision_packet_session_dir: config.decision_packet_session_dir.display().to_string(),
        execute_frozen_session_dir: Some(
            decision_packet.execute_frozen_session_dir.display().to_string(),
        ),
        turn_green_session_dir: Some(decision_packet.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: Some(
            decision_packet.launch_packet_session_dir.display().to_string(),
        ),
        package_dir: Some(decision_packet.package_dir.display().to_string()),
        install_root: Some(decision_packet.install_root.display().to_string()),
        target_service_name: Some(decision_packet.target_service_name.clone()),
        backend_command: Some(decision_packet.backend_command.clone()),
        wrapper_timeout_ms: Some(decision_packet.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(
            decision_packet.service_status_max_staleness_ms,
        ),
        session_dir: Some(session_dir.display().to_string()),
        handoff_bundle_session_path: None,
        handoff_bundle_status_path: None,
        result: Some(serialize_enum(&classification.0)),
        decision_packet_result: decision_packet.result.clone(),
        execute_frozen_result: decision_packet.execute_frozen_result.clone(),
        decision_packet_step: None,
        verify_decision_packet_command_summary: Some(verify_decision_packet_command_summary(
            &config.decision_packet_session_dir,
        )),
        frozen_live_cutover_controller_command_summary: Some(
            decision_packet
                .frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        operator_checklist_summary: Some(decision_packet.operator_checklist_summary.clone()),
        operator_runbook_summary: Some(decision_packet.operator_runbook_summary.clone()),
        handoff_bundle_summary: Some(handoff_bundle_summary(
            classification.0,
            &decision_packet.frozen_live_cutover_controller_command_summary,
        )),
        manifest,
        current_pre_activation_gate_verdict: decision_packet.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: decision_packet.current_pre_activation_gate_reason,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this final bundle freezes verified decision-packet truth, nested artifact membership, checklist, and runbook into one immutable handoff dossier without enabling production execution"
                .to_string(),
    })
}

fn render_live_package_handoff_bundle_report(
    config: &Config,
) -> Result<PackageHandoffBundleReport> {
    let plan = plan_live_package_handoff_bundle_report(config)?;
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --output for render mode"))?;
    let session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| output_path.with_extension("session"));
    let script = format!(
        "#!/usr/bin/env bash\nset -euo pipefail\n\ncopybot_tiny_live_activation_package_handoff_bundle --decision-packet-session-dir {} --plan-live-package-handoff-bundle\ncopybot_tiny_live_activation_package_handoff_bundle --decision-packet-session-dir {} --session-dir {} --run-live-package-handoff-bundle\ncopybot_tiny_live_activation_package_handoff_bundle --decision-packet-session-dir {} --session-dir {} --verify-live-package-handoff-bundle\n",
        shell_escape_path(&config.decision_packet_session_dir),
        shell_escape_path(&config.decision_packet_session_dir),
        shell_escape_path(&session_dir),
        shell_escape_path(&config.decision_packet_session_dir),
        shell_escape_path(&session_dir),
    );
    write_script(output_path, &script)?;
    Ok(PackageHandoffBundleReport {
        generated_at: Utc::now(),
        mode: "render_live_package_handoff_bundle".to_string(),
        verdict: TinyLivePackageHandoffBundleVerdict::TinyLivePackageHandoffBundleRendered,
        reason: format!(
            "rendered decision-packet-native handoff bundle script to {}",
            output_path.display()
        ),
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
        handoff_bundle_session_path: None,
        handoff_bundle_status_path: None,
        result: plan.result,
        decision_packet_result: plan.decision_packet_result,
        execute_frozen_result: plan.execute_frozen_result,
        decision_packet_step: None,
        verify_decision_packet_command_summary: plan.verify_decision_packet_command_summary,
        frozen_live_cutover_controller_command_summary: plan
            .frozen_live_cutover_controller_command_summary,
        operator_checklist_summary: plan.operator_checklist_summary,
        operator_runbook_summary: plan.operator_runbook_summary,
        handoff_bundle_summary: plan.handoff_bundle_summary,
        manifest: plan.manifest,
        current_pre_activation_gate_verdict: plan.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: plan.current_pre_activation_gate_reason,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this rendered script accepts only the verified decision-packet session as handoff input and stays archival and read-only with respect to the real target"
                .to_string(),
    })
}

fn run_live_package_handoff_bundle_report(config: &Config) -> Result<PackageHandoffBundleReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for run mode"))?;
    let verified_decision_packet = match decision_packet_handoff_bundle::verify_live_package_decision_packet_for_handoff_bundle(
        &config.decision_packet_session_dir,
    ) {
        Ok(value) => value,
        Err(error) => {
            return Ok(failure_report_without_contract(
                config,
                TinyLivePackageHandoffBundleVerdict::TinyLivePackageHandoffBundleRefusedNowByInvalidOrDriftedContract,
                format!(
                    "failed to verify decision-packet session under {}: {error}",
                    config.decision_packet_session_dir.display()
                ),
            ))
        }
    };

    validate_managed_surface_session_dir(&verified_decision_packet.contract.install_root, session_dir)
        .with_context(|| {
            format!(
                "handoff-bundle session dir {} overlaps the managed live target surface derived from verified decision-packet install_root {}",
                session_dir.display(),
                verified_decision_packet.contract.install_root.display()
            )
        })?;
    ensure_clean_handoff_bundle_session_dir(session_dir)?;
    let paths = handoff_bundle_paths(session_dir);
    let classification = classify_verified_decision_packet(&verified_decision_packet);
    let manifest = expected_manifest(&paths, &verified_decision_packet.contract);
    let session = planned_session(
        config,
        session_dir,
        &verified_decision_packet.contract,
        classification.0,
        manifest.clone(),
    );
    persist_json(&paths.session_path, &session)?;
    persist_json(
        &paths.decision_packet_report_path,
        &verified_decision_packet.report_json,
    )?;
    let decision_packet_step = Some(step_artifact(
        &paths.decision_packet_report_path,
        "verify_live_package_decision_packet",
        &verified_decision_packet.verdict,
        &verified_decision_packet.reason,
        verified_decision_packet.generated_at,
    ));
    let status = PackageHandoffBundleStatus {
        status_version: HANDOFF_BUNDLE_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        decision_packet_session_dir: config.decision_packet_session_dir.display().to_string(),
        result: classification.0,
        reason: classification.2.clone(),
        decision_packet_result: verified_decision_packet.contract.result.clone(),
        execute_frozen_result: verified_decision_packet
            .contract
            .execute_frozen_result
            .clone(),
        current_pre_activation_gate_verdict: verified_decision_packet
            .contract
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: verified_decision_packet
            .contract
            .current_pre_activation_gate_reason
            .clone(),
        decision_packet_step: decision_packet_step.clone(),
        operator_checklist_summary: verified_decision_packet
            .contract
            .operator_checklist_summary
            .clone(),
        operator_runbook_summary: verified_decision_packet
            .contract
            .operator_runbook_summary
            .clone(),
        handoff_bundle_summary: handoff_bundle_summary(
            classification.0,
            &verified_decision_packet
                .contract
                .frozen_live_cutover_controller_command_summary,
        ),
        manifest: manifest.clone(),
        explicit_statement: HANDOFF_BUNDLE_STATUS_EXPLICIT_STATEMENT.to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    Ok(report_from_status(
        config,
        session_dir,
        &paths,
        &verified_decision_packet.contract,
        classification.1,
        &status,
    ))
}

fn verify_live_package_handoff_bundle_report(
    config: &Config,
) -> Result<PackageHandoffBundleReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = handoff_bundle_paths(session_dir);
    let session: PackageHandoffBundleSession = load_json(&paths.session_path)?;
    let status: PackageHandoffBundleStatus = load_json(&paths.status_path)?;
    let mut mismatches = Vec::new();

    compare_string(
        &session.decision_packet_session_dir,
        &config.decision_packet_session_dir.display().to_string(),
        "handoff-bundle decision_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "handoff-bundle session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "handoff-bundle status session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.decision_packet_session_dir,
        &config.decision_packet_session_dir.display().to_string(),
        "handoff-bundle status decision_packet_session_dir",
        &mut mismatches,
    );

    let verified_decision_packet =
        match decision_packet_handoff_bundle::verify_live_package_decision_packet_for_handoff_bundle(
            &config.decision_packet_session_dir,
        ) {
            Ok(value) => value,
            Err(error) => {
                let raw_contract =
                decision_packet_handoff_bundle::load_live_package_decision_packet_contract_for_handoff_bundle(
                    &config.decision_packet_session_dir,
                )?;
                return Ok(failure_report_for_verify(
                    config,
                    session_dir,
                    &raw_contract,
                    vec![format!(
                        "failed to verify decision-packet session under {}: {error}",
                        config.decision_packet_session_dir.display()
                    )],
                ));
            }
        };

    let stored_decision_packet_report: serde_json::Value = load_required_step_json(
        &status.decision_packet_step,
        &paths.decision_packet_report_path,
        "decision_packet_step",
        &mut mismatches,
    )?;
    let stored_decision_packet_report_view: NestedDecisionPacketReportView =
        serde_json::from_value(stored_decision_packet_report.clone()).with_context(|| {
            format!(
                "failed parsing archived nested decision-packet report {}",
                paths.decision_packet_report_path.display()
            )
        })?;
    let verified_decision_packet_report_view: NestedDecisionPacketReportView =
        serde_json::from_value(verified_decision_packet.report_json.clone()).with_context(
            || {
                format!(
                    "failed parsing freshly verified nested decision-packet report for {}",
                    config.decision_packet_session_dir.display()
                )
            },
        )?;
    compare_json_ignoring_generated_at(
        &stored_decision_packet_report,
        &verified_decision_packet.report_json,
        "handoff-bundle archived decision-packet report json",
        &mut mismatches,
    );
    compare_string(
        &stored_decision_packet_report_view.mode,
        &verified_decision_packet_report_view.mode,
        "handoff-bundle archived decision-packet report mode",
        &mut mismatches,
    );
    compare_string(
        &stored_decision_packet_report_view.verdict,
        &verified_decision_packet_report_view.verdict,
        "handoff-bundle archived decision-packet report verdict",
        &mut mismatches,
    );
    compare_string(
        &stored_decision_packet_report_view.reason,
        &verified_decision_packet_report_view.reason,
        "handoff-bundle archived decision-packet report reason",
        &mut mismatches,
    );
    let expected_decision_packet_step = step_artifact(
        &paths.decision_packet_report_path,
        &verified_decision_packet_report_view.mode,
        &verified_decision_packet_report_view.verdict,
        &verified_decision_packet_report_view.reason,
        stored_decision_packet_report_view.generated_at,
    );
    compare_step_artifact(
        status.decision_packet_step.as_ref(),
        &expected_decision_packet_step,
        "handoff-bundle decision_packet_step",
        &mut mismatches,
    );
    if verified_decision_packet.verdict != "tiny_live_package_decision_packet_verify_ok" {
        mismatches.push(format!(
            "nested decision-packet verification is non-green: {}",
            verified_decision_packet.reason
        ));
    }

    compare_string(
        &session.execute_frozen_session_dir,
        &verified_decision_packet
            .contract
            .execute_frozen_session_dir
            .display()
            .to_string(),
        "handoff-bundle execute_frozen_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.turn_green_session_dir,
        &verified_decision_packet
            .contract
            .turn_green_session_dir
            .display()
            .to_string(),
        "handoff-bundle turn_green_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.launch_packet_session_dir,
        &verified_decision_packet
            .contract
            .launch_packet_session_dir
            .display()
            .to_string(),
        "handoff-bundle launch_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.package_dir,
        &verified_decision_packet
            .contract
            .package_dir
            .display()
            .to_string(),
        "handoff-bundle package_dir",
        &mut mismatches,
    );
    compare_string(
        &session.install_root,
        &verified_decision_packet
            .contract
            .install_root
            .display()
            .to_string(),
        "handoff-bundle install_root",
        &mut mismatches,
    );
    compare_string(
        &session.target_service_name,
        &verified_decision_packet.contract.target_service_name,
        "handoff-bundle target_service_name",
        &mut mismatches,
    );
    compare_string(
        &session.backend_command,
        &verified_decision_packet.contract.backend_command,
        "handoff-bundle backend_command",
        &mut mismatches,
    );
    if session.wrapper_timeout_ms != verified_decision_packet.contract.wrapper_timeout_ms {
        mismatches.push(format!(
            "handoff-bundle wrapper_timeout_ms {} does not match verified {}",
            session.wrapper_timeout_ms, verified_decision_packet.contract.wrapper_timeout_ms
        ));
    }
    if session.service_status_max_staleness_ms
        != verified_decision_packet
            .contract
            .service_status_max_staleness_ms
    {
        mismatches.push(format!(
            "handoff-bundle service_status_max_staleness_ms {} does not match verified {}",
            session.service_status_max_staleness_ms,
            verified_decision_packet
                .contract
                .service_status_max_staleness_ms
        ));
    }
    compare_string(
        &session.verify_decision_packet_command_summary,
        &verify_decision_packet_command_summary(&config.decision_packet_session_dir),
        "handoff-bundle verify_decision_packet_command_summary",
        &mut mismatches,
    );
    compare_string(
        &session.frozen_live_cutover_controller_command_summary,
        &verified_decision_packet
            .contract
            .frozen_live_cutover_controller_command_summary,
        "handoff-bundle frozen_live_cutover_controller_command_summary",
        &mut mismatches,
    );
    compare_string(
        &session.operator_checklist_summary,
        &verified_decision_packet.contract.operator_checklist_summary,
        "handoff-bundle session operator_checklist_summary",
        &mut mismatches,
    );
    compare_string(
        &session.operator_runbook_summary,
        &verified_decision_packet.contract.operator_runbook_summary,
        "handoff-bundle session operator_runbook_summary",
        &mut mismatches,
    );
    compare_string(
        &status.operator_checklist_summary,
        &verified_decision_packet.contract.operator_checklist_summary,
        "handoff-bundle status operator_checklist_summary",
        &mut mismatches,
    );
    compare_string(
        &status.operator_runbook_summary,
        &verified_decision_packet.contract.operator_runbook_summary,
        "handoff-bundle status operator_runbook_summary",
        &mut mismatches,
    );

    let classification = classify_verified_decision_packet(&verified_decision_packet);
    let expected_handoff_summary = handoff_bundle_summary(
        classification.0,
        &verified_decision_packet
            .contract
            .frozen_live_cutover_controller_command_summary,
    );
    compare_string(
        &session.handoff_bundle_summary,
        &expected_handoff_summary,
        "handoff-bundle session handoff_bundle_summary",
        &mut mismatches,
    );
    compare_string(
        &status.handoff_bundle_summary,
        &expected_handoff_summary,
        "handoff-bundle status handoff_bundle_summary",
        &mut mismatches,
    );
    compare_string(
        &session.explicit_statement,
        HANDOFF_BUNDLE_SESSION_EXPLICIT_STATEMENT,
        "handoff-bundle session explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &status.explicit_statement,
        HANDOFF_BUNDLE_STATUS_EXPLICIT_STATEMENT,
        "handoff-bundle status explicit_statement",
        &mut mismatches,
    );
    if status.result != classification.0 {
        mismatches.push(format!(
            "stored handoff-bundle result {} does not match verified {}",
            serialize_enum(&status.result),
            serialize_enum(&classification.0)
        ));
    }
    if status.decision_packet_result != verified_decision_packet.contract.result {
        mismatches.push(format!(
            "stored decision_packet_result {:?} does not match verified {:?}",
            status.decision_packet_result, verified_decision_packet.contract.result
        ));
    }
    if status.execute_frozen_result != verified_decision_packet.contract.execute_frozen_result {
        mismatches.push(format!(
            "stored execute_frozen_result {:?} does not match verified {:?}",
            status.execute_frozen_result, verified_decision_packet.contract.execute_frozen_result
        ));
    }
    if status.current_pre_activation_gate_verdict
        != verified_decision_packet
            .contract
            .current_pre_activation_gate_verdict
    {
        mismatches.push(format!(
            "stored current_pre_activation_gate_verdict {:?} does not match verified {:?}",
            status.current_pre_activation_gate_verdict,
            verified_decision_packet
                .contract
                .current_pre_activation_gate_verdict
        ));
    }
    if status.current_pre_activation_gate_reason
        != verified_decision_packet
            .contract
            .current_pre_activation_gate_reason
    {
        mismatches.push(format!(
            "stored current_pre_activation_gate_reason {:?} does not match verified {:?}",
            status.current_pre_activation_gate_reason,
            verified_decision_packet
                .contract
                .current_pre_activation_gate_reason
        ));
    }

    let expected_manifest = expected_manifest(&paths, &verified_decision_packet.contract);
    compare_manifest(
        &session.manifest,
        &expected_manifest,
        "handoff-bundle session manifest",
        &mut mismatches,
    );
    compare_manifest(
        &status.manifest,
        &expected_manifest,
        "handoff-bundle status manifest",
        &mut mismatches,
    );
    for entry in &expected_manifest {
        if !Path::new(&entry.path).exists() {
            mismatches.push(format!(
                "handoff-bundle manifest member {:?} is missing at {:?}",
                entry.label, entry.path
            ));
        }
    }

    let verdict = if mismatches.is_empty() {
        TinyLivePackageHandoffBundleVerdict::TinyLivePackageHandoffBundleVerifyOk
    } else {
        TinyLivePackageHandoffBundleVerdict::TinyLivePackageHandoffBundleVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "decision-packet-native handoff bundle under {} is coherent and correctly binds the final immutable go-live dossier to verified decision-packet truth",
            session_dir.display()
        )
    } else {
        mismatches.first().cloned().unwrap_or_else(|| {
            "decision-packet-native handoff bundle verification failed".to_string()
        })
    };

    Ok(PackageHandoffBundleReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_handoff_bundle".to_string(),
        verdict,
        reason,
        decision_packet_session_dir: config.decision_packet_session_dir.display().to_string(),
        execute_frozen_session_dir: Some(
            verified_decision_packet
                .contract
                .execute_frozen_session_dir
                .display()
                .to_string(),
        ),
        turn_green_session_dir: Some(
            verified_decision_packet
                .contract
                .turn_green_session_dir
                .display()
                .to_string(),
        ),
        launch_packet_session_dir: Some(
            verified_decision_packet
                .contract
                .launch_packet_session_dir
                .display()
                .to_string(),
        ),
        package_dir: Some(
            verified_decision_packet
                .contract
                .package_dir
                .display()
                .to_string(),
        ),
        install_root: Some(
            verified_decision_packet
                .contract
                .install_root
                .display()
                .to_string(),
        ),
        target_service_name: Some(
            verified_decision_packet
                .contract
                .target_service_name
                .clone(),
        ),
        backend_command: Some(verified_decision_packet.contract.backend_command.clone()),
        wrapper_timeout_ms: Some(verified_decision_packet.contract.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(
            verified_decision_packet
                .contract
                .service_status_max_staleness_ms,
        ),
        session_dir: Some(session_dir.display().to_string()),
        handoff_bundle_session_path: Some(paths.session_path.display().to_string()),
        handoff_bundle_status_path: Some(paths.status_path.display().to_string()),
        result: Some(serialize_enum(&status.result)),
        decision_packet_result: status.decision_packet_result.clone(),
        execute_frozen_result: status.execute_frozen_result.clone(),
        decision_packet_step: Some(expected_decision_packet_step),
        verify_decision_packet_command_summary: Some(verify_decision_packet_command_summary(
            &config.decision_packet_session_dir,
        )),
        frozen_live_cutover_controller_command_summary: Some(
            verified_decision_packet
                .contract
                .frozen_live_cutover_controller_command_summary,
        ),
        operator_checklist_summary: Some(
            verified_decision_packet.contract.operator_checklist_summary,
        ),
        operator_runbook_summary: Some(verified_decision_packet.contract.operator_runbook_summary),
        handoff_bundle_summary: Some(expected_handoff_summary),
        manifest: expected_manifest,
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        verification_mismatches: mismatches,
        explicit_statement: HANDOFF_BUNDLE_VERIFY_EXPLICIT_STATEMENT.to_string(),
    })
}

fn verify_decision_packet_command_summary(decision_packet_session_dir: &Path) -> String {
    format!(
        "verify immutable decision-packet session under {} before freezing the final go-live handoff bundle",
        decision_packet_session_dir.display()
    )
}

fn classify_handoff_bundle_contract(
    contract: &decision_packet_handoff_bundle::LivePackageDecisionPacketContractView,
    assume_verified: bool,
) -> (
    LivePackageHandoffBundleResult,
    TinyLivePackageHandoffBundleVerdict,
    String,
) {
    if !assume_verified {
        return (
            LivePackageHandoffBundleResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageHandoffBundleVerdict::TinyLivePackageHandoffBundleRefusedNowByInvalidOrDriftedContract,
            "the decision-packet chain is not verified, so the immutable handoff bundle remains refused".to_string(),
        );
    }
    match contract.result.as_deref() {
        Some("refused_now_by_stage3") => (
            LivePackageHandoffBundleResult::RefusedNowByStage3,
            TinyLivePackageHandoffBundleVerdict::TinyLivePackageHandoffBundleRefusedNowByStage3,
            "the final decision packet remains refused now by Stage 3, so the handoff bundle remains an explicit refusal dossier".to_string(),
        ),
        Some("refused_now_by_pre_activation_gate") => (
            LivePackageHandoffBundleResult::RefusedNowByPreActivationGate,
            TinyLivePackageHandoffBundleVerdict::TinyLivePackageHandoffBundleRefusedNowByPreActivationGate,
            "the final decision packet remains refused now by the pre-activation gate, so the handoff bundle remains an explicit refusal dossier".to_string(),
        ),
        Some("runnable_when_gate_truth_turns_green") => (
            LivePackageHandoffBundleResult::ReadyForManualGoLiveReview,
            TinyLivePackageHandoffBundleVerdict::TinyLivePackageHandoffBundleReadyForManualGoLiveReview,
            "the verified decision packet is coherent and this immutable bundle is ready for final manual go-live review when gate truth turns green".to_string(),
        ),
        _ => (
            LivePackageHandoffBundleResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageHandoffBundleVerdict::TinyLivePackageHandoffBundleRefusedNowByInvalidOrDriftedContract,
            "the verified decision-packet chain is invalid, drifted, or non-runnable, so the handoff bundle remains refused".to_string(),
        ),
    }
}

fn classify_verified_decision_packet(
    verified: &decision_packet_handoff_bundle::VerifiedLivePackageDecisionPacketHandoffStep,
) -> (
    LivePackageHandoffBundleResult,
    TinyLivePackageHandoffBundleVerdict,
    String,
) {
    if verified.verdict != "tiny_live_package_decision_packet_verify_ok" {
        return (
            LivePackageHandoffBundleResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageHandoffBundleVerdict::TinyLivePackageHandoffBundleRefusedNowByInvalidOrDriftedContract,
            format!(
                "nested decision-packet verification is non-green: {}",
                verified.reason
            ),
        );
    }
    classify_handoff_bundle_contract(&verified.contract, true)
}

fn handoff_bundle_summary(
    result: LivePackageHandoffBundleResult,
    frozen_live_cutover_controller_command_summary: &str,
) -> String {
    match result {
        LivePackageHandoffBundleResult::RefusedNowByStage3 => {
            "Dossier summary: the final activation packet remains refused now by Stage 3; archive this bundle as refusal evidence only and do not execute the frozen controller.".to_string()
        }
        LivePackageHandoffBundleResult::RefusedNowByPreActivationGate => {
            "Dossier summary: the final activation packet remains refused now by the pre-activation gate; archive this bundle as refusal evidence only and do not execute the frozen controller.".to_string()
        }
        LivePackageHandoffBundleResult::RefusedNowByInvalidOrDriftedContract => {
            "Dossier summary: the final activation packet is invalid or drifted; archive this bundle for audit only and mint a new coherent decision-packet chain before any later review.".to_string()
        }
        LivePackageHandoffBundleResult::ReadyForManualGoLiveReview => format!(
            "Dossier summary: this immutable bundle is ready for final manual go-live review when gate truth turns green; the exact frozen controller command remains: {frozen_live_cutover_controller_command_summary}"
        ),
    }
}

fn handoff_bundle_paths(session_dir: &Path) -> PackageHandoffBundlePaths {
    PackageHandoffBundlePaths {
        session_path: session_dir.join("tiny_live_activation_package_handoff_bundle.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_handoff_bundle.status.json"),
        decision_packet_report_path: session_dir
            .join("tiny_live_activation_package_handoff_bundle.decision_packet.report.json"),
    }
}

fn expected_manifest(
    paths: &PackageHandoffBundlePaths,
    contract: &decision_packet_handoff_bundle::LivePackageDecisionPacketContractView,
) -> Vec<PackageHandoffBundleManifestEntry> {
    let decision_packet_paths = decision_packet_handoff_bundle::decision_packet_artifact_paths(
        &contract.decision_packet_session_dir,
    );
    let execute_frozen_paths = decision_packet_handoff_bundle::execute_frozen_artifact_paths(
        &contract.execute_frozen_session_dir,
    );
    let mut manifest = vec![
        manifest_entry("handoff_bundle_session_json", &paths.session_path),
        manifest_entry("handoff_bundle_status_json", &paths.status_path),
        manifest_entry(
            "handoff_bundle_decision_packet_report_json",
            &paths.decision_packet_report_path,
        ),
        manifest_entry(
            "decision_packet_session_json",
            &decision_packet_paths.session_path,
        ),
        manifest_entry(
            "decision_packet_status_json",
            &decision_packet_paths.status_path,
        ),
        manifest_entry(
            "decision_packet_execute_frozen_report_json",
            &decision_packet_paths.execute_frozen_report_path,
        ),
        manifest_entry(
            "execute_frozen_session_json",
            &execute_frozen_paths.session_path,
        ),
        manifest_entry(
            "execute_frozen_status_json",
            &execute_frozen_paths.status_path,
        ),
        manifest_entry(
            "execute_frozen_turn_green_report_json",
            &execute_frozen_paths.turn_green_report_path,
        ),
    ];
    if should_include_live_cutover_report(contract.execute_frozen_result.as_deref()) {
        manifest.push(manifest_entry(
            "execute_frozen_live_cutover_report_json",
            &execute_frozen_paths.live_cutover_report_path,
        ));
    }
    manifest
}

fn should_include_live_cutover_report(result: Option<&str>) -> bool {
    matches!(
        result,
        Some("completed_keep_running") | Some("completed_with_rollback")
    )
}

fn manifest_entry(label: &str, path: &Path) -> PackageHandoffBundleManifestEntry {
    PackageHandoffBundleManifestEntry {
        label: label.to_string(),
        path: path.display().to_string(),
    }
}

fn compare_manifest(
    actual: &[PackageHandoffBundleManifestEntry],
    expected: &[PackageHandoffBundleManifestEntry],
    label: &str,
    mismatches: &mut Vec<String>,
) {
    if actual != expected {
        mismatches.push(format!(
            "{label} does not match expected immutable membership"
        ));
    }
}

fn ensure_clean_handoff_bundle_session_dir(session_dir: &Path) -> Result<()> {
    if session_dir.exists() {
        bail!(
            "refusing to overwrite existing handoff-bundle session dir {}",
            session_dir.display()
        );
    }
    fs::create_dir_all(session_dir).with_context(|| {
        format!(
            "failed creating handoff-bundle session dir {}",
            session_dir.display()
        )
    })
}

fn planned_session(
    config: &Config,
    session_dir: &Path,
    contract: &decision_packet_handoff_bundle::LivePackageDecisionPacketContractView,
    result: LivePackageHandoffBundleResult,
    manifest: Vec<PackageHandoffBundleManifestEntry>,
) -> PackageHandoffBundleSession {
    PackageHandoffBundleSession {
        session_version: HANDOFF_BUNDLE_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        decision_packet_session_dir: config.decision_packet_session_dir.display().to_string(),
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
        verify_decision_packet_command_summary: verify_decision_packet_command_summary(
            &config.decision_packet_session_dir,
        ),
        frozen_live_cutover_controller_command_summary: contract
            .frozen_live_cutover_controller_command_summary
            .clone(),
        operator_checklist_summary: contract.operator_checklist_summary.clone(),
        operator_runbook_summary: contract.operator_runbook_summary.clone(),
        handoff_bundle_summary: handoff_bundle_summary(
            result,
            &contract.frozen_live_cutover_controller_command_summary,
        ),
        manifest,
        explicit_statement: HANDOFF_BUNDLE_SESSION_EXPLICIT_STATEMENT.to_string(),
    }
}

fn report_from_status(
    config: &Config,
    session_dir: &Path,
    paths: &PackageHandoffBundlePaths,
    contract: &decision_packet_handoff_bundle::LivePackageDecisionPacketContractView,
    verdict: TinyLivePackageHandoffBundleVerdict,
    status: &PackageHandoffBundleStatus,
) -> PackageHandoffBundleReport {
    PackageHandoffBundleReport {
        generated_at: Utc::now(),
        mode: "run_live_package_handoff_bundle".to_string(),
        verdict,
        reason: status.reason.clone(),
        decision_packet_session_dir: config.decision_packet_session_dir.display().to_string(),
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
        handoff_bundle_session_path: Some(paths.session_path.display().to_string()),
        handoff_bundle_status_path: Some(paths.status_path.display().to_string()),
        result: Some(serialize_enum(&status.result)),
        decision_packet_result: status.decision_packet_result.clone(),
        execute_frozen_result: status.execute_frozen_result.clone(),
        decision_packet_step: status.decision_packet_step.clone(),
        verify_decision_packet_command_summary: Some(verify_decision_packet_command_summary(
            &config.decision_packet_session_dir,
        )),
        frozen_live_cutover_controller_command_summary: Some(
            contract
                .frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        operator_checklist_summary: Some(status.operator_checklist_summary.clone()),
        operator_runbook_summary: Some(status.operator_runbook_summary.clone()),
        handoff_bundle_summary: Some(status.handoff_bundle_summary.clone()),
        manifest: status.manifest.clone(),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        verification_mismatches: Vec::new(),
        explicit_statement: status.explicit_statement.clone(),
    }
}

fn failure_report_without_contract(
    config: &Config,
    verdict: TinyLivePackageHandoffBundleVerdict,
    reason: String,
) -> PackageHandoffBundleReport {
    PackageHandoffBundleReport {
        generated_at: Utc::now(),
        mode: match config.mode {
            Mode::PlanLivePackageHandoffBundle => "plan_live_package_handoff_bundle".to_string(),
            Mode::RenderLivePackageHandoffBundle => {
                "render_live_package_handoff_bundle".to_string()
            }
            Mode::RunLivePackageHandoffBundle => "run_live_package_handoff_bundle".to_string(),
            Mode::VerifyLivePackageHandoffBundle => {
                "verify_live_package_handoff_bundle".to_string()
            }
        },
        verdict,
        reason,
        decision_packet_session_dir: config.decision_packet_session_dir.display().to_string(),
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
        handoff_bundle_session_path: None,
        handoff_bundle_status_path: None,
        result: Some("refused_now_by_invalid_or_drifted_contract".to_string()),
        decision_packet_result: None,
        execute_frozen_result: None,
        decision_packet_step: None,
        verify_decision_packet_command_summary: Some(verify_decision_packet_command_summary(
            &config.decision_packet_session_dir,
        )),
        frozen_live_cutover_controller_command_summary: None,
        operator_checklist_summary: None,
        operator_runbook_summary: None,
        handoff_bundle_summary: None,
        manifest: Vec::new(),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this immutable handoff bundle refused before persisting because the decision-packet artifact could not be verified coherently"
                .to_string(),
    }
}

fn failure_report_for_verify(
    config: &Config,
    session_dir: &Path,
    contract: &decision_packet_handoff_bundle::LivePackageDecisionPacketContractView,
    mismatches: Vec<String>,
) -> PackageHandoffBundleReport {
    let paths = handoff_bundle_paths(session_dir);
    let reason = mismatches
        .first()
        .cloned()
        .unwrap_or_else(|| "decision-packet-native handoff bundle verification failed".to_string());
    PackageHandoffBundleReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_handoff_bundle".to_string(),
        verdict: TinyLivePackageHandoffBundleVerdict::TinyLivePackageHandoffBundleVerifyInvalid,
        reason,
        decision_packet_session_dir: config.decision_packet_session_dir.display().to_string(),
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
        handoff_bundle_session_path: Some(paths.session_path.display().to_string()),
        handoff_bundle_status_path: Some(paths.status_path.display().to_string()),
        result: None,
        decision_packet_result: contract.result.clone(),
        execute_frozen_result: contract.execute_frozen_result.clone(),
        decision_packet_step: None,
        verify_decision_packet_command_summary: Some(verify_decision_packet_command_summary(
            &config.decision_packet_session_dir,
        )),
        frozen_live_cutover_controller_command_summary: Some(
            contract
                .frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        operator_checklist_summary: Some(contract.operator_checklist_summary.clone()),
        operator_runbook_summary: Some(contract.operator_runbook_summary.clone()),
        handoff_bundle_summary: None,
        manifest: Vec::new(),
        current_pre_activation_gate_verdict: contract.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: contract.current_pre_activation_gate_reason.clone(),
        verification_mismatches: mismatches,
        explicit_statement:
            "this verification failure means the immutable handoff bundle no longer proves the exact frozen go-live dossier"
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
    fs::write(path, contents)
        .with_context(|| format!("failed writing handoff-bundle script to {}", path.display()))?;
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
) -> PackageHandoffBundleStepArtifact {
    PackageHandoffBundleStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn load_required_step_json<T: for<'de> Deserialize<'de>>(
    step: &Option<PackageHandoffBundleStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("{label} is missing from handoff-bundle status"))?;
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
    actual: Option<&PackageHandoffBundleStepArtifact>,
    expected: &PackageHandoffBundleStepArtifact,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    let Some(actual) = actual else {
        mismatches.push(format!("{label} is missing from handoff-bundle status"));
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

fn render_report_lines(report: &PackageHandoffBundleReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "decision_packet_session_dir={}",
            report.decision_packet_session_dir
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
    if let Some(value) = &report.handoff_bundle_summary {
        lines.push(format!("handoff_bundle_summary={value}"));
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
    fn run_ready_for_manual_go_live_review_then_verify_stays_green() {
        let fixture = fake_command_fixture(
            "handoff_bundle_ready",
            decision_packet_verify_report(
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_handoff_bundle_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageHandoffBundleVerdict::TinyLivePackageHandoffBundleReadyForManualGoLiveReview
        );
        assert_eq!(
            run.result.as_deref(),
            Some("ready_for_manual_go_live_review")
        );

        let verify = verify_live_package_handoff_bundle_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageHandoffBundleVerdict::TinyLivePackageHandoffBundleVerifyOk,
            "{:#?}",
            verify.verification_mismatches
        );
    }

    #[test]
    fn stage3_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "handoff_bundle_stage3",
            decision_packet_verify_report(
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "blocked",
                "stage3 blocked",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_handoff_bundle_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageHandoffBundleVerdict::TinyLivePackageHandoffBundleRefusedNowByStage3
        );
    }

    #[test]
    fn pre_activation_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "handoff_bundle_pre_activation",
            decision_packet_verify_report(
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
                "blocked",
                "pre-activation blocked",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_handoff_bundle_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageHandoffBundleVerdict::TinyLivePackageHandoffBundleRefusedNowByPreActivationGate
        );
    }

    #[test]
    fn drifted_decision_packet_contract_is_refused() {
        let fixture = fake_command_fixture(
            "handoff_bundle_drifted",
            decision_packet_verify_report(
                "refused_now_by_invalid_or_drifted_contract",
                "refused_now_by_invalid_or_drifted_contract",
                "green",
                "contract drifted",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_handoff_bundle_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageHandoffBundleVerdict::TinyLivePackageHandoffBundleRefusedNowByInvalidOrDriftedContract
        );
    }

    #[test]
    fn run_refuses_managed_surface_overlap_before_writing_any_artifacts() {
        let fixture = fake_command_fixture(
            "handoff_bundle_overlap",
            decision_packet_verify_report(
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
        let unsafe_session_dir = managed_surface.session_dir.join("handoff-bundle-session");
        let config = Config {
            mode: Mode::RunLivePackageHandoffBundle,
            decision_packet_session_dir: fixture.decision_packet_session_dir.clone(),
            session_dir: Some(unsafe_session_dir.clone()),
            output_path: None,
            json: true,
        };

        let error = run_live_package_handoff_bundle_report(&config).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("overlaps the managed live target surface"),
            "{error:#}"
        );
        let paths = handoff_bundle_paths(&unsafe_session_dir);
        assert!(!paths.session_path.exists());
        assert!(!paths.status_path.exists());
    }

    #[test]
    fn verify_rejects_tampered_decision_packet_step_path() {
        let fixture = fake_command_fixture(
            "handoff_bundle_tampered_step",
            decision_packet_verify_report(
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_handoff_bundle_report(&fixture.run_config()).unwrap();
        let paths = handoff_bundle_paths(&fixture.handoff_bundle_session_dir);
        let mut status: PackageHandoffBundleStatus = load_json(&paths.status_path).unwrap();
        let foreign = fixture
            .fixture_dir
            .join("foreign.decision-packet.report.json");
        fs::write(&foreign, "{}").unwrap();
        status.decision_packet_step.as_mut().unwrap().path = foreign.display().to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_handoff_bundle_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageHandoffBundleVerdict::TinyLivePackageHandoffBundleVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_bundle_summary_and_manifest() {
        let fixture = fake_command_fixture(
            "handoff_bundle_tampered_bundle",
            decision_packet_verify_report(
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_handoff_bundle_report(&fixture.run_config()).unwrap();
        let paths = handoff_bundle_paths(&fixture.handoff_bundle_session_dir);
        let mut session: PackageHandoffBundleSession = load_json(&paths.session_path).unwrap();
        session.handoff_bundle_summary = "tampered summary".to_string();
        session.manifest.pop();
        persist_json(&paths.session_path, &session).unwrap();

        let verify = verify_live_package_handoff_bundle_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageHandoffBundleVerdict::TinyLivePackageHandoffBundleVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_archived_decision_packet_report_json() {
        let fixture = fake_command_fixture(
            "handoff_bundle_tampered_archived_report",
            decision_packet_verify_report(
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_handoff_bundle_report(&fixture.run_config()).unwrap();
        let paths = handoff_bundle_paths(&fixture.handoff_bundle_session_dir);
        let mut archived_report: serde_json::Value =
            load_json(&paths.decision_packet_report_path).unwrap();
        archived_report.as_object_mut().unwrap().insert(
            "reason".to_string(),
            json!("tampered archived report reason"),
        );
        persist_json(&paths.decision_packet_report_path, &archived_report).unwrap();

        let verify = verify_live_package_handoff_bundle_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageHandoffBundleVerdict::TinyLivePackageHandoffBundleVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_decision_packet_step_metadata() {
        let fixture = fake_command_fixture(
            "handoff_bundle_tampered_step_metadata",
            decision_packet_verify_report(
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_handoff_bundle_report(&fixture.run_config()).unwrap();
        let paths = handoff_bundle_paths(&fixture.handoff_bundle_session_dir);
        let mut status: PackageHandoffBundleStatus = load_json(&paths.status_path).unwrap();
        let step = status.decision_packet_step.as_mut().unwrap();
        step.mode = "tampered_mode".to_string();
        step.verdict = "tampered_verdict".to_string();
        step.reason = "tampered reason".to_string();
        step.generated_at += chrono::Duration::seconds(1);
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_handoff_bundle_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageHandoffBundleVerdict::TinyLivePackageHandoffBundleVerifyInvalid
        );
    }

    struct FakeCommandFixture {
        fixture_dir: PathBuf,
        bin_dir: PathBuf,
        decision_packet_session_dir: PathBuf,
        handoff_bundle_session_dir: PathBuf,
        install_root: PathBuf,
    }

    impl FakeCommandFixture {
        fn run_config(&self) -> Config {
            Config {
                mode: Mode::RunLivePackageHandoffBundle,
                decision_packet_session_dir: self.decision_packet_session_dir.clone(),
                session_dir: Some(self.handoff_bundle_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn verify_config(&self) -> Config {
            Config {
                mode: Mode::VerifyLivePackageHandoffBundle,
                decision_packet_session_dir: self.decision_packet_session_dir.clone(),
                session_dir: Some(self.handoff_bundle_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }
    }

    fn fake_command_fixture(label: &str, verify_report: serde_json::Value) -> FakeCommandFixture {
        let fixture_dir = temp_dir(label);
        let bin_dir = fixture_dir.join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        let decision_packet_session_dir = fixture_dir.join("decision-packet-session");
        fs::create_dir_all(&decision_packet_session_dir).unwrap();
        let handoff_bundle_session_dir = fixture_dir.join("handoff-bundle-session");
        let execute_frozen_session_dir = fixture_dir.join("execute-frozen-session");
        let turn_green_session_dir = fixture_dir.join("turn-green-session");
        let launch_packet_session_dir = fixture_dir.join("launch-packet-session");
        let package_dir = fixture_dir.join("package");
        let install_root = fixture_dir.join("live-root");
        let backend_command = fixture_dir.join("fake-backend");
        let live_cutover_report_path = execute_frozen_session_dir
            .join("tiny_live_activation_package_execute_frozen.live_cutover.report.json");
        fs::create_dir_all(&execute_frozen_session_dir).unwrap();
        fs::create_dir_all(&turn_green_session_dir).unwrap();
        fs::create_dir_all(&launch_packet_session_dir).unwrap();
        fs::create_dir_all(&package_dir).unwrap();
        fs::create_dir_all(&install_root).unwrap();

        persist_json(
            &decision_packet_session_dir.join("tiny_live_activation_package_decision_packet.session.json"),
            &json!({
                "execute_frozen_session_dir": execute_frozen_session_dir.display().to_string(),
                "turn_green_session_dir": turn_green_session_dir.display().to_string(),
                "launch_packet_session_dir": launch_packet_session_dir.display().to_string(),
                "package_dir": package_dir.display().to_string(),
                "install_root": install_root.display().to_string(),
                "target_service_name": "solana-copy-bot.service",
                "backend_command": backend_command.display().to_string(),
                "wrapper_timeout_ms": 1200,
                "service_status_max_staleness_ms": 4500,
                "verify_execute_frozen_command_summary": "verify execute frozen",
                "frozen_live_cutover_controller_command_summary": format!(
                    "copybot_tiny_live_activation_package_live_cutover --package-dir {} --install-root {} --target-service solana-copy-bot.service --backend-command {} --wrapper-timeout-ms 1200 --service-status-max-staleness-ms 4500 --session-dir {} --run-live-package-cutover",
                    shell_escape_path(&package_dir),
                    shell_escape_path(&install_root),
                    shell_escape_path(&backend_command),
                    shell_escape_path(&fixture_dir.join("frozen-live-cutover-session")),
                ),
                "operator_checklist_summary": "Checklist: verify the intended prod host still matches the frozen contract",
                "operator_runbook_summary": "Runbook: 1. Reconfirm current gate truth immediately before go-live.",
                "explicit_statement": "decision-packet statement"
            }),
        )
        .unwrap();
        persist_json(
            &decision_packet_session_dir.join("tiny_live_activation_package_decision_packet.status.json"),
            &json!({
                "session_dir": decision_packet_session_dir.display().to_string(),
                "execute_frozen_session_dir": execute_frozen_session_dir.display().to_string(),
                "result": verify_report.get("result").and_then(|value| value.as_str()).unwrap(),
                "execute_frozen_result": verify_report.get("execute_frozen_result").and_then(|value| value.as_str()).unwrap(),
                "current_pre_activation_gate_verdict": verify_report.get("current_pre_activation_gate_verdict").and_then(|value| value.as_str()),
                "current_pre_activation_gate_reason": verify_report.get("current_pre_activation_gate_reason").and_then(|value| value.as_str()),
                "execute_frozen_step": {
                    "path": decision_packet_session_dir.join("tiny_live_activation_package_decision_packet.execute_frozen.report.json").display().to_string()
                },
                "operator_checklist_summary": "Checklist: verify the intended prod host still matches the frozen contract",
                "operator_runbook_summary": "Runbook: 1. Reconfirm current gate truth immediately before go-live.",
                "explicit_statement": "decision-packet status statement"
            }),
        )
        .unwrap();
        persist_json(
            &decision_packet_session_dir
                .join("tiny_live_activation_package_decision_packet.execute_frozen.report.json"),
            &json!({
                "session_dir": execute_frozen_session_dir.display().to_string()
            }),
        )
        .unwrap();
        persist_json(
            &execute_frozen_session_dir
                .join("tiny_live_activation_package_execute_frozen.session.json"),
            &json!({
                "turn_green_session_dir": turn_green_session_dir.display().to_string()
            }),
        )
        .unwrap();
        persist_json(
            &execute_frozen_session_dir.join("tiny_live_activation_package_execute_frozen.status.json"),
            &json!({
                "result": verify_report.get("execute_frozen_result").and_then(|value| value.as_str()).unwrap()
            }),
        )
        .unwrap();
        persist_json(
            &execute_frozen_session_dir
                .join("tiny_live_activation_package_execute_frozen.turn_green.report.json"),
            &json!({
                "turn_green_session_dir": turn_green_session_dir.display().to_string()
            }),
        )
        .unwrap();
        if matches!(
            verify_report
                .get("execute_frozen_result")
                .and_then(|value| value.as_str()),
            Some("completed_keep_running") | Some("completed_with_rollback")
        ) {
            persist_json(&live_cutover_report_path, &json!({"ok": true})).unwrap();
        }

        let verify_path = fixture_dir.join("decision_packet.verify.json");
        let invalid_verify_path = fixture_dir.join("decision_packet.verify.invalid.json");
        let mut verify_report = verify_report;
        bind_decision_packet_report_to_fixture(
            &mut verify_report,
            &decision_packet_session_dir,
            &execute_frozen_session_dir,
            &turn_green_session_dir,
            &launch_packet_session_dir,
            &package_dir,
            &install_root,
            &backend_command,
        );
        let invalid_verify_report = invalid_decision_packet_verify_report(&verify_report);
        persist_json(&verify_path, &verify_report).unwrap();
        persist_json(&invalid_verify_path, &invalid_verify_report).unwrap();
        write_fake_decision_packet_verify_command(
            &bin_dir.join("copybot_tiny_live_activation_package_decision_packet"),
            &verify_path,
            &invalid_verify_path,
            &execute_frozen_session_dir,
            &decision_packet_session_dir,
        );

        FakeCommandFixture {
            fixture_dir,
            bin_dir,
            decision_packet_session_dir,
            handoff_bundle_session_dir,
            install_root,
        }
    }

    fn bind_decision_packet_report_to_fixture(
        report: &mut serde_json::Value,
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
            .expect("decision-packet report fixture must be an object");
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
            "verify_execute_frozen_command_summary".to_string(),
            json!(format!(
                "verify immutable execute-frozen session under {} before freezing the final activation decision/checklist packet",
                execute_frozen_session_dir.display()
            )),
        );
        object.insert(
            "frozen_live_cutover_controller_command_summary".to_string(),
            json!(format!(
                "copybot_tiny_live_activation_package_live_cutover --package-dir {} --install-root {} --target-service solana-copy-bot.service --backend-command {} --wrapper-timeout-ms 1200 --service-status-max-staleness-ms 4500 --session-dir {} --run-live-package-cutover",
                shell_escape_path(package_dir),
                shell_escape_path(install_root),
                shell_escape_path(backend_command),
                shell_escape_path(&decision_packet_session_dir.join("frozen-live-cutover-session")),
            )),
        );
        object.insert(
            "operator_checklist_summary".to_string(),
            json!("Checklist: verify the intended prod host still matches the frozen contract"),
        );
        object.insert(
            "operator_runbook_summary".to_string(),
            json!("Runbook: 1. Reconfirm current gate truth immediately before go-live."),
        );
        object.insert(
            "explicit_statement".to_string(),
            json!("decision-packet verify statement"),
        );
    }

    fn decision_packet_verify_report(
        result: &str,
        execute_frozen_result: &str,
        gate_verdict: &str,
        gate_reason: &str,
    ) -> serde_json::Value {
        json!({
            "generated_at": Utc::now(),
            "mode": "verify_live_package_decision_packet",
            "verdict": "tiny_live_package_decision_packet_verify_ok",
            "reason": "decision-packet chain is coherent",
            "result": result,
            "execute_frozen_result": execute_frozen_result,
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason,
        })
    }

    fn invalid_decision_packet_verify_report(
        success_report: &serde_json::Value,
    ) -> serde_json::Value {
        let mut report = success_report.clone();
        let object = report
            .as_object_mut()
            .expect("decision-packet verify report fixture must be an object");
        object.insert(
            "verdict".to_string(),
            json!("tiny_live_package_decision_packet_verify_invalid"),
        );
        object.insert(
            "reason".to_string(),
            json!("stored execute_frozen_session_dir does not match the trusted nested decision-packet execute-frozen step"),
        );
        object.insert(
            "result".to_string(),
            json!("refused_now_by_invalid_or_drifted_contract"),
        );
        report
    }

    fn write_fake_decision_packet_verify_command(
        path: &Path,
        verify_json_path: &Path,
        invalid_json_path: &Path,
        expected_execute_frozen_session_dir: &Path,
        decision_packet_session_dir: &Path,
    ) {
        let expected_execute_frozen_session_dir =
            expected_execute_frozen_session_dir.display().to_string();
        let decision_packet_session_dir = decision_packet_session_dir.display().to_string();
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --execute-frozen-session-dir {expected_execute_frozen_session_dir} \"*) : ;;\n  *) echo 'unexpected execute-frozen-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {decision_packet_session_dir} \"*) : ;;\n  *) echo 'unexpected decision-packet session dir' >&2; exit 1;;\nesac\nsession_json='{decision_packet_session_dir}/tiny_live_activation_package_decision_packet.session.json'\nif grep -F '\"execute_frozen_session_dir\": \"{expected_execute_frozen_session_dir}\"' \"$session_json\" >/dev/null; then\n  cat '{}'\nelse\n  cat '{}'\nfi\n",
            verify_json_path.display(),
            invalid_json_path.display()
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
