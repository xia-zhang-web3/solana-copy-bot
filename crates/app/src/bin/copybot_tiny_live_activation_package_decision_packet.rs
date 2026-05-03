use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_app::tiny_live_activation::install_target_managed_surface::validate_turn_green_session_dir as validate_managed_surface_session_dir;
use copybot_app::tiny_live_activation::package_execute_frozen_decision_packet as execute_frozen_decision_packet;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: copybot_tiny_live_activation_package_decision_packet --execute-frozen-session-dir <path> [--session-dir <path>] [--output <path>] [--json] (--plan-live-package-decision-packet | --render-live-package-decision-packet | --run-live-package-decision-packet | --verify-live-package-decision-packet)";
const DECISION_PACKET_SESSION_VERSION: &str = "1";
const DECISION_PACKET_STATUS_VERSION: &str = "1";

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
    execute_frozen_session_dir: PathBuf,
    session_dir: Option<PathBuf>,
    output_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanLivePackageDecisionPacket,
    RenderLivePackageDecisionPacket,
    RunLivePackageDecisionPacket,
    VerifyLivePackageDecisionPacket,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageDecisionPacketVerdict {
    TinyLivePackageDecisionPacketPlanReady,
    TinyLivePackageDecisionPacketRendered,
    TinyLivePackageDecisionPacketRefusedNowByStage3,
    TinyLivePackageDecisionPacketRefusedNowByPreActivationGate,
    TinyLivePackageDecisionPacketRefusedNowByInvalidOrDriftedContract,
    TinyLivePackageDecisionPacketRunnableWhenGateTruthTurnsGreen,
    TinyLivePackageDecisionPacketVerifyOk,
    TinyLivePackageDecisionPacketVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageDecisionPacketResult {
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    RefusedNowByInvalidOrDriftedContract,
    RunnableWhenGateTruthTurnsGreen,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageDecisionPacketStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageDecisionPacketSession {
    session_version: String,
    planned_at: DateTime<Utc>,
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
    verify_execute_frozen_command_summary: String,
    frozen_live_cutover_controller_command_summary: String,
    operator_checklist_summary: String,
    operator_runbook_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageDecisionPacketStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    execute_frozen_session_dir: String,
    result: LivePackageDecisionPacketResult,
    reason: String,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    execute_frozen_step: Option<PackageDecisionPacketStepArtifact>,
    operator_checklist_summary: String,
    operator_runbook_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageDecisionPacketPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    execute_frozen_report_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageDecisionPacketReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageDecisionPacketVerdict,
    reason: String,
    execute_frozen_session_dir: String,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    session_dir: Option<String>,
    decision_packet_session_path: Option<String>,
    decision_packet_status_path: Option<String>,
    result: Option<String>,
    execute_frozen_result: Option<String>,
    execute_frozen_step: Option<PackageDecisionPacketStepArtifact>,
    verify_execute_frozen_command_summary: Option<String>,
    frozen_live_cutover_controller_command_summary: Option<String>,
    operator_checklist_summary: Option<String>,
    operator_runbook_summary: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    verification_mismatches: Vec<String>,
    explicit_statement: String,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut execute_frozen_session_dir: Option<PathBuf> = None;
    let mut session_dir: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut json = false;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--execute-frozen-session-dir" => {
                execute_frozen_session_dir = Some(PathBuf::from(parse_string_arg(
                    "--execute-frozen-session-dir",
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
            "--plan-live-package-decision-packet" => {
                set_mode(&mut mode, Mode::PlanLivePackageDecisionPacket, arg.as_str())?
            }
            "--render-live-package-decision-packet" => set_mode(
                &mut mode,
                Mode::RenderLivePackageDecisionPacket,
                arg.as_str(),
            )?,
            "--run-live-package-decision-packet" => {
                set_mode(&mut mode, Mode::RunLivePackageDecisionPacket, arg.as_str())?
            }
            "--verify-live-package-decision-packet" => set_mode(
                &mut mode,
                Mode::VerifyLivePackageDecisionPacket,
                arg.as_str(),
            )?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let Some(mode) = mode else {
        return Ok(None);
    };
    if matches!(mode, Mode::RenderLivePackageDecisionPacket) && output_path.is_none() {
        bail!("missing --output for --render-live-package-decision-packet");
    }
    if matches!(
        mode,
        Mode::RunLivePackageDecisionPacket | Mode::VerifyLivePackageDecisionPacket
    ) && session_dir.is_none()
    {
        bail!("missing --session-dir for run/verify mode");
    }
    let execute_frozen_session_dir = execute_frozen_session_dir
        .ok_or_else(|| anyhow!("missing --execute-frozen-session-dir"))?;
    Ok(Some(Config {
        mode,
        execute_frozen_session_dir,
        session_dir,
        output_path,
        json,
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanLivePackageDecisionPacket => plan_live_package_decision_packet_report(&config)?,
        Mode::RenderLivePackageDecisionPacket => {
            render_live_package_decision_packet_report(&config)?
        }
        Mode::RunLivePackageDecisionPacket => run_live_package_decision_packet_report(&config)?,
        Mode::VerifyLivePackageDecisionPacket => {
            verify_live_package_decision_packet_report(&config)?
        }
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_live_package_decision_packet_report(
    config: &Config,
) -> Result<PackageDecisionPacketReport> {
    let (frozen, classification) =
        match execute_frozen_decision_packet::verify_live_package_execute_frozen_for_decision_packet(
            &config.execute_frozen_session_dir,
        ) {
            Ok(verified) => {
                let classification = classify_verified_execute_frozen(&verified);
                (verified.contract, classification)
            }
            Err(_) => {
                let raw_contract = execute_frozen_decision_packet::load_live_package_execute_frozen_contract_for_decision_packet(
                    &config.execute_frozen_session_dir,
                )?;
                let classification = classify_decision_packet_contract(&raw_contract, false);
                (raw_contract, classification)
            }
        };
    Ok(PackageDecisionPacketReport {
        generated_at: Utc::now(),
        mode: "plan_live_package_decision_packet".to_string(),
        verdict: TinyLivePackageDecisionPacketVerdict::TinyLivePackageDecisionPacketPlanReady,
        reason: format!(
            "execute-frozen-native final activation decision packet is explicit for verified execute-frozen session {}; this command stays read-only and freezes one final operator-facing go/no-go packet plus checklist",
            config.execute_frozen_session_dir.display()
        ),
        execute_frozen_session_dir: config.execute_frozen_session_dir.display().to_string(),
        turn_green_session_dir: Some(frozen.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: Some(frozen.launch_packet_session_dir.display().to_string()),
        package_dir: Some(frozen.package_dir.display().to_string()),
        install_root: Some(frozen.install_root.display().to_string()),
        target_service_name: Some(frozen.target_service_name.clone()),
        backend_command: Some(frozen.backend_command.clone()),
        wrapper_timeout_ms: Some(frozen.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(frozen.service_status_max_staleness_ms),
        session_dir: config
            .session_dir
            .as_ref()
            .map(|value| value.display().to_string()),
        decision_packet_session_path: None,
        decision_packet_status_path: None,
        result: Some(serialize_enum(&classification.0)),
        execute_frozen_result: frozen.result.clone(),
        execute_frozen_step: None,
        verify_execute_frozen_command_summary: Some(verify_execute_frozen_command_summary(
            &config.execute_frozen_session_dir,
        )),
        frozen_live_cutover_controller_command_summary: Some(
            frozen.frozen_live_cutover_controller_command_summary.clone(),
        ),
        operator_checklist_summary: Some(operator_checklist_summary(
            classification.0,
            &frozen.frozen_live_cutover_controller_command_summary,
        )),
        operator_runbook_summary: Some(operator_runbook_summary(
            classification.0,
            &frozen.frozen_live_cutover_controller_command_summary,
        )),
        current_pre_activation_gate_verdict: frozen.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: frozen.current_pre_activation_gate_reason,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this final packet freezes verified execute-frozen truth into one operator-facing decision/checklist artifact without enabling production execution"
                .to_string(),
    })
}

fn render_live_package_decision_packet_report(
    config: &Config,
) -> Result<PackageDecisionPacketReport> {
    let plan = plan_live_package_decision_packet_report(config)?;
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --output for render mode"))?;
    let session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| output_path.with_extension("session"));
    let script = format!(
        "#!/usr/bin/env bash\nset -euo pipefail\n\ncopybot_tiny_live_activation_package_decision_packet --execute-frozen-session-dir {} --plan-live-package-decision-packet\ncopybot_tiny_live_activation_package_decision_packet --execute-frozen-session-dir {} --session-dir {} --run-live-package-decision-packet\ncopybot_tiny_live_activation_package_decision_packet --execute-frozen-session-dir {} --session-dir {} --verify-live-package-decision-packet\n",
        shell_escape_path(&config.execute_frozen_session_dir),
        shell_escape_path(&config.execute_frozen_session_dir),
        shell_escape_path(&session_dir),
        shell_escape_path(&config.execute_frozen_session_dir),
        shell_escape_path(&session_dir),
    );
    write_script(output_path, &script)?;
    Ok(PackageDecisionPacketReport {
        generated_at: Utc::now(),
        mode: "render_live_package_decision_packet".to_string(),
        verdict: TinyLivePackageDecisionPacketVerdict::TinyLivePackageDecisionPacketRendered,
        reason: format!(
            "rendered execute-frozen-native decision/checklist packet script to {}",
            output_path.display()
        ),
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
        decision_packet_session_path: None,
        decision_packet_status_path: None,
        result: plan.result,
        execute_frozen_result: plan.execute_frozen_result,
        execute_frozen_step: None,
        verify_execute_frozen_command_summary: plan.verify_execute_frozen_command_summary,
        frozen_live_cutover_controller_command_summary: plan
            .frozen_live_cutover_controller_command_summary,
        operator_checklist_summary: plan.operator_checklist_summary,
        operator_runbook_summary: plan.operator_runbook_summary,
        current_pre_activation_gate_verdict: plan.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: plan.current_pre_activation_gate_reason,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this rendered script accepts only the verified execute-frozen session as handoff input and stays read-only with respect to the real target"
                .to_string(),
    })
}

fn run_live_package_decision_packet_report(config: &Config) -> Result<PackageDecisionPacketReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for run mode"))?;
    let verified_execute_frozen = match execute_frozen_decision_packet::verify_live_package_execute_frozen_for_decision_packet(
        &config.execute_frozen_session_dir,
    ) {
        Ok(value) => value,
        Err(error) => {
            return Ok(failure_report_without_contract(
                config,
                TinyLivePackageDecisionPacketVerdict::TinyLivePackageDecisionPacketRefusedNowByInvalidOrDriftedContract,
                format!(
                    "failed to verify execute-frozen session under {}: {error}",
                    config.execute_frozen_session_dir.display()
                ),
            ))
        }
    };

    validate_managed_surface_session_dir(&verified_execute_frozen.contract.install_root, session_dir)
        .with_context(|| {
            format!(
                "decision-packet session dir {} overlaps the managed live target surface derived from verified execute-frozen install_root {}",
                session_dir.display(),
                verified_execute_frozen.contract.install_root.display()
            )
        })?;
    ensure_clean_decision_packet_session_dir(session_dir)?;
    let paths = decision_packet_paths(session_dir);
    let classification = classify_verified_execute_frozen(&verified_execute_frozen);
    let session = planned_session(
        config,
        session_dir,
        &verified_execute_frozen.contract,
        classification.0,
    );
    persist_json(&paths.session_path, &session)?;
    persist_json(
        &paths.execute_frozen_report_path,
        &verified_execute_frozen.report_json,
    )?;
    let execute_frozen_step = Some(step_artifact(
        &paths.execute_frozen_report_path,
        "verify_live_package_execute_frozen",
        &verified_execute_frozen.verdict,
        &verified_execute_frozen.reason,
        verified_execute_frozen.generated_at,
    ));
    let status = PackageDecisionPacketStatus {
        status_version: DECISION_PACKET_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        execute_frozen_session_dir: config.execute_frozen_session_dir.display().to_string(),
        result: classification.0,
        reason: classification.2.clone(),
        execute_frozen_result: verified_execute_frozen.contract.result.clone(),
        current_pre_activation_gate_verdict: verified_execute_frozen
            .contract
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: verified_execute_frozen
            .contract
            .current_pre_activation_gate_reason
            .clone(),
        execute_frozen_step: execute_frozen_step.clone(),
        operator_checklist_summary: operator_checklist_summary(
            classification.0,
            &verified_execute_frozen
                .contract
                .frozen_live_cutover_controller_command_summary,
        ),
        operator_runbook_summary: operator_runbook_summary(
            classification.0,
            &verified_execute_frozen
                .contract
                .frozen_live_cutover_controller_command_summary,
        ),
        explicit_statement:
            "this decision/checklist packet is read-only and archival; it never executes the frozen controller itself"
                .to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    Ok(report_from_status(
        config,
        session_dir,
        &paths,
        &verified_execute_frozen.contract,
        classification.1,
        &status,
    ))
}

fn verify_live_package_decision_packet_report(
    config: &Config,
) -> Result<PackageDecisionPacketReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = decision_packet_paths(session_dir);
    let session: PackageDecisionPacketSession = load_json(&paths.session_path)?;
    let status: PackageDecisionPacketStatus = load_json(&paths.status_path)?;
    let mut mismatches = Vec::new();

    compare_string(
        &session.execute_frozen_session_dir,
        &config.execute_frozen_session_dir.display().to_string(),
        "decision-packet execute_frozen_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "decision-packet session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "decision-packet status session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.execute_frozen_session_dir,
        &config.execute_frozen_session_dir.display().to_string(),
        "decision-packet status execute_frozen_session_dir",
        &mut mismatches,
    );

    let verified_execute_frozen =
        match execute_frozen_decision_packet::verify_live_package_execute_frozen_for_decision_packet(
            &config.execute_frozen_session_dir,
        ) {
            Ok(value) => value,
            Err(error) => {
                let raw_contract =
                execute_frozen_decision_packet::load_live_package_execute_frozen_contract_for_decision_packet(
                    &config.execute_frozen_session_dir,
                )?;
                return Ok(failure_report_for_verify(
                    config,
                    session_dir,
                    &raw_contract,
                    vec![format!(
                        "failed to verify execute-frozen session under {}: {error}",
                        config.execute_frozen_session_dir.display()
                    )],
                ));
            }
        };
    let _: serde_json::Value = load_required_step_json(
        &status.execute_frozen_step,
        &paths.execute_frozen_report_path,
        "execute_frozen_step",
        &mut mismatches,
    )?;
    if verified_execute_frozen.verdict != "tiny_live_package_execute_frozen_verify_ok" {
        mismatches.push(format!(
            "nested execute-frozen verification is non-green: {}",
            verified_execute_frozen.reason
        ));
    }

    compare_string(
        &session.turn_green_session_dir,
        &verified_execute_frozen
            .contract
            .turn_green_session_dir
            .display()
            .to_string(),
        "decision-packet turn_green_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.launch_packet_session_dir,
        &verified_execute_frozen
            .contract
            .launch_packet_session_dir
            .display()
            .to_string(),
        "decision-packet launch_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.package_dir,
        &verified_execute_frozen
            .contract
            .package_dir
            .display()
            .to_string(),
        "decision-packet package_dir",
        &mut mismatches,
    );
    compare_string(
        &session.install_root,
        &verified_execute_frozen
            .contract
            .install_root
            .display()
            .to_string(),
        "decision-packet install_root",
        &mut mismatches,
    );
    compare_string(
        &session.target_service_name,
        &verified_execute_frozen.contract.target_service_name,
        "decision-packet target_service_name",
        &mut mismatches,
    );
    compare_string(
        &session.backend_command,
        &verified_execute_frozen.contract.backend_command,
        "decision-packet backend_command",
        &mut mismatches,
    );
    if session.wrapper_timeout_ms != verified_execute_frozen.contract.wrapper_timeout_ms {
        mismatches.push(format!(
            "decision-packet wrapper_timeout_ms {} does not match verified {}",
            session.wrapper_timeout_ms, verified_execute_frozen.contract.wrapper_timeout_ms
        ));
    }
    if session.service_status_max_staleness_ms
        != verified_execute_frozen
            .contract
            .service_status_max_staleness_ms
    {
        mismatches.push(format!(
            "decision-packet service_status_max_staleness_ms {} does not match verified {}",
            session.service_status_max_staleness_ms,
            verified_execute_frozen
                .contract
                .service_status_max_staleness_ms
        ));
    }
    compare_string(
        &session.verify_execute_frozen_command_summary,
        &verify_execute_frozen_command_summary(&config.execute_frozen_session_dir),
        "decision-packet verify_execute_frozen_command_summary",
        &mut mismatches,
    );
    compare_string(
        &session.frozen_live_cutover_controller_command_summary,
        &verified_execute_frozen
            .contract
            .frozen_live_cutover_controller_command_summary,
        "decision-packet frozen_live_cutover_controller_command_summary",
        &mut mismatches,
    );

    let classification = classify_verified_execute_frozen(&verified_execute_frozen);
    let expected_checklist = operator_checklist_summary(
        classification.0,
        &verified_execute_frozen
            .contract
            .frozen_live_cutover_controller_command_summary,
    );
    let expected_runbook = operator_runbook_summary(
        classification.0,
        &verified_execute_frozen
            .contract
            .frozen_live_cutover_controller_command_summary,
    );
    compare_string(
        &session.operator_checklist_summary,
        &expected_checklist,
        "decision-packet session operator_checklist_summary",
        &mut mismatches,
    );
    compare_string(
        &session.operator_runbook_summary,
        &expected_runbook,
        "decision-packet session operator_runbook_summary",
        &mut mismatches,
    );
    compare_string(
        &status.operator_checklist_summary,
        &expected_checklist,
        "decision-packet status operator_checklist_summary",
        &mut mismatches,
    );
    compare_string(
        &status.operator_runbook_summary,
        &expected_runbook,
        "decision-packet status operator_runbook_summary",
        &mut mismatches,
    );
    if status.result != classification.0 {
        mismatches.push(format!(
            "stored decision-packet result {} does not match verified {}",
            serialize_enum(&status.result),
            serialize_enum(&classification.0)
        ));
    }
    if status.execute_frozen_result != verified_execute_frozen.contract.result {
        mismatches.push(format!(
            "stored execute_frozen_result {:?} does not match verified {:?}",
            status.execute_frozen_result, verified_execute_frozen.contract.result
        ));
    }
    if status.current_pre_activation_gate_verdict
        != verified_execute_frozen
            .contract
            .current_pre_activation_gate_verdict
    {
        mismatches.push(format!(
            "stored current_pre_activation_gate_verdict {:?} does not match verified {:?}",
            status.current_pre_activation_gate_verdict,
            verified_execute_frozen
                .contract
                .current_pre_activation_gate_verdict
        ));
    }
    if status.current_pre_activation_gate_reason
        != verified_execute_frozen
            .contract
            .current_pre_activation_gate_reason
    {
        mismatches.push(format!(
            "stored current_pre_activation_gate_reason {:?} does not match verified {:?}",
            status.current_pre_activation_gate_reason,
            verified_execute_frozen
                .contract
                .current_pre_activation_gate_reason
        ));
    }

    let verdict = if mismatches.is_empty() {
        TinyLivePackageDecisionPacketVerdict::TinyLivePackageDecisionPacketVerifyOk
    } else {
        TinyLivePackageDecisionPacketVerdict::TinyLivePackageDecisionPacketVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "execute-frozen-native decision/checklist packet under {} is coherent and correctly binds the final operator-facing packet to verified frozen execution truth",
            session_dir.display()
        )
    } else {
        mismatches.first().cloned().unwrap_or_else(|| {
            "execute-frozen-native decision/checklist verification failed".to_string()
        })
    };
    Ok(PackageDecisionPacketReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_decision_packet".to_string(),
        verdict,
        reason,
        execute_frozen_session_dir: config.execute_frozen_session_dir.display().to_string(),
        turn_green_session_dir: Some(
            verified_execute_frozen
                .contract
                .turn_green_session_dir
                .display()
                .to_string(),
        ),
        launch_packet_session_dir: Some(
            verified_execute_frozen
                .contract
                .launch_packet_session_dir
                .display()
                .to_string(),
        ),
        package_dir: Some(verified_execute_frozen.contract.package_dir.display().to_string()),
        install_root: Some(verified_execute_frozen.contract.install_root.display().to_string()),
        target_service_name: Some(verified_execute_frozen.contract.target_service_name.clone()),
        backend_command: Some(verified_execute_frozen.contract.backend_command.clone()),
        wrapper_timeout_ms: Some(verified_execute_frozen.contract.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(
            verified_execute_frozen.contract.service_status_max_staleness_ms,
        ),
        session_dir: Some(session_dir.display().to_string()),
        decision_packet_session_path: Some(paths.session_path.display().to_string()),
        decision_packet_status_path: Some(paths.status_path.display().to_string()),
        result: Some(serialize_enum(&status.result)),
        execute_frozen_result: status.execute_frozen_result.clone(),
        execute_frozen_step: status.execute_frozen_step.clone(),
        verify_execute_frozen_command_summary: Some(verify_execute_frozen_command_summary(
            &config.execute_frozen_session_dir,
        )),
        frozen_live_cutover_controller_command_summary: Some(
            verified_execute_frozen
                .contract
                .frozen_live_cutover_controller_command_summary,
        ),
        operator_checklist_summary: Some(expected_checklist),
        operator_runbook_summary: Some(expected_runbook),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        verification_mismatches: mismatches,
        explicit_statement:
            "this verification binds one final operator-facing decision packet to verified execute-frozen truth and never substitutes a different frozen controller contract"
                .to_string(),
    })
}

fn verify_execute_frozen_command_summary(execute_frozen_session_dir: &Path) -> String {
    format!(
        "verify immutable execute-frozen session under {} before freezing the final activation decision/checklist packet",
        execute_frozen_session_dir.display()
    )
}

fn classify_decision_packet_contract(
    contract: &execute_frozen_decision_packet::LivePackageExecuteFrozenContractView,
    assume_verified: bool,
) -> (
    LivePackageDecisionPacketResult,
    TinyLivePackageDecisionPacketVerdict,
    String,
) {
    if !assume_verified {
        return (
            LivePackageDecisionPacketResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageDecisionPacketVerdict::TinyLivePackageDecisionPacketRefusedNowByInvalidOrDriftedContract,
            "the execute-frozen chain is not verified, so the final decision packet remains refused".to_string(),
        );
    }
    match contract.result.as_deref() {
        Some("refused_now_by_stage3") => (
            LivePackageDecisionPacketResult::RefusedNowByStage3,
            TinyLivePackageDecisionPacketVerdict::TinyLivePackageDecisionPacketRefusedNowByStage3,
            "the frozen controller remains refused now by Stage 3, so the final operator packet stays explicit refusal".to_string(),
        ),
        Some("refused_now_by_pre_activation_gate") => (
            LivePackageDecisionPacketResult::RefusedNowByPreActivationGate,
            TinyLivePackageDecisionPacketVerdict::TinyLivePackageDecisionPacketRefusedNowByPreActivationGate,
            "the frozen controller remains refused now by the pre-activation gate, so the final operator packet stays explicit refusal".to_string(),
        ),
        Some("completed_keep_running") | Some("completed_with_rollback") => (
            LivePackageDecisionPacketResult::RunnableWhenGateTruthTurnsGreen,
            TinyLivePackageDecisionPacketVerdict::TinyLivePackageDecisionPacketRunnableWhenGateTruthTurnsGreen,
            "the verified frozen execution handoff is coherent and freezes the exact live cutover controller command; this packet is the final operator-facing checklist before any future manual go-live decision".to_string(),
        ),
        _ => (
            LivePackageDecisionPacketResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageDecisionPacketVerdict::TinyLivePackageDecisionPacketRefusedNowByInvalidOrDriftedContract,
            "the verified execute-frozen chain is invalid, drifted, or reflects non-runnable execution evidence, so the final packet remains refused".to_string(),
        ),
    }
}

fn classify_verified_execute_frozen(
    verified: &execute_frozen_decision_packet::VerifiedLivePackageExecuteFrozenDecisionStep,
) -> (
    LivePackageDecisionPacketResult,
    TinyLivePackageDecisionPacketVerdict,
    String,
) {
    if verified.verdict != "tiny_live_package_execute_frozen_verify_ok" {
        return (
            LivePackageDecisionPacketResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageDecisionPacketVerdict::TinyLivePackageDecisionPacketRefusedNowByInvalidOrDriftedContract,
            format!(
                "nested execute-frozen verification is non-green: {}",
                verified.reason
            ),
        );
    }
    classify_decision_packet_contract(&verified.contract, true)
}

fn operator_checklist_summary(
    result: LivePackageDecisionPacketResult,
    frozen_live_cutover_controller_command_summary: &str,
) -> String {
    match result {
        LivePackageDecisionPacketResult::RefusedNowByStage3 => {
            "Checklist: do not run the frozen controller; wait for Discovery V2/current operational truth to turn green; rerun turn_green, execute_frozen, and regenerate this final decision packet.".to_string()
        }
        LivePackageDecisionPacketResult::RefusedNowByPreActivationGate => {
            "Checklist: do not run the frozen controller; clear the current pre-activation gate blocker; rerun turn_green, execute_frozen, and regenerate this final decision packet.".to_string()
        }
        LivePackageDecisionPacketResult::RefusedNowByInvalidOrDriftedContract => {
            "Checklist: do not run the frozen controller; repair the drifted/invalid contract or mint a new coherent turn_green -> execute_frozen chain before any new packet export.".to_string()
        }
        LivePackageDecisionPacketResult::RunnableWhenGateTruthTurnsGreen => format!(
            "Checklist: verify the intended prod host still matches the frozen contract; confirm Stage 3 and pre-activation truth are green at execution time; then use exactly this frozen controller command with no substitutions: {frozen_live_cutover_controller_command_summary}"
        ),
    }
}

fn operator_runbook_summary(
    result: LivePackageDecisionPacketResult,
    frozen_live_cutover_controller_command_summary: &str,
) -> String {
    match result {
        LivePackageDecisionPacketResult::RefusedNowByStage3 => {
            "Runbook: remain read-only; monitor Discovery V2/current operational truth recovery; rerun the packet-native refresh surfaces only after production discovery truth turns green.".to_string()
        }
        LivePackageDecisionPacketResult::RefusedNowByPreActivationGate => {
            "Runbook: remain read-only; resolve the current pre-activation gate blocker; rerun turn_green, execute_frozen, and this final packet after the gate turns green.".to_string()
        }
        LivePackageDecisionPacketResult::RefusedNowByInvalidOrDriftedContract => {
            "Runbook: remain read-only; repair the invalid/drifted execute-frozen chain; mint a new coherent final packet before any later operator checklist review.".to_string()
        }
        LivePackageDecisionPacketResult::RunnableWhenGateTruthTurnsGreen => format!(
            "Runbook: 1. Reconfirm current gate truth immediately before go-live. 2. Re-open this packet and compare the frozen controller command. 3. If still accepted, execute exactly: {frozen_live_cutover_controller_command_summary}. 4. Stop immediately if any regenerated packet no longer yields runnable_when_gate_truth_turns_green."
        ),
    }
}

fn decision_packet_paths(session_dir: &Path) -> PackageDecisionPacketPaths {
    PackageDecisionPacketPaths {
        session_path: session_dir.join("tiny_live_activation_package_decision_packet.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_decision_packet.status.json"),
        execute_frozen_report_path: session_dir
            .join("tiny_live_activation_package_decision_packet.execute_frozen.report.json"),
    }
}

fn ensure_clean_decision_packet_session_dir(session_dir: &Path) -> Result<()> {
    if session_dir.exists() {
        bail!(
            "refusing to overwrite existing decision-packet session dir {}",
            session_dir.display()
        );
    }
    fs::create_dir_all(session_dir).with_context(|| {
        format!(
            "failed creating decision-packet session dir {}",
            session_dir.display()
        )
    })
}

fn planned_session(
    config: &Config,
    session_dir: &Path,
    contract: &execute_frozen_decision_packet::LivePackageExecuteFrozenContractView,
    result: LivePackageDecisionPacketResult,
) -> PackageDecisionPacketSession {
    PackageDecisionPacketSession {
        session_version: DECISION_PACKET_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        execute_frozen_session_dir: config.execute_frozen_session_dir.display().to_string(),
        turn_green_session_dir: contract.turn_green_session_dir.display().to_string(),
        launch_packet_session_dir: contract.launch_packet_session_dir.display().to_string(),
        package_dir: contract.package_dir.display().to_string(),
        install_root: contract.install_root.display().to_string(),
        target_service_name: contract.target_service_name.clone(),
        backend_command: contract.backend_command.clone(),
        wrapper_timeout_ms: contract.wrapper_timeout_ms,
        service_status_max_staleness_ms: contract.service_status_max_staleness_ms,
        session_dir: session_dir.display().to_string(),
        verify_execute_frozen_command_summary: verify_execute_frozen_command_summary(
            &config.execute_frozen_session_dir,
        ),
        frozen_live_cutover_controller_command_summary: contract
            .frozen_live_cutover_controller_command_summary
            .clone(),
        operator_checklist_summary: operator_checklist_summary(
            result,
            &contract.frozen_live_cutover_controller_command_summary,
        ),
        operator_runbook_summary: operator_runbook_summary(
            result,
            &contract.frozen_live_cutover_controller_command_summary,
        ),
        explicit_statement:
            "the verified execute-frozen session is the only handoff input here; this packet freezes one final go/no-go checklist over the exact frozen live cutover command"
                .to_string(),
    }
}

fn report_from_status(
    config: &Config,
    session_dir: &Path,
    paths: &PackageDecisionPacketPaths,
    contract: &execute_frozen_decision_packet::LivePackageExecuteFrozenContractView,
    verdict: TinyLivePackageDecisionPacketVerdict,
    status: &PackageDecisionPacketStatus,
) -> PackageDecisionPacketReport {
    PackageDecisionPacketReport {
        generated_at: Utc::now(),
        mode: "run_live_package_decision_packet".to_string(),
        verdict,
        reason: status.reason.clone(),
        execute_frozen_session_dir: config.execute_frozen_session_dir.display().to_string(),
        turn_green_session_dir: Some(contract.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: Some(contract.launch_packet_session_dir.display().to_string()),
        package_dir: Some(contract.package_dir.display().to_string()),
        install_root: Some(contract.install_root.display().to_string()),
        target_service_name: Some(contract.target_service_name.clone()),
        backend_command: Some(contract.backend_command.clone()),
        wrapper_timeout_ms: Some(contract.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(contract.service_status_max_staleness_ms),
        session_dir: Some(session_dir.display().to_string()),
        decision_packet_session_path: Some(paths.session_path.display().to_string()),
        decision_packet_status_path: Some(paths.status_path.display().to_string()),
        result: Some(serialize_enum(&status.result)),
        execute_frozen_result: status.execute_frozen_result.clone(),
        execute_frozen_step: status.execute_frozen_step.clone(),
        verify_execute_frozen_command_summary: Some(verify_execute_frozen_command_summary(
            &config.execute_frozen_session_dir,
        )),
        frozen_live_cutover_controller_command_summary: Some(
            contract
                .frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        operator_checklist_summary: Some(status.operator_checklist_summary.clone()),
        operator_runbook_summary: Some(status.operator_runbook_summary.clone()),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        verification_mismatches: Vec::new(),
        explicit_statement: status.explicit_statement.clone(),
    }
}

fn failure_report_without_contract(
    config: &Config,
    verdict: TinyLivePackageDecisionPacketVerdict,
    reason: String,
) -> PackageDecisionPacketReport {
    PackageDecisionPacketReport {
        generated_at: Utc::now(),
        mode: match config.mode {
            Mode::PlanLivePackageDecisionPacket => "plan_live_package_decision_packet".to_string(),
            Mode::RenderLivePackageDecisionPacket => {
                "render_live_package_decision_packet".to_string()
            }
            Mode::RunLivePackageDecisionPacket => "run_live_package_decision_packet".to_string(),
            Mode::VerifyLivePackageDecisionPacket => {
                "verify_live_package_decision_packet".to_string()
            }
        },
        verdict,
        reason,
        execute_frozen_session_dir: config.execute_frozen_session_dir.display().to_string(),
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
        decision_packet_session_path: None,
        decision_packet_status_path: None,
        result: Some("refused_now_by_invalid_or_drifted_contract".to_string()),
        execute_frozen_result: None,
        execute_frozen_step: None,
        verify_execute_frozen_command_summary: Some(verify_execute_frozen_command_summary(
            &config.execute_frozen_session_dir,
        )),
        frozen_live_cutover_controller_command_summary: None,
        operator_checklist_summary: None,
        operator_runbook_summary: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this final operator packet refused before persisting because the execute-frozen artifact could not be verified coherently"
                .to_string(),
    }
}

fn failure_report_for_verify(
    config: &Config,
    session_dir: &Path,
    contract: &execute_frozen_decision_packet::LivePackageExecuteFrozenContractView,
    mismatches: Vec<String>,
) -> PackageDecisionPacketReport {
    let paths = decision_packet_paths(session_dir);
    let reason = mismatches.first().cloned().unwrap_or_else(|| {
        "execute-frozen-native decision/checklist verification failed".to_string()
    });
    PackageDecisionPacketReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_decision_packet".to_string(),
        verdict: TinyLivePackageDecisionPacketVerdict::TinyLivePackageDecisionPacketVerifyInvalid,
        reason,
        execute_frozen_session_dir: config.execute_frozen_session_dir.display().to_string(),
        turn_green_session_dir: Some(contract.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: Some(contract.launch_packet_session_dir.display().to_string()),
        package_dir: Some(contract.package_dir.display().to_string()),
        install_root: Some(contract.install_root.display().to_string()),
        target_service_name: Some(contract.target_service_name.clone()),
        backend_command: Some(contract.backend_command.clone()),
        wrapper_timeout_ms: Some(contract.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(contract.service_status_max_staleness_ms),
        session_dir: Some(session_dir.display().to_string()),
        decision_packet_session_path: Some(paths.session_path.display().to_string()),
        decision_packet_status_path: Some(paths.status_path.display().to_string()),
        result: None,
        execute_frozen_result: contract.result.clone(),
        execute_frozen_step: None,
        verify_execute_frozen_command_summary: Some(verify_execute_frozen_command_summary(
            &config.execute_frozen_session_dir,
        )),
        frozen_live_cutover_controller_command_summary: Some(
            contract
                .frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        operator_checklist_summary: None,
        operator_runbook_summary: None,
        current_pre_activation_gate_verdict: contract.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: contract.current_pre_activation_gate_reason.clone(),
        verification_mismatches: mismatches,
        explicit_statement:
            "this verification failure means the final operator-facing packet no longer proves the exact frozen execution contract"
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
            "failed writing decision-packet script to {}",
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
) -> PackageDecisionPacketStepArtifact {
    PackageDecisionPacketStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn load_required_step_json<T: for<'de> Deserialize<'de>>(
    step: &Option<PackageDecisionPacketStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("{label} is missing from decision-packet status"))?;
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

fn serialize_enum<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(raw)) => raw,
        Ok(other) => other.to_string(),
        Err(_) => "<serialization-error>".to_string(),
    }
}

fn render_report_lines(report: &PackageDecisionPacketReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "execute_frozen_session_dir={}",
            report.execute_frozen_session_dir
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
    if let Some(value) = &report.operator_checklist_summary {
        lines.push(format!("operator_checklist_summary={value}"));
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
    fn run_completed_keep_running_then_verify_stays_runnable() {
        let fixture = fake_command_fixture(
            "decision_packet_runnable",
            execute_frozen_verify_report("completed_keep_running", "green", "ok"),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_decision_packet_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageDecisionPacketVerdict::TinyLivePackageDecisionPacketRunnableWhenGateTruthTurnsGreen
        );
        assert_eq!(
            run.result.as_deref(),
            Some("runnable_when_gate_truth_turns_green")
        );

        let verify = verify_live_package_decision_packet_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageDecisionPacketVerdict::TinyLivePackageDecisionPacketVerifyOk,
            "{:#?}",
            verify.verification_mismatches
        );
    }

    #[test]
    fn stage3_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "decision_packet_stage3",
            execute_frozen_verify_report("refused_now_by_stage3", "blocked", "stage3 blocked"),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_decision_packet_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageDecisionPacketVerdict::TinyLivePackageDecisionPacketRefusedNowByStage3
        );
        assert_eq!(run.result.as_deref(), Some("refused_now_by_stage3"));
    }

    #[test]
    fn pre_activation_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "decision_packet_pre_activation",
            execute_frozen_verify_report(
                "refused_now_by_pre_activation_gate",
                "blocked",
                "pre-activation blocked",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_decision_packet_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageDecisionPacketVerdict::TinyLivePackageDecisionPacketRefusedNowByPreActivationGate
        );
        assert_eq!(
            run.result.as_deref(),
            Some("refused_now_by_pre_activation_gate")
        );
    }

    #[test]
    fn drifted_execute_frozen_contract_is_refused() {
        let fixture = fake_command_fixture(
            "decision_packet_drifted",
            execute_frozen_verify_report(
                "refused_now_by_invalid_or_drifted_contract",
                "green",
                "contract drifted",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_decision_packet_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageDecisionPacketVerdict::TinyLivePackageDecisionPacketRefusedNowByInvalidOrDriftedContract
        );
        assert_eq!(
            run.result.as_deref(),
            Some("refused_now_by_invalid_or_drifted_contract")
        );
    }

    #[test]
    fn verify_rejects_tampered_execute_frozen_step_path() {
        let fixture = fake_command_fixture(
            "decision_packet_tampered_step",
            execute_frozen_verify_report("completed_keep_running", "green", "ok"),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_decision_packet_report(&fixture.run_config()).unwrap();
        let paths = decision_packet_paths(&fixture.decision_packet_session_dir);
        let mut status: PackageDecisionPacketStatus = load_json(&paths.status_path).unwrap();
        let foreign = fixture
            .fixture_dir
            .join("foreign.execute-frozen.report.json");
        fs::write(&foreign, "{}").unwrap();
        status.execute_frozen_step.as_mut().unwrap().path = foreign.display().to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_decision_packet_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageDecisionPacketVerdict::TinyLivePackageDecisionPacketVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_checklist_and_runbook_summary() {
        let fixture = fake_command_fixture(
            "decision_packet_tampered_checklist",
            execute_frozen_verify_report("completed_keep_running", "green", "ok"),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_decision_packet_report(&fixture.run_config()).unwrap();
        let paths = decision_packet_paths(&fixture.decision_packet_session_dir);
        let mut session: PackageDecisionPacketSession = load_json(&paths.session_path).unwrap();
        session.operator_checklist_summary = "tampered checklist".to_string();
        persist_json(&paths.session_path, &session).unwrap();
        let mut status: PackageDecisionPacketStatus = load_json(&paths.status_path).unwrap();
        status.operator_runbook_summary = "tampered runbook".to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_decision_packet_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageDecisionPacketVerdict::TinyLivePackageDecisionPacketVerifyInvalid
        );
    }

    #[test]
    fn run_refuses_managed_surface_overlap_before_writing_any_artifacts() {
        let fixture = fake_command_fixture(
            "decision_packet_managed_overlap",
            execute_frozen_verify_report("completed_keep_running", "green", "ok"),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let managed_surface =
            copybot_app::tiny_live_activation::install_target_managed_surface::derive_install_target_managed_surface_paths(
                &fixture.install_root,
            )
            .unwrap();
        let unsafe_session_dir = managed_surface.session_dir.join("decision-packet-session");
        let config = Config {
            mode: Mode::RunLivePackageDecisionPacket,
            execute_frozen_session_dir: fixture.execute_frozen_session_dir.clone(),
            session_dir: Some(unsafe_session_dir.clone()),
            output_path: None,
            json: true,
        };

        let error = run_live_package_decision_packet_report(&config).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("overlaps the managed live target surface"),
            "{error:#}"
        );
        let paths = decision_packet_paths(&unsafe_session_dir);
        assert!(!paths.session_path.exists());
        assert!(!paths.status_path.exists());
    }

    #[test]
    fn verify_rejects_tampered_nested_execute_frozen_turn_green_report_content() {
        let fixture = fake_command_fixture(
            "decision_packet_tampered_nested_turn_green_report",
            execute_frozen_verify_report("completed_keep_running", "green", "ok"),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_decision_packet_report(&fixture.run_config()).unwrap();
        persist_json(
            &fixture
                .execute_frozen_session_dir
                .join("tiny_live_activation_package_execute_frozen.turn_green.report.json"),
            &json!({
                "turn_green_session_dir": fixture.fixture_dir.join("foreign-turn-green").display().to_string()
            }),
        )
        .unwrap();

        let verify = verify_live_package_decision_packet_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageDecisionPacketVerdict::TinyLivePackageDecisionPacketVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_top_level_contract() {
        let fixture = fake_command_fixture(
            "decision_packet_tampered_contract",
            execute_frozen_verify_report("completed_keep_running", "green", "ok"),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_decision_packet_report(&fixture.run_config()).unwrap();
        let paths = decision_packet_paths(&fixture.decision_packet_session_dir);
        let mut session: PackageDecisionPacketSession = load_json(&paths.session_path).unwrap();
        session.target_service_name = "tampered.service".to_string();
        persist_json(&paths.session_path, &session).unwrap();

        let verify = verify_live_package_decision_packet_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageDecisionPacketVerdict::TinyLivePackageDecisionPacketVerifyInvalid
        );
    }

    struct FakeCommandFixture {
        fixture_dir: PathBuf,
        bin_dir: PathBuf,
        execute_frozen_session_dir: PathBuf,
        decision_packet_session_dir: PathBuf,
        install_root: PathBuf,
    }

    impl FakeCommandFixture {
        fn run_config(&self) -> Config {
            Config {
                mode: Mode::RunLivePackageDecisionPacket,
                execute_frozen_session_dir: self.execute_frozen_session_dir.clone(),
                session_dir: Some(self.decision_packet_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn verify_config(&self) -> Config {
            Config {
                mode: Mode::VerifyLivePackageDecisionPacket,
                execute_frozen_session_dir: self.execute_frozen_session_dir.clone(),
                session_dir: Some(self.decision_packet_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }
    }

    fn fake_command_fixture(label: &str, verify_report: serde_json::Value) -> FakeCommandFixture {
        let fixture_dir = temp_dir(label);
        let bin_dir = fixture_dir.join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        let execute_frozen_session_dir = fixture_dir.join("execute-frozen-session");
        fs::create_dir_all(&execute_frozen_session_dir).unwrap();
        let decision_packet_session_dir = fixture_dir.join("decision-packet-session");
        let turn_green_session_dir = fixture_dir.join("turn-green-session");
        let launch_packet_session_dir = fixture_dir.join("launch-packet-session");
        let package_dir = fixture_dir.join("package");
        let install_root = fixture_dir.join("live-root");
        let backend_command = fixture_dir.join("fake-backend");
        let frozen_controller_command = format!(
            "copybot_tiny_live_activation_package_live_cutover --package-dir {} --install-root {} --target-service solana-copy-bot.service --backend-command {} --wrapper-timeout-ms 1200 --service-status-max-staleness-ms 4500 --session-dir {} --run-live-package-cutover",
            shell_escape_path(&package_dir),
            shell_escape_path(&install_root),
            shell_escape_path(&backend_command),
            shell_escape_path(&fixture_dir.join("frozen-live-cutover-session")),
        );
        fs::create_dir_all(&turn_green_session_dir).unwrap();
        fs::create_dir_all(&launch_packet_session_dir).unwrap();
        fs::create_dir_all(&package_dir).unwrap();
        fs::create_dir_all(&install_root).unwrap();

        persist_json(
            &execute_frozen_session_dir.join("tiny_live_activation_package_execute_frozen.session.json"),
            &json!({
                "turn_green_session_dir": turn_green_session_dir.display().to_string(),
                "launch_packet_session_dir": launch_packet_session_dir.display().to_string(),
                "package_dir": package_dir.display().to_string(),
                "install_root": install_root.display().to_string(),
                "target_service_name": "solana-copy-bot.service",
                "backend_command": backend_command.display().to_string(),
                "wrapper_timeout_ms": 1200,
                "service_status_max_staleness_ms": 4500,
                "verify_turn_green_command_summary": "verify turn green",
                "execute_frozen_command_summary": format!(
                    "copybot_tiny_live_activation_package_execute_frozen --turn-green-session-dir {} --session-dir {} --run-live-package-execute-frozen",
                    turn_green_session_dir.display(),
                    execute_frozen_session_dir.display()
                ),
                "frozen_live_cutover_controller_command_summary": frozen_controller_command,
                "explicit_statement": "execute-frozen statement"
            }),
        )
        .unwrap();
        persist_json(
            &execute_frozen_session_dir.join("tiny_live_activation_package_execute_frozen.status.json"),
            &json!({
                "result": verify_report.get("result").and_then(|value| value.as_str()).unwrap(),
                "turn_green_result_now": verify_report.get("turn_green_result_now").and_then(|value| value.as_str()).unwrap_or("executable_now"),
                "current_pre_activation_gate_verdict": verify_report.get("current_pre_activation_gate_verdict").and_then(|value| value.as_str()),
                "current_pre_activation_gate_reason": verify_report.get("current_pre_activation_gate_reason").and_then(|value| value.as_str()),
                "turn_green_step": {
                    "path": execute_frozen_session_dir.join("tiny_live_activation_package_execute_frozen.turn_green.report.json").display().to_string()
                },
                "live_cutover_step": if verify_report.get("execution_happened").and_then(|value| value.as_bool()).unwrap_or(false) {
                    json!({"path": execute_frozen_session_dir.join("tiny_live_activation_package_execute_frozen.live_cutover.report.json").display().to_string()})
                } else {
                    serde_json::Value::Null
                },
                "explicit_statement": "execute-frozen status statement"
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

        let verify_path = fixture_dir.join("execute_frozen.verify.json");
        let invalid_verify_path = fixture_dir.join("execute_frozen.verify.invalid.json");
        let mut verify_report = verify_report;
        bind_execute_frozen_report_to_fixture(
            &mut verify_report,
            &turn_green_session_dir,
            &launch_packet_session_dir,
            &package_dir,
            &install_root,
            &backend_command,
            &frozen_controller_command,
            &execute_frozen_session_dir,
        );
        let invalid_verify_report = invalid_execute_frozen_verify_report(&verify_report);
        persist_json(&verify_path, &verify_report).unwrap();
        persist_json(&invalid_verify_path, &invalid_verify_report).unwrap();
        write_fake_execute_frozen_verify_command(
            &bin_dir.join("copybot_tiny_live_activation_package_execute_frozen"),
            &verify_path,
            &invalid_verify_path,
            &turn_green_session_dir,
            &execute_frozen_session_dir,
        );

        FakeCommandFixture {
            fixture_dir,
            bin_dir,
            execute_frozen_session_dir,
            decision_packet_session_dir,
            install_root,
        }
    }

    fn bind_execute_frozen_report_to_fixture(
        report: &mut serde_json::Value,
        turn_green_session_dir: &Path,
        launch_packet_session_dir: &Path,
        package_dir: &Path,
        install_root: &Path,
        backend_command: &Path,
        frozen_controller_command: &str,
        execute_frozen_session_dir: &Path,
    ) {
        let object = report
            .as_object_mut()
            .expect("execute-frozen report fixture must be an object");
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
            "verify_turn_green_command_summary".to_string(),
            json!("verify turn green"),
        );
        object.insert(
            "execute_frozen_command_summary".to_string(),
            json!(format!(
                "copybot_tiny_live_activation_package_execute_frozen --turn-green-session-dir {} --session-dir {} --run-live-package-execute-frozen",
                turn_green_session_dir.display(),
                execute_frozen_session_dir.display()
            )),
        );
        object.insert(
            "frozen_live_cutover_controller_command_summary".to_string(),
            json!(frozen_controller_command),
        );
        object.insert(
            "explicit_statement".to_string(),
            json!("execute-frozen verify statement"),
        );
    }

    fn execute_frozen_verify_report(
        result: &str,
        gate_verdict: &str,
        gate_reason: &str,
    ) -> serde_json::Value {
        json!({
            "generated_at": Utc::now(),
            "verdict": "tiny_live_package_execute_frozen_verify_ok",
            "reason": "execute-frozen chain is coherent",
            "result": result,
            "turn_green_result_now": "executable_now",
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason,
            "execution_happened": matches!(result, "completed_keep_running" | "completed_with_rollback")
        })
    }

    fn invalid_execute_frozen_verify_report(
        success_report: &serde_json::Value,
    ) -> serde_json::Value {
        let mut report = success_report.clone();
        let object = report
            .as_object_mut()
            .expect("execute-frozen verify report fixture must be an object");
        object.insert(
            "verdict".to_string(),
            json!("tiny_live_package_execute_frozen_verify_invalid"),
        );
        object.insert(
            "reason".to_string(),
            json!("stored turn_green_session_dir does not match the trusted nested execute-frozen turn-green step"),
        );
        object.insert(
            "result".to_string(),
            json!("refused_now_by_invalid_or_drifted_contract"),
        );
        object.insert("execution_happened".to_string(), json!(false));
        report
    }

    fn write_fake_execute_frozen_verify_command(
        path: &Path,
        verify_json_path: &Path,
        invalid_json_path: &Path,
        expected_turn_green_session_dir: &Path,
        execute_frozen_session_dir: &Path,
    ) {
        let expected_turn_green_session_dir = expected_turn_green_session_dir.display().to_string();
        let execute_frozen_session_dir = execute_frozen_session_dir.display().to_string();
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --turn-green-session-dir {expected_turn_green_session_dir} \"*) : ;;\n  *) echo 'unexpected turn-green-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {execute_frozen_session_dir} \"*) : ;;\n  *) echo 'unexpected execute-frozen session dir' >&2; exit 1;;\nesac\nsession_json='{execute_frozen_session_dir}/tiny_live_activation_package_execute_frozen.session.json'\nif grep -F '\"turn_green_session_dir\": \"{expected_turn_green_session_dir}\"' \"$session_json\" >/dev/null; then\n  cat '{}'\nelse\n  cat '{}'\nfi\n",
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
