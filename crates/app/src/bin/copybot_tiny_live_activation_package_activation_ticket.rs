use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_app::tiny_live_activation::install_target_managed_surface::validate_turn_green_session_dir as validate_managed_surface_session_dir;
use copybot_app::tiny_live_activation::package_review_receipt_activation_ticket as review_receipt_activation_ticket;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: copybot_tiny_live_activation_package_activation_ticket --review-receipt-session-dir <path> [--confirm-decision-packet-session-dir <path>] [--session-dir <path>] [--output <path>] [--json] (--plan-live-package-activation-ticket | --render-live-package-activation-ticket | --run-live-package-activation-ticket | --verify-live-package-activation-ticket)";
const ACTIVATION_TICKET_SESSION_VERSION: &str = "1";
const ACTIVATION_TICKET_STATUS_VERSION: &str = "1";
const ACTIVATION_TICKET_SESSION_EXPLICIT_STATEMENT: &str =
    "the verified review-receipt session is the primary input here; this immutable activation ticket freezes one final execution warrant over the exact reviewed frozen live cutover contract without enabling production execution";
const ACTIVATION_TICKET_STATUS_EXPLICIT_STATEMENT: &str =
    "this activation ticket is archival and read-only; it never executes or enables the frozen controller";
const ACTIVATION_TICKET_VERIFY_EXPLICIT_STATEMENT: &str =
    "this verification binds one immutable activation ticket to verified review-receipt truth and exact nested frozen execution evidence";

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
    review_receipt_session_dir: PathBuf,
    confirmed_decision_packet_session_dir: Option<PathBuf>,
    session_dir: Option<PathBuf>,
    output_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanLivePackageActivationTicket,
    RenderLivePackageActivationTicket,
    RunLivePackageActivationTicket,
    VerifyLivePackageActivationTicket,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageActivationTicketVerdict {
    TinyLivePackageActivationTicketPlanReady,
    TinyLivePackageActivationTicketRendered,
    TinyLivePackageActivationTicketRefusedNowByStage3,
    TinyLivePackageActivationTicketRefusedNowByPreActivationGate,
    TinyLivePackageActivationTicketRefusedNowByInvalidOrDriftedContract,
    TinyLivePackageActivationTicketReadyForManualExecutionWhenGateTurnsGreen,
    TinyLivePackageActivationTicketVerifyOk,
    TinyLivePackageActivationTicketVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageActivationTicketResult {
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    RefusedNowByInvalidOrDriftedContract,
    ReadyForManualExecutionWhenGateTurnsGreen,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageActivationTicketStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageActivationTicketSession {
    session_version: String,
    planned_at: DateTime<Utc>,
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
    verify_review_receipt_command_summary: String,
    reviewed_frozen_live_cutover_controller_command_summary: String,
    activation_ticket_summary: String,
    execution_warrant_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageActivationTicketStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    review_receipt_session_dir: String,
    result: LivePackageActivationTicketResult,
    reason: String,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    review_receipt_step: Option<PackageActivationTicketStepArtifact>,
    activation_ticket_summary: String,
    execution_warrant_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageActivationTicketPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    review_receipt_report_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageActivationTicketReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageActivationTicketVerdict,
    reason: String,
    review_receipt_session_dir: String,
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
    activation_ticket_session_path: Option<String>,
    activation_ticket_status_path: Option<String>,
    result: Option<String>,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    review_receipt_step: Option<PackageActivationTicketStepArtifact>,
    verify_review_receipt_command_summary: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    activation_ticket_summary: Option<String>,
    execution_warrant_summary: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    verification_mismatches: Vec<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct NestedReviewReceiptReportView {
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
    let mut review_receipt_session_dir: Option<PathBuf> = None;
    let mut confirmed_decision_packet_session_dir: Option<PathBuf> = None;
    let mut session_dir: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut json = false;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--review-receipt-session-dir" => {
                review_receipt_session_dir = Some(PathBuf::from(parse_string_arg(
                    "--review-receipt-session-dir",
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
            "--plan-live-package-activation-ticket" => set_mode(
                &mut mode,
                Mode::PlanLivePackageActivationTicket,
                arg.as_str(),
            )?,
            "--render-live-package-activation-ticket" => set_mode(
                &mut mode,
                Mode::RenderLivePackageActivationTicket,
                arg.as_str(),
            )?,
            "--run-live-package-activation-ticket" => set_mode(
                &mut mode,
                Mode::RunLivePackageActivationTicket,
                arg.as_str(),
            )?,
            "--verify-live-package-activation-ticket" => set_mode(
                &mut mode,
                Mode::VerifyLivePackageActivationTicket,
                arg.as_str(),
            )?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let Some(mode) = mode else {
        return Ok(None);
    };
    if matches!(mode, Mode::RenderLivePackageActivationTicket) && output_path.is_none() {
        bail!("missing --output for --render-live-package-activation-ticket");
    }
    if matches!(
        mode,
        Mode::RunLivePackageActivationTicket | Mode::VerifyLivePackageActivationTicket
    ) && session_dir.is_none()
    {
        bail!("missing --session-dir for run/verify mode");
    }
    if matches!(
        mode,
        Mode::RunLivePackageActivationTicket | Mode::VerifyLivePackageActivationTicket
    ) && confirmed_decision_packet_session_dir.is_none()
    {
        bail!("missing --confirm-decision-packet-session-dir for run/verify mode");
    }
    let review_receipt_session_dir = review_receipt_session_dir
        .ok_or_else(|| anyhow!("missing --review-receipt-session-dir"))?;
    Ok(Some(Config {
        mode,
        review_receipt_session_dir,
        confirmed_decision_packet_session_dir,
        session_dir,
        output_path,
        json,
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanLivePackageActivationTicket => {
            plan_live_package_activation_ticket_report(&config)?
        }
        Mode::RenderLivePackageActivationTicket => {
            render_live_package_activation_ticket_report(&config)?
        }
        Mode::RunLivePackageActivationTicket => run_live_package_activation_ticket_report(&config)?,
        Mode::VerifyLivePackageActivationTicket => {
            verify_live_package_activation_ticket_report(&config)?
        }
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_live_package_activation_ticket_report(
    config: &Config,
) -> Result<PackageActivationTicketReport> {
    let (review_receipt, classification) =
        match config.confirmed_decision_packet_session_dir.as_deref() {
            Some(confirmed_decision_packet_session_dir) => {
                match review_receipt_activation_ticket::verify_live_package_review_receipt_for_activation_ticket(
                    &config.review_receipt_session_dir,
                    confirmed_decision_packet_session_dir,
                ) {
                    Ok(verified) => {
                        let classification = classify_verified_review_receipt(&verified);
                        (verified.contract, classification)
                    }
                    Err(_) => {
                        let raw_contract =
                            review_receipt_activation_ticket::load_live_package_review_receipt_contract_for_activation_ticket(
                                &config.review_receipt_session_dir,
                            )?;
                        let classification = classify_activation_ticket_contract(&raw_contract, false);
                        (raw_contract, classification)
                    }
                }
            }
            None => {
                let raw_contract =
                    review_receipt_activation_ticket::load_live_package_review_receipt_contract_for_activation_ticket(
                        &config.review_receipt_session_dir,
                    )?;
                let classification = classify_activation_ticket_contract(&raw_contract, false);
                (raw_contract, classification)
            }
        };

    let session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| config.review_receipt_session_dir.join("activation-ticket"));

    Ok(PackageActivationTicketReport {
        generated_at: Utc::now(),
        mode: "plan_live_package_activation_ticket".to_string(),
        verdict: TinyLivePackageActivationTicketVerdict::TinyLivePackageActivationTicketPlanReady,
        reason: format!(
            "review-receipt-native activation ticket is explicit for verified review-receipt session {}; this command stays archival and read-only while freezing one immutable execution warrant",
            config.review_receipt_session_dir.display()
        ),
        review_receipt_session_dir: config.review_receipt_session_dir.display().to_string(),
        handoff_bundle_session_dir: Some(
            review_receipt
                .handoff_bundle_session_dir
                .display()
                .to_string(),
        ),
        decision_packet_session_dir: Some(
            review_receipt
                .decision_packet_session_dir
                .display()
                .to_string(),
        ),
        execute_frozen_session_dir: Some(
            review_receipt
                .execute_frozen_session_dir
                .display()
                .to_string(),
        ),
        turn_green_session_dir: Some(review_receipt.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: Some(
            review_receipt
                .launch_packet_session_dir
                .display()
                .to_string(),
        ),
        package_dir: Some(review_receipt.package_dir.display().to_string()),
        install_root: Some(review_receipt.install_root.display().to_string()),
        target_service_name: Some(review_receipt.target_service_name.clone()),
        backend_command: Some(review_receipt.backend_command.clone()),
        wrapper_timeout_ms: Some(review_receipt.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(review_receipt.service_status_max_staleness_ms),
        session_dir: Some(session_dir.display().to_string()),
        activation_ticket_session_path: None,
        activation_ticket_status_path: None,
        result: Some(serialize_enum(&classification.0)),
        handoff_bundle_result: review_receipt.handoff_bundle_result.clone(),
        decision_packet_result: review_receipt.decision_packet_result.clone(),
        execute_frozen_result: review_receipt.execute_frozen_result.clone(),
        review_receipt_step: None,
        verify_review_receipt_command_summary: Some(verify_review_receipt_command_summary(
            &config.review_receipt_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            review_receipt
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        activation_ticket_summary: Some(activation_ticket_summary(
            classification.0,
            &review_receipt.reviewed_frozen_live_cutover_controller_command_summary,
        )),
        execution_warrant_summary: Some(execution_warrant_summary(
            classification.0,
            &review_receipt.reviewed_frozen_live_cutover_controller_command_summary,
        )),
        current_pre_activation_gate_verdict: review_receipt.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: review_receipt.current_pre_activation_gate_reason,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this final activation ticket freezes verified review-receipt truth and one operator-ready execution warrant without enabling production execution"
                .to_string(),
    })
}

fn render_live_package_activation_ticket_report(
    config: &Config,
) -> Result<PackageActivationTicketReport> {
    let plan = plan_live_package_activation_ticket_report(config)?;
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
        "#!/usr/bin/env bash\nset -euo pipefail\n\ncopybot_tiny_live_activation_package_activation_ticket --review-receipt-session-dir {} --plan-live-package-activation-ticket\ncopybot_tiny_live_activation_package_activation_ticket --review-receipt-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --run-live-package-activation-ticket\ncopybot_tiny_live_activation_package_activation_ticket --review-receipt-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --verify-live-package-activation-ticket\n",
        shell_escape_path(&config.review_receipt_session_dir),
        shell_escape_path(&config.review_receipt_session_dir),
        shell_escape_path(&confirmed_decision_packet_session_dir),
        shell_escape_path(&session_dir),
        shell_escape_path(&config.review_receipt_session_dir),
        shell_escape_path(&confirmed_decision_packet_session_dir),
        shell_escape_path(&session_dir),
    );
    write_script(output_path, &script)?;
    Ok(PackageActivationTicketReport {
        generated_at: Utc::now(),
        mode: "render_live_package_activation_ticket".to_string(),
        verdict: TinyLivePackageActivationTicketVerdict::TinyLivePackageActivationTicketRendered,
        reason: format!(
            "rendered review-receipt-native activation-ticket script to {}",
            output_path.display()
        ),
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
        activation_ticket_session_path: None,
        activation_ticket_status_path: None,
        result: plan.result,
        handoff_bundle_result: plan.handoff_bundle_result,
        decision_packet_result: plan.decision_packet_result,
        execute_frozen_result: plan.execute_frozen_result,
        review_receipt_step: None,
        verify_review_receipt_command_summary: plan.verify_review_receipt_command_summary,
        reviewed_frozen_live_cutover_controller_command_summary: plan
            .reviewed_frozen_live_cutover_controller_command_summary,
        activation_ticket_summary: plan.activation_ticket_summary,
        execution_warrant_summary: plan.execution_warrant_summary,
        current_pre_activation_gate_verdict: plan.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: plan.current_pre_activation_gate_reason,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this rendered script accepts the verified review-receipt session as the primary input and stays archival and read-only with respect to the real target"
                .to_string(),
    })
}

fn run_live_package_activation_ticket_report(
    config: &Config,
) -> Result<PackageActivationTicketReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for run mode"))?;
    let verified_review_receipt = match review_receipt_activation_ticket::verify_live_package_review_receipt_for_activation_ticket(
        &config.review_receipt_session_dir,
        config
            .confirmed_decision_packet_session_dir
            .as_deref()
            .ok_or_else(|| anyhow!("missing --confirm-decision-packet-session-dir for run mode"))?,
    ) {
        Ok(value) => value,
        Err(error) => {
            return Ok(failure_report_without_contract(
                config,
                TinyLivePackageActivationTicketVerdict::TinyLivePackageActivationTicketRefusedNowByInvalidOrDriftedContract,
                format!(
                    "failed to verify review-receipt session under {}: {error}",
                    config.review_receipt_session_dir.display()
                ),
            ))
        }
    };

    validate_managed_surface_session_dir(&verified_review_receipt.contract.install_root, session_dir)
        .with_context(|| {
            format!(
                "activation-ticket session dir {} overlaps the managed live target surface derived from verified review-receipt install_root {}",
                session_dir.display(),
                verified_review_receipt.contract.install_root.display()
            )
        })?;
    ensure_clean_activation_ticket_session_dir(session_dir)?;
    let paths = activation_ticket_paths(session_dir);
    let classification = classify_verified_review_receipt(&verified_review_receipt);
    let session = planned_session(
        config,
        session_dir,
        &verified_review_receipt.contract,
        classification.0,
    );
    persist_json(&paths.session_path, &session)?;
    persist_json(
        &paths.review_receipt_report_path,
        &verified_review_receipt.report_json,
    )?;
    let review_receipt_step = Some(step_artifact(
        &paths.review_receipt_report_path,
        "verify_live_package_review_receipt",
        &verified_review_receipt.verdict,
        &verified_review_receipt.reason,
        verified_review_receipt.generated_at,
    ));
    let status = PackageActivationTicketStatus {
        status_version: ACTIVATION_TICKET_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        review_receipt_session_dir: config.review_receipt_session_dir.display().to_string(),
        result: classification.0,
        reason: classification.2.clone(),
        handoff_bundle_result: verified_review_receipt
            .contract
            .handoff_bundle_result
            .clone(),
        decision_packet_result: verified_review_receipt
            .contract
            .decision_packet_result
            .clone(),
        execute_frozen_result: verified_review_receipt
            .contract
            .execute_frozen_result
            .clone(),
        current_pre_activation_gate_verdict: verified_review_receipt
            .contract
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: verified_review_receipt
            .contract
            .current_pre_activation_gate_reason
            .clone(),
        review_receipt_step: review_receipt_step.clone(),
        activation_ticket_summary: activation_ticket_summary(
            classification.0,
            &verified_review_receipt
                .contract
                .reviewed_frozen_live_cutover_controller_command_summary,
        ),
        execution_warrant_summary: execution_warrant_summary(
            classification.0,
            &verified_review_receipt
                .contract
                .reviewed_frozen_live_cutover_controller_command_summary,
        ),
        explicit_statement: ACTIVATION_TICKET_STATUS_EXPLICIT_STATEMENT.to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    Ok(report_from_status(
        config,
        session_dir,
        &paths,
        &verified_review_receipt.contract,
        classification.1,
        &status,
    ))
}

fn verify_live_package_activation_ticket_report(
    config: &Config,
) -> Result<PackageActivationTicketReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = activation_ticket_paths(session_dir);
    let session: PackageActivationTicketSession = load_json(&paths.session_path)?;
    let status: PackageActivationTicketStatus = load_json(&paths.status_path)?;
    let mut mismatches = Vec::new();

    compare_string(
        &session.review_receipt_session_dir,
        &config.review_receipt_session_dir.display().to_string(),
        "activation-ticket review_receipt_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "activation-ticket session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "activation-ticket status session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.review_receipt_session_dir,
        &config.review_receipt_session_dir.display().to_string(),
        "activation-ticket status review_receipt_session_dir",
        &mut mismatches,
    );

    let verified_review_receipt =
        match review_receipt_activation_ticket::verify_live_package_review_receipt_for_activation_ticket(
            &config.review_receipt_session_dir,
            config
                .confirmed_decision_packet_session_dir
                .as_deref()
                .ok_or_else(|| anyhow!("missing --confirm-decision-packet-session-dir for verify mode"))?,
        ) {
            Ok(value) => value,
            Err(error) => {
                let raw_contract =
                    review_receipt_activation_ticket::load_live_package_review_receipt_contract_for_activation_ticket(
                        &config.review_receipt_session_dir,
                    )?;
                return Ok(failure_report_for_verify(
                    config,
                    session_dir,
                    &raw_contract,
                    vec![format!(
                        "failed to verify review-receipt session under {}: {error}",
                        config.review_receipt_session_dir.display()
                    )],
                ));
            }
        };

    let stored_review_receipt_report: serde_json::Value = load_required_step_json(
        &status.review_receipt_step,
        &paths.review_receipt_report_path,
        "review_receipt_step",
        &mut mismatches,
    )?;
    let stored_review_receipt_report_view: NestedReviewReceiptReportView =
        serde_json::from_value(stored_review_receipt_report.clone()).with_context(|| {
            format!(
                "failed parsing archived nested review-receipt report {}",
                paths.review_receipt_report_path.display()
            )
        })?;
    let verified_review_receipt_report_view: NestedReviewReceiptReportView =
        serde_json::from_value(verified_review_receipt.report_json.clone()).with_context(|| {
            format!(
                "failed parsing freshly verified nested review-receipt report for {}",
                config.review_receipt_session_dir.display()
            )
        })?;
    compare_json_ignoring_generated_at(
        &stored_review_receipt_report,
        &verified_review_receipt.report_json,
        "activation-ticket archived review-receipt report json",
        &mut mismatches,
    );
    compare_string(
        &stored_review_receipt_report_view.mode,
        &verified_review_receipt_report_view.mode,
        "activation-ticket archived review-receipt report mode",
        &mut mismatches,
    );
    compare_string(
        &stored_review_receipt_report_view.verdict,
        &verified_review_receipt_report_view.verdict,
        "activation-ticket archived review-receipt report verdict",
        &mut mismatches,
    );
    compare_string(
        &stored_review_receipt_report_view.reason,
        &verified_review_receipt_report_view.reason,
        "activation-ticket archived review-receipt report reason",
        &mut mismatches,
    );
    if stored_review_receipt_report_view.generated_at != verified_review_receipt.generated_at {
        mismatches.push(format!(
            "activation-ticket archived review-receipt report generated_at {:?} does not match freshly verified {:?}",
            stored_review_receipt_report_view.generated_at,
            verified_review_receipt.generated_at
        ));
    }
    let expected_review_receipt_step = step_artifact(
        &paths.review_receipt_report_path,
        &verified_review_receipt_report_view.mode,
        &verified_review_receipt_report_view.verdict,
        &verified_review_receipt_report_view.reason,
        verified_review_receipt.generated_at,
    );
    compare_step_artifact(
        status.review_receipt_step.as_ref(),
        &expected_review_receipt_step,
        "activation-ticket review_receipt_step",
        &mut mismatches,
    );
    if verified_review_receipt.verdict != "tiny_live_package_review_receipt_verify_ok" {
        mismatches.push(format!(
            "nested review-receipt verification is non-green: {}",
            verified_review_receipt.reason
        ));
    }

    compare_string(
        &session.handoff_bundle_session_dir,
        &verified_review_receipt
            .contract
            .handoff_bundle_session_dir
            .display()
            .to_string(),
        "activation-ticket handoff_bundle_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.decision_packet_session_dir,
        &verified_review_receipt
            .contract
            .decision_packet_session_dir
            .display()
            .to_string(),
        "activation-ticket decision_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.execute_frozen_session_dir,
        &verified_review_receipt
            .contract
            .execute_frozen_session_dir
            .display()
            .to_string(),
        "activation-ticket execute_frozen_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.turn_green_session_dir,
        &verified_review_receipt
            .contract
            .turn_green_session_dir
            .display()
            .to_string(),
        "activation-ticket turn_green_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.launch_packet_session_dir,
        &verified_review_receipt
            .contract
            .launch_packet_session_dir
            .display()
            .to_string(),
        "activation-ticket launch_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.package_dir,
        &verified_review_receipt
            .contract
            .package_dir
            .display()
            .to_string(),
        "activation-ticket package_dir",
        &mut mismatches,
    );
    compare_string(
        &session.install_root,
        &verified_review_receipt
            .contract
            .install_root
            .display()
            .to_string(),
        "activation-ticket install_root",
        &mut mismatches,
    );
    compare_string(
        &session.target_service_name,
        &verified_review_receipt.contract.target_service_name,
        "activation-ticket target_service_name",
        &mut mismatches,
    );
    compare_string(
        &session.backend_command,
        &verified_review_receipt.contract.backend_command,
        "activation-ticket backend_command",
        &mut mismatches,
    );
    if session.wrapper_timeout_ms != verified_review_receipt.contract.wrapper_timeout_ms {
        mismatches.push(format!(
            "activation-ticket wrapper_timeout_ms {} does not match verified {}",
            session.wrapper_timeout_ms, verified_review_receipt.contract.wrapper_timeout_ms
        ));
    }
    if session.service_status_max_staleness_ms
        != verified_review_receipt
            .contract
            .service_status_max_staleness_ms
    {
        mismatches.push(format!(
            "activation-ticket service_status_max_staleness_ms {} does not match verified {}",
            session.service_status_max_staleness_ms,
            verified_review_receipt
                .contract
                .service_status_max_staleness_ms
        ));
    }
    compare_string(
        &session.verify_review_receipt_command_summary,
        &verify_review_receipt_command_summary(&config.review_receipt_session_dir),
        "activation-ticket verify_review_receipt_command_summary",
        &mut mismatches,
    );
    compare_string(
        &session.reviewed_frozen_live_cutover_controller_command_summary,
        &verified_review_receipt
            .contract
            .reviewed_frozen_live_cutover_controller_command_summary,
        "activation-ticket reviewed_frozen_live_cutover_controller_command_summary",
        &mut mismatches,
    );

    let classification = classify_verified_review_receipt(&verified_review_receipt);
    let expected_activation_ticket_summary = activation_ticket_summary(
        classification.0,
        &verified_review_receipt
            .contract
            .reviewed_frozen_live_cutover_controller_command_summary,
    );
    let expected_execution_warrant_summary = execution_warrant_summary(
        classification.0,
        &verified_review_receipt
            .contract
            .reviewed_frozen_live_cutover_controller_command_summary,
    );
    compare_string(
        &session.activation_ticket_summary,
        &expected_activation_ticket_summary,
        "activation-ticket session activation_ticket_summary",
        &mut mismatches,
    );
    compare_string(
        &session.execution_warrant_summary,
        &expected_execution_warrant_summary,
        "activation-ticket session execution_warrant_summary",
        &mut mismatches,
    );
    compare_string(
        &status.activation_ticket_summary,
        &expected_activation_ticket_summary,
        "activation-ticket status activation_ticket_summary",
        &mut mismatches,
    );
    compare_string(
        &status.execution_warrant_summary,
        &expected_execution_warrant_summary,
        "activation-ticket status execution_warrant_summary",
        &mut mismatches,
    );
    compare_string(
        &session.explicit_statement,
        ACTIVATION_TICKET_SESSION_EXPLICIT_STATEMENT,
        "activation-ticket session explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &status.explicit_statement,
        ACTIVATION_TICKET_STATUS_EXPLICIT_STATEMENT,
        "activation-ticket status explicit_statement",
        &mut mismatches,
    );
    if status.result != classification.0 {
        mismatches.push(format!(
            "stored activation-ticket result {} does not match verified {}",
            serialize_enum(&status.result),
            serialize_enum(&classification.0)
        ));
    }
    if status.handoff_bundle_result != verified_review_receipt.contract.handoff_bundle_result {
        mismatches.push(format!(
            "stored handoff_bundle_result {:?} does not match verified {:?}",
            status.handoff_bundle_result, verified_review_receipt.contract.handoff_bundle_result
        ));
    }
    if status.decision_packet_result != verified_review_receipt.contract.decision_packet_result {
        mismatches.push(format!(
            "stored decision_packet_result {:?} does not match verified {:?}",
            status.decision_packet_result, verified_review_receipt.contract.decision_packet_result
        ));
    }
    if status.execute_frozen_result != verified_review_receipt.contract.execute_frozen_result {
        mismatches.push(format!(
            "stored execute_frozen_result {:?} does not match verified {:?}",
            status.execute_frozen_result, verified_review_receipt.contract.execute_frozen_result
        ));
    }
    if status.current_pre_activation_gate_verdict
        != verified_review_receipt
            .contract
            .current_pre_activation_gate_verdict
    {
        mismatches.push(format!(
            "stored current_pre_activation_gate_verdict {:?} does not match verified {:?}",
            status.current_pre_activation_gate_verdict,
            verified_review_receipt
                .contract
                .current_pre_activation_gate_verdict
        ));
    }
    if status.current_pre_activation_gate_reason
        != verified_review_receipt
            .contract
            .current_pre_activation_gate_reason
    {
        mismatches.push(format!(
            "stored current_pre_activation_gate_reason {:?} does not match verified {:?}",
            status.current_pre_activation_gate_reason,
            verified_review_receipt
                .contract
                .current_pre_activation_gate_reason
        ));
    }

    let verdict = if mismatches.is_empty() {
        TinyLivePackageActivationTicketVerdict::TinyLivePackageActivationTicketVerifyOk
    } else {
        TinyLivePackageActivationTicketVerdict::TinyLivePackageActivationTicketVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "review-receipt-native activation ticket under {} is coherent and correctly binds the final immutable execution warrant to verified review-receipt truth",
            session_dir.display()
        )
    } else {
        mismatches.first().cloned().unwrap_or_else(|| {
            "review-receipt-native activation ticket verification failed".to_string()
        })
    };

    Ok(PackageActivationTicketReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_activation_ticket".to_string(),
        verdict,
        reason,
        review_receipt_session_dir: config.review_receipt_session_dir.display().to_string(),
        handoff_bundle_session_dir: Some(
            verified_review_receipt
                .contract
                .handoff_bundle_session_dir
                .display()
                .to_string(),
        ),
        decision_packet_session_dir: Some(
            verified_review_receipt
                .contract
                .decision_packet_session_dir
                .display()
                .to_string(),
        ),
        execute_frozen_session_dir: Some(
            verified_review_receipt
                .contract
                .execute_frozen_session_dir
                .display()
                .to_string(),
        ),
        turn_green_session_dir: Some(
            verified_review_receipt
                .contract
                .turn_green_session_dir
                .display()
                .to_string(),
        ),
        launch_packet_session_dir: Some(
            verified_review_receipt
                .contract
                .launch_packet_session_dir
                .display()
                .to_string(),
        ),
        package_dir: Some(
            verified_review_receipt
                .contract
                .package_dir
                .display()
                .to_string(),
        ),
        install_root: Some(
            verified_review_receipt
                .contract
                .install_root
                .display()
                .to_string(),
        ),
        target_service_name: Some(verified_review_receipt.contract.target_service_name.clone()),
        backend_command: Some(verified_review_receipt.contract.backend_command.clone()),
        wrapper_timeout_ms: Some(verified_review_receipt.contract.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(
            verified_review_receipt
                .contract
                .service_status_max_staleness_ms,
        ),
        session_dir: Some(session_dir.display().to_string()),
        activation_ticket_session_path: Some(paths.session_path.display().to_string()),
        activation_ticket_status_path: Some(paths.status_path.display().to_string()),
        result: Some(serialize_enum(&status.result)),
        handoff_bundle_result: status.handoff_bundle_result.clone(),
        decision_packet_result: status.decision_packet_result.clone(),
        execute_frozen_result: status.execute_frozen_result.clone(),
        review_receipt_step: Some(expected_review_receipt_step),
        verify_review_receipt_command_summary: Some(verify_review_receipt_command_summary(
            &config.review_receipt_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            verified_review_receipt
                .contract
                .reviewed_frozen_live_cutover_controller_command_summary,
        ),
        activation_ticket_summary: Some(expected_activation_ticket_summary),
        execution_warrant_summary: Some(expected_execution_warrant_summary),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        verification_mismatches: mismatches,
        explicit_statement: ACTIVATION_TICKET_VERIFY_EXPLICIT_STATEMENT.to_string(),
    })
}

fn verify_review_receipt_command_summary(review_receipt_session_dir: &Path) -> String {
    format!(
        "verify immutable review-receipt session under {} before freezing the final activation ticket",
        review_receipt_session_dir.display()
    )
}

fn classify_activation_ticket_contract(
    contract: &review_receipt_activation_ticket::LivePackageReviewReceiptContractView,
    assume_verified: bool,
) -> (
    LivePackageActivationTicketResult,
    TinyLivePackageActivationTicketVerdict,
    String,
) {
    if !assume_verified {
        return (
            LivePackageActivationTicketResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageActivationTicketVerdict::TinyLivePackageActivationTicketRefusedNowByInvalidOrDriftedContract,
            "the review-receipt chain is not verified, so the activation ticket remains refused".to_string(),
        );
    }
    match contract.result.as_deref() {
        Some("refused_now_by_stage3") => (
            LivePackageActivationTicketResult::RefusedNowByStage3,
            TinyLivePackageActivationTicketVerdict::TinyLivePackageActivationTicketRefusedNowByStage3,
            "the immutable review receipt remains refused now by Stage 3, so the activation ticket remains an explicit refusal record".to_string(),
        ),
        Some("refused_now_by_pre_activation_gate") => (
            LivePackageActivationTicketResult::RefusedNowByPreActivationGate,
            TinyLivePackageActivationTicketVerdict::TinyLivePackageActivationTicketRefusedNowByPreActivationGate,
            "the immutable review receipt remains refused now by the pre-activation gate, so the activation ticket remains an explicit refusal record".to_string(),
        ),
        Some("ready_for_manual_go_live_signoff") => (
            LivePackageActivationTicketResult::ReadyForManualExecutionWhenGateTurnsGreen,
            TinyLivePackageActivationTicketVerdict::TinyLivePackageActivationTicketReadyForManualExecutionWhenGateTurnsGreen,
            "the immutable review receipt is coherent and this activation ticket is ready for manual execution when gate truth turns green".to_string(),
        ),
        _ => (
            LivePackageActivationTicketResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageActivationTicketVerdict::TinyLivePackageActivationTicketRefusedNowByInvalidOrDriftedContract,
            "the verified review-receipt chain is invalid, drifted, or non-runnable, so the activation ticket remains refused".to_string(),
        ),
    }
}

fn classify_verified_review_receipt(
    verified: &review_receipt_activation_ticket::VerifiedLivePackageReviewReceiptActivationTicketStep,
) -> (
    LivePackageActivationTicketResult,
    TinyLivePackageActivationTicketVerdict,
    String,
) {
    if verified.verdict != "tiny_live_package_review_receipt_verify_ok" {
        return (
            LivePackageActivationTicketResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageActivationTicketVerdict::TinyLivePackageActivationTicketRefusedNowByInvalidOrDriftedContract,
            format!(
                "nested review-receipt verification is non-green: {}",
                verified.reason
            ),
        );
    }
    classify_activation_ticket_contract(&verified.contract, true)
}

fn activation_ticket_summary(
    result: LivePackageActivationTicketResult,
    reviewed_frozen_live_cutover_controller_command_summary: &str,
) -> String {
    match result {
        LivePackageActivationTicketResult::RefusedNowByStage3 => {
            "Activation ticket outcome: the immutable reviewed chain remains refused now by Stage 3; archive this ticket as warrant-denied evidence only and do not run the frozen controller.".to_string()
        }
        LivePackageActivationTicketResult::RefusedNowByPreActivationGate => {
            "Activation ticket outcome: the immutable reviewed chain remains refused now by the pre-activation gate; archive this ticket as warrant-denied evidence only and do not run the frozen controller.".to_string()
        }
        LivePackageActivationTicketResult::RefusedNowByInvalidOrDriftedContract => {
            "Activation ticket outcome: the immutable reviewed chain is invalid or drifted; archive this ticket for audit only and mint a new coherent reviewed receipt before any later execution warrant.".to_string()
        }
        LivePackageActivationTicketResult::ReadyForManualExecutionWhenGateTurnsGreen => format!(
            "Activation ticket outcome: this immutable warrant is ready for manual execution when gate truth turns green; the exact reviewed frozen controller command remains: {reviewed_frozen_live_cutover_controller_command_summary}"
        ),
    }
}

fn execution_warrant_summary(
    result: LivePackageActivationTicketResult,
    reviewed_frozen_live_cutover_controller_command_summary: &str,
) -> String {
    match result {
        LivePackageActivationTicketResult::RefusedNowByStage3
        | LivePackageActivationTicketResult::RefusedNowByPreActivationGate
        | LivePackageActivationTicketResult::RefusedNowByInvalidOrDriftedContract => format!(
            "Execution warrant classification: refused now; the reviewed frozen controller remains non-runnable today. Reviewed command remains archived only: {reviewed_frozen_live_cutover_controller_command_summary}"
        ),
        LivePackageActivationTicketResult::ReadyForManualExecutionWhenGateTurnsGreen => format!(
            "Execution warrant classification: ready for manual execution when gate truth turns green; exact reviewed frozen controller command: {reviewed_frozen_live_cutover_controller_command_summary}"
        ),
    }
}

fn activation_ticket_paths(session_dir: &Path) -> PackageActivationTicketPaths {
    PackageActivationTicketPaths {
        session_path: session_dir
            .join("tiny_live_activation_package_activation_ticket.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_activation_ticket.status.json"),
        review_receipt_report_path: session_dir
            .join("tiny_live_activation_package_activation_ticket.review_receipt.report.json"),
    }
}

fn ensure_clean_activation_ticket_session_dir(session_dir: &Path) -> Result<()> {
    if session_dir.exists() {
        bail!(
            "refusing to overwrite existing activation-ticket session dir {}",
            session_dir.display()
        );
    }
    fs::create_dir_all(session_dir).with_context(|| {
        format!(
            "failed creating activation-ticket session dir {}",
            session_dir.display()
        )
    })
}

fn planned_session(
    config: &Config,
    session_dir: &Path,
    contract: &review_receipt_activation_ticket::LivePackageReviewReceiptContractView,
    result: LivePackageActivationTicketResult,
) -> PackageActivationTicketSession {
    PackageActivationTicketSession {
        session_version: ACTIVATION_TICKET_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        review_receipt_session_dir: config.review_receipt_session_dir.display().to_string(),
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
        verify_review_receipt_command_summary: verify_review_receipt_command_summary(
            &config.review_receipt_session_dir,
        ),
        reviewed_frozen_live_cutover_controller_command_summary: contract
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        activation_ticket_summary: activation_ticket_summary(
            result,
            &contract.reviewed_frozen_live_cutover_controller_command_summary,
        ),
        execution_warrant_summary: execution_warrant_summary(
            result,
            &contract.reviewed_frozen_live_cutover_controller_command_summary,
        ),
        explicit_statement: ACTIVATION_TICKET_SESSION_EXPLICIT_STATEMENT.to_string(),
    }
}

fn report_from_status(
    config: &Config,
    session_dir: &Path,
    paths: &PackageActivationTicketPaths,
    contract: &review_receipt_activation_ticket::LivePackageReviewReceiptContractView,
    verdict: TinyLivePackageActivationTicketVerdict,
    status: &PackageActivationTicketStatus,
) -> PackageActivationTicketReport {
    PackageActivationTicketReport {
        generated_at: Utc::now(),
        mode: "run_live_package_activation_ticket".to_string(),
        verdict,
        reason: status.reason.clone(),
        review_receipt_session_dir: config.review_receipt_session_dir.display().to_string(),
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
        activation_ticket_session_path: Some(paths.session_path.display().to_string()),
        activation_ticket_status_path: Some(paths.status_path.display().to_string()),
        result: Some(serialize_enum(&status.result)),
        handoff_bundle_result: status.handoff_bundle_result.clone(),
        decision_packet_result: status.decision_packet_result.clone(),
        execute_frozen_result: status.execute_frozen_result.clone(),
        review_receipt_step: status.review_receipt_step.clone(),
        verify_review_receipt_command_summary: Some(verify_review_receipt_command_summary(
            &config.review_receipt_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            contract
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        activation_ticket_summary: Some(status.activation_ticket_summary.clone()),
        execution_warrant_summary: Some(status.execution_warrant_summary.clone()),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        verification_mismatches: Vec::new(),
        explicit_statement: status.explicit_statement.clone(),
    }
}

fn failure_report_without_contract(
    config: &Config,
    verdict: TinyLivePackageActivationTicketVerdict,
    reason: String,
) -> PackageActivationTicketReport {
    PackageActivationTicketReport {
        generated_at: Utc::now(),
        mode: match config.mode {
            Mode::PlanLivePackageActivationTicket => "plan_live_package_activation_ticket".to_string(),
            Mode::RenderLivePackageActivationTicket => {
                "render_live_package_activation_ticket".to_string()
            }
            Mode::RunLivePackageActivationTicket => "run_live_package_activation_ticket".to_string(),
            Mode::VerifyLivePackageActivationTicket => {
                "verify_live_package_activation_ticket".to_string()
            }
        },
        verdict,
        reason,
        review_receipt_session_dir: config.review_receipt_session_dir.display().to_string(),
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
        activation_ticket_session_path: None,
        activation_ticket_status_path: None,
        result: Some("refused_now_by_invalid_or_drifted_contract".to_string()),
        handoff_bundle_result: None,
        decision_packet_result: None,
        execute_frozen_result: None,
        review_receipt_step: None,
        verify_review_receipt_command_summary: Some(verify_review_receipt_command_summary(
            &config.review_receipt_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: None,
        activation_ticket_summary: None,
        execution_warrant_summary: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this immutable activation ticket refused before persisting because the review-receipt artifact could not be verified coherently"
                .to_string(),
    }
}

fn failure_report_for_verify(
    config: &Config,
    session_dir: &Path,
    contract: &review_receipt_activation_ticket::LivePackageReviewReceiptContractView,
    mismatches: Vec<String>,
) -> PackageActivationTicketReport {
    let paths = activation_ticket_paths(session_dir);
    let reason = mismatches.first().cloned().unwrap_or_else(|| {
        "review-receipt-native activation ticket verification failed".to_string()
    });
    PackageActivationTicketReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_activation_ticket".to_string(),
        verdict: TinyLivePackageActivationTicketVerdict::TinyLivePackageActivationTicketVerifyInvalid,
        reason,
        review_receipt_session_dir: config.review_receipt_session_dir.display().to_string(),
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
        activation_ticket_session_path: Some(paths.session_path.display().to_string()),
        activation_ticket_status_path: Some(paths.status_path.display().to_string()),
        result: None,
        handoff_bundle_result: contract.handoff_bundle_result.clone(),
        decision_packet_result: contract.decision_packet_result.clone(),
        execute_frozen_result: contract.execute_frozen_result.clone(),
        review_receipt_step: None,
        verify_review_receipt_command_summary: Some(verify_review_receipt_command_summary(
            &config.review_receipt_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            contract
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        activation_ticket_summary: Some(activation_ticket_summary(
            classify_activation_ticket_contract(contract, false).0,
            &contract.reviewed_frozen_live_cutover_controller_command_summary,
        )),
        execution_warrant_summary: Some(execution_warrant_summary(
            classify_activation_ticket_contract(contract, false).0,
            &contract.reviewed_frozen_live_cutover_controller_command_summary,
        )),
        current_pre_activation_gate_verdict: contract.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: contract.current_pre_activation_gate_reason.clone(),
        verification_mismatches: mismatches,
        explicit_statement:
            "this verification failure means the immutable activation ticket no longer proves the exact reviewed frozen controller context"
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
            "failed writing activation-ticket script to {}",
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
) -> PackageActivationTicketStepArtifact {
    PackageActivationTicketStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn load_required_step_json<T: for<'de> Deserialize<'de>>(
    step: &Option<PackageActivationTicketStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("{label} is missing from activation-ticket status"))?;
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
    actual: Option<&PackageActivationTicketStepArtifact>,
    expected: &PackageActivationTicketStepArtifact,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    let Some(actual) = actual else {
        mismatches.push(format!("{label} is missing from activation-ticket status"));
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

fn render_report_lines(report: &PackageActivationTicketReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "review_receipt_session_dir={}",
            report.review_receipt_session_dir
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
    if let Some(value) = &report.activation_ticket_summary {
        lines.push(format!("activation_ticket_summary={value}"));
    }
    if let Some(value) = &report.execution_warrant_summary {
        lines.push(format!("execution_warrant_summary={value}"));
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
            "activation_ticket_ready",
            review_receipt_verify_report(
                "ready_for_manual_go_live_signoff",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_activation_ticket_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageActivationTicketVerdict::TinyLivePackageActivationTicketReadyForManualExecutionWhenGateTurnsGreen
        );
        assert_eq!(
            run.result.as_deref(),
            Some("ready_for_manual_execution_when_gate_turns_green")
        );

        let verify =
            verify_live_package_activation_ticket_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageActivationTicketVerdict::TinyLivePackageActivationTicketVerifyOk,
            "{:#?}",
            verify.verification_mismatches
        );
    }

    #[test]
    fn stage3_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "activation_ticket_stage3",
            review_receipt_verify_report(
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "blocked",
                "stage3 blocked",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_activation_ticket_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageActivationTicketVerdict::TinyLivePackageActivationTicketRefusedNowByStage3
        );
    }

    #[test]
    fn pre_activation_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "activation_ticket_pre_activation",
            review_receipt_verify_report(
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
                "blocked",
                "pre-activation blocked",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_activation_ticket_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageActivationTicketVerdict::TinyLivePackageActivationTicketRefusedNowByPreActivationGate
        );
    }

    #[test]
    fn drifted_review_receipt_contract_is_refused() {
        let fixture = fake_command_fixture(
            "activation_ticket_drifted",
            review_receipt_verify_report(
                "refused_now_by_invalid_or_drifted_contract",
                "refused_now_by_invalid_or_drifted_contract",
                "refused_now_by_invalid_or_drifted_contract",
                "refused_now_by_invalid_or_drifted_contract",
                "green",
                "contract drifted",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_activation_ticket_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageActivationTicketVerdict::TinyLivePackageActivationTicketRefusedNowByInvalidOrDriftedContract
        );
    }

    #[test]
    fn run_refuses_managed_surface_overlap_before_writing_any_artifacts() {
        let fixture = fake_command_fixture(
            "activation_ticket_overlap",
            review_receipt_verify_report(
                "ready_for_manual_go_live_signoff",
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
        let unsafe_session_dir = managed_surface
            .session_dir
            .join("activation-ticket-session");
        let config = Config {
            mode: Mode::RunLivePackageActivationTicket,
            review_receipt_session_dir: fixture.review_receipt_session_dir.clone(),
            confirmed_decision_packet_session_dir: Some(
                fixture.decision_packet_session_dir.clone(),
            ),
            session_dir: Some(unsafe_session_dir.clone()),
            output_path: None,
            json: true,
        };

        let error = run_live_package_activation_ticket_report(&config).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("overlaps the managed live target surface"),
            "{error:#}"
        );
        let paths = activation_ticket_paths(&unsafe_session_dir);
        assert!(!paths.session_path.exists());
        assert!(!paths.status_path.exists());
    }

    #[test]
    fn verify_rejects_tampered_review_receipt_step_path() {
        let fixture = fake_command_fixture(
            "activation_ticket_tampered_step",
            review_receipt_verify_report(
                "ready_for_manual_go_live_signoff",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_activation_ticket_report(&fixture.run_config()).unwrap();
        let paths = activation_ticket_paths(&fixture.activation_ticket_session_dir);
        let mut status: PackageActivationTicketStatus = load_json(&paths.status_path).unwrap();
        let foreign = fixture
            .fixture_dir
            .join("foreign.review-receipt.report.json");
        fs::write(&foreign, "{}").unwrap();
        status.review_receipt_step.as_mut().unwrap().path = foreign.display().to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify =
            verify_live_package_activation_ticket_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageActivationTicketVerdict::TinyLivePackageActivationTicketVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_activation_ticket_text() {
        let fixture = fake_command_fixture(
            "activation_ticket_tampered_text",
            review_receipt_verify_report(
                "ready_for_manual_go_live_signoff",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_activation_ticket_report(&fixture.run_config()).unwrap();
        let paths = activation_ticket_paths(&fixture.activation_ticket_session_dir);
        let mut session: PackageActivationTicketSession = load_json(&paths.session_path).unwrap();
        session.activation_ticket_summary = "tampered summary".to_string();
        persist_json(&paths.session_path, &session).unwrap();

        let verify =
            verify_live_package_activation_ticket_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageActivationTicketVerdict::TinyLivePackageActivationTicketVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_nested_review_receipt_report_content() {
        let fixture = fake_command_fixture(
            "activation_ticket_tampered_nested_report",
            review_receipt_verify_report(
                "ready_for_manual_go_live_signoff",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_activation_ticket_report(&fixture.run_config()).unwrap();
        let paths = activation_ticket_paths(&fixture.activation_ticket_session_dir);
        let mut report: serde_json::Value = load_json(&paths.review_receipt_report_path).unwrap();
        report.as_object_mut().unwrap().insert(
            "reason".to_string(),
            json!("tampered nested review receipt reason"),
        );
        persist_json(&paths.review_receipt_report_path, &report).unwrap();

        let verify =
            verify_live_package_activation_ticket_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageActivationTicketVerdict::TinyLivePackageActivationTicketVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_retimed_nested_review_receipt_report_and_step_generated_at() {
        let fixture = fake_command_fixture(
            "activation_ticket_retimed_nested_report",
            review_receipt_verify_report(
                "ready_for_manual_go_live_signoff",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_activation_ticket_report(&fixture.run_config()).unwrap();
        let forged_generated_at = Utc::now() + chrono::Duration::seconds(17);

        let paths = activation_ticket_paths(&fixture.activation_ticket_session_dir);
        let mut report: serde_json::Value = load_json(&paths.review_receipt_report_path).unwrap();
        report
            .as_object_mut()
            .unwrap()
            .insert("generated_at".to_string(), json!(forged_generated_at));
        persist_json(&paths.review_receipt_report_path, &report).unwrap();

        let mut status: PackageActivationTicketStatus = load_json(&paths.status_path).unwrap();
        status.review_receipt_step.as_mut().unwrap().generated_at = forged_generated_at;
        persist_json(&paths.status_path, &status).unwrap();

        let verify =
            verify_live_package_activation_ticket_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageActivationTicketVerdict::TinyLivePackageActivationTicketVerifyInvalid
        );
    }

    struct FakeCommandFixture {
        fixture_dir: PathBuf,
        bin_dir: PathBuf,
        review_receipt_session_dir: PathBuf,
        activation_ticket_session_dir: PathBuf,
        decision_packet_session_dir: PathBuf,
        install_root: PathBuf,
    }

    impl FakeCommandFixture {
        fn run_config(&self) -> Config {
            Config {
                mode: Mode::RunLivePackageActivationTicket,
                review_receipt_session_dir: self.review_receipt_session_dir.clone(),
                confirmed_decision_packet_session_dir: Some(
                    self.decision_packet_session_dir.clone(),
                ),
                session_dir: Some(self.activation_ticket_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn verify_config(&self) -> Config {
            Config {
                mode: Mode::VerifyLivePackageActivationTicket,
                review_receipt_session_dir: self.review_receipt_session_dir.clone(),
                confirmed_decision_packet_session_dir: Some(
                    self.decision_packet_session_dir.clone(),
                ),
                session_dir: Some(self.activation_ticket_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }
    }

    fn fake_command_fixture(label: &str, verify_report: serde_json::Value) -> FakeCommandFixture {
        let fixture_dir = temp_dir(label);
        let bin_dir = fixture_dir.join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        let review_receipt_session_dir = fixture_dir.join("review-receipt-session");
        fs::create_dir_all(&review_receipt_session_dir).unwrap();
        let activation_ticket_session_dir = fixture_dir.join("activation-ticket-session");
        let handoff_bundle_session_dir = fixture_dir.join("handoff-bundle-session");
        let decision_packet_session_dir = fixture_dir.join("decision-packet-session");
        let execute_frozen_session_dir = fixture_dir.join("execute-frozen-session");
        let turn_green_session_dir = fixture_dir.join("turn-green-session");
        let launch_packet_session_dir = fixture_dir.join("launch-packet-session");
        let package_dir = fixture_dir.join("package");
        let install_root = fixture_dir.join("live-root");
        let backend_command = fixture_dir.join("fake-backend");
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
                "verify_handoff_bundle_command_summary": format!(
                    "verify immutable handoff-bundle session under {} before freezing the final operator review receipt",
                    handoff_bundle_session_dir.display()
                ),
                "reviewed_frozen_live_cutover_controller_command_summary": format!(
                    "copybot_tiny_live_activation_package_live_cutover --package-dir {} --install-root {} --target-service solana-copy-bot.service --backend-command {} --wrapper-timeout-ms 1200 --service-status-max-staleness-ms 4500 --session-dir {} --run-live-package-cutover",
                    shell_escape_path(&package_dir),
                    shell_escape_path(&install_root),
                    shell_escape_path(&backend_command),
                    shell_escape_path(&fixture_dir.join("frozen-live-cutover-session")),
                ),
                "review_receipt_summary": "Review outcome: this operator receipt is ready for final manual go-live signoff when gate truth turns green.",
                "checklist_acknowledgement_summary": "Checklist acknowledgement: reviewed for future manual go-live signoff only.",
                "runbook_acknowledgement_summary": "Runbook acknowledgement: reviewed for future manual go-live signoff only.",
                "explicit_statement": "review-receipt statement"
            }),
        )
        .unwrap();
        persist_json(
            &review_receipt_session_dir.join("tiny_live_activation_package_review_receipt.status.json"),
            &json!({
                "session_dir": review_receipt_session_dir.display().to_string(),
                "handoff_bundle_session_dir": handoff_bundle_session_dir.display().to_string(),
                "result": verify_report.get("result").and_then(|value| value.as_str()).unwrap(),
                "handoff_bundle_result": verify_report.get("handoff_bundle_result").and_then(|value| value.as_str()).unwrap(),
                "decision_packet_result": verify_report.get("decision_packet_result").and_then(|value| value.as_str()).unwrap(),
                "execute_frozen_result": verify_report.get("execute_frozen_result").and_then(|value| value.as_str()).unwrap(),
                "current_pre_activation_gate_verdict": verify_report.get("current_pre_activation_gate_verdict").and_then(|value| value.as_str()),
                "current_pre_activation_gate_reason": verify_report.get("current_pre_activation_gate_reason").and_then(|value| value.as_str()),
                "review_receipt_summary": "Review outcome: this operator receipt is ready for final manual go-live signoff when gate truth turns green.",
                "checklist_acknowledgement_summary": "Checklist acknowledgement: reviewed for future manual go-live signoff only.",
                "runbook_acknowledgement_summary": "Runbook acknowledgement: reviewed for future manual go-live signoff only.",
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

        let mut verify_report = verify_report;
        bind_review_receipt_report_to_fixture(
            &mut verify_report,
            &handoff_bundle_session_dir,
            &decision_packet_session_dir,
            &execute_frozen_session_dir,
            &turn_green_session_dir,
            &launch_packet_session_dir,
            &package_dir,
            &install_root,
            &backend_command,
        );
        let invalid_verify_report = invalid_review_receipt_verify_report(&verify_report);
        let verify_path = fixture_dir.join("review-receipt.verify.json");
        let invalid_verify_path = fixture_dir.join("review-receipt.verify.invalid.json");
        persist_json(&verify_path, &verify_report).unwrap();
        persist_json(&invalid_verify_path, &invalid_verify_report).unwrap();
        write_fake_review_receipt_verify_command(
            &bin_dir.join("copybot_tiny_live_activation_package_review_receipt"),
            &verify_path,
            &invalid_verify_path,
            &handoff_bundle_session_dir,
            &decision_packet_session_dir,
            &review_receipt_session_dir,
        );

        FakeCommandFixture {
            fixture_dir,
            bin_dir,
            review_receipt_session_dir,
            activation_ticket_session_dir,
            decision_packet_session_dir,
            install_root,
        }
    }

    fn bind_review_receipt_report_to_fixture(
        report: &mut serde_json::Value,
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
            .expect("review-receipt report fixture must be an object");
        object.insert(
            "mode".to_string(),
            json!("verify_live_package_review_receipt"),
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
            "verify_handoff_bundle_command_summary".to_string(),
            json!(format!(
                "verify immutable handoff-bundle session under {} before freezing the final operator review receipt",
                handoff_bundle_session_dir.display()
            )),
        );
        object.insert(
            "reviewed_frozen_live_cutover_controller_command_summary".to_string(),
            json!(format!(
                "copybot_tiny_live_activation_package_live_cutover --package-dir {} --install-root {} --target-service solana-copy-bot.service --backend-command {} --wrapper-timeout-ms 1200 --service-status-max-staleness-ms 4500 --session-dir {} --run-live-package-cutover",
                shell_escape_path(package_dir),
                shell_escape_path(install_root),
                shell_escape_path(backend_command),
                shell_escape_path(&handoff_bundle_session_dir.join("frozen-live-cutover-session")),
            )),
        );
        object.insert(
            "review_receipt_summary".to_string(),
            json!("Review outcome: this operator receipt is ready for final manual go-live signoff when gate truth turns green."),
        );
        object.insert(
            "checklist_acknowledgement_summary".to_string(),
            json!("Checklist acknowledgement: reviewed for future manual go-live signoff only."),
        );
        object.insert(
            "runbook_acknowledgement_summary".to_string(),
            json!("Runbook acknowledgement: reviewed for future manual go-live signoff only."),
        );
        object.insert(
            "explicit_statement".to_string(),
            json!("review-receipt verify statement"),
        );
    }

    fn review_receipt_verify_report(
        result: &str,
        handoff_bundle_result: &str,
        decision_packet_result: &str,
        execute_frozen_result: &str,
        gate_verdict: &str,
        gate_reason: &str,
    ) -> serde_json::Value {
        json!({
            "generated_at": Utc::now(),
            "mode": "verify_live_package_review_receipt",
            "verdict": "tiny_live_package_review_receipt_verify_ok",
            "reason": "review-receipt chain is coherent",
            "result": result,
            "handoff_bundle_result": handoff_bundle_result,
            "decision_packet_result": decision_packet_result,
            "execute_frozen_result": execute_frozen_result,
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason,
        })
    }

    fn invalid_review_receipt_verify_report(
        success_report: &serde_json::Value,
    ) -> serde_json::Value {
        let mut report = success_report.clone();
        let object = report
            .as_object_mut()
            .expect("review-receipt verify report fixture must be an object");
        object.insert(
            "verdict".to_string(),
            json!("tiny_live_package_review_receipt_verify_invalid"),
        );
        object.insert(
            "reason".to_string(),
            json!("stored decision_packet_session_dir does not match the trusted review-receipt contract"),
        );
        object.insert(
            "result".to_string(),
            json!("refused_now_by_invalid_or_drifted_contract"),
        );
        report
    }

    fn write_fake_review_receipt_verify_command(
        path: &Path,
        verify_json_path: &Path,
        invalid_verify_path: &Path,
        expected_handoff_bundle_session_dir: &Path,
        expected_decision_packet_session_dir: &Path,
        review_receipt_session_dir: &Path,
    ) {
        let expected_handoff_bundle_session_dir =
            expected_handoff_bundle_session_dir.display().to_string();
        let expected_decision_packet_session_dir =
            expected_decision_packet_session_dir.display().to_string();
        let review_receipt_session_dir = review_receipt_session_dir.display().to_string();
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --handoff-bundle-session-dir {expected_handoff_bundle_session_dir} \"*) : ;;\n  *) echo 'unexpected handoff-bundle-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --confirm-decision-packet-session-dir {expected_decision_packet_session_dir} \"*) : ;;\n  *) echo 'unexpected decision-packet-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {review_receipt_session_dir} \"*) : ;;\n  *) echo 'unexpected review-receipt session dir' >&2; exit 1;;\nesac\nsession_json='{review_receipt_session_dir}/tiny_live_activation_package_review_receipt.session.json'\nif grep -F '\"decision_packet_session_dir\": \"{expected_decision_packet_session_dir}\"' \"$session_json\" >/dev/null; then\n  cat '{}'\nelse\n  cat '{}'\nfi\n",
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
