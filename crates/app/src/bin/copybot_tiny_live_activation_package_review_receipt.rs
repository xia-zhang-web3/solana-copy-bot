use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_app::tiny_live_activation::install_target_managed_surface::validate_turn_green_session_dir as validate_managed_surface_session_dir;
use copybot_app::tiny_live_activation::package_handoff_bundle_review_receipt as handoff_bundle_review_receipt;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: copybot_tiny_live_activation_package_review_receipt --handoff-bundle-session-dir <path> [--confirm-decision-packet-session-dir <path>] [--session-dir <path>] [--output <path>] [--json] (--plan-live-package-review-receipt | --render-live-package-review-receipt | --run-live-package-review-receipt | --verify-live-package-review-receipt)";
const REVIEW_RECEIPT_SESSION_VERSION: &str = "1";
const REVIEW_RECEIPT_STATUS_VERSION: &str = "1";
const REVIEW_RECEIPT_SESSION_EXPLICIT_STATEMENT: &str =
    "the verified handoff-bundle session is the only input here; this immutable review receipt freezes one operator signoff artifact over the exact frozen live cutover contract without enabling production execution";
const REVIEW_RECEIPT_STATUS_EXPLICIT_STATEMENT: &str =
    "this operator review receipt is archival and read-only; it never executes or enables the frozen controller";
const REVIEW_RECEIPT_VERIFY_EXPLICIT_STATEMENT: &str =
    "this verification binds one immutable operator review receipt to verified handoff-bundle truth and exact nested frozen execution evidence";

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
    handoff_bundle_session_dir: PathBuf,
    confirmed_decision_packet_session_dir: Option<PathBuf>,
    session_dir: Option<PathBuf>,
    output_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanLivePackageReviewReceipt,
    RenderLivePackageReviewReceipt,
    RunLivePackageReviewReceipt,
    VerifyLivePackageReviewReceipt,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageReviewReceiptVerdict {
    TinyLivePackageReviewReceiptPlanReady,
    TinyLivePackageReviewReceiptRendered,
    TinyLivePackageReviewReceiptRefusedNowByStage3,
    TinyLivePackageReviewReceiptRefusedNowByPreActivationGate,
    TinyLivePackageReviewReceiptRefusedNowByInvalidOrDriftedContract,
    TinyLivePackageReviewReceiptReadyForManualGoLiveSignoff,
    TinyLivePackageReviewReceiptVerifyOk,
    TinyLivePackageReviewReceiptVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageReviewReceiptResult {
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    RefusedNowByInvalidOrDriftedContract,
    ReadyForManualGoLiveSignoff,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageReviewReceiptStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageReviewReceiptSession {
    session_version: String,
    planned_at: DateTime<Utc>,
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
    verify_handoff_bundle_command_summary: String,
    reviewed_frozen_live_cutover_controller_command_summary: String,
    review_receipt_summary: String,
    checklist_acknowledgement_summary: String,
    runbook_acknowledgement_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageReviewReceiptStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    handoff_bundle_session_dir: String,
    result: LivePackageReviewReceiptResult,
    reason: String,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    handoff_bundle_step: Option<PackageReviewReceiptStepArtifact>,
    review_receipt_summary: String,
    checklist_acknowledgement_summary: String,
    runbook_acknowledgement_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageReviewReceiptPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    handoff_bundle_report_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageReviewReceiptReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageReviewReceiptVerdict,
    reason: String,
    handoff_bundle_session_dir: String,
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
    review_receipt_session_path: Option<String>,
    review_receipt_status_path: Option<String>,
    result: Option<String>,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    handoff_bundle_step: Option<PackageReviewReceiptStepArtifact>,
    verify_handoff_bundle_command_summary: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    review_receipt_summary: Option<String>,
    checklist_acknowledgement_summary: Option<String>,
    runbook_acknowledgement_summary: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    verification_mismatches: Vec<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct NestedHandoffBundleReportView {
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
    let mut handoff_bundle_session_dir: Option<PathBuf> = None;
    let mut confirmed_decision_packet_session_dir: Option<PathBuf> = None;
    let mut session_dir: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut json = false;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--handoff-bundle-session-dir" => {
                handoff_bundle_session_dir = Some(PathBuf::from(parse_string_arg(
                    "--handoff-bundle-session-dir",
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
            "--plan-live-package-review-receipt" => {
                set_mode(&mut mode, Mode::PlanLivePackageReviewReceipt, arg.as_str())?
            }
            "--render-live-package-review-receipt" => set_mode(
                &mut mode,
                Mode::RenderLivePackageReviewReceipt,
                arg.as_str(),
            )?,
            "--run-live-package-review-receipt" => {
                set_mode(&mut mode, Mode::RunLivePackageReviewReceipt, arg.as_str())?
            }
            "--verify-live-package-review-receipt" => set_mode(
                &mut mode,
                Mode::VerifyLivePackageReviewReceipt,
                arg.as_str(),
            )?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let Some(mode) = mode else {
        return Ok(None);
    };
    if matches!(mode, Mode::RenderLivePackageReviewReceipt) && output_path.is_none() {
        bail!("missing --output for --render-live-package-review-receipt");
    }
    if matches!(
        mode,
        Mode::RunLivePackageReviewReceipt | Mode::VerifyLivePackageReviewReceipt
    ) && session_dir.is_none()
    {
        bail!("missing --session-dir for run/verify mode");
    }
    if matches!(
        mode,
        Mode::RunLivePackageReviewReceipt | Mode::VerifyLivePackageReviewReceipt
    ) && confirmed_decision_packet_session_dir.is_none()
    {
        bail!("missing --confirm-decision-packet-session-dir for run/verify mode");
    }
    let handoff_bundle_session_dir = handoff_bundle_session_dir
        .ok_or_else(|| anyhow!("missing --handoff-bundle-session-dir"))?;
    Ok(Some(Config {
        mode,
        handoff_bundle_session_dir,
        confirmed_decision_packet_session_dir,
        session_dir,
        output_path,
        json,
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanLivePackageReviewReceipt => plan_live_package_review_receipt_report(&config)?,
        Mode::RenderLivePackageReviewReceipt => render_live_package_review_receipt_report(&config)?,
        Mode::RunLivePackageReviewReceipt => run_live_package_review_receipt_report(&config)?,
        Mode::VerifyLivePackageReviewReceipt => verify_live_package_review_receipt_report(&config)?,
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_live_package_review_receipt_report(config: &Config) -> Result<PackageReviewReceiptReport> {
    let (handoff_bundle, classification) =
        match config.confirmed_decision_packet_session_dir.as_deref() {
            Some(confirmed_decision_packet_session_dir) => {
                match handoff_bundle_review_receipt::verify_live_package_handoff_bundle_for_review_receipt(
                    &config.handoff_bundle_session_dir,
                    confirmed_decision_packet_session_dir,
                ) {
                    Ok(verified) => {
                        let classification = classify_verified_handoff_bundle(&verified);
                        (verified.contract, classification)
                    }
                    Err(_) => {
                        let raw_contract = handoff_bundle_review_receipt::load_live_package_handoff_bundle_contract_for_review_receipt(
                            &config.handoff_bundle_session_dir,
                        )?;
                        let classification = classify_review_receipt_contract(&raw_contract, false);
                        (raw_contract, classification)
                    }
                }
            }
            None => {
                let raw_contract =
                    handoff_bundle_review_receipt::load_live_package_handoff_bundle_contract_for_review_receipt(
                        &config.handoff_bundle_session_dir,
                    )?;
                let classification = classify_review_receipt_contract(&raw_contract, false);
                (raw_contract, classification)
            }
        };

    let session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| config.handoff_bundle_session_dir.join("review-receipt"));

    Ok(PackageReviewReceiptReport {
        generated_at: Utc::now(),
        mode: "plan_live_package_review_receipt".to_string(),
        verdict: TinyLivePackageReviewReceiptVerdict::TinyLivePackageReviewReceiptPlanReady,
        reason: format!(
            "handoff-bundle-native operator review receipt is explicit for verified handoff bundle session {}; this command stays archival and read-only while freezing one immutable signoff record",
            config.handoff_bundle_session_dir.display()
        ),
        handoff_bundle_session_dir: config.handoff_bundle_session_dir.display().to_string(),
        decision_packet_session_dir: Some(
            handoff_bundle
                .decision_packet_session_dir
                .display()
                .to_string(),
        ),
        execute_frozen_session_dir: Some(
            handoff_bundle.execute_frozen_session_dir.display().to_string(),
        ),
        turn_green_session_dir: Some(handoff_bundle.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: Some(
            handoff_bundle.launch_packet_session_dir.display().to_string(),
        ),
        package_dir: Some(handoff_bundle.package_dir.display().to_string()),
        install_root: Some(handoff_bundle.install_root.display().to_string()),
        target_service_name: Some(handoff_bundle.target_service_name.clone()),
        backend_command: Some(handoff_bundle.backend_command.clone()),
        wrapper_timeout_ms: Some(handoff_bundle.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(handoff_bundle.service_status_max_staleness_ms),
        session_dir: Some(session_dir.display().to_string()),
        review_receipt_session_path: None,
        review_receipt_status_path: None,
        result: Some(serialize_enum(&classification.0)),
        handoff_bundle_result: handoff_bundle.result.clone(),
        decision_packet_result: handoff_bundle.decision_packet_result.clone(),
        execute_frozen_result: handoff_bundle.execute_frozen_result.clone(),
        handoff_bundle_step: None,
        verify_handoff_bundle_command_summary: Some(verify_handoff_bundle_command_summary(
            &config.handoff_bundle_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            handoff_bundle
                .frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        review_receipt_summary: Some(review_receipt_summary(
            classification.0,
            &handoff_bundle.frozen_live_cutover_controller_command_summary,
        )),
        checklist_acknowledgement_summary: Some(checklist_acknowledgement_summary(
            classification.0,
            &handoff_bundle.operator_checklist_summary,
        )),
        runbook_acknowledgement_summary: Some(runbook_acknowledgement_summary(
            classification.0,
            &handoff_bundle.operator_runbook_summary,
        )),
        current_pre_activation_gate_verdict: handoff_bundle.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: handoff_bundle.current_pre_activation_gate_reason,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this final receipt freezes verified handoff-bundle truth, reviewed frozen controller context, and checklist/runbook acknowledgement without enabling production execution"
                .to_string(),
    })
}

fn render_live_package_review_receipt_report(
    config: &Config,
) -> Result<PackageReviewReceiptReport> {
    let plan = plan_live_package_review_receipt_report(config)?;
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
        "#!/usr/bin/env bash\nset -euo pipefail\n\ncopybot_tiny_live_activation_package_review_receipt --handoff-bundle-session-dir {} --plan-live-package-review-receipt\ncopybot_tiny_live_activation_package_review_receipt --handoff-bundle-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --run-live-package-review-receipt\ncopybot_tiny_live_activation_package_review_receipt --handoff-bundle-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --verify-live-package-review-receipt\n",
        shell_escape_path(&config.handoff_bundle_session_dir),
        shell_escape_path(&config.handoff_bundle_session_dir),
        shell_escape_path(&confirmed_decision_packet_session_dir),
        shell_escape_path(&session_dir),
        shell_escape_path(&config.handoff_bundle_session_dir),
        shell_escape_path(&confirmed_decision_packet_session_dir),
        shell_escape_path(&session_dir),
    );
    write_script(output_path, &script)?;
    Ok(PackageReviewReceiptReport {
        generated_at: Utc::now(),
        mode: "render_live_package_review_receipt".to_string(),
        verdict: TinyLivePackageReviewReceiptVerdict::TinyLivePackageReviewReceiptRendered,
        reason: format!(
            "rendered handoff-bundle-native review receipt script to {}",
            output_path.display()
        ),
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
        review_receipt_session_path: None,
        review_receipt_status_path: None,
        result: plan.result,
        handoff_bundle_result: plan.handoff_bundle_result,
        decision_packet_result: plan.decision_packet_result,
        execute_frozen_result: plan.execute_frozen_result,
        handoff_bundle_step: None,
        verify_handoff_bundle_command_summary: plan.verify_handoff_bundle_command_summary,
        reviewed_frozen_live_cutover_controller_command_summary: plan
            .reviewed_frozen_live_cutover_controller_command_summary,
        review_receipt_summary: plan.review_receipt_summary,
        checklist_acknowledgement_summary: plan.checklist_acknowledgement_summary,
        runbook_acknowledgement_summary: plan.runbook_acknowledgement_summary,
        current_pre_activation_gate_verdict: plan.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: plan.current_pre_activation_gate_reason,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this rendered script accepts only the verified handoff-bundle session as signoff input and stays archival and read-only with respect to the real target"
                .to_string(),
    })
}

fn run_live_package_review_receipt_report(config: &Config) -> Result<PackageReviewReceiptReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for run mode"))?;
    let verified_handoff_bundle = match handoff_bundle_review_receipt::verify_live_package_handoff_bundle_for_review_receipt(
        &config.handoff_bundle_session_dir,
        config
            .confirmed_decision_packet_session_dir
            .as_deref()
            .ok_or_else(|| anyhow!("missing --confirm-decision-packet-session-dir for run mode"))?,
    ) {
        Ok(value) => value,
        Err(error) => {
            return Ok(failure_report_without_contract(
                config,
                TinyLivePackageReviewReceiptVerdict::TinyLivePackageReviewReceiptRefusedNowByInvalidOrDriftedContract,
                format!(
                    "failed to verify handoff-bundle session under {}: {error}",
                    config.handoff_bundle_session_dir.display()
                ),
            ))
        }
    };

    validate_managed_surface_session_dir(&verified_handoff_bundle.contract.install_root, session_dir)
        .with_context(|| {
            format!(
                "review-receipt session dir {} overlaps the managed live target surface derived from verified handoff-bundle install_root {}",
                session_dir.display(),
                verified_handoff_bundle.contract.install_root.display()
            )
        })?;
    ensure_clean_review_receipt_session_dir(session_dir)?;
    let paths = review_receipt_paths(session_dir);
    let classification = classify_verified_handoff_bundle(&verified_handoff_bundle);
    let session = planned_session(
        config,
        session_dir,
        &verified_handoff_bundle.contract,
        classification.0,
    );
    persist_json(&paths.session_path, &session)?;
    persist_json(
        &paths.handoff_bundle_report_path,
        &verified_handoff_bundle.report_json,
    )?;
    let handoff_bundle_step = Some(step_artifact(
        &paths.handoff_bundle_report_path,
        "verify_live_package_handoff_bundle",
        &verified_handoff_bundle.verdict,
        &verified_handoff_bundle.reason,
        verified_handoff_bundle.generated_at,
    ));
    let status = PackageReviewReceiptStatus {
        status_version: REVIEW_RECEIPT_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        handoff_bundle_session_dir: config.handoff_bundle_session_dir.display().to_string(),
        result: classification.0,
        reason: classification.2.clone(),
        handoff_bundle_result: verified_handoff_bundle.contract.result.clone(),
        decision_packet_result: verified_handoff_bundle
            .contract
            .decision_packet_result
            .clone(),
        execute_frozen_result: verified_handoff_bundle
            .contract
            .execute_frozen_result
            .clone(),
        current_pre_activation_gate_verdict: verified_handoff_bundle
            .contract
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: verified_handoff_bundle
            .contract
            .current_pre_activation_gate_reason
            .clone(),
        handoff_bundle_step: handoff_bundle_step.clone(),
        review_receipt_summary: review_receipt_summary(
            classification.0,
            &verified_handoff_bundle
                .contract
                .frozen_live_cutover_controller_command_summary,
        ),
        checklist_acknowledgement_summary: checklist_acknowledgement_summary(
            classification.0,
            &verified_handoff_bundle.contract.operator_checklist_summary,
        ),
        runbook_acknowledgement_summary: runbook_acknowledgement_summary(
            classification.0,
            &verified_handoff_bundle.contract.operator_runbook_summary,
        ),
        explicit_statement: REVIEW_RECEIPT_STATUS_EXPLICIT_STATEMENT.to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    Ok(report_from_status(
        config,
        session_dir,
        &paths,
        &verified_handoff_bundle.contract,
        classification.1,
        &status,
    ))
}

fn verify_live_package_review_receipt_report(
    config: &Config,
) -> Result<PackageReviewReceiptReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = review_receipt_paths(session_dir);
    let session: PackageReviewReceiptSession = load_json(&paths.session_path)?;
    let status: PackageReviewReceiptStatus = load_json(&paths.status_path)?;
    let mut mismatches = Vec::new();

    compare_string(
        &session.handoff_bundle_session_dir,
        &config.handoff_bundle_session_dir.display().to_string(),
        "review-receipt handoff_bundle_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "review-receipt session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "review-receipt status session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.handoff_bundle_session_dir,
        &config.handoff_bundle_session_dir.display().to_string(),
        "review-receipt status handoff_bundle_session_dir",
        &mut mismatches,
    );

    let verified_handoff_bundle =
        match handoff_bundle_review_receipt::verify_live_package_handoff_bundle_for_review_receipt(
            &config.handoff_bundle_session_dir,
            config
                .confirmed_decision_packet_session_dir
                .as_deref()
                .ok_or_else(|| {
                    anyhow!("missing --confirm-decision-packet-session-dir for verify mode")
                })?,
        ) {
            Ok(value) => value,
            Err(error) => {
                let raw_contract = handoff_bundle_review_receipt::load_live_package_handoff_bundle_contract_for_review_receipt(
                    &config.handoff_bundle_session_dir,
                )?;
                return Ok(failure_report_for_verify(
                    config,
                    session_dir,
                    &raw_contract,
                    vec![format!(
                        "failed to verify handoff-bundle session under {}: {error}",
                        config.handoff_bundle_session_dir.display()
                    )],
                ));
            }
        };

    let stored_handoff_bundle_report: serde_json::Value = load_required_step_json(
        &status.handoff_bundle_step,
        &paths.handoff_bundle_report_path,
        "handoff_bundle_step",
        &mut mismatches,
    )?;
    let stored_handoff_bundle_report_view: NestedHandoffBundleReportView =
        serde_json::from_value(stored_handoff_bundle_report.clone()).with_context(|| {
            format!(
                "failed parsing archived nested handoff-bundle report {}",
                paths.handoff_bundle_report_path.display()
            )
        })?;
    let verified_handoff_bundle_report_view: NestedHandoffBundleReportView =
        serde_json::from_value(verified_handoff_bundle.report_json.clone()).with_context(|| {
            format!(
                "failed parsing freshly verified nested handoff-bundle report for {}",
                config.handoff_bundle_session_dir.display()
            )
        })?;
    compare_json_ignoring_generated_at(
        &stored_handoff_bundle_report,
        &verified_handoff_bundle.report_json,
        "review-receipt archived handoff-bundle report json",
        &mut mismatches,
    );
    compare_string(
        &stored_handoff_bundle_report_view.mode,
        &verified_handoff_bundle_report_view.mode,
        "review-receipt archived handoff-bundle report mode",
        &mut mismatches,
    );
    compare_string(
        &stored_handoff_bundle_report_view.verdict,
        &verified_handoff_bundle_report_view.verdict,
        "review-receipt archived handoff-bundle report verdict",
        &mut mismatches,
    );
    compare_string(
        &stored_handoff_bundle_report_view.reason,
        &verified_handoff_bundle_report_view.reason,
        "review-receipt archived handoff-bundle report reason",
        &mut mismatches,
    );
    let expected_handoff_bundle_step = step_artifact(
        &paths.handoff_bundle_report_path,
        &verified_handoff_bundle_report_view.mode,
        &verified_handoff_bundle_report_view.verdict,
        &verified_handoff_bundle_report_view.reason,
        stored_handoff_bundle_report_view.generated_at,
    );
    compare_step_artifact(
        status.handoff_bundle_step.as_ref(),
        &expected_handoff_bundle_step,
        "review-receipt handoff_bundle_step",
        &mut mismatches,
    );
    if verified_handoff_bundle.verdict != "tiny_live_package_handoff_bundle_verify_ok" {
        mismatches.push(format!(
            "nested handoff-bundle verification is non-green: {}",
            verified_handoff_bundle.reason
        ));
    }

    compare_string(
        &session.decision_packet_session_dir,
        &verified_handoff_bundle
            .contract
            .decision_packet_session_dir
            .display()
            .to_string(),
        "review-receipt decision_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.execute_frozen_session_dir,
        &verified_handoff_bundle
            .contract
            .execute_frozen_session_dir
            .display()
            .to_string(),
        "review-receipt execute_frozen_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.turn_green_session_dir,
        &verified_handoff_bundle
            .contract
            .turn_green_session_dir
            .display()
            .to_string(),
        "review-receipt turn_green_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.launch_packet_session_dir,
        &verified_handoff_bundle
            .contract
            .launch_packet_session_dir
            .display()
            .to_string(),
        "review-receipt launch_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.package_dir,
        &verified_handoff_bundle
            .contract
            .package_dir
            .display()
            .to_string(),
        "review-receipt package_dir",
        &mut mismatches,
    );
    compare_string(
        &session.install_root,
        &verified_handoff_bundle
            .contract
            .install_root
            .display()
            .to_string(),
        "review-receipt install_root",
        &mut mismatches,
    );
    compare_string(
        &session.target_service_name,
        &verified_handoff_bundle.contract.target_service_name,
        "review-receipt target_service_name",
        &mut mismatches,
    );
    compare_string(
        &session.backend_command,
        &verified_handoff_bundle.contract.backend_command,
        "review-receipt backend_command",
        &mut mismatches,
    );
    if session.wrapper_timeout_ms != verified_handoff_bundle.contract.wrapper_timeout_ms {
        mismatches.push(format!(
            "review-receipt wrapper_timeout_ms {} does not match verified {}",
            session.wrapper_timeout_ms, verified_handoff_bundle.contract.wrapper_timeout_ms
        ));
    }
    if session.service_status_max_staleness_ms
        != verified_handoff_bundle
            .contract
            .service_status_max_staleness_ms
    {
        mismatches.push(format!(
            "review-receipt service_status_max_staleness_ms {} does not match verified {}",
            session.service_status_max_staleness_ms,
            verified_handoff_bundle
                .contract
                .service_status_max_staleness_ms
        ));
    }
    compare_string(
        &session.verify_handoff_bundle_command_summary,
        &verify_handoff_bundle_command_summary(&config.handoff_bundle_session_dir),
        "review-receipt verify_handoff_bundle_command_summary",
        &mut mismatches,
    );
    compare_string(
        &session.reviewed_frozen_live_cutover_controller_command_summary,
        &verified_handoff_bundle
            .contract
            .frozen_live_cutover_controller_command_summary,
        "review-receipt reviewed_frozen_live_cutover_controller_command_summary",
        &mut mismatches,
    );

    let classification = classify_verified_handoff_bundle(&verified_handoff_bundle);
    let expected_review_receipt_summary = review_receipt_summary(
        classification.0,
        &verified_handoff_bundle
            .contract
            .frozen_live_cutover_controller_command_summary,
    );
    let expected_checklist_ack = checklist_acknowledgement_summary(
        classification.0,
        &verified_handoff_bundle.contract.operator_checklist_summary,
    );
    let expected_runbook_ack = runbook_acknowledgement_summary(
        classification.0,
        &verified_handoff_bundle.contract.operator_runbook_summary,
    );
    compare_string(
        &session.review_receipt_summary,
        &expected_review_receipt_summary,
        "review-receipt session review_receipt_summary",
        &mut mismatches,
    );
    compare_string(
        &session.checklist_acknowledgement_summary,
        &expected_checklist_ack,
        "review-receipt session checklist_acknowledgement_summary",
        &mut mismatches,
    );
    compare_string(
        &session.runbook_acknowledgement_summary,
        &expected_runbook_ack,
        "review-receipt session runbook_acknowledgement_summary",
        &mut mismatches,
    );
    compare_string(
        &status.review_receipt_summary,
        &expected_review_receipt_summary,
        "review-receipt status review_receipt_summary",
        &mut mismatches,
    );
    compare_string(
        &status.checklist_acknowledgement_summary,
        &expected_checklist_ack,
        "review-receipt status checklist_acknowledgement_summary",
        &mut mismatches,
    );
    compare_string(
        &status.runbook_acknowledgement_summary,
        &expected_runbook_ack,
        "review-receipt status runbook_acknowledgement_summary",
        &mut mismatches,
    );
    compare_string(
        &session.explicit_statement,
        REVIEW_RECEIPT_SESSION_EXPLICIT_STATEMENT,
        "review-receipt session explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &status.explicit_statement,
        REVIEW_RECEIPT_STATUS_EXPLICIT_STATEMENT,
        "review-receipt status explicit_statement",
        &mut mismatches,
    );
    if status.result != classification.0 {
        mismatches.push(format!(
            "stored review-receipt result {} does not match verified {}",
            serialize_enum(&status.result),
            serialize_enum(&classification.0)
        ));
    }
    if status.handoff_bundle_result != verified_handoff_bundle.contract.result {
        mismatches.push(format!(
            "stored handoff_bundle_result {:?} does not match verified {:?}",
            status.handoff_bundle_result, verified_handoff_bundle.contract.result
        ));
    }
    if status.decision_packet_result != verified_handoff_bundle.contract.decision_packet_result {
        mismatches.push(format!(
            "stored decision_packet_result {:?} does not match verified {:?}",
            status.decision_packet_result, verified_handoff_bundle.contract.decision_packet_result
        ));
    }
    if status.execute_frozen_result != verified_handoff_bundle.contract.execute_frozen_result {
        mismatches.push(format!(
            "stored execute_frozen_result {:?} does not match verified {:?}",
            status.execute_frozen_result, verified_handoff_bundle.contract.execute_frozen_result
        ));
    }
    if status.current_pre_activation_gate_verdict
        != verified_handoff_bundle
            .contract
            .current_pre_activation_gate_verdict
    {
        mismatches.push(format!(
            "stored current_pre_activation_gate_verdict {:?} does not match verified {:?}",
            status.current_pre_activation_gate_verdict,
            verified_handoff_bundle
                .contract
                .current_pre_activation_gate_verdict
        ));
    }
    if status.current_pre_activation_gate_reason
        != verified_handoff_bundle
            .contract
            .current_pre_activation_gate_reason
    {
        mismatches.push(format!(
            "stored current_pre_activation_gate_reason {:?} does not match verified {:?}",
            status.current_pre_activation_gate_reason,
            verified_handoff_bundle
                .contract
                .current_pre_activation_gate_reason
        ));
    }

    let verdict = if mismatches.is_empty() {
        TinyLivePackageReviewReceiptVerdict::TinyLivePackageReviewReceiptVerifyOk
    } else {
        TinyLivePackageReviewReceiptVerdict::TinyLivePackageReviewReceiptVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "handoff-bundle-native operator review receipt under {} is coherent and correctly binds the final immutable signoff record to verified handoff-bundle truth",
            session_dir.display()
        )
    } else {
        mismatches.first().cloned().unwrap_or_else(|| {
            "handoff-bundle-native operator review receipt verification failed".to_string()
        })
    };

    Ok(PackageReviewReceiptReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_review_receipt".to_string(),
        verdict,
        reason,
        handoff_bundle_session_dir: config.handoff_bundle_session_dir.display().to_string(),
        decision_packet_session_dir: Some(
            verified_handoff_bundle
                .contract
                .decision_packet_session_dir
                .display()
                .to_string(),
        ),
        execute_frozen_session_dir: Some(
            verified_handoff_bundle
                .contract
                .execute_frozen_session_dir
                .display()
                .to_string(),
        ),
        turn_green_session_dir: Some(
            verified_handoff_bundle
                .contract
                .turn_green_session_dir
                .display()
                .to_string(),
        ),
        launch_packet_session_dir: Some(
            verified_handoff_bundle
                .contract
                .launch_packet_session_dir
                .display()
                .to_string(),
        ),
        package_dir: Some(
            verified_handoff_bundle
                .contract
                .package_dir
                .display()
                .to_string(),
        ),
        install_root: Some(
            verified_handoff_bundle
                .contract
                .install_root
                .display()
                .to_string(),
        ),
        target_service_name: Some(verified_handoff_bundle.contract.target_service_name.clone()),
        backend_command: Some(verified_handoff_bundle.contract.backend_command.clone()),
        wrapper_timeout_ms: Some(verified_handoff_bundle.contract.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(
            verified_handoff_bundle
                .contract
                .service_status_max_staleness_ms,
        ),
        session_dir: Some(session_dir.display().to_string()),
        review_receipt_session_path: Some(paths.session_path.display().to_string()),
        review_receipt_status_path: Some(paths.status_path.display().to_string()),
        result: Some(serialize_enum(&status.result)),
        handoff_bundle_result: status.handoff_bundle_result.clone(),
        decision_packet_result: status.decision_packet_result.clone(),
        execute_frozen_result: status.execute_frozen_result.clone(),
        handoff_bundle_step: Some(expected_handoff_bundle_step),
        verify_handoff_bundle_command_summary: Some(verify_handoff_bundle_command_summary(
            &config.handoff_bundle_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            verified_handoff_bundle
                .contract
                .frozen_live_cutover_controller_command_summary,
        ),
        review_receipt_summary: Some(expected_review_receipt_summary),
        checklist_acknowledgement_summary: Some(expected_checklist_ack),
        runbook_acknowledgement_summary: Some(expected_runbook_ack),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        verification_mismatches: mismatches,
        explicit_statement: REVIEW_RECEIPT_VERIFY_EXPLICIT_STATEMENT.to_string(),
    })
}

fn verify_handoff_bundle_command_summary(handoff_bundle_session_dir: &Path) -> String {
    format!(
        "verify immutable handoff-bundle session under {} before freezing the final operator review receipt",
        handoff_bundle_session_dir.display()
    )
}

fn classify_review_receipt_contract(
    contract: &handoff_bundle_review_receipt::LivePackageHandoffBundleContractView,
    assume_verified: bool,
) -> (
    LivePackageReviewReceiptResult,
    TinyLivePackageReviewReceiptVerdict,
    String,
) {
    if !assume_verified {
        return (
            LivePackageReviewReceiptResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageReviewReceiptVerdict::TinyLivePackageReviewReceiptRefusedNowByInvalidOrDriftedContract,
            "the handoff-bundle chain is not verified, so the operator review receipt remains refused".to_string(),
        );
    }
    match contract.result.as_deref() {
        Some("refused_now_by_stage3") => (
            LivePackageReviewReceiptResult::RefusedNowByStage3,
            TinyLivePackageReviewReceiptVerdict::TinyLivePackageReviewReceiptRefusedNowByStage3,
            "the immutable handoff bundle remains refused now by Stage 3, so the operator review receipt remains an explicit refusal record".to_string(),
        ),
        Some("refused_now_by_pre_activation_gate") => (
            LivePackageReviewReceiptResult::RefusedNowByPreActivationGate,
            TinyLivePackageReviewReceiptVerdict::TinyLivePackageReviewReceiptRefusedNowByPreActivationGate,
            "the immutable handoff bundle remains refused now by the pre-activation gate, so the operator review receipt remains an explicit refusal record".to_string(),
        ),
        Some("ready_for_manual_go_live_review") => (
            LivePackageReviewReceiptResult::ReadyForManualGoLiveSignoff,
            TinyLivePackageReviewReceiptVerdict::TinyLivePackageReviewReceiptReadyForManualGoLiveSignoff,
            "the immutable handoff bundle is coherent and this review receipt is ready for final manual go-live signoff when gate truth turns green".to_string(),
        ),
        _ => (
            LivePackageReviewReceiptResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageReviewReceiptVerdict::TinyLivePackageReviewReceiptRefusedNowByInvalidOrDriftedContract,
            "the verified handoff-bundle chain is invalid, drifted, or non-runnable, so the operator review receipt remains refused".to_string(),
        ),
    }
}

fn classify_verified_handoff_bundle(
    verified: &handoff_bundle_review_receipt::VerifiedLivePackageHandoffBundleReviewStep,
) -> (
    LivePackageReviewReceiptResult,
    TinyLivePackageReviewReceiptVerdict,
    String,
) {
    if verified.verdict != "tiny_live_package_handoff_bundle_verify_ok" {
        return (
            LivePackageReviewReceiptResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageReviewReceiptVerdict::TinyLivePackageReviewReceiptRefusedNowByInvalidOrDriftedContract,
            format!(
                "nested handoff-bundle verification is non-green: {}",
                verified.reason
            ),
        );
    }
    classify_review_receipt_contract(&verified.contract, true)
}

fn review_receipt_summary(
    result: LivePackageReviewReceiptResult,
    frozen_live_cutover_controller_command_summary: &str,
) -> String {
    match result {
        LivePackageReviewReceiptResult::RefusedNowByStage3 => {
            "Review outcome: the immutable dossier remains refused now by Stage 3; archive this receipt as reviewed refusal evidence only and do not run the frozen controller.".to_string()
        }
        LivePackageReviewReceiptResult::RefusedNowByPreActivationGate => {
            "Review outcome: the immutable dossier remains refused now by the pre-activation gate; archive this receipt as reviewed refusal evidence only and do not run the frozen controller.".to_string()
        }
        LivePackageReviewReceiptResult::RefusedNowByInvalidOrDriftedContract => {
            "Review outcome: the immutable dossier is invalid or drifted; archive this receipt for audit only and mint a new coherent bundle before any later signoff.".to_string()
        }
        LivePackageReviewReceiptResult::ReadyForManualGoLiveSignoff => format!(
            "Review outcome: this operator receipt is ready for final manual go-live signoff when gate truth turns green; the exact reviewed frozen controller command remains: {frozen_live_cutover_controller_command_summary}"
        ),
    }
}

fn checklist_acknowledgement_summary(
    result: LivePackageReviewReceiptResult,
    operator_checklist_summary: &str,
) -> String {
    match result {
        LivePackageReviewReceiptResult::RefusedNowByStage3
        | LivePackageReviewReceiptResult::RefusedNowByPreActivationGate
        | LivePackageReviewReceiptResult::RefusedNowByInvalidOrDriftedContract => format!(
            "Checklist acknowledgement: reviewed and archived as refusal-only evidence. Source checklist remains: {operator_checklist_summary}"
        ),
        LivePackageReviewReceiptResult::ReadyForManualGoLiveSignoff => format!(
            "Checklist acknowledgement: reviewed for future manual go-live signoff only; no execution occurred. Source checklist remains: {operator_checklist_summary}"
        ),
    }
}

fn runbook_acknowledgement_summary(
    result: LivePackageReviewReceiptResult,
    operator_runbook_summary: &str,
) -> String {
    match result {
        LivePackageReviewReceiptResult::RefusedNowByStage3
        | LivePackageReviewReceiptResult::RefusedNowByPreActivationGate
        | LivePackageReviewReceiptResult::RefusedNowByInvalidOrDriftedContract => format!(
            "Runbook acknowledgement: reviewed and archived as refusal-only guidance. Source runbook remains: {operator_runbook_summary}"
        ),
        LivePackageReviewReceiptResult::ReadyForManualGoLiveSignoff => format!(
            "Runbook acknowledgement: reviewed for future manual go-live signoff only; no execution occurred. Source runbook remains: {operator_runbook_summary}"
        ),
    }
}

fn review_receipt_paths(session_dir: &Path) -> PackageReviewReceiptPaths {
    PackageReviewReceiptPaths {
        session_path: session_dir.join("tiny_live_activation_package_review_receipt.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_review_receipt.status.json"),
        handoff_bundle_report_path: session_dir
            .join("tiny_live_activation_package_review_receipt.handoff_bundle.report.json"),
    }
}

fn ensure_clean_review_receipt_session_dir(session_dir: &Path) -> Result<()> {
    if session_dir.exists() {
        bail!(
            "refusing to overwrite existing review-receipt session dir {}",
            session_dir.display()
        );
    }
    fs::create_dir_all(session_dir).with_context(|| {
        format!(
            "failed creating review-receipt session dir {}",
            session_dir.display()
        )
    })
}

fn planned_session(
    config: &Config,
    session_dir: &Path,
    contract: &handoff_bundle_review_receipt::LivePackageHandoffBundleContractView,
    result: LivePackageReviewReceiptResult,
) -> PackageReviewReceiptSession {
    PackageReviewReceiptSession {
        session_version: REVIEW_RECEIPT_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        handoff_bundle_session_dir: config.handoff_bundle_session_dir.display().to_string(),
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
        verify_handoff_bundle_command_summary: verify_handoff_bundle_command_summary(
            &config.handoff_bundle_session_dir,
        ),
        reviewed_frozen_live_cutover_controller_command_summary: contract
            .frozen_live_cutover_controller_command_summary
            .clone(),
        review_receipt_summary: review_receipt_summary(
            result,
            &contract.frozen_live_cutover_controller_command_summary,
        ),
        checklist_acknowledgement_summary: checklist_acknowledgement_summary(
            result,
            &contract.operator_checklist_summary,
        ),
        runbook_acknowledgement_summary: runbook_acknowledgement_summary(
            result,
            &contract.operator_runbook_summary,
        ),
        explicit_statement: REVIEW_RECEIPT_SESSION_EXPLICIT_STATEMENT.to_string(),
    }
}

fn report_from_status(
    config: &Config,
    session_dir: &Path,
    paths: &PackageReviewReceiptPaths,
    contract: &handoff_bundle_review_receipt::LivePackageHandoffBundleContractView,
    verdict: TinyLivePackageReviewReceiptVerdict,
    status: &PackageReviewReceiptStatus,
) -> PackageReviewReceiptReport {
    PackageReviewReceiptReport {
        generated_at: Utc::now(),
        mode: "run_live_package_review_receipt".to_string(),
        verdict,
        reason: status.reason.clone(),
        handoff_bundle_session_dir: config.handoff_bundle_session_dir.display().to_string(),
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
        review_receipt_session_path: Some(paths.session_path.display().to_string()),
        review_receipt_status_path: Some(paths.status_path.display().to_string()),
        result: Some(serialize_enum(&status.result)),
        handoff_bundle_result: status.handoff_bundle_result.clone(),
        decision_packet_result: status.decision_packet_result.clone(),
        execute_frozen_result: status.execute_frozen_result.clone(),
        handoff_bundle_step: status.handoff_bundle_step.clone(),
        verify_handoff_bundle_command_summary: Some(verify_handoff_bundle_command_summary(
            &config.handoff_bundle_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            contract
                .frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        review_receipt_summary: Some(status.review_receipt_summary.clone()),
        checklist_acknowledgement_summary: Some(status.checklist_acknowledgement_summary.clone()),
        runbook_acknowledgement_summary: Some(status.runbook_acknowledgement_summary.clone()),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        verification_mismatches: Vec::new(),
        explicit_statement: status.explicit_statement.clone(),
    }
}

fn failure_report_without_contract(
    config: &Config,
    verdict: TinyLivePackageReviewReceiptVerdict,
    reason: String,
) -> PackageReviewReceiptReport {
    PackageReviewReceiptReport {
        generated_at: Utc::now(),
        mode: match config.mode {
            Mode::PlanLivePackageReviewReceipt => "plan_live_package_review_receipt".to_string(),
            Mode::RenderLivePackageReviewReceipt => "render_live_package_review_receipt".to_string(),
            Mode::RunLivePackageReviewReceipt => "run_live_package_review_receipt".to_string(),
            Mode::VerifyLivePackageReviewReceipt => "verify_live_package_review_receipt".to_string(),
        },
        verdict,
        reason,
        handoff_bundle_session_dir: config.handoff_bundle_session_dir.display().to_string(),
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
        review_receipt_session_path: None,
        review_receipt_status_path: None,
        result: Some("refused_now_by_invalid_or_drifted_contract".to_string()),
        handoff_bundle_result: None,
        decision_packet_result: None,
        execute_frozen_result: None,
        handoff_bundle_step: None,
        verify_handoff_bundle_command_summary: Some(verify_handoff_bundle_command_summary(
            &config.handoff_bundle_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: None,
        review_receipt_summary: None,
        checklist_acknowledgement_summary: None,
        runbook_acknowledgement_summary: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this immutable operator review receipt refused before persisting because the handoff-bundle artifact could not be verified coherently"
                .to_string(),
    }
}

fn failure_report_for_verify(
    config: &Config,
    session_dir: &Path,
    contract: &handoff_bundle_review_receipt::LivePackageHandoffBundleContractView,
    mismatches: Vec<String>,
) -> PackageReviewReceiptReport {
    let paths = review_receipt_paths(session_dir);
    let reason = mismatches
        .first()
        .cloned()
        .unwrap_or_else(|| "handoff-bundle-native review receipt verification failed".to_string());
    PackageReviewReceiptReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_review_receipt".to_string(),
        verdict: TinyLivePackageReviewReceiptVerdict::TinyLivePackageReviewReceiptVerifyInvalid,
        reason,
        handoff_bundle_session_dir: config.handoff_bundle_session_dir.display().to_string(),
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
        review_receipt_session_path: Some(paths.session_path.display().to_string()),
        review_receipt_status_path: Some(paths.status_path.display().to_string()),
        result: None,
        handoff_bundle_result: contract.result.clone(),
        decision_packet_result: contract.decision_packet_result.clone(),
        execute_frozen_result: contract.execute_frozen_result.clone(),
        handoff_bundle_step: None,
        verify_handoff_bundle_command_summary: Some(verify_handoff_bundle_command_summary(
            &config.handoff_bundle_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            contract
                .frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        review_receipt_summary: Some(review_receipt_summary(
            classify_review_receipt_contract(contract, false).0,
            &contract.frozen_live_cutover_controller_command_summary,
        )),
        checklist_acknowledgement_summary: Some(checklist_acknowledgement_summary(
            classify_review_receipt_contract(contract, false).0,
            &contract.operator_checklist_summary,
        )),
        runbook_acknowledgement_summary: Some(runbook_acknowledgement_summary(
            classify_review_receipt_contract(contract, false).0,
            &contract.operator_runbook_summary,
        )),
        current_pre_activation_gate_verdict: contract.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: contract.current_pre_activation_gate_reason.clone(),
        verification_mismatches: mismatches,
        explicit_statement:
            "this verification failure means the immutable operator review receipt no longer proves the exact reviewed frozen controller context"
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
        .with_context(|| format!("failed writing review-receipt script to {}", path.display()))?;
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
) -> PackageReviewReceiptStepArtifact {
    PackageReviewReceiptStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn load_required_step_json<T: for<'de> Deserialize<'de>>(
    step: &Option<PackageReviewReceiptStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("{label} is missing from review-receipt status"))?;
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
    actual: Option<&PackageReviewReceiptStepArtifact>,
    expected: &PackageReviewReceiptStepArtifact,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    let Some(actual) = actual else {
        mismatches.push(format!("{label} is missing from review-receipt status"));
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

fn render_report_lines(report: &PackageReviewReceiptReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "handoff_bundle_session_dir={}",
            report.handoff_bundle_session_dir
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
    if let Some(value) = &report.review_receipt_summary {
        lines.push(format!("review_receipt_summary={value}"));
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
    fn run_ready_for_manual_go_live_signoff_then_verify_stays_green() {
        let fixture = fake_command_fixture(
            "review_receipt_ready",
            handoff_bundle_verify_report(
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_review_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageReviewReceiptVerdict::TinyLivePackageReviewReceiptReadyForManualGoLiveSignoff
        );
        assert_eq!(
            run.result.as_deref(),
            Some("ready_for_manual_go_live_signoff")
        );

        let verify = verify_live_package_review_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageReviewReceiptVerdict::TinyLivePackageReviewReceiptVerifyOk,
            "{:#?}",
            verify.verification_mismatches
        );
    }

    #[test]
    fn stage3_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "review_receipt_stage3",
            handoff_bundle_verify_report(
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "blocked",
                "stage3 blocked",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_review_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageReviewReceiptVerdict::TinyLivePackageReviewReceiptRefusedNowByStage3
        );
    }

    #[test]
    fn pre_activation_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "review_receipt_pre_activation",
            handoff_bundle_verify_report(
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
                "blocked",
                "pre-activation blocked",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_review_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageReviewReceiptVerdict::TinyLivePackageReviewReceiptRefusedNowByPreActivationGate
        );
    }

    #[test]
    fn drifted_handoff_bundle_contract_is_refused() {
        let fixture = fake_command_fixture(
            "review_receipt_drifted",
            handoff_bundle_verify_report(
                "refused_now_by_invalid_or_drifted_contract",
                "refused_now_by_invalid_or_drifted_contract",
                "refused_now_by_invalid_or_drifted_contract",
                "green",
                "contract drifted",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_review_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageReviewReceiptVerdict::TinyLivePackageReviewReceiptRefusedNowByInvalidOrDriftedContract
        );
    }

    #[test]
    fn run_refuses_managed_surface_overlap_before_writing_any_artifacts() {
        let fixture = fake_command_fixture(
            "review_receipt_overlap",
            handoff_bundle_verify_report(
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
        let unsafe_session_dir = managed_surface.session_dir.join("review-receipt-session");
        let config = Config {
            mode: Mode::RunLivePackageReviewReceipt,
            handoff_bundle_session_dir: fixture.handoff_bundle_session_dir.clone(),
            confirmed_decision_packet_session_dir: Some(
                fixture.decision_packet_session_dir.clone(),
            ),
            session_dir: Some(unsafe_session_dir.clone()),
            output_path: None,
            json: true,
        };

        let error = run_live_package_review_receipt_report(&config).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("overlaps the managed live target surface"),
            "{error:#}"
        );
        let paths = review_receipt_paths(&unsafe_session_dir);
        assert!(!paths.session_path.exists());
        assert!(!paths.status_path.exists());
    }

    #[test]
    fn verify_rejects_tampered_handoff_bundle_step_path() {
        let fixture = fake_command_fixture(
            "review_receipt_tampered_step",
            handoff_bundle_verify_report(
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_review_receipt_report(&fixture.run_config()).unwrap();
        let paths = review_receipt_paths(&fixture.review_receipt_session_dir);
        let mut status: PackageReviewReceiptStatus = load_json(&paths.status_path).unwrap();
        let foreign = fixture
            .fixture_dir
            .join("foreign.handoff-bundle.report.json");
        fs::write(&foreign, "{}").unwrap();
        status.handoff_bundle_step.as_mut().unwrap().path = foreign.display().to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_review_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageReviewReceiptVerdict::TinyLivePackageReviewReceiptVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_review_receipt_text() {
        let fixture = fake_command_fixture(
            "review_receipt_tampered_text",
            handoff_bundle_verify_report(
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_review_receipt_report(&fixture.run_config()).unwrap();
        let paths = review_receipt_paths(&fixture.review_receipt_session_dir);
        let mut session: PackageReviewReceiptSession = load_json(&paths.session_path).unwrap();
        session.review_receipt_summary = "tampered summary".to_string();
        persist_json(&paths.session_path, &session).unwrap();

        let verify = verify_live_package_review_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageReviewReceiptVerdict::TinyLivePackageReviewReceiptVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_nested_handoff_bundle_report_content() {
        let fixture = fake_command_fixture(
            "review_receipt_tampered_nested_report",
            handoff_bundle_verify_report(
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_review_receipt_report(&fixture.run_config()).unwrap();
        let paths = review_receipt_paths(&fixture.review_receipt_session_dir);
        let mut report: serde_json::Value = load_json(&paths.handoff_bundle_report_path).unwrap();
        report.as_object_mut().unwrap().insert(
            "reason".to_string(),
            json!("tampered nested handoff bundle reason"),
        );
        persist_json(&paths.handoff_bundle_report_path, &report).unwrap();

        let verify = verify_live_package_review_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageReviewReceiptVerdict::TinyLivePackageReviewReceiptVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_stored_decision_packet_session_dir_redirect_to_foreign_session() {
        let fixture = fake_command_fixture(
            "review_receipt_tampered_decision_packet_redirect",
            handoff_bundle_verify_report(
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "completed_keep_running",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_review_receipt_report(&fixture.run_config()).unwrap();
        let foreign_decision_packet_session_dir =
            fixture.fixture_dir.join("foreign-decision-packet-session");
        fs::create_dir_all(&foreign_decision_packet_session_dir).unwrap();

        let handoff_bundle_session_path = fixture
            .handoff_bundle_session_dir
            .join("tiny_live_activation_package_handoff_bundle.session.json");
        let mut handoff_bundle_session: serde_json::Value =
            load_json(&handoff_bundle_session_path).unwrap();
        handoff_bundle_session.as_object_mut().unwrap().insert(
            "decision_packet_session_dir".to_string(),
            json!(foreign_decision_packet_session_dir.display().to_string()),
        );
        persist_json(&handoff_bundle_session_path, &handoff_bundle_session).unwrap();

        let handoff_bundle_status_path = fixture
            .handoff_bundle_session_dir
            .join("tiny_live_activation_package_handoff_bundle.status.json");
        let mut handoff_bundle_status: serde_json::Value =
            load_json(&handoff_bundle_status_path).unwrap();
        handoff_bundle_status.as_object_mut().unwrap().insert(
            "decision_packet_session_dir".to_string(),
            json!(foreign_decision_packet_session_dir.display().to_string()),
        );
        persist_json(&handoff_bundle_status_path, &handoff_bundle_status).unwrap();

        let handoff_bundle_report_path = fixture
            .handoff_bundle_session_dir
            .join("tiny_live_activation_package_handoff_bundle.decision_packet.report.json");
        let mut handoff_bundle_report: serde_json::Value =
            load_json(&handoff_bundle_report_path).unwrap();
        handoff_bundle_report.as_object_mut().unwrap().insert(
            "decision_packet_session_dir".to_string(),
            json!(foreign_decision_packet_session_dir.display().to_string()),
        );
        persist_json(&handoff_bundle_report_path, &handoff_bundle_report).unwrap();

        let verify = verify_live_package_review_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageReviewReceiptVerdict::TinyLivePackageReviewReceiptVerifyInvalid
        );
    }

    struct FakeCommandFixture {
        fixture_dir: PathBuf,
        bin_dir: PathBuf,
        handoff_bundle_session_dir: PathBuf,
        review_receipt_session_dir: PathBuf,
        decision_packet_session_dir: PathBuf,
        install_root: PathBuf,
    }

    impl FakeCommandFixture {
        fn run_config(&self) -> Config {
            Config {
                mode: Mode::RunLivePackageReviewReceipt,
                handoff_bundle_session_dir: self.handoff_bundle_session_dir.clone(),
                confirmed_decision_packet_session_dir: Some(
                    self.decision_packet_session_dir.clone(),
                ),
                session_dir: Some(self.review_receipt_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn verify_config(&self) -> Config {
            Config {
                mode: Mode::VerifyLivePackageReviewReceipt,
                handoff_bundle_session_dir: self.handoff_bundle_session_dir.clone(),
                confirmed_decision_packet_session_dir: Some(
                    self.decision_packet_session_dir.clone(),
                ),
                session_dir: Some(self.review_receipt_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }
    }

    fn fake_command_fixture(label: &str, verify_report: serde_json::Value) -> FakeCommandFixture {
        let fixture_dir = temp_dir(label);
        let bin_dir = fixture_dir.join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        let handoff_bundle_session_dir = fixture_dir.join("handoff-bundle-session");
        fs::create_dir_all(&handoff_bundle_session_dir).unwrap();
        let review_receipt_session_dir = fixture_dir.join("review-receipt-session");
        let decision_packet_session_dir = fixture_dir.join("decision-packet-session");
        let execute_frozen_session_dir = fixture_dir.join("execute-frozen-session");
        let turn_green_session_dir = fixture_dir.join("turn-green-session");
        let launch_packet_session_dir = fixture_dir.join("launch-packet-session");
        let package_dir = fixture_dir.join("package");
        let install_root = fixture_dir.join("live-root");
        let backend_command = fixture_dir.join("fake-backend");
        fs::create_dir_all(&decision_packet_session_dir).unwrap();
        fs::create_dir_all(&execute_frozen_session_dir).unwrap();
        fs::create_dir_all(&turn_green_session_dir).unwrap();
        fs::create_dir_all(&launch_packet_session_dir).unwrap();
        fs::create_dir_all(&package_dir).unwrap();
        fs::create_dir_all(&install_root).unwrap();

        persist_json(
            &handoff_bundle_session_dir.join("tiny_live_activation_package_handoff_bundle.session.json"),
            &json!({
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
                "verify_decision_packet_command_summary": format!(
                    "verify immutable decision-packet session under {} before freezing the final go-live handoff bundle",
                    decision_packet_session_dir.display()
                ),
                "frozen_live_cutover_controller_command_summary": format!(
                    "copybot_tiny_live_activation_package_live_cutover --package-dir {} --install-root {} --target-service solana-copy-bot.service --backend-command {} --wrapper-timeout-ms 1200 --service-status-max-staleness-ms 4500 --session-dir {} --run-live-package-cutover",
                    shell_escape_path(&package_dir),
                    shell_escape_path(&install_root),
                    shell_escape_path(&backend_command),
                    shell_escape_path(&fixture_dir.join("frozen-live-cutover-session")),
                ),
                "operator_checklist_summary": "Checklist: verify the intended prod host still matches the frozen contract",
                "operator_runbook_summary": "Runbook: 1. Reconfirm current gate truth immediately before go-live.",
                "handoff_bundle_summary": "Dossier summary: this immutable bundle is ready for final manual go-live review when gate truth turns green",
                "explicit_statement": "handoff-bundle statement"
            }),
        )
        .unwrap();
        persist_json(
            &handoff_bundle_session_dir.join("tiny_live_activation_package_handoff_bundle.status.json"),
            &json!({
                "session_dir": handoff_bundle_session_dir.display().to_string(),
                "decision_packet_session_dir": decision_packet_session_dir.display().to_string(),
                "result": verify_report.get("result").and_then(|value| value.as_str()).unwrap(),
                "decision_packet_result": verify_report.get("decision_packet_result").and_then(|value| value.as_str()).unwrap(),
                "execute_frozen_result": verify_report.get("execute_frozen_result").and_then(|value| value.as_str()).unwrap(),
                "current_pre_activation_gate_verdict": verify_report.get("current_pre_activation_gate_verdict").and_then(|value| value.as_str()),
                "current_pre_activation_gate_reason": verify_report.get("current_pre_activation_gate_reason").and_then(|value| value.as_str()),
                "operator_checklist_summary": "Checklist: verify the intended prod host still matches the frozen contract",
                "operator_runbook_summary": "Runbook: 1. Reconfirm current gate truth immediately before go-live.",
                "handoff_bundle_summary": "Dossier summary: this immutable bundle is ready for final manual go-live review when gate truth turns green",
                "explicit_statement": "handoff-bundle status statement"
            }),
        )
        .unwrap();
        let mut verify_report = verify_report;
        bind_handoff_bundle_report_to_fixture(
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
        let invalid_verify_report = invalid_handoff_bundle_verify_report(&verify_report);
        persist_json(
            &handoff_bundle_session_dir
                .join("tiny_live_activation_package_handoff_bundle.decision_packet.report.json"),
            &json!({
                "decision_packet_session_dir": decision_packet_session_dir.display().to_string()
            }),
        )
        .unwrap();
        let verify_path = fixture_dir.join("handoff-bundle.verify.json");
        let invalid_verify_path = fixture_dir.join("handoff-bundle.verify.invalid.json");
        persist_json(&verify_path, &verify_report).unwrap();
        persist_json(&invalid_verify_path, &invalid_verify_report).unwrap();
        write_fake_handoff_bundle_verify_command(
            &bin_dir.join("copybot_tiny_live_activation_package_handoff_bundle"),
            &verify_path,
            &invalid_verify_path,
            &decision_packet_session_dir,
            &handoff_bundle_session_dir,
        );

        FakeCommandFixture {
            fixture_dir,
            bin_dir,
            handoff_bundle_session_dir,
            review_receipt_session_dir,
            decision_packet_session_dir,
            install_root,
        }
    }

    fn bind_handoff_bundle_report_to_fixture(
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
            .expect("handoff-bundle report fixture must be an object");
        object.insert(
            "mode".to_string(),
            json!("verify_live_package_handoff_bundle"),
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
            "verify_decision_packet_command_summary".to_string(),
            json!(format!(
                "verify immutable decision-packet session under {} before freezing the final go-live handoff bundle",
                decision_packet_session_dir.display()
            )),
        );
        object.insert(
            "frozen_live_cutover_controller_command_summary".to_string(),
            json!(format!(
                "copybot_tiny_live_activation_package_live_cutover --package-dir {} --install-root {} --target-service solana-copy-bot.service --backend-command {} --wrapper-timeout-ms 1200 --service-status-max-staleness-ms 4500 --session-dir {} --run-live-package-cutover",
                shell_escape_path(package_dir),
                shell_escape_path(install_root),
                shell_escape_path(backend_command),
                shell_escape_path(&handoff_bundle_session_dir.join("frozen-live-cutover-session")),
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
            "handoff_bundle_summary".to_string(),
            json!("Dossier summary: this immutable bundle is ready for final manual go-live review when gate truth turns green"),
        );
        object.insert(
            "explicit_statement".to_string(),
            json!("handoff-bundle verify statement"),
        );
    }

    fn handoff_bundle_verify_report(
        result: &str,
        decision_packet_result: &str,
        execute_frozen_result: &str,
        gate_verdict: &str,
        gate_reason: &str,
    ) -> serde_json::Value {
        json!({
            "generated_at": Utc::now(),
            "mode": "verify_live_package_handoff_bundle",
            "verdict": "tiny_live_package_handoff_bundle_verify_ok",
            "reason": "handoff-bundle chain is coherent",
            "result": result,
            "decision_packet_result": decision_packet_result,
            "execute_frozen_result": execute_frozen_result,
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason,
        })
    }

    fn invalid_handoff_bundle_verify_report(
        success_report: &serde_json::Value,
    ) -> serde_json::Value {
        let mut report = success_report.clone();
        let object = report
            .as_object_mut()
            .expect("handoff-bundle verify report fixture must be an object");
        object.insert(
            "verdict".to_string(),
            json!("tiny_live_package_handoff_bundle_verify_invalid"),
        );
        object.insert(
            "reason".to_string(),
            json!("stored decision_packet_session_dir does not match the trusted handoff-bundle contract"),
        );
        object.insert(
            "result".to_string(),
            json!("refused_now_by_invalid_or_drifted_contract"),
        );
        report
    }

    fn write_fake_handoff_bundle_verify_command(
        path: &Path,
        verify_json_path: &Path,
        invalid_json_path: &Path,
        expected_decision_packet_session_dir: &Path,
        handoff_bundle_session_dir: &Path,
    ) {
        let expected_decision_packet_session_dir =
            expected_decision_packet_session_dir.display().to_string();
        let handoff_bundle_session_dir = handoff_bundle_session_dir.display().to_string();
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --decision-packet-session-dir {expected_decision_packet_session_dir} \"*) : ;;\n  *) echo 'unexpected decision-packet-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {handoff_bundle_session_dir} \"*) : ;;\n  *) echo 'unexpected handoff-bundle session dir' >&2; exit 1;;\nesac\nsession_json='{handoff_bundle_session_dir}/tiny_live_activation_package_handoff_bundle.session.json'\nif grep -F '\"decision_packet_session_dir\": \"{expected_decision_packet_session_dir}\"' \"$session_json\" >/dev/null; then\n  cat '{}'\nelse\n  cat '{}'\nfi\n",
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
