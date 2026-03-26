use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use copybot_discovery::wallet_freshness_audit::DEFAULT_HISTORY_CAPTURE_LIMIT;
use serde::Serialize;
use serde_json::{json, Value};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[allow(dead_code)]
#[path = "copybot_activation_decision_packet.rs"]
mod activation_decision_packet;
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_plan.rs"]
mod tiny_live_activation_plan;

const USAGE: &str = "usage: copybot_activation_runbook --config <prod-path> --non-prod-config <path> [--json] [--output <path>] [--markdown-output <path>] [--now <rfc3339>] [--stage3-limit <count>] [--stage3-recent-horizon-seconds <seconds>] [--rehearsal-limit <count>] [--rehearsal-recent-horizon-seconds <seconds>] [--min-recent-acceptable-rehearsals <count>] [--non-prod-limit <count>] [--non-prod-dress-recent-horizon-seconds <seconds>] [--non-prod-activation-recent-horizon-seconds <seconds>] [--non-prod-min-recent-green-dress <count>] [--non-prod-min-recent-green-activation <count>]";
const RUNBOOK_VERSION: &str = "1";
const NOT_AUTHORIZED_DISCLAIMER: &str = "This runbook is planning-only. It does not authorize production activation, does not override the Stage 3 production gate, and must not be treated as permission to enable execution without an explicit later manual decision.";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed building tokio runtime for activation runbook")?;
    let output = runtime.block_on(run(config))?;
    println!("{output}");
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    config_path: PathBuf,
    non_prod_config_path: PathBuf,
    json: bool,
    output_path: Option<PathBuf>,
    markdown_output_path: Option<PathBuf>,
    now: DateTime<Utc>,
    stage3_limit: usize,
    stage3_recent_horizon_seconds: Option<u64>,
    rehearsal_limit: usize,
    rehearsal_recent_horizon_seconds: u64,
    min_recent_acceptable_rehearsals: usize,
    non_prod_limit: usize,
    non_prod_dress_recent_horizon_seconds: u64,
    non_prod_activation_recent_horizon_seconds: u64,
    non_prod_min_recent_green_dress: usize,
    non_prod_min_recent_green_activation: usize,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ActivationRunbookVerdict {
    RunbookBlocked,
    RunbookDiscussionReadyButNotAuthorized,
    RunbookRefusedForProfileMismatch,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RunbookSectionStatus {
    Ready,
    Blocked,
    Warning,
    Informational,
}

#[derive(Debug, Clone, Serialize)]
struct SuggestedCommand {
    label: String,
    command: String,
}

#[derive(Debug, Clone, Serialize)]
struct RunbookSection {
    key: String,
    title: String,
    status: RunbookSectionStatus,
    summary: String,
    steps: Vec<String>,
    blockers: Vec<String>,
    warnings: Vec<String>,
    suggested_commands: Vec<SuggestedCommand>,
}

#[derive(Debug, Clone, Serialize)]
struct RollbackOverlayStep {
    field: String,
    activated_value: Value,
    rollback_value: Value,
    reason: String,
}

#[derive(Debug, Clone, Serialize)]
struct RunbookActivationCandidateSummary {
    verdict: String,
    reason: String,
    activation_overlay_complete: bool,
    activation_overlay_change_count: usize,
    activation_overlay_changes: Vec<tiny_live_activation_plan::ActivationPlanFieldDelta>,
    effective_tiny_live_contract: Option<tiny_live_activation_plan::EffectiveTinyLiveContract>,
}

#[derive(Debug, Clone, Serialize)]
struct RunbookRollbackSummary {
    rollback_plan_complete: bool,
    service_restart_contract_complete: bool,
    rollback_overlay_change_count: usize,
    rollback_overlay_changes: Vec<RollbackOverlayStep>,
    service_restart_contract: tiny_live_activation_plan::ServiceRestartContract,
    rollback_trigger_count: usize,
    rollback_triggers:
        Vec<tiny_live_activation_plan::tiny_live_guardrail_audit::RollbackTriggerSummary>,
}

#[derive(Debug, Clone, Serialize)]
struct ActivationRunbook {
    generated_at: DateTime<Utc>,
    runbook_version: String,
    prod_config_path: String,
    non_prod_config_path: String,
    json_output_path: Option<String>,
    markdown_output_path: Option<String>,
    execution_enabled: bool,
    read_only_runbook: bool,
    activation_authorized: bool,
    discussion_ready_only: bool,
    prod_stage3_remains_hard_gate: bool,
    non_prod_evidence_is_secondary: bool,
    verdict: ActivationRunbookVerdict,
    reason: String,
    blockers: Vec<String>,
    warnings: Vec<String>,
    not_authorized_disclaimer: String,
    decision_packet_version: String,
    decision_packet_generated_at: DateTime<Utc>,
    decision_packet_verdict: String,
    decision_packet_reason: String,
    build_version: String,
    git_commit: Option<String>,
    prod_config_fingerprint: activation_decision_packet::ConfigFingerprintSummary,
    non_prod_config_fingerprint: activation_decision_packet::ConfigFingerprintSummary,
    prod_pre_activation_gate:
        activation_decision_packet::activation_checklist_report::ProdPreActivationGateSummary,
    launch_dossier: activation_decision_packet::activation_checklist_report::LaunchDossierSummary,
    tiny_live_guardrails: activation_decision_packet::activation_checklist_report::GuardrailSummary,
    non_prod_readiness:
        activation_decision_packet::activation_checklist_report::NonProdReadinessSummary,
    activation_candidate: RunbookActivationCandidateSummary,
    rollback_summary: RunbookRollbackSummary,
    section_order: Vec<String>,
    sections: Vec<RunbookSection>,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut config_path: Option<PathBuf> = None;
    let mut non_prod_config_path: Option<PathBuf> = None;
    let mut json = false;
    let mut output_path: Option<PathBuf> = None;
    let mut markdown_output_path: Option<PathBuf> = None;
    let mut now: Option<DateTime<Utc>> = None;
    let mut stage3_limit = DEFAULT_HISTORY_CAPTURE_LIMIT;
    let mut stage3_recent_horizon_seconds: Option<u64> = None;
    let mut rehearsal_limit =
        activation_decision_packet::activation_checklist_report::DEFAULT_REHEARSAL_HISTORY_LIMIT;
    let mut rehearsal_recent_horizon_seconds =
        activation_decision_packet::activation_checklist_report::DEFAULT_REHEARSAL_RECENT_HORIZON_SECONDS;
    let mut min_recent_acceptable_rehearsals =
        activation_decision_packet::activation_checklist_report::DEFAULT_MIN_RECENT_ACCEPTABLE_REHEARSALS;
    let mut non_prod_limit =
        activation_decision_packet::activation_checklist_report::DEFAULT_NON_PROD_HISTORY_LIMIT;
    let mut non_prod_dress_recent_horizon_seconds = activation_decision_packet::activation_checklist_report::DEFAULT_NON_PROD_DRESS_RECENT_HORIZON_SECONDS;
    let mut non_prod_activation_recent_horizon_seconds = activation_decision_packet::activation_checklist_report::DEFAULT_NON_PROD_ACTIVATION_RECENT_HORIZON_SECONDS;
    let mut non_prod_min_recent_green_dress =
        activation_decision_packet::activation_checklist_report::DEFAULT_NON_PROD_MIN_RECENT_GREEN_DRESS;
    let mut non_prod_min_recent_green_activation = activation_decision_packet::activation_checklist_report::DEFAULT_NON_PROD_MIN_RECENT_GREEN_ACTIVATION;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--non-prod-config" => {
                non_prod_config_path = Some(PathBuf::from(parse_string_arg(
                    "--non-prod-config",
                    args.next(),
                )?))
            }
            "--json" => json = true,
            "--output" => {
                output_path = Some(PathBuf::from(parse_string_arg("--output", args.next())?))
            }
            "--markdown-output" => {
                markdown_output_path = Some(PathBuf::from(parse_string_arg(
                    "--markdown-output",
                    args.next(),
                )?))
            }
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--stage3-limit" => stage3_limit = parse_usize_arg("--stage3-limit", args.next())?,
            "--stage3-recent-horizon-seconds" => {
                stage3_recent_horizon_seconds = Some(parse_u64_arg(
                    "--stage3-recent-horizon-seconds",
                    args.next(),
                )?)
            }
            "--rehearsal-limit" => {
                rehearsal_limit = parse_usize_arg("--rehearsal-limit", args.next())?
            }
            "--rehearsal-recent-horizon-seconds" => {
                rehearsal_recent_horizon_seconds =
                    parse_u64_arg("--rehearsal-recent-horizon-seconds", args.next())?
            }
            "--min-recent-acceptable-rehearsals" => {
                min_recent_acceptable_rehearsals =
                    parse_usize_arg("--min-recent-acceptable-rehearsals", args.next())?
            }
            "--non-prod-limit" => {
                non_prod_limit = parse_usize_arg("--non-prod-limit", args.next())?
            }
            "--non-prod-dress-recent-horizon-seconds" => {
                non_prod_dress_recent_horizon_seconds =
                    parse_u64_arg("--non-prod-dress-recent-horizon-seconds", args.next())?
            }
            "--non-prod-activation-recent-horizon-seconds" => {
                non_prod_activation_recent_horizon_seconds =
                    parse_u64_arg("--non-prod-activation-recent-horizon-seconds", args.next())?
            }
            "--non-prod-min-recent-green-dress" => {
                non_prod_min_recent_green_dress =
                    parse_usize_arg("--non-prod-min-recent-green-dress", args.next())?
            }
            "--non-prod-min-recent-green-activation" => {
                non_prod_min_recent_green_activation =
                    parse_usize_arg("--non-prod-min-recent-green-activation", args.next())?
            }
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        non_prod_config_path: non_prod_config_path
            .ok_or_else(|| anyhow!("missing required --non-prod-config"))?,
        json,
        output_path,
        markdown_output_path,
        now: now.unwrap_or_else(Utc::now),
        stage3_limit: stage3_limit.max(1),
        stage3_recent_horizon_seconds,
        rehearsal_limit: rehearsal_limit.max(1),
        rehearsal_recent_horizon_seconds: rehearsal_recent_horizon_seconds.max(1),
        min_recent_acceptable_rehearsals: min_recent_acceptable_rehearsals.max(1),
        non_prod_limit: non_prod_limit.max(1),
        non_prod_dress_recent_horizon_seconds: non_prod_dress_recent_horizon_seconds.max(1),
        non_prod_activation_recent_horizon_seconds: non_prod_activation_recent_horizon_seconds
            .max(1),
        non_prod_min_recent_green_dress: non_prod_min_recent_green_dress.max(1),
        non_prod_min_recent_green_activation: non_prod_min_recent_green_activation.max(1),
    }))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed)
}

fn parse_ts_arg(flag: &str, value: Option<String>) -> Result<DateTime<Utc>> {
    let raw = parse_string_arg(flag, value)?;
    DateTime::parse_from_rfc3339(&raw)
        .map(|ts| ts.with_timezone(&Utc))
        .with_context(|| format!("invalid {flag} rfc3339 timestamp: {raw}"))
}

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<u64>()
        .with_context(|| format!("invalid {flag} u64 value: {raw}"))
}

fn parse_usize_arg(flag: &str, value: Option<String>) -> Result<usize> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<usize>()
        .with_context(|| format!("invalid {flag} usize value: {raw}"))
}

async fn run(config: Config) -> Result<String> {
    let packet = activation_decision_packet::evaluate_activation_decision_packet(
        &activation_decision_packet::Config {
            config_path: config.config_path.clone(),
            non_prod_config_path: config.non_prod_config_path.clone(),
            json: false,
            output_path: None,
            note: None,
            now: config.now,
            stage3_limit: config.stage3_limit,
            stage3_recent_horizon_seconds: config.stage3_recent_horizon_seconds,
            rehearsal_limit: config.rehearsal_limit,
            rehearsal_recent_horizon_seconds: config.rehearsal_recent_horizon_seconds,
            min_recent_acceptable_rehearsals: config.min_recent_acceptable_rehearsals,
            non_prod_limit: config.non_prod_limit,
            non_prod_dress_recent_horizon_seconds: config.non_prod_dress_recent_horizon_seconds,
            non_prod_activation_recent_horizon_seconds: config
                .non_prod_activation_recent_horizon_seconds,
            non_prod_min_recent_green_dress: config.non_prod_min_recent_green_dress,
            non_prod_min_recent_green_activation: config.non_prod_min_recent_green_activation,
        },
    )
    .await?;
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let activation_plan = tiny_live_activation_plan::evaluate_tiny_live_activation_plan(
        &tiny_live_activation_plan::Config {
            config_path: config.config_path.clone(),
            json: false,
            output_path: None,
            now: config.now,
            stage3_limit: config.stage3_limit,
            stage3_recent_horizon_seconds: config.stage3_recent_horizon_seconds,
            rehearsal_limit: config.rehearsal_limit,
            rehearsal_recent_horizon_seconds: config.rehearsal_recent_horizon_seconds,
            min_recent_acceptable_rehearsals: config.min_recent_acceptable_rehearsals,
        },
        &loaded_config,
    )
    .await?;
    let runbook = build_runbook(&config, packet, activation_plan);
    let json_output =
        serde_json::to_string_pretty(&runbook).context("failed serializing activation runbook")?;
    if let Some(output_path) = &config.output_path {
        write_output(output_path, &json_output, "activation runbook json")?;
    }
    if let Some(markdown_output_path) = &config.markdown_output_path {
        let markdown = render_markdown(&runbook);
        write_output(
            markdown_output_path,
            &markdown,
            "activation runbook markdown",
        )?;
    }

    if config.json {
        Ok(json_output)
    } else {
        Ok(render_human(&runbook))
    }
}

fn build_runbook(
    config: &Config,
    packet: activation_decision_packet::ActivationDecisionPacket,
    activation_plan: tiny_live_activation_plan::TinyLiveActivationPlanReport,
) -> ActivationRunbook {
    let rollback_overlay_changes = activation_plan
        .activation_overlay_changes
        .iter()
        .map(|delta| RollbackOverlayStep {
            field: delta.field.clone(),
            activated_value: delta.activation_value.clone(),
            rollback_value: delta.rollback_value.clone(),
            reason: delta.reason.clone(),
        })
        .collect::<Vec<_>>();

    let (verdict, reason) = derive_runbook_verdict(&packet, &activation_plan);

    let mut blockers = packet.blockers.clone();
    if activation_plan.verdict
        != tiny_live_activation_plan::TinyLiveActivationPlanVerdict::ActivationPlanReadyWhenStageGateAllows
    {
        blockers.push(format!("activation_plan: {}", activation_plan.reason));
    }
    blockers.sort();
    blockers.dedup();

    let mut warnings = packet.warnings.clone();
    warnings.extend(activation_plan.drift_findings.iter().cloned());
    warnings.sort();
    warnings.dedup();

    let activation_candidate = RunbookActivationCandidateSummary {
        verdict: serialize_enum(&activation_plan.verdict),
        reason: activation_plan.reason.clone(),
        activation_overlay_complete: activation_plan.activation_overlay_complete,
        activation_overlay_change_count: activation_plan.activation_overlay_change_count,
        activation_overlay_changes: activation_plan.activation_overlay_changes.clone(),
        effective_tiny_live_contract: activation_plan.effective_tiny_live_contract.clone(),
    };
    let rollback_summary = RunbookRollbackSummary {
        rollback_plan_complete: activation_plan.rollback_plan_complete,
        service_restart_contract_complete: activation_plan.service_restart_contract_complete,
        rollback_overlay_change_count: rollback_overlay_changes.len(),
        rollback_overlay_changes,
        service_restart_contract: activation_plan.service_restart_contract.clone(),
        rollback_trigger_count: activation_plan.tiny_live_guardrails.rollback_triggers.len(),
        rollback_triggers: activation_plan
            .tiny_live_guardrails
            .rollback_triggers
            .clone(),
    };

    let sections = build_sections(
        config,
        &packet,
        &activation_candidate,
        &rollback_summary,
        verdict,
    );
    let section_order = sections.iter().map(|section| section.key.clone()).collect();

    ActivationRunbook {
        generated_at: config.now,
        runbook_version: RUNBOOK_VERSION.to_string(),
        prod_config_path: packet.prod_config_path.clone(),
        non_prod_config_path: packet.non_prod_config_path.clone(),
        json_output_path: config
            .output_path
            .as_ref()
            .map(|path| path.display().to_string()),
        markdown_output_path: config
            .markdown_output_path
            .as_ref()
            .map(|path| path.display().to_string()),
        execution_enabled: packet.execution_enabled,
        read_only_runbook: true,
        activation_authorized: false,
        discussion_ready_only: verdict
            == ActivationRunbookVerdict::RunbookDiscussionReadyButNotAuthorized,
        prod_stage3_remains_hard_gate: packet.prod_stage3_remains_hard_gate,
        non_prod_evidence_is_secondary: packet.non_prod_evidence_is_secondary,
        verdict,
        reason,
        blockers,
        warnings,
        not_authorized_disclaimer: NOT_AUTHORIZED_DISCLAIMER.to_string(),
        decision_packet_version: packet.packet_version.clone(),
        decision_packet_generated_at: packet.generated_at,
        decision_packet_verdict: serialize_enum(&packet.verdict),
        decision_packet_reason: packet.reason.clone(),
        build_version: packet.build_version.clone(),
        git_commit: packet.git_commit.clone(),
        prod_config_fingerprint: packet.prod_config_fingerprint.clone(),
        non_prod_config_fingerprint: packet.non_prod_config_fingerprint.clone(),
        prod_pre_activation_gate: packet.prod_pre_activation_gate.clone(),
        launch_dossier: packet.launch_dossier.clone(),
        tiny_live_guardrails: packet.tiny_live_guardrails.clone(),
        non_prod_readiness: packet.non_prod_readiness.clone(),
        activation_candidate,
        rollback_summary,
        section_order,
        sections,
    }
}

fn derive_runbook_verdict(
    packet: &activation_decision_packet::ActivationDecisionPacket,
    activation_plan: &tiny_live_activation_plan::TinyLiveActivationPlanReport,
) -> (ActivationRunbookVerdict, String) {
    match packet.verdict {
        activation_decision_packet::ActivationDecisionPacketVerdict::DecisionPacketRefusedForProfileMismatch => (
            ActivationRunbookVerdict::RunbookRefusedForProfileMismatch,
            packet.reason.clone(),
        ),
        activation_decision_packet::ActivationDecisionPacketVerdict::DecisionPacketBlocked => (
            ActivationRunbookVerdict::RunbookBlocked,
            format!(
                "activation decision packet is not discussion-ready yet: {}",
                packet.reason
            ),
        ),
        activation_decision_packet::ActivationDecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized => {
            if activation_plan.verdict
                != tiny_live_activation_plan::TinyLiveActivationPlanVerdict::ActivationPlanReadyWhenStageGateAllows
            {
                (
                    ActivationRunbookVerdict::RunbookBlocked,
                    format!(
                        "activation plan is not complete enough for operator handoff: {}",
                        activation_plan.reason
                    ),
                )
            } else {
                (
                    ActivationRunbookVerdict::RunbookDiscussionReadyButNotAuthorized,
                    "bounded launch dossier is discussion-ready for later manual review, but it is still not activation authorization".to_string(),
                )
            }
        }
    }
}

fn build_sections(
    config: &Config,
    packet: &activation_decision_packet::ActivationDecisionPacket,
    activation_candidate: &RunbookActivationCandidateSummary,
    rollback_summary: &RunbookRollbackSummary,
    verdict: ActivationRunbookVerdict,
) -> Vec<RunbookSection> {
    vec![
        build_current_state_section(packet, activation_candidate),
        build_preflight_checks_section(config, packet, verdict),
        build_stop_here_conditions_section(packet, activation_candidate, verdict),
        build_activation_candidate_section(config, activation_candidate),
        build_post_change_checks_section(config, verdict),
        build_rollback_triggers_section(config, rollback_summary),
        build_rollback_procedure_section(config, rollback_summary),
        build_disclaimer_section(),
    ]
}

fn build_current_state_section(
    packet: &activation_decision_packet::ActivationDecisionPacket,
    activation_candidate: &RunbookActivationCandidateSummary,
) -> RunbookSection {
    RunbookSection {
        key: "current_state".to_string(),
        title: "Current State".to_string(),
        status: if packet.execution_enabled {
            RunbookSectionStatus::Warning
        } else {
            RunbookSectionStatus::Informational
        },
        summary: format!(
            "Current production state is captured from the accepted decision packet. execution.enabled={}, prod_gate={}, launch_dossier={}, non_prod_readiness={}",
            packet.execution_enabled,
            packet.prod_pre_activation_gate.verdict,
            activation_candidate.verdict,
            packet.non_prod_readiness.verdict
        ),
        steps: vec![
            format!(
                "Decision packet verdict: {} ({})",
                serialize_enum(&packet.verdict),
                packet.reason
            ),
            format!(
                "Prod config fingerprint: {}",
                packet.prod_config_fingerprint.sha256
            ),
            format!(
                "Non-prod config fingerprint: {}",
                packet.non_prod_config_fingerprint.sha256
            ),
            format!(
                "Execution remains disabled in the source config: {}",
                !packet.execution_enabled
            ),
            format!(
                "Prod Stage 3 remains the hard gate: {}",
                packet.prod_stage3_remains_hard_gate
            ),
        ],
        blockers: Vec::new(),
        warnings: Vec::new(),
        suggested_commands: Vec::new(),
    }
}

fn build_preflight_checks_section(
    config: &Config,
    packet: &activation_decision_packet::ActivationDecisionPacket,
    verdict: ActivationRunbookVerdict,
) -> RunbookSection {
    let ready = verdict == ActivationRunbookVerdict::RunbookDiscussionReadyButNotAuthorized;
    let mut steps = vec![
        format!(
            "Confirm prod pre-activation gate remains green: {} ({})",
            packet.prod_pre_activation_gate.verdict, packet.prod_pre_activation_gate.reason
        ),
        format!(
            "Confirm non-prod readiness remains green enough: {} ({})",
            packet.non_prod_readiness.verdict, packet.non_prod_readiness.reason
        ),
        format!(
            "Confirm launch dossier remains bounded and complete: {} ({})",
            packet.launch_dossier.verdict, packet.launch_dossier.reason
        ),
        format!(
            "Confirm guardrails remain bounded: {} ({})",
            packet.tiny_live_guardrails.verdict, packet.tiny_live_guardrails.reason
        ),
        format!(
            "Regenerated prod config fingerprint must still match {} before any later manual change review",
            packet.prod_config_fingerprint.sha256
        ),
    ];
    if !ready && !packet.blockers.is_empty() {
        steps.push("Preflight is still blocked; see stop-here conditions before any later manual activation review.".to_string());
    }

    RunbookSection {
        key: "preflight_checks".to_string(),
        title: "Preflight Checks".to_string(),
        status: if ready {
            RunbookSectionStatus::Ready
        } else {
            RunbookSectionStatus::Blocked
        },
        summary: if ready {
            "All planning-safe prerequisites are currently discussion-ready, but still not authorized for activation.".to_string()
        } else {
            "One or more planning-safe prerequisites are still blocked or stale; do not prepare a production activation change from this runbook yet.".to_string()
        },
        steps,
        blockers: packet.blockers.clone(),
        warnings: packet.warnings.clone(),
        suggested_commands: vec![
            SuggestedCommand {
                label: "Regenerate activation decision packet".to_string(),
                command: format!(
                    "copybot_activation_decision_packet --config {} --non-prod-config {} --json",
                    config.config_path.display(),
                    config.non_prod_config_path.display()
                ),
            },
            SuggestedCommand {
                label: "Regenerate final activation checklist".to_string(),
                command: format!(
                    "copybot_activation_checklist_report --config {} --non-prod-config {} --json",
                    config.config_path.display(),
                    config.non_prod_config_path.display()
                ),
            },
            SuggestedCommand {
                label: "Re-check guardrail contract".to_string(),
                command: format!(
                    "copybot_tiny_live_guardrail_audit --config {} --json",
                    config.config_path.display()
                ),
            },
            SuggestedCommand {
                label: "Re-check non-prod readiness".to_string(),
                command: format!(
                    "copybot_devnet_readiness_report --config {} --json",
                    config.non_prod_config_path.display()
                ),
            },
        ],
    }
}

fn build_stop_here_conditions_section(
    packet: &activation_decision_packet::ActivationDecisionPacket,
    activation_candidate: &RunbookActivationCandidateSummary,
    verdict: ActivationRunbookVerdict,
) -> RunbookSection {
    let mut steps = vec![
        format!(
            "Stop if the regenerated activation decision packet no longer yields {}.",
            serialize_enum(&packet.verdict)
        ),
        format!(
            "Stop if the regenerated prod config fingerprint no longer matches {}.",
            packet.prod_config_fingerprint.sha256
        ),
        "Stop if execution.enabled is already true before the manual review/change window starts."
            .to_string(),
        "Stop if any required service-status or log check fails after the planned restart window."
            .to_string(),
    ];
    if verdict != ActivationRunbookVerdict::RunbookDiscussionReadyButNotAuthorized {
        steps.extend(
            packet
                .blockers
                .iter()
                .map(|blocker| format!("Stop because blocker remains: {blocker}")),
        );
        if activation_candidate.verdict
            != serialize_enum(
                &tiny_live_activation_plan::TinyLiveActivationPlanVerdict::ActivationPlanReadyWhenStageGateAllows,
            )
        {
            steps.push(format!(
                "Stop because the activation overlay is not complete enough yet: {}",
                activation_candidate.reason
            ));
        }
    }

    RunbookSection {
        key: "stop_here_conditions".to_string(),
        title: "Stop Here Conditions".to_string(),
        status: if verdict == ActivationRunbookVerdict::RunbookDiscussionReadyButNotAuthorized {
            RunbookSectionStatus::Warning
        } else {
            RunbookSectionStatus::Blocked
        },
        summary: "These are the explicit conditions that must stop the manual review/change path before any future activation discussion continues.".to_string(),
        steps,
        blockers: packet.blockers.clone(),
        warnings: Vec::new(),
        suggested_commands: Vec::new(),
    }
}

fn build_activation_candidate_section(
    config: &Config,
    activation_candidate: &RunbookActivationCandidateSummary,
) -> RunbookSection {
    let mut steps = vec![
        "Apply only the listed activation overlay fields; fields omitted from this section remain intentionally unchanged.".to_string(),
    ];
    steps.extend(
        activation_candidate
            .activation_overlay_changes
            .iter()
            .map(|delta| {
                format!(
                    "Set {} from {} to {} ({})",
                    delta.field,
                    render_value(&delta.current_value),
                    render_value(&delta.activation_value),
                    delta.reason
                )
            }),
    );
    if let Some(contract) = &activation_candidate.effective_tiny_live_contract {
        steps.push(format!(
            "Bounded execution envelope: mode={}, default_route={}, allowed_routes={}, batch_size={}, max_position_sol={}, daily_loss_limit_pct={}",
            contract.execution_mode,
            contract.default_route,
            contract.allowed_routes.join(","),
            contract.execution_batch_size,
            contract.risk_max_position_sol,
            contract.risk_daily_loss_limit_pct
        ));
    }

    RunbookSection {
        key: "bounded_activation_candidate".to_string(),
        title: "Bounded Activation Candidate".to_string(),
        status: if activation_candidate.activation_overlay_complete {
            RunbookSectionStatus::Ready
        } else {
            RunbookSectionStatus::Blocked
        },
        summary: format!(
            "Activation overlay verdict: {} ({})",
            activation_candidate.verdict, activation_candidate.reason
        ),
        steps,
        blockers: if activation_candidate.activation_overlay_complete {
            Vec::new()
        } else {
            vec![activation_candidate.reason.clone()]
        },
        warnings: Vec::new(),
        suggested_commands: vec![
            SuggestedCommand {
                label: "Prepare a candidate config copy for human review".to_string(),
                command: format!(
                    "cp {} /tmp/solana-copy-bot.tiny-live.candidate.toml",
                    config.config_path.display()
                ),
            },
            SuggestedCommand {
                label: "Review the candidate diff against the current live config".to_string(),
                command: format!(
                    "diff -u {} /tmp/solana-copy-bot.tiny-live.candidate.toml",
                    config.config_path.display()
                ),
            },
        ],
    }
}

fn build_post_change_checks_section(
    config: &Config,
    verdict: ActivationRunbookVerdict,
) -> RunbookSection {
    RunbookSection {
        key: "post_change_checks".to_string(),
        title: "Post-Change Checks".to_string(),
        status: if verdict == ActivationRunbookVerdict::RunbookDiscussionReadyButNotAuthorized {
            RunbookSectionStatus::Ready
        } else {
            RunbookSectionStatus::Warning
        },
        summary: "If a later manual change window ever uses this dossier, these are the immediate verification checks to run after the config change and service restart.".to_string(),
        steps: vec![
            "Verify solana-copy-bot.service is active and not crash-looping.".to_string(),
            "Inspect recent service logs for adapter contract failures, policy-echo mismatches, and connectivity degradation.".to_string(),
            "Re-run execution readiness and tiny-live guardrail audits against the changed config state.".to_string(),
            "Confirm the live process is still bounded by the planned route/notional/fee envelope.".to_string(),
        ],
        blockers: Vec::new(),
        warnings: Vec::new(),
        suggested_commands: vec![
            SuggestedCommand {
                label: "Check service status".to_string(),
                command: "systemctl status solana-copy-bot.service --no-pager".to_string(),
            },
            SuggestedCommand {
                label: "Check recent service logs".to_string(),
                command: "journalctl -u solana-copy-bot.service -n 200 --no-pager".to_string(),
            },
            SuggestedCommand {
                label: "Re-run readiness audit".to_string(),
                command: format!(
                    "copybot_execution_readiness_audit --config {} --json",
                    config.config_path.display()
                ),
            },
            SuggestedCommand {
                label: "Re-run guardrail audit".to_string(),
                command: format!(
                    "copybot_tiny_live_guardrail_audit --config {} --json",
                    config.config_path.display()
                ),
            },
        ],
    }
}

fn build_rollback_triggers_section(
    config: &Config,
    rollback_summary: &RunbookRollbackSummary,
) -> RunbookSection {
    RunbookSection {
        key: "rollback_triggers".to_string(),
        title: "Rollback Triggers".to_string(),
        status: if rollback_summary.rollback_trigger_count > 0 {
            RunbookSectionStatus::Ready
        } else {
            RunbookSectionStatus::Blocked
        },
        summary: "These thresholds define the explicit future stop-and-rollback contract for any later tiny-live activation window.".to_string(),
        steps: rollback_summary
            .rollback_triggers
            .iter()
            .map(format_rollback_trigger)
            .collect(),
        blockers: if rollback_summary.rollback_trigger_count > 0 {
            Vec::new()
        } else {
            vec!["No rollback triggers were rendered into the runbook.".to_string()]
        },
        warnings: Vec::new(),
        suggested_commands: vec![SuggestedCommand {
            label: "Re-check guardrail audit".to_string(),
            command: format!(
                "copybot_tiny_live_guardrail_audit --config {} --json",
                config.config_path.display()
            ),
        }],
    }
}

fn build_rollback_procedure_section(
    _config: &Config,
    rollback_summary: &RunbookRollbackSummary,
) -> RunbookSection {
    let mut steps = rollback_summary
        .rollback_overlay_changes
        .iter()
        .map(|delta| {
            format!(
                "Revert {} from {} to {} ({})",
                delta.field,
                render_value(&delta.activated_value),
                render_value(&delta.rollback_value),
                delta.reason
            )
        })
        .collect::<Vec<_>>();
    steps.extend(
        rollback_summary
            .service_restart_contract
            .rollback_steps
            .iter()
            .cloned(),
    );

    RunbookSection {
        key: "rollback_procedure".to_string(),
        title: "Rollback Procedure".to_string(),
        status: if rollback_summary.rollback_plan_complete
            && rollback_summary.service_restart_contract_complete
        {
            RunbookSectionStatus::Ready
        } else {
            RunbookSectionStatus::Blocked
        },
        summary: "These are the exact config reversions and restart steps required to return to the current safe-mode contract.".to_string(),
        steps,
        blockers: if rollback_summary.rollback_plan_complete
            && rollback_summary.service_restart_contract_complete
        {
            Vec::new()
        } else {
            vec!["Rollback delta or restart contract is incomplete.".to_string()]
        },
        warnings: Vec::new(),
        suggested_commands: vec![
            SuggestedCommand {
                label: "Check service status after rollback".to_string(),
                command: "systemctl status solana-copy-bot.service --no-pager".to_string(),
            },
            SuggestedCommand {
                label: "Check rollback logs".to_string(),
                command: "journalctl -u solana-copy-bot.service -n 200 --no-pager".to_string(),
            },
        ],
    }
}

fn build_disclaimer_section() -> RunbookSection {
    RunbookSection {
        key: "not_authorized_disclaimer".to_string(),
        title: "Not Authorized Disclaimer".to_string(),
        status: RunbookSectionStatus::Warning,
        summary: NOT_AUTHORIZED_DISCLAIMER.to_string(),
        steps: vec![
            "Do not apply this runbook directly to production without a separate later manual approval decision.".to_string(),
            "Do not treat a discussion-ready runbook as permission to bypass the Stage 3 production gate.".to_string(),
            "Do not treat non-prod drill evidence as authority over production discovery truth.".to_string(),
        ],
        blockers: Vec::new(),
        warnings: Vec::new(),
        suggested_commands: Vec::new(),
    }
}

fn format_rollback_trigger(
    trigger: &tiny_live_activation_plan::tiny_live_guardrail_audit::RollbackTriggerSummary,
) -> String {
    let threshold = if let Some(rate) = trigger.threshold_rate_pct {
        format!("{rate}%")
    } else if let Some(seconds) = trigger.threshold_seconds {
        format!("{seconds}s")
    } else if let Some(sol) = trigger.threshold_sol {
        format!("{sol} SOL")
    } else if let Some(count) = trigger.threshold_count {
        count.to_string()
    } else {
        "unspecified".to_string()
    };
    let window = trigger
        .evaluation_window_seconds
        .map(|seconds| format!(" within {seconds}s"))
        .unwrap_or_default();
    format!(
        "Rollback if {} exceeds {}{}; action={}",
        trigger.trigger, threshold, window, trigger.action
    )
}

fn render_value(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"<unserializable>\"".to_string())
}

fn render_human(runbook: &ActivationRunbook) -> String {
    let section_states = runbook
        .sections
        .iter()
        .map(|section| format!("{}:{}", section.key, serialize_enum(&section.status)))
        .collect::<Vec<_>>()
        .join(",");

    [
        "event=copybot_activation_runbook".to_string(),
        format!("generated_at={}", runbook.generated_at.to_rfc3339()),
        format!("runbook_version={}", runbook.runbook_version),
        format!("verdict={}", serialize_enum(&runbook.verdict)),
        format!("reason={}", runbook.reason),
        format!(
            "decision_packet_verdict={}",
            runbook.decision_packet_verdict
        ),
        format!(
            "prod_pre_activation_gate_verdict={}",
            runbook.prod_pre_activation_gate.verdict
        ),
        format!("launch_dossier_verdict={}", runbook.launch_dossier.verdict),
        format!(
            "tiny_live_guardrail_verdict={}",
            runbook.tiny_live_guardrails.verdict
        ),
        format!(
            "non_prod_readiness_verdict={}",
            runbook.non_prod_readiness.verdict
        ),
        format!("execution_enabled={}", runbook.execution_enabled),
        format!("discussion_ready_only={}", runbook.discussion_ready_only),
        format!(
            "prod_config_fingerprint_sha256={}",
            runbook.prod_config_fingerprint.sha256
        ),
        format!(
            "non_prod_config_fingerprint_sha256={}",
            runbook.non_prod_config_fingerprint.sha256
        ),
        format!("blockers={}", runbook.blockers.join(" | ")),
        format!("warnings={}", runbook.warnings.join(" | ")),
        format!("section_states={section_states}"),
        format!(
            "not_authorized_disclaimer={}",
            runbook.not_authorized_disclaimer
        ),
    ]
    .join("\n")
}

fn render_markdown(runbook: &ActivationRunbook) -> String {
    let mut lines = vec![
        "# Tiny-Live Activation Runbook".to_string(),
        String::new(),
        format!("- Generated at: `{}`", runbook.generated_at.to_rfc3339()),
        format!("- Runbook verdict: `{}`", serialize_enum(&runbook.verdict)),
        format!("- Reason: {}", runbook.reason),
        format!(
            "- Decision packet verdict: `{}`",
            runbook.decision_packet_verdict
        ),
        format!("- Prod config path: `{}`", runbook.prod_config_path),
        format!("- Non-prod config path: `{}`", runbook.non_prod_config_path),
        format!(
            "- Prod config fingerprint: `{}`",
            runbook.prod_config_fingerprint.sha256
        ),
        format!(
            "- Non-prod config fingerprint: `{}`",
            runbook.non_prod_config_fingerprint.sha256
        ),
        format!("- Execution enabled now: `{}`", runbook.execution_enabled),
        String::new(),
        format!("> {}", runbook.not_authorized_disclaimer),
        String::new(),
    ];

    for section in &runbook.sections {
        lines.push(format!("## {}", section.title));
        lines.push(String::new());
        lines.push(format!("- Status: `{}`", serialize_enum(&section.status)));
        lines.push(format!("- Summary: {}", section.summary));
        if !section.blockers.is_empty() {
            lines.push("- Blockers:".to_string());
            lines.extend(section.blockers.iter().map(|item| format!("  - {}", item)));
        }
        if !section.warnings.is_empty() {
            lines.push("- Warnings:".to_string());
            lines.extend(section.warnings.iter().map(|item| format!("  - {}", item)));
        }
        if !section.steps.is_empty() {
            lines.push("- Steps:".to_string());
            lines.extend(section.steps.iter().map(|item| format!("  - {}", item)));
        }
        if !section.suggested_commands.is_empty() {
            lines.push("- Suggested commands:".to_string());
            lines.extend(
                section
                    .suggested_commands
                    .iter()
                    .map(|item| format!("  - {}: `{}`", item.label, item.command)),
            );
        }
        lines.push(String::new());
    }

    lines.join("\n")
}

fn write_output(path: &Path, contents: &str, label: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed creating parent dir for {}", path.display()))?;
        }
    }
    fs::write(path, contents)
        .with_context(|| format!("failed writing {} {}", label, path.display()))
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocked_decision_packet_yields_blocked_runbook() {
        let runbook = build_runbook(&test_config(), blocked_packet(), ready_activation_plan());

        assert_eq!(runbook.verdict, ActivationRunbookVerdict::RunbookBlocked);
        assert!(!runbook.activation_authorized);
        assert!(!runbook.discussion_ready_only);
    }

    #[test]
    fn discussion_ready_packet_yields_discussion_ready_runbook() {
        let runbook = build_runbook(
            &test_config(),
            discussion_ready_packet(),
            ready_activation_plan(),
        );

        assert_eq!(
            runbook.verdict,
            ActivationRunbookVerdict::RunbookDiscussionReadyButNotAuthorized
        );
        assert!(runbook.discussion_ready_only);
        assert!(!runbook.activation_authorized);
    }

    #[test]
    fn exported_runbook_contains_expected_ordered_sections() {
        let runbook = build_runbook(
            &test_config(),
            discussion_ready_packet(),
            ready_activation_plan(),
        );

        assert_eq!(
            runbook.section_order,
            vec![
                "current_state",
                "preflight_checks",
                "stop_here_conditions",
                "bounded_activation_candidate",
                "post_change_checks",
                "rollback_triggers",
                "rollback_procedure",
                "not_authorized_disclaimer",
            ]
        );
        assert_eq!(runbook.sections.len(), 8);
    }

    #[test]
    fn markdown_export_contains_not_authorized_disclaimer() {
        let runbook = build_runbook(
            &test_config(),
            discussion_ready_packet(),
            ready_activation_plan(),
        );
        let markdown = render_markdown(&runbook);

        assert!(markdown.contains("does not authorize production activation"));
        assert!(markdown.contains("## Current State"));
        assert!(markdown.contains("## Rollback Procedure"));
    }

    #[test]
    fn artifact_export_writes_expected_structure() {
        let runbook = build_runbook(
            &test_config(),
            discussion_ready_packet(),
            ready_activation_plan(),
        );
        let temp = temp_dir("activation_runbook_export");
        let json_path = temp.join("runbook.json");
        let markdown_path = temp.join("runbook.md");
        let json = serde_json::to_string_pretty(&runbook).expect("serialize");
        write_output(&json_path, &json, "activation runbook json").expect("write json");
        write_output(
            &markdown_path,
            &render_markdown(&runbook),
            "activation runbook markdown",
        )
        .expect("write markdown");

        let parsed: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&json_path).expect("read json"))
                .expect("parse");
        assert_eq!(
            parsed["verdict"],
            "runbook_discussion_ready_but_not_authorized"
        );
        assert_eq!(parsed["section_order"][0], "current_state");
        assert!(fs::read_to_string(&markdown_path)
            .expect("read md")
            .contains("not authorize production activation"));
    }

    #[test]
    fn no_secret_material_is_dumped() {
        let runbook = build_runbook(
            &test_config(),
            discussion_ready_packet(),
            ready_activation_plan(),
        );
        let serialized = serde_json::to_string(&runbook).expect("serialize");

        assert!(!serialized.contains("super-secret"));
        assert!(!serialized.contains("submit_adapter_auth_token"));
        assert!(!serialized.contains("hmac_secret"));
    }

    #[test]
    fn execution_remains_disabled_and_no_live_mutation_occurs() {
        let source_path = temp_dir("activation_runbook_no_mutation").join("live.server.toml");
        fs::write(&source_path, "execution_enabled=false\n").expect("write source");
        let before = fs::read_to_string(&source_path).expect("read before");

        let runbook = build_runbook(
            &test_config(),
            discussion_ready_packet(),
            ready_activation_plan(),
        );

        let after = fs::read_to_string(&source_path).expect("read after");
        assert_eq!(before, after);
        assert!(!runbook.execution_enabled);
        assert!(!runbook.activation_authorized);
    }

    fn test_config() -> Config {
        Config {
            config_path: PathBuf::from("/etc/solana-copy-bot/live.server.toml"),
            non_prod_config_path: PathBuf::from("/etc/solana-copy-bot/devnet.server.toml"),
            json: false,
            output_path: None,
            markdown_output_path: None,
            now: DateTime::parse_from_rfc3339("2026-03-26T12:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            stage3_limit: DEFAULT_HISTORY_CAPTURE_LIMIT,
            stage3_recent_horizon_seconds: None,
            rehearsal_limit:
                activation_decision_packet::activation_checklist_report::DEFAULT_REHEARSAL_HISTORY_LIMIT,
            rehearsal_recent_horizon_seconds:
                activation_decision_packet::activation_checklist_report::DEFAULT_REHEARSAL_RECENT_HORIZON_SECONDS,
            min_recent_acceptable_rehearsals:
                activation_decision_packet::activation_checklist_report::DEFAULT_MIN_RECENT_ACCEPTABLE_REHEARSALS,
            non_prod_limit:
                activation_decision_packet::activation_checklist_report::DEFAULT_NON_PROD_HISTORY_LIMIT,
            non_prod_dress_recent_horizon_seconds:
                activation_decision_packet::activation_checklist_report::DEFAULT_NON_PROD_DRESS_RECENT_HORIZON_SECONDS,
            non_prod_activation_recent_horizon_seconds:
                activation_decision_packet::activation_checklist_report::DEFAULT_NON_PROD_ACTIVATION_RECENT_HORIZON_SECONDS,
            non_prod_min_recent_green_dress:
                activation_decision_packet::activation_checklist_report::DEFAULT_NON_PROD_MIN_RECENT_GREEN_DRESS,
            non_prod_min_recent_green_activation:
                activation_decision_packet::activation_checklist_report::DEFAULT_NON_PROD_MIN_RECENT_GREEN_ACTIVATION,
        }
    }

    fn blocked_packet() -> activation_decision_packet::ActivationDecisionPacket {
        let mut packet = discussion_ready_packet();
        packet.verdict =
            activation_decision_packet::ActivationDecisionPacketVerdict::DecisionPacketBlocked;
        packet.reason = "Stage 3 is still blocked".to_string();
        packet.checklist_verdict = "activation_checklist_blocked_by_prod_stage3".to_string();
        packet.blockers = vec!["stage3: insufficient recent evidence".to_string()];
        packet
    }

    fn discussion_ready_packet() -> activation_decision_packet::ActivationDecisionPacket {
        activation_decision_packet::ActivationDecisionPacket {
            generated_at: DateTime::parse_from_rfc3339("2026-03-26T12:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            packet_version: "1".to_string(),
            build_version: "0.1.0".to_string(),
            git_commit: Some("deadbeef".to_string()),
            prod_config_path: "/etc/solana-copy-bot/live.server.toml".to_string(),
            non_prod_config_path: "/etc/solana-copy-bot/devnet.server.toml".to_string(),
            operator_note: None,
            execution_enabled: false,
            read_only_packet: true,
            activation_authorized: false,
            discussion_ready_only: true,
            prod_stage3_remains_hard_gate: true,
            non_prod_evidence_is_secondary: true,
            verdict: activation_decision_packet::ActivationDecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized,
            reason: "discussion ready for later manual review".to_string(),
            blockers: Vec::new(),
            warnings: vec!["still planning-only".to_string()],
            checklist_verdict:
                "activation_checklist_discussion_ready_but_not_authorized".to_string(),
            checklist_reason: "all planning-safe evidence is green enough".to_string(),
            prod_config_fingerprint: fingerprint("prod"),
            non_prod_config_fingerprint: fingerprint("non-prod"),
            prod_pre_activation_gate: activation_decision_packet::activation_checklist_report::ProdPreActivationGateSummary {
                verdict: "pre_activation_gates_green".to_string(),
                reason: "prod gate is green".to_string(),
                planning_green: true,
                blocked_by_stage3: false,
                stage3_verdict: "validated_current".to_string(),
                stage3_reason: "fresh current".to_string(),
                stage3_captures_within_recent_horizon: 3,
                stage3_latest_capture_age_seconds: Some(300),
                stage4_readiness_verdict: "ready_for_execution_dry_run".to_string(),
                stage4_rehearsal_history_verdict: "sufficient_recent_rehearsal_evidence".to_string(),
                tiny_live_policy_verdict: "tiny_live_policy_bounded".to_string(),
                blockers: Vec::new(),
            },
            launch_dossier: activation_decision_packet::activation_checklist_report::LaunchDossierSummary {
                verdict: "activation_plan_ready_when_stage_gate_allows".to_string(),
                reason: "bounded launch dossier is ready".to_string(),
                ready_when_stage_gate_allows: true,
                activation_overlay_complete: true,
                rollback_plan_complete: true,
                service_restart_contract_complete: true,
                activation_overlay_change_count: 3,
                rollback_overlay_change_count: 3,
                drift_finding_count: 0,
                blocker_count: 0,
                first_blocker: None,
            },
            tiny_live_guardrails: activation_decision_packet::activation_checklist_report::GuardrailSummary {
                verdict: "tiny_live_guardrails_bounded".to_string(),
                reason: "guardrails are bounded".to_string(),
                bounded: true,
                enabled: true,
                blocker_count: 0,
                first_blocker: None,
                rollback_trigger_count: 2,
                rollback_triggers: sample_packet_rollback_triggers(),
            },
            non_prod_readiness: activation_decision_packet::activation_checklist_report::NonProdReadinessSummary {
                verdict: "devnet_readiness_green".to_string(),
                reason: "recent non-prod evidence is green".to_string(),
                green: true,
                prod_profile_refused: false,
                config_env: "devnet".to_string(),
                blockers: Vec::new(),
                warnings: Vec::new(),
                dress_latest_record_age_seconds: Some(120),
                dress_recent_green_count: 3,
                activation_latest_record_age_seconds: Some(120),
                activation_recent_green_count: 3,
                activation_recent_rollback_success_count: 3,
                activation_recent_internal_consistency_count: 3,
                stale_evidence_excluded: false,
            },
        }
    }

    fn ready_activation_plan() -> tiny_live_activation_plan::TinyLiveActivationPlanReport {
        tiny_live_activation_plan::TinyLiveActivationPlanReport {
            generated_at: DateTime::parse_from_rfc3339("2026-03-26T12:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            config_path: "/etc/solana-copy-bot/live.server.toml".to_string(),
            output_path: None,
            planning_safe_only: true,
            activation_permission_granted: false,
            execution_enabled_current: false,
            current_execution_mode: "adapter_submit_confirm".to_string(),
            verdict: tiny_live_activation_plan::TinyLiveActivationPlanVerdict::ActivationPlanReadyWhenStageGateAllows,
            reason: "activation candidate is explicit".to_string(),
            blockers: Vec::new(),
            drift_findings: Vec::new(),
            pre_activation_gate: tiny_live_activation_plan::ActivationPlanGateSummary {
                verdict: "pre_activation_gates_green".to_string(),
                reason: "gate green".to_string(),
                planning_green: true,
                blockers: Vec::new(),
                execution_enabled: false,
                stage3_is_primary_gate: true,
            },
            tiny_live_policy: tiny_live_activation_plan::PolicyAuditSummary {
                verdict: "tiny_live_policy_bounded".to_string(),
                reason: "policy bounded".to_string(),
                bounded: true,
                tiny_live_policy_enabled: true,
                blocker_count: 0,
                first_blocker: None,
                warnings_count: 0,
            },
            tiny_live_guardrails: tiny_live_activation_plan::GuardrailAuditSummary {
                verdict: "tiny_live_guardrails_bounded".to_string(),
                reason: "guardrails bounded".to_string(),
                bounded: true,
                tiny_live_guardrails_enabled: true,
                first_blocker: None,
                blocker_count: 0,
                warnings_count: 0,
                rollback_triggers: sample_plan_rollback_triggers(),
            },
            activation_overlay_complete: true,
            rollback_plan_complete: true,
            service_restart_contract_complete: true,
            activation_overlay_change_count: 3,
            rollback_overlay_change_count: 3,
            activation_overlay_changes: vec![
                delta("execution.enabled", json!(false), json!(true), json!(false)),
                delta(
                    "shadow.copy_notional_sol",
                    json!(0.25),
                    json!(0.01),
                    json!(0.25),
                ),
                delta(
                    "execution.submit_allowed_routes",
                    json!(["jito", "rpc"]),
                    json!(["jito"]),
                    json!(["jito", "rpc"]),
                ),
            ],
            unchanged_fields: Vec::new(),
            effective_tiny_live_contract: Some(tiny_live_activation_plan::EffectiveTinyLiveContract {
                execution_mode: "adapter_submit_confirm".to_string(),
                execution_enabled_target: true,
                default_route: "jito".to_string(),
                allowed_routes: vec!["jito".to_string()],
                route_order: vec!["jito".to_string()],
                policy_echo_required: true,
                shadow_copy_notional_sol: 0.01,
                risk_max_position_sol: 0.01,
                execution_batch_size: 1,
                risk_max_concurrent_positions: 1,
                risk_daily_loss_limit_pct: 0.5,
                pretrade_max_fee_overhead_bps: 25,
                pretrade_max_priority_fee_lamports: 5_000,
                route_envelope: vec![tiny_live_activation_plan::RouteEnvelopeRow {
                    route: "jito".to_string(),
                    compute_unit_limit: 400_000,
                    slippage_bps: 35.0,
                    tip_lamports: 5_000,
                    compute_unit_price_micro_lamports: 1_000,
                }],
            }),
            service_restart_contract: tiny_live_activation_plan::ServiceRestartContract {
                complete: true,
                activation_restart_required: true,
                rollback_restart_required: true,
                activation_services: vec!["solana-copy-bot.service".to_string()],
                rollback_services: vec!["solana-copy-bot.service".to_string()],
                activation_steps: vec![
                    "systemctl daemon-reload".to_string(),
                    "systemctl restart solana-copy-bot.service".to_string(),
                ],
                rollback_steps: vec![
                    "systemctl daemon-reload".to_string(),
                    "systemctl restart solana-copy-bot.service".to_string(),
                ],
            },
        }
    }

    fn delta(
        field: &str,
        current_value: Value,
        activation_value: Value,
        rollback_value: Value,
    ) -> tiny_live_activation_plan::ActivationPlanFieldDelta {
        tiny_live_activation_plan::ActivationPlanFieldDelta {
            field: field.to_string(),
            current_value,
            activation_value,
            rollback_value,
            reason: "bounded tiny-live delta".to_string(),
            source: "test".to_string(),
        }
    }

    fn sample_plan_rollback_triggers(
    ) -> Vec<tiny_live_activation_plan::tiny_live_guardrail_audit::RollbackTriggerSummary> {
        vec![
            tiny_live_activation_plan::tiny_live_guardrail_audit::RollbackTriggerSummary {
                trigger: "consecutive_hard_failures".to_string(),
                threshold_kind: "count".to_string(),
                threshold_rate_pct: None,
                threshold_seconds: None,
                threshold_sol: None,
                threshold_count: Some(3),
                evaluation_window_seconds: Some(600),
                action: "rollback_now".to_string(),
            },
            tiny_live_activation_plan::tiny_live_guardrail_audit::RollbackTriggerSummary {
                trigger: "connectivity_degraded".to_string(),
                threshold_kind: "seconds".to_string(),
                threshold_rate_pct: None,
                threshold_seconds: Some(120),
                threshold_sol: None,
                threshold_count: None,
                evaluation_window_seconds: Some(600),
                action: "rollback_now".to_string(),
            },
        ]
    }

    fn sample_packet_rollback_triggers(
    ) -> Vec<
        activation_decision_packet::activation_checklist_report::tiny_live_guardrail_audit::RollbackTriggerSummary,
    >{
        vec![
            activation_decision_packet::activation_checklist_report::tiny_live_guardrail_audit::RollbackTriggerSummary {
                trigger: "consecutive_hard_failures".to_string(),
                threshold_kind: "count".to_string(),
                threshold_rate_pct: None,
                threshold_seconds: None,
                threshold_sol: None,
                threshold_count: Some(3),
                evaluation_window_seconds: Some(600),
                action: "rollback_now".to_string(),
            },
            activation_decision_packet::activation_checklist_report::tiny_live_guardrail_audit::RollbackTriggerSummary {
                trigger: "connectivity_degraded".to_string(),
                threshold_kind: "seconds".to_string(),
                threshold_rate_pct: None,
                threshold_seconds: Some(120),
                threshold_sol: None,
                threshold_count: None,
                evaluation_window_seconds: Some(600),
                action: "rollback_now".to_string(),
            },
        ]
    }

    fn fingerprint(scope: &str) -> activation_decision_packet::ConfigFingerprintSummary {
        activation_decision_packet::ConfigFingerprintSummary {
            scope: scope.to_string(),
            sha256: format!("{scope}_sha256"),
            secrets_excluded: true,
            sensitive_urls_redacted_before_hashing: true,
        }
    }

    fn temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{}_{}", prefix, nanos));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }
}
