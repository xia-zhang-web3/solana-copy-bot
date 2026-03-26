use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::{load_from_path, AppConfig};
use serde::Serialize;
use std::env;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
#[path = "copybot_tiny_live_policy_audit.rs"]
mod tiny_live_policy_audit;

const USAGE: &str = "usage: copybot_tiny_live_guardrail_audit --config <path> [--json]";
const TARGET_EXECUTION_MODE: &str = "adapter_submit_confirm";
const MAX_ALLOWED_EVALUATION_WINDOW_SECONDS: u64 = 900;
const MAX_ALLOWED_EXECUTION_ERROR_RATE_PCT: f64 = 5.0;
const MAX_ALLOWED_ADAPTER_CONTRACT_FAILURE_RATE_PCT: f64 = 1.0;
const MAX_ALLOWED_POLICY_ECHO_MISMATCH_RATE_PCT: f64 = 1.0;
const MAX_ALLOWED_FEE_OR_SLIPPAGE_BREACH_RATE_PCT: f64 = 5.0;
const MAX_ALLOWED_CONNECTIVITY_DEGRADED_WINDOW_SECONDS: u64 = 180;
const MAX_ALLOWED_DAILY_REALIZED_LOSS_SOL: f64 = 0.05;
const MAX_ALLOWED_CONSECUTIVE_HARD_FAILURES: u32 = 3;

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
    config_path: PathBuf,
    json: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TinyLiveGuardrailVerdict {
    TinyLiveGuardrailsBounded,
    TinyLiveGuardrailsIncomplete,
    TinyLiveGuardrailsTooOpen,
    TinyLiveGuardrailsRollbackContractIncomplete,
    TinyLiveGuardrailsMonitoringContractIncomplete,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GuardrailReferenceEnvelope {
    pub(crate) max_evaluation_window_seconds: u64,
    pub(crate) max_execution_error_rate_pct: f64,
    pub(crate) max_adapter_contract_failure_rate_pct: f64,
    pub(crate) max_policy_echo_mismatch_rate_pct: f64,
    pub(crate) max_fee_or_slippage_breach_rate_pct: f64,
    pub(crate) max_connectivity_degraded_window_seconds: u64,
    pub(crate) max_daily_realized_loss_sol: f64,
    pub(crate) max_consecutive_hard_failures: u32,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RollbackTriggerSummary {
    pub(crate) trigger: String,
    pub(crate) threshold_kind: String,
    pub(crate) threshold_rate_pct: Option<f64>,
    pub(crate) threshold_seconds: Option<u64>,
    pub(crate) threshold_sol: Option<f64>,
    pub(crate) threshold_count: Option<u32>,
    pub(crate) evaluation_window_seconds: Option<u64>,
    pub(crate) action: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct TinyLiveGuardrailAuditReport {
    pub(crate) generated_at: DateTime<Utc>,
    pub(crate) config_path: String,
    pub(crate) execution_enabled: bool,
    pub(crate) planning_safe_only: bool,
    pub(crate) activation_permission_granted: bool,
    pub(crate) stage3_gate_not_evaluated: bool,
    pub(crate) current_execution_mode: String,
    pub(crate) mode_compatible: bool,
    pub(crate) tiny_live_policy_verdict: String,
    pub(crate) tiny_live_policy_reason: String,
    pub(crate) tiny_live_policy_bounded: bool,
    pub(crate) tiny_live_guardrails_enabled: bool,
    pub(crate) monitoring_contract_complete: bool,
    pub(crate) rollback_contract_complete: bool,
    pub(crate) current_config_bounded_for_later_tiny_live_monitoring: bool,
    pub(crate) verdict: TinyLiveGuardrailVerdict,
    pub(crate) reason: String,
    pub(crate) blockers: Vec<String>,
    pub(crate) warnings: Vec<String>,
    pub(crate) evaluation_window_seconds: u64,
    pub(crate) max_execution_error_rate_pct: f64,
    pub(crate) max_adapter_contract_failure_rate_pct: f64,
    pub(crate) max_policy_echo_mismatch_rate_pct: f64,
    pub(crate) max_fee_or_slippage_breach_rate_pct: f64,
    pub(crate) max_connectivity_degraded_window_seconds: u64,
    pub(crate) max_daily_realized_loss_sol: f64,
    pub(crate) max_consecutive_hard_failures: u32,
    pub(crate) reference_envelope: GuardrailReferenceEnvelope,
    pub(crate) rollback_triggers: Vec<RollbackTriggerSummary>,
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
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        json,
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

fn run(config: Config) -> Result<String> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let report = evaluate_tiny_live_guardrails(&config.config_path, &loaded_config)?;
    if config.json {
        serde_json::to_string_pretty(&report)
            .context("failed serializing tiny-live guardrail audit json")
    } else {
        Ok(render_human(&report))
    }
}

pub(crate) fn evaluate_tiny_live_guardrails(
    config_path: &Path,
    loaded_config: &AppConfig,
) -> Result<TinyLiveGuardrailAuditReport> {
    let policy_report =
        tiny_live_policy_audit::evaluate_tiny_live_policy(config_path, loaded_config)?;
    let guardrails = &loaded_config.tiny_live_guardrails;
    let mode = normalize_mode(loaded_config.execution.mode.as_str());
    let mode_compatible = mode == TARGET_EXECUTION_MODE;

    let mut monitoring_blockers = Vec::new();
    let mut rollback_blockers = Vec::new();
    let mut too_open_blockers = Vec::new();
    let mut warnings = Vec::new();

    if !guardrails.enabled {
        monitoring_blockers.push(
            "tiny_live_guardrails.enabled=false; explicit post-activation monitoring contract is not configured"
                .to_string(),
        );
    }
    if !mode_compatible {
        monitoring_blockers.push(format!(
            "execution.mode={} is not the tiny-live target mode; expected {}",
            if mode.is_empty() {
                "<empty>"
            } else {
                mode.as_str()
            },
            TARGET_EXECUTION_MODE
        ));
    }

    if guardrails.evaluation_window_seconds == 0 {
        monitoring_blockers
            .push("tiny_live_guardrails.evaluation_window_seconds must be >= 1".to_string());
    }
    if !is_positive_pct(guardrails.max_execution_error_rate_pct) {
        monitoring_blockers.push(
            "tiny_live_guardrails.max_execution_error_rate_pct must be finite and > 0".to_string(),
        );
    }
    if !is_positive_pct(guardrails.max_adapter_contract_failure_rate_pct) {
        monitoring_blockers.push(
            "tiny_live_guardrails.max_adapter_contract_failure_rate_pct must be finite and > 0"
                .to_string(),
        );
    }
    if !is_positive_pct(guardrails.max_policy_echo_mismatch_rate_pct) {
        monitoring_blockers.push(
            "tiny_live_guardrails.max_policy_echo_mismatch_rate_pct must be finite and > 0"
                .to_string(),
        );
    }
    if !is_positive_pct(guardrails.max_fee_or_slippage_breach_rate_pct) {
        monitoring_blockers.push(
            "tiny_live_guardrails.max_fee_or_slippage_breach_rate_pct must be finite and > 0"
                .to_string(),
        );
    }
    if guardrails.max_connectivity_degraded_window_seconds == 0 {
        rollback_blockers.push(
            "tiny_live_guardrails.max_connectivity_degraded_window_seconds must be >= 1"
                .to_string(),
        );
    }
    if !guardrails.max_daily_realized_loss_sol.is_finite()
        || guardrails.max_daily_realized_loss_sol <= 0.0
    {
        rollback_blockers.push(
            "tiny_live_guardrails.max_daily_realized_loss_sol must be finite and > 0".to_string(),
        );
    }
    if guardrails.max_consecutive_hard_failures == 0 {
        rollback_blockers
            .push("tiny_live_guardrails.max_consecutive_hard_failures must be >= 1".to_string());
    }

    if guardrails.enabled {
        if guardrails.evaluation_window_seconds > MAX_ALLOWED_EVALUATION_WINDOW_SECONDS {
            too_open_blockers.push(format!(
                "tiny_live_guardrails.evaluation_window_seconds={} exceeds planning-safe max {}",
                guardrails.evaluation_window_seconds, MAX_ALLOWED_EVALUATION_WINDOW_SECONDS
            ));
        }
        if guardrails.max_execution_error_rate_pct > MAX_ALLOWED_EXECUTION_ERROR_RATE_PCT {
            too_open_blockers.push(format!(
                "tiny_live_guardrails.max_execution_error_rate_pct={:.4} exceeds planning-safe max {:.4}",
                guardrails.max_execution_error_rate_pct, MAX_ALLOWED_EXECUTION_ERROR_RATE_PCT
            ));
        }
        if guardrails.max_adapter_contract_failure_rate_pct
            > MAX_ALLOWED_ADAPTER_CONTRACT_FAILURE_RATE_PCT
        {
            too_open_blockers.push(format!(
                "tiny_live_guardrails.max_adapter_contract_failure_rate_pct={:.4} exceeds planning-safe max {:.4}",
                guardrails.max_adapter_contract_failure_rate_pct,
                MAX_ALLOWED_ADAPTER_CONTRACT_FAILURE_RATE_PCT
            ));
        }
        if guardrails.max_policy_echo_mismatch_rate_pct > MAX_ALLOWED_POLICY_ECHO_MISMATCH_RATE_PCT
        {
            too_open_blockers.push(format!(
                "tiny_live_guardrails.max_policy_echo_mismatch_rate_pct={:.4} exceeds planning-safe max {:.4}",
                guardrails.max_policy_echo_mismatch_rate_pct,
                MAX_ALLOWED_POLICY_ECHO_MISMATCH_RATE_PCT
            ));
        }
        if guardrails.max_fee_or_slippage_breach_rate_pct
            > MAX_ALLOWED_FEE_OR_SLIPPAGE_BREACH_RATE_PCT
        {
            too_open_blockers.push(format!(
                "tiny_live_guardrails.max_fee_or_slippage_breach_rate_pct={:.4} exceeds planning-safe max {:.4}",
                guardrails.max_fee_or_slippage_breach_rate_pct,
                MAX_ALLOWED_FEE_OR_SLIPPAGE_BREACH_RATE_PCT
            ));
        }
        if guardrails.max_connectivity_degraded_window_seconds
            > MAX_ALLOWED_CONNECTIVITY_DEGRADED_WINDOW_SECONDS
        {
            too_open_blockers.push(format!(
                "tiny_live_guardrails.max_connectivity_degraded_window_seconds={} exceeds planning-safe max {}",
                guardrails.max_connectivity_degraded_window_seconds,
                MAX_ALLOWED_CONNECTIVITY_DEGRADED_WINDOW_SECONDS
            ));
        }
        if guardrails.max_daily_realized_loss_sol > MAX_ALLOWED_DAILY_REALIZED_LOSS_SOL {
            too_open_blockers.push(format!(
                "tiny_live_guardrails.max_daily_realized_loss_sol={:.6} exceeds planning-safe max {:.6}",
                guardrails.max_daily_realized_loss_sol, MAX_ALLOWED_DAILY_REALIZED_LOSS_SOL
            ));
        }
        if guardrails.max_consecutive_hard_failures > MAX_ALLOWED_CONSECUTIVE_HARD_FAILURES {
            too_open_blockers.push(format!(
                "tiny_live_guardrails.max_consecutive_hard_failures={} exceeds planning-safe max {}",
                guardrails.max_consecutive_hard_failures, MAX_ALLOWED_CONSECUTIVE_HARD_FAILURES
            ));
        }
    }

    if !loaded_config.execution.enabled {
        warnings.push(
            "execution.enabled=false; tiny-live guardrail audit is planning-safe only".to_string(),
        );
    }
    warnings.push(
        "Stage 3 remains the hard gate; bounded guardrails do not authorize activation".to_string(),
    );
    if !policy_report.current_config_bounded_for_later_tiny_live_discussion {
        warnings.push(format!(
            "tiny-live bounded policy is not green yet: {}; guardrails do not replace the activation envelope",
            policy_report.reason
        ));
    }

    let monitoring_contract_complete = monitoring_blockers.is_empty();
    let rollback_contract_complete = rollback_blockers.is_empty();
    let verdict = if !monitoring_contract_complete && !rollback_contract_complete {
        TinyLiveGuardrailVerdict::TinyLiveGuardrailsIncomplete
    } else if !rollback_contract_complete {
        TinyLiveGuardrailVerdict::TinyLiveGuardrailsRollbackContractIncomplete
    } else if !monitoring_contract_complete {
        TinyLiveGuardrailVerdict::TinyLiveGuardrailsMonitoringContractIncomplete
    } else if !too_open_blockers.is_empty() {
        TinyLiveGuardrailVerdict::TinyLiveGuardrailsTooOpen
    } else {
        TinyLiveGuardrailVerdict::TinyLiveGuardrailsBounded
    };

    let mut blockers = Vec::new();
    blockers.extend(monitoring_blockers.clone());
    blockers.extend(rollback_blockers.clone());
    blockers.extend(too_open_blockers.clone());

    Ok(TinyLiveGuardrailAuditReport {
        generated_at: Utc::now(),
        config_path: config_path.display().to_string(),
        execution_enabled: loaded_config.execution.enabled,
        planning_safe_only: true,
        activation_permission_granted: false,
        stage3_gate_not_evaluated: true,
        current_execution_mode: mode,
        mode_compatible,
        tiny_live_policy_verdict: json_enum_string(&policy_report.verdict),
        tiny_live_policy_reason: policy_report.reason.clone(),
        tiny_live_policy_bounded: policy_report
            .current_config_bounded_for_later_tiny_live_discussion,
        tiny_live_guardrails_enabled: guardrails.enabled,
        monitoring_contract_complete,
        rollback_contract_complete,
        current_config_bounded_for_later_tiny_live_monitoring: verdict
            == TinyLiveGuardrailVerdict::TinyLiveGuardrailsBounded,
        verdict,
        reason: derive_reason(
            verdict,
            &monitoring_blockers,
            &rollback_blockers,
            &too_open_blockers,
        ),
        blockers,
        warnings,
        evaluation_window_seconds: guardrails.evaluation_window_seconds,
        max_execution_error_rate_pct: guardrails.max_execution_error_rate_pct,
        max_adapter_contract_failure_rate_pct: guardrails.max_adapter_contract_failure_rate_pct,
        max_policy_echo_mismatch_rate_pct: guardrails.max_policy_echo_mismatch_rate_pct,
        max_fee_or_slippage_breach_rate_pct: guardrails.max_fee_or_slippage_breach_rate_pct,
        max_connectivity_degraded_window_seconds: guardrails
            .max_connectivity_degraded_window_seconds,
        max_daily_realized_loss_sol: guardrails.max_daily_realized_loss_sol,
        max_consecutive_hard_failures: guardrails.max_consecutive_hard_failures,
        reference_envelope: GuardrailReferenceEnvelope {
            max_evaluation_window_seconds: MAX_ALLOWED_EVALUATION_WINDOW_SECONDS,
            max_execution_error_rate_pct: MAX_ALLOWED_EXECUTION_ERROR_RATE_PCT,
            max_adapter_contract_failure_rate_pct: MAX_ALLOWED_ADAPTER_CONTRACT_FAILURE_RATE_PCT,
            max_policy_echo_mismatch_rate_pct: MAX_ALLOWED_POLICY_ECHO_MISMATCH_RATE_PCT,
            max_fee_or_slippage_breach_rate_pct: MAX_ALLOWED_FEE_OR_SLIPPAGE_BREACH_RATE_PCT,
            max_connectivity_degraded_window_seconds:
                MAX_ALLOWED_CONNECTIVITY_DEGRADED_WINDOW_SECONDS,
            max_daily_realized_loss_sol: MAX_ALLOWED_DAILY_REALIZED_LOSS_SOL,
            max_consecutive_hard_failures: MAX_ALLOWED_CONSECUTIVE_HARD_FAILURES,
        },
        rollback_triggers: build_rollback_triggers(guardrails),
    })
}

fn is_positive_pct(value: f64) -> bool {
    value.is_finite() && value > 0.0 && value <= 100.0
}

fn build_rollback_triggers(
    guardrails: &copybot_config::TinyLiveGuardrailsConfig,
) -> Vec<RollbackTriggerSummary> {
    vec![
        RollbackTriggerSummary {
            trigger: "execution_error_rate".to_string(),
            threshold_kind: "rate_pct".to_string(),
            threshold_rate_pct: Some(guardrails.max_execution_error_rate_pct),
            threshold_seconds: None,
            threshold_sol: None,
            threshold_count: None,
            evaluation_window_seconds: Some(guardrails.evaluation_window_seconds),
            action: "mandatory_rollback".to_string(),
        },
        RollbackTriggerSummary {
            trigger: "adapter_contract_failure_rate".to_string(),
            threshold_kind: "rate_pct".to_string(),
            threshold_rate_pct: Some(guardrails.max_adapter_contract_failure_rate_pct),
            threshold_seconds: None,
            threshold_sol: None,
            threshold_count: None,
            evaluation_window_seconds: Some(guardrails.evaluation_window_seconds),
            action: "mandatory_rollback".to_string(),
        },
        RollbackTriggerSummary {
            trigger: "policy_echo_mismatch_rate".to_string(),
            threshold_kind: "rate_pct".to_string(),
            threshold_rate_pct: Some(guardrails.max_policy_echo_mismatch_rate_pct),
            threshold_seconds: None,
            threshold_sol: None,
            threshold_count: None,
            evaluation_window_seconds: Some(guardrails.evaluation_window_seconds),
            action: "mandatory_rollback".to_string(),
        },
        RollbackTriggerSummary {
            trigger: "fee_or_slippage_breach_rate".to_string(),
            threshold_kind: "rate_pct".to_string(),
            threshold_rate_pct: Some(guardrails.max_fee_or_slippage_breach_rate_pct),
            threshold_seconds: None,
            threshold_sol: None,
            threshold_count: None,
            evaluation_window_seconds: Some(guardrails.evaluation_window_seconds),
            action: "mandatory_rollback".to_string(),
        },
        RollbackTriggerSummary {
            trigger: "connectivity_degraded_window".to_string(),
            threshold_kind: "window_seconds".to_string(),
            threshold_rate_pct: None,
            threshold_seconds: Some(guardrails.max_connectivity_degraded_window_seconds),
            threshold_sol: None,
            threshold_count: None,
            evaluation_window_seconds: None,
            action: "mandatory_rollback".to_string(),
        },
        RollbackTriggerSummary {
            trigger: "daily_realized_loss".to_string(),
            threshold_kind: "absolute_sol".to_string(),
            threshold_rate_pct: None,
            threshold_seconds: None,
            threshold_sol: Some(guardrails.max_daily_realized_loss_sol),
            threshold_count: None,
            evaluation_window_seconds: Some(86_400),
            action: "mandatory_rollback".to_string(),
        },
        RollbackTriggerSummary {
            trigger: "consecutive_hard_failures".to_string(),
            threshold_kind: "count".to_string(),
            threshold_rate_pct: None,
            threshold_seconds: None,
            threshold_sol: None,
            threshold_count: Some(guardrails.max_consecutive_hard_failures),
            evaluation_window_seconds: None,
            action: "mandatory_rollback".to_string(),
        },
    ]
}

fn derive_reason(
    verdict: TinyLiveGuardrailVerdict,
    monitoring_blockers: &[String],
    rollback_blockers: &[String],
    too_open_blockers: &[String],
) -> String {
    match verdict {
        TinyLiveGuardrailVerdict::TinyLiveGuardrailsBounded => {
            "current config contains an explicit bounded tiny-live monitoring and rollback envelope; execution.enabled remains false".to_string()
        }
        TinyLiveGuardrailVerdict::TinyLiveGuardrailsIncomplete => monitoring_blockers
            .first()
            .cloned()
            .or_else(|| rollback_blockers.first().cloned())
            .unwrap_or_else(|| "tiny-live guardrail contract is incomplete".to_string()),
        TinyLiveGuardrailVerdict::TinyLiveGuardrailsTooOpen => too_open_blockers
            .first()
            .cloned()
            .unwrap_or_else(|| "tiny-live guardrail thresholds are too open".to_string()),
        TinyLiveGuardrailVerdict::TinyLiveGuardrailsRollbackContractIncomplete => rollback_blockers
            .first()
            .cloned()
            .unwrap_or_else(|| "tiny-live rollback trigger contract is incomplete".to_string()),
        TinyLiveGuardrailVerdict::TinyLiveGuardrailsMonitoringContractIncomplete => monitoring_blockers
            .first()
            .cloned()
            .unwrap_or_else(|| "tiny-live monitoring contract is incomplete".to_string()),
    }
}

fn normalize_mode(value: &str) -> String {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        "<empty>".to_string()
    } else {
        normalized
    }
}

fn json_enum_string<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

pub(crate) fn render_human(report: &TinyLiveGuardrailAuditReport) -> String {
    [
        "event=copybot_tiny_live_guardrail_audit".to_string(),
        format!("generated_at={}", report.generated_at.to_rfc3339()),
        format!("config_path={}", report.config_path),
        format!("execution_enabled={}", report.execution_enabled),
        format!("planning_safe_only={}", report.planning_safe_only),
        format!(
            "activation_permission_granted={}",
            report.activation_permission_granted
        ),
        format!(
            "stage3_gate_not_evaluated={}",
            report.stage3_gate_not_evaluated
        ),
        format!("current_execution_mode={}", report.current_execution_mode),
        format!("mode_compatible={}", report.mode_compatible),
        format!("verdict={}", json_enum_string(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "tiny_live_policy_verdict={}",
            report.tiny_live_policy_verdict
        ),
        format!(
            "tiny_live_policy_bounded={}",
            report.tiny_live_policy_bounded
        ),
        format!(
            "tiny_live_guardrails_enabled={}",
            report.tiny_live_guardrails_enabled
        ),
        format!(
            "monitoring_contract_complete={}",
            report.monitoring_contract_complete
        ),
        format!(
            "rollback_contract_complete={}",
            report.rollback_contract_complete
        ),
        format!(
            "current_config_bounded_for_later_tiny_live_monitoring={}",
            report.current_config_bounded_for_later_tiny_live_monitoring
        ),
        format!(
            "evaluation_window_seconds={}",
            report.evaluation_window_seconds
        ),
        format!(
            "max_execution_error_rate_pct={:.4}",
            report.max_execution_error_rate_pct
        ),
        format!(
            "max_adapter_contract_failure_rate_pct={:.4}",
            report.max_adapter_contract_failure_rate_pct
        ),
        format!(
            "max_policy_echo_mismatch_rate_pct={:.4}",
            report.max_policy_echo_mismatch_rate_pct
        ),
        format!(
            "max_fee_or_slippage_breach_rate_pct={:.4}",
            report.max_fee_or_slippage_breach_rate_pct
        ),
        format!(
            "max_connectivity_degraded_window_seconds={}",
            report.max_connectivity_degraded_window_seconds
        ),
        format!(
            "max_daily_realized_loss_sol={:.6}",
            report.max_daily_realized_loss_sol
        ),
        format!(
            "max_consecutive_hard_failures={}",
            report.max_consecutive_hard_failures
        ),
        format!("blockers={}", report.blockers.join(" | ")),
        format!("warnings={}", report.warnings.join(" | ")),
        format!(
            "rollback_triggers={}",
            report
                .rollback_triggers
                .iter()
                .map(|trigger| render_trigger_human(trigger))
                .collect::<Vec<_>>()
                .join(" ; ")
        ),
    ]
    .join("\n")
}

fn render_trigger_human(trigger: &RollbackTriggerSummary) -> String {
    format!(
        "{}(kind={},rate_pct={},window_seconds={},sol={},count={},evaluation_window_seconds={},action={})",
        trigger.trigger,
        trigger.threshold_kind,
        trigger
            .threshold_rate_pct
            .map(|value| format!("{value:.4}"))
            .unwrap_or_else(|| "null".to_string()),
        trigger
            .threshold_seconds
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_string()),
        trigger
            .threshold_sol
            .map(|value| format!("{value:.6}"))
            .unwrap_or_else(|| "null".to_string()),
        trigger
            .threshold_count
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_string()),
        trigger
            .evaluation_window_seconds
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_string()),
        trigger.action,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_config::AppConfig;

    #[test]
    fn bounded_guardrails_return_green() {
        let report =
            evaluate_tiny_live_guardrails(Path::new("/tmp/live.server.toml"), &bounded_config())
                .expect("guardrail audit");

        assert_eq!(
            report.verdict,
            TinyLiveGuardrailVerdict::TinyLiveGuardrailsBounded
        );
        assert!(report.current_config_bounded_for_later_tiny_live_monitoring);
        assert!(!report.execution_enabled);
    }

    #[test]
    fn missing_rollback_trigger_fields_return_incomplete() {
        let mut config = bounded_config();
        config.tiny_live_guardrails.max_consecutive_hard_failures = 0;

        let report = evaluate_tiny_live_guardrails(Path::new("/tmp/live.server.toml"), &config)
            .expect("guardrail audit");

        assert_eq!(
            report.verdict,
            TinyLiveGuardrailVerdict::TinyLiveGuardrailsRollbackContractIncomplete
        );
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("max_consecutive_hard_failures")));
    }

    #[test]
    fn unbounded_error_and_connectivity_thresholds_return_too_open() {
        let mut config = bounded_config();
        config.tiny_live_guardrails.max_execution_error_rate_pct = 10.0;
        config
            .tiny_live_guardrails
            .max_connectivity_degraded_window_seconds = 300;

        let report = evaluate_tiny_live_guardrails(Path::new("/tmp/live.server.toml"), &config)
            .expect("guardrail audit");

        assert_eq!(
            report.verdict,
            TinyLiveGuardrailVerdict::TinyLiveGuardrailsTooOpen
        );
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("max_execution_error_rate_pct")));
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("max_connectivity_degraded_window_seconds")));
    }

    #[test]
    fn stage3_truth_is_not_used_as_guardrail_substitute() {
        let report =
            evaluate_tiny_live_guardrails(Path::new("/tmp/live.server.toml"), &bounded_config())
                .expect("guardrail audit");

        assert!(report.stage3_gate_not_evaluated);
        assert_eq!(
            report.verdict,
            TinyLiveGuardrailVerdict::TinyLiveGuardrailsBounded
        );
    }

    #[test]
    fn execution_remains_disabled_and_no_live_mutation_occurs() {
        let config = bounded_config();
        let report = evaluate_tiny_live_guardrails(Path::new("/tmp/live.server.toml"), &config)
            .expect("guardrail audit");

        assert!(!config.execution.enabled);
        assert!(!report.execution_enabled);
        assert!(report.planning_safe_only);
        assert!(!report.activation_permission_granted);
    }

    #[test]
    fn missing_monitoring_window_returns_monitoring_incomplete() {
        let mut config = bounded_config();
        config.tiny_live_guardrails.evaluation_window_seconds = 0;

        let report = evaluate_tiny_live_guardrails(Path::new("/tmp/live.server.toml"), &config)
            .expect("guardrail audit");

        assert_eq!(
            report.verdict,
            TinyLiveGuardrailVerdict::TinyLiveGuardrailsMonitoringContractIncomplete
        );
    }

    fn bounded_config() -> AppConfig {
        let mut config = AppConfig::default();
        config.system.env = "prod-live".to_string();
        config.execution.enabled = false;
        config.execution.mode = TARGET_EXECUTION_MODE.to_string();
        config.execution.default_route = "jito".to_string();
        config.execution.submit_allowed_routes = vec!["jito".to_string()];
        config.execution.submit_route_order = vec!["jito".to_string()];
        config.execution.submit_route_max_slippage_bps =
            [("jito".to_string(), 40.0)].into_iter().collect();
        config.execution.submit_route_tip_lamports =
            [("jito".to_string(), 10_000)].into_iter().collect();
        config.execution.submit_route_compute_unit_limit =
            [("jito".to_string(), 300_000)].into_iter().collect();
        config
            .execution
            .submit_route_compute_unit_price_micro_lamports =
            [("jito".to_string(), 1_500)].into_iter().collect();
        config.execution.submit_adapter_require_policy_echo = true;
        config.execution.pretrade_max_fee_overhead_bps = 800;
        config.execution.pretrade_max_priority_fee_lamports = 1_500;
        config.shadow.copy_notional_sol = 0.05;
        config.risk.max_position_sol = 0.05;
        config.risk.max_concurrent_positions = 1;
        config.risk.daily_loss_limit_pct = 0.75;
        config.execution.batch_size = 1;
        config.tiny_live_policy.enabled = true;
        config.tiny_live_policy.max_trade_notional_sol = 0.05;
        config.tiny_live_policy.max_batch_size = 1;
        config.tiny_live_policy.max_concurrent_positions = 1;
        config.tiny_live_policy.max_daily_loss_limit_pct = 1.0;
        config.tiny_live_policy.allowed_routes = vec!["jito".to_string()];
        config.tiny_live_policy.require_policy_echo = true;
        config.tiny_live_policy.max_pretrade_fee_overhead_bps = 1_000;
        config.tiny_live_policy.max_pretrade_priority_fee_lamports = 2_000;
        config.tiny_live_policy.max_route_slippage_bps =
            [("jito".to_string(), 50.0)].into_iter().collect();
        config.tiny_live_policy.max_route_tip_lamports =
            [("jito".to_string(), 10_000)].into_iter().collect();
        config
            .tiny_live_policy
            .max_route_compute_unit_price_micro_lamports =
            [("jito".to_string(), 1_500)].into_iter().collect();
        config.tiny_live_guardrails.enabled = true;
        config.tiny_live_guardrails.evaluation_window_seconds = 900;
        config.tiny_live_guardrails.max_execution_error_rate_pct = 5.0;
        config
            .tiny_live_guardrails
            .max_adapter_contract_failure_rate_pct = 1.0;
        config
            .tiny_live_guardrails
            .max_policy_echo_mismatch_rate_pct = 1.0;
        config
            .tiny_live_guardrails
            .max_fee_or_slippage_breach_rate_pct = 5.0;
        config
            .tiny_live_guardrails
            .max_connectivity_degraded_window_seconds = 120;
        config.tiny_live_guardrails.max_daily_realized_loss_sol = 0.05;
        config.tiny_live_guardrails.max_consecutive_hard_failures = 3;
        config
    }
}
