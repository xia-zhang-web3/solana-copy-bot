use anyhow::{anyhow, bail, Context, Result};
use copybot_config::load_from_path;
use serde::Serialize;
use std::env;
use std::path::PathBuf;

const USAGE: &str = "usage: copybot_observed_swap_ingress_enqueue_audit --config <path> [--json]";

const CONCLUSION_UPSTREAM_FEED_NOT_PRODUCING_SWAPS: &str = "upstream_feed_not_producing_swaps";
const CONCLUSION_SWAPS_PRODUCED_BUT_NOT_ENQUEUED_TO_WRITER: &str =
    "swaps_produced_but_not_enqueued_to_writer";
const CONCLUSION_SWAPS_ENQUEUED_BUT_NOT_VISIBLE_IN_WRITER_TELEMETRY_SURFACE: &str =
    "swaps_enqueued_but_not_visible_in_writer_telemetry_surface";
const CONCLUSION_INSUFFICIENT_EVIDENCE: &str = "insufficient_evidence";

const OBSERVED_SWAP_INGRESS_ENTRYPOINT: &str = "copybot_app::main::run_app_loop";
const OBSERVED_SWAP_WRITER_ENQUEUE_SURFACE: &str =
    "copybot_app::observed_swap_writer::ObservedSwapWriter::send_request";
const OBSERVED_SWAP_WRITER_BATCH_WORKER_SURFACE: &str =
    "copybot_app::observed_swap_writer::observed_swap_writer_loop";
const OBSERVED_SWAP_WRITER_ENTRYPOINT: &str =
    "copybot_app::observed_swap_writer::ObservedSwapWriter::start_with_recent_raw_journal";
const OBSERVED_SWAP_ENQUEUE_FUNCTION: &str =
    "copybot_app::observed_swap_writer::ObservedSwapWriter::send_request";
const OBSERVED_SWAP_INGRESS_PRIMARY_CALLSITE: &str =
    "copybot_app::main::run_app_loop -> copybot_ingestion::IngestionService::next_swap -> copybot_app::main::persist_relevant_observed_swap -> copybot_app::observed_swap_writer::ObservedSwapWriter::write";
const OBSERVED_SWAP_INGRESS_SECONDARY_CALLSITE: &str =
    "copybot_app::main::run_app_loop -> copybot_ingestion::IngestionService::next_swap -> copybot_app::main::enqueue_irrelevant_observed_swap_immediately -> copybot_app::observed_swap_writer::ObservedSwapWriter::try_enqueue / try_enqueue_discovery_critical";
const WRITER_PENDING_REQUESTS_METRIC_NAME: &str = "pending_requests";
const WRITER_INSERT_METRIC_NAME: &str = "observed_swaps_insert_ms_p95";

const MAIN_SOURCE: &str = include_str!("../main.rs");
const OBSERVED_SWAP_WRITER_SOURCE: &str = include_str!("../observed_swap_writer.rs");

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let report = run(&config)?;
    println!("{}", render_output(&report, config.json)?);
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    config_path: PathBuf,
    json: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct ObservedSwapIngressEnqueueAuditReport {
    config_path: String,
    observed_swap_ingress_entrypoint: String,
    observed_swap_writer_enqueue_surface: String,
    observed_swap_writer_batch_worker_surface: String,
    ingress_constructs_writer_sender: bool,
    ingress_calls_writer_enqueue_surface: bool,
    enqueue_surface_is_bounded_channel: bool,
    enqueue_surface_has_drop_or_backpressure_branch: bool,
    ingress_path_source: String,
    enqueue_failure_surface_present: bool,
    enqueue_overflow_or_drop_surface_present: bool,
    writer_telemetry_pending_requests_surface_present: bool,
    writer_telemetry_batch_insert_surface_present: bool,
    writer_telemetry_journal_surface_present: bool,
    code_implies_writer_can_be_idle_if_no_enqueue_happens: bool,
    code_implies_enqueue_can_fail_without_becoming_writer_terminal_failure: bool,
    code_implies_upstream_replay_or_feed_can_stop_before_writer_batch_insert: bool,
    observed_swap_writer_entrypoint: String,
    observed_swap_enqueue_function: String,
    observed_swap_ingress_primary_callsite: String,
    observed_swap_ingress_secondary_callsite: Option<String>,
    writer_pending_requests_metric_name: String,
    writer_insert_metric_name: String,
    observed_swap_ingress_enqueue_conclusion: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CodePathFacts {
    ingress_constructs_writer_sender: bool,
    ingress_calls_writer_enqueue_surface: bool,
    enqueue_surface_is_bounded_channel: bool,
    enqueue_surface_has_drop_or_backpressure_branch: bool,
    enqueue_failure_surface_present: bool,
    enqueue_overflow_or_drop_surface_present: bool,
    writer_telemetry_pending_requests_surface_present: bool,
    writer_telemetry_batch_insert_surface_present: bool,
    writer_telemetry_journal_surface_present: bool,
    code_implies_writer_can_be_idle_if_no_enqueue_happens: bool,
    code_implies_enqueue_can_fail_without_becoming_writer_terminal_failure: bool,
    code_implies_upstream_replay_or_feed_can_stop_before_writer_batch_insert: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConclusionEvidence {
    upstream_branch_supported: bool,
    produced_but_not_enqueued_branch_supported: bool,
    enqueued_but_not_visible_branch_supported: bool,
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

fn run(config: &Config) -> Result<ObservedSwapIngressEnqueueAuditReport> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let code_facts = current_code_path_facts();
    let conclusion = select_observed_swap_ingress_enqueue_conclusion(ConclusionEvidence {
        upstream_branch_supported: code_facts
            .code_implies_upstream_replay_or_feed_can_stop_before_writer_batch_insert
            && code_facts.code_implies_writer_can_be_idle_if_no_enqueue_happens,
        produced_but_not_enqueued_branch_supported: code_facts
            .code_implies_enqueue_can_fail_without_becoming_writer_terminal_failure
            && (code_facts.enqueue_failure_surface_present
                || code_facts.enqueue_overflow_or_drop_surface_present),
        enqueued_but_not_visible_branch_supported: code_facts.ingress_calls_writer_enqueue_surface
            && !code_facts.writer_telemetry_pending_requests_surface_present
            && !code_facts.writer_telemetry_batch_insert_surface_present,
    })
    .to_string();

    Ok(ObservedSwapIngressEnqueueAuditReport {
        config_path: config.config_path.display().to_string(),
        observed_swap_ingress_entrypoint: OBSERVED_SWAP_INGRESS_ENTRYPOINT.to_string(),
        observed_swap_writer_enqueue_surface: OBSERVED_SWAP_WRITER_ENQUEUE_SURFACE.to_string(),
        observed_swap_writer_batch_worker_surface: OBSERVED_SWAP_WRITER_BATCH_WORKER_SURFACE
            .to_string(),
        ingress_constructs_writer_sender: code_facts.ingress_constructs_writer_sender,
        ingress_calls_writer_enqueue_surface: code_facts.ingress_calls_writer_enqueue_surface,
        enqueue_surface_is_bounded_channel: code_facts.enqueue_surface_is_bounded_channel,
        enqueue_surface_has_drop_or_backpressure_branch: code_facts
            .enqueue_surface_has_drop_or_backpressure_branch,
        ingress_path_source: loaded_config.ingestion.source.clone(),
        enqueue_failure_surface_present: code_facts.enqueue_failure_surface_present,
        enqueue_overflow_or_drop_surface_present: code_facts
            .enqueue_overflow_or_drop_surface_present,
        writer_telemetry_pending_requests_surface_present: code_facts
            .writer_telemetry_pending_requests_surface_present,
        writer_telemetry_batch_insert_surface_present: code_facts
            .writer_telemetry_batch_insert_surface_present,
        writer_telemetry_journal_surface_present: code_facts
            .writer_telemetry_journal_surface_present,
        code_implies_writer_can_be_idle_if_no_enqueue_happens: code_facts
            .code_implies_writer_can_be_idle_if_no_enqueue_happens,
        code_implies_enqueue_can_fail_without_becoming_writer_terminal_failure: code_facts
            .code_implies_enqueue_can_fail_without_becoming_writer_terminal_failure,
        code_implies_upstream_replay_or_feed_can_stop_before_writer_batch_insert: code_facts
            .code_implies_upstream_replay_or_feed_can_stop_before_writer_batch_insert,
        observed_swap_writer_entrypoint: OBSERVED_SWAP_WRITER_ENTRYPOINT.to_string(),
        observed_swap_enqueue_function: OBSERVED_SWAP_ENQUEUE_FUNCTION.to_string(),
        observed_swap_ingress_primary_callsite: OBSERVED_SWAP_INGRESS_PRIMARY_CALLSITE.to_string(),
        observed_swap_ingress_secondary_callsite: Some(
            OBSERVED_SWAP_INGRESS_SECONDARY_CALLSITE.to_string(),
        ),
        writer_pending_requests_metric_name: WRITER_PENDING_REQUESTS_METRIC_NAME.to_string(),
        writer_insert_metric_name: WRITER_INSERT_METRIC_NAME.to_string(),
        observed_swap_ingress_enqueue_conclusion: conclusion,
    })
}

fn current_code_path_facts() -> CodePathFacts {
    let ingress_constructs_writer_sender = MAIN_SOURCE
        .contains("let observed_swap_writer = ObservedSwapWriter::start_with_recent_raw_journal(")
        && OBSERVED_SWAP_WRITER_SOURCE.contains("let (sender, receiver) = mpsc::channel(");
    let ingress_calls_writer_enqueue_surface = MAIN_SOURCE
        .contains("match observed_swap_writer.write(swap).await {")
        && MAIN_SOURCE.contains("observed_swap_writer.try_enqueue_discovery_critical(swap)")
        && MAIN_SOURCE.contains("observed_swap_writer.try_enqueue(swap)");
    let enqueue_surface_is_bounded_channel = OBSERVED_SWAP_WRITER_SOURCE
        .contains("let (sender, receiver) = mpsc::channel(config.channel_capacity);")
        && OBSERVED_SWAP_WRITER_SOURCE.contains(".sender")
        && OBSERVED_SWAP_WRITER_SOURCE.contains(".reserve()")
        && OBSERVED_SWAP_WRITER_SOURCE.contains(".try_reserve()");
    let enqueue_surface_has_drop_or_backpressure_branch = OBSERVED_SWAP_WRITER_SOURCE
        .contains("Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => Ok(false),")
        && MAIN_SOURCE.contains("PendingWriterBackpressure")
        && MAIN_SOURCE
            .contains("dropping non-critical irrelevant observed swap under writer backpressure");
    let enqueue_failure_surface_present = OBSERVED_SWAP_WRITER_SOURCE
        .contains(".context(OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT)?;")
        && MAIN_SOURCE.contains("failed enqueueing observed swap");
    let enqueue_overflow_or_drop_surface_present = OBSERVED_SWAP_WRITER_SOURCE
        .contains("Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => Ok(false),")
        && (MAIN_SOURCE.contains(
            "dropping non-critical irrelevant observed swap under Yellowstone output pressure",
        ) || MAIN_SOURCE
            .contains("dropping non-critical irrelevant observed swap under writer backpressure"));
    let writer_telemetry_pending_requests_surface_present = OBSERVED_SWAP_WRITER_SOURCE
        .contains("pub pending_requests: usize,")
        && OBSERVED_SWAP_WRITER_SOURCE.contains("fn note_enqueued(&self) {")
        && OBSERVED_SWAP_WRITER_SOURCE
            .contains("self.pending_requests.fetch_add(1, Ordering::Relaxed);");
    let writer_telemetry_batch_insert_surface_present = OBSERVED_SWAP_WRITER_SOURCE
        .contains("pub observed_swaps_insert_ms_p95: u64,")
        && OBSERVED_SWAP_WRITER_SOURCE
            .contains("fn note_observed_swaps_insert_completed(&self, duration_ms: u64)")
        && OBSERVED_SWAP_WRITER_SOURCE.contains("telemetry.note_observed_swaps_insert_completed(");
    let writer_telemetry_journal_surface_present = OBSERVED_SWAP_WRITER_SOURCE
        .contains("pub journal_queue_depth_batches: usize,")
        && OBSERVED_SWAP_WRITER_SOURCE.contains("fn note_journal_queue_enqueued(&self) {");
    let code_implies_writer_can_be_idle_if_no_enqueue_happens = MAIN_SOURCE
        .contains("Ok(None) => {")
        && MAIN_SOURCE.contains("debug!(\"ingestion emitted no swap\");")
        && OBSERVED_SWAP_WRITER_SOURCE.contains("match receiver.blocking_recv() {")
        && OBSERVED_SWAP_WRITER_SOURCE.contains("None => break,");
    let code_implies_enqueue_can_fail_without_becoming_writer_terminal_failure = OBSERVED_SWAP_WRITER_SOURCE
        .contains("Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => Ok(false),")
        && MAIN_SOURCE.contains("Ok(IrrelevantObservedSwapEnqueueOutcome::PendingWriterBackpressure) => {")
        && MAIN_SOURCE.contains("warn!(\n                                    error = %error,\n                                    error_chain = %error_chain,\n                                    signature = %swap.signature,\n                                    \"failed enqueueing observed swap\"");
    let code_implies_upstream_replay_or_feed_can_stop_before_writer_batch_insert = MAIN_SOURCE
        .contains("maybe_swap = ingestion.next_swap(), if ingestion_backoff_until.is_none() => {")
        && MAIN_SOURCE.contains("Ok(None) => {")
        && MAIN_SOURCE.contains("Err(error) => {")
        && (MAIN_SOURCE.contains(
            "dropping non-critical irrelevant observed swap under Yellowstone output pressure",
        ) || MAIN_SOURCE
            .contains("dropping non-critical irrelevant observed swap under writer backpressure"));

    CodePathFacts {
        ingress_constructs_writer_sender,
        ingress_calls_writer_enqueue_surface,
        enqueue_surface_is_bounded_channel,
        enqueue_surface_has_drop_or_backpressure_branch,
        enqueue_failure_surface_present,
        enqueue_overflow_or_drop_surface_present,
        writer_telemetry_pending_requests_surface_present,
        writer_telemetry_batch_insert_surface_present,
        writer_telemetry_journal_surface_present,
        code_implies_writer_can_be_idle_if_no_enqueue_happens,
        code_implies_enqueue_can_fail_without_becoming_writer_terminal_failure,
        code_implies_upstream_replay_or_feed_can_stop_before_writer_batch_insert,
    }
}

fn select_observed_swap_ingress_enqueue_conclusion(evidence: ConclusionEvidence) -> &'static str {
    let supported_count = [
        evidence.upstream_branch_supported,
        evidence.produced_but_not_enqueued_branch_supported,
        evidence.enqueued_but_not_visible_branch_supported,
    ]
    .into_iter()
    .filter(|supported| *supported)
    .count();

    if supported_count != 1 {
        return CONCLUSION_INSUFFICIENT_EVIDENCE;
    }
    if evidence.upstream_branch_supported {
        return CONCLUSION_UPSTREAM_FEED_NOT_PRODUCING_SWAPS;
    }
    if evidence.produced_but_not_enqueued_branch_supported {
        return CONCLUSION_SWAPS_PRODUCED_BUT_NOT_ENQUEUED_TO_WRITER;
    }
    if evidence.enqueued_but_not_visible_branch_supported {
        return CONCLUSION_SWAPS_ENQUEUED_BUT_NOT_VISIBLE_IN_WRITER_TELEMETRY_SURFACE;
    }
    CONCLUSION_INSUFFICIENT_EVIDENCE
}

fn format_optional_string(value: Option<&String>) -> String {
    value
        .map(|raw| serde_json::to_string(raw).unwrap_or_else(|_| "\"<encode-error>\"".to_string()))
        .unwrap_or_else(|| "null".to_string())
}

fn render_output(report: &ObservedSwapIngressEnqueueAuditReport, json: bool) -> Result<String> {
    if json {
        return serde_json::to_string_pretty(report)
            .context("failed serializing observed swap ingress/enqueue audit json");
    }

    Ok(format!(
        concat!(
            "event=copybot_observed_swap_ingress_enqueue_audit\n",
            "config_path={config_path}\n",
            "observed_swap_ingress_entrypoint={observed_swap_ingress_entrypoint}\n",
            "observed_swap_writer_enqueue_surface={observed_swap_writer_enqueue_surface}\n",
            "observed_swap_writer_batch_worker_surface={observed_swap_writer_batch_worker_surface}\n",
            "ingress_constructs_writer_sender={ingress_constructs_writer_sender}\n",
            "ingress_calls_writer_enqueue_surface={ingress_calls_writer_enqueue_surface}\n",
            "enqueue_surface_is_bounded_channel={enqueue_surface_is_bounded_channel}\n",
            "enqueue_surface_has_drop_or_backpressure_branch={enqueue_surface_has_drop_or_backpressure_branch}\n",
            "ingress_path_source={ingress_path_source}\n",
            "enqueue_failure_surface_present={enqueue_failure_surface_present}\n",
            "enqueue_overflow_or_drop_surface_present={enqueue_overflow_or_drop_surface_present}\n",
            "writer_telemetry_pending_requests_surface_present={writer_telemetry_pending_requests_surface_present}\n",
            "writer_telemetry_batch_insert_surface_present={writer_telemetry_batch_insert_surface_present}\n",
            "writer_telemetry_journal_surface_present={writer_telemetry_journal_surface_present}\n",
            "code_implies_writer_can_be_idle_if_no_enqueue_happens={code_implies_writer_can_be_idle_if_no_enqueue_happens}\n",
            "code_implies_enqueue_can_fail_without_becoming_writer_terminal_failure={code_implies_enqueue_can_fail_without_becoming_writer_terminal_failure}\n",
            "code_implies_upstream_replay_or_feed_can_stop_before_writer_batch_insert={code_implies_upstream_replay_or_feed_can_stop_before_writer_batch_insert}\n",
            "observed_swap_writer_entrypoint={observed_swap_writer_entrypoint}\n",
            "observed_swap_enqueue_function={observed_swap_enqueue_function}\n",
            "observed_swap_ingress_primary_callsite={observed_swap_ingress_primary_callsite}\n",
            "observed_swap_ingress_secondary_callsite={observed_swap_ingress_secondary_callsite}\n",
            "writer_pending_requests_metric_name={writer_pending_requests_metric_name}\n",
            "writer_insert_metric_name={writer_insert_metric_name}\n",
            "observed_swap_ingress_enqueue_conclusion={observed_swap_ingress_enqueue_conclusion}"
        ),
        config_path = report.config_path,
        observed_swap_ingress_entrypoint = report.observed_swap_ingress_entrypoint,
        observed_swap_writer_enqueue_surface = report.observed_swap_writer_enqueue_surface,
        observed_swap_writer_batch_worker_surface = report.observed_swap_writer_batch_worker_surface,
        ingress_constructs_writer_sender = report.ingress_constructs_writer_sender,
        ingress_calls_writer_enqueue_surface = report.ingress_calls_writer_enqueue_surface,
        enqueue_surface_is_bounded_channel = report.enqueue_surface_is_bounded_channel,
        enqueue_surface_has_drop_or_backpressure_branch =
            report.enqueue_surface_has_drop_or_backpressure_branch,
        ingress_path_source = report.ingress_path_source,
        enqueue_failure_surface_present = report.enqueue_failure_surface_present,
        enqueue_overflow_or_drop_surface_present = report.enqueue_overflow_or_drop_surface_present,
        writer_telemetry_pending_requests_surface_present =
            report.writer_telemetry_pending_requests_surface_present,
        writer_telemetry_batch_insert_surface_present =
            report.writer_telemetry_batch_insert_surface_present,
        writer_telemetry_journal_surface_present = report.writer_telemetry_journal_surface_present,
        code_implies_writer_can_be_idle_if_no_enqueue_happens =
            report.code_implies_writer_can_be_idle_if_no_enqueue_happens,
        code_implies_enqueue_can_fail_without_becoming_writer_terminal_failure =
            report.code_implies_enqueue_can_fail_without_becoming_writer_terminal_failure,
        code_implies_upstream_replay_or_feed_can_stop_before_writer_batch_insert =
            report.code_implies_upstream_replay_or_feed_can_stop_before_writer_batch_insert,
        observed_swap_writer_entrypoint = report.observed_swap_writer_entrypoint,
        observed_swap_enqueue_function = report.observed_swap_enqueue_function,
        observed_swap_ingress_primary_callsite = report.observed_swap_ingress_primary_callsite,
        observed_swap_ingress_secondary_callsite =
            format_optional_string(report.observed_swap_ingress_secondary_callsite.as_ref()),
        writer_pending_requests_metric_name = report.writer_pending_requests_metric_name,
        writer_insert_metric_name = report.writer_insert_metric_name,
        observed_swap_ingress_enqueue_conclusion = report.observed_swap_ingress_enqueue_conclusion,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_args_reads_required_observed_swap_ingress_enqueue_audit_flags() -> Result<()> {
        let parsed = parse_args_from([
            "--config".to_string(),
            "/tmp/example.toml".to_string(),
            "--json".to_string(),
        ])?
        .expect("config should parse");

        assert_eq!(parsed.config_path, PathBuf::from("/tmp/example.toml"));
        assert!(parsed.json);
        Ok(())
    }

    #[test]
    fn json_output_contains_all_required_fields() -> Result<()> {
        let report = run(&Config {
            config_path: repo_dev_config_path(),
            json: true,
        })?;
        let json = render_output(&report, true)?;
        let parsed: serde_json::Value = serde_json::from_str(&json)?;

        for field in [
            "config_path",
            "observed_swap_ingress_entrypoint",
            "observed_swap_writer_enqueue_surface",
            "observed_swap_writer_batch_worker_surface",
            "ingress_constructs_writer_sender",
            "ingress_calls_writer_enqueue_surface",
            "enqueue_surface_is_bounded_channel",
            "enqueue_surface_has_drop_or_backpressure_branch",
            "ingress_path_source",
            "enqueue_failure_surface_present",
            "enqueue_overflow_or_drop_surface_present",
            "writer_telemetry_pending_requests_surface_present",
            "writer_telemetry_batch_insert_surface_present",
            "writer_telemetry_journal_surface_present",
            "code_implies_writer_can_be_idle_if_no_enqueue_happens",
            "code_implies_enqueue_can_fail_without_becoming_writer_terminal_failure",
            "code_implies_upstream_replay_or_feed_can_stop_before_writer_batch_insert",
            "observed_swap_writer_entrypoint",
            "observed_swap_enqueue_function",
            "observed_swap_ingress_primary_callsite",
            "observed_swap_ingress_secondary_callsite",
            "writer_pending_requests_metric_name",
            "writer_insert_metric_name",
            "observed_swap_ingress_enqueue_conclusion",
        ] {
            assert!(parsed.get(field).is_some(), "missing json field {field}");
        }
        Ok(())
    }

    #[test]
    fn conclusion_selection_reports_insufficient_evidence_for_ambiguous_current_seams() {
        let conclusion = select_observed_swap_ingress_enqueue_conclusion(ConclusionEvidence {
            upstream_branch_supported: true,
            produced_but_not_enqueued_branch_supported: true,
            enqueued_but_not_visible_branch_supported: false,
        });

        assert_eq!(conclusion, CONCLUSION_INSUFFICIENT_EVIDENCE);
    }

    #[test]
    fn conclusion_selection_reports_produced_but_not_enqueued_when_that_is_the_only_narrow_seam() {
        let conclusion = select_observed_swap_ingress_enqueue_conclusion(ConclusionEvidence {
            upstream_branch_supported: false,
            produced_but_not_enqueued_branch_supported: true,
            enqueued_but_not_visible_branch_supported: false,
        });

        assert_eq!(
            conclusion,
            CONCLUSION_SWAPS_PRODUCED_BUT_NOT_ENQUEUED_TO_WRITER
        );
    }

    #[test]
    fn conclusion_selection_reports_upstream_not_producing_when_that_is_the_only_narrow_seam() {
        let conclusion = select_observed_swap_ingress_enqueue_conclusion(ConclusionEvidence {
            upstream_branch_supported: true,
            produced_but_not_enqueued_branch_supported: false,
            enqueued_but_not_visible_branch_supported: false,
        });

        assert_eq!(conclusion, CONCLUSION_UPSTREAM_FEED_NOT_PRODUCING_SWAPS);
    }

    #[test]
    fn current_source_rendering_finds_expected_ingress_enqueue_writer_strings() {
        let facts = current_code_path_facts();

        assert!(facts.ingress_constructs_writer_sender);
        assert!(facts.ingress_calls_writer_enqueue_surface);
        assert!(facts.enqueue_surface_is_bounded_channel);
        assert!(facts.enqueue_surface_has_drop_or_backpressure_branch);
        assert!(facts.enqueue_failure_surface_present);
        assert!(facts.enqueue_overflow_or_drop_surface_present);
        assert!(facts.writer_telemetry_pending_requests_surface_present);
        assert!(facts.writer_telemetry_batch_insert_surface_present);
        assert!(facts.writer_telemetry_journal_surface_present);
        assert!(facts.code_implies_writer_can_be_idle_if_no_enqueue_happens);
        assert!(facts.code_implies_enqueue_can_fail_without_becoming_writer_terminal_failure);
        assert!(facts.code_implies_upstream_replay_or_feed_can_stop_before_writer_batch_insert);
        assert_eq!(
            OBSERVED_SWAP_INGRESS_ENTRYPOINT,
            "copybot_app::main::run_app_loop"
        );
        assert_eq!(
            OBSERVED_SWAP_WRITER_ENQUEUE_SURFACE,
            "copybot_app::observed_swap_writer::ObservedSwapWriter::send_request"
        );
        assert_eq!(
            OBSERVED_SWAP_WRITER_BATCH_WORKER_SURFACE,
            "copybot_app::observed_swap_writer::observed_swap_writer_loop"
        );
        assert_eq!(WRITER_PENDING_REQUESTS_METRIC_NAME, "pending_requests");
        assert_eq!(WRITER_INSERT_METRIC_NAME, "observed_swaps_insert_ms_p95");
    }

    #[test]
    fn repeated_runs_with_fixed_inputs_produce_identical_json() -> Result<()> {
        let config = Config {
            config_path: repo_dev_config_path(),
            json: true,
        };
        let first = render_output(&run(&config)?, true)?;
        let second = render_output(&run(&config)?, true)?;

        assert_eq!(first, second);
        Ok(())
    }

    fn repo_dev_config_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../configs/dev.toml")
    }
}
