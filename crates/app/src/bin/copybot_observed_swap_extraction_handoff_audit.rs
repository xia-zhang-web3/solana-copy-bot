use anyhow::{anyhow, bail, Context, Result};
use copybot_config::load_from_path;
use serde::Serialize;
use std::env;
use std::path::PathBuf;

const USAGE: &str =
    "usage: copybot_observed_swap_extraction_handoff_audit --config <path> [--json]";

const CONCLUSION_UPSTREAM_MESSAGES_DO_NOT_PRODUCE_SWAP_CANDIDATES: &str =
    "upstream_messages_do_not_produce_swap_candidates";
const CONCLUSION_SWAP_CANDIDATES_PRODUCED_BUT_FILTERED_BEFORE_PERSISTENCE: &str =
    "swap_candidates_produced_but_filtered_before_persistence";
const CONCLUSION_RELEVANT_SWAPS_REACH_HANDOFF_BUT_NOT_WRITER_ENQUEUE: &str =
    "relevant_swaps_reach_handoff_but_not_writer_enqueue";
const CONCLUSION_INSUFFICIENT_EVIDENCE: &str = "insufficient_evidence";

const INGESTION_PRIMARY_SOURCE_YELLOWSTONE_GRPC: &str = "yellowstone_grpc";
const INGESTION_PRIMARY_SOURCE_WS: &str = "ws";
const INGESTION_PRIMARY_SOURCE_MIXED: &str = "mixed";
const INGESTION_PRIMARY_SOURCE_UNKNOWN: &str = "unknown";

const NEXT_SWAP_SURFACE: &str = "copybot_ingestion::IngestionService::next_swap";
const SWAP_CANDIDATE_TYPE_SURFACE: &str = "copybot_ingestion::source::RawSwapObservation";
const RELEVANT_SWAP_HANDOFF_SURFACE: &str = "copybot_app::main::persist_relevant_observed_swap";
const IRRELEVANT_SWAP_HANDOFF_SURFACE: &str = "copybot_app::main::persist_irrelevant_observed_swap";
const WRITER_ENQUEUE_SURFACE: &str =
    "copybot_app::observed_swap_writer::ObservedSwapWriter::send_request";

const FETCH_SUCCESS_METRIC_NAME: &str = "fetch_success";
const FETCH_NO_SWAP_METRIC_NAME: &str = "fetch_no_swap";
const PARSE_REJECTED_METRIC_NAME: &str = "parse_rejected_total";
const PREFETCH_STALE_DROPPED_METRIC_NAME: &str = "prefetch_stale_dropped";
const WRITER_PENDING_REQUESTS_METRIC_NAME: &str = "pending_requests";

const APP_MAIN_SOURCE: &str = include_str!("../main.rs");
const OBSERVED_SWAP_WRITER_SOURCE: &str = include_str!("../observed_swap_writer.rs");
const INGESTION_LIB_SOURCE: &str = include_str!("../../../ingestion/src/lib.rs");
const INGESTION_PARSER_SOURCE: &str = include_str!("../../../ingestion/src/parser.rs");
const INGESTION_TELEMETRY_SOURCE: &str = include_str!("../../../ingestion/src/source/telemetry.rs");
const YELLOWSTONE_PIPELINE_SOURCE: &str =
    include_str!("../../../ingestion/src/source/yellowstone_pipeline.rs");
const YELLOWSTONE_PARSE_SOURCE: &str = include_str!("../../../ingestion/src/source/yellowstone.rs");
const HELIUS_PIPELINE_SOURCE: &str =
    include_str!("../../../ingestion/src/source/helius_pipeline.rs");

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
struct ObservedSwapExtractionHandoffAuditReport {
    config_path: String,
    ingestion_primary_source: String,
    next_swap_surface: String,
    swap_candidate_type_surface: String,
    swap_extraction_metric_surface_present: bool,
    fetch_success_metric_surface_present: bool,
    fetch_no_swap_metric_surface_present: bool,
    relevant_swap_handoff_surface: String,
    irrelevant_swap_handoff_surface: Option<String>,
    swap_relevance_filter_surface_present: bool,
    pre_persistence_drop_surface_present: bool,
    parse_rejected_metric_surface_present: bool,
    prefetch_stale_dropped_metric_surface_present: bool,
    writer_enqueue_surface: String,
    relevant_swaps_call_writer_enqueue: bool,
    irrelevant_swaps_bypass_batch_writer: bool,
    handoff_can_fail_before_writer_metric_surface: bool,
    code_implies_messages_can_arrive_without_swap_candidate: bool,
    code_implies_swap_candidate_can_be_filtered_before_persistence: bool,
    code_implies_relevant_swap_handoff_can_stop_before_writer_enqueue: bool,
    fetch_success_metric_name: String,
    fetch_no_swap_metric_name: String,
    parse_rejected_metric_name: String,
    prefetch_stale_dropped_metric_name: String,
    writer_pending_requests_metric_name: String,
    observed_swap_extraction_handoff_conclusion: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CodePathFacts {
    swap_extraction_metric_surface_present: bool,
    fetch_success_metric_surface_present: bool,
    fetch_no_swap_metric_surface_present: bool,
    swap_relevance_filter_surface_present: bool,
    pre_persistence_drop_surface_present: bool,
    parse_rejected_metric_surface_present: bool,
    prefetch_stale_dropped_metric_surface_present: bool,
    relevant_swaps_call_writer_enqueue: bool,
    irrelevant_swaps_bypass_batch_writer: bool,
    handoff_can_fail_before_writer_metric_surface: bool,
    code_implies_messages_can_arrive_without_swap_candidate: bool,
    code_implies_swap_candidate_can_be_filtered_before_persistence: bool,
    code_implies_relevant_swap_handoff_can_stop_before_writer_enqueue: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConclusionEvidence {
    upstream_messages_do_not_produce_swap_candidates: bool,
    swap_candidates_produced_but_filtered_before_persistence: bool,
    relevant_swaps_reach_handoff_but_not_writer_enqueue: bool,
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

fn run(config: &Config) -> Result<ObservedSwapExtractionHandoffAuditReport> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let ingestion_primary_source =
        normalize_ingestion_primary_source(&loaded_config.ingestion.source);
    let facts = current_code_path_facts(ingestion_primary_source);
    let conclusion = select_observed_swap_extraction_handoff_conclusion(ConclusionEvidence {
        upstream_messages_do_not_produce_swap_candidates: facts
            .code_implies_messages_can_arrive_without_swap_candidate
            && (facts.swap_extraction_metric_surface_present
                || facts.parse_rejected_metric_surface_present
                || facts.fetch_no_swap_metric_surface_present),
        swap_candidates_produced_but_filtered_before_persistence: facts
            .code_implies_swap_candidate_can_be_filtered_before_persistence
            && facts.swap_relevance_filter_surface_present
            && facts.pre_persistence_drop_surface_present,
        relevant_swaps_reach_handoff_but_not_writer_enqueue: facts
            .code_implies_relevant_swap_handoff_can_stop_before_writer_enqueue
            && facts.relevant_swaps_call_writer_enqueue
            && facts.handoff_can_fail_before_writer_metric_surface,
    })
    .to_string();

    Ok(ObservedSwapExtractionHandoffAuditReport {
        config_path: config.config_path.display().to_string(),
        ingestion_primary_source: ingestion_primary_source.to_string(),
        next_swap_surface: NEXT_SWAP_SURFACE.to_string(),
        swap_candidate_type_surface: SWAP_CANDIDATE_TYPE_SURFACE.to_string(),
        swap_extraction_metric_surface_present: facts.swap_extraction_metric_surface_present,
        fetch_success_metric_surface_present: facts.fetch_success_metric_surface_present,
        fetch_no_swap_metric_surface_present: facts.fetch_no_swap_metric_surface_present,
        relevant_swap_handoff_surface: RELEVANT_SWAP_HANDOFF_SURFACE.to_string(),
        irrelevant_swap_handoff_surface: Some(IRRELEVANT_SWAP_HANDOFF_SURFACE.to_string()),
        swap_relevance_filter_surface_present: facts.swap_relevance_filter_surface_present,
        pre_persistence_drop_surface_present: facts.pre_persistence_drop_surface_present,
        parse_rejected_metric_surface_present: facts.parse_rejected_metric_surface_present,
        prefetch_stale_dropped_metric_surface_present: facts
            .prefetch_stale_dropped_metric_surface_present,
        writer_enqueue_surface: WRITER_ENQUEUE_SURFACE.to_string(),
        relevant_swaps_call_writer_enqueue: facts.relevant_swaps_call_writer_enqueue,
        irrelevant_swaps_bypass_batch_writer: facts.irrelevant_swaps_bypass_batch_writer,
        handoff_can_fail_before_writer_metric_surface: facts
            .handoff_can_fail_before_writer_metric_surface,
        code_implies_messages_can_arrive_without_swap_candidate: facts
            .code_implies_messages_can_arrive_without_swap_candidate,
        code_implies_swap_candidate_can_be_filtered_before_persistence: facts
            .code_implies_swap_candidate_can_be_filtered_before_persistence,
        code_implies_relevant_swap_handoff_can_stop_before_writer_enqueue: facts
            .code_implies_relevant_swap_handoff_can_stop_before_writer_enqueue,
        fetch_success_metric_name: FETCH_SUCCESS_METRIC_NAME.to_string(),
        fetch_no_swap_metric_name: FETCH_NO_SWAP_METRIC_NAME.to_string(),
        parse_rejected_metric_name: PARSE_REJECTED_METRIC_NAME.to_string(),
        prefetch_stale_dropped_metric_name: PREFETCH_STALE_DROPPED_METRIC_NAME.to_string(),
        writer_pending_requests_metric_name: WRITER_PENDING_REQUESTS_METRIC_NAME.to_string(),
        observed_swap_extraction_handoff_conclusion: conclusion,
    })
}

fn normalize_ingestion_primary_source(source: &str) -> &'static str {
    match source.trim().to_ascii_lowercase().as_str() {
        "yellowstone" | "yellowstone_grpc" => INGESTION_PRIMARY_SOURCE_YELLOWSTONE_GRPC,
        "helius" | "helius_ws" | "ws" => INGESTION_PRIMARY_SOURCE_WS,
        value if value.contains(',') => INGESTION_PRIMARY_SOURCE_MIXED,
        _ => INGESTION_PRIMARY_SOURCE_UNKNOWN,
    }
}

fn current_code_path_facts(ingestion_primary_source: &str) -> CodePathFacts {
    let yellowstone_extraction_metric_surface_present = INGESTION_TELEMETRY_SOURCE
        .contains("pub(super) grpc_message_total: AtomicU64,")
        && YELLOWSTONE_PIPELINE_SOURCE.contains(".telemetry")
        && YELLOWSTONE_PIPELINE_SOURCE.contains(".grpc_message_total")
        && YELLOWSTONE_PIPELINE_SOURCE.contains(".grpc_transaction_updates_total");
    let ws_fetch_success_metric_surface_present = INGESTION_TELEMETRY_SOURCE
        .contains("pub(super) fetch_success: AtomicU64,")
        && HELIUS_PIPELINE_SOURCE.contains(".fetch_success");
    let ws_fetch_no_swap_metric_surface_present = INGESTION_TELEMETRY_SOURCE
        .contains("pub(super) fetch_no_swap: AtomicU64,")
        && HELIUS_PIPELINE_SOURCE.contains(".fetch_no_swap");
    let ws_prefetch_stale_dropped_metric_surface_present = INGESTION_TELEMETRY_SOURCE
        .contains("pub(super) prefetch_stale_dropped: AtomicU64,")
        && HELIUS_PIPELINE_SOURCE.contains(".prefetch_stale_dropped");

    let swap_extraction_metric_surface_present = matches!(
        ingestion_primary_source,
        INGESTION_PRIMARY_SOURCE_YELLOWSTONE_GRPC
    ) && yellowstone_extraction_metric_surface_present;
    let fetch_success_metric_surface_present =
        matches!(ingestion_primary_source, INGESTION_PRIMARY_SOURCE_WS)
            && ws_fetch_success_metric_surface_present;
    let fetch_no_swap_metric_surface_present =
        matches!(ingestion_primary_source, INGESTION_PRIMARY_SOURCE_WS)
            && ws_fetch_no_swap_metric_surface_present;

    let swap_relevance_filter_surface_present = APP_MAIN_SOURCE
        .contains("fn classify_observed_swap_shadow_relevance(")
        && APP_MAIN_SOURCE.contains("ObservedSwapShadowRelevance::IrrelevantUnclassified")
        && APP_MAIN_SOURCE.contains("ObservedSwapShadowRelevance::IrrelevantNotFollowed")
        && APP_MAIN_SOURCE.contains("ObservedSwapShadowRelevance::Relevant(side)");
    let pre_persistence_drop_surface_present =
        APP_MAIN_SOURCE.contains(
            "dropping non-critical irrelevant observed swap under Yellowstone output pressure",
        ) && APP_MAIN_SOURCE.contains(
            "dropping non-critical irrelevant observed swap under writer backpressure",
        ) && APP_MAIN_SOURCE.contains(
            "dropping non-critical irrelevant observed swap after the empty-target zero-universe best-effort writer budget was already exhausted",
        );
    let parse_rejected_metric_surface_present = matches!(
        ingestion_primary_source,
        INGESTION_PRIMARY_SOURCE_YELLOWSTONE_GRPC
    ) && INGESTION_TELEMETRY_SOURCE
        .contains("pub(super) parse_rejected_total: AtomicU64,")
        && YELLOWSTONE_PIPELINE_SOURCE
            .contains("runtime_config.telemetry.note_parse_rejected(&error);");
    let prefetch_stale_dropped_metric_surface_present =
        matches!(ingestion_primary_source, INGESTION_PRIMARY_SOURCE_WS)
            && ws_prefetch_stale_dropped_metric_surface_present;
    let relevant_swaps_call_writer_enqueue = APP_MAIN_SOURCE
        .contains("async fn persist_relevant_observed_swap(")
        && APP_MAIN_SOURCE.contains("match observed_swap_writer.write(swap).await {")
        && OBSERVED_SWAP_WRITER_SOURCE
            .contains("pub(crate) async fn write(&self, swap: &SwapEvent) -> Result<bool> {")
        && OBSERVED_SWAP_WRITER_SOURCE.contains("self.send_request(ObservedSwapWriteRequest {");
    let irrelevant_swaps_bypass_batch_writer = false;
    let handoff_can_fail_before_writer_metric_surface = false;
    let code_implies_messages_can_arrive_without_swap_candidate = INGESTION_LIB_SOURCE
        .contains("pub async fn next_swap(&mut self) -> Result<Option<SwapEvent>> {")
        && INGESTION_LIB_SOURCE.contains("if let Some(parsed) = self.parser.parse(raw) {")
        && INGESTION_PARSER_SOURCE
            .contains("pub fn parse(&self, raw: RawSwapObservation) -> Option<SwapEvent> {")
        && INGESTION_PARSER_SOURCE.contains("return None;")
        && YELLOWSTONE_PARSE_SOURCE.contains("return Ok(None);");
    let code_implies_swap_candidate_can_be_filtered_before_persistence =
        swap_relevance_filter_surface_present
            && pre_persistence_drop_surface_present
            && APP_MAIN_SOURCE.contains("if !note_recent_swap_signature(")
            && APP_MAIN_SOURCE.contains("debug!(signature = %swap.signature, \"duplicate swap ignored by recent signature dedupe\");");
    let code_implies_relevant_swap_handoff_can_stop_before_writer_enqueue = false;

    CodePathFacts {
        swap_extraction_metric_surface_present,
        fetch_success_metric_surface_present,
        fetch_no_swap_metric_surface_present,
        swap_relevance_filter_surface_present,
        pre_persistence_drop_surface_present,
        parse_rejected_metric_surface_present,
        prefetch_stale_dropped_metric_surface_present,
        relevant_swaps_call_writer_enqueue,
        irrelevant_swaps_bypass_batch_writer,
        handoff_can_fail_before_writer_metric_surface,
        code_implies_messages_can_arrive_without_swap_candidate,
        code_implies_swap_candidate_can_be_filtered_before_persistence,
        code_implies_relevant_swap_handoff_can_stop_before_writer_enqueue,
    }
}

fn select_observed_swap_extraction_handoff_conclusion(
    evidence: ConclusionEvidence,
) -> &'static str {
    let supported_count = [
        evidence.upstream_messages_do_not_produce_swap_candidates,
        evidence.swap_candidates_produced_but_filtered_before_persistence,
        evidence.relevant_swaps_reach_handoff_but_not_writer_enqueue,
    ]
    .into_iter()
    .filter(|supported| *supported)
    .count();

    if supported_count != 1 {
        return CONCLUSION_INSUFFICIENT_EVIDENCE;
    }
    if evidence.upstream_messages_do_not_produce_swap_candidates {
        return CONCLUSION_UPSTREAM_MESSAGES_DO_NOT_PRODUCE_SWAP_CANDIDATES;
    }
    if evidence.swap_candidates_produced_but_filtered_before_persistence {
        return CONCLUSION_SWAP_CANDIDATES_PRODUCED_BUT_FILTERED_BEFORE_PERSISTENCE;
    }
    if evidence.relevant_swaps_reach_handoff_but_not_writer_enqueue {
        return CONCLUSION_RELEVANT_SWAPS_REACH_HANDOFF_BUT_NOT_WRITER_ENQUEUE;
    }
    CONCLUSION_INSUFFICIENT_EVIDENCE
}

fn format_optional_string(value: Option<&String>) -> String {
    value
        .map(|raw| serde_json::to_string(raw).unwrap_or_else(|_| "\"<encode-error>\"".to_string()))
        .unwrap_or_else(|| "null".to_string())
}

fn render_output(report: &ObservedSwapExtractionHandoffAuditReport, json: bool) -> Result<String> {
    if json {
        return serde_json::to_string_pretty(report)
            .context("failed serializing observed swap extraction/handoff audit json");
    }

    Ok(format!(
        concat!(
            "event=copybot_observed_swap_extraction_handoff_audit\n",
            "config_path={config_path}\n",
            "ingestion_primary_source={ingestion_primary_source}\n",
            "next_swap_surface={next_swap_surface}\n",
            "swap_candidate_type_surface={swap_candidate_type_surface}\n",
            "swap_extraction_metric_surface_present={swap_extraction_metric_surface_present}\n",
            "fetch_success_metric_surface_present={fetch_success_metric_surface_present}\n",
            "fetch_no_swap_metric_surface_present={fetch_no_swap_metric_surface_present}\n",
            "relevant_swap_handoff_surface={relevant_swap_handoff_surface}\n",
            "irrelevant_swap_handoff_surface={irrelevant_swap_handoff_surface}\n",
            "swap_relevance_filter_surface_present={swap_relevance_filter_surface_present}\n",
            "pre_persistence_drop_surface_present={pre_persistence_drop_surface_present}\n",
            "parse_rejected_metric_surface_present={parse_rejected_metric_surface_present}\n",
            "prefetch_stale_dropped_metric_surface_present={prefetch_stale_dropped_metric_surface_present}\n",
            "writer_enqueue_surface={writer_enqueue_surface}\n",
            "relevant_swaps_call_writer_enqueue={relevant_swaps_call_writer_enqueue}\n",
            "irrelevant_swaps_bypass_batch_writer={irrelevant_swaps_bypass_batch_writer}\n",
            "handoff_can_fail_before_writer_metric_surface={handoff_can_fail_before_writer_metric_surface}\n",
            "code_implies_messages_can_arrive_without_swap_candidate={code_implies_messages_can_arrive_without_swap_candidate}\n",
            "code_implies_swap_candidate_can_be_filtered_before_persistence={code_implies_swap_candidate_can_be_filtered_before_persistence}\n",
            "code_implies_relevant_swap_handoff_can_stop_before_writer_enqueue={code_implies_relevant_swap_handoff_can_stop_before_writer_enqueue}\n",
            "fetch_success_metric_name={fetch_success_metric_name}\n",
            "fetch_no_swap_metric_name={fetch_no_swap_metric_name}\n",
            "parse_rejected_metric_name={parse_rejected_metric_name}\n",
            "prefetch_stale_dropped_metric_name={prefetch_stale_dropped_metric_name}\n",
            "writer_pending_requests_metric_name={writer_pending_requests_metric_name}\n",
            "observed_swap_extraction_handoff_conclusion={observed_swap_extraction_handoff_conclusion}"
        ),
        config_path = report.config_path,
        ingestion_primary_source = report.ingestion_primary_source,
        next_swap_surface = report.next_swap_surface,
        swap_candidate_type_surface = report.swap_candidate_type_surface,
        swap_extraction_metric_surface_present = report.swap_extraction_metric_surface_present,
        fetch_success_metric_surface_present = report.fetch_success_metric_surface_present,
        fetch_no_swap_metric_surface_present = report.fetch_no_swap_metric_surface_present,
        relevant_swap_handoff_surface = report.relevant_swap_handoff_surface,
        irrelevant_swap_handoff_surface =
            format_optional_string(report.irrelevant_swap_handoff_surface.as_ref()),
        swap_relevance_filter_surface_present = report.swap_relevance_filter_surface_present,
        pre_persistence_drop_surface_present = report.pre_persistence_drop_surface_present,
        parse_rejected_metric_surface_present = report.parse_rejected_metric_surface_present,
        prefetch_stale_dropped_metric_surface_present =
            report.prefetch_stale_dropped_metric_surface_present,
        writer_enqueue_surface = report.writer_enqueue_surface,
        relevant_swaps_call_writer_enqueue = report.relevant_swaps_call_writer_enqueue,
        irrelevant_swaps_bypass_batch_writer = report.irrelevant_swaps_bypass_batch_writer,
        handoff_can_fail_before_writer_metric_surface =
            report.handoff_can_fail_before_writer_metric_surface,
        code_implies_messages_can_arrive_without_swap_candidate =
            report.code_implies_messages_can_arrive_without_swap_candidate,
        code_implies_swap_candidate_can_be_filtered_before_persistence =
            report.code_implies_swap_candidate_can_be_filtered_before_persistence,
        code_implies_relevant_swap_handoff_can_stop_before_writer_enqueue =
            report.code_implies_relevant_swap_handoff_can_stop_before_writer_enqueue,
        fetch_success_metric_name = report.fetch_success_metric_name,
        fetch_no_swap_metric_name = report.fetch_no_swap_metric_name,
        parse_rejected_metric_name = report.parse_rejected_metric_name,
        prefetch_stale_dropped_metric_name = report.prefetch_stale_dropped_metric_name,
        writer_pending_requests_metric_name = report.writer_pending_requests_metric_name,
        observed_swap_extraction_handoff_conclusion =
            report.observed_swap_extraction_handoff_conclusion,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_args_reads_required_observed_swap_extraction_handoff_audit_flags() -> Result<()> {
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
            "ingestion_primary_source",
            "next_swap_surface",
            "swap_candidate_type_surface",
            "swap_extraction_metric_surface_present",
            "fetch_success_metric_surface_present",
            "fetch_no_swap_metric_surface_present",
            "relevant_swap_handoff_surface",
            "irrelevant_swap_handoff_surface",
            "swap_relevance_filter_surface_present",
            "pre_persistence_drop_surface_present",
            "parse_rejected_metric_surface_present",
            "prefetch_stale_dropped_metric_surface_present",
            "writer_enqueue_surface",
            "relevant_swaps_call_writer_enqueue",
            "irrelevant_swaps_bypass_batch_writer",
            "handoff_can_fail_before_writer_metric_surface",
            "code_implies_messages_can_arrive_without_swap_candidate",
            "code_implies_swap_candidate_can_be_filtered_before_persistence",
            "code_implies_relevant_swap_handoff_can_stop_before_writer_enqueue",
            "fetch_success_metric_name",
            "fetch_no_swap_metric_name",
            "parse_rejected_metric_name",
            "prefetch_stale_dropped_metric_name",
            "writer_pending_requests_metric_name",
            "observed_swap_extraction_handoff_conclusion",
        ] {
            assert!(parsed.get(field).is_some(), "missing json field {field}");
        }
        Ok(())
    }

    #[test]
    fn conclusion_selection_reports_upstream_messages_without_swap_candidates_when_that_is_the_only_supported_seam(
    ) {
        let conclusion = select_observed_swap_extraction_handoff_conclusion(ConclusionEvidence {
            upstream_messages_do_not_produce_swap_candidates: true,
            swap_candidates_produced_but_filtered_before_persistence: false,
            relevant_swaps_reach_handoff_but_not_writer_enqueue: false,
        });

        assert_eq!(
            conclusion,
            CONCLUSION_UPSTREAM_MESSAGES_DO_NOT_PRODUCE_SWAP_CANDIDATES
        );
    }

    #[test]
    fn conclusion_selection_reports_filtered_before_persistence_when_that_is_the_only_supported_seam(
    ) {
        let conclusion = select_observed_swap_extraction_handoff_conclusion(ConclusionEvidence {
            upstream_messages_do_not_produce_swap_candidates: false,
            swap_candidates_produced_but_filtered_before_persistence: true,
            relevant_swaps_reach_handoff_but_not_writer_enqueue: false,
        });

        assert_eq!(
            conclusion,
            CONCLUSION_SWAP_CANDIDATES_PRODUCED_BUT_FILTERED_BEFORE_PERSISTENCE
        );
    }

    #[test]
    fn conclusion_selection_reports_insufficient_evidence_when_multiple_seams_remain_supported() {
        let conclusion = select_observed_swap_extraction_handoff_conclusion(ConclusionEvidence {
            upstream_messages_do_not_produce_swap_candidates: true,
            swap_candidates_produced_but_filtered_before_persistence: true,
            relevant_swaps_reach_handoff_but_not_writer_enqueue: false,
        });

        assert_eq!(conclusion, CONCLUSION_INSUFFICIENT_EVIDENCE);
    }

    #[test]
    fn current_source_inspection_finds_expected_extraction_handoff_and_metric_strings() {
        let facts = current_code_path_facts(INGESTION_PRIMARY_SOURCE_YELLOWSTONE_GRPC);

        assert!(facts.swap_extraction_metric_surface_present);
        assert!(!facts.fetch_success_metric_surface_present);
        assert!(!facts.fetch_no_swap_metric_surface_present);
        assert!(facts.swap_relevance_filter_surface_present);
        assert!(facts.pre_persistence_drop_surface_present);
        assert!(facts.parse_rejected_metric_surface_present);
        assert!(!facts.prefetch_stale_dropped_metric_surface_present);
        assert!(facts.relevant_swaps_call_writer_enqueue);
        assert!(!facts.irrelevant_swaps_bypass_batch_writer);
        assert!(!facts.handoff_can_fail_before_writer_metric_surface);
        assert!(facts.code_implies_messages_can_arrive_without_swap_candidate);
        assert!(facts.code_implies_swap_candidate_can_be_filtered_before_persistence);
        assert!(!facts.code_implies_relevant_swap_handoff_can_stop_before_writer_enqueue);
        assert_eq!(
            NEXT_SWAP_SURFACE,
            "copybot_ingestion::IngestionService::next_swap"
        );
        assert_eq!(
            SWAP_CANDIDATE_TYPE_SURFACE,
            "copybot_ingestion::source::RawSwapObservation"
        );
        assert_eq!(
            RELEVANT_SWAP_HANDOFF_SURFACE,
            "copybot_app::main::persist_relevant_observed_swap"
        );
        assert_eq!(
            IRRELEVANT_SWAP_HANDOFF_SURFACE,
            "copybot_app::main::persist_irrelevant_observed_swap"
        );
        assert_eq!(
            WRITER_ENQUEUE_SURFACE,
            "copybot_app::observed_swap_writer::ObservedSwapWriter::send_request"
        );
        assert_eq!(FETCH_SUCCESS_METRIC_NAME, "fetch_success");
        assert_eq!(FETCH_NO_SWAP_METRIC_NAME, "fetch_no_swap");
        assert_eq!(PARSE_REJECTED_METRIC_NAME, "parse_rejected_total");
        assert_eq!(PREFETCH_STALE_DROPPED_METRIC_NAME, "prefetch_stale_dropped");
        assert_eq!(WRITER_PENDING_REQUESTS_METRIC_NAME, "pending_requests");
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
