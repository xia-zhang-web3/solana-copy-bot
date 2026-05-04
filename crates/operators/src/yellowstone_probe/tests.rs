use copybot_config::IngestionConfig;
use yellowstone_grpc_proto::prelude::CommitmentLevel;

use super::client::{
    error_reason_for_phase, timeout_reason_for_phase, ProbePhase, REASON_CONNECT_TIMEOUT,
    REASON_FIRST_MESSAGE_TIMEOUT, REASON_SUBSCRIBE_OPEN_FAILED, REASON_SUBSCRIBE_OPEN_TIMEOUT,
    REASON_SUBSCRIBE_SEND_TIMEOUT,
};
use super::config::{build_probe_config, parse_args_from, ProbeMode};
use super::report::{redact_error, redacted_endpoint_host};
use super::request::build_subscribe_request;
use super::{classify_config_error, REASON_CONFIG_MISSING_YELLOWSTONE};

fn yellowstone_ingestion_config() -> IngestionConfig {
    let mut config = IngestionConfig::default();
    config.source = "yellowstone_grpc".to_string();
    config.yellowstone_grpc_url =
        "https://user:secret@example.quicknode.invalid:10000/path?token=url-secret".to_string();
    config.yellowstone_x_token = "x-token-secret".to_string();
    config.yellowstone_connect_timeout_ms = 5_000;
    config.yellowstone_subscribe_timeout_ms = 15_000;
    config.yellowstone_program_ids = vec!["Program1111111111111111111111111111111111".to_string()];
    config
}

#[test]
fn reason_classifier_maps_subscribe_open_timeout() {
    assert_eq!(
        timeout_reason_for_phase(ProbePhase::SubscribeOpen),
        REASON_SUBSCRIBE_OPEN_TIMEOUT
    );
    assert_eq!(
        timeout_reason_for_phase(ProbePhase::Connect),
        REASON_CONNECT_TIMEOUT
    );
    assert_eq!(
        timeout_reason_for_phase(ProbePhase::SubscribeSend),
        REASON_SUBSCRIBE_SEND_TIMEOUT
    );
    assert_eq!(
        timeout_reason_for_phase(ProbePhase::FirstMessage),
        REASON_FIRST_MESSAGE_TIMEOUT
    );
}

#[test]
fn mode_parser_accepts_all_probe_modes() {
    assert_eq!(
        ProbeMode::parse("transaction-filter").unwrap(),
        ProbeMode::TransactionFilter
    );
    assert_eq!(
        ProbeMode::parse("slots-only").unwrap(),
        ProbeMode::SlotsOnly
    );
    assert_eq!(
        ProbeMode::parse("blocks-meta").unwrap(),
        ProbeMode::BlocksMeta
    );
    assert_eq!(
        ProbeMode::parse("empty-then-send").unwrap(),
        ProbeMode::EmptyThenSend
    );
    assert!(ProbeMode::parse("unknown").is_err());
}

#[test]
fn cli_defaults_to_transaction_filter_mode() {
    let cli = parse_args_from(["--config", "configs/prod.toml", "--json"])
        .expect("minimal cli should parse");
    assert_eq!(cli.mode, ProbeMode::TransactionFilter);

    let cli = parse_args_from([
        "--config",
        "configs/prod.toml",
        "--json",
        "--mode",
        "blocks-meta",
    ])
    .expect("mode cli should parse");
    assert_eq!(cli.mode, ProbeMode::BlocksMeta);
}

#[test]
fn subscribe_open_timeout_status_error_classifies_as_timeout() {
    let live_error = "gRPC status: code: 'The operation was cancelled', message: \"Timeout expired\", source: tonic::transport::Error(Transport, TimeoutExpired(()))";
    assert_eq!(
        error_reason_for_phase(ProbePhase::SubscribeOpen, live_error),
        REASON_SUBSCRIBE_OPEN_TIMEOUT
    );
}

#[test]
fn subscribe_open_non_timeout_error_remains_failed() {
    assert_eq!(
        error_reason_for_phase(
            ProbePhase::SubscribeOpen,
            "gRPC status: code: 'Unauthenticated', message: \"bad token\""
        ),
        REASON_SUBSCRIBE_OPEN_FAILED
    );
}

#[test]
fn redaction_removes_token_host_user_password_and_query() {
    let endpoint = "https://user:secret@example.quicknode.invalid:10000/path?token=url-secret";
    let error = "failed for https://user:secret@example.quicknode.invalid:10000/path?token=url-secret with x-token-secret";
    let redacted = redact_error(error, endpoint, "x-token-secret");
    assert!(!redacted.contains("x-token-secret"));
    assert!(!redacted.contains("example.quicknode.invalid"));
    assert!(!redacted.contains("url-secret"));
    assert!(!redacted.contains("user:secret"));
    assert!(redacted.contains("<redacted-host>"));
    assert!(redacted.contains("<redacted-token>"));
}

#[test]
fn endpoint_host_redaction_keeps_scheme_and_port_only() {
    let redacted = redacted_endpoint_host(
        "https://user:secret@example.quicknode.invalid:10000/path?token=url-secret",
    );
    assert_eq!(redacted, "https://<redacted-host>:10000");
}

#[test]
fn probe_config_uses_yellowstone_program_filters_and_timeout_overrides() {
    let config = yellowstone_ingestion_config();
    let probe = build_probe_config(&config, ProbeMode::TransactionFilter, Some(111), Some(222))
        .expect("yellowstone config should build probe config");
    assert_eq!(probe.connect_timeout_ms, 111);
    assert_eq!(probe.subscribe_timeout_ms, 222);
    assert_eq!(probe.program_ids, config.yellowstone_program_ids);
    assert_eq!(probe.mode, ProbeMode::TransactionFilter);
}

#[test]
fn probe_config_falls_back_to_subscribe_program_filters() {
    let mut config = yellowstone_ingestion_config();
    config.yellowstone_program_ids.clear();
    config.subscribe_program_ids = vec!["Fallback111111111111111111111111111111111".to_string()];
    let probe = build_probe_config(&config, ProbeMode::TransactionFilter, None, None)
        .expect("subscribe program fallback should be accepted");
    assert_eq!(probe.program_ids, config.subscribe_program_ids);
}

#[test]
fn probe_config_falls_back_to_runtime_dex_program_filters() {
    let mut config = yellowstone_ingestion_config();
    config.yellowstone_program_ids.clear();
    config.subscribe_program_ids.clear();
    let probe = build_probe_config(&config, ProbeMode::TransactionFilter, None, None)
        .expect("dex program fallback should match runtime source");
    assert!(probe
        .program_ids
        .contains(&"675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".to_string()));
    assert!(probe
        .program_ids
        .contains(&"pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA".to_string()));
}

#[test]
fn non_yellowstone_config_fails_closed() {
    let mut config = yellowstone_ingestion_config();
    config.source = "mock".to_string();
    let error = build_probe_config(&config, ProbeMode::TransactionFilter, None, None)
        .expect_err("non-yellowstone source must fail closed")
        .to_string();
    assert!(error.contains(REASON_CONFIG_MISSING_YELLOWSTONE));
}

#[test]
fn config_error_classifier_preserves_missing_yellowstone_reason() {
    assert_eq!(
        classify_config_error(&format!(
            "{REASON_CONFIG_MISSING_YELLOWSTONE}: token missing"
        )),
        REASON_CONFIG_MISSING_YELLOWSTONE
    );
    assert_eq!(
        classify_config_error("failed to parse TOML: config.toml"),
        "yellowstone_source_probe_config_malformed"
    );
}

#[test]
fn subscribe_request_matches_runtime_transaction_filter_shape() {
    let request = build_subscribe_request(
        ProbeMode::TransactionFilter,
        &["Program1111111111111111111111111111111111".to_string()],
    );
    assert_eq!(request.commitment, Some(CommitmentLevel::Confirmed as i32));
    let filter = request
        .transactions
        .get("copybot-swaps")
        .expect("copybot transaction filter should exist");
    assert_eq!(filter.vote, Some(false));
    assert_eq!(filter.failed, Some(false));
    assert_eq!(filter.account_include.len(), 1);
}

#[test]
fn subscribe_request_slots_only_shape_is_docs_minimal() {
    let request = build_subscribe_request(ProbeMode::SlotsOnly, &[]);
    assert_eq!(request.commitment, Some(CommitmentLevel::Confirmed as i32));
    assert!(request.transactions.is_empty());
    assert!(request.blocks_meta.is_empty());
    let filter = request
        .slots
        .get("copybot-slots-probe")
        .expect("slots filter should exist");
    assert_eq!(filter.filter_by_commitment, Some(true));
    assert_eq!(filter.interslot_updates, Some(false));
}

#[test]
fn subscribe_request_blocks_meta_shape_is_docs_minimal() {
    let request = build_subscribe_request(ProbeMode::BlocksMeta, &[]);
    assert_eq!(request.commitment, Some(CommitmentLevel::Confirmed as i32));
    assert!(request.transactions.is_empty());
    assert!(request.slots.is_empty());
    assert!(request
        .blocks_meta
        .contains_key("copybot-blocks-meta-probe"));
}

#[test]
fn subscribe_request_empty_then_send_shape_is_default_empty() {
    let request = build_subscribe_request(ProbeMode::EmptyThenSend, &[]);
    assert!(request.accounts.is_empty());
    assert!(request.slots.is_empty());
    assert!(request.transactions.is_empty());
    assert!(request.transactions_status.is_empty());
    assert!(request.blocks.is_empty());
    assert!(request.blocks_meta.is_empty());
    assert!(request.entry.is_empty());
    assert_eq!(request.commitment, None);
    assert!(request.accounts_data_slice.is_empty());
    assert!(request.ping.is_none());
    assert!(request.from_slot.is_none());
}
