use anyhow::{anyhow, Context, Result};
use copybot_config::{load_from_path, IngestionConfig};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::time;
use tonic::transport::ClientTlsConfig;
use url::Url;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterBlocksMeta,
    SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
};

const REASON_OK: &str = "yellowstone_source_probe_subscribe_opened";
const REASON_CONFIG_UNREADABLE: &str = "yellowstone_source_probe_config_unreadable";
const REASON_CONFIG_MALFORMED: &str = "yellowstone_source_probe_config_malformed";
const REASON_CONFIG_MISSING_YELLOWSTONE: &str =
    "yellowstone_source_probe_config_missing_yellowstone";
const REASON_CONNECT_TIMEOUT: &str = "yellowstone_connect_timeout";
const REASON_CONNECT_FAILED: &str = "yellowstone_connect_failed";
const REASON_SUBSCRIBE_OPEN_TIMEOUT: &str = "yellowstone_subscription_open_timeout";
const REASON_SUBSCRIBE_OPEN_FAILED: &str = "yellowstone_subscription_open_failed";
const REASON_SUBSCRIBE_SEND_TIMEOUT: &str = "yellowstone_subscribe_request_send_timeout";
const REASON_SUBSCRIBE_SEND_FAILED: &str = "yellowstone_subscribe_request_send_failed";
const REASON_FIRST_MESSAGE_TIMEOUT: &str = "yellowstone_first_message_timeout";
const REASON_FIRST_MESSAGE_RECEIVED: &str = "yellowstone_first_message_received";
const REASON_STREAM_CLOSED: &str = "yellowstone_stream_closed_before_first_message";
const REASON_STREAM_ERROR: &str = "yellowstone_stream_error_before_first_message";

#[derive(Debug, Clone)]
struct Cli {
    config_path: PathBuf,
    json: bool,
    mode: ProbeMode,
    connect_timeout_ms: Option<u64>,
    subscribe_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone)]
struct ProbeConfig {
    grpc_url: String,
    x_token: String,
    connect_timeout_ms: u64,
    subscribe_timeout_ms: u64,
    program_ids: Vec<String>,
    mode: ProbeMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbeMode {
    TransactionFilter,
    SlotsOnly,
    BlocksMeta,
    EmptyThenSend,
}

impl ProbeMode {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "transaction-filter" => Ok(Self::TransactionFilter),
            "slots-only" => Ok(Self::SlotsOnly),
            "blocks-meta" => Ok(Self::BlocksMeta),
            "empty-then-send" => Ok(Self::EmptyThenSend),
            other => Err(anyhow!(
                "--mode must be one of transaction-filter, slots-only, blocks-meta, empty-then-send; got {other}"
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::TransactionFilter => "transaction-filter",
            Self::SlotsOnly => "slots-only",
            Self::BlocksMeta => "blocks-meta",
            Self::EmptyThenSend => "empty-then-send",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbePhase {
    Connect,
    SubscribeOpen,
    SubscribeSend,
    FirstMessage,
}

#[derive(Debug, Serialize)]
struct ProbeReport {
    probe_mode: String,
    config_loaded: bool,
    endpoint_host_redacted: Option<String>,
    connect_started: bool,
    connect_completed: bool,
    subscribe_started: bool,
    subscribe_completed: bool,
    subscribe_send_completed: bool,
    first_message_received: bool,
    elapsed_ms: u64,
    reason_class: String,
    error_redacted: Option<String>,
    production_green: bool,
}

impl ProbeReport {
    fn failed(reason_class: &str, error_redacted: Option<String>, elapsed_ms: u64) -> Self {
        Self {
            probe_mode: "unknown".to_string(),
            config_loaded: false,
            endpoint_host_redacted: None,
            connect_started: false,
            connect_completed: false,
            subscribe_started: false,
            subscribe_completed: false,
            subscribe_send_completed: false,
            first_message_received: false,
            elapsed_ms,
            reason_class: reason_class.to_string(),
            error_redacted,
            production_green: false,
        }
    }

    fn exit_code(&self) -> i32 {
        match self.reason_class.as_str() {
            REASON_OK | REASON_FIRST_MESSAGE_RECEIVED => 0,
            _ => 1,
        }
    }
}

#[tokio::main]
async fn main() {
    let started = Instant::now();
    let report = match parse_args_from(env::args().skip(1)) {
        Ok(cli) => {
            if !cli.json {
                let mut report = ProbeReport::failed(
                    "yellowstone_source_probe_json_required",
                    Some("--json is required for operator output".to_string()),
                    elapsed_ms(started),
                );
                report.probe_mode = cli.mode.as_str().to_string();
                report
            } else {
                run(cli, started).await
            }
        }
        Err(error) => ProbeReport::failed(
            "yellowstone_source_probe_cli_error",
            Some(error.to_string()),
            elapsed_ms(started),
        ),
    };

    println!(
        "{}",
        serde_json::to_string(&report).expect("probe report must serialize")
    );
    std::process::exit(report.exit_code());
}

async fn run(cli: Cli, started: Instant) -> ProbeReport {
    let loaded = match load_probe_config(&cli) {
        Ok(config) => config,
        Err(error) => {
            let mut report = ProbeReport::failed(
                classify_config_error(&error.to_string()),
                Some(redact_error(&error.to_string(), "", "")),
                elapsed_ms(started),
            );
            report.probe_mode = cli.mode.as_str().to_string();
            return report;
        }
    };

    match run_yellowstone_probe(&loaded, started).await {
        Ok(report) => report,
        Err(error) => {
            let mut report = ProbeReport::failed(
                REASON_CONNECT_FAILED,
                Some(redact_error(
                    &error.to_string(),
                    &loaded.grpc_url,
                    &loaded.x_token,
                )),
                elapsed_ms(started),
            );
            report.probe_mode = loaded.mode.as_str().to_string();
            report.config_loaded = true;
            report.endpoint_host_redacted = Some(redacted_endpoint_host(&loaded.grpc_url));
            report
        }
    }
}

fn load_probe_config(cli: &Cli) -> Result<ProbeConfig> {
    let app_config = load_from_path(&cli.config_path)
        .with_context(|| format!("failed to load config: {}", cli.config_path.display()))?;
    build_probe_config(
        &app_config.ingestion,
        cli.mode,
        cli.connect_timeout_ms,
        cli.subscribe_timeout_ms,
    )
}

fn build_probe_config(
    ingestion: &IngestionConfig,
    mode: ProbeMode,
    connect_timeout_override_ms: Option<u64>,
    subscribe_timeout_override_ms: Option<u64>,
) -> Result<ProbeConfig> {
    let source = ingestion.source.trim().to_ascii_lowercase();
    if source != "yellowstone" && source != "yellowstone_grpc" {
        return Err(anyhow!(
            "{REASON_CONFIG_MISSING_YELLOWSTONE}: ingestion.source must be yellowstone_grpc"
        ));
    }
    let grpc_url = ingestion.yellowstone_grpc_url.trim();
    let x_token = ingestion.yellowstone_x_token.trim();
    if grpc_url.is_empty() {
        return Err(anyhow!(
            "{REASON_CONFIG_MISSING_YELLOWSTONE}: ingestion.yellowstone_grpc_url is empty"
        ));
    }
    if grpc_url.contains("REPLACE_ME")
        || !(grpc_url.starts_with("http://") || grpc_url.starts_with("https://"))
    {
        return Err(anyhow!(
            "{REASON_CONFIG_MISSING_YELLOWSTONE}: ingestion.yellowstone_grpc_url must be a real http(s) endpoint"
        ));
    }
    if x_token.is_empty() {
        return Err(anyhow!(
            "{REASON_CONFIG_MISSING_YELLOWSTONE}: ingestion.yellowstone_x_token is empty"
        ));
    }
    if x_token.contains("REPLACE_ME") {
        return Err(anyhow!(
            "{REASON_CONFIG_MISSING_YELLOWSTONE}: ingestion.yellowstone_x_token is not configured"
        ));
    }
    let program_ids = if !ingestion.yellowstone_program_ids.is_empty() {
        ingestion.yellowstone_program_ids.clone()
    } else if !ingestion.subscribe_program_ids.is_empty() {
        ingestion.subscribe_program_ids.clone()
    } else {
        let mut fallback = ingestion.raydium_program_ids.clone();
        fallback.extend(ingestion.pumpswap_program_ids.iter().cloned());
        fallback
    };
    if mode == ProbeMode::TransactionFilter && program_ids.is_empty() {
        return Err(anyhow!(
            "{REASON_CONFIG_MISSING_YELLOWSTONE}: no yellowstone program filters configured"
        ));
    }

    Ok(ProbeConfig {
        grpc_url: grpc_url.to_string(),
        x_token: x_token.to_string(),
        connect_timeout_ms: connect_timeout_override_ms
            .unwrap_or(ingestion.yellowstone_connect_timeout_ms)
            .max(1),
        subscribe_timeout_ms: subscribe_timeout_override_ms
            .unwrap_or(ingestion.yellowstone_subscribe_timeout_ms)
            .max(1),
        program_ids,
        mode,
    })
}

async fn run_yellowstone_probe(config: &ProbeConfig, started: Instant) -> Result<ProbeReport> {
    let mut report = ProbeReport {
        probe_mode: config.mode.as_str().to_string(),
        config_loaded: true,
        endpoint_host_redacted: Some(redacted_endpoint_host(&config.grpc_url)),
        connect_started: false,
        connect_completed: false,
        subscribe_started: false,
        subscribe_completed: false,
        subscribe_send_completed: false,
        first_message_received: false,
        elapsed_ms: 0,
        reason_class: REASON_OK.to_string(),
        error_redacted: None,
        production_green: false,
    };

    let mut builder = GeyserGrpcClient::build_from_shared(config.grpc_url.clone())
        .map_err(|error| anyhow!("{REASON_CONNECT_FAILED}: {error}"))?;
    builder = builder
        .x_token(Some(config.x_token.as_str()))
        .map_err(|error| anyhow!("{REASON_CONNECT_FAILED}: {error}"))?;
    if config
        .grpc_url
        .trim()
        .to_ascii_lowercase()
        .starts_with("https://")
    {
        builder = builder
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .map_err(|error| anyhow!("{REASON_CONNECT_FAILED}: {error}"))?;
    }
    builder = builder
        .connect_timeout(Duration::from_millis(config.connect_timeout_ms))
        .timeout(Duration::from_millis(config.subscribe_timeout_ms))
        .http2_adaptive_window(true)
        .tcp_nodelay(true);

    report.connect_started = true;
    let mut client = match time::timeout(
        Duration::from_millis(config.connect_timeout_ms),
        builder.connect(),
    )
    .await
    {
        Ok(Ok(client)) => {
            report.connect_completed = true;
            client
        }
        Ok(Err(error)) => {
            report.reason_class = REASON_CONNECT_FAILED.to_string();
            report.error_redacted = Some(redact_error(
                &error.to_string(),
                &config.grpc_url,
                &config.x_token,
            ));
            report.elapsed_ms = elapsed_ms(started);
            return Ok(report);
        }
        Err(_) => {
            report.reason_class = timeout_reason_for_phase(ProbePhase::Connect).to_string();
            report.elapsed_ms = elapsed_ms(started);
            return Ok(report);
        }
    };

    report.subscribe_started = true;
    let (mut subscribe_tx, mut stream) = match time::timeout(
        Duration::from_millis(config.subscribe_timeout_ms),
        client.subscribe(),
    )
    .await
    {
        Ok(Ok(parts)) => {
            report.subscribe_completed = true;
            parts
        }
        Ok(Err(error)) => {
            report.reason_class =
                error_reason_for_phase(ProbePhase::SubscribeOpen, &error.to_string()).to_string();
            report.error_redacted = Some(redact_error(
                &error.to_string(),
                &config.grpc_url,
                &config.x_token,
            ));
            report.elapsed_ms = elapsed_ms(started);
            return Ok(report);
        }
        Err(_) => {
            report.reason_class = timeout_reason_for_phase(ProbePhase::SubscribeOpen).to_string();
            report.elapsed_ms = elapsed_ms(started);
            return Ok(report);
        }
    };

    let subscribe_request = build_subscribe_request(config.mode, &config.program_ids);
    match time::timeout(
        Duration::from_millis(config.subscribe_timeout_ms),
        subscribe_tx.send(subscribe_request),
    )
    .await
    {
        Ok(Ok(())) => {
            report.subscribe_send_completed = true;
        }
        Ok(Err(error)) => {
            report.reason_class = REASON_SUBSCRIBE_SEND_FAILED.to_string();
            report.error_redacted = Some(redact_error(
                &error.to_string(),
                &config.grpc_url,
                &config.x_token,
            ));
            report.elapsed_ms = elapsed_ms(started);
            return Ok(report);
        }
        Err(_) => {
            report.reason_class = timeout_reason_for_phase(ProbePhase::SubscribeSend).to_string();
            report.elapsed_ms = elapsed_ms(started);
            return Ok(report);
        }
    }

    match time::timeout(
        Duration::from_millis(config.subscribe_timeout_ms),
        stream.next(),
    )
    .await
    {
        Ok(Some(Ok(_))) => {
            report.first_message_received = true;
            report.reason_class = REASON_FIRST_MESSAGE_RECEIVED.to_string();
        }
        Ok(Some(Err(error))) => {
            report.reason_class = REASON_STREAM_ERROR.to_string();
            report.error_redacted = Some(redact_error(
                &error.to_string(),
                &config.grpc_url,
                &config.x_token,
            ));
        }
        Ok(None) => {
            report.reason_class = REASON_STREAM_CLOSED.to_string();
        }
        Err(_) => {
            report.reason_class = timeout_reason_for_phase(ProbePhase::FirstMessage).to_string();
        }
    }
    report.elapsed_ms = elapsed_ms(started);
    Ok(report)
}

fn build_subscribe_request(mode: ProbeMode, program_ids: &[String]) -> SubscribeRequest {
    match mode {
        ProbeMode::TransactionFilter => build_transaction_filter_subscribe_request(program_ids),
        ProbeMode::SlotsOnly => build_slots_only_subscribe_request(),
        ProbeMode::BlocksMeta => build_blocks_meta_subscribe_request(),
        ProbeMode::EmptyThenSend => build_empty_subscribe_request(),
    }
}

fn build_transaction_filter_subscribe_request(program_ids: &[String]) -> SubscribeRequest {
    let mut transactions = HashMap::new();
    transactions.insert(
        "copybot-swaps".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            signature: None,
            account_include: program_ids.to_vec(),
            account_exclude: Vec::new(),
            account_required: Vec::new(),
        },
    );

    SubscribeRequest {
        accounts: HashMap::new(),
        slots: HashMap::new(),
        transactions,
        transactions_status: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        entry: HashMap::new(),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        accounts_data_slice: Vec::new(),
        ping: None,
        from_slot: None,
    }
}

fn build_slots_only_subscribe_request() -> SubscribeRequest {
    let mut slots = HashMap::new();
    slots.insert(
        "copybot-slots-probe".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(true),
            interslot_updates: Some(false),
        },
    );

    SubscribeRequest {
        accounts: HashMap::new(),
        slots,
        transactions: HashMap::new(),
        transactions_status: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        entry: HashMap::new(),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        accounts_data_slice: Vec::new(),
        ping: None,
        from_slot: None,
    }
}

fn build_blocks_meta_subscribe_request() -> SubscribeRequest {
    let mut blocks_meta = HashMap::new();
    blocks_meta.insert(
        "copybot-blocks-meta-probe".to_string(),
        SubscribeRequestFilterBlocksMeta {},
    );

    SubscribeRequest {
        accounts: HashMap::new(),
        slots: HashMap::new(),
        transactions: HashMap::new(),
        transactions_status: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta,
        entry: HashMap::new(),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        accounts_data_slice: Vec::new(),
        ping: None,
        from_slot: None,
    }
}

fn build_empty_subscribe_request() -> SubscribeRequest {
    SubscribeRequest {
        accounts: HashMap::new(),
        slots: HashMap::new(),
        transactions: HashMap::new(),
        transactions_status: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        entry: HashMap::new(),
        commitment: None,
        accounts_data_slice: Vec::new(),
        ping: None,
        from_slot: None,
    }
}

fn parse_args_from<I>(args: I) -> Result<Cli>
where
    I: IntoIterator,
    I::Item: Into<String>,
{
    let mut config_path = None;
    let mut json = false;
    let mut mode = ProbeMode::TransactionFilter;
    let mut connect_timeout_ms = None;
    let mut subscribe_timeout_ms = None;
    let mut iter = args.into_iter().map(Into::into);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => {
                let value = iter
                    .next()
                    .ok_or_else(|| anyhow!("--config requires a path"))?;
                config_path = Some(PathBuf::from(value));
            }
            "--json" => json = true,
            "--mode" => {
                let value = iter
                    .next()
                    .ok_or_else(|| anyhow!("--mode requires a value"))?;
                mode = ProbeMode::parse(&value)?;
            }
            "--connect-timeout-ms" => {
                let value = iter
                    .next()
                    .ok_or_else(|| anyhow!("--connect-timeout-ms requires a value"))?;
                connect_timeout_ms = Some(parse_positive_u64("--connect-timeout-ms", &value)?);
            }
            "--subscribe-timeout-ms" => {
                let value = iter
                    .next()
                    .ok_or_else(|| anyhow!("--subscribe-timeout-ms requires a value"))?;
                subscribe_timeout_ms = Some(parse_positive_u64("--subscribe-timeout-ms", &value)?);
            }
            other => return Err(anyhow!("unknown argument: {other}")),
        }
    }

    Ok(Cli {
        config_path: config_path.ok_or_else(|| anyhow!("--config is required"))?,
        json,
        mode,
        connect_timeout_ms,
        subscribe_timeout_ms,
    })
}

fn parse_positive_u64(name: &str, value: &str) -> Result<u64> {
    let parsed = value
        .parse::<u64>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        return Err(anyhow!("{name} must be greater than zero"));
    }
    Ok(parsed)
}

fn timeout_reason_for_phase(phase: ProbePhase) -> &'static str {
    match phase {
        ProbePhase::Connect => REASON_CONNECT_TIMEOUT,
        ProbePhase::SubscribeOpen => REASON_SUBSCRIBE_OPEN_TIMEOUT,
        ProbePhase::SubscribeSend => REASON_SUBSCRIBE_SEND_TIMEOUT,
        ProbePhase::FirstMessage => REASON_FIRST_MESSAGE_TIMEOUT,
    }
}

fn error_reason_for_phase(phase: ProbePhase, message: &str) -> &'static str {
    if phase == ProbePhase::SubscribeOpen && error_message_is_timeout_shaped(message) {
        return timeout_reason_for_phase(phase);
    }

    match phase {
        ProbePhase::Connect => REASON_CONNECT_FAILED,
        ProbePhase::SubscribeOpen => REASON_SUBSCRIBE_OPEN_FAILED,
        ProbePhase::SubscribeSend => REASON_SUBSCRIBE_SEND_FAILED,
        ProbePhase::FirstMessage => REASON_STREAM_ERROR,
    }
}

fn error_message_is_timeout_shaped(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    let compact = normalized
        .chars()
        .filter(|ch| !ch.is_whitespace() && *ch != '_' && *ch != '-')
        .collect::<String>();

    normalized.contains("timeout expired")
        || normalized.contains("timed out")
        || normalized.contains("deadline exceeded")
        || compact.contains("timeoutexpired")
        || (normalized.contains("deadline") && normalized.contains("timeout"))
        || (normalized.contains("cancelled") && compact.contains("timeout"))
        || (normalized.contains("canceled") && compact.contains("timeout"))
}

fn classify_config_error(message: &str) -> &'static str {
    if message.contains(REASON_CONFIG_MISSING_YELLOWSTONE) {
        REASON_CONFIG_MISSING_YELLOWSTONE
    } else if message.contains("failed to parse TOML") {
        REASON_CONFIG_MALFORMED
    } else {
        REASON_CONFIG_UNREADABLE
    }
}

fn redacted_endpoint_host(raw_url: &str) -> String {
    match Url::parse(raw_url.trim()) {
        Ok(parsed) => {
            let port = parsed
                .port()
                .map(|port| format!(":{port}"))
                .unwrap_or_default();
            format!("{}://<redacted-host>{port}", parsed.scheme())
        }
        Err(_) => "<invalid-endpoint-redacted>".to_string(),
    }
}

fn redact_error(message: &str, endpoint_url: &str, token: &str) -> String {
    let mut redacted = message.to_string();
    let token = token.trim();
    if !token.is_empty() && token != "REPLACE_ME" {
        redacted = redacted.replace(token, "<redacted-token>");
    }
    if let Ok(parsed) = Url::parse(endpoint_url.trim()) {
        if let Some(host) = parsed.host_str() {
            redacted = redacted.replace(host, "<redacted-host>");
        }
        if !parsed.username().is_empty() {
            redacted = redacted.replace(parsed.username(), "<redacted-user>");
        }
        if let Some(password) = parsed.password() {
            redacted = redacted.replace(password, "<redacted-password>");
        }
        if let Some(query) = parsed.query() {
            redacted = redacted.replace(query, "<redacted-query>");
        }
    }
    redacted
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_config::IngestionConfig;

    fn yellowstone_ingestion_config() -> IngestionConfig {
        let mut config = IngestionConfig::default();
        config.source = "yellowstone_grpc".to_string();
        config.yellowstone_grpc_url =
            "https://user:secret@example.quicknode.invalid:10000/path?token=url-secret".to_string();
        config.yellowstone_x_token = "x-token-secret".to_string();
        config.yellowstone_connect_timeout_ms = 5_000;
        config.yellowstone_subscribe_timeout_ms = 15_000;
        config.yellowstone_program_ids =
            vec!["Program1111111111111111111111111111111111".to_string()];
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
        config.subscribe_program_ids =
            vec!["Fallback111111111111111111111111111111111".to_string()];
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
            REASON_CONFIG_MALFORMED
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
}
