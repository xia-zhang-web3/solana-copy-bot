use anyhow::{anyhow, Result};
use futures_util::StreamExt;
use std::time::{Duration, Instant};
use tokio::time;
use tonic::transport::ClientTlsConfig;
use yellowstone_grpc_client::GeyserGrpcClient;

use super::config::ProbeConfig;
use super::report::{elapsed_ms, redact_error, redacted_endpoint_host, ProbeReport};
use super::request::build_subscribe_request;

pub(crate) const REASON_CONNECT_TIMEOUT: &str = "yellowstone_connect_timeout";
const REASON_CONNECT_FAILED: &str = "yellowstone_connect_failed";
pub(crate) const REASON_SUBSCRIBE_OPEN_TIMEOUT: &str = "yellowstone_subscription_open_timeout";
pub(crate) const REASON_SUBSCRIBE_OPEN_FAILED: &str = "yellowstone_subscription_open_failed";
pub(crate) const REASON_SUBSCRIBE_SEND_TIMEOUT: &str = "yellowstone_subscribe_request_send_timeout";
const REASON_SUBSCRIBE_SEND_FAILED: &str = "yellowstone_subscribe_request_send_failed";
pub(crate) const REASON_FIRST_MESSAGE_TIMEOUT: &str = "yellowstone_first_message_timeout";
const REASON_FIRST_MESSAGE_RECEIVED: &str = "yellowstone_first_message_received";
const REASON_STREAM_CLOSED: &str = "yellowstone_stream_closed_before_first_message";
const REASON_STREAM_ERROR: &str = "yellowstone_stream_error_before_first_message";
const REASON_OK: &str = "yellowstone_source_probe_subscribe_opened";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProbePhase {
    Connect,
    SubscribeOpen,
    #[allow(dead_code)]
    SubscribeSend,
    FirstMessage,
}

pub(crate) async fn run_yellowstone_probe(
    config: &ProbeConfig,
    started: Instant,
) -> Result<ProbeReport> {
    let mut report = ProbeReport {
        probe_mode: config.mode.as_str().to_string(),
        config_loaded: true,
        endpoint_host_redacted: Some(redacted_endpoint_host(&config.grpc_url)),
        connect_started: false,
        connect_completed: false,
        subscribe_started: false,
        subscribe_completed: false,
        subscribe_send_completed: false,
        initial_request_sent_during_subscribe_open: false,
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

    let subscribe_request = build_subscribe_request(config.mode, &config.program_ids);
    report.subscribe_started = true;
    report.initial_request_sent_during_subscribe_open = true;
    let (_subscribe_tx, mut stream) = match time::timeout(
        Duration::from_millis(config.subscribe_timeout_ms),
        client.subscribe_with_request(Some(subscribe_request)),
    )
    .await
    {
        Ok(Ok(parts)) => {
            report.subscribe_completed = true;
            report.subscribe_send_completed = true;
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

pub(crate) fn timeout_reason_for_phase(phase: ProbePhase) -> &'static str {
    match phase {
        ProbePhase::Connect => REASON_CONNECT_TIMEOUT,
        ProbePhase::SubscribeOpen => REASON_SUBSCRIBE_OPEN_TIMEOUT,
        ProbePhase::SubscribeSend => REASON_SUBSCRIBE_SEND_TIMEOUT,
        ProbePhase::FirstMessage => REASON_FIRST_MESSAGE_TIMEOUT,
    }
}

pub(crate) fn error_reason_for_phase(phase: ProbePhase, message: &str) -> &'static str {
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
