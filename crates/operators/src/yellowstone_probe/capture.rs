use super::{
    capture_config::CaptureConfig,
    capture_files::CaptureFiles,
    capture_terminal::{TerminalStage, TerminalStatus},
    config::ProbeConfig,
    report::{elapsed_ms, ProbeReport},
    request::build_subscribe_request,
};
use futures_util::StreamExt;
use serde_json::json;
use std::time::{Duration, Instant};
use tokio::time::{timeout_at, Instant as Deadline};
use tonic::transport::ClientTlsConfig;
use yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientError};
use yellowstone_grpc_proto::prost::Message;

pub(crate) async fn run(
    config: &ProbeConfig,
    limits: &CaptureConfig,
    started: Instant,
) -> ProbeReport {
    let mut report = ProbeReport::failed("capture_output_create_failed", None, elapsed_ms(started));
    report.probe_mode = "association-capture".into();
    report.config_loaded = true;
    // No provider-controlled error text enters capture output. Existing modes
    // retain their redaction/report contract unchanged.
    if config.program_ids.is_empty()
        || config.program_ids.len() > 32
        || config
            .program_ids
            .iter()
            .any(|p| bs58::decode(p).into_vec().map_or(true, |v| v.len() != 32))
    {
        report.reason_class = "capture_invalid_program_policy".into();
        return report;
    }
    let session_start = Instant::now();
    let deadline = Deadline::now() + Duration::from_millis(limits.duration_ms);
    let mut files = match CaptureFiles::create(limits) {
        Ok(files) => files,
        Err(_) => return report,
    };
    let request = build_subscribe_request(config.mode, &config.program_ids);
    use base64::Engine;
    let request_base64 = base64::engine::general_purpose::STANDARD.encode(request.encode_to_vec());
    let request_description = json!({"program_ids":config.program_ids,"commitment":"confirmed",
        "transactions":{"vote":false,"failed":false,"account_include":config.program_ids},
        "blocks":{"account_include":config.program_ids,"include_transactions":true,
            "include_accounts":false,"include_entries":false},
        "subscribe_request_base64":request_base64});
    let reason = capture_stream(config, &mut files, &mut report, deadline, session_start).await;
    let outcome =
        super::capture_persist::persist(files, reason, request_description, ns(session_start))
            .await;
    report.reason_class = outcome.reason.into();
    report.capture = Some(outcome.capture);
    report.elapsed_ms = elapsed_ms(started);
    report
}

pub(super) async fn capture_stream(
    config: &ProbeConfig,
    files: &mut CaptureFiles,
    report: &mut ProbeReport,
    deadline: Deadline,
    session_start: Instant,
) -> &'static str {
    let Ok(builder) = GeyserGrpcClient::build_from_shared(config.grpc_url.clone()) else {
        return "connect_failed";
    };
    let Ok(mut builder) = builder.x_token(Some(config.x_token.as_str())) else {
        return "connect_failed";
    };
    if config.grpc_url.starts_with("https://") {
        builder = match builder.tls_config(ClientTlsConfig::new().with_native_roots()) {
            Ok(b) => b,
            Err(_) => return "connect_failed",
        };
    }
    builder = builder
        .max_decoding_message_size(files.config.transport_bytes())
        .tcp_nodelay(true);
    report.connect_started = true;
    let mut client = match timeout_at(deadline, builder.connect()).await {
        Err(_) => return "connect_deadline",
        Ok(Err(_)) => return "connect_failed",
        Ok(Ok(client)) => client,
    };
    report.connect_completed = true;
    report.subscribe_started = true;
    report.initial_request_sent_during_subscribe_open = true;
    let request = build_subscribe_request(config.mode, &config.program_ids);
    let (_sender, mut stream) =
        match timeout_at(deadline, client.subscribe_with_request(Some(request))).await {
            Err(_) => return "subscribe_open_deadline",
            Ok(Err(error)) => {
                if let GeyserGrpcClientError::TonicStatus(status) = error {
                    files.terminal_status = Some(TerminalStatus::from_status(
                        TerminalStage::SubscribeOpen,
                        &status,
                        files.config.transport_bytes() as u64,
                    ));
                }
                return "subscribe_open_failed";
            }
            Ok(Ok(parts)) => parts,
        };
    report.subscribe_completed = true;
    report.subscribe_send_completed = true;
    loop {
        if Deadline::now() >= deadline {
            return "stream_deadline";
        }
        if files.received >= files.config.messages {
            return "message_count_limit";
        }
        match timeout_at(deadline, stream.next()).await {
            Err(_) => return "stream_deadline",
            Ok(None) => return "stream_closed",
            Ok(Some(Err(error))) => {
                files.terminal_status = Some(TerminalStatus::from_status(
                    TerminalStage::StreamNext,
                    &error,
                    files.config.transport_bytes() as u64,
                ));
                return if error.code() == tonic::Code::OutOfRange
                    || error.code() == tonic::Code::ResourceExhausted
                {
                    "transport_decode_limit_or_resource_error"
                } else {
                    "stream_error"
                };
            }
            Ok(Some(Ok(message))) => {
                let arrival_ns = ns(session_start);
                report.first_message_received = true;
                if let Err(reason) = files.record(&message, arrival_ns) {
                    return reason;
                }
            }
        }
    }
}
fn ns(started: Instant) -> u64 {
    started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX)
}
