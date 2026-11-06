use super::{
    config::{load_probe_config, Cli},
    delivery_config::DeliveryConfig,
    delivery_output::DeliveryFiles,
    delivery_report::{DeliveryReport, Stage},
    delivery_stream::{pump, wait_stage, WaitFailure},
    report::{elapsed_ms, redacted_endpoint_host, ProbeReport},
    request::build_subscribe_request,
};
use std::time::{Duration, Instant as HostInstant};
use tokio::time::{self, Instant, MissedTickBehavior};
use tonic::transport::ClientTlsConfig;
use yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientError};

pub(super) fn finish(
    mut state: DeliveryReport,
    files: Option<&DeliveryFiles>,
    started: Instant,
    host_started: HostInstant,
    endpoint: Option<String>,
) -> ProbeReport {
    state.refresh(started);
    state.terminal = true;
    state.stage = Stage::Terminal;
    state.reason_class = state.reason_class();
    if let Some(files) = files {
        if files.progress(&mut state).is_err() {
            state.artifact_errors.push("terminal_progress_write_failed");
        }
        state.reason_class = state.reason_class();
        if files.final_report(&state).is_err() {
            state.artifact_errors.push("final_write_failed");
            state.reason_class = state.reason_class();
        }
    }
    let mut report = ProbeReport::failed(state.reason_class(), None, elapsed_ms(host_started));
    report.probe_mode = "transaction-delivery".into();
    report.config_loaded = state.endpoint_sha256.is_some();
    report.endpoint_host_redacted = endpoint;
    report.connect_started = state.connect_started;
    report.connect_completed = state.connect_completed;
    report.subscribe_started = state.subscribe_started;
    report.subscribe_completed = state.subscribe_completed;
    report.subscribe_send_completed = state.initial_request_send_completed;
    report.initial_request_sent_during_subscribe_open = state.initial_request_send_completed;
    report.first_message_received = state.counts.decoded_updates > 0;
    report.delivery =
        Some(serde_json::to_value(state).expect("bounded delivery report serializes"));
    report
}

pub(crate) async fn run(
    cli: &Cli,
    limits: &DeliveryConfig,
    host_started: HostInstant,
) -> ProbeReport {
    let started = Instant::now();
    let deadline = started + Duration::from_millis(limits.duration_ms);
    let mut ticker = time::interval_at(started + Duration::from_secs(1), Duration::from_secs(1));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut state = DeliveryReport::new(limits);
    let files = match DeliveryFiles::create(limits.output.clone()) {
        Ok(files) => files,
        Err(_) => {
            state.fail(Stage::Artifact, "output_create_failed", None);
            state.artifact_errors.push("output_create_failed");
            return finish(state, None, started, host_started, None);
        }
    };
    let config = match load_probe_config(cli) {
        Ok(config) => config,
        Err(_) => {
            state.fail(Stage::Config, "effective_config_invalid", None);
            return finish(state, Some(&files), started, host_started, None);
        }
    };
    let endpoint = Some(redacted_endpoint_host(&config.grpc_url));
    let request = build_subscribe_request(config.mode, &config.program_ids);
    if files.request(&config, &request, &mut state).is_err() {
        state.fail(Stage::Artifact, "request_artifact_failed", None);
        state.artifact_errors.push("request_artifact_failed");
        return finish(state, Some(&files), started, host_started, endpoint);
    }
    let builder = GeyserGrpcClient::build_from_shared(config.grpc_url.clone())
        .and_then(|builder| builder.x_token(Some(config.x_token.as_str())));
    let mut builder = match builder {
        Ok(builder) => builder,
        Err(_) => {
            state.fail(Stage::Config, "client_configuration_failed", None);
            return finish(state, Some(&files), started, host_started, endpoint);
        }
    };
    if config.grpc_url.starts_with("https://") {
        builder = match builder.tls_config(ClientTlsConfig::new().with_native_roots()) {
            Ok(builder) => builder,
            Err(_) => {
                state.fail(Stage::Config, "tls_configuration_failed", None);
                return finish(state, Some(&files), started, host_started, endpoint);
            }
        };
    }
    builder = builder
        .connect_timeout(Duration::from_millis(config.connect_timeout_ms))
        .timeout(Duration::from_millis(config.subscribe_timeout_ms))
        .http2_adaptive_window(true)
        .tcp_nodelay(true)
        .max_decoding_message_size(limits.max_message_bytes);
    state.stage = Stage::Connect;
    state.connect_started = true;
    let connect_deadline =
        deadline.min(Instant::now() + Duration::from_millis(config.connect_timeout_ms));
    let mut client = match wait_stage(
        builder.connect(),
        connect_deadline,
        &mut ticker,
        &mut state,
        &files,
        started,
    )
    .await
    {
        Ok(Ok(client)) => client,
        Ok(Err(_)) => {
            state.fail(Stage::Connect, "transport_error", None);
            return finish(state, Some(&files), started, host_started, endpoint);
        }
        Err(error) => {
            if error == WaitFailure::Deadline {
                state.fail(Stage::Connect, "deadline", None);
            }
            if error == WaitFailure::ExternalStop {
                state.stop_reason = "external_stop";
            }
            return finish(state, Some(&files), started, host_started, endpoint);
        }
    };
    state.connect_completed = true;
    state.stage = Stage::SubscribeOpen;
    state.subscribe_started = true;
    let subscribe_deadline =
        deadline.min(Instant::now() + Duration::from_millis(config.subscribe_timeout_ms));
    let (mut sender, mut stream) = match wait_stage(
        client.subscribe_with_request(Some(request)),
        subscribe_deadline,
        &mut ticker,
        &mut state,
        &files,
        started,
    )
    .await
    {
        Ok(Ok(parts)) => parts,
        Ok(Err(GeyserGrpcClientError::SubscribeSendError(_))) => {
            state.fail(Stage::InitialSend, "send_failed", None);
            return finish(state, Some(&files), started, host_started, endpoint);
        }
        Ok(Err(GeyserGrpcClientError::TonicStatus(error))) => {
            state.fail(
                Stage::SubscribeOpen,
                "grpc_status_error",
                Some(error.code() as i32),
            );
            return finish(state, Some(&files), started, host_started, endpoint);
        }
        Err(error) => {
            if error == WaitFailure::Deadline {
                state.fail(Stage::SubscribeOpen, "deadline", None);
            }
            if error == WaitFailure::ExternalStop {
                state.stop_reason = "external_stop";
            }
            return finish(state, Some(&files), started, host_started, endpoint);
        }
    };
    state.subscribe_completed = true;
    state.initial_request_send_completed = true;
    state.stage = Stage::Stream;
    state.stop_reason = pump(
        &mut stream,
        &mut sender,
        deadline,
        &mut ticker,
        &mut state,
        &files,
        started,
    )
    .await;
    finish(state, Some(&files), started, host_started, endpoint)
}
