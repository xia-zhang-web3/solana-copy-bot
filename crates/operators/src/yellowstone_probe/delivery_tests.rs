use super::super::{
    config::{build_probe_config, parse_args_from, ProbeMode},
    delivery,
    delivery_config::DeliveryConfig,
    delivery_output::DeliveryFiles,
    delivery_report::{DeliveryReport, Stage},
    delivery_stream::{pump, wait_stage, WaitFailure},
    request::build_subscribe_request,
};
use copybot_config::IngestionConfig;
use futures_util::{stream, Sink, StreamExt};
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::time::{self, Instant};
use yellowstone_grpc_proto::{prelude::*, prost::Message};

#[derive(Default)]
struct Sender {
    requests: Arc<Mutex<Vec<SubscribeRequest>>>,
    fail: bool,
}
impl Sink<SubscribeRequest> for Sender {
    type Error = &'static str;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(self: Pin<&mut Self>, request: SubscribeRequest) -> Result<(), Self::Error> {
        if self.fail {
            return Err("synthetic-private-provider-text");
        }
        self.requests.lock().unwrap().push(request);
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

struct Fixture {
    _temp: tempfile::TempDir,
    limits: DeliveryConfig,
    files: DeliveryFiles,
}
impl Fixture {
    fn new(duration_ms: u64) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let limits = DeliveryConfig {
            output: temp.path().join("out"),
            stop_file: None,
            duration_ms,
            progress_ms: 1000,
            max_messages: 100,
            max_message_bytes: 4_194_304,
            max_decoded_bytes: 8_589_934_592,
            sample_transactions: 0,
            max_sample_bytes: 0,
        };
        let files = DeliveryFiles::create(limits.output.clone()).unwrap();
        Self {
            _temp: temp,
            limits,
            files,
        }
    }
}
fn transaction(slot: u64) -> SubscribeUpdate {
    SubscribeUpdate {
        update_oneof: Some(subscribe_update::UpdateOneof::Transaction(
            SubscribeUpdateTransaction {
                slot,
                transaction: Some(SubscribeUpdateTransactionInfo::default()),
                ..Default::default()
            },
        )),
        created_at: Some(yellowstone_grpc_proto::prost_types::Timestamp {
            seconds: 123,
            nanos: 456,
        }),
        ..Default::default()
    }
}
fn ping() -> SubscribeUpdate {
    SubscribeUpdate {
        update_oneof: Some(subscribe_update::UpdateOneof::Ping(SubscribeUpdatePing {})),
        ..Default::default()
    }
}
fn ticker(started: Instant) -> time::Interval {
    time::interval_at(started + Duration::from_secs(1), Duration::from_secs(1))
}

#[test]
fn cli_bounds_and_exact_app_request_without_widening() {
    let cli = parse_args_from([
        "--config",
        "unused.toml",
        "--json",
        "--mode",
        "transaction-delivery",
        "--output-dir",
        "unused-output",
        "--duration-ms",
        "600000",
        "--stop-file",
        "control/STOP_PROBE",
    ])
    .unwrap();
    let limits = cli.delivery.unwrap();
    assert_eq!(limits.max_message_bytes, 4_194_304);
    assert_eq!(limits.max_messages, 8_000_000);
    assert_eq!(limits.max_decoded_bytes, 8_589_934_592);
    assert_eq!(limits.progress_ms, 1000);
    assert!(cli.capture.is_none());
    for (flag, value) in [
        ("--duration-ms", "600001"),
        ("--max-message-bytes", "4194305"),
        ("--progress-ms", "999"),
    ] {
        assert!(parse_args_from([
            "--config",
            "unused",
            "--mode",
            "transaction-delivery",
            "--output-dir",
            "out",
            flag,
            value
        ])
        .is_err());
    }
    assert!(parse_args_from([
        "--config",
        "unused",
        "--mode",
        "transaction-filter",
        "--progress-ms",
        "1000"
    ])
    .is_err());
    let mut config = IngestionConfig::default();
    config.source = "yellowstone_grpc".into();
    config.yellowstone_grpc_url = "http://127.0.0.1:1".into();
    config.yellowstone_x_token = "synthetic-secret".into();
    config.yellowstone_program_ids = vec!["b".into(), "a".into(), "b".into()];
    let probe =
        build_probe_config(&config, ProbeMode::TransactionDelivery, Some(1), Some(1)).unwrap();
    assert_eq!(probe.program_ids, vec!["a", "b"]);
    assert_eq!(probe.connect_timeout_ms, 500);
    assert_eq!(probe.subscribe_timeout_ms, 1000);
    let request = build_subscribe_request(ProbeMode::TransactionDelivery, &probe.program_ids);
    let filter = &request.transactions["copybot-swaps"];
    assert_eq!(request.commitment, Some(CommitmentLevel::Confirmed as i32));
    assert_eq!(filter.vote, Some(false));
    assert_eq!(filter.failed, Some(false));
    assert_eq!(filter.signature, None);
    assert_eq!(filter.account_include, probe.program_ids);
    assert!(filter.account_exclude.is_empty());
    assert!(filter.account_required.is_empty());
    assert!(filter.cuckoo_account_include.is_none());
    assert!(filter.token_accounts.is_none());
    assert_eq!(request.transactions.len(), 1);
    assert!(request.accounts.is_empty());
    assert!(request.slots.is_empty());
    assert!(request.blocks.is_empty());
    assert!(request.blocks_meta.is_empty());
    assert!(request.transactions_status.is_empty());
    assert!(request.entry.is_empty());
    assert!(request.accounts_data_slice.is_empty());
    assert!(request.from_slot.is_none());
    assert!(request.ping.is_none());
    let f = Fixture::new(1);
    let mut report = DeliveryReport::new(&f.limits);
    f.files.request(&probe, &request, &mut report).unwrap();
    let saved = std::fs::read(f.limits.output.join("request.json")).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&saved).unwrap();
    assert_eq!(
        value["endpoint_sha256"],
        super::super::capture_hash::sha256(probe.grpc_url.as_bytes())
    );
    assert_eq!(
        value["x_token_sha256"],
        super::super::capture_hash::sha256(probe.x_token.as_bytes())
    );
    assert!(!String::from_utf8(saved)
        .unwrap()
        .contains("synthetic-secret"));
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(value["subscribe_request_base64"].as_str().unwrap())
        .unwrap();
    let decoded = SubscribeRequest::decode(bytes.as_slice()).unwrap();
    assert_eq!(decoded, request);
}

#[tokio::test]
async fn ping_only_reports_periodically_and_cannot_pass_transaction_delivery() {
    let f = Fixture::new(1250);
    let mut report = DeliveryReport::new(&f.limits);
    report.stage = Stage::Stream;
    let started = Instant::now();
    let mut ticks = ticker(started);
    let mut sender = Sender::default();
    let sent = sender.requests.clone();
    let mut updates = stream::iter(vec![Ok(ping())]).chain(stream::pending());
    let reason = {
        let future = pump(
            &mut updates,
            &mut sender,
            started + Duration::from_millis(1250),
            &mut ticks,
            &mut report,
            &f.files,
            started,
        );
        tokio::pin!(future);
        tokio::select! {
            result=&mut future=>panic!("finished before independent progress: {result}"),
            _=time::sleep(Duration::from_millis(1100))=>{
                let value:serde_json::Value=serde_json::from_slice(&std::fs::read(f.limits.output.join("progress.json")).unwrap()).unwrap();
                assert_eq!(value["update_counts"]["ping"],1);assert_eq!(value["update_counts"]["transaction"],0);
                assert!(value["elapsed_ms"].as_u64().unwrap()>=1000);
            }
        }
        future.await
    };
    assert_eq!(reason, "duration_limit");
    assert_eq!(report.counts.ping_replies, 1);
    assert_eq!(
        report.reason_class(),
        "transaction_delivery_no_transactions"
    );
    let requests = sent.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].ping.as_ref().unwrap().id, 1);
    assert!(requests[0].transactions.is_empty());
}

#[tokio::test]
async fn decoded_transaction_metadata_samples_and_periodic_empty_tail_are_independent_of_swap_parser(
) {
    let mut f = Fixture::new(1100);
    f.limits.sample_transactions = 1;
    f.limits.max_sample_bytes = 1024;
    let mut report = DeliveryReport::new(&f.limits);
    let started = Instant::now();
    let mut ticks = ticker(started);
    let mut updates =
        stream::iter(vec![Ok(transaction(77)), Ok(transaction(78))]).chain(stream::pending());
    let reason = pump(
        &mut updates,
        &mut Sender::default(),
        started + Duration::from_millis(1100),
        &mut ticks,
        &mut report,
        &f.files,
        started,
    )
    .await;
    assert_eq!(reason, "duration_limit");
    assert_eq!(report.counts.transactions, 2);
    assert_eq!(report.last_transaction_slot, Some(78));
    assert_eq!(report.first_transaction_slot, Some(77));
    assert_eq!(
        report
            .first_transaction_provider_created_at
            .as_ref()
            .unwrap()
            .seconds,
        123
    );
    let created = report
        .last_transaction_provider_created_at
        .as_ref()
        .unwrap();
    assert_eq!((created.seconds, created.nanos), (123, 456));
    assert!(report
        .provider_created_at_basis
        .contains("not chain block time"));
    assert_eq!(report.reason_class(), "transaction_delivery_observed");
    assert!(report.progress_sequence >= 1);
    assert_eq!(report.samples.len(), 1);
    assert_eq!(report.samples_omitted_by_limits, 1);
    let bytes = std::fs::read(f.limits.output.join(&report.samples[0].file)).unwrap();
    let decoded = SubscribeUpdate::decode(bytes.as_slice()).unwrap();
    assert!(matches!(
        decoded.update_oneof,
        Some(subscribe_update::UpdateOneof::Transaction(_))
    ));
    assert!(report.sample_bytes <= 1024);
    assert!(report
        .byte_counter_basis
        .contains("not transport wire bytes"));
}

#[tokio::test]
async fn no_message_deadline_and_early_eof_are_distinct_non_successes() {
    for eof in [false, true] {
        let f = Fixture::new(15);
        let mut report = DeliveryReport::new(&f.limits);
        let started = Instant::now();
        let mut ticks = ticker(started);
        let mut source = stream::empty();
        let mut pending = stream::pending();
        let mut sender = Sender::default();
        let reason = if eof {
            pump(
                &mut source,
                &mut sender,
                started + Duration::from_millis(15),
                &mut ticks,
                &mut report,
                &f.files,
                started,
            )
            .await
        } else {
            pump(
                &mut pending,
                &mut sender,
                started + Duration::from_millis(15),
                &mut ticks,
                &mut report,
                &f.files,
                started,
            )
            .await
        };
        assert_eq!(report.counts.transactions, 0);
        assert_eq!(
            reason,
            if eof {
                "stream_closed"
            } else {
                "duration_limit"
            }
        );
        assert_eq!(report.possible_update_loss, eof);
        assert_ne!(report.reason_class(), "transaction_delivery_observed");
    }
}

#[tokio::test]
async fn grpc_failure_caps_and_ping_send_failure_retain_loss_classification_without_text() {
    let f = Fixture::new(20);
    let mut report = DeliveryReport::new(&f.limits);
    let started = Instant::now();
    let mut ticks = ticker(started);
    let mut source = stream::iter(vec![Err(tonic::Status::resource_exhausted(
        "synthetic-private-provider-text",
    ))]);
    assert_eq!(
        pump(
            &mut source,
            &mut Sender::default(),
            started + Duration::from_millis(20),
            &mut ticks,
            &mut report,
            &f.files,
            started
        )
        .await,
        "stream_error"
    );
    assert_eq!(report.failure.as_ref().unwrap().grpc_code, Some(8));
    assert!(report.possible_update_loss);
    assert!(!serde_json::to_string(&report)
        .unwrap()
        .contains("synthetic-private"));
    let mut report = DeliveryReport::new(&f.limits);
    report.limits.max_decoded_bytes = 1;
    let mut source = stream::iter(vec![Ok(transaction(9))]);
    assert_eq!(
        pump(
            &mut source,
            &mut Sender::default(),
            Instant::now() + Duration::from_millis(20),
            &mut ticks,
            &mut report,
            &f.files,
            started
        )
        .await,
        "decoded_byte_limit"
    );
    assert!(report.possible_update_loss);
    assert!(!report.full_window_observed);
    let mut report = DeliveryReport::new(&f.limits);
    let mut source = stream::iter(vec![Ok(ping())]);
    assert_eq!(
        pump(
            &mut source,
            &mut Sender {
                fail: true,
                ..Default::default()
            },
            Instant::now() + Duration::from_millis(20),
            &mut ticks,
            &mut report,
            &f.files,
            started
        )
        .await,
        "ping_reply_failed"
    );
    assert_eq!(report.failure.as_ref().unwrap().stage, Stage::PingReply);
    assert!(report.possible_update_loss);
}

#[tokio::test]
async fn stop_file_is_checked_before_stage_poll_and_during_empty_stream() {
    let mut f = Fixture::new(3000);
    let stop = f._temp.path().join("STOP_PROBE");
    f.limits.stop_file = Some(stop.clone());
    std::fs::write(&stop, b"stop").unwrap();
    let polled = Arc::new(AtomicBool::new(false));
    let marker = polled.clone();
    let mut report = DeliveryReport::new(&f.limits);
    report.stage = Stage::SubscribeOpen;
    report.subscribe_started = true;
    let started = Instant::now();
    let mut ticks = ticker(started);
    let result = wait_stage(
        async move {
            marker.store(true, Ordering::SeqCst);
        },
        started + Duration::from_secs(3),
        &mut ticks,
        &mut report,
        &f.files,
        started,
    )
    .await;
    assert_eq!(result, Err(WaitFailure::ExternalStop));
    assert!(!polled.load(Ordering::SeqCst));
    report.stop_reason = "external_stop";
    let terminal = delivery::finish(report, None, started, std::time::Instant::now(), None);
    assert!(terminal.subscribe_started);
    assert!(!terminal.initial_request_sent_during_subscribe_open);
    let nested = terminal.delivery.as_ref().unwrap();
    assert_eq!(nested["subscribe_started"], true);
    assert_eq!(nested["initial_request_send_completed"], false);
    let mut report = DeliveryReport::new(&f.limits);
    std::fs::remove_file(&stop).unwrap();
    let stop_during_result = stop.clone();
    let result = wait_stage(
        async move {
            std::fs::write(stop_during_result, b"parent-common-deadline").unwrap();
            Err::<(), _>(tonic::Status::unavailable("synthetic-private-error"))
        },
        started + Duration::from_secs(3),
        &mut ticks,
        &mut report,
        &f.files,
        started,
    )
    .await;
    assert!(matches!(result, Err(WaitFailure::ExternalStop)));
    assert!(report.failure.is_none());
    std::fs::remove_file(&stop).unwrap();
    tokio::spawn(async move {
        time::sleep(Duration::from_millis(40)).await;
        std::fs::write(stop, b"parent-common-deadline").unwrap();
    });
    let mut source = stream::pending();
    let reason = pump(
        &mut source,
        &mut Sender::default(),
        started + Duration::from_secs(3),
        &mut ticks,
        &mut report,
        &f.files,
        started,
    )
    .await;
    report.stop_reason = reason;
    assert_eq!(reason, "external_stop");
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(report.failure.is_none());
    assert!(!report.full_window_observed);
    assert_eq!(
        report.reason_class(),
        "transaction_delivery_no_transactions"
    );
}

#[tokio::test]
async fn stage_timeout_progress_failure_and_config_error_finalize_without_provider() {
    let f = Fixture::new(30);
    let mut report = DeliveryReport::new(&f.limits);
    let started = Instant::now();
    let mut ticks = ticker(started);
    assert_eq!(
        wait_stage(
            std::future::pending::<()>(),
            started + Duration::from_millis(10),
            &mut ticks,
            &mut report,
            &f.files,
            started
        )
        .await,
        Err(WaitFailure::Deadline)
    );
    std::fs::create_dir(f.limits.output.join("progress.tmp")).unwrap();
    let mut ticks = time::interval_at(Instant::now(), Duration::from_secs(1));
    report.limits.progress_ms = 1;
    report.last_progress_elapsed_ms = 0;
    assert_eq!(
        wait_stage(
            std::future::pending::<()>(),
            Instant::now() + Duration::from_millis(20),
            &mut ticks,
            &mut report,
            &f.files,
            started
        )
        .await,
        Err(WaitFailure::Artifact)
    );
    assert!(report.artifact_errors.contains(&"progress_write_failed"));
    let temp = tempfile::tempdir().unwrap();
    let output = temp.path().join("config-failed");
    let cli = parse_args_from([
        "--config",
        "/definitely-missing-synthetic-config",
        "--mode",
        "transaction-delivery",
        "--output-dir",
        output.to_str().unwrap(),
        "--duration-ms",
        "30",
        "--json",
    ])
    .unwrap();
    let result = delivery::run(
        &cli,
        cli.delivery.as_ref().unwrap(),
        std::time::Instant::now(),
    )
    .await;
    assert_ne!(result.exit_code(), 0);
    assert!(!result.connect_started);
    let final_report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output.join("final.json")).unwrap()).unwrap();
    assert_eq!(final_report["terminal"], true);
    assert_eq!(final_report["failure"]["stage"], "config");
    assert!(result.error_redacted.is_none());

    let f = Fixture::new(30);
    std::fs::create_dir(f.limits.output.join("final.json")).unwrap();
    let mut state = DeliveryReport::new(&f.limits);
    state.full_window_observed = true;
    state.stop_reason = "duration_limit";
    state.counts.transactions = 1;
    assert_eq!(state.reason_class(), "transaction_delivery_observed");
    let result = delivery::finish(
        state,
        Some(&f.files),
        Instant::now(),
        std::time::Instant::now(),
        None,
    );
    assert_eq!(result.reason_class, "transaction_delivery_artifact_failed");
    assert_ne!(result.exit_code(), 0);
    let nested = result.delivery.as_ref().unwrap();
    assert_eq!(nested["reason_class"], result.reason_class);
    assert_eq!(
        nested["artifact_errors"],
        serde_json::json!(["final_write_failed"])
    );
    assert_eq!(nested["terminal"], true);
}
