use super::super::super::{
    capture_config::CaptureConfig, capture_files::CaptureFiles, capture_persist,
};
use super::*;

pub(super) fn files(temp: &tempfile::TempDir) -> CaptureFiles {
    let mut a = args(&temp.path().join("capture"));
    a.extend(["--config".into(), "unused".into()]);
    let config: CaptureConfig = parse_args_from(a).unwrap().capture.unwrap();
    CaptureFiles::create(&config).unwrap()
}

#[test]
fn buffer_count_n_n_plus_one_and_no_receive_disk_io() {
    let temp = tempfile::tempdir().unwrap();
    let mut files = files(&temp);
    files.config.messages = 2;
    let message = transaction();
    for offset in [10, 11] {
        files.record(&message, offset).unwrap();
    }
    assert_eq!(files.received, 2);
    assert_eq!(files.pending.len(), 2);
    assert_eq!(files.buffered_bytes, 2 * message.encoded_len() as u64);
    assert_eq!(files.written, 0);
    assert_eq!(files.payload_bytes, 0);
    assert_eq!(std::fs::read_dir(&files.config.output).unwrap().count(), 0);
    assert_eq!(files.record(&message, 12), Err("message_count_limit"));
    assert_eq!(files.rows.len(), 2);
    assert_eq!(files.pending[1].1.as_ref(), message.encode_to_vec());
    assert_eq!(files.rows[1]["arrival_offset_ns"], 11);
}

#[test]
fn buffer_byte_full_preserves_refused_row_without_allocating_payload() {
    let temp = tempfile::tempdir().unwrap();
    let mut files = files(&temp);
    let message = transaction();
    let size = message.encoded_len() as u64;
    files.config.total_bytes = files.config.metadata_reserve() + size;
    files.record(&message, 1).unwrap();
    assert_eq!(files.record(&message, 2), Err("total_output_limit"));
    assert_eq!(files.received, 2);
    assert_eq!(files.pending.len(), 1);
    assert_eq!(files.buffered_bytes, size);
    assert_eq!(files.rows[1]["refused"], "total_output_limit");
    assert_eq!(files.rows[1]["saved"], false);
}

#[test]
fn buffer_ignored_metadata_is_count_bounded_and_not_charged_as_payload() {
    let temp = tempfile::tempdir().unwrap();
    let mut files = files(&temp);
    files.config.messages = 2;
    files.record(&ping(), 1).unwrap();
    files.record(&ping(), 2).unwrap();
    assert_eq!(files.record(&ping(), 3), Err("message_count_limit"));
    assert_eq!(files.received, 2);
    assert_eq!(files.kinds["ping"], 2);
    assert_eq!(files.pending.len(), 0);
    assert_eq!(files.buffered_bytes, 0);
    assert_eq!(files.rows.len(), 2);
}

#[tokio::test]
async fn durable_phase_keeps_arrival_source_timestamp_info_and_receive_cutoff() {
    let temp = tempfile::tempdir().unwrap();
    let mut files = files(&temp);
    let mut message = transaction();
    // Deliberately invalid source time is diagnostic data, never normalized.
    message.created_at = Some(yellowstone_grpc_proto::prost_types::Timestamp {
        seconds: -1,
        nanos: -7,
    });
    if let Some(subscribe_update::UpdateOneof::Transaction(tx)) = message.update_oneof.as_mut() {
        tx.transaction = Some(SubscribeUpdateTransactionInfo {
            signature: vec![3; 64],
            index: 17,
            ..Default::default()
        });
    }
    files.record(&message, 9).unwrap();
    let output = files.config.output.clone();
    let session = files.session.clone();
    let result = capture_persist::persist(files, "stream_deadline", json!({}), 10).await;
    let m = &result.capture["manifest"];
    assert_eq!(m["complete"], false);
    assert_eq!(m["receive_stop_reason"], "stream_deadline");
    assert_eq!(m["stop_reason"], "stream_deadline");
    assert_eq!(m["elapsed_ns"], 10);
    assert_eq!(m["receive_elapsed_ns"], 10);
    assert_eq!(m["messages"][0]["arrival_offset_ns"], 9);
    assert_eq!(m["messages"][0]["session_id"], session);
    let raw = std::fs::read(output.join("000001.pb")).unwrap();
    assert_eq!(raw, message.encode_to_vec());
    assert_eq!(SubscribeUpdate::decode(raw.as_slice()).unwrap(), message);
    assert_eq!(m["messages"][0]["sha256"], sha256(&raw));
    assert_eq!(m["payload_bytes"], raw.len());
    assert!(m["persistence_elapsed_ns"].as_u64().unwrap() > 0);
    assert_eq!(result.capture["manifest_persisted"], true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loopback_receive_two_before_first_durable_release() {
    use super::super::super::{
        capture::capture_stream, capture_files::write_new, config::ProbeConfig, report::ProbeReport,
    };
    use std::{sync::mpsc, time::Instant};
    let temp = tempfile::tempdir().unwrap();
    let mut files = files(&temp);
    let output = files.config.output.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(vec![]));
    let subscriptions = Arc::new(AtomicUsize::new(0));
    let server = server::Server {
        messages: vec![transaction(); 2],
        mode: "window-fast",
        output: output.clone(),
        requests: requests.clone(),
        subscriptions: subscriptions.clone(),
    };
    let incoming = futures_util::stream::unfold(listener, |l| async {
        Some((l.accept().await.map(|(s, _)| s), l))
    });
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(geyser_server::GeyserServer::new(server))
            .serve_with_incoming(incoming),
    );
    let config = ProbeConfig {
        grpc_url: format!("http://{address}"),
        x_token: "local".into(),
        connect_timeout_ms: 1000,
        subscribe_timeout_ms: 1000,
        program_ids: vec![RAY.into(), PUMP.into()],
        mode: ProbeMode::AssociationCapture,
    };
    let started = Instant::now();
    let mut report = ProbeReport::failed("test", None, 0);
    let reason = capture_stream(
        &config,
        &mut files,
        &mut report,
        tokio::time::Instant::now() + Duration::from_secs(2),
        started,
    )
    .await;
    assert_eq!(reason, "stream_closed");
    assert_eq!(files.received, 2);
    assert_eq!(files.buffered_bytes, 4);
    assert_eq!(files.pending.len(), 2);
    assert_eq!(files.written, 0);
    assert_eq!(std::fs::read_dir(&output).unwrap().count(), 0);
    let (entered, wait) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    let mut gate = Some(gate);
    let outcome = tokio::spawn(capture_persist::persist_with(
        files,
        reason,
        json!({}),
        started.elapsed().as_nanos() as u64,
        Duration::from_secs(5),
        move |p, b| {
            if let Some(gate) = gate.take() {
                entered.send(b.to_vec()).unwrap();
                gate.recv_timeout(Duration::from_secs(3)).unwrap();
            }
            write_new(p, b)
        },
    ));
    let held =
        tokio::task::spawn_blocking(move || wait.recv_timeout(Duration::from_secs(3)).unwrap())
            .await
            .unwrap();
    assert_eq!(held, transaction().encode_to_vec());
    assert!(!output.join("000001.pb").exists());
    release.send(()).unwrap();
    let result = outcome.await.unwrap();
    assert_eq!(result.capture["manifest"]["messages_received"], 2);
    assert_eq!(result.capture["manifest"]["envelopes_written"], 2);
    assert_eq!(result.capture["manifest"]["payload_bytes"], 4);
    assert_eq!(subscriptions.load(Ordering::SeqCst), 1);
    assert_eq!(
        requests.lock().unwrap()[0],
        build_subscribe_request(ProbeMode::AssociationCapture, &[RAY.into(), PUMP.into()])
    );
    for n in ["000001.pb", "000002.pb"] {
        assert_eq!(
            std::fs::read(output.join(n)).unwrap(),
            transaction().encode_to_vec()
        );
    }
    server.abort();
}
