//! Closed transient classifications and immutable-clock request bounds.
use super::super::{
    delivery_error::{BrokerFailure, ClientFailure},
    ConfirmedHttpRecovery,
};
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[derive(Clone)]
enum Reply {
    Success,
    Broker(&'static str, &'static str, &'static str, &'static str),
    Delay,
    CutBody,
    MalformedChunk,
    WrongRequest,
    WrongSlot,
}
struct Server {
    url: String,
    calls: Arc<Mutex<Vec<Value>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn serve(replies: Vec<Reply>, deadline_ms: Option<u64>, change: bool) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/private-route", listener.local_addr().unwrap());
    let calls = Arc::new(Mutex::new(vec![]));
    let seen = calls.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = vec![];
            let mut buf = [0u8; 4096];
            let (end, len) = loop {
                let n = socket.read(&mut buf).await.unwrap();
                if n == 0 {
                    break (0, 0);
                };
                bytes.extend_from_slice(&buf[..n]);
                if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let len = String::from_utf8_lossy(&bytes[..pos])
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|s| s.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    break (pos + 4, len);
                }
            };
            if end == 0 {
                continue;
            };
            while bytes.len() < end + len {
                let n = socket.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                };
                bytes.extend_from_slice(&buf[..n]);
            }
            let request: Value = serde_json::from_slice(&bytes[end..end + len]).unwrap();
            let index = {
                let mut calls = seen.lock().unwrap();
                let index = calls.len();
                calls.push(request.clone());
                index
            };
            let reply = replies[index.min(replies.len() - 1)].clone();
            if matches!(reply, Reply::MalformedChunk) {
                let _ = socket.write_all(b"HTTP/1.1 200 Fixture\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\nINVALID\r\n").await;
                continue;
            }
            let (status, mut body) = match reply {
                Reply::Broker(kind, stage, reason, cause) => (
                    502,
                    json!({"broker_error":{"schema":"http_recovery_broker_v1",
                    "kind":kind,"method":"getBlocks","reservation_id":index+1,"stage":stage,"reason":reason,
                    "cause_type":cause,"http_status":null,"verify_code":null}}),
                ),
                Reply::WrongRequest | Reply::WrongSlot => (
                    502,
                    json!({"broker_error":{
                    "schema":"http_recovery_broker_v1","kind":"failed","method":"getBlocks",
                    "reservation_id":1,"stage":"upstream_body","reason":"http_protocol",
                    "cause_type":"IncompleteRead","http_status":null,"verify_code":null}}),
                ),
                _ => (
                    200,
                    json!({"jsonrpc":"2.0","id":request["id"],"result":[10]}),
                ),
            };
            if matches!(reply, Reply::WrongRequest) {
                body["broker_error"]["request_id"] = json!(999);
            }
            if matches!(reply, Reply::WrongSlot) {
                body["broker_error"]["slot"] = json!(999);
            }
            if matches!(reply, Reply::Delay) {
                tokio::time::sleep(Duration::from_millis(90)).await;
            }
            let body = serde_json::to_vec(&body).unwrap();
            let clock = deadline_ms
                .map(|n| {
                    format!(
                        "X-Copybot-Session-Deadline-Unix-Ms: {}\r\n",
                        n + u64::from(change && index > 0)
                    )
                })
                .unwrap_or_default();
            let length = body.len() + usize::from(matches!(reply, Reply::CutBody)) * 100;
            let header=format!("HTTP/1.1 {status} Fixture\r\nContent-Length: {length}\r\nConnection: close\r\n{clock}\r\n");
            let _ = socket.write_all(header.as_bytes()).await;
            let _ = socket.write_all(&body).await;
        }
    });
    Server { url, calls, task }
}
fn make_client(server: &Server, millis: u64) -> ConfirmedHttpRecovery {
    ConfirmedHttpRecovery::new(
        &server.url,
        Some(("X-Copybot-Broker-Token", "PRIVATE_HEADER_TOKEN")),
        1024,
        10000,
        Duration::from_millis(millis),
    )
    .unwrap()
}
fn transient() -> Reply {
    Reply::Broker("failed", "upstream_body", "http_protocol", "IncompleteRead")
}
fn epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[tokio::test]
async fn transient_broker_read_retries_fresh_ids_and_preserves_reservation_failure() {
    let server = serve(vec![transient(), Reply::Success], None, false).await;
    assert_eq!(
        make_client(&server, 1000).slots(10, 11).await.unwrap(),
        vec![10]
    );
    let calls = server.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0]["id"], 1);
    assert_eq!(calls[1]["id"], 2);
    assert_eq!(calls[0]["params"], calls[1]["params"]);
}
#[tokio::test]
async fn broker_attempt_exhaustion_stops_at_three_and_keeps_typed_last_cause() {
    let server = serve(vec![transient()], None, false).await;
    let error = make_client(&server, 1000).slots(10, 11).await.unwrap_err();
    let cause = error.downcast_ref::<BrokerFailure>().unwrap();
    assert_eq!(cause.reservation, Some(3));
    assert_eq!(cause.cause, "IncompleteRead");
    assert_eq!(server.calls.lock().unwrap().len(), 3);
}
#[tokio::test]
async fn reqwest_timeout_and_partial_body_are_distinct_typed_causes() {
    for (reply, timeout, expected) in [(Reply::Delay, 25, true), (Reply::CutBody, 1000, false)] {
        let server = serve(vec![reply], None, false).await;
        let error = make_client(&server, timeout)
            .slots(10, 11)
            .await
            .unwrap_err();
        let cause = error.downcast_ref::<ClientFailure>().unwrap();
        eprintln!("TYPED_CONTROL {cause}");
        assert_eq!(cause.timeout, expected);
        assert_eq!(cause.method, "getBlocks");
        assert_eq!(cause.slot, 10);
        assert_eq!(cause.attempt, 3);
        if !expected {
            assert_eq!(cause.io_kind, Some(std::io::ErrorKind::UnexpectedEof));
            assert_eq!(cause.stage, "client_body");
        }
        let displayed = format!("{error:?}");
        assert!(!displayed.contains("PRIVATE_HEADER_TOKEN"));
        assert!(!error.to_string().contains("private-route"));
        assert!(!error.to_string().contains("http://"));
        assert_eq!(server.calls.lock().unwrap().len(), 3);
    }
}
#[tokio::test]
async fn refusals_and_unknown_or_tls_causes_never_retry() {
    for reply in [
        Reply::Broker("refused", "gate", "http_or_rpc_cap_exhausted", "Refused"),
        Reply::Broker(
            "failed",
            "tls",
            "tls_certificate",
            "SSLCertVerificationError",
        ),
        Reply::Broker(
            "failed",
            "upstream_body",
            "upstream_unavailable",
            "UnknownFailure",
        ),
        Reply::Broker(
            "failed",
            "transport",
            "session_deadline_exhausted",
            "DeadlineExceededTimeout",
        ),
    ] {
        let server = serve(vec![reply], None, false).await;
        let error = make_client(&server, 1000).slots(10, 11).await.unwrap_err();
        assert!(!error.downcast_ref::<BrokerFailure>().unwrap().retryable());
        assert_eq!(server.calls.lock().unwrap().len(), 1);
    }
}
#[tokio::test]
async fn unclassified_body_protocol_failure_is_terminal() {
    let server = serve(vec![Reply::MalformedChunk], None, false).await;
    let error = make_client(&server, 1000).slots(10, 11).await.unwrap_err();
    let cause = error.downcast_ref::<ClientFailure>().unwrap();
    assert_eq!(cause.stage, "client_body");
    assert!(!cause.retryable());
    assert_eq!(server.calls.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn original_session_deadline_stops_backoff_and_cannot_change() {
    let server = serve(
        vec![Reply::Success, transient()],
        Some(epoch_ms() + 150),
        false,
    )
    .await;
    let client = make_client(&server, 1000);
    client.slots(10, 11).await.unwrap();
    let error = client.slots(10, 11).await.unwrap_err();
    assert!(error.to_string().contains("session_deadline_exhausted"));
    assert_eq!(server.calls.lock().unwrap().len(), 2);
    let server = serve(vec![Reply::Success], Some(epoch_ms() + 10000), true).await;
    let client = make_client(&server, 1000);
    client.slots(10, 11).await.unwrap();
    assert!(client
        .slots(10, 11)
        .await
        .unwrap_err()
        .to_string()
        .contains("session_deadline_changed"));
    assert_eq!(server.calls.lock().unwrap().len(), 2);
}
#[tokio::test]
async fn original_session_deadline_clips_inflight_request_without_losing_timeout_type() {
    let server = serve(
        vec![Reply::Success, Reply::Delay],
        Some(epoch_ms() + 75),
        false,
    )
    .await;
    let client = make_client(&server, 15000);
    client.slots(10, 11).await.unwrap();
    let began = std::time::Instant::now();
    let error = client.slots(10, 11).await.unwrap_err();
    assert!(error.to_string().contains("session_deadline_exhausted"));
    let cause = error.downcast_ref::<ClientFailure>().unwrap();
    assert!(cause.timeout);
    assert!(cause.deadline_ms <= 75);
    assert!(began.elapsed() < Duration::from_millis(250));
    assert_eq!(server.calls.lock().unwrap().len(), 2);
}
#[tokio::test]
async fn broker_request_and_slot_identity_fail_before_retry() {
    for (reply, expected) in [
        (Reply::WrongRequest, "broker_request_identity"),
        (Reply::WrongSlot, "broker_slot_identity"),
    ] {
        let server = serve(vec![reply], None, false).await;
        let error = make_client(&server, 1000).slots(10, 11).await.unwrap_err();
        assert!(error.to_string().contains(expected));
        assert_eq!(server.calls.lock().unwrap().len(), 1);
    }
}
#[tokio::test]
async fn financial_method_cannot_enter_read_only_retry_path() {
    let server = serve(vec![Reply::Success], None, false).await;
    let error = make_client(&server, 1000)
        .request("sendTransaction", json!(["UNSIGNED_TEST_ONLY"]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("read_only_method_required"));
    assert_eq!(server.calls.lock().unwrap().len(), 0);
}
