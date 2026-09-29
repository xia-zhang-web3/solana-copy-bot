use super::super::{response, ConfirmedHttpRecovery};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

async fn one_response(status: u16, envelope: Value) -> (String, tokio::task::JoinHandle<Value>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/rpc", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let end = loop {
            let mut chunk = [0u8; 4096];
            let n = socket.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&chunk[..n]);
            if let Some(i) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
                break i + 4;
            }
        };
        let length: usize = String::from_utf8_lossy(&bytes[..end])
            .lines()
            .find_map(|l| {
                l.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(|v| v.trim().parse().unwrap())
            })
            .unwrap();
        while bytes.len() - end < length {
            let mut chunk = [0u8; 4096];
            let n = socket.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&chunk[..n]);
        }
        let request = serde_json::from_slice(&bytes[end..end + length]).unwrap();
        let body = serde_json::to_vec(&envelope).unwrap();
        socket
            .write_all(
                format!(
                    "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        socket.write_all(&body).await.unwrap();
        request
    });
    (url, task)
}

fn broker(kind: &str, reason: &str, reservation: Value) -> Value {
    json!({"broker_error":{"schema":"http_recovery_broker_v1","kind":kind,"method":"getBlocks",
        "reservation_id":reservation,"stage":"tls","reason":reason,
        "cause_type":"SSLCertVerificationError","http_status":null,"verify_code":20}})
}

#[tokio::test]
async fn actual_adapter_preserves_tls_failure_and_does_not_retry() {
    let (url, task) = one_response(502, broker("failed", "tls_certificate", json!(7))).await;
    let client =
        ConfirmedHttpRecovery::new(&url, None, 1024, 1000, Duration::from_secs(2)).unwrap();
    let error = client.slots(10, 11).await.unwrap_err().to_string();
    for field in [
        "http_recovery_broker_error",
        "stage=tls",
        "reason=tls_certificate",
        "cause_type=SSLCertVerificationError",
        "broker_http_status=502",
        "upstream_http_status=unknown",
        "reservation_id=7",
        "verify_code=20",
    ] {
        assert!(error.contains(field), "missing {field}: {error}");
    }
    assert!(!error.contains("response_identity"));
    assert_eq!(task.await.unwrap()["method"], "getBlocks");
}

#[tokio::test]
async fn actual_adapter_keeps_gate_refusal_with_no_reservation() {
    let mut envelope = broker("refused", "read_only_clock_or_lease_invalid", Value::Null);
    envelope["broker_error"]["stage"] = json!("gate");
    envelope["broker_error"]["cause_type"] = json!("Refused");
    let (url, task) = one_response(429, envelope).await;
    let client =
        ConfirmedHttpRecovery::new(&url, None, 1024, 1000, Duration::from_secs(2)).unwrap();
    let error = client.slots(10, 11).await.unwrap_err().to_string();
    assert!(error.contains("kind=refused stage=gate reason=read_only_clock_or_lease_invalid"));
    assert!(error.contains("reservation_id=unknown"));
    task.await.unwrap();
}

#[tokio::test]
async fn actual_adapter_keeps_http_status_and_strict_rpc_identity() {
    for (status, envelope, expected) in [
        (
            503,
            json!({"error":"private unstructured body"}),
            "http_recovery_http_status 503",
        ),
        (
            500,
            json!({"jsonrpc":"2.0","id":1,"error":{"code":-32004,"message":"secret"}}),
            "http_recovery_rpc_error http_status=500 code=-32004",
        ),
        (
            200,
            json!({"jsonrpc":"2.0","id":2,"result":[10]}),
            "http_recovery_response_identity",
        ),
        (
            200,
            json!({"jsonrpc":"2.0","id":2,"error":{"code":-32004}}),
            "http_recovery_response_identity",
        ),
    ] {
        let (url, task) = one_response(status, envelope).await;
        let client =
            ConfirmedHttpRecovery::new(&url, None, 1024, 1000, Duration::from_secs(2)).unwrap();
        let error = client.slots(10, 11).await.unwrap_err().to_string();
        assert!(error.contains(expected), "{error}");
        assert!(!error.contains("secret"));
        task.await.unwrap();
    }
}

#[test]
fn diagnostics_do_not_echo_body_secrets_or_trust_malformed_metadata() {
    let mut envelope = broker("failed", "https://private.invalid/token", json!(1));
    envelope["broker_error"]["cause_type"] = json!("Authorization-Bearer-secret");
    envelope["broker_error"]["stage"] = json!("private-stage-secret");
    let error = response::interpret(502, 1, "getBlocks", &serde_json::to_vec(&envelope).unwrap())
        .unwrap_err()
        .to_string();
    assert!(error.contains("reason=unclassified_failure cause_type=UnknownFailure"));
    assert!(!error.contains("private") && !error.contains("secret") && !error.contains("https"));
    envelope["broker_error"]["method"] = json!("getBlock");
    assert!(
        response::interpret(502, 1, "getBlocks", &serde_json::to_vec(&envelope).unwrap())
            .unwrap_err()
            .to_string()
            .contains("broker_method_identity")
    );
    envelope["broker_error"]["method"] = json!("getBlocks");
    envelope["broker_error"]["reservation_id"] = json!(0);
    assert!(
        response::interpret(502, 1, "getBlocks", &serde_json::to_vec(&envelope).unwrap()).is_err()
    );
}

#[test]
fn successful_envelope_and_rpc_error_remain_strict() {
    assert_eq!(
        response::interpret(
            200,
            1,
            "getBlocks",
            br#"{"jsonrpc":"2.0","id":1,"result":[10]}"#
        )
        .unwrap(),
        json!([10])
    );
    let error=response::interpret(200,1,"getBlocks",br#"{"jsonrpc":"2.0","id":1,"error":{"code":-32004,"message":"https://secret.invalid/key"}}"#).unwrap_err().to_string();
    assert_eq!(error, "http_recovery_rpc_error http_status=200 code=-32004");
    assert!(response::interpret(
        500,
        1,
        "getBlocks",
        br#"{"jsonrpc":"2.0","id":2,"error":{"code":-32004}}"#
    )
    .unwrap_err()
    .to_string()
    .contains("response_identity"));
}

struct LocalBroker {
    process: std::process::Child,
    directory: std::path::PathBuf,
}
impl Drop for LocalBroker {
    fn drop(&mut self) {
        let _ = std::fs::write(self.directory.join("STOP_TEST"), b"stopped");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if self.process.try_wait().ok().flatten().is_some() {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

/// Explicit local check: requires Python/OpenSSL tooling, never a provider.
#[tokio::test]
#[ignore = "explicit local HTTPS broker boundary check"]
async fn actual_adapter_to_real_broker_and_local_https() {
    let repository = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let root = std::env::var("COPYBOT_HTTP_TLS_EVIDENCE")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| {
            std::env::temp_dir().join(format!("copybot-http-tls-{}", std::process::id()))
        });
    std::fs::create_dir_all(&root).unwrap();
    let python = std::env::var("COPYBOT_HTTP_TLS_PYTHON").unwrap_or_else(|_| "python3".to_owned());
    for mode in ["trusted", "untrusted", "hostname", "status", "wrong-id"] {
        let directory = root.join(mode);
        std::fs::create_dir(&directory).unwrap();
        let process = std::process::Command::new(&python)
            .arg("-B")
            .arg(repository.join("tools/tests/http_recovery_tls_server.py"))
            .args(["--serve", "--mode", mode, "--directory"])
            .arg(&directory)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let mut broker = LocalBroker {
            process,
            directory: directory.clone(),
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while !directory.join("ready.json").exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "fixture startup timeout {mode}"
            );
            assert!(
                broker.process.try_wait().unwrap().is_none(),
                "fixture exited {mode}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let ready: Value =
            serde_json::from_slice(&std::fs::read(directory.join("ready.json")).unwrap()).unwrap();
        let client = ConfirmedHttpRecovery::new(
            ready["front_url"].as_str().unwrap(),
            None,
            1024,
            8388608,
            Duration::from_secs(5),
        )
        .unwrap();
        let outcome = match client.slots(123, 124).await {
            Ok(slots) => {
                assert_eq!(mode, "trusted");
                assert_eq!(slots, vec![123]);
                "success".to_owned()
            }
            Err(error) => {
                let text = error.to_string();
                let expected = match mode {
                    "untrusted" => "reason=tls_certificate cause_type=SSLCertVerificationError",
                    "hostname" => "reason=tls_hostname cause_type=SSLCertVerificationError",
                    "status" => "reason=http_status cause_type=HTTPStatus",
                    "wrong-id" => "http_recovery_response_identity",
                    _ => panic!("trusted failed: {text}"),
                };
                assert!(text.contains(expected), "{mode}: {text}");
                assert!(!text.contains("FAKE_OFFLINE") && !text.contains("https://"));
                if mode != "wrong-id" {
                    assert!(text.contains("reservation_id=1"));
                }
                text
            }
        };
        std::fs::write(
            directory.join("RUST_ADAPTER_RESULT.json"),
            serde_json::to_vec_pretty(&json!({"mode":mode,"outcome":outcome,"requests":1}))
                .unwrap(),
        )
        .unwrap();
        drop(broker);
    }
}
