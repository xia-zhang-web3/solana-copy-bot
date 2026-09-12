use super::*;
use tonic::{Code, Status};

pub(super) fn server_status(mode: &str) -> Option<Status> {
    let (code, message) = match mode {
        "terminal-range" => (Code::OutOfRange, "synthetic-secret https://private-query.invalid"),
        "terminal-resource" => (Code::ResourceExhausted, "synthetic-secret quota unproven"),
        "terminal-unknown" => (Code::Unknown, "synthetic-secret private-query"),
        "terminal-data-loss" => (Code::DataLoss, "synthetic-secret private-query"),
        "terminal-decode-spoof" => (
            Code::OutOfRange,
            "Error, decoded message length too large: found 1048583 bytes, the limit is: 1048576 bytes",
        ),
        "terminal-decompress-spoof" => (
            Code::ResourceExhausted,
            "Error decompressing: size limit, of 1048576 bytes, exceeded while decompressing message",
        ),
        _ => return None,
    };
    let mut status =
        Status::with_details(code, message, b"synthetic-secret details".to_vec().into());
    status
        .metadata_mut()
        .insert("x-token", "synthetic-secret".parse().unwrap());
    Some(status)
}

fn assert_terminal(result: &Value, code: i32, category: &str, stage: &str) {
    let manifest = &result["capture"]["manifest"];
    let terminal = &manifest["terminal_status"];
    assert_eq!(terminal["stage"], stage);
    assert_eq!(terminal["status_code"], code);
    assert_eq!(terminal["category"], category);
    assert_eq!(terminal["origin"], "unknown");
    assert_eq!(terminal["configured_transport_limit_bytes"], 1_048_576);
    assert_eq!(manifest["transport_decode_bytes"], 1_048_576);
    assert_eq!(manifest["limits"]["messages"], 256);
    assert_eq!(manifest["limits"]["message_bytes"], 1_048_576);
    assert_eq!(manifest["limits"]["total_bytes"], 16_777_216);
    assert_eq!(manifest["complete"], false);
    assert!(serde_json::to_vec(terminal).unwrap().len() < 512);
}

#[tokio::test]
#[ignore = "requires actual local probe binary and fresh evidence directory"]
async fn actual_cli_terminal_oversized_preserves_safe_status_and_prefix() {
    let prefix = transaction();
    let mut oversized = ping();
    oversized.filters = vec!["x".repeat(1_048_577)];
    let length = oversized.encoded_len() as u64;
    let result = cli_case(
        "terminal-oversized",
        vec![prefix.clone(), oversized],
        "close",
        &[],
    )
    .await;
    assert_terminal(&result, 11, "tonic_0_14_4_decode_size_shape", "stream_next");
    let manifest = &result["capture"]["manifest"];
    assert_eq!(
        manifest["stop_reason"],
        "transport_decode_limit_or_resource_error"
    );
    assert_eq!(manifest["terminal_status"]["message_length_bytes"], length);
    assert_eq!(
        manifest["terminal_status"]["message_limit_bytes"],
        1_048_576
    );
    assert_eq!(manifest["messages_received"], 1);
    assert_eq!(manifest["envelopes_written"], 1);
    assert_eq!(
        manifest["messages"][0]["sha256"],
        sha256(&prefix.encode_to_vec())
    );
    assert_eq!(
        std::fs::read(root().join("terminal-oversized/000001.pb")).unwrap(),
        prefix.encode_to_vec()
    );
}

#[tokio::test]
#[ignore = "requires actual local probe binary and fresh evidence directory"]
async fn actual_cli_terminal_server_codes_and_spoofs_remain_unknown() {
    for (mode, code, category) in [
        ("terminal-range", 11, "range_or_resource_status"),
        ("terminal-resource", 8, "range_or_resource_status"),
        ("terminal-unknown", 2, "other_status"),
        ("terminal-data-loss", 15, "other_status"),
        (
            "terminal-decode-spoof",
            11,
            "tonic_0_14_4_decode_size_shape",
        ),
        (
            "terminal-decompress-spoof",
            8,
            "tonic_0_14_4_decompress_limit_shape",
        ),
    ] {
        let prefix = transaction();
        let result = cli_case(mode, vec![prefix.clone()], mode, &[]).await;
        assert_terminal(&result, code, category, "stream_next");
        let manifest = &result["capture"]["manifest"];
        assert_eq!(manifest["messages_received"], 1);
        assert_eq!(manifest["envelopes_written"], 1);
        assert_eq!(
            manifest["messages"][0]["sha256"],
            sha256(&prefix.encode_to_vec())
        );
        assert_eq!(
            manifest["stop_reason"],
            if code == 8 || code == 11 {
                "transport_decode_limit_or_resource_error"
            } else {
                "stream_error"
            }
        );
        if mode == "terminal-decode-spoof" {
            assert_eq!(
                manifest["terminal_status"]["message_length_bytes"],
                1_048_583
            );
        } else {
            assert!(manifest["terminal_status"]["message_length_bytes"].is_null());
        }
    }
}

#[tokio::test]
#[ignore = "requires actual local probe binary and fresh evidence directory"]
async fn actual_cli_terminal_open_error_and_healthy_controls() {
    let result = cli_case("terminal-open", vec![], "open-error", &[]).await;
    assert_terminal(&result, 7, "other_status", "subscribe_open");
    assert_eq!(
        result["capture"]["manifest"]["stop_reason"],
        "subscribe_open_failed"
    );
    let result = cli_case(
        "terminal-internal",
        vec![transaction()],
        "stream-error",
        &[],
    )
    .await;
    assert_terminal(&result, 13, "other_status", "stream_next");
    for (name, mode, changes, reason) in [
        ("terminal-healthy", "close", vec![], "stream_closed"),
        (
            "terminal-count",
            "close",
            vec![("--max-messages", 1)],
            "message_count_limit",
        ),
        (
            "terminal-deadline",
            "silent",
            vec![("--duration-ms", 250)],
            "stream_deadline",
        ),
    ] {
        let result = cli_case(name, vec![transaction()], mode, &changes).await;
        let manifest = &result["capture"]["manifest"];
        assert!(manifest["terminal_status"].is_null());
        assert_eq!(manifest["stop_reason"], reason);
        assert_eq!(manifest["complete"], reason == "stream_closed");
    }
}
