use super::*;

#[tokio::test]
#[ignore = "requires explicit frozen05 directory and actual local probe binary"]
async fn frozen05_burst_all_1268_payloads_hashes_and_source_fields() {
    let frozen = PathBuf::from(std::env::var("B87_FROZEN05_DIR").unwrap());
    let original: Value =
        serde_json::from_slice(&std::fs::read(frozen.join("manifest.json")).unwrap()).unwrap();
    let rows = original["messages"].as_array().unwrap();
    assert_eq!(rows.len(), 1268);
    let mut messages = Vec::new();
    for row in rows {
        assert_eq!(row["saved"], true);
        let raw = std::fs::read(frozen.join(row["file"].as_str().unwrap())).unwrap();
        assert_eq!(row["sha256"], sha256(&raw));
        assert_eq!(row["encoded_bytes"], raw.len());
        let message = SubscribeUpdate::decode(raw.as_slice()).unwrap();
        assert_eq!(message.encode_to_vec(), raw);
        messages.push(message);
    }
    let r = cli_case_profile(
        "frozen05-burst",
        messages,
        "window-fast",
        &[
            ("--duration-ms", 10000),
            ("--max-messages", 4096),
            ("--max-message-bytes", 8_388_608),
            ("--max-total-bytes", 67_108_864),
        ],
        Some("window-v1"),
    )
    .await;
    let captured = &r["capture"]["manifest"];
    assert_eq!(captured["stop_reason"], "stream_closed");
    assert_eq!(captured["messages_received"], 1268);
    assert_eq!(captured["envelopes_written"], 1268);
    assert_eq!(captured["payload_bytes"], 22_485_026);
    assert_eq!(captured["buffered_encoded_bytes"], 22_485_026);
    let mut previous = 0;
    for (row, saved) in rows.iter().zip(captured["messages"].as_array().unwrap()) {
        for key in ["sequence", "kind", "encoded_bytes", "sha256", "file"] {
            assert_eq!(row[key], saved[key], "{key}");
        }
        let file = row["file"].as_str().unwrap();
        let raw = std::fs::read(root().join("frozen05-burst").join(file)).unwrap();
        assert_eq!(raw, std::fs::read(frozen.join(file)).unwrap());
        let arrival = saved["arrival_offset_ns"].as_u64().unwrap();
        assert!(arrival >= previous && arrival <= captured["receive_elapsed_ns"].as_u64().unwrap());
        previous = arrival;
    }
    println!(
        "frozen05 exact: 1268/1268 payloads, 22485026 bytes; receive_ns={} persistence_ns={}",
        captured["receive_elapsed_ns"], r["capture"]["persistence_wall_elapsed_ns"]
    );
}
