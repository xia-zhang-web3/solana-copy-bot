use super::*;

#[test]
#[ignore = "requires actual CLI files for deliberate corruption controls"]
fn capture_replay_rejects_tampered_evidence() {
    let captures = env_path("B81_CAPTURE_DIR");
    let case = "buy-native-tx-first";
    let dir = captures.join(case);
    let mutations = env_path("B81_REPLAY_OUT").with_extension("integrity");
    std::fs::create_dir(&mutations).unwrap();
    let mut outcomes = vec![];
    for field in [
        "sha256",
        "encoded_bytes",
        "sequence",
        "session_id",
        "arrival_offset_ns",
        "kind",
        "payload_bytes",
        "protobuf",
        "request",
    ] {
        let target = mutations.join(field);
        std::fs::create_dir(&target).unwrap();
        for entry in std::fs::read_dir(&dir).unwrap() {
            let entry = entry.unwrap();
            std::fs::copy(entry.path(), target.join(entry.file_name())).unwrap();
        }
        let mut m: Value =
            serde_json::from_slice(&std::fs::read(target.join("manifest.json")).unwrap()).unwrap();
        match field {
            "payload_bytes" => m[field] = json!(0),
            "protobuf" => std::fs::write(target.join("000002.pb"), b"broken").unwrap(),
            "request" => m["request"]["blocks"]["include_accounts"] = json!(true),
            "sha256" => m["messages"][1][field] = json!("00"),
            "session_id" => m["messages"][1][field] = json!("another session"),
            "kind" => m["messages"][1][field] = json!("block"),
            _ => m["messages"][1][field] = json!(0),
        }
        std::fs::write(
            target.join("manifest.json"),
            serde_json::to_vec(&m).unwrap(),
        )
        .unwrap();
        let refused = reader::read(&target).is_err();
        outcomes.push(json!({"mutation":field,"refused":refused}));
        assert!(refused, "{field}");
    }
    write_json(&mutations.join("outcomes.json"), &json!(outcomes));
}
