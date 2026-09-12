use super::*;

pub(super) fn complete() -> PathBuf {
    env_path("B81_ROOT_CAPTURE_DIR").join("buy-native-tx-first")
}
pub(super) fn destination(name: &str) -> PathBuf {
    let root = env_path("B81_R1_OUT_DIR");
    std::fs::create_dir_all(&root).unwrap();
    root.join(name)
}
pub(super) fn copied(name: &str) -> PathBuf {
    let target = destination(name);
    std::fs::create_dir(&target).unwrap();
    for e in std::fs::read_dir(complete()).unwrap() {
        let e = e.unwrap();
        std::fs::copy(e.path(), target.join(e.file_name())).unwrap();
    }
    target
}
pub(super) fn edited(dir: &Path, f: impl FnOnce(&mut Value)) {
    let p = dir.join("manifest.json");
    let mut m: Value = serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
    f(&mut m);
    std::fs::write(p, serde_json::to_vec(&m).unwrap()).unwrap();
}
fn analysis(name: &str) -> Value {
    let c = reader::read(&env_path("B81_ROOT_CAPTURE_DIR").join(name)).unwrap();
    let result = replay::analyze(&c);
    write_json(&destination(&format!("{name}.analysis.json")), &result);
    result
}
fn intact_prefix(name: &str, stop: &str) {
    let r = analysis(name);
    assert_eq!(r["counts"]["raw_transactions"], 1);
    assert_eq!(r["counts"]["checked_swap_subset"], 1);
    assert_eq!(r["counts"]["matched"], 1);
    assert_eq!(r["capture_complete"], false);
    assert_eq!(r["capture_stop_reason"], stop);
    assert_eq!(r["dataset_coverage"], "unknown");
    assert_eq!(r["production_green"], false);
}
#[test]
#[ignore = "actual root count capture regression"]
fn r1_count_cutoff_must_replay() {
    intact_prefix("root-count-cutoff", "message_count_limit");
}
#[test]
#[ignore = "actual root deadline capture regression"]
fn r1_deadline_cutoff_must_replay() {
    intact_prefix("root-deadline-cutoff", "stream_deadline");
}
#[test]
#[ignore = "actual root capture with no original oracle"]
fn r1_capture_without_original_oracle_must_replay() {
    let dir = copied("no-oracle-capture");
    assert!(!dir.join("inputs.json").exists() && !dir.join("scenarios.json").exists());
    let result = replay::analyze(&reader::read(&dir).unwrap());
    assert_eq!(result["counts"]["matched"], 1);
    write_json(&destination("no-oracle.analysis.json"), &result);
}
#[test]
#[ignore = "actual root invalid request regression"]
fn r1_invalid_recorded_request_must_be_refused() {
    let dir = copied("invalid-recorded-request");
    edited(&dir, |m| {
        m["request"]["subscribe_request_base64"] = json!("!!!! invalid protobuf base64 !!!!")
    });
    let reason = reader::read(&dir)
        .err()
        .expect("corrupt request must refuse")
        .to_string();
    assert_eq!(reason, "invalid recorded request base64");
    write_json(
        &destination("invalid-request.refusal.json"),
        &json!({"refusal":reason,"association_started":false}),
    );
}
#[test]
#[ignore = "actual complete control retained"]
fn r1_complete_capture_control_matches() {
    let r = analysis("buy-native-tx-first");
    assert_eq!(r["counts"]["matched"], 1);
    assert_eq!(r["capture_complete"], true);
    assert_eq!(r["capture_stop_reason"], "stream_closed");
}
#[test]
#[ignore = "explicit terminal-prefix policy refusals"]
fn r1_terminal_errors_and_unsaved_inputs_are_refused() {
    let mut results = vec![];
    for stop in [
        "stream_error",
        "connect_failed",
        "connect_deadline",
        "subscribe_open_failed",
        "subscribe_open_deadline",
        "transport_decode_limit_or_resource_error",
        "unexpected_heavy_input",
        "message_byte_limit",
        "total_output_limit",
        "output_io_failure",
        "manifest_io_failure",
        "invented_stop",
    ] {
        let d = copied(&format!("terminal-{stop}"));
        edited(&d, |m| {
            m["stop_reason"] = json!(stop);
            m["complete"] = json!(false);
        });
        let reason = reader::read(&d).err().unwrap().to_string();
        assert!(reason.contains("unsupported terminal capture reason"));
        results.push(json!({"stop_reason":stop,"refusal":reason}));
    }
    let d = copied("unsaved-tx");
    edited(&d, |m| m["messages"][1]["saved"] = json!(false));
    let reason = reader::read(&d).err().unwrap().to_string();
    assert_eq!(reason, "unsaved transaction/block in retained prefix");
    results.push(json!({"unsaved_tx_refusal":reason}));
    write_json(&destination("terminal-policy.json"), &json!(results));
}
#[test]
#[ignore = "actual loopback metadata kinds"]
fn r1_legitimate_metadata_kinds_count_without_association() {
    let d = env_path("B81_R1_OUT_DIR")
        .parent()
        .unwrap()
        .join("new-captures/r1-metadata");
    let c = reader::read(&d).unwrap();
    let r = replay::analyze(&c);
    assert_eq!(r["counts"]["raw_transactions"], 1);
    assert_eq!(r["counts"]["matched"], 1);
    for k in ["ping", "pong", "slot", "block_meta", "transaction_status"] {
        assert_eq!(r["ignored_message_kinds"][k], 1);
    }
    write_json(&destination("metadata.analysis.json"), &r);
}
