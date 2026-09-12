use super::super::super::capture_config::CaptureProfile;
use super::*;

fn profile_args() -> Vec<String> {
    let mut a = args(Path::new("fresh-profile"));
    a.extend(["--config".into(), "unused.toml".into()]);
    a.extend(["--capture-profile".into(), "diagnostic-v1".into()]);
    a
}

#[test]
fn profile_explicit_opt_in_bounds_and_selected_transport_are_exact() {
    let mut a = profile_args();
    replace(&mut a, "--max-message-bytes", 8_388_608);
    replace(&mut a, "--max-total-bytes", 67_108_864);
    let c = parse_args_from(a.clone()).unwrap().capture.unwrap();
    assert_eq!(c.profile, CaptureProfile::DiagnosticV1);
    assert_eq!(c.transport_bytes(), 8_388_608);
    assert_eq!(c.total_bytes, 67_108_864);
    for (key, n) in [
        ("--max-message-bytes", 8_388_609),
        ("--max-total-bytes", 67_108_865),
        ("--duration-ms", 60_001),
        ("--max-messages", 257),
        ("--max-message-bytes", 0),
        ("--max-total-bytes", 262_144),
    ] {
        let mut invalid = a.clone();
        replace(&mut invalid, key, n);
        assert!(parse_args_from(invalid).is_err(), "{key}");
    }
    replace(&mut a, "--max-message-bytes", 2_097_152);
    replace(&mut a, "--max-total-bytes", 16_777_216);
    let c = parse_args_from(a).unwrap().capture.unwrap();
    assert_eq!(c.transport_bytes(), c.message_bytes as usize);
    assert_eq!(c.transport_bytes(), 2_097_152);
}

#[test]
fn profile_absence_unknown_and_legacy_lower_cap_keep_original_contract() {
    let mut legacy = args(Path::new("fresh-legacy"));
    legacy.extend(["--config".into(), "unused.toml".into()]);
    let c = parse_args_from(legacy.clone()).unwrap().capture.unwrap();
    assert_eq!(c.profile, CaptureProfile::Legacy);
    assert_eq!(c.transport_bytes(), 1_048_576);
    assert_eq!(c.total_bytes, 16_777_216);
    for (key, n) in [
        ("--max-message-bytes", 1_048_577),
        ("--max-total-bytes", 16_777_217),
    ] {
        let mut invalid = legacy.clone();
        replace(&mut invalid, key, n);
        assert!(parse_args_from(invalid).is_err());
    }
    replace(&mut legacy, "--max-message-bytes", 100);
    let c = parse_args_from(legacy).unwrap().capture.unwrap();
    assert_eq!(c.message_bytes, 100);
    assert_eq!(c.transport_bytes(), 1_048_576);
    for value in ["", "legacy", "diagnostic-v2", "DIAGNOSTIC-V1", "null"] {
        let mut a = profile_args();
        replace(&mut a, "--capture-profile", value);
        assert!(parse_args_from(a).is_err());
    }
    let mut duplicate = profile_args();
    duplicate.extend(["--capture-profile".into(), "diagnostic-v1".into()]);
    assert!(parse_args_from(duplicate).is_err());
    let mut other_mode = profile_args();
    replace(&mut other_mode, "--mode", "slots-only");
    assert!(parse_args_from(other_mode).is_err());
}

fn sized_transaction(size: usize) -> SubscribeUpdate {
    let mut message = transaction();
    message.filters = vec!["x".repeat(size - 6)];
    assert_eq!(message.encoded_len(), size);
    message
}

#[tokio::test]
#[ignore = "requires actual local probe binary and fresh capture directory"]
async fn actual_cli_profile_above_legacy_and_selected_n_n_plus_one() {
    for (name, size, accepted) in [
        ("profile-above-legacy", 1_048_577, true),
        ("profile-selected-n", 2_097_152, true),
        ("profile-selected-n-plus-one", 2_097_153, false),
    ] {
        let message = sized_transaction(size);
        let result = cli_case_profile(
            name,
            vec![message.clone()],
            "close",
            &[("--max-message-bytes", 2_097_152)],
            Some("diagnostic-v1"),
        )
        .await;
        let m = &result["capture"]["manifest"];
        assert_eq!(m["capture_profile"], "diagnostic-v1");
        assert_eq!(m["transport_decode_bytes"], 2_097_152);
        assert_eq!(m["limits"]["message_bytes"], 2_097_152);
        assert_eq!(m["limits"]["total_bytes"], 16_777_216);
        assert_eq!(m["metadata_reserve_bytes"], 262_144);
        if accepted {
            assert_eq!(m["stop_reason"], "stream_closed");
            assert_eq!(m["complete"], true);
            assert_eq!(m["payload_bytes"], size);
            assert_eq!(m["messages"][0]["sha256"], sha256(&message.encode_to_vec()));
            assert_eq!(
                std::fs::read(root().join(name).join("000001.pb")).unwrap(),
                message.encode_to_vec()
            );
        } else {
            assert_eq!(m["stop_reason"], "transport_decode_limit_or_resource_error");
            assert_eq!(m["complete"], false);
            assert_eq!(m["envelopes_written"], 0);
            assert_eq!(m["terminal_status"]["message_length_bytes"], size);
            assert_eq!(m["terminal_status"]["message_limit_bytes"], 2_097_152);
            assert_eq!(
                m["terminal_status"]["configured_transport_limit_bytes"],
                2_097_152
            );
            assert_eq!(m["terminal_status"]["origin"], "unknown");
        }
    }
}
