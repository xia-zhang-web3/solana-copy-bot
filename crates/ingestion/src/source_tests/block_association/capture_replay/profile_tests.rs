use super::*;

fn manifest() -> Value {
    json!({"schema":1,"mode":"association-capture","production_green":false,
        "stop_reason":"stream_closed","complete":true,
        "capture_profile":"diagnostic-v1","transport_decode_bytes":2_097_152,
        "metadata_reserve_bytes":262_144,
        "limits":{"duration_ms":60_000,"messages":256,"message_bytes":2_097_152,
            "total_bytes":16_777_216},
        "session_id":"00000000000000000000000000000000","messages_received":0,
        "elapsed_ns":1,"provider_coverage":"unmeasured","association_verdict":"not_evaluated"})
}

#[test]
fn profile_manifest_explicit_bounds_preserve_legacy_and_diagnostic_n() {
    let mut m = manifest();
    assert!(manifest::validate(&m).is_ok());
    m["transport_decode_bytes"] = json!(8_388_608);
    m["limits"]["message_bytes"] = json!(8_388_608);
    m["limits"]["total_bytes"] = json!(67_108_864);
    assert!(manifest::validate(&m).is_ok());
    m.as_object_mut().unwrap().remove("capture_profile");
    m["transport_decode_bytes"] = json!(1_048_576);
    m["limits"]["message_bytes"] = json!(1_048_576);
    m["limits"]["total_bytes"] = json!(16_777_216);
    assert!(manifest::validate(&m).is_ok());
    m["limits"]["message_bytes"] = json!(100);
    assert!(manifest::validate(&m).is_ok());
}

#[test]
fn profile_manifest_missing_unknown_mismatch_and_n_plus_one_refuse() {
    for name in [
        "missing",
        "unknown",
        "null",
        "object",
        "transport",
        "message-cap",
        "total-cap",
        "duration",
        "messages",
        "reserve",
        "zero",
        "legacy-message",
        "legacy-total",
    ] {
        let mut m = manifest();
        match name {
            "missing" => {
                m.as_object_mut().unwrap().remove("capture_profile");
            }
            "unknown" => m["capture_profile"] = json!("diagnostic-v2"),
            "null" => m["capture_profile"] = Value::Null,
            "object" => m["capture_profile"] = json!({"name":"diagnostic-v1"}),
            "transport" => m["transport_decode_bytes"] = json!(1_048_576),
            "message-cap" => {
                m["limits"]["message_bytes"] = json!(8_388_609);
                m["transport_decode_bytes"] = json!(8_388_609);
            }
            "total-cap" => m["limits"]["total_bytes"] = json!(67_108_865),
            "duration" => m["limits"]["duration_ms"] = json!(60_001),
            "messages" => m["limits"]["messages"] = json!(257),
            "reserve" => m["metadata_reserve_bytes"] = json!(262_145),
            "zero" => {
                m["limits"]["message_bytes"] = json!(0);
                m["transport_decode_bytes"] = json!(0);
            }
            "legacy-message" | "legacy-total" => {
                m.as_object_mut().unwrap().remove("capture_profile");
                m["transport_decode_bytes"] = json!(1_048_576);
                m["limits"]["message_bytes"] = json!(if name == "legacy-message" {
                    1_048_577
                } else {
                    1_048_576
                });
                m["limits"]["total_bytes"] = json!(if name == "legacy-total" {
                    16_777_217
                } else {
                    16_777_216
                });
            }
            _ => unreachable!(),
        }
        assert!(manifest::validate(&m).is_err(), "{name}");
    }
}

#[test]
fn profile_manifest_does_not_admit_terminal_prefix() {
    let mut m = manifest();
    m["stop_reason"] = json!("transport_decode_limit_or_resource_error");
    m["complete"] = json!(false);
    assert!(manifest::validate(&m)
        .err()
        .unwrap()
        .to_string()
        .contains("unsupported terminal"));
}

#[test]
#[ignore = "requires actual profile CLI captures from the operator tests"]
fn profile_reader_actual_large_envelopes_and_terminal_refusal() {
    let root = env_path("B85_PROFILE_CAPTURE_DIR");
    for (name, size) in [
        ("profile-above-legacy", 1_048_577),
        ("profile-selected-n", 2_097_152),
    ] {
        let capture = reader::read(&root.join(name)).unwrap();
        assert_eq!(capture.messages.len(), 1);
        assert_eq!(capture.messages[0].1.encoded_len(), size);
        assert_eq!(capture.manifest["capture_profile"], "diagnostic-v1");
    }
    let reason = reader::read(&root.join("profile-selected-n-plus-one"))
        .err()
        .expect("terminal prefix must refuse")
        .to_string();
    assert!(reason.contains("unsupported terminal capture reason"));
}
