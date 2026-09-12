use super::*;

fn window_manifest() -> Value {
    json!({"schema":1,"mode":"association-capture","production_green":false,
        "stop_reason":"stream_closed","complete":true,"capture_profile":"window-v1",
        "transport_decode_bytes":8_388_608,"metadata_reserve_bytes":2_097_152,
        "limits":{"duration_ms":60_000,"messages":4096,"message_bytes":8_388_608,
            "total_bytes":67_108_864},"session_id":"00000000000000000000000000000000",
        "messages_received":0,"elapsed_ns":1,"provider_coverage":"unmeasured",
        "association_verdict":"not_evaluated"})
}

#[test]
fn window_manifest_caps_and_reserve_are_profile_specific() {
    assert!(manifest::validate(&window_manifest()).is_ok());
    for (pointer, value) in [
        ("/limits/messages", 4097),
        ("/limits/duration_ms", 60_001),
        ("/limits/message_bytes", 8_388_609),
        ("/limits/total_bytes", 67_108_865),
        ("/limits/total_bytes", 2_097_152),
        ("/metadata_reserve_bytes", 262_144),
        ("/transport_decode_bytes", 1_048_576),
    ] {
        let mut m = window_manifest();
        *m.pointer_mut(pointer).unwrap() = json!(value);
        assert!(manifest::validate(&m).is_err(), "{pointer}");
    }
    for value in [Value::Null, json!("window-v2"), json!("diagnostic-v1")] {
        let mut m = window_manifest();
        m["capture_profile"] = value;
        assert!(manifest::validate(&m).is_err());
    }
    let mut m = window_manifest();
    m.as_object_mut().unwrap().remove("capture_profile");
    assert!(manifest::validate(&m).is_err());
}

#[test]
fn window_reader_preread_does_not_widen_old_manifest_size_limits() {
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    for profile in [None, Some("diagnostic-v1")] {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("copybot-b86-{}-{nonce}", std::process::id()));
        std::fs::create_dir(&path).unwrap();
        let dir = Cleanup(path);
        let mut m = window_manifest();
        m["limits"]["messages"] = json!(256);
        m["limits"]["message_bytes"] = json!(1_048_576);
        m["limits"]["total_bytes"] = json!(16_777_216);
        m["transport_decode_bytes"] = json!(1_048_576);
        m["metadata_reserve_bytes"] = json!(262_144);
        if let Some(profile) = profile {
            m["capture_profile"] = json!(profile);
        } else {
            m.as_object_mut().unwrap().remove("capture_profile");
        }
        let mut raw = serde_json::to_vec(&m).unwrap();
        raw.resize(262_145, b' ');
        std::fs::write(dir.0.join("manifest.json"), raw).unwrap();
        let reason = reader::read(&dir.0).err().unwrap().to_string();
        assert_eq!(reason, "profile manifest size bound");
    }
}

#[test]
#[ignore = "requires actual window CLI capture files"]
fn window_reader_actual_count_metadata_and_reserve_controls() {
    let root = env_path("B86_WINDOW_CAPTURE_DIR");
    let c = reader::read(&root.join("window-above-256")).unwrap();
    assert_eq!(c.messages.len(), 257);
    for name in ["window-old-diagnostic", "window-old-legacy"] {
        let c = reader::read(&root.join(name)).unwrap();
        assert_eq!(c.messages.len(), 256);
        assert_eq!(c.manifest["complete"], false);
    }
    let c = reader::read(&root.join("window-4096")).unwrap();
    assert!(c.messages.is_empty());
    assert_eq!(c.ignored_kinds["ping"], 4096);
    assert_eq!(c.manifest["complete"], false);
    assert_eq!(
        reader::read(&root.join("window-reserve-n"))
            .unwrap()
            .messages
            .len(),
        1
    );
    let reason = reader::read(&root.join("window-reserve-n-plus-one"))
        .err()
        .unwrap()
        .to_string();
    assert!(reason.contains("unsupported terminal capture reason"));
}
