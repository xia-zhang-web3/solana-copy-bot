use super::*;

#[test]
fn capture_cli_limits_are_explicit_positive_and_hard_bounded() {
    let mut valid = args(Path::new("fresh"));
    valid.extend(["--config".into(), "test.toml".into()]);
    assert!(parse_args_from(valid.clone()).is_ok());
    for (flag, max) in [
        ("--duration-ms", 60000),
        ("--max-messages", 256),
        ("--max-message-bytes", 1048576),
        ("--max-total-bytes", 16777216),
    ] {
        for value in [
            "0".to_string(),
            "-1".into(),
            "18446744073709551616".into(),
            (max + 1).to_string(),
        ] {
            let mut a = valid.clone();
            replace(&mut a, flag, value);
            assert!(parse_args_from(a).is_err(), "{flag}");
        }
        let mut a = valid.clone();
        let n = a.iter().position(|v| v == flag).unwrap();
        a.drain(n..n + 2);
        assert!(parse_args_from(a).is_err());
    }
    let mut old = valid.clone();
    replace(&mut old, "--mode", "slots-only");
    assert!(parse_args_from(old).is_err());
    let mut duplicate = valid;
    duplicate.extend(["--duration-ms".into(), "1".into()]);
    assert!(parse_args_from(duplicate).is_err());
}
#[test]
fn capture_sha256_known_vectors_and_padding_boundaries() {
    assert_eq!(
        sha256(b""),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    assert_eq!(
        sha256(b"abc"),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert_eq!(
        sha256(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
        "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
    );
    assert_eq!(
        sha256(&vec![b'a'; 1000000]),
        "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
    );
}
#[tokio::test]
#[ignore = "requires actual local probe binary and evidence directory"]
async fn actual_cli_stop_and_io_controls() {
    for (name, msgs, mode, reason) in [
        ("empty-close", vec![], "close", "stream_closed"),
        ("ping-close", vec![ping()], "close", "stream_closed"),
        ("silent", vec![], "silent", "stream_deadline"),
        ("ping-silent", vec![ping()], "silent", "stream_deadline"),
        ("stream-error", vec![ping()], "stream-error", "stream_error"),
        ("open-error", vec![], "open-error", "subscribe_open_failed"),
        (
            "open-silent",
            vec![],
            "open-silent",
            "subscribe_open_deadline",
        ),
        (
            "output-failure",
            vec![transaction()],
            "output-failure",
            "output_io_failure",
        ),
        (
            "output-existing-file",
            vec![transaction()],
            "output-existing-file",
            "output_io_failure",
        ),
        (
            "manifest-failure",
            vec![transaction()],
            "manifest-failure",
            "manifest_io_failure",
        ),
        (
            "heavy-account",
            vec![SubscribeUpdate {
                update_oneof: Some(subscribe_update::UpdateOneof::Account(Default::default())),
                ..Default::default()
            }],
            "close",
            "unexpected_heavy_input",
        ),
        (
            "heavy-entry",
            vec![SubscribeUpdate {
                update_oneof: Some(subscribe_update::UpdateOneof::Entry(Default::default())),
                ..Default::default()
            }],
            "close",
            "unexpected_heavy_input",
        ),
    ] {
        let result = cli_case(name, msgs, mode, &[("--duration-ms", 250)]).await;
        assert_eq!(
            result["capture"]["manifest"]["stop_reason"], reason,
            "{name}: {result}"
        );
    }
    assert_eq!(
        std::fs::read(root().join("output-existing-file/000001.pb")).unwrap(),
        b"prior envelope"
    );
    assert_eq!(
        std::fs::read(root().join("manifest-failure/manifest.json")).unwrap(),
        b"prior evidence"
    );
    let mut heavy = SubscribeUpdateBlock::default();
    heavy.accounts.push(Default::default());
    let result = cli_case(
        "heavy-block",
        vec![SubscribeUpdate {
            update_oneof: Some(subscribe_update::UpdateOneof::Block(heavy)),
            ..Default::default()
        }],
        "close",
        &[],
    )
    .await;
    assert_eq!(
        result["capture"]["manifest"]["stop_reason"],
        "unexpected_heavy_input"
    );
    let mut oversized = ping();
    oversized.filters = vec!["x".repeat(1_048_577)];
    let result = cli_case("transport-limit", vec![oversized], "close", &[]).await;
    assert_eq!(
        result["capture"]["manifest"]["stop_reason"],
        "transport_decode_limit_or_resource_error"
    );
}
#[tokio::test]
#[ignore = "requires actual local probe binary and evidence directory"]
async fn actual_cli_n_and_n_plus_one_limits() {
    for n in [2, 3] {
        let result = cli_case(
            &format!("count-{n}"),
            vec![ping(); n],
            "close",
            &[("--max-messages", 2)],
        )
        .await;
        let m = &result["capture"]["manifest"];
        assert_eq!(m["stop_reason"], "message_count_limit");
        assert_eq!(m["messages_received"], 2);
        assert_eq!(m["message_kinds"]["ping"], 2);
        assert_eq!(m["complete"], false);
    }
    let mut a = transaction();
    a.filters = vec!["a".repeat(200)];
    let size = a.encoded_len() as u64;
    for (name, limit, reason) in [
        ("bytes-n", size, "stream_closed"),
        ("bytes-n-plus-one", size - 1, "message_byte_limit"),
    ] {
        let result = cli_case(
            name,
            vec![a.clone()],
            "close",
            &[("--max-message-bytes", limit)],
        )
        .await;
        assert_eq!(result["capture"]["manifest"]["stop_reason"], reason);
    }
    for (name, limit, reason) in [
        ("total-n", 262144 + size, "stream_closed"),
        ("total-n-plus-one", 262144 + size - 1, "total_output_limit"),
    ] {
        let result = cli_case(
            name,
            vec![a.clone()],
            "close",
            &[("--max-total-bytes", limit)],
        )
        .await;
        assert_eq!(result["capture"]["manifest"]["stop_reason"], reason);
    }
    let result = cli_case(
        "total-two",
        vec![a.clone(), a],
        "close",
        &[("--max-total-bytes", 262144 + size)],
    )
    .await;
    let m = &result["capture"]["manifest"];
    assert_eq!(m["messages_received"], 2);
    assert_eq!(m["envelopes_written"], 1);
    assert_eq!(m["stop_reason"], "total_output_limit");
}

#[test]
#[ignore = "requires actual local probe binary and evidence directory"]
fn actual_cli_no_overwrite_and_validation() {
    std::fs::create_dir_all(root()).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("local.toml");
    let binary = std::env::var("B81_PROBE_BIN").unwrap();
    let baseline=format!("[ingestion]\nsource='yellowstone_grpc'\nyellowstone_grpc_url='http://127.0.0.1:1'\nyellowstone_x_token='synthetic-secret'\nyellowstone_program_ids=['{PUMP}']\n");
    for name in [
        "existing-dir",
        "existing-file",
        "missing-parent",
        "invalid-policy",
        "empty-policy",
        "connect-failure",
        "invalid-limits",
    ] {
        std::fs::write(&config, &baseline).unwrap();
        let output = root().join(name);
        let mut a = args(&output);
        match name {
            "existing-dir" => {
                std::fs::create_dir(&output).unwrap();
                std::fs::write(output.join("keep"), b"unchanged").unwrap();
            }
            "existing-file" => std::fs::write(&output, b"unchanged").unwrap(),
            "missing-parent" => replace(&mut a, "--output-dir", output.join("child").display()),
            "invalid-policy" => std::fs::write(&config, baseline.replace(PUMP, "invalid")).unwrap(),
            "empty-policy" => std::fs::write(
                &config,
                baseline.replace(&format!("['{PUMP}']"), "[]")
                    + "subscribe_program_ids=[]\nraydium_program_ids=[]\npumpswap_program_ids=[]\n",
            )
            .unwrap(),
            "invalid-limits" => replace(&mut a, "--duration-ms", "0"),
            _ => {}
        }
        a.extend(["--config".into(), config.to_str().unwrap().into()]);
        let output_cmd = std::process::Command::new(&binary)
            .env_clear()
            .args(a)
            .output()
            .unwrap();
        assert!(!output_cmd.status.success());
        let report: Value = serde_json::from_slice(&output_cmd.stdout).unwrap();
        let expected = match name {
            "invalid-policy" => "capture_invalid_program_policy",
            "empty-policy" => "yellowstone_source_probe_config_missing_yellowstone",
            "connect-failure" => "connect_failed",
            "invalid-limits" => "yellowstone_source_probe_cli_error",
            _ => "capture_output_create_failed",
        };
        write_json(
            &root().join(format!("{name}.result.json")),
            &json!({"exit":output_cmd.status.code(),"stdout":report}),
        );
        assert_eq!(report["reason_class"], expected, "{name}: {report}");
        if name == "existing-dir" {
            assert_eq!(std::fs::read(output.join("keep")).unwrap(), b"unchanged");
        }
        if name == "existing-file" {
            assert_eq!(std::fs::read(output).unwrap(), b"unchanged");
        }
    }
}
