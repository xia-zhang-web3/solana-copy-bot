use super::*;

fn config(path: &Path, endpoint: &str) {
    std::fs::write(path,format!("[ingestion]\nsource='yellowstone_grpc'\nyellowstone_grpc_url='{endpoint}'\nyellowstone_x_token='synthetic-secret'\nyellowstone_program_ids=['{PUMP}']\n")).unwrap();
}
async fn invoke(output: &Path, config: &Path) -> Value {
    let mut arguments = args(output);
    replace(&mut arguments, "--duration-ms", 200);
    arguments.extend(["--config".into(), config.to_str().unwrap().into()]);
    let binary = std::env::var("B81_PROBE_BIN").unwrap();
    let result = tokio::task::spawn_blocking(move || {
        std::process::Command::new(binary)
            .env_clear()
            .args(arguments)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(!result.status.success());
    assert!(result.stderr.is_empty());
    let report: Value = serde_json::from_slice(&result.stdout).unwrap();
    write_json(
        &root().join(format!(
            "{}.connection-result.json",
            output.file_name().unwrap().to_str().unwrap()
        )),
        &json!({"exit":result.status.code(),"stdout":report}),
    );
    report
}
#[tokio::test]
#[ignore = "requires actual local probe binary and evidence directory"]
async fn actual_cli_connect_deadline_and_config_redaction() {
    std::fs::create_dir_all(root()).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let conf = temp.path().join("local.toml");
    for (name, scheme, reason) in [
        ("tcp-open-deadline", "http", "subscribe_open_deadline"),
        ("tls-connect-deadline", "https", "connect_deadline"),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        config(
            &conf,
            &format!("{scheme}://{}", listener.local_addr().unwrap()),
        );
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(2)).await;
            drop(socket);
        });
        let r = invoke(&root().join(name), &conf).await;
        server.abort();
        assert_eq!(r["capture"]["manifest"]["stop_reason"], reason);
    }
    std::fs::write(
        &conf,
        "[ingestion]\nyellowstone_x_token = synthetic-secret\n",
    )
    .unwrap();
    let r = invoke(&root().join("malformed-config"), &conf).await;
    assert_eq!(
        r["reason_class"],
        "yellowstone_source_probe_config_unreadable"
    );
    // Preserve the existing outer-context config error classification.
    assert!(!r.to_string().contains("synthetic-secret"));
    assert_eq!(r["error_redacted"], Value::Null);
}
#[tokio::test]
#[ignore = "requires earlier actual CLI capture corpus"]
async fn actual_cli_rerun_keeps_previous_capture() {
    let capture = root().join("buy-native-tx-first");
    let before: std::collections::BTreeMap<_, _> = std::fs::read_dir(&capture)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (entry.file_name(), std::fs::read(entry.path()).unwrap())
        })
        .collect();
    let temp = tempfile::tempdir().unwrap();
    let conf = temp.path().join("local.toml");
    config(&conf, "http://127.0.0.1:1");
    let r = invoke(&capture, &conf).await;
    assert_eq!(r["reason_class"], "capture_output_create_failed");
    assert_eq!(r["connect_started"], false);
    for (name, bytes) in before {
        assert_eq!(std::fs::read(capture.join(name)).unwrap(), bytes);
    }
}
