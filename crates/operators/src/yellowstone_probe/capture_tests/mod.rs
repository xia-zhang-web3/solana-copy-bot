use super::super::{
    capture_hash::sha256,
    config::{parse_args_from, ProbeMode},
    request::build_subscribe_request,
};
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use yellowstone_grpc_proto::{prelude::*, prost::Message};
mod controls;
mod server;
const PUMP: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const RAY: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
fn root() -> PathBuf {
    PathBuf::from(std::env::var("B81_CAPTURE_DIR").unwrap())
}
fn ping() -> SubscribeUpdate {
    SubscribeUpdate {
        update_oneof: Some(subscribe_update::UpdateOneof::Ping(SubscribeUpdatePing {})),
        ..Default::default()
    }
}
fn transaction() -> SubscribeUpdate {
    SubscribeUpdate {
        update_oneof: Some(subscribe_update::UpdateOneof::Transaction(
            SubscribeUpdateTransaction::default(),
        )),
        ..Default::default()
    }
}
fn write_json(path: &Path, value: &Value) {
    use std::io::Write;
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap()
        .write_all(&serde_json::to_vec_pretty(value).unwrap())
        .unwrap();
}
fn args(output: &Path) -> Vec<String> {
    [
        "--mode",
        "association-capture",
        "--json",
        "--output-dir",
        output.to_str().unwrap(),
        "--duration-ms",
        "1000",
        "--max-messages",
        "256",
        "--max-message-bytes",
        "1048576",
        "--max-total-bytes",
        "16777216",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}
fn replace(args: &mut [String], name: &str, value: impl ToString) {
    let n = args.iter().position(|v| v == name).unwrap();
    args[n + 1] = value.to_string();
}
async fn cli_case(
    name: &str,
    messages: Vec<SubscribeUpdate>,
    mode: &'static str,
    changes: &[(&str, u64)],
) -> Value {
    cli_case_profile(name, messages, mode, changes, None).await
}
async fn cli_case_profile(
    name: &str,
    messages: Vec<SubscribeUpdate>,
    mode: &'static str,
    changes: &[(&str, u64)],
    profile: Option<&str>,
) -> Value {
    std::fs::create_dir_all(root()).unwrap();
    let output = root().join(name);
    let mut args = args(&output);
    for (key, value) in changes {
        replace(&mut args, key, *value);
    }
    if let Some(profile) = profile {
        args.extend(["--capture-profile".into(), profile.into()]);
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(vec![]));
    let subscriptions = Arc::new(AtomicUsize::new(0));
    let server = server::Server {
        messages,
        mode,
        output: output.clone(),
        requests: requests.clone(),
        subscriptions: subscriptions.clone(),
    };
    let incoming = futures_util::stream::unfold(listener, |listener| async move {
        Some((listener.accept().await.map(|(s, _)| s), listener))
    });
    let task = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(geyser_server::GeyserServer::new(server))
            .serve_with_incoming(incoming),
    );
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("local.toml");
    std::fs::write(&config,format!("[ingestion]\nsource='yellowstone_grpc'\nyellowstone_grpc_url='http://{address}?credential=private-query'\nyellowstone_x_token='synthetic-secret'\nyellowstone_program_ids=['{RAY}','{PUMP}']\n")).unwrap();
    args.extend(["--config".into(), config.to_str().unwrap().into()]);
    let binary = std::env::var("B81_PROBE_BIN").unwrap();
    let child = tokio::task::spawn_blocking(move || {
        std::process::Command::new(binary)
            .env_clear()
            .args(args)
            .output()
            .unwrap()
    });
    let result = tokio::time::timeout(Duration::from_secs(75), child)
        .await
        .unwrap()
        .unwrap();
    task.abort();
    let stdout = String::from_utf8(result.stdout).unwrap();
    let stderr = String::from_utf8(result.stderr).unwrap();
    assert!(!stdout.contains("synthetic-secret") && !stdout.contains("private-query"));
    assert!(stderr.is_empty(), "{stderr}");
    let report: Value = serde_json::from_str(&stdout).unwrap();
    write_json(
        &root().join(format!("{name}.result.json")),
        &json!({"exit":result.status.code(),"stdout":report,"stderr":stderr,
        "subscriptions":subscriptions.load(Ordering::SeqCst)}),
    );
    assert_eq!(subscriptions.load(Ordering::SeqCst), 1, "{name}: {stdout}");
    let request = &requests.lock().unwrap()[0];
    let expected =
        build_subscribe_request(ProbeMode::AssociationCapture, &[RAY.into(), PUMP.into()]);
    assert_eq!(*request, expected);
    let captured = &report["capture"]["manifest"];
    if report["capture"]["manifest_persisted"] == true {
        let actual: Value =
            serde_json::from_slice(&std::fs::read(output.join("manifest.json")).unwrap()).unwrap();
        assert_eq!(*captured, actual);
    }
    assert_eq!(report["production_green"], false);
    assert_eq!(
        result.status.success(),
        report["reason_class"] == "capture_complete"
    );
    report
}

#[tokio::test]
#[ignore = "requires explicit local capture directories and actual probe binary"]
async fn actual_cli_capture_corpus() {
    let fixtures = PathBuf::from(std::env::var("B81_FIXTURE_DIR").unwrap());
    let scenarios: Value =
        serde_json::from_slice(&std::fs::read(fixtures.join("scenarios.json")).unwrap()).unwrap();
    for case in scenarios.as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let dir = fixtures.join(name);
        let inputs: Value =
            serde_json::from_slice(&std::fs::read(dir.join("inputs.json")).unwrap()).unwrap();
        let messages = inputs
            .as_array()
            .unwrap()
            .iter()
            .map(|v| {
                SubscribeUpdate::decode(
                    std::fs::read(dir.join(v["file"].as_str().unwrap()))
                        .unwrap()
                        .as_slice(),
                )
                .unwrap()
            })
            .collect();
        let result = cli_case(name, messages, "close", &[]).await;
        assert_eq!(
            result["reason_class"], "capture_complete",
            "{name}: {result}"
        );
        assert_eq!(
            result["capture"]["manifest"]["messages_received"],
            json!(inputs.as_array().unwrap().len())
        );
    }
}

mod connection_tests;

mod profile_tests;
mod revision_tests;
mod terminal_cli_tests;
mod terminal_tests;
mod window_tests;

mod burst_tests;
mod deferred_io_tests;
mod deferred_tests;
