use super::*;
use std::path::{Path, PathBuf};
mod delivery_export_tests;
mod fixtures;
mod manifest;
mod oracle;
mod policy;
mod profile_tests;
mod reader;
mod replay;
mod request;
mod request_tests;
mod revision_tests;
mod window_tests;

fn env_path(key: &str) -> PathBuf {
    PathBuf::from(std::env::var(key).expect(key))
}
fn write_json(path: &Path, value: &Value) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    file.write_all(&serde_json::to_vec_pretty(value).unwrap())
        .unwrap();
}

#[test]
#[ignore = "requires explicit bounded CLI fixture output directory"]
fn generate_capture_fixtures() {
    fixtures::generate(&env_path("B81_FIXTURE_DIR"));
}

#[test]
#[ignore = "requires actual loopback CLI capture files"]
fn replay_actual_cli_files() {
    let fixtures = env_path("B81_FIXTURE_DIR");
    let captures = env_path("B81_CAPTURE_DIR");
    let scenarios: Value =
        serde_json::from_slice(&std::fs::read(fixtures.join("scenarios.json")).unwrap()).unwrap();
    let mut results = Vec::new();
    for scenario in scenarios.as_array().unwrap() {
        let name = scenario["name"].as_str().unwrap();
        let capture = oracle::read(&captures.join(name), &fixtures.join(name));
        let result = replay::analyze(&capture);
        let actual: Vec<_> = result["transactions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["status"].clone())
            .collect();
        assert_eq!(json!(actual), scenario["expected"], "{name}");
        if name.ends_with("block-first") {
            assert!(result["transactions"]
                .as_array()
                .unwrap()
                .iter()
                .all(|v| v["assertions"].as_array().unwrap().iter().all(|a| a
                    ["arrival_block_minus_tx_ns"]
                    .as_i64()
                    .unwrap()
                    < 0)));
        }
        results.push(json!({"name":name,"analysis":result}));
    }
    write_json(&env_path("B81_REPLAY_OUT"), &json!(results));
}

mod integrity;

#[test]
#[ignore = "offline analysis: requires only B81_OFFLINE_CAPTURE_DIR and B81_OFFLINE_OUTPUT"]
fn offline_capture_analysis() -> Result<()> {
    let dir = env_path("B81_OFFLINE_CAPTURE_DIR");
    let output = env_path("B81_OFFLINE_OUTPUT");
    let result = reader::read(&dir).map(|capture| replay::analyze(&capture));
    let value = match &result {
        Ok(analysis) => json!({"analysis":analysis,"production_green":false}),
        Err(error) => json!({"refusal":format!("{error:#}"),"production_green":false}),
    };
    write_json(&output, &value);
    result.map(|_| ())
}

mod streaming_tests;
