use copybot_config::{validate_association_delivery, AppConfig};
fn configured() -> AppConfig {
    toml::from_str(
        r#"
[ingestion]
source="yellowstone_grpc"
capture_scope_db="/state/capture.db"
yellowstone_delivery_mode="durable_association_v1"
[ingestion.capture_recovery]
policy="protected_complete_recovery_v1"
checkpoint_directory="/state/protected-checkpoints"
migrations_directory="/install/migrations"
max_scope_wallets=128
max_scope_revisions=64
provenance_policy="historical_missed_no_execution_v1"
[ingestion.yellowstone_http_recovery]
broker_url="http://127.0.0.1:9000"
broker_token="local-fixture"
range_slots=100
max_response_bytes=1000000
timeout_ms=1000
fetch_concurrency=1
raw_window_blocks=1
[ingestion.yellowstone_association]
pending={count=10,bytes=100000}
blocks={count=10,bytes=100000}
history={count=10,bytes=100000}
outputs={count=10,bytes=100000}
queue={count=10,bytes=100000}
inbox={count=100,bytes=1000000}
input_bytes=100000
metadata_bytes=100000
pending_ttl_ms=1000
block_ttl_ms=1000
history_ttl_ms=1000
tick_ms=100
sqlite_busy_ms=10
[execution]
enabled=false
canary_tiny_submit_enabled=false
canary_entry_submit_enabled=false
"#,
    )
    .unwrap()
}
#[test]
fn protected_mode_requires_the_complete_explicit_observation_contract() {
    validate_association_delivery(&configured()).unwrap();
    for missing in 0..9 {
        let mut c = configured();
        match missing {
            0 => c.ingestion.capture_recovery = None,
            1 => c.ingestion.yellowstone_http_recovery = None,
            2 => c.ingestion.yellowstone_association = None,
            3 => c.ingestion.capture_scope_db = None,
            4 => {
                c.ingestion
                    .capture_recovery
                    .as_mut()
                    .unwrap()
                    .max_scope_wallets = 129
            }
            5 => {
                c.ingestion
                    .capture_recovery
                    .as_mut()
                    .unwrap()
                    .max_scope_revisions = 0
            }
            6 => {
                c.ingestion
                    .capture_recovery
                    .as_mut()
                    .unwrap()
                    .provenance_policy = "fresh".into()
            }
            7 => c
                .ingestion
                .yellowstone_replay_wallets
                .push("11111111111111111111111111111111".into()),
            _ => c.ingestion.capture_recovery.as_mut().unwrap().policy = "typo".into(),
        }
        assert!(validate_association_delivery(&c).is_err(), "case {missing}");
    }
}
#[test]
fn protected_mode_cannot_activate_financial_flags() {
    for flag in 0..4 {
        let mut c = configured();
        match flag {
            0 => c.execution.enabled = true,
            1 => c.execution.canary_tiny_submit_enabled = true,
            2 => c.execution.canary_entry_submit_enabled = true,
            _ => c.execution.tiny_experiment.activate = true,
        }
        assert!(validate_association_delivery(&c).is_err());
    }
}
