use copybot_config::{validate_association_delivery, AppConfig};
fn configured() -> AppConfig {
    toml::from_str(
        r#"
[ingestion]
source="yellowstone_grpc"
yellowstone_delivery_mode="durable_association_v1"
yellowstone_replay_wallets=["11111111111111111111111111111111"]
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
fn observation_scope_cannot_activate_financial_paths() {
    let base = configured();
    validate_association_delivery(&base).unwrap();
    for flag in 0..4 {
        let mut c = base.clone();
        match flag {
            0 => c.execution.enabled = true,
            1 => c.execution.canary_tiny_submit_enabled = true,
            2 => c.execution.canary_entry_submit_enabled = true,
            _ => c.execution.tiny_experiment.activate = true,
        }
        assert!(validate_association_delivery(&c).is_err());
    }
    let mut c = base.clone();
    c.ingestion.yellowstone_delivery_mode = "legacy".into();
    assert!(validate_association_delivery(&c).is_err());
    let mut c = base;
    c.ingestion
        .yellowstone_replay_wallets
        .push(c.ingestion.yellowstone_replay_wallets[0].clone());
    assert!(validate_association_delivery(&c).is_err());
}
