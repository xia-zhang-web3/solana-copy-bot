use copybot_config::{validate_association_delivery, AppConfig};
fn configured() -> AppConfig {
    toml::from_str(
        r#"
[ingestion]
source="yellowstone_grpc"
yellowstone_delivery_mode="durable_association_v1"
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
"#,
    )
    .unwrap()
}
#[test]
fn b89_startup_modes_flags_and_explicit_limits() {
    let legacy = AppConfig::default();
    assert_eq!(legacy.ingestion.yellowstone_delivery_mode, "legacy");
    validate_association_delivery(&legacy).unwrap();
    for source in ["yellowstone_grpc", "mock", "helius_ws"] {
        for enabled in [false, true] {
            for tiny in [false, true] {
                let mut c = configured();
                c.ingestion.source = source.into();
                c.execution.enabled = enabled;
                c.execution.canary_tiny_submit_enabled = tiny;
                assert_eq!(
                    validate_association_delivery(&c).is_ok(),
                    source == "yellowstone_grpc" && !enabled && !tiny
                );
            }
        }
    }
    let mut c = configured();
    c.ingestion.yellowstone_association = None;
    assert!(validate_association_delivery(&c).is_err());
    let mut c = configured();
    c.ingestion.yellowstone_delivery_mode = "typo".into();
    assert!(validate_association_delivery(&c).is_err());
    for field in 0..5 {
        let mut c = configured();
        let l = c.ingestion.yellowstone_association.as_mut().unwrap();
        match field {
            0 => l.queue.count = 0,
            1 => l.pending_ttl_ms = 0,
            2 => l.inbox.bytes = usize::MAX,
            3 => l.queue.bytes = u32::MAX as usize + 1,
            _ => l.input_bytes = 0,
        };
        assert!(validate_association_delivery(&c).is_err());
    }
}
#[test]
fn b89_partial_limits_are_not_silently_defaulted() {
    assert!(toml::from_str::<AppConfig>(
        r#"[ingestion.yellowstone_association]
input_bytes=123
"#
    )
    .is_err());
}
