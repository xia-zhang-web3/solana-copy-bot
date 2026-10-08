use copybot_config::{validate_delivery_source, HttpRecoveryConfig, IngestionConfig};

fn config(extra: &str, width: usize) -> HttpRecoveryConfig {
    toml::from_str(&format!(
        "broker_url='http://127.0.0.1:1'\nbroker_token=''\nrange_slots=1024\nmax_response_bytes=16777216\ntimeout_ms=25000\nfetch_concurrency={width}\n{extra}"
    )).unwrap()
}
#[test]
fn omitted_window_preserves_width_and_explicit_10_32_stays_bounded() {
    let old = config("", 4);
    assert_eq!(old.raw_window_blocks, None);
    assert_eq!(old.raw_window(), 4);
    old.validate().unwrap();
    let explicit = config("raw_window_blocks=32", 10);
    assert_eq!(explicit.fetch_concurrency, 10);
    assert_eq!(explicit.raw_window(), 32);
    explicit.validate().unwrap();
    for (window, width) in [(0, 10), (9, 10), (33, 10), (32, 33)] {
        let invalid = config(&format!("raw_window_blocks={window}"), width);
        assert!(invalid
            .validate()
            .unwrap_err()
            .to_string()
            .contains("raw_window_bounds"));
        assert_eq!(invalid.fetch_concurrency, width);
        assert_eq!(invalid.raw_window(), window);
    }
}
#[test]
fn explicit_window_does_not_bypass_the_ingestion_active_request_cap() {
    // The HTTP cap check precedes the unrelated delivery-budget validation.
    let mut ingestion = IngestionConfig::default();
    ingestion.source = "yellowstone_grpc".into();
    ingestion.yellowstone_delivery_mode = "durable_association_v1".into();
    ingestion.fetch_concurrency = 4;
    ingestion.yellowstone_http_recovery = Some(config("raw_window_blocks=32", 10));
    assert!(validate_delivery_source(&ingestion)
        .unwrap_err()
        .to_string()
        .contains("fetch_concurrency_exceeds_ingestion_limit"));
    assert_eq!(ingestion.fetch_concurrency, 4);
    assert_eq!(
        ingestion
            .yellowstone_http_recovery
            .unwrap()
            .fetch_concurrency,
        10
    );
}
