#[test]
fn config_debug_redacts_secret_values() {
    let mut ingestion = IngestionConfig::default();
    ingestion.helius_ws_url = "wss://mainnet.helius-rpc.com/?api-key=helius-ws-secret".to_string();
    ingestion.helius_http_url =
        "https://mainnet.helius-rpc.com/?api-key=helius-http-secret".to_string();
    ingestion.helius_http_urls =
        vec!["https://backup.helius.example/?api-key=backup-secret".to_string()];
    ingestion.yellowstone_x_token = "yellowstone-secret".to_string();
    let ingestion_debug = format!("{ingestion:?}");
    for secret in [
        "helius-ws-secret",
        "helius-http-secret",
        "backup-secret",
        "yellowstone-secret",
    ] {
        assert!(
            !ingestion_debug.contains(secret),
            "ingestion debug output leaked secret={secret}: {ingestion_debug}"
        );
    }
    assert!(
        ingestion_debug.contains("wss://mainnet.helius-rpc.com/?<redacted>"),
        "ingestion debug output should redact ws URL query: {ingestion_debug}"
    );
    assert!(
        ingestion_debug.contains("https://mainnet.helius-rpc.com/?<redacted>"),
        "ingestion debug output should redact http URL query: {ingestion_debug}"
    );
    assert!(
        ingestion_debug.contains("[REDACTED]"),
        "ingestion debug output should show redaction marker: {ingestion_debug}"
    );

    let mut discovery = DiscoveryConfig::default();
    discovery.helius_http_url =
        "https://discovery.helius.example/?api-key=discovery-secret".to_string();
    let discovery_debug = format!("{discovery:?}");
    assert!(
        !discovery_debug.contains("discovery-secret"),
        "discovery debug output leaked URL secret: {discovery_debug}"
    );
    assert!(
        discovery_debug.contains("https://discovery.helius.example/?<redacted>"),
        "discovery debug output should redact URL query: {discovery_debug}"
    );

    let mut shadow = ShadowConfig::default();
    shadow.helius_http_url = "https://shadow.helius.example/?api-key=shadow-secret".to_string();
    let shadow_debug = format!("{shadow:?}");
    assert!(
        !shadow_debug.contains("shadow-secret"),
        "shadow debug output leaked URL secret: {shadow_debug}"
    );
    assert!(
        shadow_debug.contains("https://shadow.helius.example/?<redacted>"),
        "shadow debug output should redact URL query: {shadow_debug}"
    );

    let app_debug = format!(
        "{:?}",
        AppConfig {
            discovery,
            ingestion,
            shadow,
            ..AppConfig::default()
        }
    );
    assert!(
        !app_debug.contains("yellowstone-secret"),
        "AppConfig debug output leaked ingestion token: {app_debug}"
    );
    for secret in ["discovery-secret", "shadow-secret"] {
        assert!(
            !app_debug.contains(secret),
            "AppConfig debug output leaked nested URL secret={secret}: {app_debug}"
        );
    }
}
