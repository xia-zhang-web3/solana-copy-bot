use super::*;
use anyhow::{anyhow, Context, Result};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_storage::SqliteStore;
use rusqlite::Connection;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::thread;
use tempfile::tempdir;

fn make_service(shadow_quality: ShadowConfig) -> DiscoveryService {
    DiscoveryService::new(DiscoveryConfig::default(), shadow_quality)
}

fn make_service_with_helius(
    shadow_quality: ShadowConfig,
    helius_http_url: String,
) -> DiscoveryService {
    DiscoveryService::new_with_helius(
        DiscoveryConfig::default(),
        shadow_quality,
        Some(helius_http_url),
    )
}

fn spawn_fake_helius_server() -> Result<(String, thread::JoinHandle<Result<()>>)> {
    let listener =
        TcpListener::bind("127.0.0.1:0").context("failed to bind fake helius test server")?;
    let addr = listener
        .local_addr()
        .context("failed to read fake helius addr")?;
    let handle = thread::spawn(move || -> Result<()> {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().context("failed accepting test request")?;
            let mut headers = Vec::new();
            let mut byte = [0u8; 1];
            while !headers.ends_with(b"\r\n\r\n") {
                stream
                    .read_exact(&mut byte)
                    .context("failed reading test request headers")?;
                headers.push(byte[0]);
            }
            let headers_raw = String::from_utf8(headers).context("request headers must be utf-8")?;
            let content_length = headers_raw
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            let mut body = vec![0u8; content_length];
            if content_length > 0 {
                stream
                    .read_exact(&mut body)
                    .context("failed reading test request body")?;
            }
            let body = String::from_utf8(body).context("request body must be utf-8")?;
            let response_body = if body.contains("\"method\":\"getProgramAccounts\"") {
                "{\"jsonrpc\":\"2.0\",\"result\":[{\"account\":{\"data\":{\"parsed\":{\"info\":{\"owner\":\"OwnerA\",\"tokenAmount\":{\"amount\":\"10\"}}}}}}]}".to_string()
            } else if body.contains("\"method\":\"getSignaturesForAddress\"") {
                format!(
                    "{{\"jsonrpc\":\"2.0\",\"result\":[{{\"signature\":\"sig-quality-test\",\"blockTime\":{}}}]}}",
                    Utc::now().timestamp() - 3600
                )
            } else {
                "{\"jsonrpc\":\"2.0\",\"result\":[]}".to_string()
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            stream
                .write_all(response.as_bytes())
                .context("failed writing fake helius response")?;
            stream
                .flush()
                .context("failed flushing fake helius response")?;
        }
        Ok(())
    });
    Ok((format!("http://{addr}"), handle))
}

fn make_state(sol_notional: f64, unique_traders: usize) -> TokenRollingState {
    let mut state = TokenRollingState::default();
    let now = DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    for idx in 0..unique_traders {
        let wallet = format!("wallet-{idx}");
        state.sol_traders_5m.insert(wallet.clone(), 1);
        state.sol_trades_5m.push_back(SolLegTrade {
            ts: now,
            wallet_id: wallet,
            sol_notional,
        });
        state.sol_volume_5m += sol_notional;
    }
    state
}

fn make_cache_row() -> TokenQualityCacheRow {
    TokenQualityCacheRow {
        mint: "TokenA".to_string(),
        holders: Some(42),
        liquidity_sol: Some(3.0),
        token_age_seconds: Some(3_600),
        fetched_at: DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc),
    }
}

#[test]
    fn is_tradable_token_rejects_missing_rpc_age_and_holders_when_thresholded() {
        let mut shadow_quality = ShadowConfig::default();
        shadow_quality.min_token_age_seconds = 600;
        shadow_quality.min_holders = 10;
        shadow_quality.min_volume_5m_sol = 0.5;
        shadow_quality.min_unique_traders_5m = 1;
        let discovery = make_service(shadow_quality);
        let state = make_state(1.0, 1);

        assert!(
            matches!(
                discovery.evaluate_buy_tradability(&state, None, Utc::now()),
                BuyTradability::Rejected
            ),
            "missing rpc/cache quality must fail closed when age/holders thresholds are enabled"
        );
    }

    #[test]
    fn is_tradable_token_rejects_missing_holders_field_when_thresholded() {
        let mut shadow_quality = ShadowConfig::default();
        shadow_quality.min_token_age_seconds = 0;
        shadow_quality.min_holders = 10;
        shadow_quality.min_volume_5m_sol = 0.5;
        shadow_quality.min_unique_traders_5m = 1;
        let discovery = make_service(shadow_quality);
        let state = make_state(1.0, 1);
        let mut row = make_cache_row();
        row.holders = None;

        assert!(
            matches!(
                discovery.evaluate_buy_tradability(
                    &state,
                    Some(&TokenQualityResolution::Fresh(row)),
                    Utc::now()
                ),
                BuyTradability::Rejected
            ),
            "missing holders field must fail closed when min_holders is enabled"
        );
    }

    #[test]
    fn is_tradable_token_uses_liquidity_proxy_when_rpc_quality_missing() {
        let mut shadow_quality = ShadowConfig::default();
        shadow_quality.min_token_age_seconds = 0;
        shadow_quality.min_holders = 0;
        shadow_quality.min_liquidity_sol = 0.75;
        shadow_quality.min_volume_5m_sol = 0.5;
        shadow_quality.min_unique_traders_5m = 1;
        let discovery = make_service(shadow_quality);
        let state = make_state(1.0, 1);

        assert!(
            matches!(
                discovery.evaluate_buy_tradability(&state, None, Utc::now()),
                BuyTradability::Tradable
            ),
            "liquidity gate should still use rolling proxy when rpc quality is unavailable"
        );
    }

    #[test]
    fn is_tradable_token_rejects_when_liquidity_proxy_is_below_threshold() {
        let mut shadow_quality = ShadowConfig::default();
        shadow_quality.min_token_age_seconds = 0;
        shadow_quality.min_holders = 0;
        shadow_quality.min_liquidity_sol = 1.5;
        shadow_quality.min_volume_5m_sol = 0.5;
        shadow_quality.min_unique_traders_5m = 1;
        let discovery = make_service(shadow_quality);
        let state = make_state(1.0, 1);

        assert!(
            matches!(
                discovery.evaluate_buy_tradability(&state, None, Utc::now()),
                BuyTradability::Rejected
            ),
            "missing rpc quality must not bypass liquidity threshold"
        );
    }

    #[test]
    fn evaluate_buy_tradability_defers_missing_age_holders_when_fetch_was_deferred() {
        let mut shadow_quality = ShadowConfig::default();
        shadow_quality.min_token_age_seconds = 600;
        shadow_quality.min_holders = 10;
        shadow_quality.min_liquidity_sol = 0.5;
        shadow_quality.min_volume_5m_sol = 0.5;
        shadow_quality.min_unique_traders_5m = 1;
        let discovery = make_service(shadow_quality);
        let state = make_state(1.0, 1);

        assert!(
            matches!(
                discovery.evaluate_buy_tradability(
                    &state,
                    Some(&TokenQualityResolution::Deferred),
                    Utc::now()
                ),
                BuyTradability::Deferred
            ),
            "budget-deferred quality should not hard-reject tradability"
        );
    }

    #[test]
    fn evaluate_buy_tradability_does_not_trust_stale_holders_for_pass() {
        let mut shadow_quality = ShadowConfig::default();
        shadow_quality.min_token_age_seconds = 0;
        shadow_quality.min_holders = 10;
        shadow_quality.min_liquidity_sol = 0.5;
        shadow_quality.min_volume_5m_sol = 0.5;
        shadow_quality.min_unique_traders_5m = 1;
        let discovery = make_service(shadow_quality);
        let state = make_state(1.0, 1);
        let row = make_cache_row();

        assert!(
            matches!(
                discovery.evaluate_buy_tradability(
                    &state,
                    Some(&TokenQualityResolution::Stale(row)),
                    Utc::now()
                ),
                BuyTradability::Deferred
            ),
            "stale holders above threshold must not be treated as authoritative pass"
        );
    }

    #[test]
    fn evaluate_buy_tradability_uses_liquidity_proxy_for_stale_rows() {
        let mut shadow_quality = ShadowConfig::default();
        shadow_quality.min_token_age_seconds = 0;
        shadow_quality.min_holders = 0;
        shadow_quality.min_liquidity_sol = 1.5;
        shadow_quality.min_volume_5m_sol = 0.5;
        shadow_quality.min_unique_traders_5m = 1;
        let discovery = make_service(shadow_quality);
        let state = make_state(1.0, 1);
        let mut row = make_cache_row();
        row.liquidity_sol = Some(10.0);

        assert!(
            matches!(
                discovery.evaluate_buy_tradability(
                    &state,
                    Some(&TokenQualityResolution::Stale(row)),
                    Utc::now()
                ),
                BuyTradability::Rejected
            ),
            "stale liquidity rows must not keep a token passing if current proxy is below threshold"
        );
    }

    #[test]
    fn discovery_quality_cache_error_requires_abort_on_xshmmap_io_failure() {
        let error = anyhow!("disk I/O error: Error code 4874: I/O error within the xShmMap method");
        assert!(discovery_quality_cache_error_requires_abort(&error));
    }

    #[test]
    fn discovery_quality_cache_error_does_not_require_abort_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!discovery_quality_cache_error_requires_abort(&error));
    }

    #[test]
    fn resolve_token_quality_for_mints_returns_error_on_fatal_cache_write_failure() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("quality-cache-fatal.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for token quality trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_token_quality_cache_insert
             BEFORE INSERT ON token_quality_cache
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let (helius_http_url, server_handle) = spawn_fake_helius_server()?;
        let discovery = make_service_with_helius(ShadowConfig::default(), helius_http_url);
        let now = DateTime::parse_from_rfc3339("2026-03-14T14:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let mints = vec!["TokenQualityFatal11111111111111111111111".to_string()];

        let error = discovery
            .resolve_token_quality_for_mints(&store, &mints, now)
            .expect_err("fatal token quality cache write must propagate");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "discovery token quality cache refresh write failed with fatal sqlite I/O"
            ),
            "expected fatal token quality cache write context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed upserting token_quality_cache row"),
            "expected token quality cache upsert context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(
            store.get_token_quality_cache(&mints[0])?.is_none(),
            "fatal token quality cache write failure must not leave a persisted cache row"
        );

        server_handle
            .join()
            .map_err(|_| anyhow!("fake helius server thread panicked"))??;
        drop(trigger_conn);
        Ok(())
    }
