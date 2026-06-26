use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::SwapEvent;
use copybot_discovery_v2::{build_discovery_v2_status, DiscoveryV2BuildOptions};
use copybot_storage_core::{ensure_discovery_v2_schema, SqliteDiscoveryStore};
use std::io::ErrorKind;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread::JoinHandle;
use std::time::{Duration as StdDuration, Instant};
use tempfile::tempdir;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[test]
fn live_portfolio_gate_replaces_drained_wallet_with_live_token_holder() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    let token_a = "LiveTokenA1111111111111111111111111111111";
    let token_b = "LiveTokenB2222222222222222222222222222222";
    let token_c = "LiveTokenC3333333333333333333333333333333";
    store.insert_observed_swaps_batch(&[
        tail_coverage_swap("sig-coverage-floor", 9, now - Duration::hours(25)),
        swap_with_token("wallet_a", token_a, "sig-a", 10, now - Duration::minutes(4)),
        swap_with_token("wallet_b", token_b, "sig-b", 11, now - Duration::minutes(3)),
        swap_with_token("wallet_c", token_c, "sig-c", 12, now - Duration::minutes(2)),
        tail_coverage_swap("sig-tail", 13, now - Duration::minutes(1)),
    ])?;
    insert_quality_for_token(&store, token_a, now, Some(1.0))?;
    insert_quality_for_token(&store, token_b, now, Some(1.0))?;
    insert_quality_for_token(&store, token_c, now, Some(1.0))?;
    let (mut discovery, shadow) = strict_policy();
    discovery.follow_top_n = 2;
    discovery.live_portfolio_gate_enabled = true;
    discovery.min_live_sol_balance = 0.25;
    discovery.live_portfolio_max_wallets = 3;
    discovery.live_portfolio_max_token_accounts = 8;
    let (rpc_url, rpc_thread) = start_live_portfolio_rpc(token_b, 6)?;
    let mut build_options = options(now);
    build_options.live_portfolio_rpc_url = Some(rpc_url);

    let status = build_discovery_v2_status(&store, &discovery, &shadow, build_options)?;
    rpc_thread.join().expect("rpc server thread");

    assert!(
        status.production_green,
        "blockers={:?} candidates={:?} live={:?}",
        status.blockers, status.candidate_wallets, status.live_portfolio
    );
    assert_eq!(
        status.candidate_wallets,
        vec!["wallet_b".to_string(), "wallet_c".to_string()]
    );
    let live = status.live_portfolio.expect("live portfolio status");
    assert_eq!(live.checked_wallets, 3);
    assert_eq!(live.accepted_wallets, 2);
    assert_eq!(live.rejected_wallets, 1);
    let drained = status
        .wallet_metrics
        .iter()
        .find(|metric| metric.wallet_id == "wallet_a")
        .expect("wallet_a metric");
    assert!(drained
        .reject_reasons
        .contains(&"capital_drained_after_window".to_string()));
    let token_holder = status
        .wallet_metrics
        .iter()
        .find(|metric| metric.wallet_id == "wallet_b")
        .expect("wallet_b metric");
    assert!(token_holder.live_token_value_sol.unwrap_or_default() >= 0.25);
    assert_eq!(token_holder.live_tradable_token_positions, Some(1));
    Ok(())
}

#[test]
fn live_portfolio_gate_reserves_slow_hold_slots_after_baseline_accepts() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T11:00:00Z")?.with_timezone(&Utc);
    let token_a = "SlowLiveTokenA111111111111111111111111111";
    let token_b = "SlowLiveTokenB222222222222222222222222222";
    let token_slow = "SlowLiveTokenS333333333333333333333333333";
    store.insert_observed_swaps_batch(&[
        tail_coverage_swap("sig-slow-live-coverage", 9, now - Duration::hours(25)),
        swap_with_token(
            "wallet_a",
            token_a,
            "sig-slow-live-a",
            10,
            now - Duration::minutes(4),
        ),
        swap_with_token(
            "wallet_b",
            token_b,
            "sig-slow-live-b",
            11,
            now - Duration::minutes(3),
        ),
        swap_with_token(
            "wallet_slow",
            token_slow,
            "sig-slow-live-buy",
            12,
            now - Duration::minutes(90),
        ),
        sell_with_token(
            "wallet_slow",
            token_slow,
            "sig-slow-live-sell",
            13,
            now - Duration::minutes(20),
        ),
        tail_coverage_swap("sig-slow-live-tail", 14, now - Duration::minutes(1)),
    ])?;
    insert_quality_for_token(&store, token_a, now, Some(1.0))?;
    insert_quality_for_token(&store, token_b, now, Some(1.0))?;
    insert_quality_for_token(&store, token_slow, now, Some(1.0))?;
    let (mut discovery, shadow) = strict_policy();
    discovery.follow_top_n = 2;
    discovery.live_portfolio_gate_enabled = true;
    discovery.live_portfolio_max_wallets = 2;
    discovery.live_portfolio_max_token_accounts = 8;
    discovery.min_live_sol_balance = 0.25;
    discovery.slow_hold_wallets_enabled = true;
    discovery.slow_hold_top_m = 1;
    discovery.slow_hold_min_hold_median_seconds = 30 * 60;
    discovery.slow_hold_min_trades = 2;
    discovery.slow_hold_min_buy_count = 1;
    discovery.slow_hold_min_score = 0.0;
    let (rpc_url, rpc_thread) = start_accepting_all_live_portfolio_rpc(6)?;
    let mut build_options = options(now);
    build_options.live_portfolio_rpc_url = Some(rpc_url);

    let status = build_discovery_v2_status(&store, &discovery, &shadow, build_options)?;
    rpc_thread.join().expect("rpc server thread");

    assert!(
        status.production_green,
        "blockers={:?} candidates={:?} live={:?}",
        status.blockers, status.candidate_wallets, status.live_portfolio
    );
    assert_eq!(
        status.candidate_wallets,
        vec![
            "wallet_a".to_string(),
            "wallet_b".to_string(),
            "wallet_slow".to_string()
        ]
    );
    assert_eq!(
        status
            .candidate_wallet_sources
            .iter()
            .map(|source| (source.wallet_id.as_str(), source.source_cohort.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("wallet_a", "baseline"),
            ("wallet_b", "baseline"),
            ("wallet_slow", "slow_hold")
        ]
    );
    let live = status.live_portfolio.expect("live portfolio status");
    assert_eq!(live.checked_wallets, 3);
    assert_eq!(live.accepted_wallets, 3);
    Ok(())
}

fn test_store() -> Result<(tempfile::TempDir, SqliteDiscoveryStore)> {
    let dir = tempdir()?;
    let store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    ensure_discovery_v2_schema(&store)?;
    Ok((dir, store))
}

fn swap_with_token(
    wallet: &str,
    token_mint: &str,
    signature: &str,
    slot: u64,
    ts_utc: DateTime<Utc>,
) -> SwapEvent {
    SwapEvent {
        wallet: wallet.to_string(),
        dex: "test".to_string(),
        token_in: SOL_MINT.to_string(),
        token_out: token_mint.to_string(),
        amount_in: 1.0,
        amount_out: 10.0,
        signature: signature.to_string(),
        slot,
        ts_utc,
        exact_amounts: None,
    }
}

fn sell_with_token(
    wallet: &str,
    token_mint: &str,
    signature: &str,
    slot: u64,
    ts_utc: DateTime<Utc>,
) -> SwapEvent {
    SwapEvent {
        wallet: wallet.to_string(),
        dex: "test".to_string(),
        token_in: token_mint.to_string(),
        token_out: SOL_MINT.to_string(),
        amount_in: 10.0,
        amount_out: 1.2,
        signature: signature.to_string(),
        slot,
        ts_utc,
        exact_amounts: None,
    }
}

fn tail_coverage_swap(signature: &str, slot: u64, ts_utc: DateTime<Utc>) -> SwapEvent {
    swap_with_token(
        "tail_wallet",
        "TailCoverageToken11111111111111111111111111",
        signature,
        slot,
        ts_utc,
    )
}

fn insert_quality_for_token(
    store: &SqliteDiscoveryStore,
    token_mint: &str,
    now: DateTime<Utc>,
    liquidity_sol: Option<f64>,
) -> Result<()> {
    store.upsert_token_quality_cache(token_mint, Some(5), liquidity_sol, Some(60), now)
}

fn options(now: DateTime<Utc>) -> DiscoveryV2BuildOptions {
    DiscoveryV2BuildOptions {
        now,
        window_minutes: 24 * 60,
        max_tail_lag_seconds: 1_200,
        max_rows: 100,
        time_budget_ms: 5_000,
        execution_enabled: false,
        live_portfolio_rpc_url: None,
    }
}

fn strict_policy() -> (DiscoveryConfig, ShadowConfig) {
    let mut discovery = DiscoveryConfig::default();
    discovery.min_leader_notional_sol = 0.0;
    discovery.min_trades = 1;
    discovery.min_active_days = 1;
    discovery.min_score = 0.0;
    discovery.min_buy_count = 1;
    discovery.follow_top_n = 1;
    discovery.min_tradable_ratio = 0.25;
    discovery.require_open_positions_for_publication = true;
    discovery.max_rug_ratio = 0.60;
    discovery.rug_lookahead_seconds = 60;
    discovery.thin_market_min_volume_sol = 0.5;
    discovery.thin_market_min_unique_traders = 1;
    let mut shadow = ShadowConfig::default();
    shadow.quality_gates_enabled = true;
    shadow.min_token_age_seconds = 30;
    shadow.min_holders = 5;
    shadow.min_liquidity_sol = 1.0;
    shadow.min_volume_5m_sol = 0.5;
    shadow.min_unique_traders_5m = 1;
    (discovery, shadow)
}

fn start_live_portfolio_rpc(
    token_b: &str,
    expected_requests: usize,
) -> Result<(String, JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let url = format!("http://{}", listener.local_addr()?);
    listener.set_nonblocking(true)?;
    let token_b = token_b.to_string();
    let handle = std::thread::spawn(move || {
        let mut handled = 0usize;
        let mut last_request = Instant::now();
        while handled < expected_requests && last_request.elapsed() < StdDuration::from_millis(250)
        {
            let (mut stream, _) = match listener.accept() {
                Ok(accepted) => accepted,
                Err(err) if err.kind() == ErrorKind::WouldBlock => {
                    std::thread::sleep(StdDuration::from_millis(10));
                    continue;
                }
                Err(err) => panic!("accept rpc request: {err}"),
            };
            handled += 1;
            last_request = Instant::now();
            let mut buffer = [0u8; 8192];
            let read = stream.read(&mut buffer).expect("read rpc request");
            let request = String::from_utf8_lossy(&buffer[..read]);
            let body = live_portfolio_rpc_response(&request, &token_b);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write rpc response");
        }
    });
    Ok((url, handle))
}

fn start_accepting_all_live_portfolio_rpc(
    expected_requests: usize,
) -> Result<(String, JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let url = format!("http://{}", listener.local_addr()?);
    listener.set_nonblocking(true)?;
    let handle = std::thread::spawn(move || {
        let mut handled = 0usize;
        let mut last_request = Instant::now();
        while handled < expected_requests && last_request.elapsed() < StdDuration::from_millis(250)
        {
            let (mut stream, _) = match listener.accept() {
                Ok(accepted) => accepted,
                Err(err) if err.kind() == ErrorKind::WouldBlock => {
                    std::thread::sleep(StdDuration::from_millis(10));
                    continue;
                }
                Err(err) => panic!("accept rpc request: {err}"),
            };
            handled += 1;
            last_request = Instant::now();
            let mut buffer = [0u8; 8192];
            let read = stream.read(&mut buffer).expect("read rpc request");
            let request = String::from_utf8_lossy(&buffer[..read]);
            let body = accepting_all_live_portfolio_rpc_response(&request);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write rpc response");
        }
    });
    Ok((url, handle))
}

fn accepting_all_live_portfolio_rpc_response(request: &str) -> String {
    if request.contains("\"getBalance\"") {
        return r#"{"jsonrpc":"2.0","id":1,"result":{"value":300000000}}"#.to_string();
    }
    r#"{"jsonrpc":"2.0","id":1,"result":{"value":[]}}"#.to_string()
}

fn live_portfolio_rpc_response(request: &str, token_b: &str) -> String {
    if request.contains("\"getBalance\"") {
        let lamports = if request.contains("wallet_c") {
            300_000_000u64
        } else {
            0u64
        };
        return format!(r#"{{"jsonrpc":"2.0","id":1,"result":{{"value":{lamports}}}}}"#);
    }
    let token_accounts = if request.contains("wallet_b") {
        format!(
            r#"[{{"account":{{"data":{{"parsed":{{"info":{{"mint":"{token_b}","tokenAmount":{{"amount":"10000000","decimals":6,"uiAmountString":"10"}}}}}}}}}}}}]"#
        )
    } else {
        "[]".to_string()
    };
    format!(r#"{{"jsonrpc":"2.0","id":1,"result":{{"value":{token_accounts}}}}}"#)
}
