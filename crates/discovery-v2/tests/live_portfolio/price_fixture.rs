use super::{
    fixture::{self, Fixture},
    rpc,
};
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_discovery_v2::{
    build_discovery_v2_status, build_discovery_v2_wallet_report, DiscoveryV2BuildOptions,
    DiscoveryV2DecisionContext, DiscoveryV2Status, DiscoveryV2WalletReport,
    DiscoveryV2WalletReportOptions,
};
use serde_json::{json, Value};

pub fn fixture() -> Result<Fixture> {
    let mut f = Fixture::new()?;
    f.discovery.status_scan_window_minutes = 120;
    f.discovery.metric_snapshot_interval_seconds = 600;
    f.discovery.refresh_seconds = 60;
    Ok(f)
}

pub fn options(f: &Fixture, now: DateTime<Utc>) -> DiscoveryV2BuildOptions {
    let mut options = super::options(now);
    options.window_minutes = f.discovery.status_scan_window_minutes;
    options
}

pub fn observation(f: &Fixture, mint: char, age: Duration, price: f64, buy: bool) -> Result<()> {
    let mut event = if buy {
        super::swap_with_token(
            &rpc::key('Z'),
            &rpc::key(mint),
            &format!("price-{mint}"),
            8,
            f.now - age,
        )
    } else {
        super::sell_with_token(
            &rpc::key('Z'),
            &rpc::key(mint),
            &format!("price-{mint}"),
            8,
            f.now - age,
        )
    };
    if buy {
        event.amount_in = price * 10.0;
        event.amount_out = 10.0;
    } else {
        event.amount_in = 10.0;
        event.amount_out = price * 10.0;
    }
    f.store.insert_observed_swap(&event)?;
    super::insert_quality_for_token(&f.store, &rpc::key(mint), f.now, Some(1.0))?;
    Ok(())
}

pub fn token(mint: char, account: char, raw: &str) -> Value {
    rpc::account(
        &rpc::key('A'),
        &rpc::key(mint),
        &rpc::key(account),
        rpc::CLASSIC,
        raw,
        6,
    )
}

pub fn build(f: &Fixture, sol: u64, classic: Value, modern: Value) -> Result<DiscoveryV2Status> {
    let server = fixture::responses(sol, classic, modern);
    let options = options(f, f.now).with_live_portfolio_rpc_url(Some(server.url.clone()));
    let status = build_discovery_v2_status(&f.store, &f.discovery, &f.shadow, options)?;
    println!("B35 STATUS {}", serde_json::to_string(&status)?);
    rpc::assert_requests(&server.finish(), &rpc::key('A'), 3);
    Ok(status)
}

pub fn report(
    f: &Fixture,
    status: DiscoveryV2Status,
    now: DateTime<Utc>,
) -> Result<DiscoveryV2WalletReport> {
    let report = build_discovery_v2_wallet_report(
        &f.store,
        &f.discovery,
        &f.shadow,
        status,
        DiscoveryV2WalletReportOptions {
            now,
            limit: 10,
            include_rejected: true,
        },
        DiscoveryV2DecisionContext::new(&f.discovery, &f.shadow, &options(f, now)),
    )?;
    println!("B35 WALLET_REPORT {}", serde_json::to_string(&report)?);
    Ok(report)
}

pub fn assert_unknown(status: &DiscoveryV2Status, expected_value: f64) {
    let row = fixture::metric(status);
    assert!(status.candidate_wallets.is_empty(), "{row}");
    assert!(!status.production_green);
    assert_eq!(row["live_token_value_sol"], json!(expected_value));
    assert!(row["reject_reasons"]
        .as_array()
        .unwrap()
        .contains(&json!("live_portfolio_price_valuation_unknown")));
    for forbidden in [
        "capital_drained_after_window",
        "only_dust_positions",
        "only_illiquid_positions",
    ] {
        assert!(
            !row["reject_reasons"]
                .as_array()
                .unwrap()
                .contains(&json!(forbidden)),
            "{row}"
        );
    }
}
