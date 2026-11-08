//! Public-pipeline seed: 800 eligibility swaps; no production thresholds are weakened.
use super::rpc;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_core_types::SwapEvent;
use copybot_discovery_v2::{DiscoveryV2BuildOptions, DiscoveryV2Status};
use copybot_storage_core::{ensure_discovery_v2_schema, SqliteDiscoveryStore};
use serde_json::Value;

pub const SOL: &str = "So11111111111111111111111111111111111111112";

#[derive(Clone, Copy, Debug)]
pub enum PriceCase {
    Fresh,
    Stale,
    Future,
    QualityFailed,
}

pub struct Fixture {
    _dir: tempfile::TempDir,
    pub store: SqliteDiscoveryStore,
    pub discovery: DiscoveryConfig,
    pub shadow: ShadowConfig,
    pub now: DateTime<Utc>,
}

impl Fixture {
    pub fn new(price: PriceCase) -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
        ensure_discovery_v2_schema(&store)?;
        let now = DateTime::parse_from_rfc3339("2026-11-07T12:00:00Z")?.with_timezone(&Utc);
        let mut swaps = vec![swap(
            "coverage",
            &rpc::key('D'),
            "coverage",
            1,
            now - Duration::minutes(125),
            true,
        )];
        // Every leader buy gets >=3SOL and ten distinct traders within its 900s rug horizon.
        // Leader has 80 trades, 41 buys, 39 profitable closes and two actionable open lots.
        for i in 0..800u64 {
            let cycle = i / 20;
            let row = i % 20;
            let leader = row == 0 || row == 10;
            let wallet = if leader {
                rpc::key('A')
            } else {
                format!("market-{}", row % 10)
            };
            let buy = leader && (row == 0 || cycle == 39);
            swaps.push(swap(
                &wallet,
                &rpc::key('D'),
                &format!("eligibility-{i}"),
                i + 10,
                now - Duration::minutes(40) + Duration::seconds(i as i64 * 3),
                buy,
            ));
        }
        let age = match price {
            PriceCase::Stale => Duration::minutes(121),
            PriceCase::Future => Duration::seconds(-1),
            _ => Duration::minutes(2),
        };
        let mut observed = swap(
            &rpc::key('Z'),
            &rpc::key('E'),
            "held-price",
            900,
            now - age,
            true,
        );
        observed.amount_in = 1.0;
        observed.amount_out = 10.0; // observed SOL/token = .1, not a quote or executable price.
        swaps.push(observed);
        store.insert_observed_swaps_batch(&swaps)?;
        store.upsert_token_quality_cache(&rpc::key('D'), Some(5), Some(1.0), Some(60), now)?;
        store.upsert_token_quality_cache(
            &rpc::key('E'),
            Some(5),
            Some(if matches!(price, PriceCase::QualityFailed) {
                0.5
            } else {
                1.0
            }),
            Some(60),
            now,
        )?;
        let mut discovery = DiscoveryConfig::default();
        discovery.status_scan_window_minutes = 120;
        discovery.scoring_window_days = 1;
        discovery.min_trades = 10;
        discovery.min_active_days = 1;
        discovery.min_buy_count = 10;
        discovery.min_leader_notional_sol = 0.5;
        discovery.min_score = 0.4;
        discovery.min_tradable_ratio = 0.25;
        discovery.max_tx_per_minute = 50;
        discovery.max_rug_ratio = 0.60;
        discovery.rug_lookahead_seconds = 900;
        discovery.thin_market_min_volume_sol = 3.0;
        discovery.thin_market_min_unique_traders = 10;
        discovery.require_open_positions_for_publication = true;
        discovery.metric_snapshot_interval_seconds = 1800;
        discovery.follow_top_n = 1; // isolated accepted private one-leader cohort.
        discovery.live_portfolio_gate_enabled = true;
        discovery.live_portfolio_max_wallets = 1;
        discovery.live_portfolio_max_token_accounts = 8;
        discovery.min_live_sol_balance = 0.05;
        discovery.min_live_portfolio_value_sol = 0.05;
        let mut shadow = ShadowConfig::default();
        shadow.quality_gates_enabled = true;
        shadow.min_token_age_seconds = 30;
        shadow.min_holders = 5;
        shadow.min_liquidity_sol = 1.0;
        shadow.min_volume_5m_sol = 0.5;
        shadow.min_unique_traders_5m = 1;
        Ok(Self {
            _dir: dir,
            store,
            discovery,
            shadow,
            now,
        })
    }

    pub fn options(&self, server: &rpc::RpcStub) -> DiscoveryV2BuildOptions {
        DiscoveryV2BuildOptions {
            now: self.now,
            window_minutes: 120,
            max_tail_lag_seconds: 1200,
            max_rows: 2000,
            time_budget_ms: 5000,
            execution_enabled: false,
            live_portfolio_rpc_url: Some(server.url.clone()),
        }
    }
}

fn swap(
    wallet: &str,
    mint: &str,
    signature: &str,
    slot: u64,
    ts_utc: DateTime<Utc>,
    buy: bool,
) -> SwapEvent {
    SwapEvent {
        wallet: wallet.into(),
        dex: "local-pipeline-fixture".into(),
        token_in: if buy { SOL.into() } else { mint.into() },
        token_out: if buy { mint.into() } else { SOL.into() },
        amount_in: if buy { 1.0 } else { 10.0 },
        amount_out: if buy { 10.0 } else { 1.2 },
        signature: signature.into(),
        slot,
        ts_utc,
        exact_amounts: None,
    }
}

pub fn metric(status: &DiscoveryV2Status) -> Value {
    let row = status
        .wallet_metrics
        .iter()
        .find(|row| row.wallet_id == rpc::key('A'))
        .unwrap();
    // Prove the unchanged pre-live eligibility gates were actually passed by this fixture.
    assert_eq!(row.trades, 80);
    assert_eq!(row.buys, 41);
    assert!(row.max_buy_notional_sol >= 0.5);
    assert!(row.tradable_ratio >= 0.25);
    assert_eq!(row.rug_lookahead_unevaluated, 0);
    assert!(
        row.reject_reasons
            .iter()
            .all(|r| r.starts_with("live_portfolio_")
                || matches!(
                    r.as_str(),
                    "only_dust_positions"
                        | "only_illiquid_positions"
                        | "capital_drained_after_window"
                )),
        "pre-live reject: {:?}",
        row.reject_reasons
    );
    serde_json::to_value(row).unwrap()
}
