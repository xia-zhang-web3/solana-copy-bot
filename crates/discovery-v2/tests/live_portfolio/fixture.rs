use super::rpc::{self, Reply, RpcStub, CLASSIC};
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::{DiscoveryConfig, ShadowConfig};
use copybot_discovery_v2::{build_discovery_v2_status, DiscoveryV2BuildOptions, DiscoveryV2Status};
use copybot_storage_core::SqliteDiscoveryStore;
use serde_json::{json, Value};

pub struct Fixture {
    _dir: tempfile::TempDir,
    pub store: SqliteDiscoveryStore,
    pub discovery: DiscoveryConfig,
    pub shadow: ShadowConfig,
    pub now: DateTime<Utc>,
}

impl Fixture {
    pub fn reopen(&self) -> Result<SqliteDiscoveryStore> {
        SqliteDiscoveryStore::open(self._dir.path().join("runtime.db"))
    }

    pub fn new() -> Result<Self> {
        let (_dir, store) = super::test_store()?;
        let now = DateTime::parse_from_rfc3339("2026-09-07T12:00:00Z")?.with_timezone(&Utc);
        store.insert_observed_swaps_batch(&[
            super::tail_coverage_swap("floor", 9, now - Duration::hours(25)),
            super::swap_with_token(
                &rpc::key('A'),
                &rpc::key('D'),
                "leader-buy",
                10,
                now - Duration::minutes(4),
            ),
            super::tail_coverage_swap("tail", 11, now - Duration::minutes(1)),
        ])?;
        super::insert_quality_for_token(&store, &rpc::key('D'), now, Some(1.0))?;
        let (mut discovery, shadow) = super::strict_policy();
        discovery.live_portfolio_gate_enabled = true;
        discovery.live_portfolio_max_wallets = 1;
        discovery.live_portfolio_max_token_accounts = 8;
        discovery.min_live_sol_balance = 0.25;
        discovery.min_live_portfolio_value_sol = 0.25;
        Ok(Self {
            _dir,
            store,
            discovery,
            shadow,
            now,
        })
    }

    pub fn options(&self, rpc: &RpcStub) -> DiscoveryV2BuildOptions {
        super::options(self.now).with_live_portfolio_rpc_url(Some(rpc.url.clone()))
    }

    pub fn build(&self, rpc: &RpcStub) -> Result<DiscoveryV2Status> {
        build_discovery_v2_status(
            &self.store,
            &self.discovery,
            &self.shadow,
            self.options(rpc),
        )
    }
}

pub fn token(program: &str, raw: &str) -> Value {
    rpc::account(
        &rpc::key('A'),
        &rpc::key('D'),
        &rpc::key(if program == CLASSIC { 'J' } else { 'K' }),
        program,
        raw,
        6,
    )
}

pub fn responses(sol: u64, classic: Value, token_2022: Value) -> RpcStub {
    RpcStub::start(move |request| -> Reply {
        let value = if request["method"] == "getBalance" {
            json!(sol)
        } else if request["params"][1]["programId"] == CLASSIC {
            classic.clone()
        } else {
            token_2022.clone()
        };
        rpc::result(value).into()
    })
}

pub fn metric(status: &DiscoveryV2Status) -> Value {
    serde_json::to_value(
        status
            .wallet_metrics
            .iter()
            .find(|m| m.wallet_id == rpc::key('A'))
            .unwrap(),
    )
    .unwrap()
}

pub fn assert_outcome(status: &DiscoveryV2Status, reason: Option<&str>, rpc_failure: bool) {
    let row = metric(status);
    let live = status.live_portfolio.as_ref().unwrap();
    assert_eq!(
        status.production_green,
        reason.is_none(),
        "{row} blockers={:?}",
        status.blockers
    );
    assert_eq!(
        status.candidate_wallets.len(),
        usize::from(reason.is_none())
    );
    assert_eq!(live.rpc_failures, usize::from(rpc_failure));
    assert_eq!(
        status.filters.total_wallets, 2,
        "missing quality tail remains in denominator"
    );
    if let Some(reason) = reason {
        assert!(
            row["reject_reasons"]
                .as_array()
                .unwrap()
                .contains(&json!(reason)),
            "{row}"
        );
    }
    println!(
        "B29 outcome={} live={} metric={}",
        reason.unwrap_or("accepted"),
        serde_json::to_string(live).unwrap(),
        row
    );
}
