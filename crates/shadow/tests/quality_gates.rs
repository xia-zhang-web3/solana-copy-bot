use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::ShadowConfig;
use copybot_core_types::SwapEvent;
use copybot_shadow::{FollowSnapshot, ShadowDropReason, ShadowProcessOutcome, ShadowService};
use copybot_storage_core::SqliteStore;
use std::collections::HashSet;
use std::path::Path;
use tempfile::tempdir;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[test]
fn quality_gate_uses_proxy_holders_when_rpc_cache_zero() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-quality-rpc-zero-proxy-pass.db");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    store.run_migrations(&migration_dir)?;

    let buy_ts = ts("2026-06-10T05:30:00Z");
    seed_proxy_holders(&store, "TokenMint", buy_ts, 5)?;
    store.upsert_token_quality_cache("TokenMint", Some(0), None, Some(120), buy_ts)?;
    activate_leader(&store, buy_ts)?;

    let service = ShadowService::new(quality_gate_config());
    let outcome = service.process_swap(
        &store,
        &leader_buy("sig-rpc-zero-proxy-pass", buy_ts),
        &follow_snapshot(&["leader-wallet"]),
        buy_ts + Duration::seconds(1),
    )?;

    expect_recorded(outcome, "proxy holders should override impossible rpc zero");
    Ok(())
}

#[test]
fn quality_gate_keeps_low_holders_when_rpc_zero_and_proxy_below_threshold() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-quality-rpc-zero-proxy-low.db");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    store.run_migrations(&migration_dir)?;

    let buy_ts = ts("2026-06-10T05:35:00Z");
    seed_proxy_holders(&store, "TokenMint", buy_ts, 4)?;
    store.upsert_token_quality_cache("TokenMint", Some(0), None, Some(120), buy_ts)?;
    activate_leader(&store, buy_ts)?;

    let service = ShadowService::new(quality_gate_config());
    let outcome = service.process_swap(
        &store,
        &leader_buy("sig-rpc-zero-proxy-low", buy_ts),
        &follow_snapshot(&["leader-wallet"]),
        buy_ts + Duration::seconds(1),
    )?;

    expect_dropped(
        outcome,
        ShadowDropReason::LowHolders,
        "proxy below threshold must remain fail-closed",
    );
    Ok(())
}

#[test]
fn quality_gate_keeps_nonzero_rpc_holders_authoritative() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-quality-rpc-nonzero.db");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    store.run_migrations(&migration_dir)?;

    let buy_ts = ts("2026-06-10T05:40:00Z");
    seed_proxy_holders(&store, "TokenMint", buy_ts, 6)?;
    store.upsert_token_quality_cache("TokenMint", Some(3), None, Some(120), buy_ts)?;
    activate_leader(&store, buy_ts)?;

    let service = ShadowService::new(quality_gate_config());
    let outcome = service.process_swap(
        &store,
        &leader_buy("sig-rpc-nonzero-low", buy_ts),
        &follow_snapshot(&["leader-wallet"]),
        buy_ts + Duration::seconds(1),
    )?;

    expect_dropped(
        outcome,
        ShadowDropReason::LowHolders,
        "nonzero rpc holders should keep previous authoritative behavior",
    );
    Ok(())
}

fn quality_gate_config() -> ShadowConfig {
    let mut cfg = ShadowConfig::default();
    cfg.copy_notional_sol = 0.5;
    cfg.min_leader_notional_sol = 0.25;
    cfg.quality_gates_enabled = true;
    cfg.min_token_age_seconds = 0;
    cfg.min_holders = 5;
    cfg.min_liquidity_sol = 0.0;
    cfg.min_volume_5m_sol = 0.0;
    cfg.min_unique_traders_5m = 0;
    cfg
}

fn leader_buy(signature: &str, ts_utc: DateTime<Utc>) -> SwapEvent {
    SwapEvent {
        wallet: "leader-wallet".to_string(),
        dex: "pumpswap".to_string(),
        token_in: SOL_MINT.to_string(),
        token_out: "TokenMint".to_string(),
        amount_in: 1.0,
        amount_out: 1000.0,
        signature: signature.to_string(),
        slot: 900,
        ts_utc,
        exact_amounts: None,
    }
}

fn seed_proxy_holders(
    store: &SqliteStore,
    token: &str,
    as_of: DateTime<Utc>,
    count: usize,
) -> Result<()> {
    for index in 0..count {
        store.insert_observed_swap(&SwapEvent {
            wallet: format!("proxy-wallet-{index}"),
            dex: "pumpswap".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: token.to_string(),
            amount_in: 0.1,
            amount_out: 100.0,
            signature: format!("sig-proxy-holder-{index}"),
            slot: 800 + index as u64,
            ts_utc: as_of - Duration::seconds((count - index) as i64),
            exact_amounts: None,
        })?;
    }
    Ok(())
}

fn activate_leader(store: &SqliteStore, buy_ts: DateTime<Utc>) -> Result<()> {
    store
        .activate_follow_wallet(
            "leader-wallet",
            buy_ts - Duration::seconds(30),
            "test-seed-follow",
        )
        .map(|_| ())
}

fn follow_snapshot(active_wallets: &[&str]) -> FollowSnapshot {
    FollowSnapshot::from_active_wallets(
        active_wallets
            .iter()
            .map(|wallet| wallet.to_string())
            .collect::<HashSet<_>>(),
    )
}

fn expect_recorded(outcome: ShadowProcessOutcome, message: &str) {
    if let ShadowProcessOutcome::Dropped(reason) = outcome {
        panic!("{message}: dropped with reason {}", reason.as_str());
    }
}

fn expect_dropped(outcome: ShadowProcessOutcome, expected: ShadowDropReason, message: &str) {
    match outcome {
        ShadowProcessOutcome::Dropped(reason) => assert_eq!(reason, expected, "{message}"),
        ShadowProcessOutcome::Recorded(_) => panic!("{message}: expected dropped, got recorded"),
    }
}

fn ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("timestamp")
        .with_timezone(&Utc)
}
