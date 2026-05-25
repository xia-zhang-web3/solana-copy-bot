use super::*;
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::ShadowConfig;
use copybot_core_types::{
    ExactSwapAmounts, Lamports, SwapEvent, TokenQuantity, COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE,
};
use copybot_storage_core::SqliteStore;
use std::path::Path;
use tempfile::tempdir;

use crate::types::SOL_MINT;

#[path = "tests_parts/02_exact_and_gates.rs"]
mod exact_and_gates;
#[path = "tests_parts/03_outcome_ext.rs"]
mod outcome_ext;
#[path = "tests_parts/01_process.rs"]
mod process;

use outcome_ext::*;
use process::*;

#[test]
fn buy_after_recent_same_wallet_token_sell_is_dropped() -> Result<()> {
    let temp = tempdir().context("failed to create tempdir")?;
    let db_path = temp.path().join("shadow-recent-sell-cooldown.db");
    let mut store = SqliteStore::open(Path::new(&db_path))?;
    let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    store.run_migrations(&migration_dir)?;

    let follow = follow_snapshot(&["leader-wallet"]);

    let mut cfg = ShadowConfig::default();
    cfg.copy_notional_sol = 0.5;
    cfg.min_leader_notional_sol = 0.25;
    cfg.recent_sell_cooldown_enabled = true;
    cfg.recent_sell_cooldown_minutes = 120;
    cfg.quality_gates_enabled = false;
    let service = ShadowService::new(cfg);

    let sell_ts = DateTime::parse_from_rfc3339("2026-02-12T12:00:00Z")
        .expect("timestamp")
        .with_timezone(&Utc);
    let blocked_buy_ts = sell_ts + Duration::minutes(30);
    let allowed_buy_ts = sell_ts + Duration::minutes(121);
    store.activate_follow_wallet(
        "leader-wallet",
        sell_ts - Duration::seconds(30),
        "test-seed-follow",
    )?;

    let sell = SwapEvent {
        wallet: "leader-wallet".to_string(),
        dex: "pumpswap".to_string(),
        token_in: "TokenMint".to_string(),
        token_out: SOL_MINT.to_string(),
        amount_in: 1000.0,
        amount_out: 1.0,
        signature: "sig-recent-sell".to_string(),
        slot: 1,
        ts_utc: sell_ts,
        exact_amounts: None,
    };
    service
        .process_swap(&store, &sell, &follow, sell_ts + Duration::seconds(1))?
        .expect_recorded("followed sell should still record signal history");

    let blocked_buy = SwapEvent {
        wallet: "leader-wallet".to_string(),
        dex: "pumpswap".to_string(),
        token_in: SOL_MINT.to_string(),
        token_out: "TokenMint".to_string(),
        amount_in: 1.0,
        amount_out: 1000.0,
        signature: "sig-blocked-buy-after-sell".to_string(),
        slot: 2,
        ts_utc: blocked_buy_ts,
        exact_amounts: None,
    };
    service
        .process_swap(
            &store,
            &blocked_buy,
            &follow,
            blocked_buy_ts + Duration::seconds(1),
        )?
        .expect_dropped(
            ShadowDropReason::RecentSellCooldown,
            "recent sell should suppress same wallet/token buy",
        );
    assert_eq!(
        store
            .list_copy_signals_by_status("shadow_recorded", 10)?
            .len(),
        1,
        "dropped buy must not insert a copy signal"
    );

    let allowed_buy = SwapEvent {
        wallet: "leader-wallet".to_string(),
        dex: "pumpswap".to_string(),
        token_in: SOL_MINT.to_string(),
        token_out: "TokenMint".to_string(),
        amount_in: 1.0,
        amount_out: 1000.0,
        signature: "sig-allowed-buy-after-cooldown".to_string(),
        slot: 3,
        ts_utc: allowed_buy_ts,
        exact_amounts: None,
    };
    service
        .process_swap(
            &store,
            &allowed_buy,
            &follow,
            allowed_buy_ts + Duration::seconds(1),
        )?
        .expect_recorded("buy after cooldown should be allowed");

    Ok(())
}
