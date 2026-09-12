use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::ShadowConfig;
use copybot_core_types::{ExactSwapAmounts, SwapEvent, TokenQuantity};
use copybot_shadow::{FollowSnapshot, ShadowDropReason, ShadowProcessOutcome, ShadowService};
use copybot_storage_core::SqliteStore;
use tempfile::{tempdir, TempDir};

const SOL: &str = "So11111111111111111111111111111111111111112";

fn setup() -> Result<(TempDir, SqliteStore, ShadowService, DateTime<Utc>)> {
    let dir = tempdir()?;
    let mut store = SqliteStore::open(dir.path().join("shadow.db"))?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let mut cfg = ShadowConfig::default();
    cfg.enabled = true;
    cfg.quality_gates_enabled = false;
    cfg.min_leader_notional_sol = 0.25;
    cfg.copy_notional_sol = 0.5;
    cfg.max_signal_lag_seconds = 30;
    Ok((
        dir,
        store,
        ShadowService::new(cfg),
        "2026-09-05T12:00:00Z".parse()?,
    ))
}
fn follow() -> FollowSnapshot {
    FollowSnapshot::from_active_wallets(["wallet".into()].into())
}
fn event(ts: DateTime<Utc>, side: &str, notional: f64, exact: bool) -> SwapEvent {
    let buy = side == "buy";
    let (amount_in, amount_out) = if buy {
        (notional, 10.0)
    } else {
        (10.0, notional)
    };
    SwapEvent {
        wallet: "wallet".into(),
        dex: "pumpswap".into(),
        token_in: if buy { SOL } else { "token" }.into(),
        token_out: if buy { "token" } else { SOL }.into(),
        amount_in,
        amount_out,
        signature: format!("sig-{side}"),
        slot: 10,
        ts_utc: ts,
        exact_amounts: exact.then(|| ExactSwapAmounts {
            amount_in_raw: (if buy { (notional * 1e9) as u64 } else { 10000 }).to_string(),
            amount_in_decimals: if buy { 9 } else { 3 },
            amount_out_raw: (if buy { 10000 } else { (notional * 1e9) as u64 }).to_string(),
            amount_out_decimals: if buy { 3 } else { 9 },
        }),
    }
}
fn recorded(outcome: ShadowProcessOutcome) -> copybot_shadow::ShadowSignalResult {
    match outcome {
        ShadowProcessOutcome::Recorded(value) => value,
        other => panic!("expected recorded: {other:?}"),
    }
}
fn dropped(outcome: ShadowProcessOutcome, reason: ShadowDropReason) {
    assert!(
        matches!(outcome, ShadowProcessOutcome::Dropped(actual) if actual == reason),
        "{outcome:?}"
    );
}

#[test]
fn followed_open_risk_sell_bypasses_only_entry_notional_and_lag() -> Result<()> {
    for exact in [false, true] {
        for (notional, lag, buy_reason) in [
            (0.1, 1, ShadowDropReason::BelowNotional),
            (0.5, 120, ShadowDropReason::LagExceeded),
            (0.1, 120, ShadowDropReason::BelowNotional),
        ] {
            let (_dir, store, service, ts) = setup()?;
            store.insert_shadow_lot_exact(
                "wallet",
                "token",
                20.0,
                exact.then_some(TokenQuantity::new(20000, 3)),
                0.2,
                ts,
            )?;
            let sell = event(ts + Duration::seconds(1), "sell", notional, exact);
            let now = sell.ts_utc + Duration::seconds(lag);
            let result = recorded(service.process_swap(&store, &sell, &follow(), now)?);
            assert_eq!(result.closed_qty, 10.0);
            assert_eq!(result.has_open_lots_after_signal, Some(true));
            let signal = store
                .load_copy_signal_by_signal_id(&result.signal_id)?
                .unwrap();
            assert_eq!(signal.ts, sell.ts_utc);
            assert_eq!(signal.notional_sol, notional);
            let remaining = &store.list_shadow_lots("wallet", "token")?[0];
            assert_eq!(remaining.qty, 10.0);
            assert_eq!(
                remaining.qty_exact,
                exact.then_some(TokenQuantity::new(10000, 3))
            );
            dropped(
                service.process_swap(
                    &store,
                    &event(sell.ts_utc, "buy", notional, exact),
                    &follow(),
                    now,
                )?,
                buy_reason,
            );
        }
    }
    Ok(())
}

#[test]
fn sell_exemption_requires_same_pair_and_risk_at_event_time() -> Result<()> {
    for case in ["no_lot", "wrong_wallet", "wrong_mint", "future", "dust"] {
        let (_dir, store, service, ts) = setup()?;
        if case != "no_lot" {
            store.insert_shadow_lot(
                if case == "wrong_wallet" {
                    "other"
                } else {
                    "wallet"
                },
                if case == "wrong_mint" {
                    "other"
                } else {
                    "token"
                },
                if case == "dust" { 1e-13 } else { 10.0 },
                0.1,
                if case == "future" {
                    ts + Duration::seconds(2)
                } else {
                    ts
                },
            )?;
        }
        let sell = event(ts + Duration::seconds(1), "sell", 0.1, false);
        dropped(
            service.process_swap(&store, &sell, &follow(), ts + Duration::minutes(10))?,
            ShadowDropReason::BelowNotional,
        );
        dropped(
            service.process_swap(
                &store,
                &sell,
                &FollowSnapshot::default(),
                ts + Duration::minutes(10),
            )?,
            ShadowDropReason::NotFollowed,
        );
    }
    Ok(())
}

#[test]
fn unfollowed_exit_and_partial_replay_survive_reopen_without_second_effect() -> Result<()> {
    let (dir, store, service, ts) = setup()?;
    store.insert_shadow_lot_exact(
        "wallet",
        "token",
        20.0,
        Some(TokenQuantity::new(20000, 3)),
        0.2,
        ts,
    )?;
    let sell = event(ts + Duration::seconds(1), "sell", 0.1, true);
    let mut demoted = follow();
    demoted.active.clear();
    demoted
        .promoted_at
        .insert("wallet".into(), ts - Duration::hours(1));
    demoted.demoted_at.insert("wallet".into(), ts);
    assert_eq!(
        recorded(service.process_swap(&store, &sell, &demoted, ts + Duration::minutes(10))?)
            .closed_qty,
        10.0
    );
    drop(store);
    let store = SqliteStore::open(dir.path().join("shadow.db"))?;
    dropped(
        service.process_swap(&store, &sell, &demoted, ts + Duration::minutes(11))?,
        ShadowDropReason::DuplicateSignal,
    );
    let recovery =
        service.process_restart_recovery_sell(&store, &sell, ts + Duration::minutes(12))?;
    if let ShadowProcessOutcome::Recorded(result) = recovery {
        assert_eq!(result.closed_qty, 0.0);
    }
    assert_eq!(
        store.list_shadow_lots("wallet", "token")?[0].qty_exact,
        Some(TokenQuantity::new(10000, 3))
    );
    Ok(())
}

#[test]
fn followed_sell_with_future_only_lot_keeps_ordinary_permissions_and_no_close() -> Result<()> {
    let (_dir, store, service, ts) = setup()?;
    store.insert_shadow_lot("wallet", "token", 10.0, 0.1, ts + Duration::seconds(2))?;
    let sell = event(ts + Duration::seconds(1), "sell", 0.5, false);
    let result = recorded(service.process_swap(&store, &sell, &follow(), sell.ts_utc)?);
    assert_eq!(result.closed_qty, 0.0);
    assert_eq!(store.list_shadow_lots("wallet", "token")?[0].qty, 10.0);
    assert!(store
        .list_execution_quote_canary_close_candidates_for_signal(&result.signal_id, 10)?
        .is_empty());
    Ok(())
}

#[test]
fn disabled_and_temporal_buy_gates_remain_closed() -> Result<()> {
    let (_dir, store, _service, ts) = setup()?;
    store.insert_shadow_lot("wallet", "token", 10.0, 0.1, ts)?;
    let mut cfg = ShadowConfig::default();
    cfg.enabled = false;
    dropped(
        ShadowService::new(cfg).process_swap(
            &store,
            &event(ts, "sell", 0.1, false),
            &follow(),
            ts,
        )?,
        ShadowDropReason::Disabled,
    );
    let (_dir, store, service, ts) = setup()?;
    let mut future_follow = follow();
    future_follow
        .promoted_at
        .insert("wallet".into(), ts + Duration::seconds(1));
    dropped(
        service.process_swap(&store, &event(ts, "buy", 0.5, false), &future_follow, ts)?,
        ShadowDropReason::NotFollowed,
    );
    Ok(())
}
