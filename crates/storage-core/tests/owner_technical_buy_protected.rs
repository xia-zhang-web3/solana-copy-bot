use anyhow::Result;
use chrono::{Duration, TimeZone, Utc};
use copybot_storage_core::{
    ExecutionCanaryDispatch, ExecutionDispatchClaim, OwnerTechnicalBuyIntent,
    ProtectedCapitalClaim, SqliteStore, TinyBudgetClaim, EXECUTION_SIMULATION_STATUS_PASSED,
};

fn intent() -> OwnerTechnicalBuyIntent {
    let activated_at = Utc.with_ymd_and_hms(2026, 9, 24, 12, 0, 0).unwrap();
    OwnerTechnicalBuyIntent {
        intent_id: "protected-owner".into(),
        run_id: "protected-run".into(),
        wallet: "synthetic-wallet".into(),
        signer: "synthetic-wallet".into(),
        genesis_hash: "synthetic-genesis".into(),
        mint: "synthetic-usdc".into(),
        amount_lamports: 10_000_000,
        route: "jupiter_swap_instructions".into(),
        activated_at,
        expires_at: activated_at + Duration::minutes(15),
        authority_sha256: "a".repeat(64),
        max_priority_fee_lamports: 50_000,
        min_reserve_lamports: 50_000_001,
        max_slippage_bps: 50,
        max_daily_loss_lamports: 20_000_000,
        max_open_positions: 1,
        max_buy_count: 1,
    }
}
fn open(path: &std::path::Path) -> Result<SqliteStore> {
    let mut store = SqliteStore::open(path)?;
    store.run_migrations(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations"),
    )?;
    Ok(store)
}

#[test]
fn owner_protected_anchor_is_bound_and_immutable_after_restart_or_inflow() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    let store = open(&path)?;
    let i = intent();
    store.register_owner_technical_buy_intent(&i)?;
    let now = i.activated_at + Duration::seconds(1);
    let order = store
        .reserve_owner_technical_buy_order(&i.intent_id, || Ok(now))?
        .order;
    let policy = store.prepare_tiny_native_policy_for_owner_buy(
        &i.run_id,
        &i.wallet,
        175_200_031,
        50_000_001,
        120,
        now,
        &i,
        &order,
        || Ok(now),
    )?;
    assert_eq!(policy.floor_lamports, 160_200_031);
    assert_eq!(policy.allowance, 15_000_000);
    drop(store);
    let store = SqliteStore::open(&path)?;
    let after = now + Duration::seconds(1);
    assert_eq!(
        store.prepare_tiny_native_policy_for_owner_buy(
            &i.run_id,
            &i.wallet,
            1_175_200_031,
            50_000_001,
            121,
            after,
            &i,
            &order,
            || Ok(after),
        )?,
        policy
    );
    let mut changed = i.clone();
    changed.amount_lamports -= 1;
    assert!(store
        .prepare_tiny_native_policy_for_owner_buy(
            &i.run_id,
            &i.wallet,
            175_200_031,
            50_000_001,
            121,
            after,
            &changed,
            &order,
            || Ok(after),
        )
        .is_err());
    assert_eq!(
        store.tiny_native_policy(&i.run_id, &i.wallet, after)?,
        policy
    );
    Ok(())
}

#[test]
fn owner_protected_stale_or_expiring_authority_cannot_activate() -> Result<()> {
    for fault in ["stale", "expiry", "reserve", "order"] {
        let dir = tempfile::tempdir()?;
        let store = open(&dir.path().join("db"))?;
        let i = intent();
        store.register_owner_technical_buy_intent(&i)?;
        let now = i.activated_at + Duration::seconds(40);
        let mut order = store
            .reserve_owner_technical_buy_order(&i.intent_id, || Ok(now))?
            .order;
        if fault == "order" {
            order.route = "substituted".into();
        }
        let time = if fault == "stale" {
            now - Duration::seconds(31)
        } else {
            now
        };
        let reserve = if fault == "reserve" {
            50_000_000
        } else {
            50_000_001
        };
        let calls = std::cell::Cell::new(0);
        let result = store.prepare_tiny_native_policy_for_owner_buy(
            &i.run_id,
            &i.wallet,
            175_200_031,
            reserve,
            120,
            time,
            &i,
            &order,
            || {
                calls.set(calls.get() + 1);
                Ok(if fault == "expiry" && calls.get() > 1 {
                    i.expires_at
                } else {
                    now
                })
            },
        );
        assert!(result.is_err(), "{fault}");
        assert!(store.load_tiny_experiment(now)?.is_none(), "{fault}");
    }
    Ok(())
}

#[test]
fn owner_protected_dispatch_requires_exact_decoded_amount_and_stays_single_use() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    let store = open(&path)?;
    let i = intent();
    store.register_owner_technical_buy_intent(&i)?;
    let now = i.activated_at + Duration::seconds(1);
    let order = store
        .reserve_owner_technical_buy_order(&i.intent_id, || Ok(now))?
        .order;
    let policy = store.prepare_tiny_native_policy_for_owner_buy(
        &i.run_id,
        &i.wallet,
        175_200_031,
        50_000_001,
        120,
        now,
        &i,
        &order,
        || Ok(now),
    )?;
    store.mark_execution_canary_built(&order.order_id, now)?;
    let order = store.mark_execution_canary_simulated(
        &order.order_id,
        now,
        EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    let dispatch = ExecutionCanaryDispatch {
        order_id: order.order_id.clone(),
        signal_id: order.signal_id.clone(),
        client_order_id: order.client_order_id.clone(),
        route: order.route.clone(),
        attempt: 1,
        wallet: i.wallet.clone(),
        token: i.mint.clone(),
        side: "buy".into(),
        tx_signature: "synthetic-signature".into(),
        transaction_sha256: "b".repeat(64),
        message_sha256: "c".repeat(64),
    };
    let budget = TinyBudgetClaim {
        experiment_id: i.run_id.clone(),
        wallet: i.wallet.clone(),
        tx_signature: dispatch.tx_signature.clone(),
        message_sha256: dispatch.message_sha256.clone(),
        transaction_sha256: dispatch.transaction_sha256.clone(),
        buy_lamports: Some(10_000_000),
        protected_capital: Some(ProtectedCapitalClaim {
            policy: policy.clone(),
            requested_lamports: 10_000_000,
            floor_lamports: policy.floor_lamports,
            request_sha256: "d".repeat(64),
        }),
        total_fee: 19_000,
        priority_fee: 2_000,
        fee_slot: 120,
    };
    for bad in [None, Some(10_000_001)] {
        let mut claim = budget.clone();
        claim.buy_lamports = bad;
        assert!(store
            .claim_owner_technical_buy_dispatch(&order, &dispatch, &claim, || Ok(now))
            .is_err());
        assert!(store
            .load_execution_canary_dispatch(&order.order_id)?
            .is_none());
    }
    assert_eq!(
        store.claim_owner_technical_buy_dispatch(&order, &dispatch, &budget, || Ok(now))?,
        ExecutionDispatchClaim::New
    );
    drop(store);
    let store = SqliteStore::open(&path)?;
    assert_eq!(
        store.claim_owner_technical_buy_dispatch(&order, &dispatch, &budget, || Ok(i
            .expires_at
            + Duration::seconds(1)))?,
        ExecutionDispatchClaim::Existing
    );
    let conn = rusqlite::Connection::open(path)?;
    let values: (u64, u64, String) = conn.query_row(
        "SELECT r.buy_lamports,e.requested_lamports,e.floor_lamports
        FROM execution_tiny_reservations r JOIN execution_tiny_capital_evidence e USING(order_id)",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    assert_eq!(values, (10_000_000, 10_000_000, "160200031".into()));
    assert!(store.execution_canary_unresolved_buy()?);
    Ok(())
}
