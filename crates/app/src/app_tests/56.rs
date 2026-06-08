use super::*;

#[test]
fn confirmed_buy_fill_accounting_opens_only_after_confirmation() -> Result<()> {
    let db_path = unique_confirmed_fill_test_path("buy-confirmed");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let order_id = simulated_confirmed_fill_order(&store, "buy-confirmed", "buy", now)?;

    let error = crate::execution_submit_adapter::record_confirmed_fill_accounting(
        &store,
        buy_fill(&order_id, now + chrono::Duration::seconds(3)),
    )
    .expect_err("unconfirmed order must not open fill accounting");
    store.mark_execution_canary_submitted(
        &order_id,
        now + chrono::Duration::seconds(4),
        "tx-buy",
    )?;
    store.mark_execution_canary_confirmed(&order_id, now + chrono::Duration::seconds(5))?;
    let opened = crate::execution_submit_adapter::record_confirmed_fill_accounting(
        &store,
        buy_fill(&order_id, now + chrono::Duration::seconds(6)),
    )?;
    let repeated = crate::execution_submit_adapter::record_confirmed_fill_accounting(
        &store,
        buy_fill(&order_id, now + chrono::Duration::seconds(7)),
    )?;

    assert!(format!("{error:#}").contains("requires confirmed status"));
    assert_eq!(opened.buy_opened, 1);
    assert_eq!(opened.buy_existing, 0);
    assert_eq!(repeated.buy_opened, 0);
    assert_eq!(repeated.buy_existing, 1);
    assert_eq!(store.execution_canary_open_position_count()?, 1);

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[test]
fn confirmed_sell_fill_accounting_closes_only_after_confirmation() -> Result<()> {
    let db_path = unique_confirmed_fill_test_path("sell-confirmed");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let buy_order_id = simulated_confirmed_fill_order(&store, "buy-owned", "buy", now)?;
    store.mark_execution_canary_submitted(
        &buy_order_id,
        now + chrono::Duration::seconds(3),
        "tx-buy",
    )?;
    store.mark_execution_canary_confirmed(&buy_order_id, now + chrono::Duration::seconds(4))?;
    crate::execution_submit_adapter::record_confirmed_fill_accounting(
        &store,
        buy_fill(&buy_order_id, now + chrono::Duration::seconds(5)),
    )?;
    let sell_order_id = simulated_confirmed_fill_order(
        &store,
        "sell-owned",
        "sell",
        now + chrono::Duration::minutes(1),
    )?;

    let error = crate::execution_submit_adapter::record_confirmed_fill_accounting(
        &store,
        sell_fill(
            &sell_order_id,
            now + chrono::Duration::minutes(1) + chrono::Duration::seconds(3),
        ),
    )
    .expect_err("unconfirmed sell order must not close fill accounting");
    store.mark_execution_canary_submitted(
        &sell_order_id,
        now + chrono::Duration::minutes(1) + chrono::Duration::seconds(4),
        "tx-sell",
    )?;
    store.mark_execution_canary_confirmed(
        &sell_order_id,
        now + chrono::Duration::minutes(1) + chrono::Duration::seconds(5),
    )?;
    let closed = crate::execution_submit_adapter::record_confirmed_fill_accounting(
        &store,
        sell_fill(
            &sell_order_id,
            now + chrono::Duration::minutes(1) + chrono::Duration::seconds(6),
        ),
    )?;

    assert!(format!("{error:#}").contains("requires confirmed status"));
    assert_eq!(closed.sell_closed, 1);
    assert_eq!(closed.sell_no_position, 0);
    assert_eq!(closed.closed_qty, 10.0);
    assert!((closed.pnl_sol - 0.2).abs() < 1e-9);
    assert!(store
        .load_execution_canary_open_position("TokenMint")?
        .is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn buy_fill(
    order_id: &str,
    fill_ts: chrono::DateTime<Utc>,
) -> crate::execution_submit_adapter::ExecutionConfirmedFill {
    crate::execution_submit_adapter::ExecutionConfirmedFill::Buy(
        crate::execution_submit_adapter::ExecutionConfirmedBuyFill {
            order_id: order_id.to_string(),
            token: "TokenMint".to_string(),
            qty: 10.0,
            qty_exact: Some(copybot_core_types::TokenQuantity::new(10_000, 3)),
            cost_sol: 1.0,
            fill_ts,
        },
    )
}

fn sell_fill(
    order_id: &str,
    fill_ts: chrono::DateTime<Utc>,
) -> crate::execution_submit_adapter::ExecutionConfirmedFill {
    crate::execution_submit_adapter::ExecutionConfirmedFill::Sell(
        crate::execution_submit_adapter::ExecutionConfirmedSellFill {
            order_id: order_id.to_string(),
            token: "TokenMint".to_string(),
            target_qty: 10.0,
            target_qty_exact: Some(copybot_core_types::TokenQuantity::new(10_000, 3)),
            exit_price_sol: 0.12,
            dust_qty_epsilon: 0.001,
            fill_ts,
        },
    )
}

fn simulated_confirmed_fill_order(
    store: &SqliteStore,
    name: &str,
    side: &str,
    now: chrono::DateTime<Utc>,
) -> Result<String> {
    let signal = confirmed_fill_signal(name, side, now);
    store.insert_copy_signal(&signal)?;
    let reserve = store.reserve_execution_canary_order(&signal.signal_id, "metis-canary", now)?;
    store
        .mark_execution_canary_built(&reserve.order.order_id, now + chrono::Duration::seconds(1))?;
    store.mark_execution_canary_simulated(
        &reserve.order.order_id,
        now + chrono::Duration::seconds(2),
        copybot_storage_core::EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    Ok(reserve.order.order_id)
}

fn confirmed_fill_signal(
    name: &str,
    side: &str,
    ts: chrono::DateTime<Utc>,
) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: format!("shadow:sig-confirmed-fill:leader-wallet:{name}:TokenMint"),
        wallet_id: "leader-wallet".to_string(),
        side: side.to_string(),
        token: "TokenMint".to_string(),
        notional_sol: 0.2,
        notional_lamports: Some(Lamports::new(200_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
        ts,
        status: "shadow_recorded".to_string(),
    }
}

fn unique_confirmed_fill_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-confirmed-fill-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
