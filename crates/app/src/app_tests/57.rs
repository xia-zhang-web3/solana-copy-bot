use super::*;
use crate::execution_submit_adapter::ExecutionConfirmationTracker;

#[test]
fn confirmation_tracker_pending_keeps_submitted_order_without_fill() -> Result<()> {
    let db_path = unique_confirmation_tracker_test_path("pending");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let order_id = submitted_confirmation_order(&store, "buy-pending", "buy", "tx-pending", now)?;
    let request: crate::execution_submit_adapter::ExecutionConfirmationRequest =
        crate::execution_submit_adapter::build_confirmation_request_from_order(
            &store,
            &order_id,
            now + chrono::Duration::seconds(4),
        )?;
    let tracker = crate::execution_submit_adapter::NoSendExecutionConfirmationTracker;
    let outcome = tracker.check_confirmation(&request)?;
    let record: crate::execution_submit_adapter::ExecutionConfirmationTrackerRecordOutcome =
        crate::execution_submit_adapter::record_confirmation_tracker_outcome(
            &store,
            &request,
            outcome,
            confirmation_buy_fill(&order_id, now + chrono::Duration::seconds(5)),
        )?;
    let order = store
        .load_execution_canary_order(&order_id)?
        .expect("order should exist");

    assert_eq!(record.pending, 1);
    assert_eq!(record.confirmed, 0);
    assert_eq!(
        record.reason.as_deref(),
        Some("confirmation_tracker_no_send")
    );
    assert!(record.fill_accounting.is_none());
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMITTED
    );
    assert!(store
        .load_execution_canary_open_position("TokenMint")?
        .is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[test]
fn confirmation_tracker_confirmed_buy_marks_order_and_records_fill() -> Result<()> {
    let db_path = unique_confirmation_tracker_test_path("confirmed-buy");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let order_id = submitted_confirmation_order(&store, "buy-confirmed", "buy", "tx-buy", now)?;
    let request: crate::execution_submit_adapter::ExecutionConfirmationRequest =
        crate::execution_submit_adapter::build_confirmation_request_from_order(
            &store,
            &order_id,
            now + chrono::Duration::seconds(4),
        )?;
    let tracker = crate::execution_submit_adapter::MockConfirmedExecutionConfirmationTracker {
        slot: Some(42),
        confirmation_status: "finalized".to_string(),
        confirmed_at: now + chrono::Duration::seconds(5),
    };
    let outcome = tracker.check_confirmation(&request)?;
    let record: crate::execution_submit_adapter::ExecutionConfirmationTrackerRecordOutcome =
        crate::execution_submit_adapter::record_confirmation_tracker_outcome(
            &store,
            &request,
            outcome,
            confirmation_buy_fill(&order_id, now + chrono::Duration::seconds(6)),
        )?;
    let order = store
        .load_execution_canary_order(&order_id)?
        .expect("order should exist");
    let fill = record
        .fill_accounting
        .as_ref()
        .expect("confirmed buy should record fill accounting");

    assert_eq!(record.confirmed, 1);
    assert_eq!(record.pending, 0);
    assert_eq!(record.confirmation_status.as_deref(), Some("finalized"));
    assert_eq!(record.slot, Some(42));
    assert_eq!(fill.buy_opened, 1);
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_CONFIRMED
    );
    assert_eq!(order.confirm_ts, Some(now + chrono::Duration::seconds(5)));
    assert!(store
        .load_execution_canary_open_position("TokenMint")?
        .is_some());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[test]
fn confirmation_tracker_confirmed_sell_marks_order_and_closes_fill() -> Result<()> {
    let db_path = unique_confirmation_tracker_test_path("confirmed-sell");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let buy_order_id = submitted_confirmation_order(&store, "buy-owned", "buy", "tx-buy", now)?;
    let buy_request = crate::execution_submit_adapter::build_confirmation_request_from_order(
        &store,
        &buy_order_id,
        now + chrono::Duration::seconds(4),
    )?;
    let buy_tracker = crate::execution_submit_adapter::MockConfirmedExecutionConfirmationTracker {
        slot: Some(40),
        confirmation_status: "finalized".to_string(),
        confirmed_at: now + chrono::Duration::seconds(5),
    };
    let buy_outcome = buy_tracker.check_confirmation(&buy_request)?;
    crate::execution_submit_adapter::record_confirmation_tracker_outcome(
        &store,
        &buy_request,
        buy_outcome,
        confirmation_buy_fill(&buy_order_id, now + chrono::Duration::seconds(6)),
    )?;
    let sell_order_id = submitted_confirmation_order(
        &store,
        "sell-owned",
        "sell",
        "tx-sell",
        now + chrono::Duration::minutes(1),
    )?;
    let sell_request = crate::execution_submit_adapter::build_confirmation_request_from_order(
        &store,
        &sell_order_id,
        now + chrono::Duration::minutes(1) + chrono::Duration::seconds(4),
    )?;
    let sell_tracker = crate::execution_submit_adapter::MockConfirmedExecutionConfirmationTracker {
        slot: Some(41),
        confirmation_status: "finalized".to_string(),
        confirmed_at: now + chrono::Duration::minutes(1) + chrono::Duration::seconds(5),
    };
    let sell_outcome = sell_tracker.check_confirmation(&sell_request)?;
    let record = crate::execution_submit_adapter::record_confirmation_tracker_outcome(
        &store,
        &sell_request,
        sell_outcome,
        confirmation_sell_fill(
            &sell_order_id,
            now + chrono::Duration::minutes(1) + chrono::Duration::seconds(6),
        ),
    )?;
    let fill = record
        .fill_accounting
        .as_ref()
        .expect("confirmed sell should close fill accounting");

    assert_eq!(record.confirmed, 1);
    assert_eq!(record.slot, Some(41));
    assert_eq!(fill.sell_closed, 1);
    assert_eq!(fill.sell_no_position, 0);
    assert_eq!(fill.closed_qty, 10.0);
    assert!((fill.pnl_sol - 0.2).abs() < 1e-9);
    assert!(store
        .load_execution_canary_open_position("TokenMint")?
        .is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

#[test]
fn confirmation_tracker_rejects_signature_mismatch_without_fill() -> Result<()> {
    let db_path = unique_confirmation_tracker_test_path("mismatch");
    let mut store = SqliteStore::open(&db_path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let now = Utc::now();
    let order_id = submitted_confirmation_order(&store, "buy-mismatch", "buy", "tx-good", now)?;
    let request = crate::execution_submit_adapter::build_confirmation_request_from_order(
        &store,
        &order_id,
        now + chrono::Duration::seconds(4),
    )?;
    let outcome = crate::execution_submit_adapter::ExecutionConfirmationTrackerOutcome::Confirmed(
        crate::execution_submit_adapter::ExecutionConfirmationProof {
            tx_signature: "tx-wrong".to_string(),
            confirmation_status: "finalized".to_string(),
            slot: Some(99),
            confirmed_at: now + chrono::Duration::seconds(5),
        },
    );

    let error = crate::execution_submit_adapter::record_confirmation_tracker_outcome(
        &store,
        &request,
        outcome,
        confirmation_buy_fill(&order_id, now + chrono::Duration::seconds(6)),
    )
    .expect_err("mismatched confirmation signature must fail");
    let order = store
        .load_execution_canary_order(&order_id)?
        .expect("order should exist");

    assert!(format!("{error:#}").contains("confirmation tx_signature mismatch"));
    assert_eq!(
        order.status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMITTED
    );
    assert!(store
        .load_execution_canary_open_position("TokenMint")?
        .is_none());

    let _ = std::fs::remove_file(db_path);
    Ok(())
}

fn submitted_confirmation_order(
    store: &SqliteStore,
    name: &str,
    side: &str,
    tx_signature: &str,
    now: chrono::DateTime<Utc>,
) -> Result<String> {
    let signal = confirmation_signal(name, side, now);
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
    store.mark_execution_canary_submitted(
        &reserve.order.order_id,
        now + chrono::Duration::seconds(3),
        tx_signature,
    )?;
    Ok(reserve.order.order_id)
}

fn confirmation_buy_fill(
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

fn confirmation_sell_fill(
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

fn confirmation_signal(
    name: &str,
    side: &str,
    ts: chrono::DateTime<Utc>,
) -> copybot_core_types::CopySignalRow {
    copybot_core_types::CopySignalRow {
        signal_id: format!("shadow:sig-confirmation:leader-wallet:{name}:TokenMint"),
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

fn unique_confirmation_tracker_test_path(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "copybot-app-confirmation-tracker-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}
