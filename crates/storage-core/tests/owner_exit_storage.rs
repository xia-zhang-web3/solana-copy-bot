use anyhow::Result;
use chrono::{Duration, TimeZone, Utc};
use copybot_storage_core::{
    owner_exit_order_id, ExecutionCanaryDispatch, ExecutionDispatchClaim, ExecutionOrderOrigin,
    OwnerExitIntent, OwnerExitIntentRecordOutcome, OwnerTechnicalBuyIntent, SqliteStore,
    TinyBudgetClaim, EXECUTION_SIMULATION_STATUS_PASSED, OWNER_EXIT_BUY_ORDER, OWNER_EXIT_DECIMALS,
    OWNER_EXIT_MINT, OWNER_EXIT_RAW,
};
use rusqlite::{params, Connection};
use std::path::Path;

const WALLET: &str = "synthetic-owner-wallet";
const SIG: &str = "synthetic-confirmed-buy-signature";

fn open(path: &Path) -> Result<SqliteStore> {
    let mut s = SqliteStore::open(path)?;
    s.run_migrations(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations"))?;
    Ok(s)
}

fn seed_buy(path: &Path, s: &SqliteStore) -> Result<()> {
    let start = Utc.with_ymd_and_hms(2026, 9, 24, 12, 0, 0).unwrap();
    let buy = OwnerTechnicalBuyIntent {
        intent_id: "copybot-owner-buy-20260924-04-usdc-01".into(),
        run_id: "old-buy-run".into(),
        wallet: WALLET.into(),
        signer: WALLET.into(),
        genesis_hash: "synthetic-genesis".into(),
        mint: OWNER_EXIT_MINT.into(),
        amount_lamports: 10_000_000,
        route: "jupiter_swap_instructions".into(),
        activated_at: start,
        expires_at: start + Duration::minutes(10),
        authority_sha256: "a".repeat(64),
        max_priority_fee_lamports: 50_000,
        min_reserve_lamports: 50_000_001,
        max_slippage_bps: 100,
        max_daily_loss_lamports: 20_000_000,
        max_open_positions: 1,
        max_buy_count: 1,
    };
    s.register_owner_technical_buy_intent(&buy)?;
    let order = s
        .reserve_owner_technical_buy_order(&buy.intent_id, || Ok(start))?
        .order;
    assert_eq!(order.order_id, OWNER_EXIT_BUY_ORDER);
    let c = Connection::open(path)?;
    c.execute(
        "UPDATE orders SET status='execution_canary_confirmed',tx_signature=?1,
        simulation_status='passed' WHERE order_id=?2",
        params![SIG, OWNER_EXIT_BUY_ORDER],
    )?;
    c.execute(
        "INSERT INTO execution_canary_dispatch(order_id,signal_id,client_order_id,route,
        attempt,wallet,token,side,tx_signature,transaction_sha256,message_sha256,claimed_at)
        VALUES(?1,?2,?3,?4,1,?5,?6,'buy',?7,?8,?9,?10)",
        params![
            order.order_id,
            order.signal_id,
            order.client_order_id,
            order.route,
            WALLET,
            OWNER_EXIT_MINT,
            SIG,
            "b".repeat(64),
            "c".repeat(64),
            start.to_rfc3339()
        ],
    )?;
    c.execute(
        "INSERT INTO execution_canary_receipt_proofs(order_id,tx_signature,wallet_pubkey,
        token,side,confirmation_status,confirmed_at,last_attempt_at,reason)
        VALUES(?1,?2,?3,?4,'buy','finalized',?5,?5,'accounting_complete')",
        params![
            OWNER_EXIT_BUY_ORDER,
            SIG,
            WALLET,
            OWNER_EXIT_MINT,
            start.to_rfc3339()
        ],
    )?;
    c.execute(
        "INSERT INTO execution_canary_receipt_facts(order_id,tx_signature,wallet_pubkey,
        token,side,slot,wallet_native_pre,wallet_native_post,wallet_native_delta,
        transaction_fee,fee_coverage,fee_payer,token_delta_raw,token_decimals,
        token_coverage,wsol_coverage,decomposition,recorded_at)
        VALUES(?1,?2,?3,?4,'buy','1','175200031','163706591','-11493440',
        '5000','known',?3,'1167085',6,'proven_lifecycle','observed','unresolved',?5)",
        params![
            OWNER_EXIT_BUY_ORDER,
            SIG,
            WALLET,
            OWNER_EXIT_MINT,
            start.to_rfc3339()
        ],
    )?;
    let position = format!("exec-canary-pos:{OWNER_EXIT_BUY_ORDER}");
    c.execute(
        "INSERT INTO positions(position_id,token,qty,cost_sol,opened_ts,state,
        cost_lamports,qty_raw,qty_decimals,accounting_bucket)
        VALUES(?1,?2,1.167085,0.01149344,?3,'open',11493440,'1167085',6,'execution_canary')",
        params![position, OWNER_EXIT_MINT, start.to_rfc3339()],
    )?;
    c.execute(
        "INSERT INTO fills(order_id,token,qty,avg_price,notional_lamports,
        qty_raw,qty_decimals,position_id)
        VALUES(?1,?2,1.167085,0.009847988792590088,11493440,'1167085',6,?3)",
        params![OWNER_EXIT_BUY_ORDER, OWNER_EXIT_MINT, position],
    )?;
    Ok(())
}

fn exit() -> OwnerExitIntent {
    let start = Utc.with_ymd_and_hms(2026, 9, 24, 13, 0, 0).unwrap();
    OwnerExitIntent {
        intent_id: "one-usdc-exit".into(),
        run_id: "new-exit-run".into(),
        buy_order_id: OWNER_EXIT_BUY_ORDER.into(),
        buy_receipt_signature: SIG.into(),
        position_id: format!("exec-canary-pos:{OWNER_EXIT_BUY_ORDER}"),
        wallet: WALLET.into(),
        signer: WALLET.into(),
        genesis_hash: "synthetic-genesis".into(),
        mint: OWNER_EXIT_MINT.into(),
        amount_raw: OWNER_EXIT_RAW,
        decimals: OWNER_EXIT_DECIMALS,
        route: "jupiter_swap_instructions".into(),
        activated_at: start,
        expires_at: start + Duration::minutes(10),
        authority_sha256: "d".repeat(64),
        max_priority_fee_lamports: 50_000,
        min_reserve_lamports: 50_000_001,
        max_slippage_bps: 100,
        max_daily_loss_lamports: 20_000_000,
    }
}

#[test]
fn one_position_binding_and_restart_unknown_are_durable() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let s = open(&path)?;
    seed_buy(&path, &s)?;
    let i = exit();
    let mut changed = i.clone();
    changed.amount_raw -= 1;
    assert!(s.register_owner_exit_intent(&changed).is_err());
    let mut changed = i.clone();
    changed.wallet = "other-wallet".into();
    changed.signer = changed.wallet.clone();
    assert!(s.register_owner_exit_intent(&changed).is_err());
    assert_eq!(
        s.register_owner_exit_intent(&i)?,
        OwnerExitIntentRecordOutcome::Inserted
    );
    assert_eq!(
        s.register_owner_exit_intent(&i)?,
        OwnerExitIntentRecordOutcome::Existing
    );
    let now = i.activated_at + Duration::seconds(1);
    let order = s.reserve_owner_exit_order(&i.intent_id, || Ok(now))?.order;
    assert_eq!(order.order_id, owner_exit_order_id(&i.intent_id));
    assert_eq!(
        s.execution_order_origin(&order.order_id)?,
        Some(ExecutionOrderOrigin::OwnerExit {
            intent_id: i.intent_id.clone()
        })
    );
    s.mark_execution_canary_built(&order.order_id, now)?;
    let order = s.mark_execution_canary_simulated(
        &order.order_id,
        now,
        EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    let d = ExecutionCanaryDispatch {
        order_id: order.order_id.clone(),
        signal_id: order.signal_id.clone(),
        client_order_id: order.client_order_id.clone(),
        route: order.route.clone(),
        attempt: 1,
        wallet: WALLET.into(),
        token: OWNER_EXIT_MINT.into(),
        side: "sell".into(),
        tx_signature: "synthetic-sell-signature".into(),
        transaction_sha256: "e".repeat(64),
        message_sha256: "f".repeat(64),
    };
    let budget = TinyBudgetClaim {
        experiment_id: i.run_id.clone(),
        wallet: WALLET.into(),
        tx_signature: d.tx_signature.clone(),
        message_sha256: d.message_sha256.clone(),
        transaction_sha256: d.transaction_sha256.clone(),
        buy_lamports: Some(0),
        protected_capital: None,
        total_fee: 5000,
        priority_fee: 0,
        fee_slot: 1,
    };
    assert_eq!(
        s.claim_owner_exit_dispatch(&order, &d, &budget, || Ok(now))?,
        ExecutionDispatchClaim::New
    );
    drop(s);
    let s = open(&path)?;
    assert_eq!(s.list_owner_exit_recovery_intents()?, vec![i.clone()]);
    let c = Connection::open(&path)?;
    let fee: (String, u64, Option<u64>) = c.query_row(
        "SELECT state,fee_bound,actual_fee FROM owner_exit_fee_reservations WHERE order_id=?1",
        [&d.order_id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    assert_eq!(fee, ("pending".into(), 5000, None));
    assert_eq!(
        s.claim_owner_exit_dispatch(&order, &d, &budget, || Ok(now))?,
        ExecutionDispatchClaim::Existing
    );
    let mut altered = d.clone();
    altered.tx_signature = "different-sell".into();
    assert!(s
        .claim_owner_exit_dispatch(&order, &altered, &budget, || Ok(now))
        .is_err());
    assert!(s.reserve_owner_exit_order(&i.intent_id, || Ok(now)).is_ok());
    assert!(c.execute("INSERT INTO orders(order_id,signal_id,route,submit_ts,status,client_order_id)
        VALUES('second','owner-exit:one-usdc-exit','jupiter_swap_instructions','2026-09-24T13:00:00Z',
        'execution_canary_candidate','second-client')",[]).is_err());
    // The original lot can be closed by another path while a signed SELL waits
    // for a receipt. A new open USDC lot must never inherit this exit.
    c.execute(
        "UPDATE positions SET state='closed',qty=0,qty_raw='0'
        WHERE position_id=?1",
        [&i.position_id],
    )?;
    c.execute(
        "INSERT INTO positions(position_id,token,qty,cost_sol,opened_ts,state,
        cost_lamports,qty_raw,qty_decimals,pnl_lamports,accounting_bucket)
        VALUES('replacement-usdc',?1,1.167085,0.01149344,?2,'open',
        11493440,'1167085',6,0,'execution_canary')",
        params![OWNER_EXIT_MINT, now.to_rfc3339()],
    )?;
    c.execute(
        "UPDATE orders SET status='execution_canary_confirmed_unreconciled'
        WHERE order_id=?1",
        [&d.order_id],
    )?;
    c.execute(
        "INSERT INTO execution_canary_receipt_proofs(order_id,tx_signature,wallet_pubkey,
        token,side,confirmation_status,slot,confirmed_at,last_attempt_at,reason)
        VALUES(?1,?2,?3,?4,'sell','finalized','2',?5,?5,'receipt_pending_accounting')",
        params![
            d.order_id,
            d.tx_signature,
            WALLET,
            OWNER_EXIT_MINT,
            now.to_rfc3339()
        ],
    )?;
    c.execute(
        "INSERT INTO execution_canary_receipt_facts(order_id,tx_signature,wallet_pubkey,
        token,side,slot,wallet_native_pre,wallet_native_post,wallet_native_delta,
        transaction_fee,fee_coverage,fee_payer,token_delta_raw,token_decimals,
        token_coverage,wsol_coverage,decomposition,recorded_at)
        VALUES(?1,?2,?3,?4,'sell','2','100000000','110000000','10000000',
        '5000','known',?3,'-1167085',6,'paired_balances','observed','unresolved',?5)",
        params![
            d.order_id,
            d.tx_signature,
            WALLET,
            OWNER_EXIT_MINT,
            now.to_rfc3339()
        ],
    )?;
    let refusal = s
        .plan_execution_canary_sell_settlement(&d.order_id)
        .unwrap_err();
    assert!(
        refusal
            .to_string()
            .contains("owner_exit_exact_position_changed"),
        "unexpected settlement refusal: {refusal}"
    );
    let legacy_refusal = s
        .confirm_execution_canary_sell_fill(
            &d.order_id,
            OWNER_EXIT_MINT,
            1.167085,
            Some(copybot_core_types::TokenQuantity::new(
                OWNER_EXIT_RAW,
                OWNER_EXIT_DECIMALS,
            )),
            0.01,
            0.0,
            now,
            now,
            Some(copybot_core_types::Lamports::new(10_000_000)),
        )
        .unwrap_err();
    assert!(
        format!("{legacy_refusal:#}").contains("owner_exit_exact_position_changed"),
        "unexpected legacy fill refusal: {legacy_refusal:#}"
    );
    assert!(!s.execution_canary_fill_exists(&d.order_id)?);
    assert!(!c.prepare("PRAGMA foreign_key_check")?.exists([])?);
    Ok(())
}

#[test]
fn predispatch_rearm_is_bounded_and_never_rearms_a_claim() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("db");
    let s = open(&path)?;
    seed_buy(&path, &s)?;
    let i = exit();
    s.register_owner_exit_intent(&i)?;
    let now = i.activated_at + Duration::seconds(1);
    let order = s.reserve_owner_exit_order(&i.intent_id, || Ok(now))?.order;
    s.mark_execution_canary_built(&order.order_id, now)?;
    drop(s);
    let s = open(&path)?;
    assert!(s.rearm_owner_exit_undispatched(&i.intent_id)?);
    assert_eq!(s.load_execution_canary_order(&order.order_id)?.unwrap().status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_CANDIDATE);
    s.mark_execution_canary_failed(&order.order_id, now,
        copybot_storage_core::EXECUTION_ERROR_BUILD_FAILED, "temporary")?;
    assert!(s.rearm_owner_exit_undispatched(&i.intent_id)?);
    assert!(!s.owner_exit_failed_retry_available(&i.intent_id)?);
    s.mark_execution_canary_failed(&order.order_id, now,
        copybot_storage_core::EXECUTION_ERROR_BUILD_FAILED, "temporary-again")?;
    assert!(!s.rearm_owner_exit_undispatched(&i.intent_id)?);
    assert_eq!(s.load_execution_canary_order(&order.order_id)?.unwrap().status,
        copybot_storage_core::EXECUTION_STATUS_CANARY_FAILED);
    Ok(())
}
