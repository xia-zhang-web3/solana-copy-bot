//! LOCAL SOURCE witness for the genuine controlled native BUY capture guard.
use super::{SOURCE, SOURCE_SIG};
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_storage_core::*;
use rusqlite::{params, Connection};
use std::path::Path;

pub(super) fn seed(scope: &controlled_receipt_lot::Scope, now: DateTime<Utc>) -> Result<()> {
    let path = Path::new(&scope.source_state_db_path);
    let mut store = SqliteStore::open(path)?;
    store.run_migrations(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations")))?;
    store.register_controlled_receipt_lot_scope(scope, SOURCE)?;
    let parent = OwnerTechnicalBuyIntent {
        intent_id: scope.source_buy_intent_id.clone(), run_id: scope.source_run_id.clone(),
        wallet: SOURCE.into(), signer: SOURCE.into(), genesis_hash: "saved-genesis".into(),
        mint: OWNER_EXIT_MINT.into(), amount_lamports: 10000000,
        route: "jupiter_swap_instructions".into(), activated_at: now,
        expires_at: now + Duration::seconds(280), authority_sha256: "f".repeat(64),
        max_priority_fee_lamports: 50000, min_reserve_lamports: 0, max_slippage_bps: 50,
        max_daily_loss_lamports: 20000000, max_open_positions: 1, max_buy_count: 1,
    };
    store.register_owner_technical_buy_intent(&parent)?;
    let order = store.reserve_owner_technical_buy_order(&parent.intent_id, || Ok(now))?.order;
    let c = Connection::open(path)?;
    c.execute("UPDATE orders SET status='execution_canary_confirmed',tx_signature=?2,
        simulation_status='passed' WHERE order_id=?1", params![order.order_id, SOURCE_SIG])?;
    c.execute("INSERT INTO execution_canary_dispatch(order_id,signal_id,client_order_id,route,attempt,
        wallet,token,side,tx_signature,transaction_sha256,message_sha256,claimed_at)
        VALUES(?1,?2,?3,?4,1,?5,?6,'buy',?7,?8,?9,?10)",params![order.order_id,order.signal_id,
        order.client_order_id,order.route,SOURCE,OWNER_EXIT_MINT,SOURCE_SIG,"a".repeat(64),"b".repeat(64),now.to_rfc3339()])?;
    c.execute("INSERT INTO execution_canary_receipt_proofs(order_id,tx_signature,wallet_pubkey,token,
        side,confirmation_status,slot,confirmed_at,last_attempt_at,reason)
        VALUES(?1,?2,?3,?4,'buy','finalized','42',?5,?5,'accounting_complete')",
        params![order.order_id,SOURCE_SIG,SOURCE,OWNER_EXIT_MINT,now.to_rfc3339()])?;
    c.execute("INSERT INTO execution_canary_receipt_facts(order_id,tx_signature,wallet_pubkey,token,
        side,slot,wallet_native_pre,wallet_native_post,wallet_native_delta,transaction_fee,fee_coverage,
        fee_payer,token_delta_raw,token_decimals,token_coverage,wsol_coverage,decomposition,recorded_at)
        VALUES(?1,?2,?3,?4,'buy','42','265762817','255707817','-10055000','55000','known',?3,
        '988701',6,'paired_balances','observed','unresolved',?5)",
        params![order.order_id,SOURCE_SIG,SOURCE,OWNER_EXIT_MINT,now.to_rfc3339()])?;
    let position = format!("exec-canary-pos:{}",order.order_id);
    c.execute("INSERT INTO positions(position_id,token,qty,cost_sol,opened_ts,state,cost_lamports,
        qty_raw,qty_decimals,accounting_bucket) VALUES(?1,?2,0.988701,0.010055,?3,'open',10055000,
        '988701',6,'execution_canary')",params![position,OWNER_EXIT_MINT,now.to_rfc3339()])?;
    c.execute("INSERT INTO fills(order_id,token,qty,avg_price,notional_lamports,qty_raw,qty_decimals,
        position_id) VALUES(?1,?2,0.988701,0.01,10055000,'988701',6,?3)",params![order.order_id,OWNER_EXIT_MINT,position])?;
    Ok(())
}
