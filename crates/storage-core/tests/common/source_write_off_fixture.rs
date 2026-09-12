#![allow(dead_code)]
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::{CopySignalRow, Lamports, SignedLamports, TokenQuantity};
use copybot_storage_core::*;

pub fn proven_buy(
    store: &SqliteStore,
    id: &str,
    source: &str,
    now: DateTime<Utc>,
    qty: TokenQuantity,
) -> Result<String> {
    let wallet = source;
    let side = "buy";
    let token = "mint";
    let execution_wallet = "execution-wallet";
    let signature = format!("receipt:{id}");
    store.insert_copy_signal(&CopySignalRow {
        signal_id: id.into(),
        wallet_id: wallet.into(),
        token: token.into(),
        side: side.into(),
        notional_sol: 0.000001,
        notional_lamports: Some(Lamports::new(1000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
        ts: now,
        status: "shadow_recorded".into(),
    })?;
    let id = store
        .reserve_execution_canary_order(id, "tiny", now)?
        .order
        .order_id;
    store.mark_execution_canary_built(&id, now)?;
    store.mark_execution_canary_simulated(&id, now, EXECUTION_SIMULATION_STATUS_PASSED, None)?;
    store.mark_execution_canary_submitted(&id, now, &signature.clone())?;
    store.mark_execution_canary_confirmed_unreconciled(
        &id,
        &ExecutionCanaryReceiptProof {
            tx_signature: signature.clone(),
            wallet_pubkey: execution_wallet.into(),
            token: token.into(),
            side: side.into(),
            confirmation_status: "confirmed".into(),
            slot: Some(42),
            confirmed_at: now,
            reason: "receipt_not_fetched".into(),
        },
        now,
    )?;
    store.record_execution_canary_receipt_facts(
        &ExecutionCanaryReceiptFacts {
            order_id: id.clone(),
            tx_signature: signature.clone(),
            wallet_pubkey: execution_wallet.into(),
            token: token.into(),
            side: side.into(),
            slot: 42,
            wallet_native_pre: Lamports::new(2000),
            wallet_native_post: Lamports::new(if side == "buy" { 1000 } else { 2500 }),
            wallet_native_delta: SignedLamports::new(if side == "buy" { -1000 } else { 500 }),
            transaction_fee: Some(Lamports::new(50)),
            fee_coverage: ReceiptFeeCoverage::Known,
            fee_payer: Some(execution_wallet.into()),
            token_delta: Some(ReceiptTokenDelta {
                raw: i128::from(qty.raw()),
                decimals: qty.decimals(),
            }),
            token_coverage: ReceiptTokenCoverage::PairedBalances,
            token_coverage_reason: None,
            wsol_coverage: ReceiptWsolCoverage::Unresolved,
            block_time: Some(now.timestamp()),
            decomposition: ReceiptDecomposition::Unresolved,
        },
        now,
    )?;
    store.confirm_execution_canary_buy_fill(
        &id,
        token,
        qty.as_f64(),
        Some(qty),
        0.000001,
        now,
        now,
        Some(Lamports::new(1000)),
    )?;
    Ok(id)
}

pub fn staged(
    store: &SqliteStore,
    signature: &str,
    now: DateTime<Utc>,
) -> Result<ExecutionSourceSellIntent> {
    let event = copybot_core_types::SwapEvent {
        signature: signature.into(),
        wallet: "source-a".into(),
        dex: "pumpswap".into(),
        token_in: "mint".into(),
        token_out: "So11111111111111111111111111111111111111112".into(),
        amount_in: 4.0,
        amount_out: 0.1,
        slot: 42,
        ts_utc: now + chrono::Duration::seconds(1),
        exact_amounts: Some(copybot_core_types::ExactSwapAmounts {
            amount_in_raw: "4000".into(),
            amount_in_decimals: 3,
            amount_out_raw: "100000000".into(),
            amount_out_decimals: 9,
        }),
    };
    store.insert_observed_swap(&event)?;
    let position = store.load_execution_canary_open_position("mint")?.unwrap();
    let ExecutionSourceSellOutcome::Inserted(staged) =
        store.stage_execution_source_sell_intent(&event, &position.position_id)?
    else {
        anyhow::bail!("fixture staging refused")
    };
    Ok(staged)
}

pub fn fail_order(
    store: &SqliteStore,
    signal_id: &str,
    route: &str,
    now: DateTime<Utc>,
    simulation: bool,
    attempts: u32,
) -> Result<ExecutionCanaryOrder> {
    let id = store
        .reserve_execution_canary_order(signal_id, route, now)?
        .order
        .order_id;
    for n in 0..attempts {
        if simulation {
            store.mark_execution_canary_built(&id, now)?;
            store.mark_execution_canary_simulated(
                &id,
                now,
                EXECUTION_SIMULATION_STATUS_FAILED,
                Some("simulation failed"),
            )?;
        }
        store.mark_execution_canary_failed(
            &id,
            now,
            if simulation {
                EXECUTION_ERROR_SIMULATION_FAILED
            } else {
                EXECUTION_ERROR_BUILD_FAILED
            },
            if simulation {
                "simulation failed"
            } else {
                "owned_sell_quote_failed: NO_ROUTES_FOUND"
            },
        )?;
        if n + 1 < attempts {
            if simulation {
                store.mark_execution_canary_failed_simulation_retry_candidate(
                    &id,
                    now,
                    "retry_failed_sell_with_owned_position_amount",
                )?;
            } else {
                store.mark_execution_canary_failed_build_retry_candidate(
                    &id,
                    now,
                    "retry_failed_sell_with_owned_position_amount",
                )?;
            }
        }
    }
    Ok(store.load_execution_canary_order(&id)?.unwrap())
}

pub fn snapshot(
    conn: &rusqlite::Connection,
    exclude: &[&str],
) -> Result<std::collections::BTreeMap<String, Vec<String>>> {
    let tables = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")?
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut result = std::collections::BTreeMap::new();
    for table in tables {
        if exclude.contains(&table.as_str()) {
            continue;
        }
        let mut stmt =
            conn.prepare(&format!("SELECT * FROM \"{}\"", table.replace('"', "\"\"")))?;
        let n = stmt.column_count();
        let mut rows = stmt
            .query_map([], |r| {
                let data = (0..n)
                    .map(|i| r.get::<_, rusqlite::types::Value>(i))
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(format!("{data:?}"))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows.sort();
        result.insert(table, rows);
    }
    Ok(result)
}

pub fn snapshot_without_order(
    conn: &rusqlite::Connection,
    excluded_order_id: &str,
) -> Result<std::collections::BTreeMap<String, Vec<String>>> {
    let mut result = snapshot(conn, &["orders"])?;
    let mut stmt = conn.prepare("SELECT * FROM orders WHERE order_id <> ?1")?;
    let columns = stmt.column_count();
    let mut rows = stmt
        .query_map([excluded_order_id], |row| {
            let values = (0..columns)
                .map(|i| row.get::<_, rusqlite::types::Value>(i))
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(format!("{values:?}"))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    rows.sort();
    result.insert("orders".into(), rows);
    Ok(result)
}
