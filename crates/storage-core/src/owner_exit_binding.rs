//! Durable BUY chain and SELL receipt ownership for one owner exit.
use crate::owner_exit::{owner_exit_order_id, OwnerExitIntent};
use anyhow::{ensure, Context, Result};
use copybot_core_types::TokenQuantity;
use rusqlite::{params, Connection, OptionalExtension};

pub(crate) fn token_side(conn: &Connection, order_id: &str) -> Result<(String, String)> {
    let row: Option<(String, String, String, String)> = conn
        .query_row(
            "SELECT i.mint,i.wallet,o.tx_signature,i.intent_id FROM orders o
         JOIN execution_order_sources s ON s.identity_id=o.signal_id
         JOIN owner_exit_intents i ON i.intent_id=s.owner_exit_intent_id
         WHERE o.order_id=?1 AND o.signal_id='owner-exit:'||i.intent_id",
            [order_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()?;
    let (mint, wallet, signature, id) = row.context("owner_exit_order_binding_missing")?;
    ensure!(
        !signature.is_empty() && !wallet.is_empty() && order_id == owner_exit_order_id(&id),
        "owner_exit_order_unsubmitted"
    );
    Ok((mint, "sell".to_owned()))
}

pub(crate) fn validate_sell_receipt(conn: &Connection, order_id: &str, token: &str) -> Result<()> {
    let bound: bool = conn.query_row(
        "SELECT EXISTS(
        SELECT 1 FROM orders o JOIN execution_order_sources s ON s.identity_id=o.signal_id
        JOIN owner_exit_intents i ON i.intent_id=s.owner_exit_intent_id
        JOIN execution_canary_dispatch d ON d.order_id=o.order_id
        JOIN execution_canary_receipt_proofs p ON p.order_id=o.order_id
        JOIN execution_canary_receipt_facts f ON f.order_id=o.order_id
        WHERE o.order_id=?1 AND o.signal_id='owner-exit:'||i.intent_id
          AND o.tx_signature=d.tx_signature AND d.tx_signature=p.tx_signature
          AND p.tx_signature=f.tx_signature AND d.wallet=i.wallet
          AND d.token=i.mint AND d.side='sell' AND d.attempt=1
          AND p.wallet_pubkey=i.wallet AND f.wallet_pubkey=i.wallet
          AND p.token=i.mint AND f.token=i.mint AND i.mint=?2
          AND p.side='sell' AND f.side='sell')",
        params![order_id, token],
        |r| r.get(0),
    )?;
    ensure!(bound, "owner_exit_sell_receipt_unproven");
    let (expected_token, side) = token_side(conn, order_id)?;
    ensure!(
        expected_token == token && side == "sell",
        "owner_exit_sell_order_unproven"
    );
    crate::sell_receipt_ownership::validate(conn, order_id)?;
    Ok(())
}

pub(crate) fn ensure_buy_binding(conn: &Connection, i: &OwnerExitIntent) -> Result<()> {
    ensure_buy_binding_parts(
        conn,
        &i.buy_order_id,
        &i.buy_receipt_signature,
        &i.wallet,
        &i.genesis_hash,
        &i.mint,
        &i.position_id,
    )
}

/// Called by both settlement implementations inside their inventory write tx.
/// The current same-mint row must still be the exact original BUY lot.
pub(crate) fn validate_position(
    conn: &Connection,
    order_id: &str,
    position_id: Option<&str>,
    token: &str,
    exact: Option<TokenQuantity>,
) -> Result<()> {
    let signal: Option<String> = conn
        .query_row(
            "SELECT signal_id FROM orders WHERE order_id=?1",
            [order_id],
            |r| r.get(0),
        )
        .optional()?;
    let Some(signal) = signal else {
        anyhow::bail!("owner_exit_order_missing");
    };
    if !signal.starts_with("owner-exit:") {
        return Ok(());
    }
    let bound: Option<(
        String,
        String,
        String,
        String,
        String,
        String,
        u64,
        u8,
        String,
    )> = conn
        .query_row(
            "SELECT i.intent_id,i.buy_order_id,i.buy_receipt_signature,i.wallet,
            i.genesis_hash,i.mint,i.amount_raw,i.decimals,i.position_id
         FROM execution_order_sources s JOIN owner_exit_intents i
            ON i.intent_id=s.owner_exit_intent_id
         WHERE s.identity_id=?1 AND s.identity_id='owner-exit:'||i.intent_id",
            [&signal],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                    r.get(8)?,
                ))
            },
        )
        .optional()?;
    let (id, buy, sig, wallet, genesis, mint, raw, decimals, position) =
        bound.context("owner_exit_position_origin_missing")?;
    ensure!(
        order_id == owner_exit_order_id(&id)
            && position_id == Some(position.as_str())
            && token == mint
            && exact.is_some_and(|q| q.raw() == raw && q.decimals() == decimals),
        "owner_exit_exact_position_changed"
    );
    ensure_buy_binding_parts(conn, &buy, &sig, &wallet, &genesis, &mint, &position)
}

fn ensure_buy_binding_parts(
    conn: &Connection,
    buy_order: &str,
    receipt_sig: &str,
    wallet: &str,
    genesis: &str,
    mint: &str,
    position: &str,
) -> Result<()> {
    let bound: bool = conn.query_row(
        "SELECT EXISTS(
        SELECT 1 FROM orders o
        JOIN execution_order_sources s ON s.identity_id=o.signal_id
        JOIN owner_technical_buy_intents b ON b.intent_id=s.owner_buy_intent_id
        JOIN execution_canary_dispatch d ON d.order_id=o.order_id
        JOIN execution_canary_receipt_proofs p ON p.order_id=o.order_id
        JOIN execution_canary_receipt_facts f ON f.order_id=o.order_id
        JOIN fills v ON v.order_id=o.order_id
        JOIN positions x ON x.position_id=v.position_id
        WHERE o.order_id=?1 AND o.signal_id='owner-buy:'||b.intent_id
          AND o.status='execution_canary_confirmed' AND o.attempt=1
          AND o.tx_signature=?2 AND d.tx_signature=?2 AND p.tx_signature=?2 AND f.tx_signature=?2
          AND p.confirmation_status='finalized' AND p.side='buy' AND f.side='buy'
          AND p.wallet_pubkey=?3 AND f.wallet_pubkey=?3 AND d.wallet=?3
          AND b.wallet=?3 AND b.signer=?3 AND b.genesis_hash=?4
          AND p.token=?5 AND f.token=?5 AND d.token=?5 AND b.mint=?5
          AND v.token=?5 AND x.token=?5 AND x.position_id=?6
          AND x.state='open' AND x.accounting_bucket='execution_canary'
          AND v.qty_raw='1167085' AND v.qty_decimals=6
          AND x.qty_raw='1167085' AND x.qty_decimals=6
          AND f.token_delta_raw='1167085' AND f.token_decimals=6
          AND f.token_coverage!='unresolved')",
        params![buy_order, receipt_sig, wallet, genesis, mint, position],
        |r| r.get(0),
    )?;
    ensure!(bound, "owner_exit_buy_position_binding_changed");
    Ok(())
}
