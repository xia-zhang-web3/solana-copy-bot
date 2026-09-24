//! Durable BUY→SELL result written with the confirmed SELL fill. The historical
//! position P&L column is wallet cash basis; economic result is a distinct field.
use crate::{ExecutionCanarySellSettlementPlan, ReceiptCashComponents, SqliteDiscoveryStore};
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptTradeCycle {
    pub sell_order_id: String,
    pub buy_order_id: Option<String>,
    pub position_id: String,
    pub sold_raw: String,
    pub remaining_raw: String,
    pub state: String,
    pub wallet_cash_result_lamports: Option<String>,
    pub swap_input_lamports: Option<String>,
    pub swap_output_lamports: Option<String>,
    pub buy_transaction_fee_lamports: Option<String>,
    pub sell_transaction_fee_lamports: Option<String>,
    pub buy_priority_fee_lamports: Option<String>,
    pub sell_priority_fee_lamports: Option<String>,
    pub target_ata_rent_locked_lamports: Option<String>,
    pub economic_result_lamports: Option<String>,
}

impl SqliteDiscoveryStore {
    pub fn load_receipt_trade_cycle(
        &self,
        sell_order_id: &str,
    ) -> Result<Option<ReceiptTradeCycle>> {
        let row: Option<(String,String)> = self.conn.query_row(
            "SELECT position_id,accounting_json FROM execution_receipt_trade_cycles WHERE sell_order_id=?1",
            [sell_order_id], |r| Ok((r.get(0)?,r.get(1)?)),
        ).optional()?;
        row.map(|(position, json)| {
            let value: ReceiptTradeCycle = serde_json::from_str(&json)?;
            let fill: (String, String, String, String) = self.conn.query_row(
                "SELECT f.position_id,f.qty_raw,f.remaining_qty_raw,o.status FROM fills f
                 JOIN orders o ON o.order_id=f.order_id
                 WHERE f.order_id=?1 AND f.accounting_basis='receipt_native_cash'",
                [sell_order_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )?;
            ensure!(
                value.sell_order_id == sell_order_id
                    && value.position_id == position
                    && value.position_id == fill.0
                    && value.sold_raw == fill.1
                    && value.remaining_raw == fill.2
                    && fill.3 == crate::EXECUTION_STATUS_CANARY_CONFIRMED,
                "trade cycle binding conflict"
            );
            Ok(value)
        })
        .transpose()
    }
}

pub(crate) fn record_on_conn(
    conn: &Connection,
    p: &ExecutionCanarySellSettlementPlan,
    at: DateTime<Utc>,
) -> Result<()> {
    let mut buys = conn.prepare(
        "SELECT f.order_id FROM fills f JOIN orders o ON o.order_id=f.order_id
         JOIN execution_canary_receipt_facts r ON r.order_id=f.order_id
         WHERE f.position_id=?1 AND o.signal_id LIKE 'owner-buy:%'
           AND r.side='buy' ORDER BY f.order_id LIMIT 2",
    )?;
    let rows = buys.query_map([&p.expected_position.position_id], |r| {
        r.get::<_, String>(0)
    })?;
    let ids = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    let buy_id = if ids.len() == 1 {
        Some(ids[0].clone())
    } else {
        None
    };
    let buy = buy_id
        .as_deref()
        .map(|id| load_components(conn, id))
        .transpose()?
        .flatten();
    let sell = load_components(conn, &p.receipt.order_id)?;
    let sell_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM fills f JOIN execution_canary_receipt_facts r ON r.order_id=f.order_id
         WHERE f.position_id=?1 AND r.side='sell'",
        [&p.expected_position.position_id], |r| r.get(0),
    )?;
    let cycle = calculate(
        &p.receipt.order_id,
        buy_id.as_deref(),
        &p.expected_position.position_id,
        p.sold_quantity.raw(),
        p.remaining_quantity.raw(),
        Some(p.accumulated_cash_result.as_i128()),
        sell_count == 1,
        buy.as_ref(),
        sell.as_ref(),
    )?;
    conn.execute("INSERT INTO execution_receipt_trade_cycles(sell_order_id,position_id,accounting_json,recorded_at)
        VALUES(?1,?2,?3,?4)", params![cycle.sell_order_id,cycle.position_id,
            serde_json::to_string(&cycle)?,at.to_rfc3339()])?;
    Ok(())
}

fn load_components(conn: &Connection, order_id: &str) -> Result<Option<ReceiptCashComponents>> {
    let json: Option<String> = conn
        .query_row(
            "SELECT components_json FROM execution_receipt_cash_components WHERE order_id=?1",
            [order_id],
            |r| r.get(0),
        )
        .optional()?;
    json.map(|json| {
        let c: ReceiptCashComponents = serde_json::from_str(&json)?;
        let facts = crate::receipt_facts_rows::load(conn, order_id)?
            .ok_or_else(|| anyhow::anyhow!("trade cycle component receipt missing"))?;
        ensure!(
            c.order_id == order_id
                && c.tx_signature == facts.tx_signature
                && c.side == facts.side
                && c.wallet_native_delta_lamports
                    == facts.wallet_native_delta.as_i128().to_string(),
            "trade cycle component identity conflict"
        );
        Ok(c)
    })
    .transpose()
}

fn number(value: Option<&str>) -> Option<i128> {
    let text = value?;
    let n = text.parse::<i128>().ok()?;
    (n.to_string() == text).then_some(n)
}

/// No partial fill is called a completed cycle. A complete cycle receives an
/// economic result only when both receipt classifications have zero residual.
pub fn calculate(
    sell_id: &str,
    buy_id: Option<&str>,
    position_id: &str,
    sold_raw: u64,
    remaining_raw: u64,
    wallet_cash_result: Option<i128>,
    single_sell: bool,
    buy: Option<&ReceiptCashComponents>,
    sell: Option<&ReceiptCashComponents>,
) -> Result<ReceiptTradeCycle> {
    ensure!(
        sold_raw > 0 && !sell_id.is_empty() && !position_id.is_empty(),
        "trade cycle identity or quantity invalid"
    );
    if let Some(buy) = buy {
        ensure!(
            Some(buy.order_id.as_str()) == buy_id && buy.side == "buy",
            "trade cycle BUY component mismatch"
        );
    }
    if let Some(sell) = sell {
        ensure!(
            sell.order_id == sell_id && sell.side == "sell",
            "trade cycle SELL component mismatch"
        );
    }
    let buy_swap = buy.and_then(|c| number(c.swap_native_delta_lamports.as_deref()));
    let sell_swap = sell.and_then(|c| number(c.swap_native_delta_lamports.as_deref()));
    let buy_fee = buy.and_then(|c| number(c.transaction_fee_lamports.as_deref()));
    let sell_fee = sell.and_then(|c| number(c.transaction_fee_lamports.as_deref()));
    let buy_rent = buy.and_then(|c| number(c.target_ata_rent_delta_lamports.as_deref()));
    let sell_rent = sell.and_then(|c| number(c.target_ata_rent_delta_lamports.as_deref()));
    let classified = buy
        .is_some_and(|c| c.unclassified_native_delta_lamports.as_deref() == Some("0"))
        && sell.is_some_and(|c| c.unclassified_native_delta_lamports.as_deref() == Some("0"));
    let cash_matches = buy
        .and_then(|c| number(Some(&c.wallet_native_delta_lamports)))
        .zip(sell.and_then(|c| number(Some(&c.wallet_native_delta_lamports))))
        .and_then(|(a, b)| a.checked_add(b))
        == wallet_cash_result;
    let valid_signs = buy_swap.is_some_and(|n| n < 0)
        && sell_swap.is_some_and(|n| n > 0)
        && buy_fee.is_some_and(|n| n >= 0)
        && sell_fee.is_some_and(|n| n >= 0);
    let economic = if remaining_raw == 0 && single_sell && classified && cash_matches && valid_signs
    {
        buy_swap.zip(sell_swap).zip(buy_fee.zip(sell_fee)).and_then(
            |((input, output), (buy_fee, sell_fee))| {
                input
                    .checked_add(output)?
                    .checked_sub(buy_fee)?
                    .checked_sub(sell_fee)
            },
        )
    } else {
        None
    };
    let rent_locked = buy_rent.zip(sell_rent).and_then(|(a, b)| a.checked_add(b));
    Ok(ReceiptTradeCycle {
        sell_order_id: sell_id.into(),
        buy_order_id: buy_id.map(str::to_owned),
        position_id: position_id.into(),
        sold_raw: sold_raw.to_string(),
        remaining_raw: remaining_raw.to_string(),
        state: if remaining_raw == 0 {
            "closed"
        } else {
            "partial"
        }
        .into(),
        wallet_cash_result_lamports: wallet_cash_result.map(|n| n.to_string()),
        swap_input_lamports: buy_swap.filter(|n| *n < 0).map(|n| (-n).to_string()),
        swap_output_lamports: sell_swap.filter(|n| *n > 0).map(|n| n.to_string()),
        buy_transaction_fee_lamports: buy_fee.map(|n| n.to_string()),
        sell_transaction_fee_lamports: sell_fee.map(|n| n.to_string()),
        buy_priority_fee_lamports: buy.and_then(|c| c.priority_fee_lamports.clone()),
        sell_priority_fee_lamports: sell.and_then(|c| c.priority_fee_lamports.clone()),
        target_ata_rent_locked_lamports: rent_locked.map(|n| n.to_string()),
        economic_result_lamports: economic.map(|n| n.to_string()),
    })
}
