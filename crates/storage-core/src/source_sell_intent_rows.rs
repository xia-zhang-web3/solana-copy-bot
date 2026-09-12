use crate::{
    observed_row::row_to_swap_event, source_sell_event as event, ExecutionSourceSellIntent,
    ProvenBuyContributor,
};
use anyhow::{ensure, Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection};

pub(crate) fn load(conn: &Connection, id: &str) -> Result<Option<ExecutionSourceSellIntent>> {
    let mut stmt = conn.prepare("SELECT event_signature,source_wallet,dex,token,token_out,amount_in,amount_out,slot,event_ts,
        amount_in_raw,amount_in_decimals,amount_out_raw,amount_out_decimals,
        intent_id,position_id,buy_fill_id,buy_order_id,buy_signal_id,buy_tx_signature,buy_execution_wallet,staged_at
        FROM execution_source_sell_intents WHERE intent_id=?1")?;
    let mut rows = stmt.query([id])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let event = row_to_swap_event(row)?;
    ensure!(
        event::valid(&event) && event::intent_id(&event.signature) == id,
        "invalid staged SELL event identity"
    );
    let value = ExecutionSourceSellIntent {
        intent_id: row.get(13)?,
        position_id: row.get(14)?,
        buy_witness: ProvenBuyContributor {
            fill_id: row.get(15)?,
            order_id: row.get(16)?,
            signal_id: row.get(17)?,
            source_wallet: event.wallet.clone(),
            tx_signature: row.get(18)?,
        },
        buy_execution_wallet: row.get(19)?,
        staged_at: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(20)?)?
            .with_timezone(&Utc),
        event,
    };
    ensure!(
        !value.position_id.trim().is_empty()
            && !value.buy_witness.order_id.trim().is_empty()
            && !value.buy_witness.signal_id.trim().is_empty()
            && !value.buy_witness.tx_signature.trim().is_empty()
            && !value.buy_execution_wallet.trim().is_empty(),
        "invalid staged SELL witness identity"
    );
    Ok(Some(value))
}

pub(crate) fn insert(conn: &Connection, value: &ExecutionSourceSellIntent) -> Result<()> {
    let s = &value.event;
    let exact = s.exact_amounts.as_ref();
    let w = &value.buy_witness;
    let changed = conn.execute("INSERT INTO execution_source_sell_intents
        (intent_id,event_signature,source_wallet,dex,token,token_out,amount_in,amount_out,slot,event_ts,
         amount_in_raw,amount_in_decimals,amount_out_raw,amount_out_decimals,
         position_id,buy_fill_id,buy_order_id,buy_signal_id,buy_tx_signature,buy_execution_wallet,staged_at)
        VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21)",
        params![value.intent_id,s.signature,s.wallet,s.dex,s.token_in,s.token_out,s.amount_in,s.amount_out,
            i64::try_from(s.slot)?,s.ts_utc.to_rfc3339(),exact.map(|a|a.amount_in_raw.as_str()),
            exact.map(|a|i64::from(a.amount_in_decimals)),exact.map(|a|a.amount_out_raw.as_str()),
            exact.map(|a|i64::from(a.amount_out_decimals)),value.position_id,w.fill_id,w.order_id,w.signal_id,
            w.tx_signature,value.buy_execution_wallet,value.staged_at.to_rfc3339()])
        .context("insert staged source SELL intent")?;
    ensure!(changed == 1, "staged SELL insertion refused");
    Ok(())
}
