use crate::{source_sell_event, ExecutionSourceSellIntent, ExecutionSourceSellPromotion};
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::{
    CopySignalRow, Lamports, COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE,
    COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
};
use rusqlite::{params, Connection};

pub(crate) fn load(
    conn: &Connection,
    signal_id: Option<&str>,
    intent_id: Option<&str>,
) -> Result<Option<ExecutionSourceSellPromotion>> {
    let mut stmt = conn.prepare(
        "SELECT signal_id,intent_id,promoted_at
        FROM execution_source_sell_promotions WHERE signal_id=?1 OR intent_id=?2",
    )?;
    let mut rows = stmt.query(params![signal_id, intent_id])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let value = ExecutionSourceSellPromotion {
        signal_id: row.get(0)?,
        intent_id: row.get(1)?,
        promoted_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(2)?)?.with_timezone(&Utc),
    };
    ensure!(
        !value.signal_id.trim().is_empty() && !value.intent_id.trim().is_empty(),
        "malformed source SELL promotion identity"
    );
    ensure!(
        rows.next()?.is_none(),
        "conflicting source SELL promotion associations"
    );
    Ok(Some(value))
}

pub(crate) fn insert(conn: &Connection, value: &ExecutionSourceSellPromotion) -> Result<()> {
    let changed = conn.execute(
        "INSERT INTO execution_source_sell_promotions(signal_id,intent_id,promoted_at)
        VALUES (?1,?2,?3)",
        params![
            value.signal_id,
            value.intent_id,
            value.promoted_at.to_rfc3339()
        ],
    )?;
    ensure!(
        changed == 1,
        "source SELL promotion binding insertion refused"
    );
    Ok(())
}

pub(crate) fn signal(staged: &ExecutionSourceSellIntent) -> Result<CopySignalRow> {
    let event = &staged.event;
    let exact = event
        .exact_amounts
        .as_ref()
        .map(|a| a.amount_out_quantity().map(|q| Lamports::new(q.raw())))
        .transpose()?;
    Ok(CopySignalRow {
        signal_id: source_sell_event::signal_id(event),
        wallet_id: event.wallet.clone(),
        side: "sell".into(),
        token: event.token_in.clone(),
        notional_sol: event.amount_out,
        notional_lamports: exact,
        notional_origin: if exact.is_some() {
            COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS
        } else {
            COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE
        }
        .into(),
        ts: event.ts_utc,
        status: crate::EXECUTION_SELL_INTENT_STATUS.into(),
    })
}

// Status is mutable workflow state; every other canonical event field is immutable.
pub(crate) fn same_identity(a: &CopySignalRow, b: &CopySignalRow) -> bool {
    a.signal_id == b.signal_id
        && a.wallet_id == b.wallet_id
        && a.side == b.side
        && a.token == b.token
        && a.ts == b.ts
        && a.notional_sol == b.notional_sol
        && a.notional_lamports == b.notional_lamports
        && a.notional_origin == b.notional_origin
}
