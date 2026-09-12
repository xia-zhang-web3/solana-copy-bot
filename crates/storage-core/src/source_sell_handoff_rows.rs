use crate::{observed_row::row_to_swap_event, source_sell_event, SourceSellHandoff};
use anyhow::{ensure, Result};
use copybot_core_types::SwapEvent;
use rusqlite::{params, Connection};

pub(crate) fn load(conn: &Connection, signature: &str) -> Result<Option<SourceSellHandoff>> {
    let mut stmt = conn.prepare("SELECT signature,wallet_id,dex,token_in,token_out,qty_in,qty_out,slot,ts,
        qty_in_raw,qty_in_decimals,qty_out_raw,qty_out_decimals,sequence,original_position_id,disposition,reason
        FROM source_sell_handoffs WHERE signature=?1")?;
    let mut rows = stmt.query([signature])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let out = SourceSellHandoff {
        event: row_to_swap_event(row)?,
        sequence: row.get(13)?,
        original_position_id: row.get(14)?,
        disposition: row.get(15)?,
        reason: row.get(16)?,
    };
    ensure!(
        out.sequence > 0 && out.reason.len() <= 64,
        "invalid source SELL handoff row"
    );
    ensure!(
        matches!(
            out.disposition.as_str(),
            "pending" | "staged" | "refused" | "unknown"
        ),
        "invalid handoff disposition"
    );
    ensure!(
        (out.disposition == "unknown" && out.original_position_id.is_none())
            || (out.disposition != "unknown"
                && out
                    .original_position_id
                    .as_deref()
                    .is_some_and(|p| !p.trim().is_empty())),
        "invalid original handoff generation"
    );
    Ok(Some(out))
}

pub(crate) fn insert_known(conn: &Connection, e: &SwapEvent, position: &str) -> Result<()> {
    let exact = e.exact_amounts.as_ref();
    let n = conn.execute("INSERT INTO source_sell_handoffs(signature,wallet_id,dex,token_in,token_out,
        qty_in,qty_out,slot,ts,qty_in_raw,qty_in_decimals,qty_out_raw,qty_out_decimals,original_position_id,disposition,reason)
        VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,'pending','pending')",
        params![e.signature,e.wallet,e.dex,e.token_in,e.token_out,e.amount_in,e.amount_out,e.slot as i64,e.ts_utc.to_rfc3339(),
            exact.map(|v| v.amount_in_raw.as_str()),exact.map(|v| i64::from(v.amount_in_decimals)),
            exact.map(|v| v.amount_out_raw.as_str()),exact.map(|v| i64::from(v.amount_out_decimals)),position])?;
    ensure!(n == 1, "handoff INSERT did not persist");
    let row = load(conn, &e.signature)?.ok_or_else(|| anyhow::anyhow!("handoff INSERT missing"))?;
    ensure!(
        source_sell_event::same(&row.event, e)
            && row.original_position_id.as_deref() == Some(position)
            && row.disposition == "pending",
        "handoff INSERT changed original identity"
    );
    Ok(())
}

pub(crate) fn finish(
    conn: &Connection,
    signature: &str,
    disposition: &str,
    reason: &str,
) -> Result<()> {
    ensure!(reason.len() <= 64, "handoff reason unbounded");
    let n=conn.execute("UPDATE source_sell_handoffs SET disposition=?2,reason=?3 WHERE signature=?1 AND disposition='pending'",
        params![signature,disposition,reason])?;
    ensure!(n == 1, "handoff disposition updated {n} rows");
    let row =
        load(conn, signature)?.ok_or_else(|| anyhow::anyhow!("handoff disposition missing"))?;
    ensure!(
        row.disposition == disposition && row.reason == reason,
        "handoff disposition changed after write"
    );
    Ok(())
}
