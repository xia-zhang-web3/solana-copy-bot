use crate::{
    money::{sol_to_lamports_ceil, token_quantity_from_sql, u64_to_sql_i64},
    ShadowLotRow, SqliteDiscoveryStore, POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER,
    POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER, SHADOW_LOT_OPEN_EPS, SHADOW_RISK_CONTEXT_MARKET,
    SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::TokenQuantity;
use rusqlite::params;
use std::collections::HashSet;
use std::io;

impl SqliteDiscoveryStore {
    pub fn insert_shadow_lot(
        &self,
        wallet_id: &str,
        token: &str,
        qty: f64,
        cost_sol: f64,
        opened_ts: DateTime<Utc>,
    ) -> Result<i64> {
        self.insert_shadow_lot_exact(wallet_id, token, qty, None, cost_sol, opened_ts)
    }

    pub fn insert_shadow_lot_exact(
        &self,
        wallet_id: &str,
        token: &str,
        qty: f64,
        qty_exact: Option<TokenQuantity>,
        cost_sol: f64,
        opened_ts: DateTime<Utc>,
    ) -> Result<i64> {
        self.insert_shadow_lot_exact_with_risk_context(
            wallet_id,
            token,
            qty,
            qty_exact,
            cost_sol,
            SHADOW_RISK_CONTEXT_MARKET,
            opened_ts,
        )
    }

    pub fn insert_shadow_lot_exact_with_risk_context(
        &self,
        wallet_id: &str,
        token: &str,
        qty: f64,
        qty_exact: Option<TokenQuantity>,
        cost_sol: f64,
        risk_context: &str,
        opened_ts: DateTime<Utc>,
    ) -> Result<i64> {
        let qty_exact = reject_zero_raw_exact_qty(qty_exact, "insert shadow lot")?;
        validate_shadow_risk_context(risk_context)?;
        self.execute_with_retry_result(|conn| {
            let cost_lamports = sol_to_lamports_ceil(cost_sol, "shadow lot cost_sol")
                .map_err(to_sql_conversion_error)?;
            let accounting_bucket = shadow_accounting_bucket_for_qty_exact(qty_exact);
            conn.execute(
                "INSERT INTO shadow_lots(
                    wallet_id,
                    token,
                    accounting_bucket,
                    risk_context,
                    qty,
                    qty_raw,
                    qty_decimals,
                    cost_sol,
                    cost_lamports,
                    opened_ts
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    wallet_id,
                    token,
                    accounting_bucket,
                    risk_context,
                    qty,
                    qty_exact.as_ref().map(|value| value.raw().to_string()),
                    qty_exact.as_ref().map(|value| i64::from(value.decimals())),
                    cost_sol,
                    u64_to_sql_i64("shadow_lots.cost_lamports", cost_lamports.as_u64())
                        .map_err(to_sql_conversion_error)?,
                    opened_ts.to_rfc3339()
                ],
            )?;
            Ok(conn.last_insert_rowid())
        })
        .context("failed to insert shadow lot")
    }

    pub fn has_shadow_lots(&self, wallet_id: &str, token: &str) -> Result<bool> {
        let has_lots: i64 = self
            .conn
            .query_row(
                "SELECT EXISTS(
                    SELECT 1
                    FROM shadow_lots
                    WHERE wallet_id = ?1
                      AND token = ?2
                      AND qty > ?3
                )",
                params![wallet_id, token, SHADOW_LOT_OPEN_EPS],
                |row| row.get(0),
            )
            .context("failed querying shadow lots existence")?;
        Ok(has_lots > 0)
    }

    pub fn list_shadow_open_pairs(&self) -> Result<HashSet<(String, String)>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT DISTINCT wallet_id, token
                 FROM shadow_lots
                 WHERE qty > ?1",
            )
            .context("failed to prepare shadow open lots query")?;
        let mut rows = stmt
            .query(params![SHADOW_LOT_OPEN_EPS])
            .context("failed querying shadow open lots")?;
        let mut pairs = HashSet::new();
        while let Some(row) = rows.next().context("failed iterating shadow open lots")? {
            pairs.insert((
                row.get(0)
                    .context("failed reading shadow_lots.wallet_id in open lots query")?,
                row.get(1)
                    .context("failed reading shadow_lots.token in open lots query")?,
            ));
        }
        Ok(pairs)
    }

    pub fn list_open_shadow_lots_older_than(
        &self,
        cutoff: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<ShadowLotRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, wallet_id, token, accounting_bucket, risk_context, qty, qty_raw, qty_decimals, cost_sol, cost_lamports, opened_ts
                 FROM shadow_lots
                 WHERE qty > ?1
                   AND opened_ts <= ?2
                 ORDER BY opened_ts ASC, id ASC
                 LIMIT ?3",
            )
            .context("failed to prepare stale shadow lot query")?;
        let mut rows = stmt
            .query(params![
                SHADOW_LOT_OPEN_EPS,
                cutoff.to_rfc3339(),
                limit.max(1) as i64
            ])
            .context("failed querying stale shadow lots")?;
        let mut lots = Vec::new();
        while let Some(row) = rows.next().context("failed iterating stale shadow lots")? {
            lots.push(read_shadow_lot_row(row)?);
        }
        Ok(lots)
    }

    pub fn list_shadow_lots(&self, wallet_id: &str, token: &str) -> Result<Vec<ShadowLotRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, wallet_id, token, accounting_bucket, risk_context, qty, qty_raw, qty_decimals, cost_sol, cost_lamports, opened_ts
                 FROM shadow_lots
                 WHERE wallet_id = ?1 AND token = ?2
                 ORDER BY id ASC",
            )
            .context("failed to prepare shadow lot query")?;
        let mut rows = stmt
            .query(params![wallet_id, token])
            .context("failed querying shadow lots")?;

        let mut lots = Vec::new();
        while let Some(row) = rows.next().context("failed iterating shadow lots")? {
            lots.push(read_shadow_lot_row(row)?);
        }
        Ok(lots)
    }
}

pub(crate) fn read_shadow_lot_row(row: &rusqlite::Row<'_>) -> Result<ShadowLotRow> {
    read_shadow_lot_row_result(row).map_err(anyhow::Error::from)
}

pub(crate) fn read_shadow_lot_row_sql(row: &rusqlite::Row<'_>) -> rusqlite::Result<ShadowLotRow> {
    read_shadow_lot_row_result(row).map_err(to_sql_conversion_error)
}

fn read_shadow_lot_row_result(row: &rusqlite::Row<'_>) -> Result<ShadowLotRow> {
    let opened_raw: String = row
        .get(10)
        .context("failed reading shadow_lots.opened_ts")?;
    let opened_ts = DateTime::parse_from_rfc3339(&opened_raw)
        .map(|dt| dt.with_timezone(&Utc))
        .with_context(|| format!("invalid shadow_lots.opened_ts value: {opened_raw}"))?;
    let qty_exact = token_quantity_from_sql(
        row.get(6).context("failed reading shadow_lots.qty_raw")?,
        row.get(7)
            .context("failed reading shadow_lots.qty_decimals")?,
        "listing shadow lots",
    )?;
    let cost_lamports_raw: Option<i64> = row
        .get(9)
        .context("failed reading shadow_lots.cost_lamports")?;
    let cost_lamports = match cost_lamports_raw {
        Some(raw) if raw < 0 => {
            return Err(anyhow!(
                "invalid negative shadow_lots.cost_lamports={raw} while listing lots"
            ));
        }
        Some(raw) => Some(copybot_core_types::Lamports::new(raw as u64)),
        None => None,
    };
    Ok(ShadowLotRow {
        id: row.get(0).context("failed reading shadow_lots.id")?,
        wallet_id: row.get(1).context("failed reading shadow_lots.wallet_id")?,
        token: row.get(2).context("failed reading shadow_lots.token")?,
        accounting_bucket: row
            .get(3)
            .context("failed reading shadow_lots.accounting_bucket")?,
        risk_context: row
            .get(4)
            .context("failed reading shadow_lots.risk_context")?,
        qty: row.get(5).context("failed reading shadow_lots.qty")?,
        qty_exact,
        cost_sol: row.get(8).context("failed reading shadow_lots.cost_sol")?,
        cost_lamports,
        opened_ts,
    })
}

pub(crate) fn to_sql_conversion_error(error: anyhow::Error) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(io::Error::other(error.to_string())))
}

pub(crate) fn reject_zero_raw_exact_qty(
    qty_exact: Option<TokenQuantity>,
    context: &str,
) -> Result<Option<TokenQuantity>> {
    match qty_exact {
        Some(qty_exact) if qty_exact.raw() == 0 => {
            Err(anyhow!("zero-raw exact quantity is invalid in {context}"))
        }
        other => Ok(other),
    }
}

pub(crate) fn shadow_accounting_bucket_for_qty_exact(
    qty_exact: Option<TokenQuantity>,
) -> &'static str {
    if qty_exact.is_some() {
        POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER
    } else {
        POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER
    }
}

pub(crate) fn validate_shadow_risk_context(risk_context: &str) -> Result<()> {
    match risk_context {
        SHADOW_RISK_CONTEXT_MARKET | SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY => Ok(()),
        other => Err(anyhow!("unsupported shadow risk_context: {other}")),
    }
}
