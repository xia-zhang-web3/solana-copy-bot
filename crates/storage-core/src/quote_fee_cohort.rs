use crate::{
    execution_canary_quote_pnl_rows::{QuoteEvent, QuotePnlRow},
    quote_fee_allocation::allocate,
    quote_fee_identity::{bind_entry, bind_exit, events},
    SqliteDiscoveryStore,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use rusqlite::params;
use std::collections::{BTreeMap, HashSet};

const CLOSE_COLUMNS: &str = "id, signal_id, wallet_id, token, pnl_sol, opened_ts, closed_ts, COALESCE(close_context, 'market')";
// All side spellings accepted by the table's lower(side) constraint, while still
// allowing the existing side_request_ts index to serve the leading equality.
const SELL_SIDE: &str = "side IN ('sell','selL','seLl','seLL','sEll','sElL','sELl','sELL','Sell','SelL','SeLl','SeLL','SEll','SElL','SELl','SELL')";
const BUY_SIDE: &str = "side IN ('buy','buY','bUy','bUY','Buy','BuY','BUy','BUY')";

pub(crate) struct QuoteFeeCohort {
    pub(crate) window_total_closed_trades: u64,
    pub(crate) rows: Vec<QuotePnlRow>,
}

impl SqliteDiscoveryStore {
    pub(crate) fn quote_fee_cohort_rows(
        &self,
        since: DateTime<Utc>,
        as_of: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> Result<Vec<QuotePnlRow>> {
        Ok(self.quote_fee_cohort(since, as_of, limit)?.rows)
    }

    pub(crate) fn quote_fee_cohort(
        &self,
        since: DateTime<Utc>,
        as_of: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> Result<QuoteFeeCohort> {
        // A single read snapshot covers selection, attribution and prior history.
        let snapshot = self.conn.unchecked_transaction()?;
        let range = bounds(since, as_of);
        let mut selected = closes(
            self,
            "closed_ts >= ?1 AND (?2 IS NULL OR closed_ts < ?2)",
            params![range.0, range.1],
        )?;
        selected
            .retain(|r| r.market && r.closed_ts >= since && as_of.is_none_or(|t| r.closed_ts <= t));
        // Count the exact qualifying set in this snapshot, before truncation.
        // The shadow breakdown has a different window contract and cannot supply it.
        let window_total_closed_trades =
            u64::try_from(selected.len()).context("quote cohort window count exceeds u64")?;
        selected.sort_by_key(|r| std::cmp::Reverse((r.closed_ts, r.id)));
        if let Some(limit) = limit {
            selected.truncate(limit.max(1) as usize);
        }
        let ids: HashSet<_> = selected.iter().map(|r| r.id).collect();
        let groups: BTreeMap<_, _> = selected
            .into_iter()
            .map(|r| ((r.wallet_id, r.token, r.opened_ts), ()))
            .collect();
        let mut result = Vec::new();
        let mut quote_context = BTreeMap::new();
        for ((wallet, token, opened), ()) in groups {
            let range = bounds(opened, as_of);
            let opened_end = bounds(opened, Some(opened)).1;
            // wallet_closed_ts index bounds the prior query to selected groups' wallets
            // through as_of, including malformed, non-market and missing-quote closes.
            let mut history = closes(
                self,
                "wallet_id = ?1 AND token = ?2 AND opened_ts >= ?3 AND (?4 IS NULL OR opened_ts < ?4) AND (?5 IS NULL OR closed_ts < ?5)",
                params![wallet, token, range.0, opened_end, range.1],
            )?;
            history.retain(|r| r.opened_ts == opened && as_of.is_none_or(|t| r.closed_ts <= t));
            history.sort_by_key(|r| (r.closed_ts, r.id));
            let context = (wallet.clone(), token.clone());
            if !quote_context.contains_key(&context) {
                let buys = events(self, &format!("{BUY_SIDE} AND wallet_id = ?1 AND token = ?2 AND event_id NOT LIKE 'quote:entry-shadow-diag:%'"), params![wallet, token])?;
                // Positive gap evidence: a retained SELL has lost its persisted close.
                // No entry identity can be recovered for it. Cache per wallet/token,
                // so multiple selected entries never repeat these side-index scans.
                let orphans = events(self, &format!("{SELL_SIDE} AND wallet_id = ?1 AND token = ?2 AND shadow_closed_trade_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM shadow_closed_trades c WHERE c.id = shadow_closed_trade_id)"), params![wallet, token])?;
                quote_context.insert(context.clone(), (buys, orphans));
            }
            let (candidates, orphans) = &quote_context[&context];
            let undated_gap = orphans.iter().any(|q| q.signal_ts.is_none());
            let gap_time = orphans
                .iter()
                .filter_map(|q| q.signal_ts)
                .filter(|t| *t >= opened && as_of.is_none_or(|a| *t <= a))
                .min();
            for row in &mut history {
                bind_entry(self, row, candidates)?;
                bind_exit(self, row)?;
                if undated_gap || gap_time.is_some_and(|t| t <= row.closed_ts) {
                    row.binding_error.get_or_insert("missing_close_history");
                }
            }
            allocate(&mut history);
            result.extend(history.into_iter().filter(|r| ids.contains(&r.id)));
        }
        result.sort_by_key(|r| std::cmp::Reverse((r.closed_ts, r.id)));
        snapshot.commit()?;
        Ok(QuoteFeeCohort {
            window_total_closed_trades,
            rows: result,
        })
    }
}

fn bounds(since: DateTime<Utc>, as_of: Option<DateTime<Utc>>) -> (String, Option<String>) {
    // Stored closes use UTC RFC3339. Broad second bounds preserve the indexed range;
    // chrono comparison above retains every fractional digit before order/limit.
    (
        since.to_rfc3339_opts(SecondsFormat::Secs, false),
        as_of
            .and_then(|t| t.checked_add_signed(Duration::seconds(1)))
            .map(|t| t.to_rfc3339_opts(SecondsFormat::Secs, false)),
    )
}

fn closes(
    store: &SqliteDiscoveryStore,
    predicate: &str,
    args: impl rusqlite::Params,
) -> Result<Vec<QuotePnlRow>> {
    let mut stmt = store.conn.prepare(&format!(
        "SELECT {CLOSE_COLUMNS} FROM shadow_closed_trades WHERE {predicate}"
    ))?;
    let mut cursor = stmt.query(args)?;
    let mut rows = Vec::new();
    while let Some(r) = cursor.next()? {
        let signal_id: String = r.get(1)?;
        let context: String = r.get(7)?;
        rows.push(QuotePnlRow {
            id: r.get(0)?,
            market: context == "market" && !signal_id.starts_with("stale-close-"),
            signal_id,
            wallet_id: r.get(2)?,
            token: r.get(3)?,
            shadow_pnl_sol: r.get(4)?,
            opened_ts: r.get::<_, String>(5)?.parse()?,
            closed_ts: r.get::<_, String>(6)?.parse()?,
            buy: QuoteEvent::default(),
            sell: QuoteEvent::default(),
            allocation: Default::default(),
            entry_attributed: false,
            binding_error: None,
        });
    }
    Ok(rows)
}
