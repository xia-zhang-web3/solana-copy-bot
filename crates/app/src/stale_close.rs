use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_storage::{
    SqliteStore, STALE_CLOSE_RELIABLE_PRICE_MAX_SAMPLES, STALE_CLOSE_RELIABLE_PRICE_MIN_SAMPLES,
    STALE_CLOSE_RELIABLE_PRICE_MIN_SOL_NOTIONAL, STALE_CLOSE_RELIABLE_PRICE_WINDOW_MINUTES,
};
use std::collections::HashSet;
use tracing::warn;

use super::{sanitize_json_value, STALE_LOT_CLEANUP_BATCH_LIMIT};

pub(crate) fn close_stale_shadow_lots(
    store: &SqliteStore,
    open_shadow_lots: &mut HashSet<(String, String)>,
    max_hold_hours: u32,
    now: DateTime<Utc>,
) -> Result<(u64, u64)> {
    const EPS: f64 = 1e-12;

    if max_hold_hours == 0 {
        return Ok((0, 0));
    }

    let cutoff = now - chrono::Duration::hours(max_hold_hours as i64);
    let stale_lots = store
        .list_open_shadow_lots_older_than(cutoff, STALE_LOT_CLEANUP_BATCH_LIMIT)
        .context("failed to list stale shadow lots")?;
    if stale_lots.is_empty() {
        return Ok((0, 0));
    }

    let mut closed = 0u64;
    let mut skipped_unpriced = 0u64;

    for lot in stale_lots {
        if lot.qty <= EPS {
            continue;
        }

        let Some(exit_price_sol) =
            store.reliable_token_sol_price_for_stale_close(&lot.token, now)?
        else {
            skipped_unpriced = skipped_unpriced.saturating_add(1);
            let details_json = format!(
                "{{\"wallet_id\":\"{}\",\"token\":\"{}\",\"lot_id\":{},\"as_of\":\"{}\",\"window_minutes\":{},\"min_notional_sol\":{},\"min_samples\":{},\"max_samples\":{}}}",
                sanitize_json_value(&lot.wallet_id),
                sanitize_json_value(&lot.token),
                lot.id,
                sanitize_json_value(&now.to_rfc3339()),
                STALE_CLOSE_RELIABLE_PRICE_WINDOW_MINUTES,
                STALE_CLOSE_RELIABLE_PRICE_MIN_SOL_NOTIONAL,
                STALE_CLOSE_RELIABLE_PRICE_MIN_SAMPLES,
                STALE_CLOSE_RELIABLE_PRICE_MAX_SAMPLES
            );
            if let Err(error) = store.insert_risk_event(
                "shadow_stale_close_price_unavailable",
                "warn",
                now,
                Some(&details_json),
            ) {
                warn!(
                    error = %error,
                    wallet_id = %lot.wallet_id,
                    token = %lot.token,
                    lot_id = lot.id,
                    "failed to record stale-close price unavailable risk event"
                );
            }
            continue;
        };
        if exit_price_sol <= EPS {
            skipped_unpriced = skipped_unpriced.saturating_add(1);
            continue;
        }

        let signal_id = format!("stale-close-{}-{}", lot.id, now.timestamp_millis());
        let close = store
            .close_shadow_lots_fifo_atomic(
                &signal_id,
                &lot.wallet_id,
                &lot.token,
                lot.qty,
                exit_price_sol,
                now,
            )
            .with_context(|| {
                format!(
                    "failed stale close for wallet={} token={} lot_id={}",
                    lot.wallet_id, lot.token, lot.id
                )
            })?;

        if close.closed_qty > EPS {
            closed = closed.saturating_add(1);
        }

        let key = (lot.wallet_id.clone(), lot.token.clone());
        if close.has_open_lots_after {
            open_shadow_lots.insert(key);
        } else {
            open_shadow_lots.remove(&key);
        }
    }

    Ok((closed, skipped_unpriced))
}
