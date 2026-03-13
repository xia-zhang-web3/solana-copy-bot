use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_storage::{
    SqliteStore, SHADOW_CLOSE_CONTEXT_MARKET, SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE,
    SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE, STALE_CLOSE_RELIABLE_PRICE_MAX_SAMPLES,
    STALE_CLOSE_RELIABLE_PRICE_MIN_SAMPLES, STALE_CLOSE_RELIABLE_PRICE_MIN_SOL_NOTIONAL,
    STALE_CLOSE_RELIABLE_PRICE_WINDOW_MINUTES,
};
use std::collections::HashSet;
use tracing::warn;

use super::{sanitize_json_value, STALE_LOT_CLEANUP_BATCH_LIMIT};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct StaleLotCleanupStats {
    pub closed_priced: u64,
    pub recovery_zero_closed: u64,
    pub terminal_zero_closed: u64,
    pub skipped_unpriced: u64,
}

pub(crate) fn close_stale_shadow_lots(
    store: &SqliteStore,
    open_shadow_lots: &mut HashSet<(String, String)>,
    max_hold_hours: u32,
    terminal_zero_price_hours: u32,
    recovery_zero_price_enabled: bool,
    now: DateTime<Utc>,
) -> Result<StaleLotCleanupStats> {
    const EPS: f64 = 1e-12;

    if max_hold_hours == 0 {
        return Ok(StaleLotCleanupStats::default());
    }

    let cutoff = now - chrono::Duration::hours(max_hold_hours as i64);
    let stale_lots = store
        .list_open_shadow_lots_older_than(cutoff, STALE_LOT_CLEANUP_BATCH_LIMIT)
        .context("failed to list stale shadow lots")?;
    if stale_lots.is_empty() {
        return Ok(StaleLotCleanupStats::default());
    }

    let mut stats = StaleLotCleanupStats::default();
    let terminal_zero_cutoff = if terminal_zero_price_hours > 0 {
        Some(now - chrono::Duration::hours(terminal_zero_price_hours as i64))
    } else {
        None
    };

    for lot in stale_lots {
        if lot.qty <= EPS {
            continue;
        }

        let (exit_price_sol, close_context) = match store
            .reliable_token_sol_price_for_stale_close(&lot.token, now)?
        {
            Some(price) if price > EPS => (price, SHADOW_CLOSE_CONTEXT_MARKET),
            _ => {
                let terminal_zero_reason_code = if terminal_zero_cutoff.is_none() {
                    "guard_disabled"
                } else if lot.opened_ts > terminal_zero_cutoff.expect("checked above") {
                    "below_terminal_age_threshold"
                } else {
                    "eligible_for_terminal_zero_price"
                };
                let details_json = format!(
                    "{{\"wallet_id\":\"{}\",\"token\":\"{}\",\"lot_id\":{},\"as_of\":\"{}\",\"window_minutes\":{},\"min_notional_sol\":{},\"min_samples\":{},\"max_samples\":{},\"terminal_zero_price_hours\":{},\"terminal_zero_reason_code\":\"{}\"}}",
                    sanitize_json_value(&lot.wallet_id),
                    sanitize_json_value(&lot.token),
                    lot.id,
                    sanitize_json_value(&now.to_rfc3339()),
                    STALE_CLOSE_RELIABLE_PRICE_WINDOW_MINUTES,
                    STALE_CLOSE_RELIABLE_PRICE_MIN_SOL_NOTIONAL,
                    STALE_CLOSE_RELIABLE_PRICE_MIN_SAMPLES,
                    STALE_CLOSE_RELIABLE_PRICE_MAX_SAMPLES,
                    terminal_zero_price_hours,
                    terminal_zero_reason_code
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
                if recovery_zero_price_enabled {
                    if let Some(terminal_zero_cutoff) = terminal_zero_cutoff {
                        if lot.opened_ts > terminal_zero_cutoff {
                            let recovery_details_json = format!(
                                "{{\"wallet_id\":\"{}\",\"token\":\"{}\",\"lot_id\":{},\"as_of\":\"{}\",\"close_strategy\":\"recovery_zero_price\",\"exit_price_sol\":0.0,\"max_hold_hours\":{},\"terminal_zero_price_hours\":{},\"opened_ts\":\"{}\"}}",
                                sanitize_json_value(&lot.wallet_id),
                                sanitize_json_value(&lot.token),
                                lot.id,
                                sanitize_json_value(&now.to_rfc3339()),
                                max_hold_hours,
                                terminal_zero_price_hours,
                                sanitize_json_value(&lot.opened_ts.to_rfc3339())
                            );
                            if let Err(error) = store.insert_risk_event(
                                "shadow_stale_close_recovery_zero_price",
                                "warn",
                                now,
                                Some(&recovery_details_json),
                            ) {
                                warn!(
                                    error = %error,
                                    wallet_id = %lot.wallet_id,
                                    token = %lot.token,
                                    lot_id = lot.id,
                                    "failed to record stale-close recovery zero-price risk event"
                                );
                            }
                            (0.0, SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE)
                        } else {
                            let terminal_details_json = format!(
                                "{{\"wallet_id\":\"{}\",\"token\":\"{}\",\"lot_id\":{},\"as_of\":\"{}\",\"close_strategy\":\"zero_price_terminal\",\"exit_price_sol\":0.0,\"terminal_zero_price_hours\":{},\"opened_ts\":\"{}\"}}",
                                sanitize_json_value(&lot.wallet_id),
                                sanitize_json_value(&lot.token),
                                lot.id,
                                sanitize_json_value(&now.to_rfc3339()),
                                terminal_zero_price_hours,
                                sanitize_json_value(&lot.opened_ts.to_rfc3339())
                            );
                            if let Err(error) = store.insert_risk_event(
                                "shadow_stale_close_terminal_zero_price",
                                "warn",
                                now,
                                Some(&terminal_details_json),
                            ) {
                                warn!(
                                    error = %error,
                                    wallet_id = %lot.wallet_id,
                                    token = %lot.token,
                                    lot_id = lot.id,
                                    "failed to record stale-close terminal zero-price risk event"
                                );
                            }
                            (0.0, SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE)
                        }
                    } else {
                        stats.skipped_unpriced = stats.skipped_unpriced.saturating_add(1);
                        continue;
                    }
                } else {
                    let Some(terminal_zero_cutoff) = terminal_zero_cutoff else {
                        stats.skipped_unpriced = stats.skipped_unpriced.saturating_add(1);
                        continue;
                    };
                    if lot.opened_ts > terminal_zero_cutoff {
                        stats.skipped_unpriced = stats.skipped_unpriced.saturating_add(1);
                        continue;
                    }
                    let terminal_details_json = format!(
                        "{{\"wallet_id\":\"{}\",\"token\":\"{}\",\"lot_id\":{},\"as_of\":\"{}\",\"close_strategy\":\"zero_price_terminal\",\"exit_price_sol\":0.0,\"terminal_zero_price_hours\":{},\"opened_ts\":\"{}\"}}",
                        sanitize_json_value(&lot.wallet_id),
                        sanitize_json_value(&lot.token),
                        lot.id,
                        sanitize_json_value(&now.to_rfc3339()),
                        terminal_zero_price_hours,
                        sanitize_json_value(&lot.opened_ts.to_rfc3339())
                    );
                    if let Err(error) = store.insert_risk_event(
                        "shadow_stale_close_terminal_zero_price",
                        "warn",
                        now,
                        Some(&terminal_details_json),
                    ) {
                        warn!(
                            error = %error,
                            wallet_id = %lot.wallet_id,
                            token = %lot.token,
                            lot_id = lot.id,
                            "failed to record stale-close terminal zero-price risk event"
                        );
                    }
                    (0.0, SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE)
                }
            }
        };

        let signal_id = format!("stale-close-{}-{}", lot.id, now.timestamp_millis());
        let close = store
            .close_shadow_lots_fifo_atomic_exact_with_context(
                &signal_id,
                &lot.wallet_id,
                &lot.token,
                lot.qty,
                lot.qty_exact,
                exit_price_sol,
                close_context,
                now,
            )
            .with_context(|| {
                format!(
                    "failed stale close for wallet={} token={} lot_id={}",
                    lot.wallet_id, lot.token, lot.id
                )
            })?;

        if close.closed_qty > EPS {
            if close_context == SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE {
                stats.recovery_zero_closed = stats.recovery_zero_closed.saturating_add(1);
            } else if close_context == SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE {
                stats.terminal_zero_closed = stats.terminal_zero_closed.saturating_add(1);
            } else {
                stats.closed_priced = stats.closed_priced.saturating_add(1);
            }
        }

        let key = (lot.wallet_id.clone(), lot.token.clone());
        if close.has_open_lots_after {
            open_shadow_lots.insert(key);
        } else {
            open_shadow_lots.remove(&key);
        }
    }

    Ok(stats)
}
