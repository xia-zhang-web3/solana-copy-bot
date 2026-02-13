use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_config::ShadowConfig;
use copybot_core_types::SwapEvent;
use copybot_storage::{CopySignalRow, SqliteStore};
use std::collections::HashSet;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const EPS: f64 = 1e-12;

#[derive(Debug, Clone)]
pub struct ShadowService {
    config: ShadowConfig,
}

#[derive(Debug, Clone)]
pub struct ShadowSignalResult {
    pub signal_id: String,
    pub wallet_id: String,
    pub side: String,
    pub token: String,
    pub notional_sol: f64,
    pub latency_ms: i64,
    pub closed_qty: f64,
    pub realized_pnl_sol: f64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ShadowSnapshot {
    pub closed_trades_24h: u64,
    pub realized_pnl_sol_24h: f64,
    pub open_lots: u64,
}

#[derive(Debug, Clone)]
struct ShadowCandidate {
    side: String,
    token: String,
    leader_notional_sol: f64,
    price_sol_per_token: f64,
}

#[derive(Debug, Clone, Copy, Default)]
struct CloseResult {
    closed_qty: f64,
    realized_pnl_sol: f64,
}

impl ShadowService {
    pub fn new(config: ShadowConfig) -> Self {
        Self { config }
    }

    pub fn process_swap(
        &self,
        store: &SqliteStore,
        swap: &SwapEvent,
        active_follow_wallets: &HashSet<String>,
        now: DateTime<Utc>,
    ) -> Result<Option<ShadowSignalResult>> {
        if !self.config.enabled {
            return Ok(None);
        }
        if !active_follow_wallets.contains(&swap.wallet) {
            return Ok(None);
        }

        let Some(candidate) = Self::to_shadow_candidate(swap) else {
            return Ok(None);
        };
        if candidate.leader_notional_sol < self.config.min_leader_notional_sol {
            return Ok(None);
        }

        let latency_ms = (now - swap.ts_utc).num_milliseconds();
        if latency_ms > (self.config.max_signal_lag_seconds as i64 * 1_000) {
            return Ok(None);
        }

        let copy_notional_sol = self
            .config
            .copy_notional_sol
            .min(candidate.leader_notional_sol);
        if copy_notional_sol <= EPS || candidate.price_sol_per_token <= EPS {
            return Ok(None);
        }
        let signal_id = format!(
            "shadow:{}:{}:{}:{}",
            swap.signature, swap.wallet, candidate.side, candidate.token
        );
        let signal = CopySignalRow {
            signal_id: signal_id.clone(),
            wallet_id: swap.wallet.clone(),
            side: candidate.side.clone(),
            token: candidate.token.clone(),
            notional_sol: copy_notional_sol,
            ts: swap.ts_utc,
            status: "shadow_recorded".to_string(),
        };
        if !store.insert_copy_signal(&signal)? {
            return Ok(None);
        }

        let mut close = CloseResult::default();
        match candidate.side.as_str() {
            "buy" => {
                let qty = copy_notional_sol / candidate.price_sol_per_token;
                if qty > EPS {
                    let _ = store.insert_shadow_lot(
                        &swap.wallet,
                        &candidate.token,
                        qty,
                        copy_notional_sol,
                        swap.ts_utc,
                    )?;
                }
            }
            "sell" => {
                let qty = copy_notional_sol / candidate.price_sol_per_token;
                close = self.close_fifo_lots(
                    store,
                    &signal_id,
                    &swap.wallet,
                    &candidate.token,
                    qty,
                    candidate.price_sol_per_token,
                    now,
                )?;
            }
            _ => return Ok(None),
        }

        Ok(Some(ShadowSignalResult {
            signal_id,
            wallet_id: swap.wallet.clone(),
            side: candidate.side,
            token: candidate.token,
            notional_sol: copy_notional_sol,
            latency_ms,
            closed_qty: close.closed_qty,
            realized_pnl_sol: close.realized_pnl_sol,
        }))
    }

    pub fn snapshot_24h(&self, store: &SqliteStore, now: DateTime<Utc>) -> Result<ShadowSnapshot> {
        let since = now - Duration::hours(24);
        let (closed_trades_24h, realized_pnl_sol_24h) = store.shadow_realized_pnl_since(since)?;
        let open_lots = store.shadow_open_lots_count()?;
        Ok(ShadowSnapshot {
            closed_trades_24h,
            realized_pnl_sol_24h,
            open_lots,
        })
    }

    fn close_fifo_lots(
        &self,
        store: &SqliteStore,
        signal_id: &str,
        wallet_id: &str,
        token: &str,
        target_qty: f64,
        exit_price_sol: f64,
        closed_ts: DateTime<Utc>,
    ) -> Result<CloseResult> {
        if target_qty <= EPS {
            return Ok(CloseResult::default());
        }

        let lots = store.list_shadow_lots(wallet_id, token)?;
        let mut qty_remaining = target_qty;
        let mut closed_qty = 0.0;
        let mut realized_pnl_sol = 0.0;

        for lot in lots {
            if qty_remaining <= EPS {
                break;
            }
            if lot.qty <= EPS {
                store.delete_shadow_lot(lot.id)?;
                continue;
            }

            let take_qty = qty_remaining.min(lot.qty);
            let entry_cost_sol = lot.cost_sol * (take_qty / lot.qty);
            let remaining_qty = (lot.qty - take_qty).max(0.0);
            let remaining_cost = (lot.cost_sol - entry_cost_sol).max(0.0);
            if remaining_qty <= EPS {
                store.delete_shadow_lot(lot.id)?;
            } else {
                store.update_shadow_lot(lot.id, remaining_qty, remaining_cost)?;
            }

            let exit_value_sol = take_qty * exit_price_sol;
            let pnl_sol = exit_value_sol - entry_cost_sol;
            store.insert_shadow_closed_trade(
                signal_id,
                wallet_id,
                token,
                take_qty,
                entry_cost_sol,
                exit_value_sol,
                pnl_sol,
                lot.opened_ts,
                closed_ts,
            )?;
            realized_pnl_sol += pnl_sol;
            closed_qty += take_qty;
            qty_remaining -= take_qty;
        }

        Ok(CloseResult {
            closed_qty,
            realized_pnl_sol,
        })
    }

    fn to_shadow_candidate(swap: &SwapEvent) -> Option<ShadowCandidate> {
        if swap.amount_in <= EPS || swap.amount_out <= EPS {
            return None;
        }

        if swap.token_in == SOL_MINT && swap.token_out != SOL_MINT {
            return Some(ShadowCandidate {
                side: "buy".to_string(),
                token: swap.token_out.clone(),
                leader_notional_sol: swap.amount_in,
                price_sol_per_token: swap.amount_in / swap.amount_out,
            });
        }

        if swap.token_out == SOL_MINT && swap.token_in != SOL_MINT {
            return Some(ShadowCandidate {
                side: "sell".to_string(),
                token: swap.token_in.clone(),
                leader_notional_sol: swap.amount_out,
                price_sol_per_token: swap.amount_out / swap.amount_in,
            });
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use copybot_core_types::SwapEvent;
    use copybot_storage::SqliteStore;
    use std::path::Path;
    use tempfile::tempdir;

    #[test]
    fn creates_shadow_signal_and_realized_pnl() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-test.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut follow = HashSet::new();
        follow.insert("leader-wallet".to_string());

        let mut cfg = ShadowConfig::default();
        cfg.copy_notional_sol = 0.5;
        cfg.min_leader_notional_sol = 0.25;
        let service = ShadowService::new(cfg);

        let buy_ts = DateTime::parse_from_rfc3339("2026-02-12T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let sell_ts = DateTime::parse_from_rfc3339("2026-02-12T12:05:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let buy = SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "TokenMint".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-buy".to_string(),
            slot: 1,
            ts_utc: buy_ts,
        };
        let buy_signal = service
            .process_swap(&store, &buy, &follow, buy_ts + Duration::seconds(1))?
            .expect("buy signal expected");
        assert_eq!(buy_signal.side, "buy");
        assert!(store.shadow_open_lots_count()? > 0);

        let sell = SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "TokenMint".to_string(),
            token_out: SOL_MINT.to_string(),
            amount_in: 1000.0,
            amount_out: 1.2,
            signature: "sig-sell".to_string(),
            slot: 2,
            ts_utc: sell_ts,
        };
        let sell_signal = service
            .process_swap(&store, &sell, &follow, sell_ts + Duration::seconds(1))?
            .expect("sell signal expected");
        assert_eq!(sell_signal.side, "sell");
        assert!(sell_signal.realized_pnl_sol > 0.0);

        let snapshot = service.snapshot_24h(&store, sell_ts + Duration::seconds(2))?;
        assert!(snapshot.closed_trades_24h >= 1);
        assert!(snapshot.realized_pnl_sol_24h > 0.0);
        Ok(())
    }
}
