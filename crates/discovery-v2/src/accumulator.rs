use crate::token_market::{is_sol_buy, is_sol_sell};
use crate::tradability::BuyTradability;
use chrono::{DateTime, Utc};
use copybot_config::DiscoveryConfig;
use copybot_core_types::SwapEvent;
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Clone)]
pub(crate) struct WalletAccumulator {
    pub trades: u32,
    pub buys: u32,
    pub sells: u32,
    pub spent_sol: f64,
    pub max_buy_notional_sol: f64,
    pub realized_pnl_sol: f64,
    pub wins: u32,
    pub closed_trades: u32,
    pub hold_samples_sec: Vec<i64>,
    pub realized_pnl_by_day: HashMap<chrono::NaiveDate, f64>,
    pub active_days: HashSet<chrono::NaiveDate>,
    pub tx_per_minute: HashMap<i64, u32>,
    pub suspicious: bool,
    pub positions: HashMap<String, VecDeque<OpenLot>>,
    pub buy_observations: Vec<BuyObservation>,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub(crate) struct BuyObservation {
    pub token: String,
    pub ts: DateTime<Utc>,
    pub tradable: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct OpenLot {
    qty: f64,
    cost_sol: f64,
    opened_at: DateTime<Utc>,
}

impl WalletAccumulator {
    pub fn new(ts: DateTime<Utc>) -> Self {
        Self {
            trades: 0,
            buys: 0,
            sells: 0,
            spent_sol: 0.0,
            max_buy_notional_sol: 0.0,
            realized_pnl_sol: 0.0,
            wins: 0,
            closed_trades: 0,
            hold_samples_sec: Vec::new(),
            realized_pnl_by_day: HashMap::new(),
            active_days: HashSet::new(),
            tx_per_minute: HashMap::new(),
            suspicious: false,
            positions: HashMap::new(),
            buy_observations: Vec::new(),
            first_seen: ts,
            last_seen: ts,
        }
    }

    pub fn observe_swap(
        &mut self,
        swap: &SwapEvent,
        discovery: &DiscoveryConfig,
        tradability: Option<BuyTradability>,
    ) {
        self.trades = self.trades.saturating_add(1);
        self.active_days.insert(swap.ts_utc.date_naive());
        self.first_seen = self.first_seen.min(swap.ts_utc);
        self.last_seen = self.last_seen.max(swap.ts_utc);
        self.mark_tx_minute(swap.ts_utc.timestamp() / 60, discovery.max_tx_per_minute);
        if is_sol_buy(swap) {
            self.observe_buy(
                swap.token_out.as_str(),
                swap.amount_out,
                swap.amount_in,
                swap.ts_utc,
                tradability.unwrap_or(BuyTradability::Rejected),
            );
        } else if is_sol_sell(swap) {
            self.observe_sell(
                swap.token_in.as_str(),
                swap.amount_in,
                swap.amount_out,
                swap.ts_utc,
            );
        }
    }

    pub fn has_actionable_open_positions(
        &self,
        now: DateTime<Utc>,
        metric_snapshot_interval_seconds: u64,
    ) -> bool {
        let max_age =
            self.actionable_open_position_max_age_seconds(metric_snapshot_interval_seconds);
        self.positions.values().flatten().any(|lot| {
            lot.qty > 1e-12
                && lot.cost_sol > 1e-12
                && max_age.is_none_or(|limit| (now - lot.opened_at).num_seconds().max(0) <= limit)
        })
    }

    fn observe_buy(
        &mut self,
        token: &str,
        qty: f64,
        cost_sol: f64,
        ts: DateTime<Utc>,
        tradability: BuyTradability,
    ) {
        self.buys = self.buys.saturating_add(1);
        if qty <= 0.0 || cost_sol <= 0.0 {
            return;
        }
        self.spent_sol += cost_sol;
        self.max_buy_notional_sol = self.max_buy_notional_sol.max(cost_sol);
        self.buy_observations.push(BuyObservation {
            token: token.to_string(),
            ts,
            tradable: matches!(tradability, BuyTradability::Tradable),
        });
        self.positions
            .entry(token.to_string())
            .or_default()
            .push_back(OpenLot {
                qty,
                cost_sol,
                opened_at: ts,
            });
    }

    fn observe_sell(&mut self, token: &str, qty: f64, proceeds_sol: f64, ts: DateTime<Utc>) {
        self.sells = self.sells.saturating_add(1);
        if qty <= 0.0 || proceeds_sol <= 0.0 {
            return;
        }
        let Some(lots) = self.positions.get_mut(token) else {
            return;
        };
        let mut qty_remaining = qty;
        let mut matched_qty = 0.0;
        let mut sell_pnl = 0.0;
        while qty_remaining > 1e-12 {
            let Some(front) = lots.front_mut() else { break };
            let take_qty = qty_remaining.min(front.qty);
            let lot_fraction = take_qty / front.qty;
            let cost_part = front.cost_sol * lot_fraction;
            let opened_at = front.opened_at;
            front.qty -= take_qty;
            front.cost_sol -= cost_part;
            qty_remaining -= take_qty;
            matched_qty += take_qty;
            sell_pnl += proceeds_sol * (take_qty / qty) - cost_part;
            self.hold_samples_sec
                .push((ts - opened_at).num_seconds().max(0));
            if front.qty <= 1e-12 {
                lots.pop_front();
            }
        }
        if lots.is_empty() {
            self.positions.remove(token);
        }
        if matched_qty > 1e-12 {
            self.realized_pnl_sol += sell_pnl;
            self.closed_trades = self.closed_trades.saturating_add(1);
            self.wins += u32::from(sell_pnl > 0.0);
            *self
                .realized_pnl_by_day
                .entry(ts.date_naive())
                .or_insert(0.0) += sell_pnl;
        }
    }

    fn mark_tx_minute(&mut self, minute_bucket: i64, max_tx_per_minute: u32) {
        let next = self
            .tx_per_minute
            .entry(minute_bucket)
            .and_modify(|value| *value += 1)
            .or_insert(1);
        if *next > max_tx_per_minute.max(1) {
            self.suspicious = true;
        }
    }

    fn actionable_open_position_max_age_seconds(&self, cadence_seconds: u64) -> Option<i64> {
        if self.hold_samples_sec.len() < 3 {
            return None;
        }
        let cadence_floor = i64::try_from(cadence_seconds.max(1)).unwrap_or(i64::MAX);
        let historical = self
            .hold_samples_sec
            .iter()
            .copied()
            .max()
            .unwrap_or(0)
            .saturating_mul(4);
        Some(cadence_floor.max(historical))
    }
}
