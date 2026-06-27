use crate::universe_de_risk_report_db::ObservedSolLegRow;
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet, VecDeque};

const EPSILON_QTY: f64 = 1e-12;

#[derive(Debug, Default)]
pub(crate) struct TokenAcc {
    pub(crate) dex_counts: HashMap<String, u64>,
    pub(crate) wallets: HashSet<String>,
    pub(crate) notionals: Vec<f64>,
    pub(crate) swaps: u64,
    pub(crate) buys: u64,
    pub(crate) sells: u64,
    pub(crate) volume_sol: f64,
    pub(crate) last_ts: Option<DateTime<Utc>>,
    pub(crate) round_trips: u64,
    pub(crate) wins: u64,
    pub(crate) pnl_sol: f64,
    pub(crate) holds: Vec<i64>,
    pub(crate) unmatched_open_cost_sol: f64,
}

#[derive(Debug, Default)]
pub(crate) struct WalletSegmentAcc {
    pub(crate) round_trips: u64,
    pub(crate) pnl_sol: f64,
    pub(crate) holds: Vec<i64>,
}

#[derive(Debug, Default)]
pub(crate) struct ReplayCounters {
    pub(crate) unmatched_sell_events: u64,
    pub(crate) unmatched_sell_proceeds_sol: f64,
}

pub(crate) struct ReplayResult {
    pub(crate) tokens: HashMap<String, TokenAcc>,
    pub(crate) wallet_totals: HashMap<String, WalletSegmentAcc>,
    pub(crate) wallet_segments: HashMap<(String, String), WalletSegmentAcc>,
    pub(crate) wallet_tokens: HashMap<(String, String), WalletSegmentAcc>,
    pub(crate) dex_wallets: HashMap<String, HashSet<String>>,
    pub(crate) all_wallets: HashSet<String>,
    pub(crate) counters: ReplayCounters,
}

#[derive(Debug, Clone)]
struct Lot {
    qty: f64,
    cost_sol: f64,
    ts: DateTime<Utc>,
    dex: String,
}

struct CloseRecord<'a> {
    row: &'a ObservedSolLegRow,
    dex: &'a str,
    pnl_sol: f64,
    hold_seconds: i64,
}

pub(crate) fn replay_rows(rows: Vec<ObservedSolLegRow>) -> ReplayResult {
    let mut tokens: HashMap<String, TokenAcc> = HashMap::new();
    let mut wallet_totals: HashMap<String, WalletSegmentAcc> = HashMap::new();
    let mut wallet_segments: HashMap<(String, String), WalletSegmentAcc> = HashMap::new();
    let mut wallet_tokens: HashMap<(String, String), WalletSegmentAcc> = HashMap::new();
    let mut dex_wallets: HashMap<String, HashSet<String>> = HashMap::new();
    let mut positions: HashMap<(String, String), VecDeque<Lot>> = HashMap::new();
    let mut all_wallets = HashSet::new();
    let mut counters = ReplayCounters::default();

    for row in rows {
        observe_row(&mut tokens, &mut all_wallets, &mut dex_wallets, &row);
        if row.is_buy {
            record_buy(&mut positions, &row);
        } else {
            record_sell(
                &mut positions,
                &mut tokens,
                &mut wallet_totals,
                &mut wallet_segments,
                &mut wallet_tokens,
                &mut counters,
                &row,
            );
        }
    }
    apply_unmatched_open_cost(&mut tokens, &positions);

    ReplayResult {
        tokens,
        wallet_totals,
        wallet_segments,
        wallet_tokens,
        dex_wallets,
        all_wallets,
        counters,
    }
}

fn observe_row(
    tokens: &mut HashMap<String, TokenAcc>,
    all_wallets: &mut HashSet<String>,
    dex_wallets: &mut HashMap<String, HashSet<String>>,
    row: &ObservedSolLegRow,
) {
    if !row.sol_notional.is_finite() || row.sol_notional <= 0.0 {
        return;
    }
    all_wallets.insert(row.wallet_id.clone());
    dex_wallets
        .entry(row.dex.clone())
        .or_default()
        .insert(row.wallet_id.clone());
    let token = tokens.entry(row.token_mint.clone()).or_default();
    *token.dex_counts.entry(row.dex.clone()).or_default() += 1;
    token.wallets.insert(row.wallet_id.clone());
    token.notionals.push(row.sol_notional);
    token.swaps += 1;
    token.volume_sol += row.sol_notional;
    token.last_ts = Some(token.last_ts.map_or(row.ts, |current| current.max(row.ts)));
    if row.is_buy {
        token.buys += 1;
    } else {
        token.sells += 1;
    }
}

fn record_buy(positions: &mut HashMap<(String, String), VecDeque<Lot>>, row: &ObservedSolLegRow) {
    if !valid_amounts(row) {
        return;
    }
    positions
        .entry((row.wallet_id.clone(), row.token_mint.clone()))
        .or_default()
        .push_back(Lot {
            qty: row.token_qty,
            cost_sol: row.sol_notional,
            ts: row.ts,
            dex: row.dex.clone(),
        });
}

fn record_sell(
    positions: &mut HashMap<(String, String), VecDeque<Lot>>,
    tokens: &mut HashMap<String, TokenAcc>,
    wallet_totals: &mut HashMap<String, WalletSegmentAcc>,
    wallet_segments: &mut HashMap<(String, String), WalletSegmentAcc>,
    wallet_tokens: &mut HashMap<(String, String), WalletSegmentAcc>,
    counters: &mut ReplayCounters,
    row: &ObservedSolLegRow,
) {
    if !valid_amounts(row) {
        return;
    }
    let key = (row.wallet_id.clone(), row.token_mint.clone());
    let Some(lots) = positions.get_mut(&key) else {
        counters.unmatched_sell_events += 1;
        counters.unmatched_sell_proceeds_sol += row.sol_notional;
        return;
    };
    let mut remaining_qty = row.token_qty;
    while remaining_qty > EPSILON_QTY {
        let Some(front) = lots.front_mut() else {
            break;
        };
        let take_qty = remaining_qty.min(front.qty);
        let sell_fraction = take_qty / row.token_qty;
        let lot_fraction = take_qty / front.qty;
        let proceeds_sol = row.sol_notional * sell_fraction;
        let cost_sol = front.cost_sol * lot_fraction;
        let pnl_sol = proceeds_sol - cost_sol;
        let hold_seconds = row.ts.signed_duration_since(front.ts).num_seconds().max(0);
        record_close(
            tokens,
            wallet_totals,
            wallet_segments,
            wallet_tokens,
            CloseRecord {
                row,
                dex: &front.dex,
                pnl_sol,
                hold_seconds,
            },
        );
        front.qty -= take_qty;
        front.cost_sol -= cost_sol;
        remaining_qty -= take_qty;
        if front.qty <= EPSILON_QTY {
            lots.pop_front();
        }
    }
    if lots.is_empty() {
        positions.remove(&key);
        if remaining_qty > EPSILON_QTY {
            counters.unmatched_sell_events += 1;
            counters.unmatched_sell_proceeds_sol +=
                row.sol_notional * (remaining_qty / row.token_qty);
        }
    }
}

fn record_close(
    tokens: &mut HashMap<String, TokenAcc>,
    wallet_totals: &mut HashMap<String, WalletSegmentAcc>,
    wallet_segments: &mut HashMap<(String, String), WalletSegmentAcc>,
    wallet_tokens: &mut HashMap<(String, String), WalletSegmentAcc>,
    close: CloseRecord<'_>,
) {
    let row = close.row;
    if let Some(token) = tokens.get_mut(&row.token_mint) {
        token.round_trips += 1;
        token.pnl_sol += close.pnl_sol;
        token.wins += u64::from(close.pnl_sol > 0.0);
        token.holds.push(close.hold_seconds);
    }
    let total = wallet_totals.entry(row.wallet_id.clone()).or_default();
    total.round_trips += 1;
    total.pnl_sol += close.pnl_sol;
    total.holds.push(close.hold_seconds);

    let segment = wallet_segments
        .entry((row.wallet_id.clone(), close.dex.to_string()))
        .or_default();
    segment.round_trips += 1;
    segment.pnl_sol += close.pnl_sol;
    segment.holds.push(close.hold_seconds);

    let token = wallet_tokens
        .entry((row.wallet_id.clone(), row.token_mint.clone()))
        .or_default();
    token.round_trips += 1;
    token.pnl_sol += close.pnl_sol;
    token.holds.push(close.hold_seconds);
}

fn valid_amounts(row: &ObservedSolLegRow) -> bool {
    row.token_qty.is_finite()
        && row.sol_notional.is_finite()
        && row.token_qty > EPSILON_QTY
        && row.sol_notional > EPSILON_QTY
}

fn apply_unmatched_open_cost(
    tokens: &mut HashMap<String, TokenAcc>,
    positions: &HashMap<(String, String), VecDeque<Lot>>,
) {
    for ((_, token_mint), lots) in positions {
        if let Some(token) = tokens.get_mut(token_mint) {
            token.unmatched_open_cost_sol += lots.iter().map(|lot| lot.cost_sol).sum::<f64>();
        }
    }
}
