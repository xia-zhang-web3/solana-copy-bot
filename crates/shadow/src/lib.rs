use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ShadowConfig;
use copybot_core_types::{
    Lamports, SwapEvent, TokenQuantity, COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE,
    COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
};
use copybot_storage::{CopySignalRow, SqliteStore};
use std::collections::{HashMap, HashSet};
use tracing::info;

mod candidate;
use self::candidate::to_shadow_candidate;
mod quality_gates;
mod signals;
mod snapshots;
use self::signals::log_gate_drop;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const EPS: f64 = 1e-12;
const QUALITY_CACHE_TTL_SECONDS: i64 = 10 * 60;
const QUALITY_RPC_TIMEOUT_MS: u64 = 700;
const QUALITY_MAX_SIGNATURE_PAGES: u32 = 1;

fn sol_to_lamports_floor(sol: f64) -> Option<Lamports> {
    if !sol.is_finite() || sol < 0.0 {
        return None;
    }
    let scaled = sol * 1_000_000_000.0;
    if !scaled.is_finite() || scaled > u64::MAX as f64 {
        return None;
    }
    Some(Lamports::new(scaled.floor() as u64))
}

fn sol_to_lamports_ceil(sol: f64) -> Option<Lamports> {
    if !sol.is_finite() || sol < 0.0 {
        return None;
    }
    let scaled = sol * 1_000_000_000.0;
    if !scaled.is_finite() || scaled > u64::MAX as f64 {
        return None;
    }
    Some(Lamports::new(scaled.ceil() as u64))
}

fn lamports_to_sol(lamports: Lamports) -> f64 {
    lamports.as_u64() as f64 / 1_000_000_000.0
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScaledExactShadowQty {
    Exact(TokenQuantity),
    Approximate,
    InvalidZeroRaw,
}

fn scaled_exact_shadow_qty(
    exact_token_qty: Option<TokenQuantity>,
    exact_leader_notional_lamports: Option<Lamports>,
    copy_notional_lamports: Option<Lamports>,
    copy_notional_sol: f64,
) -> ScaledExactShadowQty {
    let Some(exact_token_qty) = exact_token_qty else {
        return ScaledExactShadowQty::Approximate;
    };
    let Some(leader_notional) = exact_leader_notional_lamports else {
        return ScaledExactShadowQty::Approximate;
    };
    if leader_notional == Lamports::ZERO {
        return ScaledExactShadowQty::Approximate;
    }
    let Some(copy_notional) =
        copy_notional_lamports.or_else(|| sol_to_lamports_floor(copy_notional_sol))
    else {
        return ScaledExactShadowQty::Approximate;
    };
    if copy_notional > leader_notional {
        return ScaledExactShadowQty::Approximate;
    }
    let Some(scaled_raw) = u128::from(exact_token_qty.raw())
        .checked_mul(u128::from(copy_notional.as_u64()))
        .and_then(|value| value.checked_div(u128::from(leader_notional.as_u64())))
        .and_then(|value| u64::try_from(value).ok())
    else {
        return ScaledExactShadowQty::Approximate;
    };
    if scaled_raw == 0 && copy_notional != Lamports::ZERO {
        return ScaledExactShadowQty::InvalidZeroRaw;
    }
    ScaledExactShadowQty::Exact(TokenQuantity::new(scaled_raw, exact_token_qty.decimals()))
}

#[derive(Debug, Clone)]
pub struct ShadowService {
    config: ShadowConfig,
    copy_notional_lamports: Option<Lamports>,
    min_leader_notional_lamports: Option<Lamports>,
    helius_http_url: Option<String>,
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
    pub has_open_lots_after_signal: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShadowDropReason {
    Disabled,
    NotFollowed,
    NotSolLeg,
    BelowNotional,
    LagExceeded,
    TooNew,
    LowHolders,
    LowLiquidity,
    LowVolume,
    ThinMarket,
    InvalidSizing,
    DuplicateSignal,
    UnsupportedSide,
}

impl ShadowDropReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::NotFollowed => "not_followed",
            Self::NotSolLeg => "not_sol_leg",
            Self::BelowNotional => "below_notional",
            Self::LagExceeded => "lag_exceeded",
            Self::TooNew => "too_new",
            Self::LowHolders => "low_holders",
            Self::LowLiquidity => "low_liquidity",
            Self::LowVolume => "low_volume",
            Self::ThinMarket => "thin_market",
            Self::InvalidSizing => "invalid_sizing",
            Self::DuplicateSignal => "duplicate_signal",
            Self::UnsupportedSide => "unsupported_side",
        }
    }
}

#[derive(Debug, Clone)]
pub enum ShadowProcessOutcome {
    Recorded(ShadowSignalResult),
    Dropped(ShadowDropReason),
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ShadowSnapshot {
    pub closed_trades_24h: u64,
    pub realized_pnl_sol_24h: f64,
    pub open_lots: u64,
}

#[derive(Debug, Clone, Default)]
pub struct FollowSnapshot {
    pub active: HashSet<String>,
    pub promoted_at: HashMap<String, DateTime<Utc>>,
    pub demoted_at: HashMap<String, DateTime<Utc>>,
}

impl FollowSnapshot {
    pub fn from_active_wallets(active: HashSet<String>) -> Self {
        Self {
            active,
            promoted_at: HashMap::new(),
            demoted_at: HashMap::new(),
        }
    }

    pub fn is_active(&self, wallet_id: &str) -> bool {
        self.active.contains(wallet_id)
    }

    pub fn is_followed_at(&self, wallet_id: &str, ts: DateTime<Utc>) -> bool {
        let promoted_at = self.promoted_at.get(wallet_id).cloned();
        let demoted_at = self.demoted_at.get(wallet_id).cloned();
        match (promoted_at, demoted_at) {
            // Promotion happened after the latest demotion: active from promotion onward.
            (Some(promoted), Some(demoted)) if promoted >= demoted => ts >= promoted,
            // Demotion happened after the latest promotion: active only in [promotion, demotion).
            (Some(promoted), Some(demoted)) => ts >= promoted && ts < demoted,
            (Some(promoted), None) => ts >= promoted,
            (None, Some(demoted)) => ts < demoted,
            (None, None) => self.is_active(wallet_id),
        }
    }
}

impl ShadowService {
    pub fn new(config: ShadowConfig) -> Self {
        Self {
            copy_notional_lamports: sol_to_lamports_floor(config.copy_notional_sol),
            min_leader_notional_lamports: sol_to_lamports_ceil(config.min_leader_notional_sol),
            config,
            helius_http_url: None,
        }
    }

    pub fn new_with_helius(config: ShadowConfig, helius_http_url: Option<String>) -> Self {
        let helius_http_url = helius_http_url
            .map(|url| url.trim().to_string())
            .filter(|url| !url.is_empty() && !url.contains("REPLACE_ME"));
        Self {
            copy_notional_lamports: sol_to_lamports_floor(config.copy_notional_sol),
            min_leader_notional_lamports: sol_to_lamports_ceil(config.min_leader_notional_sol),
            config,
            helius_http_url,
        }
    }

    pub fn process_swap(
        &self,
        store: &SqliteStore,
        swap: &SwapEvent,
        follow_snapshot: &FollowSnapshot,
        now: DateTime<Utc>,
    ) -> Result<ShadowProcessOutcome> {
        if !self.config.enabled {
            return Ok(ShadowProcessOutcome::Dropped(ShadowDropReason::Disabled));
        }
        let Some(candidate) = to_shadow_candidate(swap) else {
            return Ok(ShadowProcessOutcome::Dropped(ShadowDropReason::NotSolLeg));
        };
        let latency_ms = (now - swap.ts_utc).num_milliseconds();
        let runtime_followed = follow_snapshot.is_active(&swap.wallet);
        let temporal_followed = follow_snapshot.is_followed_at(&swap.wallet, swap.ts_utc);
        if runtime_followed && !temporal_followed {
            info!(
                wallet = %swap.wallet,
                token = %candidate.token,
                side = %candidate.side,
                leader_notional_sol = candidate.leader_notional_sol,
                latency_ms,
                "shadow runtime_follow_stale_temporal_miss"
            );
        }
        if !runtime_followed && temporal_followed {
            info!(
                wallet = %swap.wallet,
                token = %candidate.token,
                side = %candidate.side,
                leader_notional_sol = candidate.leader_notional_sol,
                latency_ms,
                "shadow runtime_not_followed_temporal_hit"
            );
        }
        let is_followed = temporal_followed;
        let is_unfollowed_sell_exit = !is_followed
            && candidate.side == "sell"
            && store.has_shadow_lots(&swap.wallet, &candidate.token)?;
        if is_followed {
            info!(
                wallet = %swap.wallet,
                token = %candidate.token,
                side = %candidate.side,
                leader_notional_sol = candidate.leader_notional_sol,
                latency_ms,
                runtime_followed,
                temporal_followed,
                "shadow followed wallet swap reached pipeline"
            );
        } else if is_unfollowed_sell_exit {
            info!(
                wallet = %swap.wallet,
                token = %candidate.token,
                side = %candidate.side,
                leader_notional_sol = candidate.leader_notional_sol,
                latency_ms,
                "shadow unfollowed sell exit allowed"
            );
        }
        if !is_followed && !is_unfollowed_sell_exit {
            return Ok(ShadowProcessOutcome::Dropped(ShadowDropReason::NotFollowed));
        }
        let below_notional =
            if let (Some(exact_leader_notional_lamports), Some(min_notional_lamports)) = (
                candidate.exact_leader_notional_lamports,
                self.min_leader_notional_lamports,
            ) {
                exact_leader_notional_lamports < min_notional_lamports
            } else {
                candidate.leader_notional_sol < self.config.min_leader_notional_sol
            };
        if !is_unfollowed_sell_exit && below_notional {
            log_gate_drop(
                "notional",
                ShadowDropReason::BelowNotional,
                swap,
                &candidate,
                latency_ms,
                runtime_followed,
                temporal_followed,
                is_unfollowed_sell_exit,
            );
            return Ok(ShadowProcessOutcome::Dropped(
                ShadowDropReason::BelowNotional,
            ));
        }

        if !is_unfollowed_sell_exit
            && latency_ms > (self.config.max_signal_lag_seconds as i64 * 1_000)
        {
            log_gate_drop(
                "lag",
                ShadowDropReason::LagExceeded,
                swap,
                &candidate,
                latency_ms,
                runtime_followed,
                temporal_followed,
                is_unfollowed_sell_exit,
            );
            return Ok(ShadowProcessOutcome::Dropped(ShadowDropReason::LagExceeded));
        }
        if candidate.side == "buy" {
            if let Some(reason) =
                self.drop_reason_for_buy_quality_gate(store, &candidate.token, swap.ts_utc, now)?
            {
                log_gate_drop(
                    "quality",
                    reason,
                    swap,
                    &candidate,
                    latency_ms,
                    runtime_followed,
                    temporal_followed,
                    is_unfollowed_sell_exit,
                );
                return Ok(ShadowProcessOutcome::Dropped(reason));
            }
        }

        let copy_notional_lamports = match (
            self.copy_notional_lamports,
            candidate.exact_leader_notional_lamports,
        ) {
            (Some(config_copy_notional_lamports), Some(exact_leader_notional_lamports)) => {
                Some(std::cmp::min(
                    config_copy_notional_lamports,
                    exact_leader_notional_lamports,
                ))
            }
            _ => None,
        };
        let copy_notional_sol = copy_notional_lamports
            .map(lamports_to_sol)
            .unwrap_or_else(|| {
                self.config
                    .copy_notional_sol
                    .min(candidate.leader_notional_sol)
            });
        if copy_notional_sol <= EPS || candidate.price_sol_per_token <= EPS {
            log_gate_drop(
                "sizing",
                ShadowDropReason::InvalidSizing,
                swap,
                &candidate,
                latency_ms,
                runtime_followed,
                temporal_followed,
                is_unfollowed_sell_exit,
            );
            return Ok(ShadowProcessOutcome::Dropped(
                ShadowDropReason::InvalidSizing,
            ));
        }
        let exact_qty = match scaled_exact_shadow_qty(
            candidate.exact_token_qty,
            candidate.exact_leader_notional_lamports,
            copy_notional_lamports,
            copy_notional_sol,
        ) {
            ScaledExactShadowQty::Exact(value) => Some(value),
            ScaledExactShadowQty::Approximate => None,
            ScaledExactShadowQty::InvalidZeroRaw => {
                log_gate_drop(
                    "sizing",
                    ShadowDropReason::InvalidSizing,
                    swap,
                    &candidate,
                    latency_ms,
                    runtime_followed,
                    temporal_followed,
                    is_unfollowed_sell_exit,
                );
                return Ok(ShadowProcessOutcome::Dropped(
                    ShadowDropReason::InvalidSizing,
                ));
            }
        };
        let signal_id = format!(
            "shadow:{}:{}:{}:{}",
            swap.signature, swap.wallet, candidate.side, candidate.token
        );
        let (signal_notional_lamports, signal_notional_origin) =
            if let Some(exact_notional) = copy_notional_lamports {
                (
                    Some(exact_notional),
                    COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
                )
            } else {
                (
                    sol_to_lamports_ceil(copy_notional_sol),
                    COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
                )
            };
        let signal = CopySignalRow {
            signal_id: signal_id.clone(),
            wallet_id: swap.wallet.clone(),
            side: candidate.side.clone(),
            token: candidate.token.clone(),
            notional_sol: copy_notional_sol,
            notional_lamports: signal_notional_lamports,
            notional_origin: signal_notional_origin,
            ts: swap.ts_utc,
            status: "shadow_recorded".to_string(),
        };
        if !store.insert_copy_signal(&signal)? {
            log_gate_drop(
                "dedupe",
                ShadowDropReason::DuplicateSignal,
                swap,
                &candidate,
                latency_ms,
                runtime_followed,
                temporal_followed,
                is_unfollowed_sell_exit,
            );
            return Ok(ShadowProcessOutcome::Dropped(
                ShadowDropReason::DuplicateSignal,
            ));
        }

        let (close, has_open_lots_after_signal) = match candidate.side.as_str() {
            "buy" => {
                let qty = copy_notional_sol / candidate.price_sol_per_token;
                if qty > EPS {
                    let _ = store.insert_shadow_lot_exact(
                        &swap.wallet,
                        &candidate.token,
                        qty,
                        exact_qty,
                        copy_notional_sol,
                        swap.ts_utc,
                    )?;
                }
                (copybot_storage::ShadowCloseOutcome::default(), Some(true))
            }
            "sell" => {
                let qty = copy_notional_sol / candidate.price_sol_per_token;
                let close = store.close_shadow_lots_fifo_atomic_exact(
                    &signal_id,
                    &swap.wallet,
                    &candidate.token,
                    qty,
                    exact_qty,
                    candidate.price_sol_per_token,
                    swap.ts_utc,
                )?;
                (close, Some(close.has_open_lots_after))
            }
            _ => {
                log_gate_drop(
                    "side",
                    ShadowDropReason::UnsupportedSide,
                    swap,
                    &candidate,
                    latency_ms,
                    runtime_followed,
                    temporal_followed,
                    is_unfollowed_sell_exit,
                );
                return Ok(ShadowProcessOutcome::Dropped(
                    ShadowDropReason::UnsupportedSide,
                ));
            }
        };

        Ok(ShadowProcessOutcome::Recorded(ShadowSignalResult {
            signal_id,
            wallet_id: swap.wallet.clone(),
            side: candidate.side,
            token: candidate.token,
            notional_sol: copy_notional_sol,
            latency_ms,
            closed_qty: close.closed_qty,
            realized_pnl_sol: close.realized_pnl_sol,
            has_open_lots_after_signal,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use chrono::Duration;
    use copybot_core_types::SwapEvent;
    use copybot_core_types::{ExactSwapAmounts, TokenQuantity};
    use copybot_storage::SqliteStore;
    use std::path::Path;
    use tempfile::tempdir;

    fn follow_snapshot(active_wallets: &[&str]) -> FollowSnapshot {
        FollowSnapshot::from_active_wallets(
            active_wallets
                .iter()
                .map(|wallet| wallet.to_string())
                .collect(),
        )
    }

    #[test]
    fn creates_shadow_signal_and_realized_pnl() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-test.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let follow = follow_snapshot(&["leader-wallet"]);

        let mut cfg = ShadowConfig::default();
        cfg.copy_notional_sol = 0.5;
        cfg.min_leader_notional_sol = 0.25;
        cfg.quality_gates_enabled = false;
        let service = ShadowService::new(cfg);

        let buy_ts = DateTime::parse_from_rfc3339("2026-02-12T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let sell_ts = DateTime::parse_from_rfc3339("2026-02-12T12:05:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        store.activate_follow_wallet(
            "leader-wallet",
            buy_ts - Duration::seconds(30),
            "test-seed-follow",
        )?;

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
            exact_amounts: None,
        };
        let buy_signal = service
            .process_swap(&store, &buy, &follow, buy_ts + Duration::seconds(1))?
            .expect_recorded("buy signal expected");
        assert_eq!(buy_signal.side, "buy");
        let signals = store.list_copy_signals_by_status("shadow_recorded", 10)?;
        assert_eq!(signals.len(), 1);
        assert_eq!(
            signals[0].notional_lamports,
            Some(Lamports::new(500_000_000))
        );
        assert_eq!(
            signals[0].notional_origin,
            COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE
        );
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
            exact_amounts: None,
        };
        let sell_signal = service
            .process_swap(&store, &sell, &follow, sell_ts + Duration::seconds(1))?
            .expect_recorded("sell signal expected");
        assert_eq!(sell_signal.side, "sell");
        assert!(sell_signal.realized_pnl_sol > 0.0);

        let snapshot = service.snapshot_24h(&store, sell_ts + Duration::seconds(2))?;
        assert!(snapshot.closed_trades_24h >= 1);
        assert!(snapshot.realized_pnl_sol_24h > 0.0);
        Ok(())
    }

    #[test]
    fn sell_closes_existing_lot_even_if_wallet_demoted() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-unfollowed-exit.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut follow = follow_snapshot(&["leader-wallet"]);

        let mut cfg = ShadowConfig::default();
        cfg.copy_notional_sol = 0.5;
        cfg.min_leader_notional_sol = 0.25;
        cfg.quality_gates_enabled = false;
        let service = ShadowService::new(cfg);

        let buy_ts = DateTime::parse_from_rfc3339("2026-02-12T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let sell_ts = DateTime::parse_from_rfc3339("2026-02-12T12:05:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        store.activate_follow_wallet(
            "leader-wallet",
            buy_ts - Duration::seconds(30),
            "test-seed-follow",
        )?;

        let buy = SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "TokenMint".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-buy-demote".to_string(),
            slot: 10,
            ts_utc: buy_ts,
            exact_amounts: None,
        };
        service
            .process_swap(&store, &buy, &follow, buy_ts + Duration::seconds(1))?
            .expect_recorded("buy signal expected");
        assert_eq!(store.shadow_open_lots_count()?, 1);

        // Simulate a discovery demotion: wallet is no longer in active followlist.
        follow.active.clear();
        follow
            .demoted_at
            .insert("leader-wallet".to_string(), sell_ts - Duration::seconds(30));
        store.deactivate_follow_wallet(
            "leader-wallet",
            sell_ts - Duration::seconds(30),
            "test-demote",
        )?;

        let sell = SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "TokenMint".to_string(),
            token_out: SOL_MINT.to_string(),
            amount_in: 1000.0,
            amount_out: 1.0,
            signature: "sig-sell-demote".to_string(),
            slot: 11,
            ts_utc: sell_ts,
            exact_amounts: None,
        };
        let sell_signal = service
            .process_swap(&store, &sell, &follow, sell_ts + Duration::seconds(1))?
            .expect_recorded("sell signal should close orphaned lot");
        assert_eq!(sell_signal.side, "sell");
        assert_eq!(store.shadow_open_lots_count()?, 0);

        let snapshot = service.snapshot_24h(&store, sell_ts + Duration::seconds(2))?;
        assert!(snapshot.closed_trades_24h >= 1);
        Ok(())
    }

    #[test]
    fn sell_does_not_treat_dust_lot_as_unfollowed_exit() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-unfollowed-dust-exit.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut follow = follow_snapshot(&["leader-wallet"]);

        let mut cfg = ShadowConfig::default();
        cfg.copy_notional_sol = 0.5;
        cfg.min_leader_notional_sol = 0.25;
        cfg.quality_gates_enabled = false;
        let service = ShadowService::new(cfg);

        let buy_ts = DateTime::parse_from_rfc3339("2026-02-12T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let sell_ts = DateTime::parse_from_rfc3339("2026-02-12T12:05:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        store.activate_follow_wallet(
            "leader-wallet",
            buy_ts - Duration::seconds(30),
            "test-seed-follow",
        )?;

        let buy = SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "TokenMint".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-buy-dust-demote".to_string(),
            slot: 20,
            ts_utc: buy_ts,
            exact_amounts: None,
        };
        service
            .process_swap(&store, &buy, &follow, buy_ts + Duration::seconds(1))?
            .expect_recorded("buy signal expected");
        let signals = store.list_copy_signals_by_status("shadow_recorded", 10)?;
        assert_eq!(signals.len(), 1);
        assert_eq!(
            signals[0].notional_lamports,
            Some(Lamports::new(500_000_000))
        );
        assert_eq!(
            signals[0].notional_origin,
            COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE
        );

        let lots = store.list_shadow_lots("leader-wallet", "TokenMint")?;
        assert_eq!(lots.len(), 1, "expected single open lot before dusting");
        store.update_shadow_lot(lots[0].id, 1e-13, 1e-15)?;
        assert!(
            !store.has_shadow_lots("leader-wallet", "TokenMint")?,
            "dust lot should not count as open inventory"
        );
        let dust_snapshot = service.snapshot_24h(&store, sell_ts - Duration::seconds(1))?;
        assert_eq!(
            dust_snapshot.open_lots, 0,
            "dust lot should not appear in shadow snapshot open lots"
        );

        follow.active.clear();
        follow
            .demoted_at
            .insert("leader-wallet".to_string(), sell_ts - Duration::seconds(30));
        store.deactivate_follow_wallet(
            "leader-wallet",
            sell_ts - Duration::seconds(30),
            "test-demote",
        )?;

        let sell = SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "TokenMint".to_string(),
            token_out: SOL_MINT.to_string(),
            amount_in: 1000.0,
            amount_out: 1.0,
            signature: "sig-sell-dust-demote".to_string(),
            slot: 21,
            ts_utc: sell_ts,
            exact_amounts: None,
        };
        let outcome =
            service.process_swap(&store, &sell, &follow, sell_ts + Duration::seconds(1))?;
        outcome.expect_dropped(
            ShadowDropReason::NotFollowed,
            "dust lot should not unlock unfollowed sell exit",
        );
        Ok(())
    }

    #[test]
    fn buy_uses_temporal_follow_membership_when_runtime_set_is_stale() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-temporal-follow.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut follow = follow_snapshot(&[]);

        let mut cfg = ShadowConfig::default();
        cfg.copy_notional_sol = 0.5;
        cfg.min_leader_notional_sol = 0.25;
        cfg.quality_gates_enabled = false;
        let service = ShadowService::new(cfg);

        let buy_ts = DateTime::parse_from_rfc3339("2026-02-12T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        follow
            .promoted_at
            .insert("leader-wallet".to_string(), buy_ts - Duration::seconds(30));

        let buy = SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "TokenMint".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-buy-temporal-follow".to_string(),
            slot: 22,
            ts_utc: buy_ts,
            exact_amounts: None,
        };

        let outcome = service.process_swap(&store, &buy, &follow, buy_ts + Duration::seconds(1))?;
        outcome.expect_recorded("buy should pass with temporal follow membership");
        Ok(())
    }

    #[test]
    fn process_swap_preserves_exact_shadow_quantities() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-exact-qty.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let follow = follow_snapshot(&["leader-wallet"]);

        let mut cfg = ShadowConfig::default();
        cfg.copy_notional_sol = 0.5;
        cfg.min_leader_notional_sol = 0.25;
        cfg.quality_gates_enabled = false;
        let service = ShadowService::new(cfg);

        let buy_ts = DateTime::parse_from_rfc3339("2026-02-12T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let sell_ts = DateTime::parse_from_rfc3339("2026-02-12T12:05:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        store.activate_follow_wallet(
            "leader-wallet",
            buy_ts - Duration::seconds(30),
            "test-seed-follow",
        )?;

        let buy = SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "TokenMint".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-buy-exact".to_string(),
            slot: 90,
            ts_utc: buy_ts,
            exact_amounts: Some(ExactSwapAmounts {
                amount_in_raw: "1000000000".to_string(),
                amount_in_decimals: 9,
                amount_out_raw: "1000000000".to_string(),
                amount_out_decimals: 6,
            }),
        };
        service
            .process_swap(&store, &buy, &follow, buy_ts + Duration::seconds(1))?
            .expect_recorded("buy signal expected");

        let lots = store.list_shadow_lots("leader-wallet", "TokenMint")?;
        assert_eq!(lots.len(), 1);
        assert_eq!(lots[0].accounting_bucket, "exact_post_cutover");
        assert_eq!(lots[0].qty_exact, Some(TokenQuantity::new(500_000_000, 6)));

        let sell = SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "TokenMint".to_string(),
            token_out: SOL_MINT.to_string(),
            amount_in: 1000.0,
            amount_out: 1.2,
            signature: "sig-sell-exact".to_string(),
            slot: 91,
            ts_utc: sell_ts,
            exact_amounts: Some(ExactSwapAmounts {
                amount_in_raw: "500000000".to_string(),
                amount_in_decimals: 6,
                amount_out_raw: "600000000".to_string(),
                amount_out_decimals: 9,
            }),
        };
        service
            .process_swap(&store, &sell, &follow, sell_ts + Duration::seconds(1))?
            .expect_recorded("sell signal expected");

        let lots = store.list_shadow_lots("leader-wallet", "TokenMint")?;
        assert_eq!(lots.len(), 1);
        assert_eq!(lots[0].accounting_bucket, "exact_post_cutover");
        assert_eq!(lots[0].qty_exact, Some(TokenQuantity::new(83_333_334, 6)));

        let closed_bucket = store.shadow_closed_trade_accounting_bucket(
            "shadow:sig-sell-exact:leader-wallet:sell:TokenMint",
        )?;
        assert_eq!(closed_bucket.as_deref(), Some("exact_post_cutover"));
        let closed_qty_exact = store
            .shadow_closed_trade_qty_exact("shadow:sig-sell-exact:leader-wallet:sell:TokenMint")?;
        assert_eq!(closed_qty_exact, Some(TokenQuantity::new(416_666_666, 6)));
        Ok(())
    }

    #[test]
    fn drops_buy_when_exact_shadow_qty_truncates_to_zero_raw() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-buy-zero-raw-exact.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let follow = follow_snapshot(&["leader-wallet"]);

        let mut cfg = ShadowConfig::default();
        cfg.copy_notional_sol = 0.5;
        cfg.min_leader_notional_sol = 0.25;
        cfg.quality_gates_enabled = false;
        let service = ShadowService::new(cfg);

        let buy_ts = DateTime::parse_from_rfc3339("2026-03-10T09:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        store.activate_follow_wallet(
            "leader-wallet",
            buy_ts - Duration::seconds(30),
            "test-seed-follow",
        )?;

        let buy = SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "TokenMint".to_string(),
            amount_in: 1.0,
            amount_out: 0.000001,
            signature: "sig-buy-zero-raw-exact".to_string(),
            slot: 190,
            ts_utc: buy_ts,
            exact_amounts: Some(ExactSwapAmounts {
                amount_in_raw: "1000000000".to_string(),
                amount_in_decimals: 9,
                amount_out_raw: "1".to_string(),
                amount_out_decimals: 6,
            }),
        };

        let outcome = service.process_swap(&store, &buy, &follow, buy_ts + Duration::seconds(1))?;
        outcome.expect_dropped(
            ShadowDropReason::InvalidSizing,
            "zero-raw exact buy should fail closed",
        );
        assert!(
            store
                .list_shadow_lots("leader-wallet", "TokenMint")?
                .is_empty(),
            "zero-raw exact buy must not persist a shadow lot"
        );
        assert!(
            store
                .list_copy_signals_by_status("shadow_recorded", 10)?
                .is_empty(),
            "zero-raw exact buy must not persist a copy signal"
        );
        Ok(())
    }

    #[test]
    fn drops_buy_when_leader_exact_qty_zero_raw_source_side() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-buy-leader-zero-raw.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let follow = follow_snapshot(&["leader-wallet"]);

        let mut cfg = ShadowConfig::default();
        cfg.copy_notional_sol = 0.5;
        cfg.min_leader_notional_sol = 0.25;
        cfg.quality_gates_enabled = false;
        let service = ShadowService::new(cfg);

        let buy_ts = DateTime::parse_from_rfc3339("2026-03-10T09:10:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        store.activate_follow_wallet(
            "leader-wallet",
            buy_ts - Duration::seconds(30),
            "test-seed-follow",
        )?;

        let buy = SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "TokenMint".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-buy-leader-zero-raw".to_string(),
            slot: 190,
            ts_utc: buy_ts,
            exact_amounts: Some(ExactSwapAmounts {
                amount_in_raw: "1000000000".to_string(),
                amount_in_decimals: 9,
                amount_out_raw: "0".to_string(),
                amount_out_decimals: 6,
            }),
        };

        let outcome = service.process_swap(&store, &buy, &follow, buy_ts + Duration::seconds(1))?;
        outcome.expect_dropped(
            ShadowDropReason::InvalidSizing,
            "leader zero-raw exact buy should fail closed",
        );
        assert!(
            store
                .list_shadow_lots("leader-wallet", "TokenMint")?
                .is_empty(),
            "leader zero-raw exact buy must not persist a shadow lot"
        );
        assert!(
            store
                .list_copy_signals_by_status("shadow_recorded", 10)?
                .is_empty(),
            "leader zero-raw exact buy must not persist a copy signal"
        );
        Ok(())
    }

    #[test]
    fn drops_sell_when_exact_shadow_qty_truncates_to_zero_raw() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-sell-zero-raw-exact.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let follow = follow_snapshot(&["leader-wallet"]);

        let mut cfg = ShadowConfig::default();
        cfg.copy_notional_sol = 0.5;
        cfg.min_leader_notional_sol = 0.25;
        cfg.quality_gates_enabled = false;
        let service = ShadowService::new(cfg);

        let opened_ts = DateTime::parse_from_rfc3339("2026-03-10T09:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let sell_ts = opened_ts + Duration::minutes(5);
        store.activate_follow_wallet(
            "leader-wallet",
            opened_ts - Duration::seconds(30),
            "test-seed-follow",
        )?;
        store.insert_shadow_lot_exact(
            "leader-wallet",
            "TokenMint",
            1.0,
            Some(TokenQuantity::new(1_000_000, 6)),
            0.50,
            opened_ts,
        )?;

        let sell = SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: "TokenMint".to_string(),
            token_out: SOL_MINT.to_string(),
            amount_in: 0.000001,
            amount_out: 1.2,
            signature: "sig-sell-zero-raw-exact".to_string(),
            slot: 191,
            ts_utc: sell_ts,
            exact_amounts: Some(ExactSwapAmounts {
                amount_in_raw: "1".to_string(),
                amount_in_decimals: 6,
                amount_out_raw: "1200000000".to_string(),
                amount_out_decimals: 9,
            }),
        };

        let outcome =
            service.process_swap(&store, &sell, &follow, sell_ts + Duration::seconds(1))?;
        outcome.expect_dropped(
            ShadowDropReason::InvalidSizing,
            "zero-raw exact sell should fail closed",
        );

        let lots = store.list_shadow_lots("leader-wallet", "TokenMint")?;
        assert_eq!(lots.len(), 1);
        assert_eq!(lots[0].qty_exact, Some(TokenQuantity::new(1_000_000, 6)));
        assert!(
            store
                .shadow_closed_trade_qty_exact(
                    "shadow:sig-sell-zero-raw-exact:leader-wallet:sell:TokenMint"
                )?
                .is_none(),
            "zero-raw exact sell must not persist a closed trade"
        );
        assert!(
            store
                .list_copy_signals_by_status("shadow_recorded", 10)?
                .is_empty(),
            "zero-raw exact sell must not persist a copy signal"
        );
        Ok(())
    }

    #[test]
    fn drops_buy_when_runtime_follow_set_is_stale_after_demotion() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-runtime-stale-demotion.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let mut follow = follow_snapshot(&["leader-wallet"]);

        let mut cfg = ShadowConfig::default();
        cfg.copy_notional_sol = 0.5;
        cfg.min_leader_notional_sol = 0.25;
        cfg.quality_gates_enabled = false;
        let service = ShadowService::new(cfg);

        let demoted_at = DateTime::parse_from_rfc3339("2026-02-12T11:59:30Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let buy_ts = DateTime::parse_from_rfc3339("2026-02-12T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        follow
            .demoted_at
            .insert("leader-wallet".to_string(), demoted_at);

        let buy = SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "TokenMint".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-buy-runtime-stale".to_string(),
            slot: 55,
            ts_utc: buy_ts,
            exact_amounts: None,
        };

        let outcome = service.process_swap(&store, &buy, &follow, buy_ts + Duration::seconds(1))?;
        outcome.expect_dropped(
            ShadowDropReason::NotFollowed,
            "buy should be dropped when runtime follow set is stale after demotion",
        );
        Ok(())
    }

    #[test]
    fn drops_buy_when_token_is_too_new() -> Result<()> {
        let temp = tempdir().context("failed to create tempdir")?;
        let db_path = temp.path().join("shadow-quality-too-new.db");
        let mut store = SqliteStore::open(Path::new(&db_path))?;
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        store.run_migrations(&migration_dir)?;

        let follow = follow_snapshot(&["leader-wallet"]);

        let mut cfg = ShadowConfig::default();
        cfg.copy_notional_sol = 0.5;
        cfg.min_leader_notional_sol = 0.25;
        cfg.min_token_age_seconds = 600;
        let service = ShadowService::new(cfg);

        let buy_ts = DateTime::parse_from_rfc3339("2026-02-12T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        store.activate_follow_wallet(
            "leader-wallet",
            buy_ts - Duration::seconds(30),
            "test-seed-follow",
        )?;
        let buy = SwapEvent {
            wallet: "leader-wallet".to_string(),
            dex: "pumpswap".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "TokenMint".to_string(),
            amount_in: 1.0,
            amount_out: 1000.0,
            signature: "sig-buy-too-new".to_string(),
            slot: 101,
            ts_utc: buy_ts,
            exact_amounts: None,
        };

        let outcome = service.process_swap(&store, &buy, &follow, buy_ts + Duration::seconds(1))?;
        outcome.expect_dropped(ShadowDropReason::TooNew, "buy should be dropped by age");
        Ok(())
    }

    trait TestOutcomeExt {
        fn expect_recorded(self, message: &str) -> ShadowSignalResult;
        fn expect_dropped(self, expected: ShadowDropReason, message: &str);
    }

    impl TestOutcomeExt for ShadowProcessOutcome {
        fn expect_recorded(self, message: &str) -> ShadowSignalResult {
            match self {
                ShadowProcessOutcome::Recorded(result) => result,
                ShadowProcessOutcome::Dropped(reason) => {
                    panic!("{message}: dropped with reason {}", reason.as_str())
                }
            }
        }

        fn expect_dropped(self, expected: ShadowDropReason, message: &str) {
            match self {
                ShadowProcessOutcome::Dropped(reason) => {
                    assert_eq!(
                        reason,
                        expected,
                        "{message}: expected {}, got {}",
                        expected.as_str(),
                        reason.as_str()
                    );
                }
                ShadowProcessOutcome::Recorded(_) => {
                    panic!("{message}: expected dropped, got recorded")
                }
            }
        }
    }
}
