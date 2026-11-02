//! Explicit normal-network session grant. No future transactions or hidden caps.
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
pub const REAL_SESSION_V1: &str = "real_bounded_usdc_session_v1";
pub const MAINNET_GENESIS: &str = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d";
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RealSession {
    pub policy: String,
    pub session_id: String,
    pub bot_wallet: String,
    pub source_wallet: String,
    pub mint: String,
    pub route: String,
    pub genesis: String,
    pub activated_at: String,
    pub deadline: String,
    pub max_cycles: u32,
    pub max_open_positions: u32,
    pub buy_lamports: u64,
    pub gross_buy_cap: u64,
    pub fee_cap: u64,
    pub loss_cap: u64,
    pub total_fee_cap: u64,
    pub priority_fee_cap: u64,
    pub native_floor: u64,
    pub personal_usdc_raw: u64,
    pub slippage_bps: u64,
}
pub fn validate_real_session(e: &crate::ExecutionConfig) -> Result<()> {
    let Some(p)=&e.real_session else {return Ok(())};
    let t=e.technical_cohort.as_ref().ok_or_else(||anyhow::anyhow!("real_session_cohort"))?;
    let s=e.controlled_receipt_lot.as_ref().ok_or_else(||anyhow::anyhow!("real_session_receipt_scope"))?;
    let rpc=e.owned_sell_preparation.as_ref().ok_or_else(||anyhow::anyhow!("real_session_rpc"))?;
    ensure!(e.local_bounded_session.is_none() && !e.enabled && e.owner_technical_buy.is_none() && e.owner_exit.is_none(),"real_session_exclusive");
    ensure!(p.policy==REAL_SESSION_V1 && p.genesis==MAINNET_GENESIS && rpc.genesis_hash==p.genesis
        && p.mint=="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" && p.route=="jupiter_swap_instructions",
        "real_session_profile");
    ensure!((1..=90).contains(&p.session_id.len()) && p.session_id.bytes().all(|b|b.is_ascii_alphanumeric() || b"-_.:".contains(&b))
        && p.bot_wallet==e.canary_wallet_pubkey && p.bot_wallet==e.execution_signer_pubkey
        && p.source_wallet!=p.bot_wallet && t.wallet_ids==[p.source_wallet.clone()]
        && t.run_id==p.session_id && t.route==p.route && t.activate
        && t.activated_at==p.activated_at && t.deadline==p.deadline
        && e.tiny_experiment.id.as_deref()==Some(&p.session_id)
        && s.bot_wallet==p.bot_wallet && s.source_wallet==p.source_wallet && s.bot_run_id==p.session_id
        && s.source_buy_intent_id==p.session_id,
        "real_session_identity");
    ensure!((1..=32).contains(&p.max_cycles) && t.max_buy_count==p.max_cycles && t.max_source_sell_count==p.max_cycles
        && p.max_open_positions==1 && e.canary_max_open_positions==1
        && p.buy_lamports==10_000_000 && p.gross_buy_cap>0 && p.gross_buy_cap<=u64::from(p.max_cycles)*p.buy_lamports
        && p.fee_cap>0 && p.total_fee_cap==100_000
        && p.priority_fee_cap<=50_000 && p.priority_fee_cap<=p.total_fee_cap
        && e.pretrade_max_priority_fee_lamports==p.priority_fee_cap
        && p.loss_cap>0 && p.loss_cap<=20_000_000 && (e.canary_max_daily_loss_sol*1e9).round()==p.loss_cap as f64
        && p.native_floor==0 && e.pretrade_min_sol_reserve==0.0 && p.personal_usdc_raw==100
        && p.slippage_bps<=50 && e.quote_canary_buy_slippage_bps==p.slippage_bps && e.quote_canary_sell_slippage_bps==p.slippage_bps,
        "real_session_explicit_bounds");
    Ok(())
}
