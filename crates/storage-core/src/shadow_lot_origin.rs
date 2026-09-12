//! Immutable source BUY facts. Shadow allocation is deliberately not a receipt amount.
use crate::{shadow_lots, SqliteStore, SHADOW_RISK_CONTEXT_MARKET};
use anyhow::{ensure, Context, Result};
use copybot_core_types::{
    association_delivery::CheckedFacts, ExactSwapAmounts, SwapEvent, TokenQuantity,
};
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
#[path = "shadow_lot_origin_schema.rs"]
pub mod schema;
const SOL: &str = "So11111111111111111111111111111111111111112";
pub(crate) const MAX_ORIGIN_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShadowLotOrigin {
    pub version: u8,
    pub signal_id: String,
    pub signature: String,
    pub slot: u64,
    pub wallet: String,
    pub token_in: String,
    pub token_out: String,
    pub amount_in_bits: u64,
    pub amount_out_bits: u64,
    pub exact_amounts: Option<ExactSwapAmounts>,
    pub dex: String,
}
impl ShadowLotOrigin {
    fn from_swap(s: &SwapEvent, signal_id: &str) -> Result<Self> {
        let o = Self {
            version: 1,
            signal_id: signal_id.into(),
            signature: s.signature.clone(),
            slot: s.slot,
            wallet: s.wallet.clone(),
            token_in: s.token_in.clone(),
            token_out: s.token_out.clone(),
            amount_in_bits: s.amount_in.to_bits(),
            amount_out_bits: s.amount_out.to_bits(),
            exact_amounts: s.exact_amounts.clone(),
            dex: s.dex.clone(),
        };
        o.validate()?;
        Ok(o)
    }
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.version == 1 && self.token_in == SOL && self.token_out != SOL,
            "origin is not a source BUY"
        );
        ensure!(
            [&self.signature, &self.wallet, &self.token_out]
                .iter()
                .all(|s| !s.is_empty() && !s.contains(':') && !s.chars().any(char::is_whitespace)),
            "invalid source BUY identity"
        );
        ensure!(
            self.signal_id
                == format!(
                    "shadow:{}:{}:buy:{}",
                    self.signature, self.wallet, self.token_out
                ),
            "substituted source BUY signal"
        );
        ensure!(
            [self.amount_in_bits, self.amount_out_bits]
                .iter()
                .all(|b| f64::from_bits(*b).is_finite() && f64::from_bits(*b) > 0.0),
            "invalid source BUY amounts"
        );
        if let Some(e) = &self.exact_amounts {
            ensure!(
                e.amount_in_decimals == 9
                    && e.amount_out_decimals <= 38
                    && [&e.amount_in_raw, &e.amount_out_raw].iter().all(|s| s
                        .parse::<u128>()
                        .is_ok_and(|v| v > 0 && v.to_string() == **s)),
                "invalid source BUY exact amounts"
            );
        }
        ensure!(
            serde_json::to_vec(self)?.len() <= MAX_ORIGIN_BYTES,
            "source BUY origin too large"
        );
        Ok(())
    }
    pub(crate) fn matches(&self, f: &CheckedFacts) -> bool {
        self.signature == f.signature
            && self.slot == f.slot
            && self.wallet == f.wallet
            && self.token_in == f.token_in
            && self.token_out == f.token_out
            && self.amount_in_bits == f.amount_in_bits
            && self.amount_out_bits == f.amount_out_bits
            && self.exact_amounts == f.exact_amounts
            && self.dex == f.dex
    }
}
impl SqliteStore {
    /// The signal has already committed in the producer. Only lot+origin are atomic.
    /// Errors return no lot id; signal dedupe still prevents replay credit after failure.
    pub fn insert_shadow_buy_lot(
        &self,
        swap: &SwapEvent,
        signal_id: &str,
        qty: f64,
        qty_exact: Option<TokenQuantity>,
        cost_sol: f64,
    ) -> Result<i64> {
        let origin = ShadowLotOrigin::from_swap(swap, signal_id)?;
        let qty_exact = shadow_lots::reject_zero_raw_exact_qty(qty_exact, "insert shadow BUY lot")?;
        let wire = serde_json::to_string(&origin)?;
        self.with_immediate_transaction_retry("shadow lot and origin", |c| {
            schema::required(c)?;
            let signal: Option<(String,String,String,String,f64)> = c.query_row(
                "SELECT wallet_id,token,side,ts,notional_sol FROM copy_signals WHERE signal_id=?1",
                [signal_id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
            ensure!(signal == Some((swap.wallet.clone(), swap.token_out.clone(), "buy".into(),
                swap.ts_utc.to_rfc3339(), cost_sol)), "source BUY signal facts changed or missing");
            if let Some(a) = crate::association_inbox::identity(c, &origin.signature)? {
                ensure!(origin.matches(&a.admission.facts), "source BUY anchor facts substituted");
            }
            let id = shadow_lots::insert_on_conn(c, &swap.wallet, &swap.token_out, qty,
                qty_exact, cost_sol, SHADOW_RISK_CONTEXT_MARKET, swap.ts_utc)?;
            ensure!(c.changes() == 1, "shadow lot insert ignored");
            ensure!(c.execute("INSERT INTO shadow_lot_origins(lot_id,signal_id,origin) VALUES(?1,?2,?3)",
                params![id,signal_id,wire])? == 1, "shadow origin insert ignored");
            let actual: Option<(String,String)> = c.query_row(
                "SELECT signal_id,origin FROM shadow_lot_origins WHERE lot_id=?1", [id],
                |r| Ok((r.get(0)?,r.get(1)?))).optional()?;
            ensure!(actual == Some((signal_id.into(),wire.clone())), "shadow origin insert changed");
            let lot = c.query_row("SELECT wallet_id,token,qty,qty_raw,qty_decimals,cost_sol,opened_ts FROM shadow_lots WHERE id=?1",[id],
                |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,f64>(2)?,r.get::<_,Option<String>>(3)?,r.get::<_,Option<u8>>(4)?,r.get::<_,f64>(5)?,r.get::<_,String>(6)?)))?;
            ensure!(lot == (swap.wallet.clone(),swap.token_out.clone(),qty,qty_exact.map(|q|q.raw().to_string()),
                qty_exact.map(|q|q.decimals()),cost_sol,swap.ts_utc.to_rfc3339()), "shadow lot insert changed");
            Ok(id)
        })
    }
    /// Exact lot-id lookup only. Legacy lots have no origin; no historical backfill.
    pub fn shadow_lot_origin(&self, lot_id: i64) -> Result<Option<ShadowLotOrigin>> {
        schema::required(&self.conn)?;
        let wire: Option<String> = self
            .conn
            .query_row(
                "SELECT origin FROM shadow_lot_origins WHERE lot_id=?1",
                [lot_id],
                |r| r.get(0),
            )
            .optional()?;
        wire.map(|s| {
            ensure!(s.len() <= MAX_ORIGIN_BYTES, "source BUY origin too large");
            let o: ShadowLotOrigin =
                serde_json::from_str(&s).context("invalid source BUY origin")?;
            o.validate()?;
            Ok(o)
        })
        .transpose()
    }
}
