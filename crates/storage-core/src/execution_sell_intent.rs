use crate::{observed_row::row_to_swap_event, SqliteDiscoveryStore};
use anyhow::{Context, Result};
use copybot_core_types::{
    CopySignalRow, Lamports, SwapEvent, COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE,
    COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS,
};

pub const EXECUTION_SELL_INTENT_STATUS: &str = "execution_sell_intent";
const SOL: &str = "So11111111111111111111111111111111111111112";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionSellIntentReject {
    InvalidSell,
    SourceNotActive,
    SourceTemporalMiss,
    ObservedEventMismatch,
    NoOwnedPosition,
    SellBeforePosition,
    SellBeforeLatestBuy,
    ShadowRiskPresent,
    SignalAlreadyExists,
    SourceSignatureClaimed,
}
impl ExecutionSellIntentReject {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidSell => "owned_sell_invalid_event",
            Self::SourceNotActive => "owned_sell_source_not_active",
            Self::SourceTemporalMiss => "owned_sell_source_temporal_miss",
            Self::ObservedEventMismatch => "owned_sell_observed_event_mismatch",
            Self::NoOwnedPosition => "owned_sell_no_position",
            Self::SellBeforePosition => "owned_sell_before_position",
            Self::SellBeforeLatestBuy => "owned_sell_before_latest_buy",
            Self::ShadowRiskPresent => "owned_sell_shadow_risk_present",
            Self::SignalAlreadyExists => "owned_sell_signal_already_exists",
            Self::SourceSignatureClaimed => "owned_sell_source_signature_claimed",
        }
    }
}
#[derive(Debug)]
pub enum ExecutionSellIntentOutcome {
    Inserted(CopySignalRow),
    Rejected(ExecutionSellIntentReject),
}
impl SqliteDiscoveryStore {
    /// Called after serialized shadow dispatch rejects an entry-only gate.
    /// This records an execution intent, never a shadow fill or close.
    pub fn record_execution_sell_intent(
        &self,
        swap: &SwapEvent,
    ) -> Result<ExecutionSellIntentOutcome> {
        use ExecutionSellIntentReject::*;
        let reject = |reason| Ok(ExecutionSellIntentOutcome::Rejected(reason));
        if swap.token_out != SOL
            || swap.token_in == SOL
            || swap.token_in.trim().is_empty()
            || swap.signature.trim().is_empty()
            || swap.wallet.trim().is_empty()
            || !swap.amount_in.is_finite()
            || !swap.amount_out.is_finite()
            || swap.amount_in <= 1e-12
            || swap.amount_out <= 1e-12
        {
            return reject(InvalidSell);
        }
        let exact_sol = match swap.exact_amounts.as_ref() {
            None => None,
            Some(exact) => {
                let (Ok(token), Ok(sol)) =
                    (exact.amount_in_quantity(), exact.amount_out_quantity())
                else {
                    return reject(InvalidSell);
                };
                if token.raw() == 0 || sol.raw() == 0 || sol.decimals() != 9 {
                    return reject(InvalidSell);
                }
                Some(Lamports::new(sol.raw()))
            }
        };
        self.with_immediate_transaction_retry("record owned sell intent", |conn| {
            // The same write reservation covers the claim check and canonical signal insert.
            if crate::ordered_source_sell::ownership::blocks_legacy(conn, &swap.signature)? {
                return reject(SourceSignatureClaimed);
            }
            let active: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM followlist WHERE wallet_id = ?1 AND active = 1)", [&swap.wallet], |r|r.get(0))?;
            if !active { return reject(SourceNotActive); }
            if !self.was_wallet_followed_at(&swap.wallet, swap.ts_utc)? { return reject(SourceTemporalMiss); }
            let mut stmt = conn.prepare("SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals FROM observed_swaps WHERE signature = ?1")?;
            let mut rows = stmt.query([&swap.signature])?;
            let Some(row) = rows.next()? else { return reject(ObservedEventMismatch); };
            let saved = row_to_swap_event(row)?;
            if saved.wallet != swap.wallet || saved.token_in != swap.token_in || saved.token_out != swap.token_out
                || saved.ts_utc != swap.ts_utc || saved.slot != swap.slot || saved.dex != swap.dex
                || saved.amount_in != swap.amount_in || saved.amount_out != swap.amount_out || saved.exact_amounts != swap.exact_amounts {
                return reject(ObservedEventMismatch);
            }
            let Some(position) = self.load_execution_canary_open_position(&swap.token_in)? else { return reject(NoOwnedPosition); };
            if !position.qty.is_finite() || position.qty <= 1e-12 || position.qty_exact.is_some_and(|q|q.raw()==0) { return reject(NoOwnedPosition); }
            if swap.ts_utc < position.opened_ts { return reject(SellBeforePosition); }
            if self.latest_live_execution_canary_buy_signal_ts(&swap.token_in)?.is_some_and(|ts|swap.ts_utc < ts) { return reject(SellBeforeLatestBuy); }
            if self.has_shadow_lots_at(&swap.wallet, &swap.token_in, swap.ts_utc)? { return reject(ShadowRiskPresent); }
            let signal = CopySignalRow {
                signal_id: format!("shadow:{}:{}:sell:{}",swap.signature,swap.wallet,swap.token_in),
                wallet_id: swap.wallet.clone(), side: "sell".into(), token: swap.token_in.clone(),
                notional_sol: swap.amount_out, notional_lamports: exact_sol,
                notional_origin: if exact_sol.is_some() { COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS } else { COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE }.into(),
                ts: swap.ts_utc, status: EXECUTION_SELL_INTENT_STATUS.into(),
            };
            if !self.insert_copy_signal(&signal)? { return reject(SignalAlreadyExists); }
            Ok(ExecutionSellIntentOutcome::Inserted(signal))
        }).context("failed to persist owned sell intent")
    }
}

impl SqliteDiscoveryStore {
    pub fn execution_sell_intent_position_block_reason(
        &self,
        signal: &CopySignalRow,
    ) -> Result<Option<&'static str>> {
        let tx = self
            .conn
            .unchecked_transaction()
            .context("begin source SELL guard snapshot")?;
        let result = self.execution_sell_intent_block_in_snapshot(&tx, signal)?;
        tx.commit().context("finish source SELL guard snapshot")?;
        Ok(result)
    }

    pub(crate) fn execution_sell_intent_block_in_snapshot(
        &self,
        conn: &rusqlite::Connection,
        signal: &CopySignalRow,
    ) -> Result<Option<&'static str>> {
        // Check both durable directions before caller status/side/token or legacy fallback.
        Ok(
            if let Some(binding) = crate::source_sell_promotion_guard::binding_for_signal(
                self,
                conn,
                &signal.signal_id,
            )? {
                crate::source_sell_promotion_guard::block_reason(self, conn, signal, &binding)?
            } else {
                self.legacy_execution_sell_intent_position_block_reason(signal)?
            },
        )
    }

    fn legacy_execution_sell_intent_position_block_reason(
        &self,
        signal: &CopySignalRow,
    ) -> Result<Option<&'static str>> {
        if signal.status != EXECUTION_SELL_INTENT_STATUS {
            return Ok(None);
        }
        let Some(position) = self.load_execution_canary_open_position(&signal.token)? else {
            return Ok(Some("no_owned_position"));
        };
        if !position.qty.is_finite()
            || position.qty <= 1e-12
            || position.qty_exact.is_some_and(|q| q.raw() == 0)
        {
            return Ok(Some("no_owned_position"));
        }
        if signal.ts < position.opened_ts {
            return Ok(Some("sell_before_position"));
        }
        if self
            .latest_live_execution_canary_buy_signal_ts(&signal.token)?
            .is_some_and(|ts| signal.ts < ts)
        {
            return Ok(Some("sell_before_latest_buy"));
        }
        Ok(None)
    }
}
