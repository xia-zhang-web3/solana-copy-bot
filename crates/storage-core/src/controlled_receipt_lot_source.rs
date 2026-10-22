//! Read-only daemon receipt witnesses, never an operator-provided fraction.
use super::{load, Scope};
use crate::{ExecutionOrderOrigin, SqliteStore, EXECUTION_STATUS_CANARY_CONFIRMED};
use anyhow::{ensure, Context, Result};
use copybot_core_types::association_delivery::CheckedFacts;
use serde::{Deserialize, Serialize};
const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const SOL: &str = "So11111111111111111111111111111111111111112";
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LotReceipt {
    pub order_id: String,
    pub signature: String,
    pub slot: u64,
    pub amount_raw: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceReceipts {
    pub buy: LotReceipt,
    pub sell: LotReceipt,
    pub remaining_before_raw: u64,
}
fn source(scope: &Scope) -> Result<SqliteStore> {
    let db = SqliteStore::open_read_only(&scope.source_state_db_path)?;
    let (wallet, run, actual) = load(&db.conn)?.context("receipt_lot_source_scope_missing")?;
    ensure!(
        wallet == scope.source_wallet && run == scope.source_run_id && actual.same_scenario(scope),
        "receipt_lot_source_scope_changed"
    );
    Ok(db)
}
fn confirmed(db: &SqliteStore, id: &str, wallet: &str, side: &str) -> Result<Option<LotReceipt>> {
    let Some(order) = db.load_execution_canary_order(id)? else {
        return Ok(None);
    };
    let Some(proof) = db.load_execution_canary_receipt_proof(id)? else {
        return Ok(None);
    };
    if order.status != EXECUTION_STATUS_CANARY_CONFIRMED
        || proof.reason != "accounting_complete"
        || !db.execution_canary_fill_exists(id)?
    {
        return Ok(None);
    }
    let facts = db
        .load_execution_canary_receipt_facts(id)?
        .context("receipt_lot_facts_missing")?;
    crate::receipt_facts_identity::validate_identity(&db.conn, &facts)?;
    let delta = facts.token_delta.context("receipt_lot_token_unknown")?;
    ensure!(
        facts.wallet_pubkey == wallet
            && facts.token == USDC
            && facts.side == side
            && delta.decimals == 6
            && (if side == "buy" {
                delta.raw > 0
            } else {
                delta.raw < 0
            })
            && order.tx_signature.as_deref() == Some(facts.tx_signature.as_str())
            && proof.tx_signature == facts.tx_signature
            && proof.slot == Some(facts.slot)
            && matches!(
                proof.confirmation_status.as_str(),
                "confirmed" | "finalized"
            )
            && facts.fee_payer.as_deref() == Some(wallet)
            && facts.transaction_fee.is_some(),
        "receipt_lot_receipt_identity"
    );
    Ok(Some(LotReceipt {
        order_id: id.into(),
        signature: facts.tx_signature,
        slot: facts.slot,
        amount_raw: u64::try_from(delta.raw.unsigned_abs())?,
    }))
}
fn buy_parent(db: &SqliteStore, s: &Scope) -> Result<crate::OwnerTechnicalBuyIntent> {
    let p = db
        .load_owner_technical_buy_intent(&s.source_buy_intent_id)?
        .context("receipt_lot_source_buy_intent_missing")?;
    ensure!(
        p.run_id == s.source_run_id
            && p.wallet == s.source_wallet
            && p.signer == p.wallet
            && p.mint == USDC
            && p.amount_lamports == 10_000_000,
        "receipt_lot_source_buy_parent"
    );
    Ok(p)
}
impl SqliteStore {
    pub fn controlled_buy_candidate(
        &self,
        scope: &Scope,
        signature: &str,
        slot: u64,
        wallet: &str,
        mint: &str,
    ) -> Result<()> {
        let buy = Self::controlled_source_buy(scope)?.context("receipt_lot_source_buy_pending")?;
        let raw: String = self.conn.query_row(
            "SELECT admission FROM association_inbox_identities WHERE signature=?1",
            [signature],
            |r| r.get(0),
        )?;
        let a: copybot_core_types::association_delivery::AdmissionFacts =
            serde_json::from_str(&raw)?;
        let exact = a
            .facts
            .exact_amounts
            .context("receipt_lot_source_buy_exact")?;
        ensure!(
            a.facts.signature == signature
                && signature == buy.signature
                && a.facts.slot == slot
                && slot == buy.slot
                && a.facts.wallet == wallet
                && wallet == scope.source_wallet
                && mint == USDC
                && a.facts.token_in == SOL
                && a.facts.token_out == mint
                && exact.amount_in_raw == "10000000"
                && exact.amount_in_decimals == 9
                && exact.amount_out_raw == buy.amount_raw.to_string()
                && exact.amount_out_decimals == 6,
            "receipt_lot_source_buy_event_amount"
        );
        Ok(())
    }
    pub fn controlled_source_buy(scope: &Scope) -> Result<Option<LotReceipt>> {
        let db = source(scope)?;
        let tx = db.conn.unchecked_transaction()?;
        buy_parent(&db, scope)?;
        let id = crate::owner_technical_buy_order_id(&scope.source_buy_intent_id);
        ensure!(
            db.execution_order_origin(&id)?
                == Some(ExecutionOrderOrigin::OwnerTechnicalBuy {
                    intent_id: scope.source_buy_intent_id.clone()
                }),
            "receipt_lot_source_buy_origin"
        );
        let result = confirmed(&db, &id, &scope.source_wallet, "buy")?;
        tx.commit()?;
        Ok(result)
    }
    pub fn controlled_source_receipts(
        scope: &Scope,
        signature: &str,
        slot: u64,
    ) -> Result<Option<SourceReceipts>> {
        let db = source(scope)?;
        let tx = db.conn.unchecked_transaction()?;
        let parent = buy_parent(&db, scope)?;
        let bid = crate::owner_technical_buy_order_id(&scope.source_buy_intent_id);
        let Some(buy) = confirmed(&db, &bid, &scope.source_wallet, "buy")? else {
            return Ok(None);
        };
        let exit_id = format!("execution-only:{}", scope.source_buy_intent_id);
        let Some(exit) = db.load_owner_exit_intent(&exit_id)? else {
            return Ok(None);
        };
        ensure!(
            exit.run_id == scope.source_run_id
                && exit.buy_order_id == bid
                && exit.buy_receipt_signature == buy.signature
                && exit.wallet == scope.source_wallet
                && exit.mint == USDC
                && exit.activated_at == parent.activated_at
                && exit.expires_at == parent.expires_at
                && exit.amount_raw <= buy.amount_raw,
            "receipt_lot_source_exit_parent"
        );
        let sid = crate::owner_exit_order_id(&exit_id);
        ensure!(
            db.execution_order_origin(&sid)?
                == Some(ExecutionOrderOrigin::OwnerExit { intent_id: exit_id }),
            "receipt_lot_source_exit_origin"
        );
        let Some(sell) = confirmed(&db, &sid, &scope.source_wallet, "sell")? else {
            return Ok(None);
        };
        ensure!(
            sell.signature == signature
                && sell.slot == slot
                && sell.slot >= buy.slot
                && sell.amount_raw == exit.amount_raw
                && sell.amount_raw <= buy.amount_raw,
            "receipt_lot_source_sell_event"
        );
        let result = SourceReceipts {
            remaining_before_raw: buy.amount_raw,
            buy,
            sell,
        };
        tx.commit()?;
        Ok(Some(result))
    }
}
/// Executed during the genuine native admission transaction. The SOURCE dispatch
/// is durable before send; confirmation may arrive later, without retiming the event.
pub(crate) fn event_allowed(c: &rusqlite::Connection, f: &CheckedFacts, buy: bool) -> Result<bool> {
    let Some((wallet, _, s)) = load(c)? else {
        return Ok(true);
    };
    if wallet != s.bot_wallet {
        return Ok(false);
    }
    if f.wallet != s.source_wallet
        || (if buy {
            f.token_in != SOL || f.token_out != USDC
        } else {
            f.token_in != USDC || f.token_out != SOL
        })
    {
        return Ok(false);
    }
    let db = source(&s)?;
    let parent = match db.load_owner_technical_buy_intent(&s.source_buy_intent_id)? {
        Some(p) => p,
        None => return Ok(false),
    };
    ensure!(
        parent.run_id == s.source_run_id && parent.wallet == s.source_wallet,
        "receipt_lot_source_dispatch_parent"
    );
    let (id, origin) = if buy {
        (
            crate::owner_technical_buy_order_id(&s.source_buy_intent_id),
            ExecutionOrderOrigin::OwnerTechnicalBuy {
                intent_id: s.source_buy_intent_id.clone(),
            },
        )
    } else {
        let exit = format!("execution-only:{}", s.source_buy_intent_id);
        (
            crate::owner_exit_order_id(&exit),
            ExecutionOrderOrigin::OwnerExit { intent_id: exit },
        )
    };
    let Some(dispatch) = db.load_execution_canary_dispatch(&id)? else {
        return Ok(false);
    };
    Ok(db.execution_order_origin(&id)? == Some(origin)
        && dispatch.tx_signature == f.signature
        && dispatch.wallet == s.source_wallet
        && dispatch.token == USDC
        && dispatch.side == if buy { "buy" } else { "sell" })
}
