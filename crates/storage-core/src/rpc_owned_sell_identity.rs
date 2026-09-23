//! Receipt identity survives stop/config removal; it never grants send permission.
use super::Prepared;
use crate::{ExecutionCanaryDispatch, SqliteDiscoveryStore};
use anyhow::{ensure, Context, Result};
use rusqlite::{Connection, OptionalExtension};

pub(crate) fn owned(c: &Connection, id: &str) -> Result<Option<Prepared>> {
    if !id.starts_with("exec-canary:rpc-owned-sell:") {
        return Ok(None);
    }
    super::required(c)?;
    let (prepared,dispatch,owner,intent):(String,String,String,String)=c.query_row(
        "SELECT prepared,dispatch,handoff_owner,intent_id FROM rpc_owned_sell_dispatches WHERE order_id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
    let p: Prepared = serde_json::from_str(&prepared)?;
    let d: ExecutionCanaryDispatch = serde_json::from_str(&dispatch)?;
    ensure!(
        p.order_id() == id
            && p.handoff.owner == owner
            && p.handoff.intent_id == intent
            && p.matches_saved(c)?,
        "owned_sell_receipt_owner_changed"
    );
    let actual:Option<(String,String,String,String,u32,String,String,String,String,String,String)>=c.query_row("SELECT signal_id,client_order_id,route,tx_signature,attempt,wallet,token,side,message_sha256,transaction_sha256,order_id FROM execution_canary_dispatch WHERE order_id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?,r.get(9)?,r.get(10)?))).optional()?;
    ensure!(
        actual
            == Some((
                d.signal_id.clone(),
                d.client_order_id.clone(),
                d.route.clone(),
                d.tx_signature.clone(),
                d.attempt,
                d.wallet.clone(),
                d.token.clone(),
                d.side.clone(),
                d.message_sha256.clone(),
                d.transaction_sha256.clone(),
                d.order_id.clone()
            )),
        "owned_sell_receipt_dispatch_changed"
    );
    let order:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM orders WHERE order_id=?1 AND signal_id=?2 AND client_order_id=?3 AND route=?4 AND attempt=?5 AND tx_signature=?6)",rusqlite::params![id,d.signal_id,d.client_order_id,d.route,d.attempt,d.tx_signature],|r|r.get(0))?;
    ensure!(
        order
            && d.signal_id == p.handoff.intent_id
            && d.client_order_id == p.handoff.owner
            && d.wallet == p.handoff.wallet
            && d.token == p.handoff.snapshot.quote.mint
            && d.side == "sell"
            && d.message_sha256 == p.message_sha256,
        "owned_sell_receipt_identity"
    );
    Ok(Some(p))
}
pub(crate) fn token_side(c: &Connection, id: &str) -> Result<(String, String)> {
    if let Some(p) = owned(c, id)? {
        return Ok((p.handoff.snapshot.quote.mint, "sell".into()));
    }
    c.query_row("SELECT s.token,s.side FROM orders o JOIN copy_signals s ON s.signal_id=o.signal_id WHERE o.order_id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?))).context("receipt signal missing")
}
pub(crate) fn position(
    c: &Connection,
    id: &str,
    p: &crate::SellSettlementExpectedPosition,
) -> Result<()> {
    if let Some(bound) = owned(c, id)? {
        let b = &bound.handoff.snapshot.quote;
        let held_raw = if let Some(d) = &b.fractional {
            let facts =
                crate::receipt_facts_rows::load(c, id)?.context("fraction_receipt_missing")?;
            ensure!(
                b.version == 2
                    && d.version == 1
                    && d.selected_raw == b.raw
                    && crate::ordered_sell_quote::fractional::inventory::allocate(
                        &[d.owned_raw],
                        d.inventory.numerator,
                        d.inventory.denominator.parse()?
                    )?[0]
                        == b.raw
                    && facts
                        .token_delta
                        .as_ref()
                        .is_some_and(|q| q.raw == -i128::from(b.raw) && q.decimals == b.decimals),
                crate::SellSettlementUnsupported::OwnedSelectedQuantityChanged
            );
            d.owned_raw
        } else {
            b.raw
        };
        ensure!(
            p.position_id == b.position_id
                && p.opened_ts == b.position_opened_ts
                && p.token == b.mint
                && p.quantity.raw() == held_raw
                && p.quantity.decimals() == b.decimals,
            crate::SellSettlementUnsupported::OwnedPositionChanged
        );
        let attribution = crate::execution_canary_buy_attribution::read_on_conn(c, &b.mint)?;
        let crate::ExecutionCanaryBuyAttribution::Open(a) = attribution else {
            anyhow::bail!(crate::SellSettlementUnsupported::OwnedContributorsChanged);
        };
        ensure!(
            a.unproven_links.is_empty()
                && a.proven_contributors
                    == bound
                        .handoff
                        .snapshot
                        .receipts
                        .iter()
                        .map(|r| r.contributor.clone())
                        .collect::<Vec<_>>(),
            crate::SellSettlementUnsupported::OwnedContributorsChanged
        );
        for r in &bound.handoff.snapshot.receipts {
            let f = crate::receipt_facts_rows::load(c, &r.contributor.order_id)?
                .context("owned_sell_origin_receipt_missing")?;
            ensure!(
                format!("receipt_facts_v1:{f:?}") == r.receipt_fingerprint,
                crate::SellSettlementUnsupported::OwnedOriginReceiptChanged
            );
        }
    }
    Ok(())
}
impl SqliteDiscoveryStore {
    pub fn execution_receipt_token_side(&self, id: &str) -> Result<(String, String)> {
        token_side(&self.conn, id)
    }
}
