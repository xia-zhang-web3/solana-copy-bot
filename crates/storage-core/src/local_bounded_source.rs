//! Exact planned external SOURCE transactions; genuine native finality remains mandatory.
use super::*;
use crate::controlled_receipt_lot::{LotReceipt, SourceReceipts};
use copybot_core_types::association_delivery::{CheckedFacts, Terminal};
const SOL: &str = "So11111111111111111111111111111111111111112";
const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
pub(crate) fn event_allowed(c: &Connection, f: &CheckedFacts, buy: bool) -> Result<bool> {
    let a = load(c)?.context("local_session_missing")?;
    let current = state::current(c)?;
    if current.state == "closed" {
        return Ok(false);
    }
    let p = a
        .model
        .cycles
        .iter()
        .find(|p| p.cycle_id == current.id)
        .context("local_session_cycle_missing")?;
    let amount = f.exact_amounts.as_ref();
    Ok(f.wallet == a.scope.source_wallet
        && f.slot > 0
        && !f.program_fallback
        && &f.signature
            == (if buy {
                &p.buy_signature
            } else {
                &p.sell_signature
            })
        && f.token_in == if buy { SOL } else { USDC }
        && f.token_out == if buy { USDC } else { SOL }
        && amount.is_some_and(|x| {
            if buy {
                x.amount_in_raw == "10000000"
                    && x.amount_in_decimals == 9
                    && x.amount_out_raw == p.acquired_raw.to_string()
                    && x.amount_out_decimals == 6
            } else {
                x.amount_in_raw == p.sold_raw.to_string()
                    && x.amount_in_decimals == 6
                    && x.amount_out_decimals == 9
            }
        }))
}
fn database(s: &Scope) -> Result<Option<(SqliteStore, Authority, SourceCycle)>> {
    let db = SqliteStore::open_read_only(&s.source_state_db_path)?;
    let Some(a) = load(&db.conn)? else {
        return Ok(None);
    };
    let p = a
        .model
        .cycles
        .iter()
        .find(|p| p.cycle_id == s.source_buy_intent_id)
        .context("local_session_scope_cycle")?
        .clone();
    let mut exact = a.scope.clone();
    exact.source_buy_intent_id = p.cycle_id.clone();
    ensure!(*s == exact, "local_session_source_scope_changed");
    Ok(Some((db, a, p)))
}
fn receipt(
    c: &Connection,
    a: &Authority,
    p: &SourceCycle,
    buy: bool,
) -> Result<Option<LotReceipt>> {
    let sig = if buy {
        &p.buy_signature
    } else {
        &p.sell_signature
    };
    let Some(i) = crate::association_inbox::identity(c, sig)? else {
        return Ok(None);
    };
    if i.conflict || i.recovery {
        return Ok(None);
    }
    let Some(Terminal::ProviderAsserted(t)) = i.terminal else {
        return Ok(None);
    };
    let f = &i.admission.facts;
    ensure!(
        t.signature == *sig
            && t.slot == f.slot
            && f.signature == *sig
            && f.wallet == a.scope.source_wallet,
        "local_session_source_terminal"
    );
    let witnessed: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM local_bounded_source_receipts
        WHERE signature=?1 AND cycle_id=?2 AND side=?3 AND slot=?4 AND message_sha256=?5)",
        params![
            sig,
            p.cycle_id,
            if buy { "buy" } else { "sell" },
            f.slot,
            if buy {
                &p.buy_message_sha256
            } else {
                &p.sell_message_sha256
            }
        ],
        |r| r.get(0),
    )?;
    if !witnessed {
        return Ok(None);
    }
    let raw = if buy { p.acquired_raw } else { p.sold_raw };
    let x = f
        .exact_amounts
        .as_ref()
        .context("local_session_source_amount")?;
    ensure!(
        if buy {
            f.token_in == SOL
                && f.token_out == USDC
                && x.amount_in_raw == "10000000"
                && x.amount_in_decimals == 9
                && x.amount_out_raw == raw.to_string()
                && x.amount_out_decimals == 6
        } else {
            f.token_in == USDC
                && f.token_out == SOL
                && x.amount_in_raw == raw.to_string()
                && x.amount_in_decimals == 6
        },
        "local_session_source_receipt_amount"
    );
    if buy {
        let finalized: bool = c.query_row(
            "SELECT EXISTS(SELECT 1 FROM native_buy_decisions WHERE signature=?1
            AND finalized_at IS NOT NULL AND finalized_slot=?2 AND late=0)",
            params![sig, f.slot],
            |r| r.get(0),
        )?;
        if !finalized {
            return Ok(None);
        }
    }
    Ok(Some(LotReceipt {
        order_id: format!("external-source:{sig}"),
        signature: sig.clone(),
        slot: f.slot,
        amount_raw: raw,
    }))
}
pub(crate) fn buy(s: &Scope) -> Result<Option<Option<LotReceipt>>> {
    if !cfg!(feature = "local-bounded-session") {
        return Ok(None);
    }
    let Some((db, a, p)) = database(s)? else {
        return Ok(None);
    };
    Ok(Some(receipt(&db.conn, &a, &p, true)?))
}
impl SqliteStore {
    pub fn record_local_source_receipt(
        &self,
        signature: &str,
        side: &str,
        slot: u64,
        message: &str,
        transaction: &str,
        receipt: &str,
        now: DateTime<Utc>,
    ) -> Result<()> {
        self.with_immediate_transaction_retry("record local source receipt", |c| {
            let a = load(c)?.context("local_session_missing")?;
            let current = state::current(c)?;
            ensure!(
                now >= a.cohort.activated_at && now < a.cohort.deadline,
                "local_session_source_deadline"
            );
            let p = a
                .model
                .cycles
                .iter()
                .find(|p| p.cycle_id == current.id)
                .context("local_session_cycle_missing")?;
            ensure!(
                (side == "buy" && signature == p.buy_signature && message == p.buy_message_sha256)
                    || (side == "sell"
                        && signature == p.sell_signature
                        && message == p.sell_message_sha256),
                "local_session_source_message_binding"
            );
            let old: Option<(String, String, u64, String, String, String)> = c
                .query_row(
                    "SELECT cycle_id,side,slot,message_sha256,transaction_sha256,receipt_sha256
                FROM local_bounded_source_receipts WHERE signature=?1",
                    [signature],
                    |r| {
                        Ok((
                            r.get(0)?,
                            r.get(1)?,
                            r.get(2)?,
                            r.get(3)?,
                            r.get(4)?,
                            r.get(5)?,
                        ))
                    },
                )
                .optional()?;
            if let Some(old) = old {
                ensure!(
                    old == (
                        p.cycle_id.clone(),
                        side.into(),
                        slot,
                        message.into(),
                        transaction.into(),
                        receipt.into()
                    ),
                    "local_session_source_receipt_conflict"
                );
                return Ok(());
            }
            c.execute(
                "INSERT INTO local_bounded_source_receipts VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    signature,
                    p.cycle_id,
                    side,
                    slot,
                    message,
                    transaction,
                    receipt,
                    now.to_rfc3339()
                ],
            )?;
            Ok(())
        })
    }
}
pub(crate) fn pair(s: &Scope, sig: &str, slot: u64) -> Result<Option<Option<SourceReceipts>>> {
    if !cfg!(feature = "local-bounded-session") {
        return Ok(None);
    }
    let Some((db, a, p)) = database(s)? else {
        return Ok(None);
    };
    let (Some(buy), Some(sell)) = (
        receipt(&db.conn, &a, &p, true)?,
        receipt(&db.conn, &a, &p, false)?,
    ) else {
        return Ok(Some(None));
    };
    ensure!(
        sell.signature == sig
            && sell.slot == slot
            && sell.slot > buy.slot
            && sell.amount_raw <= buy.amount_raw,
        "local_session_source_pair"
    );
    Ok(Some(Some(SourceReceipts {
        remaining_before_raw: buy.amount_raw,
        buy,
        sell,
    })))
}
