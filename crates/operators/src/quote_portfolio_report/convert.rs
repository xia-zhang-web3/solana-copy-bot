use super::{binding, input::*};
use crate::quote_portfolio as k;
use anyhow::{Context, Result};
use copybot_storage_core::SqliteStore;
use serde_json::{json, Value};

pub fn origin(o: &Origin) -> k::Provenance {
    match o {
        Origin::Observed(s) => k::Provenance::Observed(s.clone()),
        Origin::Assumed(s) => k::Provenance::Assumed(s.clone()),
        Origin::Synthetic(s) => k::Provenance::Synthetic(s.clone()),
    }
}
pub fn side(s: Side) -> k::Direction {
    match s {
        Side::Buy => k::Direction::Buy,
        Side::Sell => k::Direction::Sell,
    }
}
fn amount(a: Option<&Amount>) -> Result<k::Lamports> {
    let Some(a) = a else {
        return Ok(k::Lamports {
            amount: k::Knowledge::Unknown("operand absent".into()),
            provenance: k::Provenance::Observed("absent operand; no value asserted".into()),
        });
    };
    a.provenance.validate()?;
    let value = match &a.value {
        Presence::Known(s) => k::Knowledge::Known(raw(s)?),
        Presence::Unknown(s) => k::Knowledge::Unknown(s.clone()),
    };
    Ok(k::Lamports {
        amount: value,
        provenance: origin(&a.provenance),
    })
}
fn known(a: &Amount) -> Result<u64> {
    match amount(Some(a))?.amount {
        k::Knowledge::Known(n) => Ok(n),
        k::Knowledge::Unknown(s) => anyhow::bail!("initial operand unknown: {s}"),
    }
}
pub fn initial(i: &Initial) -> Result<k::InitialState> {
    Ok(k::InitialState {
        cash_lamports: known(&i.cash_lamports)?,
        max_open_positions: usize::try_from(known(&i.max_open_positions)?)?,
        inventory_raw: amount(Some(&i.inventory_raw))?.amount,
        external_transfers_lamports: match amount(Some(&i.external_transfers_lamports))?.amount {
            k::Knowledge::Known(v) => k::Knowledge::Known(i128::from(v)),
            k::Knowledge::Unknown(s) => k::Knowledge::Unknown(s),
        },
    })
}
fn costs(c: &CostsInput) -> Result<k::TradeCosts> {
    Ok(k::TradeCosts {
        event_id: c.event_id.clone(),
        position_id: c.position_id.clone(),
        direction: side(c.side),
        base: amount(c.base.as_ref())?,
        priority: amount(c.priority.as_ref())?,
        setup: amount(c.setup.as_ref())?,
        exit: amount(c.exit.as_ref())?,
    })
}
pub fn event(e: &EventInput, store: &SqliteStore, window: &Window) -> Result<(k::Event, Value)> {
    let unix_ms = raw(&e.unix_ms)?;
    let mut evidence = json!({"state":"not_applicable"});
    let mut quote = |q: &Option<QuoteRef>| -> k::Knowledge<k::ExactQuote> {
        let result = q
            .as_ref()
            .context("quote reference absent")
            .and_then(|q| binding::resolve(store, q, unix_ms));
        match result {
            Ok((q, verified)) => {
                evidence = verified;
                k::Knowledge::Known(q)
            }
            Err(err) => {
                let reason = format!("{err:#}");
                evidence = json!({"state":"unavailable", "reason":reason});
                k::Knowledge::Unknown(reason)
            }
        }
    };
    let action = match &e.action {
        ActionInput::Buy {
            mint,
            decimals,
            input_lamports,
            quote: q,
            costs: c,
            rent_deposit,
        } => k::Action::Buy {
            mint: binding::mint(mint)?,
            decimals: *decimals,
            input_lamports: raw(input_lamports)?,
            quote: quote(q),
            costs: costs(c)?,
            rent_deposit: amount(rent_deposit.as_ref())?,
        },
        ActionInput::Sell {
            raw: size,
            quote: q,
            costs: c,
        } => k::Action::Sell {
            raw: raw(size)?,
            quote: quote(q),
            costs: costs(c)?,
        },
        ActionInput::Mark { quote: q, costs: c } => k::Action::Mark {
            quote: quote(q),
            costs: costs(c)?,
        },
        ActionInput::RentRefund {
            deposit_event_id,
            amount: a,
        } => k::Action::RentRefund {
            deposit_event_id: deposit_event_id.clone(),
            amount: amount(a.as_ref())?,
        },
        ActionInput::UnsupportedExpense {
            description,
            amount: a,
        } => k::Action::UnsupportedExpense {
            kind: description.clone(),
            amount: amount(a.as_ref())?,
        },
        ActionInput::FailedAttemptExpense {
            amount: a,
            receipt_ref,
        } => {
            let amount = if let Some(r) = receipt_ref {
                let (amount, verified) =
                    super::receipt_binding::expense(store, r, a.as_ref(), e, window);
                evidence = verified;
                amount
            } else {
                evidence = json!({"mode":"scalar_caller_assertion","state":"caller_assertion",
                    "amount":a,"scope":"supplied scalar103; no observed payment binding"});
                amount(a.as_ref())?
            };
            k::Action::FailedAttemptExpense { amount }
        }
    };
    Ok((
        k::Event {
            id: e.id.clone(),
            position_id: e.position_id.clone(),
            order: k::Order {
                sequence: raw(&e.sequence)?,
                unix_ms,
            },
            action,
        },
        evidence,
    ))
}

pub fn assumed(i: &Input) -> bool {
    let initial = [
        &i.initial.cash_lamports,
        &i.initial.max_open_positions,
        &i.initial.inventory_raw,
        &i.initial.external_transfers_lamports,
    ];
    i.scenario_provenance.assumed()
        || i.window.input_provenance.assumed()
        || initial.iter().any(|a| a.provenance.assumed())
        || i.events.iter().any(|e| {
            if e.identity_provenance.assumed() {
                return true;
            }
            if let ActionInput::FailedAttemptExpense {
                receipt_ref: Some(r),
                ..
            } = &e.action
            {
                if r.source_provenance.assumed() {
                    return true;
                }
            }
            let (q, c, a) = match &e.action {
                ActionInput::Buy {
                    quote,
                    costs,
                    rent_deposit,
                    ..
                } => (quote.as_ref(), Some(costs), rent_deposit.as_ref()),
                ActionInput::Sell { quote, costs, .. } | ActionInput::Mark { quote, costs } => {
                    (quote.as_ref(), Some(costs), None)
                }
                ActionInput::RentRefund { amount, .. }
                | ActionInput::FailedAttemptExpense { amount, .. }
                | ActionInput::UnsupportedExpense { amount, .. } => (None, None, amount.as_ref()),
            };
            q.is_some_and(|q| q.provenance.assumed())
                || a.is_some_and(|a| a.provenance.assumed())
                || c.is_some_and(|c| {
                    [&c.base, &c.priority, &c.setup, &c.exit]
                        .into_iter()
                        .flatten()
                        .any(|a| a.provenance.assumed())
                })
        })
}
