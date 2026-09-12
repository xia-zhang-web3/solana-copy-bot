use crate::quote_portfolio as k;
use serde_json::{json, Value};

pub fn knowledge<T: ToString>(v: &k::Knowledge<T>) -> Value {
    match v {
        k::Knowledge::Known(v) => json!({"known": v.to_string()}),
        k::Knowledge::Unknown(s) => unknown(s),
    }
}
pub fn unknown(s: &str) -> Value {
    json!({"unknown":s})
}
fn components(c: &k::Components) -> Value {
    json!({"principal":c.principal.to_string(),"base":c.base.to_string(),
        "priority":c.priority.to_string(),"setup":c.setup.to_string(),"exit":c.exit.to_string()})
}
fn flows(f: &k::CashFlows) -> Value {
    json!({"buy_principal":f.buy_principal.to_string(),"sell_gross":f.sell_gross.to_string(),
        "expenses":f.expenses.to_string(),"rent_deposited":f.rent_deposited.to_string(),
        "rent_refunded":f.rent_refunded.to_string()})
}
fn position(p: &k::Position) -> Value {
    json!({"id":p.id,"mint":bs58::encode(p.mint).into_string(),"decimals":p.decimals.to_string(),
        "entry_event_id":p.entry_event_id,"entry_raw":p.entry_raw.to_string(),
        "remaining_raw":p.remaining_raw.to_string(),"entry":components(&p.entry),
        "allocated":components(&p.allocated),"remainder":components(&p.remainder),
        "locked_rent_lamports":p.locked_rent_lamports.to_string(),
        "mark":p.mark.as_ref().map(|m| json!({"event_id":m.event.id,
            "raw":m.raw.to_string(),"net_lamports":m.net_lamports.to_string(),
            "assumed_or_synthetic":m.assumed_or_synthetic,
            "operands":"retained in events by event_id"}))})
}
pub fn book(b: &k::Book) -> Value {
    let positions: serde_json::Map<String, Value> = b
        .positions
        .iter()
        .map(|(id, p)| (id.clone(), position(p)))
        .collect();
    json!({"initial_cash_lamports":b.initial_cash_lamports.to_string(),
        "cash_lamports":b.cash_lamports.to_string(),"max_open_positions":b.max_open_positions.to_string(),
        "open_slots":b.open_slots.to_string(),"locked_rent_lamports":b.locked_rent_lamports.to_string(),
        "flows":flows(&b.flows),"positions":positions,"kernel_assumed_or_synthetic":b.assumed_or_synthetic})
}
fn state(s: &k::EventState) -> Value {
    json!({"cash_lamports":s.cash_lamports.to_string(),"locked_rent_lamports":s.locked_rent_lamports.to_string(),
        "open_slots":s.open_slots.to_string(),"position":s.position.as_ref().map(position),"flows":flows(&s.flows)})
}
fn refusal(r: &k::Refusal) -> Value {
    use k::Refusal::*;
    let code = match r {
        InvalidIdentity => "invalid_identity",
        UnsupportedInitialHistory => "unsupported_initial_history",
        ConflictId => "conflict_id",
        OutOfOrder => "out_of_order",
        InvalidRaw => "invalid_raw",
        QuoteBinding => "quote_binding",
        CostBinding => "cost_binding",
        MissingOperand { .. } => "missing_operand",
        InvalidProvenance => "invalid_provenance",
        Arithmetic => "arithmetic",
        PositionMissing => "position_missing",
        PositionExists => "position_exists",
        RefundBinding => "refund_binding",
        RentExceeded => "rent_exceeded",
        UnsupportedExpense(_) => "unsupported_expense",
        CashAvailabilityUnknown { .. } => "cash_availability_unknown",
    };
    match r {
        MissingOperand { field, reason } => json!({"code":code,"field":field,"reason":reason}),
        UnsupportedExpense(s) => json!({"code":code,"reason":s}),
        CashAvailabilityUnknown { expense_event_id } => {
            json!({"code":code,"expense_event_id":expense_event_id})
        }
        _ => json!({"code":code}),
    }
}
pub fn disposition(d: &k::Disposition) -> Value {
    match d {
        k::Disposition::Applied => json!({"state":"applied"}),
        k::Disposition::Refused(r) => json!({"state":"refused","refusal":refusal(r)}),
        k::Disposition::Skipped(reasons) => {
            json!({"state":"skipped","reasons":reasons.iter().map(|r| match r {
            k::AdmissionReason::Cash {available,required} => json!({"code":"cash","available":available.to_string(),"required":required.to_string()}),
            k::AdmissionReason::PositionCap {open,maximum} => json!({"code":"position_cap","open":open.to_string(),"maximum":maximum.to_string()}),
        }).collect::<Vec<_>>()})
        }
    }
}
pub fn valuation(v: &k::Valuation, assumed: bool, input_complete: bool) -> Value {
    let full = if input_complete {
        knowledge(&v.full_equity_lamports)
    } else {
        unknown("caller declares incomplete input history")
    };
    let net = if input_complete {
        knowledge(&v.net_change_lamports)
    } else {
        unknown("caller declares incomplete input history")
    };
    json!({"scope":"independent_exact_quotes_and_rent_book_value",
        "basis":if assumed || v.basis == k::EstimateBasis::AssumedOrSyntheticOperands {
            "assumed_or_synthetic_operands" } else { "caller_observed_operands" },
        "cash_subtotal_lamports":v.cash_subtotal_lamports.to_string(),
        "locked_rent_lamports":v.locked_rent_lamports.to_string(),
        "known_net_marks_lamports":v.known_net_marks_lamports.to_string(),
        "full_equity_lamports":full,"net_change_lamports":net,
        "missing_marks":v.missing_marks,"unresolved":v.unresolved.iter().map(|i|
            json!({"event_id":i.event_id,"refusal":refusal(&i.refusal)})).collect::<Vec<_>>()})
}
pub fn outcome(o: &k::Outcome, assumed: bool, complete: bool) -> Value {
    json!({"disposition":disposition(&o.disposition),"before":state(&o.before),"after":state(&o.after),
        "allocated_this_event":components(&o.allocated_this_event),
        "valuation_after":valuation(&o.valuation_after,assumed,complete)})
}
