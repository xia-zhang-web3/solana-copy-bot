//! Explicit quote-only replay shared by two reports; never inferred from old PnL.
//! v1 input schema lives in input.rs. Integer amounts are decimal strings. Costs,
//! initial state, time/order and virtual position associations are caller assertions.
//! EventInput.unix_ms is the declared instant of virtual BUY/SELL/Mark application,
//! not the source leader signal time. Both CLI callers pass this explicit input.
//! HTTP start must not follow that instant; response availability/freshness remain
//! unproved. request_ts identifies a historical quote version, not HTTP provenance.
//! Exact DB quote/optional failed-receipt ledger bindings are checked; no network.
mod binding;
mod convert;
mod decimals;
mod input;
mod receipt_binding;
mod serialize;
mod time_binding;

use crate::quote_portfolio::Portfolio;
use copybot_storage_core::SqliteStore;
use serde_json::{json, Value};
use std::path::Path;

pub fn unavailable(reason: &str) -> Value {
    json!({"version":1,"status":"unavailable","reason":reason,
        "production_green":false,"scope":"independent_exact_quotes_and_rent_book_value",
        "dataset_coverage":serialize::unknown("no independent source-window proof")})
}

pub fn build(path: Option<&Path>, db: Option<&Path>) -> Value {
    let Some(path) = path else {
        return unavailable("portfolio_input_not_supplied");
    };
    let input = match input::load(path) {
        Ok(i) => i,
        Err(e) => return unavailable(&format!("{e:#}")),
    };
    let result = replay(&input, db);
    let mut report = match result {
        Ok(report) => report,
        Err(e) => unavailable(&format!("{e:#}")),
    };
    // Includes initial provenance even for empty/failed scenarios. InitialState
    // has no provenance; kernel-only basis is insufficient for this envelope.
    report["assumed_or_synthetic"] = json!(convert::assumed(&input));
    report["caller_input"] = json!(input);
    report
}
fn replay(input: &input::Input, db: Option<&Path>) -> anyhow::Result<Value> {
    let expense_mode = receipt_binding::preflight(input)?;
    let initial = convert::initial(&input.initial)?;
    let mut portfolio =
        Portfolio::new(initial).map_err(|e| anyhow::anyhow!("initial state refused: {e:?}"))?;
    let store = SqliteStore::open_read_only(
        db.ok_or_else(|| anyhow::anyhow!("report DB context unavailable"))?,
    )?;
    let assumed = convert::assumed(input);
    let complete = input.window.input_complete;
    let mut events = Vec::new();
    let mut seen = std::collections::BTreeMap::new();
    for e in &input.events {
        let (event, binding) = convert::event(e, &store, &input.window)?;
        let caller = json!(e);
        if let Some((previous_caller, previous_event)) = seen.get(&e.id) {
            // The kernel intentionally has no DB-reference/caller metadata. Do
            // not mistake changed external identity for its exact-event replay.
            anyhow::ensure!(
                previous_event != &event || previous_caller == &caller,
                "caller event identity changed for an otherwise identical kernel replay: {}",
                e.id
            );
        }
        seen.entry(e.id.clone()).or_insert((caller, event.clone()));
        let outcome = portfolio.apply(event);
        events.push(json!({"input":e,"source_binding":binding,
            "outcome":serialize::outcome(&outcome,assumed,complete)}));
    }
    let value = portfolio.valuation();
    Ok(
        json!({"version":1,"status":"replayed","production_green":false,
        "scope":"independent_exact_quotes_and_rent_book_value",
        "input_coverage":{"caller_declared_complete":complete,
            "kernel_events_complete":value.unresolved.is_empty(),
            "full_valuation_known":complete && matches!(value.full_equity_lamports,crate::quote_portfolio::Knowledge::Known(_))},
        "dataset_coverage":serialize::unknown("no independent source-window proof; supplied event list is not coverage; retry rows can change"),
        "dataset_full_equity_lamports":serialize::unknown("source-window coverage unproved"),
        "dataset_net_change_lamports":serialize::unknown("source-window coverage unproved"),
        "source_snapshot":"individual read-only lookups; no cross-row snapshot or immutable dataset guarantee",
        "failed_expense_mode":expense_mode,
        "events":events,"book":serialize::book(portfolio.book()),
        "valuation":serialize::valuation(&value,assumed,complete),
        "limitations":["caller order/time/position associations are assertions",
            "costs/rent are explicit operands, not observed charged-fee proof",
            "scalar failed_attempt_expense is a caller assertion; receipt-bound mode validates a selected DB operand; position_id is only a correlation label",
            "receipt source provenance is caller-declared; local ledger validation is not current-network receipt truth or virtual-trade causation",
            "after a refused failed_attempt_expense cash is a subtotal only; later BUY cash admission remains unknown",
            "opening cash must precede modeled expense; post-fee cash would double debit; cash history and wallet reconciliation remain unproved",
            "HTTP start not after transition is necessary only; response availability and freshness unproved; floored latency is not completion proof",
            "independent quotes do not prove simultaneous liquidation or rent recovery"]}),
    )
}
