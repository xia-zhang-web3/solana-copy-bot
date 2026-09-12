//! In-memory, quote-only book transitions. No report ingestion or execution proof.
//!
//! Start with explicit zero inventory/transfers. Feed events in caller order;
//! sequence must increase and time cannot decrease (timestamps are Unix ms).
//! IDs are opaque, nonempty ASCII labels of at most 128 bytes; mints are 32-byte
//! identities. Closed position IDs cannot be reused. Exact replay returns the
//! original outcome, including its historical before/after and valuation.
//!
//! Refusals preserve the committed book and append sticky incomplete coverage.
//! Valid ordered attempts, including skips/refusals, consume their order. Invalid
//! order/ID conflicts do not advance it. Corrections require a fresh explicit
//! replay; this API provides no persistence, repair or coverage-reset mechanism.
//! BUY/SELL expense bundles own their fees exactly once. FailedAttemptExpense is
//! a separate portfolio debit; other standalone expenses remain unsupported.
//! A refused failed-attempt charge blocks subsequent BUY cash admission until a
//! fresh replay. Rent refund is a separate event bound to its deposit.
//!
//! Valuation is an estimate of cash + independent exact net SELL quotes + locked
//! rent at book value. It is not jointly executable liquidation, rent recovery,
//! observed wallet net, current-network fee correctness or production acceptance.
//! Provenance is supplied by the caller, never verified here. There is no freshness
//! policy beyond explicit ordering and exact remaining-size binding.

mod costs;
mod event;
mod state;
mod trades;
mod transition;
mod types;
mod valuation;

pub use event::*;
pub use state::*;
pub use transition::Portfolio;
pub use types::*;
