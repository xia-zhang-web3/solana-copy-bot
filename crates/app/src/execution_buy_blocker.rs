//! One current unresolved BUY witness, independent of the last processed order.
use copybot_storage_core::{SqliteStore, EXECUTION_UNRESOLVED_BUY_REASON};

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) enum BuyBlocker {
    #[default]
    NotObserved,
    Unresolved(Box<str>),
    Unavailable,
}

impl BuyBlocker {
    pub(crate) fn unresolved(order_id: String) -> Self {
        Self::Unresolved(order_id.into_boxed_str())
    }

    pub(crate) fn merge(&mut self, other: Self) {
        if other != Self::NotObserved {
            *self = other;
        }
    }

    /// A later receipt in this tick may have settled the observed A. Refresh only
    /// diagnostics; a read error neither releases risk nor reports a stale ID.
    pub(crate) fn refresh(&mut self, store: &SqliteStore) {
        if *self != Self::NotObserved {
            *self = match store.execution_canary_unresolved_buy_order_id() {
                Ok(Some(id)) => Self::unresolved(id),
                Ok(None) => Self::NotObserved,
                Err(_) => Self::Unavailable,
            };
        }
    }

    pub(crate) fn order_id(&self) -> &str {
        match self {
            Self::Unresolved(id) => id,
            _ => "none",
        }
    }

    pub(crate) fn reason(&self) -> &'static str {
        match self {
            Self::Unresolved(_) => EXECUTION_UNRESOLVED_BUY_REASON,
            Self::Unavailable => "unresolved_buy_state_unavailable",
            Self::NotObserved => "none",
        }
    }
}
