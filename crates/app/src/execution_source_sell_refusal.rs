//! Bounded identity for local write-off refusals, independent of the last successful order.
#[derive(Debug, Clone, PartialEq)]
struct Refusal {
    order_id: String,
    reason: &'static str,
    count: usize,
}
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct SourceSellWriteOffRefusals {
    last: Option<Box<Refusal>>,
}
impl SourceSellWriteOffRefusals {
    pub(crate) fn record(&mut self, order_id: &str, reason: &'static str) {
        let reason = if reason.starts_with("source_sell_") && reason.len() <= 64 {
            reason
        } else {
            "source_sell_write_off_unavailable"
        };
        self.last = Some(Box::new(Refusal {
            order_id: order_id.into(),
            reason,
            count: self.count().saturating_add(1),
        }));
    }
    pub(crate) fn merge(&mut self, other: Self) {
        if let Some(mut next) = other.last {
            next.count = next.count.saturating_add(self.count());
            self.last = Some(next);
        }
    }
    pub(crate) fn count(&self) -> usize {
        self.last.as_ref().map_or(0, |r| r.count)
    }
    pub(crate) fn order_id(&self) -> &str {
        self.last.as_ref().map_or("none", |r| r.order_id.as_str())
    }
    pub(crate) fn reason(&self) -> &'static str {
        self.last.as_ref().map_or("none", |r| r.reason)
    }
}

/// A transaction-local rejection may be skipped; broken schema/I/O/locking must surface.
pub(crate) fn is_local_source_write_off_error(error: &anyhow::Error) -> bool {
    for cause in error.chain() {
        if let Some(sql) = cause.downcast_ref::<rusqlite::Error>() {
            return match sql {
                rusqlite::Error::SqliteFailure(code, _) => {
                    code.code == rusqlite::ErrorCode::ConstraintViolation
                }
                rusqlite::Error::FromSqlConversionFailure(..)
                | rusqlite::Error::IntegralValueOutOfRange(..)
                | rusqlite::Error::InvalidColumnType(..) => true,
                _ => false,
            };
        }
    }
    // The writer's missing/mismatched proof and affected-row checks are order-local.
    true
}
