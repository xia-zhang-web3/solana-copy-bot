//! Synthetic measurement accessors; compiled only by the parent cfg(test) module.
use super::{measurement_input, Reader};

impl Reader {
    #[cfg(test)]
    pub(in crate::source) fn measurement_input_snapshot() -> serde_json::Value {
        measurement_input::snapshot()
    }
    #[cfg(test)]
    pub(in crate::source) fn measurement_input_count() -> u64 {
        measurement_input::attempted()
    }
    #[cfg(test)]
    pub(in crate::source) fn measurement_input_errors() -> u64 {
        measurement_input::errors()
    }
}
