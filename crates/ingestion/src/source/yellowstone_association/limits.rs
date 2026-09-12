use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub(in crate::source) struct Budget {
    pub count: usize,
    pub encoded_bytes: usize,
}
#[derive(Debug, Clone, Copy)]
pub(in crate::source) struct Limits {
    pub pending: Budget,
    pub blocks: Budget,
    /// Reserved at admission, includes pending as well as completed identities.
    pub history: Budget,
    pub outputs: Budget,
    pub input_bytes: usize,
    pub metadata_bytes: usize,
    pub pending_ttl: Duration,
    pub block_ttl: Duration,
    pub history_ttl: Duration,
}
#[derive(Debug, PartialEq, Eq)]
pub(in crate::source) enum InvalidLimits {
    Zero,
    Overflow,
}
impl Limits {
    pub(super) fn validate(self) -> Result<Self, InvalidLimits> {
        let budgets = [self.pending, self.blocks, self.history, self.outputs];
        if budgets.iter().any(|b| b.count == 0 || b.encoded_bytes == 0)
            || self.input_bytes == 0
            || self.metadata_bytes == 0
            || [self.pending_ttl, self.block_ttl, self.history_ttl]
                .iter()
                .any(Duration::is_zero)
        {
            return Err(InvalidLimits::Zero);
        }
        // Bound arithmetic for state, one input, output and API80 scratch. This
        // is not a claim about decoded protobuf allocations or resident memory.
        let mut total = self
            .input_bytes
            .checked_mul(3)
            .and_then(|n| n.checked_add(self.metadata_bytes));
        for b in budgets {
            total = total
                .and_then(|v| v.checked_add(b.encoded_bytes))
                .and_then(|v| b.count.checked_mul(1024).and_then(|n| v.checked_add(n)));
        }
        total
            .and_then(|v| v.checked_add(2 * 65536))
            .filter(|v| *v <= isize::MAX as usize)
            .ok_or(InvalidLimits::Overflow)?;
        Ok(self)
    }
}

pub(super) fn expired(now: Duration, then: Duration, ttl: Duration) -> bool {
    now.saturating_sub(then) > ttl
}
