/// Explicit presence; Known(0) is distinct from Unknown, including for fees.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Knowledge<T> {
    Known(T),
    Unknown(String),
}

/// Caller-declared origin. Observed is an assertion, not a receipt verification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Provenance {
    Observed(String),
    Assumed(String),
    Synthetic(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lamports {
    pub amount: Knowledge<u64>,
    pub provenance: Provenance,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Order {
    pub sequence: u64,
    pub unix_ms: u64,
}

/// Raw mint bytes avoid price conversions and textual mint aliases.
pub type Mint = [u8; 32];

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refusal {
    InvalidIdentity,
    UnsupportedInitialHistory,
    ConflictId,
    OutOfOrder,
    InvalidRaw,
    QuoteBinding,
    CostBinding,
    MissingOperand {
        field: &'static str,
        reason: String,
    },
    InvalidProvenance,
    Arithmetic,
    PositionMissing,
    PositionExists,
    RefundBinding,
    RentExceeded,
    UnsupportedExpense(String),
    /// A refused standalone failed-attempt charge makes the cash subtotal
    /// insufficient evidence for any subsequent BUY admission in this session.
    CashAvailabilityUnknown {
        expense_event_id: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdmissionReason {
    Cash { available: u64, required: u64 },
    PositionCap { open: usize, maximum: usize },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoverageIssue {
    pub event_id: String,
    pub refusal: Refusal,
}

/// Every field is lamports. Allocation is entry component × cumulative sold raw
/// / entry raw, rounded up separately per component, minus previous allocation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Components {
    pub principal: u64,
    pub base: u64,
    pub priority: u64,
    pub setup: u64,
    pub exit: u64,
}

impl Components {
    pub fn total(&self) -> Result<u64, Refusal> {
        [
            self.principal,
            self.base,
            self.priority,
            self.setup,
            self.exit,
        ]
        .into_iter()
        .try_fold(0u64, |a, b| a.checked_add(b).ok_or(Refusal::Arithmetic))
    }

    pub(super) fn subtract(self, other: Self) -> Result<Self, Refusal> {
        let sub = |a: u64, b| a.checked_sub(b).ok_or(Refusal::Arithmetic);
        Ok(Self {
            principal: sub(self.principal, other.principal)?,
            base: sub(self.base, other.base)?,
            priority: sub(self.priority, other.priority)?,
            setup: sub(self.setup, other.setup)?,
            exit: sub(self.exit, other.exit)?,
        })
    }

    pub(super) fn cumulative(self, sold: u64, entry: u64) -> Result<Self, Refusal> {
        if entry == 0 || sold > entry {
            return Err(Refusal::InvalidRaw);
        }
        let allocate = |value: u64| -> Result<u64, Refusal> {
            let product = u128::from(value)
                .checked_mul(u128::from(sold))
                .ok_or(Refusal::Arithmetic)?;
            let numerator = product
                .checked_add(u128::from(entry) - 1)
                .ok_or(Refusal::Arithmetic)?;
            u64::try_from(numerator / u128::from(entry)).map_err(|_| Refusal::Arithmetic)
        };
        Ok(Self {
            principal: allocate(self.principal)?,
            base: allocate(self.base)?,
            priority: allocate(self.priority)?,
            setup: allocate(self.setup)?,
            exit: allocate(self.exit)?,
        })
    }
}
