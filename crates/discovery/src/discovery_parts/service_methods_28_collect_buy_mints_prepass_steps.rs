use crate::*;

pub(crate) enum CollectBuyMintsPrepassStep {
    Continue,
    Break(PersistedStreamBudgetExhaustedReason),
    Return(PersistedStreamPhaseAdvance),
}
