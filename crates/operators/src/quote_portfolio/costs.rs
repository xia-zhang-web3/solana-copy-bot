use super::{Components, Direction, Event, ExactQuote, Knowledge, Lamports, Mint};
use super::{Provenance, Refusal, TradeCosts};

pub(super) fn known<'a, T>(value: &'a Knowledge<T>, field: &'static str) -> Result<&'a T, Refusal> {
    match value {
        Knowledge::Known(value) => Ok(value),
        Knowledge::Unknown(reason) => Err(Refusal::MissingOperand {
            field,
            reason: reason.clone(),
        }),
    }
}

pub(super) fn provenance(value: &Provenance) -> Result<bool, Refusal> {
    let (reference, assumed) = match value {
        Provenance::Observed(s) => (s, false),
        Provenance::Assumed(s) | Provenance::Synthetic(s) => (s, true),
    };
    if reference.trim().is_empty() {
        return Err(Refusal::InvalidProvenance);
    }
    Ok(assumed)
}

pub(super) fn lamports(value: &Lamports, field: &'static str) -> Result<(u64, bool), Refusal> {
    Ok((
        *known(&value.amount, field)?,
        provenance(&value.provenance)?,
    ))
}

pub(super) fn expenses(
    event: &Event,
    costs: &TradeCosts,
    direction: Direction,
) -> Result<(Components, bool), Refusal> {
    if costs.event_id != event.id
        || costs.position_id != event.position_id
        || costs.direction != direction
    {
        return Err(Refusal::CostBinding);
    }
    let (base, a) = lamports(&costs.base, "base fee")?;
    let (priority, b) = lamports(&costs.priority, "priority fee")?;
    let (setup, c) = lamports(&costs.setup, "setup expense")?;
    let (exit, d) = lamports(&costs.exit, "exit expense")?;
    if (direction == Direction::Buy && exit != 0) || (direction == Direction::Sell && setup != 0) {
        return Err(Refusal::CostBinding);
    }
    Ok((
        Components {
            principal: 0,
            base,
            priority,
            setup,
            exit,
        },
        a || b || c || d,
    ))
}

pub(super) fn quote(
    event: &Event,
    quote: &Knowledge<ExactQuote>,
    mint: Mint,
    decimals: u8,
    direction: Direction,
    size: u64,
) -> Result<(u64, bool), Refusal> {
    let quote = known(quote, "exact quote")?;
    if quote.position_id != event.position_id
        || quote.mint != mint
        || quote.decimals != decimals
        || quote.direction != direction
        || quote.input != size
    {
        return Err(Refusal::QuoteBinding);
    }
    Ok((
        *known(&quote.output, "quote output")?,
        provenance(&quote.provenance)?,
    ))
}

pub(super) fn add(to: &mut u64, amount: u64) -> Result<(), Refusal> {
    *to = to.checked_add(amount).ok_or(Refusal::Arithmetic)?;
    Ok(())
}
