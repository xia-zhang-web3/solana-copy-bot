use super::costs::{add, expenses, lamports, quote};
use super::{Action, AdmissionReason, Book, Components, Direction, Disposition, Event};
use super::{Mark, Position, Refusal};

pub(super) fn apply(book: &mut Book, event: &Event) -> Result<(Disposition, Components), Refusal> {
    let allocation = match &event.action {
        Action::Buy { .. } => return buy(book, event),
        Action::Sell { .. } => sell(book, event)?,
        Action::Mark { .. } => {
            mark(book, event)?;
            Components::default()
        }
        Action::RentRefund { .. } => {
            refund(book, event)?;
            Components::default()
        }
        Action::FailedAttemptExpense { amount } => {
            let (charge, assumed) = lamports(amount, "failed attempt expense")?;
            book.cash_lamports = book
                .cash_lamports
                .checked_sub(charge)
                .ok_or(Refusal::Arithmetic)?;
            add(&mut book.flows.expenses, charge)?;
            book.assumed_or_synthetic |= assumed;
            Components::default()
        }
        Action::UnsupportedExpense { kind, .. } => {
            return Err(Refusal::UnsupportedExpense(kind.clone()));
        }
    };
    Ok((Disposition::Applied, allocation))
}

fn buy(book: &mut Book, event: &Event) -> Result<(Disposition, Components), Refusal> {
    let Action::Buy {
        mint,
        decimals,
        input_lamports,
        quote: supplied,
        costs,
        rent_deposit,
    } = &event.action
    else {
        unreachable!()
    };
    if book.positions.contains_key(&event.position_id) {
        return Err(Refusal::PositionExists);
    }
    if *input_lamports == 0 {
        return Err(Refusal::InvalidRaw);
    }
    let (raw, quote_assumed) = quote(
        event,
        supplied,
        *mint,
        *decimals,
        Direction::Buy,
        *input_lamports,
    )?;
    if raw == 0 {
        return Err(Refusal::InvalidRaw);
    }
    let (mut entry, costs_assumed) = expenses(event, costs, Direction::Buy)?;
    let expense_total = entry.total()?;
    entry.principal = *input_lamports;
    let (rent, rent_assumed) = lamports(rent_deposit, "rent deposit")?;
    let debit = entry
        .total()?
        .checked_add(rent)
        .ok_or(Refusal::Arithmetic)?;
    let mut reasons = Vec::new();
    if book.cash_lamports < debit {
        reasons.push(AdmissionReason::Cash {
            available: book.cash_lamports,
            required: debit,
        });
    }
    if book.open_slots >= book.max_open_positions {
        reasons.push(AdmissionReason::PositionCap {
            open: book.open_slots,
            maximum: book.max_open_positions,
        });
    }
    // Admission used these operands even when this BUY is skipped.
    book.assumed_or_synthetic |= quote_assumed || costs_assumed || rent_assumed;
    if !reasons.is_empty() {
        return Ok((Disposition::Skipped(reasons), Components::default()));
    }
    book.cash_lamports = book
        .cash_lamports
        .checked_sub(debit)
        .ok_or(Refusal::Arithmetic)?;
    book.open_slots = book.open_slots.checked_add(1).ok_or(Refusal::Arithmetic)?;
    add(&mut book.locked_rent_lamports, rent)?;
    add(&mut book.flows.buy_principal, *input_lamports)?;
    add(&mut book.flows.expenses, expense_total)?;
    add(&mut book.flows.rent_deposited, rent)?;
    book.positions.insert(
        event.position_id.clone(),
        Position {
            id: event.position_id.clone(),
            mint: *mint,
            decimals: *decimals,
            entry_event_id: event.id.clone(),
            entry_raw: raw,
            remaining_raw: raw,
            entry,
            allocated: Components::default(),
            remainder: entry,
            locked_rent_lamports: rent,
            mark: None,
        },
    );
    Ok((Disposition::Applied, Components::default()))
}

fn sell(book: &mut Book, event: &Event) -> Result<Components, Refusal> {
    let Action::Sell {
        raw,
        quote: supplied,
        costs,
    } = &event.action
    else {
        unreachable!()
    };
    let position = book
        .positions
        .get_mut(&event.position_id)
        .ok_or(Refusal::PositionMissing)?;
    if *raw == 0 || *raw > position.remaining_raw {
        return Err(Refusal::InvalidRaw);
    }
    let (gross, quote_assumed) = quote(
        event,
        supplied,
        position.mint,
        position.decimals,
        Direction::Sell,
        *raw,
    )?;
    let (expense, costs_assumed) = expenses(event, costs, Direction::Sell)?;
    let expense_total = expense.total()?;
    // Signed net first: gross+cash may exceed u64 transiently while the exact
    // final cash fits. Fees greater than proceeds debit existing cash explicitly.
    let next_cash = i128::from(book.cash_lamports)
        .checked_add(i128::from(gross))
        .and_then(|v| v.checked_sub(i128::from(expense_total)))
        .ok_or(Refusal::Arithmetic)?;
    let remaining = position
        .remaining_raw
        .checked_sub(*raw)
        .ok_or(Refusal::Arithmetic)?;
    let sold = position
        .entry_raw
        .checked_sub(remaining)
        .ok_or(Refusal::Arithmetic)?;
    let allocated = position.entry.cumulative(sold, position.entry_raw)?;
    let delta = allocated.subtract(position.allocated)?;
    let remainder = position.entry.subtract(allocated)?;
    book.cash_lamports = u64::try_from(next_cash).map_err(|_| Refusal::Arithmetic)?;
    add(&mut book.flows.sell_gross, gross)?;
    add(&mut book.flows.expenses, expense_total)?;
    position.remaining_raw = remaining;
    position.allocated = allocated;
    position.remainder = remainder;
    position.mark = None;
    if remaining == 0 {
        book.open_slots = book.open_slots.checked_sub(1).ok_or(Refusal::Arithmetic)?;
    }
    book.assumed_or_synthetic |= quote_assumed || costs_assumed;
    Ok(delta)
}

fn mark(book: &mut Book, event: &Event) -> Result<(), Refusal> {
    let Action::Mark {
        quote: supplied,
        costs,
    } = &event.action
    else {
        unreachable!()
    };
    let position = book
        .positions
        .get_mut(&event.position_id)
        .ok_or(Refusal::PositionMissing)?;
    if position.remaining_raw == 0 {
        return Err(Refusal::InvalidRaw);
    }
    let (gross, a) = quote(
        event,
        supplied,
        position.mint,
        position.decimals,
        Direction::Sell,
        position.remaining_raw,
    )?;
    let (expense, b) = expenses(event, costs, Direction::Sell)?;
    let net_lamports = i128::from(gross)
        .checked_sub(i128::from(expense.total()?))
        .ok_or(Refusal::Arithmetic)?;
    position.mark = Some(Mark {
        event: event.clone(),
        raw: position.remaining_raw,
        net_lamports,
        assumed_or_synthetic: a || b,
    });
    Ok(())
}

fn refund(book: &mut Book, event: &Event) -> Result<(), Refusal> {
    let Action::RentRefund {
        deposit_event_id,
        amount,
    } = &event.action
    else {
        unreachable!()
    };
    let position = book
        .positions
        .get_mut(&event.position_id)
        .ok_or(Refusal::PositionMissing)?;
    if *deposit_event_id != position.entry_event_id {
        return Err(Refusal::RefundBinding);
    }
    let (amount, assumed) = lamports(amount, "rent refund")?;
    position.locked_rent_lamports = position
        .locked_rent_lamports
        .checked_sub(amount)
        .ok_or(Refusal::RentExceeded)?;
    book.locked_rent_lamports = book
        .locked_rent_lamports
        .checked_sub(amount)
        .ok_or(Refusal::Arithmetic)?;
    add(&mut book.cash_lamports, amount)?;
    add(&mut book.flows.rent_refunded, amount)?;
    book.assumed_or_synthetic |= assumed;
    Ok(())
}
