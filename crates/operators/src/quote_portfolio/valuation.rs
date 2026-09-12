use super::{Book, CoverageIssue, EstimateBasis, Knowledge, Valuation, ValuationScope};

pub(super) fn value(book: &Book, unresolved: &[CoverageIssue]) -> Valuation {
    let mut missing_marks = Vec::new();
    let mut known_net_marks = 0i128;
    let mut overflow = false;
    let mut assumed = book.assumed_or_synthetic;
    for position in book.positions.values().filter(|p| p.remaining_raw > 0) {
        match &position.mark {
            Some(mark) if mark.raw == position.remaining_raw => {
                assumed |= mark.assumed_or_synthetic;
                match known_net_marks.checked_add(mark.net_lamports) {
                    Some(sum) => known_net_marks = sum,
                    None => overflow = true,
                }
            }
            _ => missing_marks.push(position.id.clone()),
        }
    }
    let equity = i128::from(book.cash_lamports)
        .checked_add(i128::from(book.locked_rent_lamports))
        .and_then(|v| v.checked_add(known_net_marks));
    let full_equity = if !unresolved.is_empty() {
        Knowledge::Unknown("unresolved refused events; committed book subtotal only".into())
    } else if !missing_marks.is_empty() {
        Knowledge::Unknown("missing exact remaining-size marks".into())
    } else if overflow {
        Knowledge::Unknown("mark sum overflow".into())
    } else {
        match equity {
            Some(equity) => Knowledge::Known(equity),
            None => Knowledge::Unknown("equity overflow".into()),
        }
    };
    let net_change = match &full_equity {
        Knowledge::Known(equity) => {
            match equity.checked_sub(i128::from(book.initial_cash_lamports)) {
                Some(net) => Knowledge::Known(net),
                None => Knowledge::Unknown("net change overflow".into()),
            }
        }
        Knowledge::Unknown(reason) => Knowledge::Unknown(reason.clone()),
    };
    Valuation {
        scope: ValuationScope::IndependentExactQuotesAndRentBookValue,
        basis: if assumed {
            EstimateBasis::AssumedOrSyntheticOperands
        } else {
            EstimateBasis::CallerObservedOperands
        },
        cash_subtotal_lamports: book.cash_lamports,
        locked_rent_lamports: book.locked_rent_lamports,
        known_net_marks_lamports: known_net_marks,
        full_equity_lamports: full_equity,
        net_change_lamports: net_change,
        missing_marks,
        unresolved: unresolved.to_vec(),
    }
}
