#![allow(dead_code)]
use copybot_operators::quote_portfolio::*;

pub fn amount(n: u64) -> Lamports {
    Lamports {
        amount: Knowledge::Known(n),
        provenance: Provenance::Synthetic("explicit fixture".into()),
    }
}
pub fn initial(cash: u64, cap: usize) -> InitialState {
    InitialState {
        cash_lamports: cash,
        max_open_positions: cap,
        inventory_raw: Knowledge::Known(0),
        external_transfers_lamports: Knowledge::Known(0),
    }
}
pub fn portfolio(cash: u64, cap: usize) -> Portfolio {
    Portfolio::new(initial(cash, cap)).unwrap()
}
pub fn costs(id: &str, pos: &str, direction: Direction) -> TradeCosts {
    TradeCosts {
        event_id: id.into(),
        position_id: pos.into(),
        direction,
        base: amount(0),
        priority: amount(0),
        setup: amount(0),
        exit: amount(0),
    }
}
pub fn buy(id: &str, pos: &str, seq: u64, mint: u8, input: u64, raw: u64) -> Event {
    Event {
        id: id.into(),
        position_id: pos.into(),
        order: Order {
            sequence: seq,
            unix_ms: seq,
        },
        action: Action::Buy {
            mint: [mint; 32],
            decimals: 0,
            input_lamports: input,
            quote: Knowledge::Known(ExactQuote {
                position_id: pos.into(),
                mint: [mint; 32],
                decimals: 0,
                direction: Direction::Buy,
                input,
                output: Knowledge::Known(raw),
                provenance: Provenance::Synthetic("exact size fixture".into()),
            }),
            costs: costs(id, pos, Direction::Buy),
            rent_deposit: amount(0),
        },
    }
}
pub fn sell(id: &str, pos: &str, seq: u64, mint: u8, raw: u64, gross: u64) -> Event {
    Event {
        id: id.into(),
        position_id: pos.into(),
        order: Order {
            sequence: seq,
            unix_ms: seq,
        },
        action: Action::Sell {
            raw,
            quote: Knowledge::Known(ExactQuote {
                position_id: pos.into(),
                mint: [mint; 32],
                decimals: 0,
                direction: Direction::Sell,
                input: raw,
                output: Knowledge::Known(gross),
                provenance: Provenance::Synthetic("exact size fixture".into()),
            }),
            costs: costs(id, pos, Direction::Sell),
        },
    }
}
pub fn mark(id: &str, pos: &str, seq: u64, mint: u8, raw: u64, gross: u64) -> Event {
    let mut e = sell(id, pos, seq, mint, raw, gross);
    let Action::Sell { quote, costs, .. } = e.action else {
        unreachable!()
    };
    e.action = Action::Mark { quote, costs };
    e
}
pub fn refund(id: &str, pos: &str, seq: u64, deposit: &str, n: u64) -> Event {
    Event {
        id: id.into(),
        position_id: pos.into(),
        order: Order {
            sequence: seq,
            unix_ms: seq,
        },
        action: Action::RentRefund {
            deposit_event_id: deposit.into(),
            amount: amount(n),
        },
    }
}
pub fn event_costs(e: &mut Event) -> &mut TradeCosts {
    match &mut e.action {
        Action::Buy { costs, .. } | Action::Sell { costs, .. } | Action::Mark { costs, .. } => {
            costs
        }
        _ => panic!("trade required"),
    }
}
pub fn event_quote(e: &mut Event) -> &mut Knowledge<ExactQuote> {
    match &mut e.action {
        Action::Buy { quote, .. } | Action::Sell { quote, .. } | Action::Mark { quote, .. } => {
            quote
        }
        _ => panic!("trade required"),
    }
}
pub fn applied(p: &mut Portfolio, e: Event) -> Outcome {
    let out = p.apply(e);
    assert_eq!(out.disposition, Disposition::Applied);
    conservation(p);
    out
}
pub fn refused(p: &mut Portfolio, e: Event, reason: Refusal) -> Outcome {
    let before = p.book().clone();
    let out = p.apply(e);
    assert_eq!(out.disposition, Disposition::Refused(reason));
    assert_eq!(*p.book(), before);
    assert_eq!(out.before, out.after);
    assert_eq!(out.allocated_this_event, Components::default());
    assert!(!out.valuation_after.unresolved.is_empty());
    assert!(matches!(
        p.valuation().full_equity_lamports,
        Knowledge::Unknown(_)
    ));
    conservation(p);
    out
}
pub fn conservation(p: &Portfolio) {
    let b = p.book();
    let f = &b.flows;
    assert_eq!(
        u128::from(b.initial_cash_lamports)
            + u128::from(f.sell_gross)
            + u128::from(f.rent_refunded),
        u128::from(b.cash_lamports)
            + u128::from(f.buy_principal)
            + u128::from(f.expenses)
            + u128::from(f.rent_deposited)
    );
    assert_eq!(b.locked_rent_lamports, f.rent_deposited - f.rent_refunded);
    assert_eq!(
        b.locked_rent_lamports,
        b.positions
            .values()
            .map(|p| p.locked_rent_lamports)
            .sum::<u64>()
    );
    assert_eq!(
        b.open_slots,
        b.positions.values().filter(|p| p.remaining_raw > 0).count()
    );
    for p in b.positions.values() {
        assert!(p.remaining_raw <= p.entry_raw);
        assert_eq!(
            p.entry.principal,
            p.allocated.principal + p.remainder.principal
        );
        assert_eq!(p.entry.base, p.allocated.base + p.remainder.base);
        assert_eq!(
            p.entry.priority,
            p.allocated.priority + p.remainder.priority
        );
        assert_eq!(p.entry.setup, p.allocated.setup + p.remainder.setup);
        assert_eq!(p.entry.exit, p.allocated.exit + p.remainder.exit);
    }
}
pub fn record(name: &str, p: &Portfolio, outcomes: &[Outcome]) {
    if let Ok(dir) = std::env::var("B75_EVIDENCE") {
        let data = serde_json::json!({"case":name,"cash_lamports":p.book().cash_lamports,
            "open_slots":p.book().open_slots,"book":format!("{:#?}",p.book()),
            "valuation":format!("{:#?}",p.valuation()),
            "outcomes":outcomes.iter().map(|o|serde_json::json!({
                "id":o.event.id,"disposition":format!("{:?}",o.disposition),
                "cash_before":o.before.cash_lamports,"cash_after":o.after.cash_lamports,
                "raw_before":o.before.position.as_ref().map(|p|p.remaining_raw),
                "raw_after":o.after.position.as_ref().map(|p|p.remaining_raw),
                "allocated_priority":o.allocated_this_event.priority,
                "remaining_priority":o.after.position.as_ref().map(|p|p.remainder.priority),
                "full_outcome":format!("{:#?}",o)
            })).collect::<Vec<_>>()});
        std::fs::write(
            std::path::Path::new(&dir).join(format!("{name}.json")),
            serde_json::to_vec_pretty(&data).unwrap(),
        )
        .unwrap();
    }
}
