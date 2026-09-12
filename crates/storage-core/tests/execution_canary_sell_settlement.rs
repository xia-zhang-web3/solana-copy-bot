#[path = "common/sell_settlement_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_storage_core::*;
use fixture::*;

#[test]
fn sequential_fixture_states_conserve_basis_and_keep_one_raw_open() -> Result<()> {
    // Each Db is an explicitly prepared next inventory state, NOT production apply.
    for (initial_cost, sales, allocations) in [
        (10, [3, 3, 1], [5, 4, 1]),
        (1, [3, 3, 1], [1, 0, 0]),
        (10, [4, 2, 1], [6, 3, 1]),
        (0, [2, 4, 1], [0, 0, 0]),
    ] {
        let (mut raw, mut cost, mut accumulated) = (7, initial_cost, 0);
        let (mut total_sold, mut total_allocated) = (0, 0);
        for (sold, expected_allocation) in sales.into_iter().zip(allocations) {
            let db = Db::new(raw, cost, accumulated, sold, -1)?;
            let before = snapshot(&db.conn()?)?;
            let p = db.ready()?;
            assert_eq!(p.allocated_entry_basis.as_u64(), expected_allocation);
            total_allocated += expected_allocation;
            total_sold += sold;
            raw = p.remaining_quantity.raw();
            cost = i64::try_from(p.remaining_entry_basis.as_u64())?;
            accumulated = i64::try_from(p.accumulated_cash_result.as_i128())?;
            assert_eq!(raw + total_sold, 7);
            assert_eq!(
                u64::try_from(cost)? + total_allocated,
                u64::try_from(initial_cost)?
            );
            assert_eq!(
                p.remaining_position_state,
                if raw == 0 { "closed" } else { "open" }
            );
            assert_eq!(snapshot(&db.conn()?)?, before);
        }
        assert_eq!((raw, cost, accumulated), (0, 0, -3 - initial_cost));
    }
    Ok(())
}

#[test]
fn integer_allocation_above_float_precision_and_at_u64_boundary() -> Result<()> {
    let large = 9_007_199_254_740_993;
    for (raw, cost, sold, allocated) in [
        (3, large, 1, 3_002_399_751_580_331),
        (large, large, large - 1, large - 1),
        (u64::MAX, i64::MAX as u64, 1, 1),
        (u64::MAX, i64::MAX as u64, u64::MAX - 1, i64::MAX as u64),
        (u64::MAX, i64::MAX as u64, u64::MAX, i64::MAX as u64),
        (2, 1, 1, 1),
        (7, 11, 2, 4),
    ] {
        let db = Db::new(raw, i64::try_from(cost)?, 0, sold, 0)?;
        let p = db.ready()?;
        assert_eq!(p.expected_position.quantity.raw(), raw);
        assert_eq!(p.allocated_entry_basis.as_u64(), allocated);
        assert_eq!(p.remaining_entry_basis.as_u64(), cost - allocated);
        assert_eq!(p.remaining_quantity.raw(), raw - sold);
        assert_eq!(p.cash_result_delta.as_i128(), -i128::from(allocated));
    }
    Ok(())
}

#[test]
fn signed_cash_and_accumulation_never_narrow_to_sqlite_integer() -> Result<()> {
    use SettlementSqliteSignedValue::{Fits, OutOfRange};
    let max = i128::from(i64::MAX);
    let min = i128::from(i64::MIN);
    for (cash, cost, prior, expected_delta, expected_total, delta_sql, total_sql) in [
        (0, 0, i64::MAX, 0, max, Fits(0), Fits(i64::MAX)),
        (1, 0, i64::MAX, 1, max + 1, Fits(1), OutOfRange),
        (0, 1, i64::MIN, -1, min - 1, Fits(-1), OutOfRange),
        (-1, 0, i64::MIN + 1, -1, min, Fits(-1), Fits(i64::MIN)),
        (max, 0, 1, max, max + 1, Fits(i64::MAX), OutOfRange),
        (min, 1, 0, min - 1, min - 1, OutOfRange, OutOfRange),
        (
            i128::from(u64::MAX),
            i64::MAX,
            0,
            max + 1,
            max + 1,
            OutOfRange,
            OutOfRange,
        ),
        (
            -i128::from(u64::MAX),
            i64::MAX,
            i64::MIN,
            -i128::from(u64::MAX) - max,
            -i128::from(u64::MAX) - max + min,
            OutOfRange,
            OutOfRange,
        ),
    ] {
        let db = Db::new(1, cost, prior, 1, cash)?;
        let p = db.ready()?;
        assert_eq!(p.wallet_native_cash_delta.as_i128(), cash);
        assert_eq!(p.cash_result_delta.as_i128(), expected_delta);
        assert_eq!(p.accumulated_cash_result.as_i128(), expected_total);
        assert_eq!(p.cash_result_delta_sqlite, delta_sql);
        assert_eq!(p.accumulated_cash_result_sqlite, total_sql);
        assert_eq!(
            p.native_delta_sqlite,
            match i64::try_from(cash) {
                Ok(v) => Fits(v),
                Err(_) => OutOfRange,
            }
        );
    }
    Ok(())
}

#[test]
fn fee_coverage_and_foreign_payer_do_not_adjust_observed_cash() -> Result<()> {
    use copybot_core_types::Lamports;
    for cash in [-900, 0, 300] {
        for (fee, coverage) in [
            (None, ReceiptFeeCoverage::Missing),
            (None, ReceiptFeeCoverage::Invalid),
            (Some(0), ReceiptFeeCoverage::Known),
            (Some(500), ReceiptFeeCoverage::Known),
        ] {
            for payer in [None, Some("wallet"), Some("foreign")] {
                let db = Db::new(7, 10, 0, 3, cash)?;
                // Fixture replacement before planning, not a receipt enrichment test.
                db.conn()?
                    .execute("DELETE FROM execution_canary_receipt_facts", [])?;
                let mut facts = db.facts(3, cash);
                facts.transaction_fee = fee.map(Lamports::new);
                facts.fee_coverage = coverage;
                facts.fee_payer = payer.map(str::to_owned);
                db.store
                    .record_execution_canary_receipt_facts(&facts, db.now)?;
                let before = snapshot(&db.conn()?)?;
                let p = db.ready()?;
                assert_eq!(p.receipt, facts);
                assert_eq!(p.wallet_native_cash_delta.as_i128(), cash);
                assert_eq!(p.cash_result_delta.as_i128(), cash - 5);
                assert_eq!(p.swap_price, None);
                assert_eq!(p.decomposition, ReceiptDecomposition::Unresolved);
                assert_eq!(snapshot(&db.conn()?)?, before);
            }
        }
    }
    Ok(())
}

#[test]
fn full_and_partial_signed_cash_are_exact_without_price_or_writes() -> Result<()> {
    for sold in [3, 7] {
        for cash in [300, 0, -900] {
            let db = Db::new(7, 10, -2, sold, cash)?;
            let before = snapshot(&db.conn()?)?;
            let plan = db.ready()?;
            let allocated = if sold == 7 { 10 } else { 5 };
            assert_eq!(plan.receipt, db.facts(sold, cash));
            assert_eq!(plan.expected_position.quantity.raw(), 7);
            assert_eq!(plan.expected_position.entry_basis.as_u64(), 10);
            assert_eq!(plan.expected_position.accumulated_cash_result.as_i128(), -2);
            assert_eq!(plan.sold_quantity.raw(), sold);
            assert_eq!(plan.remaining_quantity.raw(), 7 - sold);
            assert_eq!(plan.allocated_entry_basis.as_u64(), allocated);
            assert_eq!(plan.remaining_entry_basis.as_u64(), 10 - allocated);
            assert_eq!(plan.wallet_native_cash_delta.as_i128(), cash);
            assert_eq!(
                plan.cash_result_delta.as_i128(),
                cash - i128::from(allocated)
            );
            assert_eq!(
                plan.accumulated_cash_result.as_i128(),
                cash - i128::from(allocated) - 2
            );
            assert_eq!(
                plan.remaining_position_state,
                if sold == 7 { "closed" } else { "open" }
            );
            assert_eq!(plan.swap_price, None);
            assert_eq!(plan.decomposition, ReceiptDecomposition::Unresolved);
            assert_eq!(snapshot(&db.conn()?)?, before);
        }
    }
    Ok(())
}
