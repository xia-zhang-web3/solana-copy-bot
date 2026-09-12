#[path = "common/tiny_budget_fixture.rs"]
mod fixture;
use anyhow::Result;
use copybot_core_types::TokenQuantity;
use fixture::*;

#[test]
fn tiny_budget_open_or_success_receipt_without_confirmed_fill_cannot_authorize_sell() -> Result<()>
{
    for receipt in [false, true] {
        let d = Db::new()?;
        let buy = d.candidate("buy", "buy")?;
        d.claim(&buy, d.now)?;
        if receipt {
            d.successful(&buy, 5000, d.now)?;
        }
        // Generic import is deliberately not canonical confirmed BUY accounting.
        d.store.record_execution_canary_open_position(
            "unproven",
            "mint",
            7.0,
            Some(TokenQuantity::new(7000, 3)),
            0.01,
            d.now,
        )?;
        d.conn()?.execute(
            "UPDATE positions SET position_id=?1",
            [format!("exec-canary-pos:{}", buy.0.order_id)],
        )?;
        let before = d.totals()?;
        let sell = d.candidate("sell", "sell")?;
        assert!(d.claim(&sell, d.now).is_err());
        assert_eq!(d.totals()?, before);
        assert!(d
            .store
            .load_execution_canary_dispatch(&sell.0.order_id)?
            .is_none());
    }
    Ok(())
}
#[test]
fn tiny_budget_successful_fill_must_keep_original_reservation_identity() -> Result<()> {
    for field in ["tx_signature", "wallet", "position_id"] {
        let d = Db::new()?;
        let buy = d.candidate("buy", "buy")?;
        d.open_buy(&buy, 5000)?;
        if field == "position_id" {
            d.conn()?.execute(
                "UPDATE execution_tiny_experiment SET position_id='foreign-position'",
                [],
            )?;
        } else {
            d.conn()?.execute(
                &format!(
                    "UPDATE execution_tiny_reservations SET {field}='foreign' WHERE side='buy'"
                ),
                [],
            )?;
        }
        let before = d.totals()?;
        let sell = d.candidate("sell", "sell")?;
        assert!(d.claim(&sell, d.now).is_err(), "{field}");
        assert_eq!(d.totals()?, before);
        assert!(d
            .store
            .load_execution_canary_dispatch(&sell.0.order_id)?
            .is_none());
    }
    Ok(())
}
