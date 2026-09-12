use super::fresh_buy_size_fixture::*;
use crate::execution_canary_entry_gate::validate_execution_canary_entry_metadata as gate;
use anyhow::Result;

#[tokio::test]
async fn fresh_buy_size_counterexample_a_blocks_false_entry() -> Result<()> {
    let old = metadata(10_000_000, 100, 0.0);
    let (config, fresh) = refresh(old, 20_000_000, quote("20000000", "180")).await?;
    // Exact ratio = (20m * 100)/(10m * 180) = 10/9; slippage = 10000/9.
    assert_quote(&fresh, 20_000_000, 180, 10_000.0 / 9.0, "would_skip");
    assert_eq!(gate(&config, &fresh), Some("entry_decision_not_execute"));
    Ok(())
}

#[tokio::test]
async fn fresh_buy_size_counterexample_b_allows_same_unit_price() -> Result<()> {
    let old = metadata(20_000_000, 200, 0.0);
    let (config, fresh) = refresh(old, 10_000_000, quote("10000000", "100")).await?;
    // Exact ratio = (10m * 200)/(20m * 100) = 1.
    assert_quote(&fresh, 10_000_000, 100, 0.0, "would_execute");
    assert_eq!(gate(&config, &fresh), None);
    Ok(())
}
