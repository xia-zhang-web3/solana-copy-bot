//! Root scope probe: unchanged supported persistent-WSOL swap, one target row removed.
//! Wire as a sibling of native_cpi::harness; no production edits.
use super::harness::{capture, fixture};
use anyhow::Result;

#[test]
fn root_b62_existing_target_missing_row_cannot_turn_inventory_into_trade_amount() -> Result<()> {
    let mut failures = Vec::new();
    let mut arms = 0;
    for buy in [true, false] {
        let healthy = fixture("persistent", if buy { "buy" } else { "sell" });
        let owner = healthy["roles"]["user"].as_str().unwrap();
        let target = healthy["roles"]["quote_mint"].as_str().unwrap();
        let field = if buy {
            "preTokenBalances"
        } else {
            "postTokenBalances"
        };
        let original = healthy["result"]["meta"][field].as_array().unwrap();
        let row = original
            .iter()
            .find(|r| r["owner"] == owner && r["mint"] == target)
            .unwrap();
        assert_eq!(row["uiTokenAmount"]["amount"], "7000000");
        let index = row["accountIndex"].as_u64().unwrap() as usize;
        for name in ["preBalances", "postBalances"] {
            assert!(healthy["result"]["meta"][name][index].as_u64().unwrap() > 0);
        }
        let mut damaged = healthy.clone();
        let rows = damaged["result"]["meta"][field].as_array_mut().unwrap();
        rows.retain(|r| !(r["owner"] == owner && r["mint"] == target));
        assert_eq!(rows.len() + 1, original.len());
        for provider in ["rpc_backfill", "helius_fetch", "yellowstone"] {
            let label = format!("root-b62-{buy}");
            let baseline = capture(&format!("{label}-healthy"), &healthy, provider)?;
            assert!(!baseline.is_null());
            let (sol, token, sol_raw, token_raw) = if buy {
                ("amount_in", "amount_out", "amount_in_raw", "amount_out_raw")
            } else {
                ("amount_out", "amount_in", "amount_out_raw", "amount_in_raw")
            };
            // Legacy float subtraction may differ by one ULP; exact raw proof stays strict.
            assert!((baseline[sol].as_f64().unwrap() - if buy { 1.0 } else { 1.2 }).abs() < 1e-12);
            assert_eq!(baseline[token], 10.0);
            assert_eq!(
                baseline["exact_amounts"][sol_raw],
                if buy { "1000000000" } else { "1200000000" }
            );
            assert_eq!(baseline["exact_amounts"][token_raw], "10000000");
            let result = capture(&format!("{label}-missing"), &damaged, provider)?;
            let restored = capture(&format!("{label}-restored"), &healthy, provider)?;
            assert_eq!(restored, baseline, "{provider}/{buy}");
            arms += 1;
            eprintln!("ROOT_B62_MISSING_ROW buy={buy} provider={provider} baseline={baseline} damaged={result}");
            if !result.is_null() {
                failures.push(format!(
                    "{provider}/{buy}: inventory with absent opposite row became swap {result}"
                ));
            }
        }
    }
    assert_eq!(arms, 6);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}
