use super::{
    ata_fixture::{as_parsed, built},
    harness::capture,
};
use anyhow::Result;

#[test]
fn builder_ata_legs_ignore_independent_payment_rent_and_fee() -> Result<()> {
    let mut failures = Vec::new();
    for name in ["temporary", "payment", "rent", "fee_only"] {
        for buy in [true, false] {
            let f = built(name, buy);
            for provider in ["rpc_backfill", "helius_fetch", "yellowstone"] {
                for repr in ["raw", "parsed"] {
                    if provider == "yellowstone" && repr == "parsed" {
                        continue;
                    }
                    let input = if repr == "parsed" {
                        as_parsed(&f)
                    } else {
                        f.clone()
                    };
                    let label = format!("b59-{name}-{}-{repr}", if buy { "buy" } else { "sell" });
                    let ev = capture(&label, &input, provider)?;
                    let sol = &ev[if buy { "amount_in" } else { "amount_out" }];
                    let target = &ev[if buy { "amount_out" } else { "amount_in" }];
                    if sol != if buy { 1.0 } else { 1.2 }
                        || target != 10.0
                        || ev["exact_amounts"].is_null()
                    {
                        failures.push(format!(
                            "{label}-{provider}: {sol}; exact={}",
                            ev["exact_amounts"]
                        ));
                    }
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}
