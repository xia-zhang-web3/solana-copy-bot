use super::*;

#[test]
fn ambiguity_is_independent_of_row_order_decimals_and_quantity_ratios() -> Result<()> {
    for sell in [false, true] {
        for native in [false, true] {
            for decimals in [0, 6, 9] {
                // Above epsilon; below/equal/above 15% of canonical target10;
                // equal target; and gift larger than target by two orders.
                for (n, (raw, scale)) in [(2, 12), (15, 1), (2, 0), (10, 0), (1000, 0)]
                    .into_iter()
                    .enumerate()
                {
                    for reverse in [false, true] {
                        let mut f = fixtures::base(sell, native);
                        fixtures::target_decimals(&mut f, decimals);
                        fixtures::gift(&mut f, sell, raw, scale);
                        if reverse {
                            fixtures::reverse_rows(&mut f);
                        }
                        fixtures::rename(
                            &mut f,
                            &format!("ambiguous-{sell}-{native}-{decimals}-{n}-{reverse}"),
                        );
                        controls::ledger(&f);
                        for provider in PROVIDERS {
                            check(&f, provider, false)?;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn pure_raydium_ambiguity_is_refused_in_both_directions_and_sol_paths() -> Result<()> {
    for sell in [false, true] {
        for native in [false, true] {
            let mut f = fixtures::base(sell, native);
            fixtures::gift(&mut f, sell, 1000, 0);
            fixtures::raydium(&mut f);
            fixtures::rename(&mut f, &format!("ambiguous-ray-{sell}-{native}"));
            controls::ledger(&f);
            for provider in PROVIDERS {
                check(&f, provider, false)?;
            }
        }
    }
    Ok(())
}

fn terminal(provider: &str, sell: bool) -> Result<()> {
    let mut f = fixtures::fallback_trap(sell);
    fixtures::rename(&mut f, &format!("fallback-trap-{sell}"));
    controls::ledger(&f);
    check(&f, provider, false)?;
    Ok(())
}

#[test]
fn terminal_yellowstone_buy() -> Result<()> {
    terminal("yellowstone", false)
}
#[test]
fn terminal_backfill_buy() -> Result<()> {
    terminal("rpc_backfill", false)
}
#[test]
fn terminal_helius_buy() -> Result<()> {
    terminal("helius_fetch", false)
}

#[test]
fn terminal_yellowstone_sell() -> Result<()> {
    terminal("yellowstone", true)
}
#[test]
fn terminal_backfill_sell() -> Result<()> {
    terminal("rpc_backfill", true)
}
#[test]
fn terminal_helius_sell() -> Result<()> {
    terminal("helius_fetch", true)
}
