#[path = "common/batch72_wallet.rs"]
mod harness;
#[path = "common/batch72_http.rs"]
mod http;
use anyhow::Result;
use harness::*;
use http::{Account, Quote};

fn a(id: u8, mint: u8, raw: u64, out: u64) -> Account {
    Account {
        id,
        mint,
        raw,
        response: Quote::Known(out),
    }
}

#[test]
fn red_account_identity_must_survive_rpc_order() -> Result<()> {
    let accounts = vec![a(11, 1, 100, 1_000_000_000), a(12, 1, 300, 9_000_000_000)];
    let forward = run_case("red-identity-forward", accounts.clone(), &[], false)?;
    let reverse = run_case(
        "red-identity-reverse",
        accounts.into_iter().rev().collect(),
        &[],
        false,
    )?;
    same_other_reports(&forward, &reverse);
    assert_eq!(
        row(&forward, 11)["sell_quote"]["out_amount_raw"],
        "1000000000",
        "mint proof replaced account 11's quote for amount 100"
    );
    assert_eq!(
        row(&reverse, 12)["sell_quote"]["out_amount_raw"],
        "9000000000"
    );
    Ok(())
}

#[test]
fn red_unknown_open_value_must_remain_unknown() -> Result<()> {
    let pair = run_case(
        "red-unknown-open",
        vec![Account {
            id: 11,
            mint: 1,
            raw: 100,
            response: Quote::Error,
        }],
        &[(1, 100, 500_000_000)],
        false,
    )?;
    assert!(row(&pair, 11)["sell_quote"]["out_sol"].is_null());
    assert!(
        pair.economics["open_mark_to_quote"]["quoted_value_sol"].is_null(),
        "missing quote became known open value zero"
    );
    Ok(())
}

#[test]
fn own_request_amount_and_unknown_are_order_invariant() -> Result<()> {
    for no_route in [false, true] {
        let mut accounts = vec![a(11, 1, 100, 1_000_000_000), a(12, 1, 300, 9_000_000_000)];
        if no_route {
            accounts[1].response = Quote::NoRoute;
        }
        let f = run_case(&format!("order-{no_route}-f"), accounts.clone(), &[], false)?;
        let r = run_case(
            &format!("order-{no_route}-r"),
            accounts.into_iter().rev().collect(),
            &[],
            false,
        )?;
        same_other_reports(&f, &r);
        assert_eq!(wallet(&f)["balances"], wallet(&r)["balances"]);
        assert_eq!(f.economics["equity_view"], r.economics["equity_view"]);
        close(&row(&f, 11)["sell_quote"]["out_sol"], 1.0);
        for id in [11, 12] {
            let quote = &row(&f, id)["sell_quote"];
            assert_eq!(quote["request"]["token_account"], http::key(id));
            assert_eq!(quote["request"]["amount_raw"], row(&f, id)["amount_raw"]);
            assert_eq!(quote["request"]["owner"], http::key(99));
            assert_eq!(quote["request"]["output_mint"], http::SOL);
        }
        if no_route {
            assert_eq!(row(&f, 12)["sell_quote"]["status"], "no_route");
            assert!(f.economics["equity_view"]["wallet_mark_value_sol"].is_null());
            assert_eq!(wallet(&f)["quote_no_route_count"], 1);
        } else {
            close(&f.economics["equity_view"]["wallet_mark_value_sol"], 15.0);
        }
    }
    Ok(())
}
#[test]
fn bot_remainder_requires_unique_exact_match() -> Result<()> {
    let exact = run_case(
        "bot-exact",
        vec![a(11, 1, 40, 800_000_000)],
        &[(1, 40, 400_000_000)],
        false,
    )?;
    close(
        &exact.economics["open_mark_to_quote"]["quoted_value_sol"],
        0.8,
    );
    close(
        &exact.economics["open_mark_to_quote"]["unrealized_pnl_sol"],
        0.4,
    );
    for (name, accounts, reason) in [
        (
            "wrong-size",
            vec![a(11, 1, 100, 1_000_000_000)],
            "bot_wallet_quantity_mismatch",
        ),
        (
            "additional-account",
            vec![a(11, 1, 40, 800_000_000), a(12, 1, 300, 9_000_000_000)],
            "ambiguous_wallet_accounts",
        ),
        ("unmatched", vec![], "unmatched_bot_position"),
    ] {
        let p = run_case(name, accounts, &[(1, 40, 400_000_000)], false)?;
        unknown_bot(&p);
        assert_eq!(
            wallet(&p)["bot_remainder_mark"]["positions"][0]["binding_reason"],
            reason
        );
        assert_eq!(wallet(&p)["bot_open_position_count"], 1);
        assert_eq!(wallet(&p)["matched_open_position_count"], 0);
        if name == "wrong-size" {
            close(&row(&p, 11)["sell_quote"]["out_sol"], 1.0);
        }
        if name == "additional-account" {
            close(&row(&p, 11)["sell_quote"]["out_sol"], 0.8);
            close(&row(&p, 12)["sell_quote"]["out_sol"], 9.0);
        }
    }
    Ok(())
}
#[test]
fn full_exact_cohort_survives_limit_duplicates_and_missing_quantity() -> Result<()> {
    for mode in [
        "missing_exact",
        "invalid_exact",
        "decimals_mismatch",
        "duplicate_positions",
    ] {
        let positions = if mode == "duplicate_positions" {
            vec![(1, 40, 400_000_000), (1, 20, 200_000_000)]
        } else {
            vec![(1, 40, 400_000_000)]
        };
        let p = run_options(
            mode,
            vec![a(11, 1, 40, 800_000_000)],
            &positions,
            false,
            Options {
                db_mode: mode,
                ..Default::default()
            },
        )?;
        unknown_bot(&p);
        if mode == "duplicate_positions" {
            assert_eq!(wallet(&p)["bot_open_position_count"], 2);
            assert_eq!(wallet(&p)["unmatched_open_position_count"], 2);
        }
        if mode == "invalid_exact" {
            assert_eq!(wallet(&p)["bot_remainder_mark"]["positions_loaded"], false);
        }
    }
    let p = run_options(
        "beyond-limit",
        vec![a(11, 1, 40, 800_000_000)],
        &[(1, 40, 400_000_000), (2, 100, 500_000_000)],
        false,
        Options {
            limit: Some(1),
            ..Default::default()
        },
    )?;
    assert_eq!(wallet(&p)["bot_open_position_count"], 2);
    assert_eq!(
        p.primary["tiny_execution_proof"]["open_positions"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    close(&p.economics["open_mark_to_quote"]["open_cost_sol"], 0.9);
    unknown_bot(&p);
    let full = run_options(
        "full-beyond-limit",
        vec![a(11, 1, 40, 800_000_000), a(12, 2, 100, 1_000_000_000)],
        &[(1, 40, 400_000_000), (2, 100, 500_000_000)],
        false,
        Options {
            limit: Some(1),
            ..Default::default()
        },
    )?;
    assert_eq!(full.economics["open_mark_to_quote"]["open_positions"], 2);
    close(
        &full.economics["open_mark_to_quote"]["quoted_value_sol"],
        1.8,
    );
    close(
        &full.economics["open_mark_to_quote"]["unrealized_pnl_sol"],
        0.9,
    );
    Ok(())
}
#[test]
fn unknown_operands_propagate_and_numeric_zero_is_known() -> Result<()> {
    for (name, response, missing) in [
        ("missing", Quote::Known(1), true),
        ("error", Quote::Error, false),
        ("no-route", Quote::NoRoute, false),
        ("missing-out", Quote::MissingAmount, false),
        ("input-mismatch", Quote::Invalid("inputMint"), false),
        ("output-mismatch", Quote::Invalid("outputMint"), false),
        ("amount-mismatch", Quote::Invalid("inAmount"), false),
        ("input-missing", Quote::Missing("inputMint"), false),
        ("output-missing", Quote::Missing("outputMint"), false),
        ("amount-missing", Quote::Missing("inAmount"), false),
        ("invalid-out", Quote::Invalid("outAmount"), false),
    ] {
        for owned in [false, true] {
            let accounts = vec![
                a(11, 1, 40, 800_000_000),
                Account {
                    id: 12,
                    mint: 2,
                    raw: 100,
                    response,
                },
            ];
            let positions = if owned {
                vec![(1, 40, 400_000_000), (2, 100, 500_000_000)]
            } else {
                vec![]
            };
            let p = run_case(
                &format!("unknown-{name}-{owned}"),
                accounts,
                &positions,
                missing,
            )?;
            assert!(row(&p, 12)["sell_quote"]["out_sol"].is_null());
            assert_eq!(wallet(&p)["wallet_account_mark"]["complete"], false);
            assert!(p.economics["equity_view"]["wallet_mark_value_sol"].is_null());
            if owned {
                unknown_bot(&p);
            } else {
                assert!(p.economics["equity_view"]["untracked_quote_value_sol"].is_null());
            }
            assert_eq!(row(&p, 12)["sell_quote"]["request"]["amount_raw"], "100");
        }
    }
    let zero = run_case(
        "known-zero",
        vec![a(11, 1, 40, 0)],
        &[(1, 40, 400_000_000)],
        false,
    )?;
    close(
        &zero.economics["open_mark_to_quote"]["quoted_value_sol"],
        0.0,
    );
    close(
        &zero.economics["open_mark_to_quote"]["unrealized_pnl_sol"],
        -0.4,
    );
    assert_eq!(wallet(&zero)["near_zero_quote_count"], 1);
    let empty = run_case("known-empty", vec![], &[], false)?;
    close(
        &empty.economics["open_mark_to_quote"]["quoted_value_sol"],
        0.0,
    );
    close(
        &empty.economics["equity_view"]["wallet_mark_value_sol"],
        5.0,
    );
    Ok(())
}
#[test]
fn incomplete_or_malformed_inventory_never_becomes_complete_empty() -> Result<()> {
    for fault in [
        "partial_rpc",
        "invalid_account",
        "invalid_mint",
        "invalid_raw",
        "missing_decimals",
        "wrong_owner",
        "wrong_program",
        "duplicate",
    ] {
        let p = run_options(
            fault,
            vec![a(11, 1, 40, 800_000_000)],
            &[],
            false,
            Options {
                fault,
                ..Default::default()
            },
        )?;
        assert_eq!(wallet(&p)["inventory_complete"], false);
        assert_eq!(wallet(&p)["wallet_account_mark"]["complete"], false);
        assert!(p.economics["equity_view"]["wallet_mark_value_sol"].is_null());
        assert_eq!(wallet(&p)["zero_token_account_count"], 0);
    }
    Ok(())
}
fn unknown_bot(p: &Pair) {
    assert!(p.economics["open_mark_to_quote"]["quoted_value_sol"].is_null());
    assert!(p.economics["open_mark_to_quote"]["unrealized_pnl_sol"].is_null());
    assert!(p.economics["equity_view"]["wallet_mark_value_sol"].is_null());
    assert!(!p.economics["open_mark_to_quote"]["quote_errors"]
        .as_array()
        .unwrap()
        .is_empty());
}
