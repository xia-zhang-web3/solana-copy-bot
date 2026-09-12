#[path = "common/batch72_wallet.rs"]
mod harness;
#[path = "common/batch72_http.rs"]
mod http;
use anyhow::Result;
use harness::*;
use http::{key, Account, Quote};
fn account(id: u8, mint: u8, raw: u64, response: Quote) -> Account {
    Account {
        id,
        mint,
        raw,
        response,
    }
}
#[test]
fn terminal_and_failed_history_keep_classification_and_unknown() -> Result<()> {
    for unknown in [false, true] {
        let p = run_options(
            &format!("leftover-history-{unknown}"),
            vec![
                account(
                    11,
                    1,
                    100,
                    if unknown {
                        Quote::NoRoute
                    } else {
                        Quote::Known(1_000_000_000)
                    },
                ),
                account(
                    12,
                    2,
                    300,
                    if unknown {
                        Quote::Error
                    } else {
                        Quote::Known(9_000_000_000)
                    },
                ),
            ],
            &[],
            false,
            Options {
                db_mode: "history",
                ..Default::default()
            },
        )?;
        assert_eq!(row(&p, 11)["classification"], "terminal_no_route_leftover");
        assert_eq!(row(&p, 12)["classification"], "failed_sell_leftover");
        assert_eq!(wallet(&p)["terminal_no_route_leftover_count"], 1);
        assert_eq!(wallet(&p)["failed_sell_leftover_count"], 1);
        assert_eq!(row(&p, 11)["sell_failure"]["terminal_no_route_orders"], 1);
        if unknown {
            assert!(p.economics["equity_view"]["terminal_leftover_quote_value_sol"].is_null());
            assert!(p.economics["equity_view"]["wallet_mark_value_sol"].is_null());
        } else {
            close(
                &p.economics["equity_view"]["terminal_leftover_quote_value_sol"],
                10.0,
            );
        }
    }
    Ok(())
}
#[test]
fn manual_dry_run_binding_precedes_value_and_explicit_overrides() -> Result<()> {
    for (name, response, flag, selected_reason) in [
        ("known", Quote::Known(5000), "--token", "selected"),
        (
            "no-route",
            Quote::NoRoute,
            "--allow-no-route-token",
            "selected_no_route_explicit_token",
        ),
        (
            "threshold",
            Quote::ThresholdError,
            "--allow-threshold-error-token",
            "selected_threshold_error_explicit_token",
        ),
    ] {
        for mismatch in [false, true] {
            let p = run_options(
                &format!("manual-{name}-{mismatch}"),
                vec![account(11, 1, if mismatch { 100 } else { 40 }, response)],
                &[(1, 40, 400_000_000)],
                false,
                Options {
                    manual_args: Some(vec![flag.into(), key(1)]),
                    ..Default::default()
                },
            )?;
            assert_eq!(p.manual["selected_positions"], if mismatch { 0 } else { 1 });
            assert_eq!(
                p.manual["candidates"][0]["decision_reason"],
                if mismatch {
                    "quote_binding_unknown"
                } else {
                    selected_reason
                }
            );
            assert_eq!(p.manual["write_offs"], serde_json::json!([]));
            assert_eq!(p.manual["commit"], false);
        }
    }
    for (name, accounts, mode) in [
        (
            "ambiguous",
            vec![
                account(11, 1, 40, Quote::NoRoute),
                account(12, 1, 100, Quote::NoRoute),
            ],
            "",
        ),
        (
            "missing-exact",
            vec![account(11, 1, 40, Quote::NoRoute)],
            "missing_exact",
        ),
    ] {
        let p = run_options(
            &format!("manual-{name}"),
            accounts,
            &[(1, 40, 400_000_000)],
            false,
            Options {
                db_mode: mode,
                manual_args: Some(vec!["--allow-no-route-token".into(), key(1)]),
                ..Default::default()
            },
        )?;
        assert_eq!(p.manual["selected_positions"], 0);
        assert_eq!(
            p.manual["candidates"][0]["decision_reason"],
            "quote_binding_unknown"
        );
    }
    Ok(())
}
#[test]
fn manual_age_value_and_token_guards_remain_active() -> Result<()> {
    for (name, quote, args, reason) in [
        (
            "age",
            Quote::Known(5000),
            vec!["--min-age-minutes".into(), "1000000000".into()],
            "position_too_fresh",
        ),
        (
            "value",
            Quote::Known(2_000_000),
            vec![],
            "quote_value_above_position_guard",
        ),
        (
            "token",
            Quote::Known(5000),
            vec!["--token".into(), key(2)],
            "token_not_requested",
        ),
        (
            "no-route-unapproved",
            Quote::NoRoute,
            vec![],
            "quote_status_no_route",
        ),
    ] {
        let p = run_options(
            &format!("manual-guard-{name}"),
            vec![account(11, 1, 40, quote)],
            &[(1, 40, 400_000_000)],
            false,
            Options {
                manual_args: Some(args),
                ..Default::default()
            },
        )?;
        assert_eq!(p.manual["selected_positions"], 0);
        assert_eq!(p.manual["candidates"][0]["decision_reason"], reason);
    }
    Ok(())
}
