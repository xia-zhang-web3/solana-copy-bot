use super::{
    fixture::*,
    rpc::{self, CLASSIC, TOKEN_2022},
};
use anyhow::Result;
use copybot_discovery_v2::publish_discovery_v2_status;
use serde_json::json;

const UNKNOWN: &str = "live_portfolio_token_2022_valuation_unknown";
const MALFORMED: &str = "live_portfolio_inventory_malformed";
const UNSUPPORTED: &str = "live_portfolio_inventory_unsupported";
const CONFLICT: &str = "live_portfolio_inventory_conflict";

#[test]
fn token_2022_presence_is_unknown_and_known_capital_controls_publish() -> Result<()> {
    for (label, sol, classic, modern, reason, known, unvalued) in [
        (
            "classic",
            0,
            json!([token(CLASSIC, "10000000")]),
            json!([]),
            None,
            1.0,
            0,
        ),
        (
            "sol-only",
            1_000_000_000,
            json!([]),
            json!([]),
            None,
            0.0,
            0,
        ),
        (
            "empty",
            0,
            json!([]),
            json!([]),
            Some("capital_drained_after_window"),
            0.0,
            0,
        ),
        (
            "raw-zero",
            0,
            json!([token(CLASSIC, "0")]),
            json!([token(TOKEN_2022, "0")]),
            Some("capital_drained_after_window"),
            0.0,
            0,
        ),
        (
            "token2022",
            0,
            json!([]),
            json!([token(TOKEN_2022, "10000000")]),
            Some(UNKNOWN),
            0.0,
            1,
        ),
        (
            "sol-tail",
            1_000_000_000,
            json!([]),
            json!([token(TOKEN_2022, "10000000")]),
            None,
            0.0,
            1,
        ),
        (
            "classic-same-mint-tail",
            0,
            json!([token(CLASSIC, "10000000")]),
            json!([token(TOKEN_2022, "10000000")]),
            None,
            1.0,
            1,
        ),
        (
            "dust",
            0,
            json!([token(CLASSIC, "10000")]),
            json!([]),
            Some("only_dust_positions"),
            0.001,
            0,
        ),
        (
            "dust-tail",
            0,
            json!([token(CLASSIC, "10000")]),
            json!([token(TOKEN_2022, "10000000")]),
            Some(UNKNOWN),
            0.001,
            1,
        ),
    ] {
        let f = Fixture::new()?;
        let rpc = responses(sol, classic, modern);
        let status = f.build(&rpc)?;
        rpc::assert_requests(&rpc.finish(), &rpc::key('A'), 3);
        println!("B29 control={label}");
        assert_outcome(&status, reason, false);
        let row = metric(&status);
        assert_eq!(row["live_inventory"]["contract_version"], 1);
        assert!(
            (row["live_inventory"]["known_classic_value_sol"]
                .as_f64()
                .unwrap()
                - known)
                .abs()
                < 1e-12
        );
        assert_eq!(row["live_inventory"]["unvalued_token_positions"], unvalued);
        assert_eq!(
            row["live_inventory"]["valuation_basis"],
            "classic_observed_price_quality_subtotal"
        );
        let published = publish_discovery_v2_status(
            &f.store,
            status,
            true,
            168,
            copybot_discovery_v2::DiscoveryV2DecisionContext::new(
                &f.discovery,
                &f.shadow,
                &super::options(f.now),
            ),
        );
        assert_eq!(published.is_ok(), reason.is_none());
    }
    Ok(())
}

#[test]
fn malformed_inventory_cannot_disappear_or_bypass_with_sol() -> Result<()> {
    for program in [CLASSIC, TOKEN_2022] {
        for (field, value, reason) in [
            ("/account/data", json!({}), UNSUPPORTED),
            ("/pubkey", json!("invalid-account"), MALFORMED),
            ("/pubkey", json!("1".repeat(33)), MALFORMED),
            ("/pubkey", json!("z".repeat(44)), MALFORMED),
            ("/account/owner", json!(rpc::key('Z')), MALFORMED),
            ("/account/executable", json!(true), MALFORMED),
            ("/account/data/program", json!("unsupported"), UNSUPPORTED),
            ("/account/data/parsed/type", json!("mint"), UNSUPPORTED),
            (
                "/account/data/parsed/info/owner",
                json!(rpc::key('Z')),
                MALFORMED,
            ),
            (
                "/account/data/parsed/info/mint",
                json!("invalid-mint"),
                MALFORMED,
            ),
            (
                "/account/data/parsed/info/state",
                json!("uninitialized"),
                UNSUPPORTED,
            ),
            (
                "/account/data/parsed/info/tokenAmount/amount",
                json!(null),
                MALFORMED,
            ),
        ] {
            let f = Fixture::new()?;
            let mut row = token(program, "10000000");
            *row.pointer_mut(field).unwrap() = value;
            row["account"]["data"]["parsed"]["info"]["tokenAmount"]["uiAmountString"] =
                json!("999999");
            let (classic, modern) = if program == CLASSIC {
                (json!([row]), json!([]))
            } else {
                (json!([]), json!([row]))
            };
            let rpc = responses(1_000_000_000, classic, modern);
            let status = f.build(&rpc)?;
            rpc::assert_requests(
                &rpc.finish(),
                &rpc::key('A'),
                if program == CLASSIC { 2 } else { 3 },
            );
            println!("B29 malformed program={program} field={field}");
            assert_outcome(&status, Some(reason), true);
            assert!(metric(&status)["live_inventory"].is_null());
        }
    }
    Ok(())
}

#[test]
fn canonical_raw_amount_and_decimals_are_required_without_ui_fallback() -> Result<()> {
    let mut cases = Vec::new();
    for raw in [
        json!(""),
        json!("-1"),
        json!("+1"),
        json!("1.1"),
        json!("1e9"),
        json!("18446744073709551616"),
        json!(1),
        json!(null),
    ] {
        cases.push(("amount", raw));
    }
    for decimals in [json!(256), json!(-1), json!(1.5), json!("6"), json!(null)] {
        cases.push(("decimals", decimals));
    }
    for program in [CLASSIC, TOKEN_2022] {
        for (field, value) in &cases {
            let f = Fixture::new()?;
            let mut row = token(program, "0");
            row["account"]["data"]["parsed"]["info"]["tokenAmount"][field] = value.clone();
            row["account"]["data"]["parsed"]["info"]["tokenAmount"]["uiAmountString"] =
                json!("12345");
            if value.is_null() {
                row["account"]["data"]["parsed"]["info"]["tokenAmount"]
                    .as_object_mut()
                    .unwrap()
                    .remove(*field);
            }
            let rpc = responses(
                1_000_000_000,
                if program == CLASSIC {
                    json!([row.clone()])
                } else {
                    json!([])
                },
                if program == TOKEN_2022 {
                    json!([row])
                } else {
                    json!([])
                },
            );
            let status = f.build(&rpc)?;
            rpc::assert_requests(
                &rpc.finish(),
                &rpc::key('A'),
                if program == CLASSIC { 2 } else { 3 },
            );
            println!("B29 raw program={program} field={field} value={value}");
            assert_outcome(&status, Some(MALFORMED), true);
        }
    }
    for decimals in [0, 255] {
        let f = Fixture::new()?;
        let mut row = token(TOKEN_2022, "18446744073709551615");
        row["account"]["data"]["parsed"]["info"]["tokenAmount"]["decimals"] = json!(decimals);
        row["account"]["data"]["parsed"]["info"]["extensions"] =
            json!([{"extension":"unknown_future_extension"}]);
        let rpc = responses(0, json!([]), json!([row]));
        let status = f.build(&rpc)?;
        rpc::assert_requests(&rpc.finish(), &rpc::key('A'), 3);
        assert_outcome(&status, Some(UNKNOWN), false);
    }
    Ok(())
}

#[test]
fn duplicates_do_not_double_count_and_conflicts_are_unavailable() -> Result<()> {
    let original = token(CLASSIC, "10000000");
    for (same_account, expected) in [(true, 1.0), (false, 2.0)] {
        let f = Fixture::new()?;
        let mut second = original.clone();
        if !same_account {
            second["pubkey"] = json!(rpc::key('L'));
        }
        let rpc = responses(0, json!([original.clone(), second]), json!([]));
        let status = f.build(&rpc)?;
        rpc::assert_requests(&rpc.finish(), &rpc::key('A'), 3);
        assert_outcome(&status, None, false);
        assert_eq!(metric(&status)["live_token_value_sol"], expected);
        assert_eq!(metric(&status)["live_token_positions"], 1);
    }
    for (field, value) in [
        ("/account/data/parsed/info/mint", json!(rpc::key('E'))),
        ("/account/data/parsed/info/tokenAmount/amount", json!("20")),
        ("/account/data/parsed/info/tokenAmount/decimals", json!(5)),
        ("/account/data/parsed/info/state", json!("frozen")),
    ] {
        let f = Fixture::new()?;
        let mut second = original.clone();
        *second.pointer_mut(field).unwrap() = value;
        let rpc = responses(1_000_000_000, json!([original.clone(), second]), json!([]));
        let status = f.build(&rpc)?;
        rpc::assert_requests(&rpc.finish(), &rpc::key('A'), 2);
        assert_outcome(&status, Some(CONFLICT), true);
    }
    let f = Fixture::new()?;
    let mut cross = token(TOKEN_2022, "10000000");
    cross["pubkey"] = original["pubkey"].clone();
    let rpc = responses(1_000_000_000, json!([original.clone()]), json!([cross]));
    let status = f.build(&rpc)?;
    rpc::assert_requests(&rpc.finish(), &rpc::key('A'), 3);
    assert_outcome(&status, Some(CONFLICT), true);
    let mut conflict = original.clone();
    conflict["pubkey"] = json!(rpc::key('L'));
    conflict["account"]["data"]["parsed"]["info"]["tokenAmount"]["decimals"] = json!(5);
    let rpc = responses(1_000_000_000, json!([original, conflict]), json!([]));
    let status = f.build(&rpc)?;
    rpc::assert_requests(&rpc.finish(), &rpc::key('A'), 3);
    assert_outcome(&status, Some(CONFLICT), true);
    Ok(())
}

#[test]
fn account_budget_is_shared_across_both_programs() -> Result<()> {
    for count in [2, 3] {
        let mut f = Fixture::new()?;
        f.discovery.live_portfolio_max_token_accounts = 2;
        let mut extra = token(TOKEN_2022, "0");
        extra["pubkey"] = json!(rpc::key('L'));
        let modern = if count == 2 {
            json!([token(TOKEN_2022, "0")])
        } else {
            json!([token(TOKEN_2022, "0"), extra])
        };
        let rpc = responses(1_000_000_000, json!([token(CLASSIC, "0")]), modern);
        let status = f.build(&rpc)?;
        rpc::assert_requests(&rpc.finish(), &rpc::key('A'), 3);
        assert_outcome(
            &status,
            if count == 2 {
                None
            } else {
                Some("live_portfolio_account_budget_exceeded")
            },
            count == 3,
        );
    }
    Ok(())
}

#[test]
fn ui_display_values_cannot_replace_positive_or_zero_raw_inventory() -> Result<()> {
    for (raw, display, reason, value) in [
        ("10000000", "0", None, 1.0),
        ("10000000", "99999999", None, 1.0),
        ("0", "99999999", Some("capital_drained_after_window"), 0.0),
    ] {
        let f = Fixture::new()?;
        let mut row = token(CLASSIC, raw);
        row["account"]["data"]["parsed"]["info"]["tokenAmount"]["uiAmountString"] = json!(display);
        row["account"]["data"]["parsed"]["info"]["tokenAmount"]["uiAmount"] = json!(123456789.0);
        let rpc = responses(0, json!([row]), json!([]));
        let status = f.build(&rpc)?;
        rpc::assert_requests(&rpc.finish(), &rpc::key('A'), 3);
        assert_outcome(&status, reason, false);
        assert_eq!(metric(&status)["live_token_value_sol"], value);
    }
    Ok(())
}
