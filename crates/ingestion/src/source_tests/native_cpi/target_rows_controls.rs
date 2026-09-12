use super::{
    harness::{capture, capture_proto},
    proto,
    target_rows_fixture::{healthy, missing, mutated, target},
};
use anyhow::Result;
use serde_json::{json, Value};
use yellowstone_grpc_proto::prelude::{subscribe_update, SubscribeUpdate, TransactionStatusMeta};

fn metadata(update: &mut SubscribeUpdate) -> &mut TransactionStatusMeta {
    let Some(subscribe_update::UpdateOneof::Transaction(tx)) = &mut update.update_oneof else {
        panic!("transaction fixture required");
    };
    tx.transaction.as_mut().unwrap().meta.as_mut().unwrap()
}

#[test]
fn target_rows_pair_by_index_and_preserve_zero_and_foreign_policy() -> Result<()> {
    for buy in [true, false] {
        for provider in ["rpc_backfill", "helius_fetch", "yellowstone"] {
            let baseline = capture(&format!("b62-pairs-{buy}-healthy"), &healthy(buy), provider)?;
            assert!(!baseline.is_null());
            for case in [
                "reordered",
                "second_pair",
                "foreign_noise",
                "paired_zero",
                "second_pair_missing",
                "duplicate",
                "zero_duplicate",
                "owner_conflict",
                "mint_conflict",
                "empty_owner",
                "empty_mint",
                "index_conflict",
                "native_zero_pre",
                "native_zero_post",
            ] {
                let f = mutated(buy, case);
                let ev = capture(&format!("b62-pairs-{buy}-{case}"), &f, provider)?;
                if ["reordered", "second_pair", "foreign_noise", "paired_zero"].contains(&case) {
                    assert_eq!(ev, baseline, "{buy}/{provider}/{case}");
                } else if case.starts_with("native_zero") {
                    // Deliberately retain the old zero-endpoint exception (17).
                    assert_eq!(ev[if buy { "amount_out" } else { "amount_in" }], 17.0);
                    assert_eq!(
                        ev["exact_amounts"][if buy {
                            "amount_out_raw"
                        } else {
                            "amount_in_raw"
                        }],
                        "17000000"
                    );
                } else {
                    assert!(ev.is_null(), "{buy}/{provider}/{case}: {ev}");
                }
            }
            assert_eq!(
                capture(
                    &format!("b62-pairs-{buy}-restored"),
                    &healthy(buy),
                    provider
                )?,
                baseline
            );
        }
    }
    Ok(())
}

#[test]
fn target_rows_missing_operands_do_not_prove_native_zero() -> Result<()> {
    let mut base = healthy(true);
    missing(&mut base, true);
    let row = target(&base, "postTokenBalances");
    let index = base["result"]["meta"]["postTokenBalances"][row]["accountIndex"]
        .as_u64()
        .unwrap() as usize;
    for case in [
        "index_absent",
        "index_string",
        "index_negative",
        "index_fraction",
        "index_oob",
        "pre_absent",
        "post_absent",
        "pre_short",
        "post_short",
        "pre_null",
        "post_string",
        "pre_negative",
        "post_fraction",
        "zero_but_other_missing",
        "owner_absent",
        "mint_absent",
    ] {
        let mut f = base.clone();
        match case {
            "index_absent" => {
                f["result"]["meta"]["postTokenBalances"][row]
                    .as_object_mut()
                    .unwrap()
                    .remove("accountIndex");
            }
            "index_string" => {
                f["result"]["meta"]["postTokenBalances"][row]["accountIndex"] = json!("4")
            }
            "index_negative" => {
                f["result"]["meta"]["postTokenBalances"][row]["accountIndex"] = json!(-1)
            }
            "index_fraction" => {
                f["result"]["meta"]["postTokenBalances"][row]["accountIndex"] = json!(4.5)
            }
            "index_oob" => {
                f["result"]["meta"]["postTokenBalances"][row]["accountIndex"] = json!(999)
            }
            "pre_absent" => {
                f["result"]["meta"]
                    .as_object_mut()
                    .unwrap()
                    .remove("preBalances");
            }
            "post_absent" => {
                f["result"]["meta"]
                    .as_object_mut()
                    .unwrap()
                    .remove("postBalances");
            }
            "pre_short" => f["result"]["meta"]["preBalances"]
                .as_array_mut()
                .unwrap()
                .truncate(index),
            "post_short" => f["result"]["meta"]["postBalances"]
                .as_array_mut()
                .unwrap()
                .truncate(index),
            "pre_null" => f["result"]["meta"]["preBalances"][index] = Value::Null,
            "post_string" => f["result"]["meta"]["postBalances"][index] = json!("0"),
            "pre_negative" => f["result"]["meta"]["preBalances"][index] = json!(-1),
            "post_fraction" => f["result"]["meta"]["postBalances"][index] = json!(0.5),
            "zero_but_other_missing" => {
                f["result"]["meta"]["preBalances"][index] = json!(0);
                f["result"]["meta"]["postBalances"]
                    .as_array_mut()
                    .unwrap()
                    .truncate(index);
            }
            "owner_absent" | "mint_absent" => {
                // Leave the opposite owned row present so missing identity is relevant.
                f = healthy(true);
                let i = target(&f, "preTokenBalances");
                f["result"]["meta"]["preTokenBalances"][i]
                    .as_object_mut()
                    .unwrap()
                    .remove(if case == "owner_absent" {
                        "owner"
                    } else {
                        "mint"
                    });
            }
            _ => unreachable!(),
        }
        for provider in ["rpc_backfill", "helius_fetch"] {
            assert!(
                capture(&format!("b62-operands-{case}"), &f, provider)?.is_null(),
                "{case}/{provider}"
            );
        }
    }
    for case in [
        "index_oob",
        "pre_short",
        "post_short",
        "zero_but_other_missing",
    ] {
        let mut update = proto::update(&base);
        let meta = metadata(&mut update);
        let row = meta
            .post_token_balances
            .iter_mut()
            .find(|r| r.account_index as usize == index)
            .unwrap();
        match case {
            "index_oob" => row.account_index = u32::MAX,
            "pre_short" => meta.pre_balances.truncate(index),
            "post_short" => meta.post_balances.truncate(index),
            "zero_but_other_missing" => {
                meta.pre_balances[index] = 0;
                meta.post_balances.truncate(index);
            }
            _ => unreachable!(),
        }
        assert!(capture_proto(&format!("b62-operands-pb-{case}"), &base, update)?.is_null());
    }
    Ok(())
}

#[test]
fn target_rows_gate_precedes_attribution_and_legacy_fallbacks() -> Result<()> {
    use crate::source::{yellowstone_proto as pb, HeliusWsSource};
    let healthy = healthy(true);
    let owner = healthy["roles"]["user"].as_str().unwrap();
    let mut damaged = healthy.clone();
    missing(&mut damaged, true);
    let m = &damaged["result"]["meta"];
    assert!(HeliusWsSource::infer_swap_from_json_balances(m, 0, owner).is_none());
    assert!(
        HeliusWsSource::infer_swap_from_json_balances_with_attribution(m, 0, owner, || panic!(
            "incomplete target must be refused before attribution"
        ))
        .is_none()
    );
    let mut update = proto::update(&damaged);
    let m = metadata(&mut update);
    assert!(pb::infer_swap_from_proto_balances(m, 0, owner).is_none());
    assert!(
        pb::infer_swap_from_proto_balances_with_attribution(m, 0, owner, || panic!(
            "incomplete target must be refused before attribution"
        ))
        .is_none()
    );

    // A complete identity pair does not require extra native facts. Existing
    // WSOL inference policy remains in charge after the narrow target gate.
    let mut m = healthy["result"]["meta"].clone();
    m.as_object_mut().unwrap().remove("preBalances");
    m.as_object_mut().unwrap().remove("postBalances");
    let event = HeliusWsSource::infer_swap_from_json_balances(&m, 0, owner).unwrap();
    assert_eq!(event.3.raw_amount.as_deref(), Some("10000000"));
    let mut update = proto::update(&healthy);
    let m = metadata(&mut update);
    m.pre_balances.clear();
    m.post_balances.clear();
    let event = pb::infer_swap_from_proto_balances(m, 0, owner).unwrap();
    assert_eq!(event.3.raw_amount.as_deref(), Some("10000000"));
    Ok(())
}

#[test]
fn target_rows_gate_leaves_legacy_wsol_rows_outside_its_scope() {
    use crate::source::{yellowstone_proto as pb, HeliusWsSource, SOL_MINT};
    let mut f = healthy(false);
    let owner = f["roles"]["user"].as_str().unwrap().to_owned();
    f["result"]["meta"]["preTokenBalances"]
        .as_array_mut()
        .unwrap()
        .retain(|r| !(r["owner"] == owner && r["mint"] == SOL_MINT));
    // This is unchanged legacy policy, not proof that a one-sided WSOL amount is correct.
    let ev =
        HeliusWsSource::infer_swap_from_json_balances(&f["result"]["meta"], 0, &owner).unwrap();
    assert_eq!(ev.1.raw_amount.as_deref(), Some("10000000"));
    assert_eq!(ev.3.raw_amount.as_deref(), Some("2200000000"));
    let mut update = proto::update(&f);
    let ev = pb::infer_swap_from_proto_balances(metadata(&mut update), 0, &owner).unwrap();
    assert_eq!(ev.1.raw_amount.as_deref(), Some("10000000"));
    assert_eq!(ev.3.raw_amount.as_deref(), Some("2200000000"));
}
