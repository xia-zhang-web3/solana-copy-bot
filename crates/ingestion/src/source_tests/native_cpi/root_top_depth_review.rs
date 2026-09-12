//! Root independent depth-only representation probe; wire/economics stay identical.
//! Wire as a sibling of native_cpi::ata_fixture, harness (one mod declaration).
use super::{
    ata_fixture::{as_parsed, built},
    harness::{capture, capture_proto},
    proto,
};
use anyhow::Result;
use serde_json::{json, Value};
use yellowstone_grpc_proto::prelude::subscribe_update;

fn top_depth(v: &mut Value, depth: u32) {
    for ix in v["result"]["transaction"]["message"]["instructions"]
        .as_array_mut()
        .unwrap()
    {
        ix["stackHeight"] = json!(depth);
    }
}
fn inner_depth(v: &mut Value, depth: u32) {
    for group in v["result"]["meta"]["innerInstructions"]
        .as_array_mut()
        .unwrap()
    {
        for ix in group["instructions"].as_array_mut().unwrap() {
            ix["stackHeight"] = json!(depth);
        }
    }
}
fn exact(ev: &Value, buy: bool) -> bool {
    let sol = if buy { "amount_in" } else { "amount_out" };
    let raw = if buy {
        "amount_in_raw"
    } else {
        "amount_out_raw"
    };
    let other = if buy { "amount_out" } else { "amount_in" };
    !ev.is_null()
        && ev[sol] == if buy { 1.0 } else { 1.2 }
        && ev[other] == 10.0
        && ev["exact_amounts"][raw] == if buy { "1000000000" } else { "1200000000" }
}
#[test]
fn root_ata_top_one_matches_absent_depth_and_protobuf() -> Result<()> {
    let mut failures = Vec::new();
    let mut arms = 0;
    for buy in [true, false] {
        let healthy = built("temporary", buy);
        for provider in ["rpc_backfill", "helius_fetch"] {
            for repr in ["raw", "parsed"] {
                let original = if repr == "raw" {
                    healthy.clone()
                } else {
                    as_parsed(&healthy)
                };
                let prefix = format!("root59-depth-{buy}-{repr}");
                let baseline = capture(&format!("{prefix}-absent"), &original, provider)?;
                arms += 1;
                if !exact(&baseline, buy) {
                    failures.push(format!("{prefix}/{provider}: baseline={baseline}"));
                }
                for (label, top, inner, known) in [
                    ("top1", Some(1), None, true),
                    ("top2", Some(2), None, false),
                    ("inner1", None, Some(1), false),
                    ("inner3", None, Some(3), false),
                ] {
                    let mut input = original.clone();
                    if let Some(d) = top {
                        top_depth(&mut input, d);
                    }
                    if let Some(d) = inner {
                        inner_depth(&mut input, d);
                    }
                    let ev = capture(&format!("{prefix}-{label}"), &input, provider)?;
                    arms += 1;
                    let pass = if known {
                        exact(&ev, buy) && ev == baseline
                    } else {
                        ev.is_null()
                    };
                    eprintln!("ROOT_B59_DEPTH {prefix}/{provider}/{label} expected_known={known} actual_known={} pass={pass}", !ev.is_null());
                    if !pass {
                        failures.push(format!(
                            "{prefix}/{provider}/{label}: expected_known={known}, event={ev}"
                        ));
                    }
                }
            }
        }
        // The real protobuf converter has no top-depth field: explicit top 1 is
        // semantically identical there. The fixture encoder hardcodes inner depth
        // to 2; mutate the actual protobuf afterward before encode/decode.
        let baseline = capture(
            &format!("root59-depth-{buy}-pb-absent"),
            &healthy,
            "yellowstone",
        )?;
        arms += 1;
        if !exact(&baseline, buy) {
            failures.push(format!("pb {buy}: baseline={baseline}"));
        }
        for (label, top, inner, known) in [
            ("top1", Some(1), None, true),
            ("inner1", None, Some(1), false),
            ("inner3", None, Some(3), false),
        ] {
            let mut input = healthy.clone();
            if let Some(d) = top {
                top_depth(&mut input, d);
            }
            if let Some(d) = inner {
                inner_depth(&mut input, d);
            }
            let mut update = proto::update(&input);
            if let Some(depth) = inner {
                let Some(subscribe_update::UpdateOneof::Transaction(tx)) = &mut update.update_oneof
                else {
                    panic!("fixture must contain a transaction");
                };
                for group in &mut tx
                    .transaction
                    .as_mut()
                    .unwrap()
                    .meta
                    .as_mut()
                    .unwrap()
                    .inner_instructions
                {
                    for ix in &mut group.instructions {
                        ix.stack_height = Some(depth);
                    }
                }
            }
            let ev = capture_proto(&format!("root59-depth-{buy}-pb-{label}"), &input, update)?;
            arms += 1;
            let pass = if known {
                exact(&ev, buy) && ev == baseline
            } else {
                ev.is_null()
            };
            eprintln!("ROOT_B59_DEPTH {buy}/yellowstone/{label} expected_known={known} actual_known={} pass={pass}", !ev.is_null());
            if !pass {
                failures.push(format!(
                    "pb {buy}/{label}: expected_known={known}, event={ev}"
                ));
            }
        }
    }
    eprintln!(
        "ROOT_B59_DEPTH completed_arms={arms} failing_arms={}",
        failures.len()
    );
    assert_eq!(arms, 48, "probe inventory unexpectedly changed");
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}
