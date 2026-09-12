use super::{
    ata_fixture::{as_parsed, built, ATA},
    harness::{capture, capture_proto, known},
    proto,
};
use anyhow::Result;
use serde_json::{json, Value};
use yellowstone_grpc_proto::prelude::subscribe_update;

fn canonical(buy: bool, parsed: bool) -> Value {
    let mut f = built("temporary", buy);
    if parsed {
        f = as_parsed(&f);
    }
    for ix in f["result"]["transaction"]["message"]["instructions"]
        .as_array_mut()
        .unwrap()
    {
        if parsed && ix["programId"] == ATA {
            let a = &ix["accounts"];
            *ix = json!({"programId":ATA,"parsed":{"type":"createIdempotent","info":{
                "source":a[0],"account":a[1],"wallet":a[2],"mint":a[3],
                "systemProgram":a[4],"tokenProgram":a[5]}}});
        }
        ix["stackHeight"] = json!(1);
    }
    f
}

#[test]
fn ata_top_one_canonical_and_null_compatibility_across_providers() -> Result<()> {
    for buy in [true, false] {
        let absent = built("temporary", buy);
        let baseline = capture(
            &format!("b59r1-canonical-{buy}-absent"),
            &absent,
            "rpc_backfill",
        )?;
        known(&baseline, buy);
        for (provider, parsed) in [
            ("rpc_backfill", false),
            ("rpc_backfill", true),
            ("helius_fetch", false),
            ("helius_fetch", true),
            ("yellowstone", false),
        ] {
            let f = canonical(buy, parsed);
            let label = format!("b59r1-canonical-{buy}-{parsed}-top1");
            assert_eq!(
                capture(&label, &f, provider)?,
                baseline,
                "{label}/{provider}"
            );
        }
        for (provider, parsed) in [("rpc_backfill", false), ("helius_fetch", true)] {
            let mut f = canonical(buy, parsed);
            for ix in f["result"]["transaction"]["message"]["instructions"]
                .as_array_mut()
                .unwrap()
            {
                ix["stackHeight"] = Value::Null;
            }
            assert_eq!(
                capture(&format!("b59r1-canonical-{buy}-null"), &f, provider)?,
                baseline
            );
        }
    }
    Ok(())
}

#[test]
fn ata_each_top_proof_operand_requires_its_own_depth() -> Result<()> {
    for buy in [true, false] {
        // BUY: create/fund/sync/existing-target-create/parent/close.
        // SELL: create/parent/close. Other proof operands always retain depth 1.
        let f = canonical(buy, !buy);
        let provider = if buy { "rpc_backfill" } else { "helius_fetch" };
        let count = if buy { 6 } else { 3 };
        assert_eq!(
            f["result"]["transaction"]["message"]["instructions"]
                .as_array()
                .unwrap()
                .len(),
            count
        );
        for index in 0..count {
            for (label, depth) in [
                ("absent", None),
                ("two", Some(json!(2))),
                ("three", Some(json!(3))),
                ("string", Some(json!("1"))),
            ] {
                let mut input = f.clone();
                let ix = &mut input["result"]["transaction"]["message"]["instructions"][index];
                if let Some(depth) = depth {
                    ix["stackHeight"] = depth;
                } else {
                    ix.as_object_mut().unwrap().remove("stackHeight");
                }
                let label = format!("b59r1-top-{buy}-{index}-{label}");
                let ev = capture(&label, &input, provider)?;
                if label.ends_with("absent") {
                    known(&ev, buy);
                } else {
                    assert!(ev.is_null(), "{label}: {ev}");
                }
            }
        }
    }
    // JSON numeric conversion must not discard malformed explicit metadata.
    for (label, depth) in [
        ("negative", json!(-1)),
        ("fraction", json!(1.5)),
        ("overflow", json!(4294967296u64)),
        ("boolean", json!(true)),
    ] {
        let mut input = canonical(true, true);
        input["result"]["transaction"]["message"]["instructions"][4]["stackHeight"] = depth;
        assert!(capture(&format!("b59r1-top-parent-{label}"), &input, "helius_fetch")?.is_null());
    }
    Ok(())
}

#[test]
fn ata_each_creation_cpi_keeps_direct_depth_in_json_and_actual_protobuf() -> Result<()> {
    for index in 0..4 {
        for (label, depth) in [
            ("null", Value::Null),
            ("one", json!(1)),
            ("three", json!(3)),
            ("string", json!("2")),
        ] {
            // Convert first: the fixture converter supplies some CPI depths itself.
            let mut input = canonical(true, true);
            input["result"]["meta"]["innerInstructions"][0]["instructions"][index]["stackHeight"] =
                depth;
            let ev = capture(
                &format!("b59r1-inner-{index}-{label}"),
                &input,
                "rpc_backfill",
            )?;
            if label == "null" {
                known(&ev, true);
            } else {
                assert!(ev.is_null(), "creation CPI {index}/{label}: {ev}");
            }
        }
        for depth in [None, Some(1), Some(3)] {
            let input = canonical(false, false);
            let mut update = proto::update(&input);
            let Some(subscribe_update::UpdateOneof::Transaction(tx)) = &mut update.update_oneof
            else {
                panic!("fixture transaction missing");
            };
            let groups = &mut tx
                .transaction
                .as_mut()
                .unwrap()
                .meta
                .as_mut()
                .unwrap()
                .inner_instructions;
            assert_eq!(groups[0].index, 0);
            assert_eq!(groups[0].instructions[index].stack_height, Some(2));
            groups[0].instructions[index].stack_height = depth;
            let ev = capture_proto(&format!("b59r1-inner-{index}-pb-{depth:?}"), &input, update)?;
            if depth.is_none() {
                known(&ev, false);
            } else {
                assert!(
                    ev.is_null(),
                    "protobuf creation CPI {index}/{depth:?}: {ev}"
                );
            }
        }
    }
    Ok(())
}
