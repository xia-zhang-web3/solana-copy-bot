//! Exact saved COPY08 anchor, not a provider or fresh-market observation.
use super::anchor_tail_pair;
use crate::source::http_recovery::{anchor_evidence_io_tests, identity};
use serde_json::json;
use std::os::unix::fs::PermissionsExt;

#[test]
#[ignore = "requires pinned saved COPY08 pair; no external requests"]
fn copy08_anchor_tail_exact_saved_old_red_then_compatible() {
    let pair = anchor_tail_pair::load();
    let old = std::env::var("COPYBOT_ANCHOR_EXPECT_OLD_RED").as_deref() == Ok("1");
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let matches = anchor_evidence_io_tests::compare(
        Some(dir.path().to_str().unwrap()),
        &pair.grpc,
        &pair.http,
        &pair.raw_http,
    )
    .unwrap();
    assert_eq!(matches, !old, "production complete-anchor predicate");
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("pair-01/manifest.json")).unwrap())
            .unwrap();
    assert_eq!(
        manifest["first_mismatch"], pair.manifest["first_mismatch"],
        "the real raw log difference must remain visible even after compatibility"
    );
    let g = anchor_tail_pair::execution_order(&pair.grpc);
    let h = anchor_tail_pair::execution_order(&pair.http);
    let different = g
        .iter()
        .zip(&h)
        .filter(|(a, b)| !identity::info_equal(a, b))
        .count();
    assert_eq!(different, 1, "generic Info/dedup identity stays strict");
    if old {
        assert_eq!(manifest["comparison"], "MISMATCH");
    } else {
        assert_eq!(manifest["comparison"], "MATCH");
        assert_eq!(manifest["comparison_kind"], "SUCCESSFUL_TERMINAL_UNWIND");
    }
    let value = json!({"kind":"LOCAL_EXACT_SAVED_COPY08_ANCHOR", "old_production_red":old,
        "complete_anchor_matches":matches,"strict_info_mismatches":different,
        "first_raw_difference":manifest["first_mismatch"],"comparison_kind":manifest["comparison_kind"],
        "original_raws_retained":true,"provider_requests":0,"financial_actions":0});
    if let Ok(path) = std::env::var("COPYBOT_ANCHOR_CASE_OUTPUT") {
        std::fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    }
    println!("COPY08_ANCHOR_CASE {value}");
}

fn changed_position(pair: &anchor_tail_pair::Pair) -> usize {
    let changed = anchor_tail_pair::execution_order(&pair.grpc)
        .into_iter()
        .zip(anchor_tail_pair::execution_order(&pair.http))
        .filter(|(a, b)| !identity::info_equal(a, b))
        .map(|(_, b)| b.index)
        .collect::<Vec<_>>();
    assert_eq!(changed.len(), 1);
    pair.http
        .transactions
        .iter()
        .position(|t| t.index == changed[0])
        .unwrap()
}
#[test]
#[ignore = "requires pinned saved COPY08 pair; no external requests"]
fn copy08_anchor_tail_same_execution_hidden_payload_and_metadata_refuse() {
    use identity::AnchorComparison::{Mismatch, SuccessfulTerminalUnwind};
    let pair = anchor_tail_pair::load();
    let p = changed_position(&pair);
    let http_logs = &pair.http.transactions[p]
        .meta
        .as_ref()
        .unwrap()
        .log_messages;
    let closing = http_logs.last().unwrap().clone();
    let foreign = bs58::encode([231; 32]).into_string();
    let tails = [
        vec![format!("Program {foreign} success")],
        vec!["Program log: raydium".into(), closing.clone()],
        vec!["Program data: AA==".into(), closing.clone()],
        vec![format!("Program return: {foreign} AA=="), closing.clone()],
        vec![
            format!("Program {foreign} invoke [2]"),
            format!("Program {foreign} success"),
            closing.clone(),
        ],
        vec![closing.replace("success", "failed: custom program error: 0x1")],
        vec![closing.clone(), closing.clone()],
        vec![],
        vec!["Log truncated".into(), closing.clone()],
        vec![closing.replace(" success", " success ")],
        vec!["unknown runtime payload".into(), closing],
    ];
    for (case, tail) in tails.into_iter().enumerate() {
        let mut http = pair.http.clone();
        let logs = &mut http.transactions[p].meta.as_mut().unwrap().log_messages;
        logs.pop();
        logs.extend(tail);
        assert_eq!(
            identity::block_comparison(&pair.grpc, &http),
            Mismatch,
            "hidden tail case {case}"
        );
    }
    assert_eq!(
        identity::block_comparison(&pair.grpc, &pair.http),
        SuccessfulTerminalUnwind
    );
    for case in 0..13 {
        let mut http = pair.http.clone();
        let tx = &mut http.transactions[p];
        let m = tx.meta.as_mut().unwrap();
        match case {
            0 => m.fee += 1,
            1 => m.log_messages_none = true,
            2 => m.inner_instructions_none = !m.inner_instructions_none,
            3 => m.loaded_writable_addresses.push(vec![1; 32]),
            4 => m.compute_units_consumed = Some(m.compute_units_consumed.unwrap_or(0) + 1),
            5 => m.pre_balances[0] += 1,
            6 => {
                let ui = m
                    .pre_token_balances
                    .iter_mut()
                    .chain(&mut m.post_token_balances)
                    .find_map(|t| t.ui_token_amount.as_mut())
                    .unwrap();
                ui.ui_amount = f64::from_bits(ui.ui_amount.to_bits() ^ 1);
            }
            7 => tx.signature[0] ^= 1,
            8 => tx.index += pair.http.transactions.len() as u64,
            9 => {
                tx.transaction
                    .as_mut()
                    .unwrap()
                    .message
                    .as_mut()
                    .unwrap()
                    .instructions
                    .last_mut()
                    .unwrap()
                    .data[4] ^= 1
            }
            10 => m.inner_instructions[0].instructions[0].data[0] ^= 1,
            11 => {
                let amount = &mut m.post_token_balances[0]
                    .ui_token_amount
                    .as_mut()
                    .unwrap()
                    .amount;
                *amount = (amount.parse::<u64>().unwrap() + 1).to_string();
            }
            12 => m.log_messages[0].push(' '),
            _ => unreachable!(),
        }
        assert_eq!(
            identity::block_comparison(&pair.grpc, &http),
            Mismatch,
            "other field case {case}"
        );
    }
    // A failed execution never receives the new successful-unwind permission,
    // even when both sources agree on the failure bytes.
    let err = pair
        .http
        .transactions
        .iter()
        .find_map(|t| t.meta.as_ref()?.err.clone())
        .unwrap();
    let mut grpc = pair.grpc.clone();
    let mut http = pair.http.clone();
    let index = http.transactions[p].index;
    grpc.transactions
        .iter_mut()
        .find(|t| t.index == index)
        .unwrap()
        .meta
        .as_mut()
        .unwrap()
        .err = Some(err.clone());
    http.transactions[p].meta.as_mut().unwrap().err = Some(err);
    assert_eq!(identity::block_comparison(&grpc, &http), Mismatch);
}

#[test]
#[ignore = "requires pinned saved COPY08 pair; no external requests"]
fn copy08_anchor_tail_stack_and_marker_are_causal() {
    use identity::AnchorComparison::{Mismatch, SuccessfulTerminalUnwind};
    let pair = anchor_tail_pair::load();
    let p = changed_position(&pair);
    let index = pair.http.transactions[p].index;
    let a = bs58::encode([61; 32]).into_string();
    let b = bs58::encode([62; 32]).into_string();
    let prefix = vec![
        format!("Program {a} invoke [1]"),
        format!("Program log: Program {b} success"),
        format!("Program {b} invoke [2]"),
    ];
    let compare = |prefix: Vec<String>, tail: Vec<String>, marker: &str| {
        let mut grpc = pair.grpc.clone();
        let mut http = pair.http.clone();
        let g = grpc
            .transactions
            .iter_mut()
            .find(|t| t.index == index)
            .unwrap()
            .meta
            .as_mut()
            .unwrap();
        g.log_messages = prefix.clone();
        g.log_messages.push(marker.into());
        let h = http.transactions[p].meta.as_mut().unwrap();
        h.log_messages = prefix;
        h.log_messages.extend(tail);
        identity::block_comparison(&grpc, &http)
    };
    let tail = vec![
        format!("Program {b} success"),
        format!("Program {a} success"),
    ];
    assert_eq!(
        compare(prefix.clone(), tail.clone(), "Log truncated"),
        SuccessfulTerminalUnwind
    );
    for marker in [
        "Program log: Log truncated",
        "Log truncated ",
        "log truncated",
        "",
    ] {
        assert_eq!(compare(prefix.clone(), tail.clone(), marker), Mismatch);
    }
    for bad in [
        format!("Program {b} invoke [3]"),
        format!("Program {b} invoke [02]"),
        format!("Program {b} invoke [2]garbage"),
        "Program invalid invoke [2]".into(),
    ] {
        let mut prefix = prefix.clone();
        *prefix.last_mut().unwrap() = bad;
        assert_eq!(compare(prefix, tail.clone(), "Log truncated"), Mismatch);
    }
    let mut duplicate = prefix.clone();
    duplicate.insert(0, "Log truncated".into());
    assert_eq!(compare(duplicate, tail.clone(), "Log truncated"), Mismatch);
    assert_eq!(
        compare(
            prefix.clone(),
            tail.iter().cloned().rev().collect(),
            "Log truncated"
        ),
        Mismatch
    );
    assert_eq!(
        compare(prefix, tail[..1].to_vec(), "Log truncated"),
        Mismatch
    );
}
