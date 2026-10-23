//! Exact COPY09 baseline: strict comparison must retain the hidden-payload refusal.
use super::anchor_tail_pair::load;
use crate::source::http_recovery::identity;
use serde_json::json;

#[test]
#[ignore = "requires pinned COPY09 seven-file pair; no provider"]
fn copy09_anchor_complete_saved_old_predicate_red() {
    let pair = load();
    assert!(!identity::block_equivalent(&pair.http, &pair.grpc));
    let differing: Vec<_> = pair
        .http
        .transactions
        .iter()
        .filter(|http| {
            let grpc = pair
                .grpc
                .transactions
                .iter()
                .find(|t| t.index == http.index)
                .unwrap();
            !identity::info_equal(http, grpc)
        })
        .collect();
    assert_eq!(differing.len(), 1);
    let http = differing[0];
    let grpc = pair
        .grpc
        .transactions
        .iter()
        .find(|t| t.index == http.index)
        .unwrap();
    let (g, h) = (
        &grpc.meta.as_ref().unwrap().log_messages,
        &http.meta.as_ref().unwrap().log_messages,
    );
    assert_eq!(g.last().map(String::as_str), Some("Log truncated"));
    assert!(h.starts_with(&g[..g.len() - 1]));
    let proof = json!({"kind":"EXACT_SAVED_COPY09_OLD_PRODUCTION_RED", "slot":pair.grpc.slot,
        "transactions":pair.grpc.transactions.len(),"execution_index":http.index,
        "prefix_lines":g.len()-1,"HTTP_tail_lines":h.len()-(g.len()-1),
        "strict_comparison":"MISMATCH","generic_Info_conflict_retained":true,
        "provider_requests":0,"financial_actions":0,"raw_inputs_preserved":true});
    if let Ok(path) = std::env::var("COPYBOT_ANCHOR_CASE_OUTPUT") {
        std::fs::write(path, serde_json::to_vec_pretty(&proof).unwrap()).unwrap();
    }
    eprintln!("COPY09_OLD_RED {proof}");
}

// These mutations all exercise the new permission on the SAME truncated Info.
fn mismatch_index(pair: &super::anchor_tail_pair::Pair) -> usize {
    pair.http
        .transactions
        .iter()
        .position(|h| {
            let g = pair
                .grpc
                .transactions
                .iter()
                .find(|g| g.index == h.index)
                .unwrap();
            !identity::info_equal(h, g)
        })
        .unwrap()
}
#[test]
#[ignore = "pinned COPY09 pair; LOCAL causal mutations only"]
fn copy09_anchor_complete_directional_identity_and_completeness_refusals() {
    use crate::source::http_recovery::complete_anchor::eligible;
    use yellowstone_grpc_proto::prelude::*;
    let pair = load();
    assert!(eligible(&pair.http, &pair.grpc));
    assert!(
        !eligible(&pair.grpc, &pair.http),
        "directional, never equality"
    );
    assert!(!identity::block_equivalent(&pair.http, &pair.grpc));
    let index = mismatch_index(&pair);
    let mutate = |label: &str, f: fn(&mut SubscribeUpdateTransactionInfo)| {
        let mut h = pair.http.clone();
        f(&mut h.transactions[index]);
        assert!(!eligible(&h, &pair.grpc), "new completion accepted {label}");
    };
    for (label, f) in [
        (
            "fee",
            (|i: &mut SubscribeUpdateTransactionInfo| i.meta.as_mut().unwrap().fee += 1)
                as fn(&mut SubscribeUpdateTransactionInfo),
        ),
        ("status", |i| {
            i.meta.as_mut().unwrap().err = Some(TransactionError { err: vec![1] })
        }),
        ("signature", |i| i.signature[0] ^= 1),
        ("execution_index", |i| i.index = 0),
        ("is_vote", |i| i.is_vote = !i.is_vote),
        ("transaction_message", |i| {
            i.transaction
                .as_mut()
                .unwrap()
                .message
                .as_mut()
                .unwrap()
                .recent_blockhash[0] ^= 1
        }),
        ("CPI", |i| {
            i.meta.as_mut().unwrap().inner_instructions[0].index += 1
        }),
        ("raw_amount", |i| {
            i.meta.as_mut().unwrap().post_token_balances[0]
                .ui_token_amount
                .as_mut()
                .unwrap()
                .amount
                .push('0')
        }),
        ("compute", |i| {
            i.meta.as_mut().unwrap().compute_units_consumed = Some(0)
        }),
        ("cost", |i| i.meta.as_mut().unwrap().cost_units = Some(0)),
        ("loaded", |i| {
            i.meta
                .as_mut()
                .unwrap()
                .loaded_writable_addresses
                .push(vec![42; 32])
        }),
        ("prebalance", |i| {
            i.meta.as_mut().unwrap().pre_balances[0] += 1
        }),
        ("missing_meta", |i| i.meta = None),
        ("log_none", |i| {
            i.meta.as_mut().unwrap().log_messages_none = true
        }),
        ("prefix", |i| {
            i.meta.as_mut().unwrap().log_messages[0].push('x')
        }),
        ("second_truncation", |i| {
            i.meta
                .as_mut()
                .unwrap()
                .log_messages
                .push("Log truncated".into())
        }),
        ("malformed_marker", |i| {
            i.meta
                .as_mut()
                .unwrap()
                .log_messages
                .push("Log truncated extra".into())
        }),
        ("missing_close", |i| {
            i.meta.as_mut().unwrap().log_messages.pop();
        }),
        ("extra_success", |i| {
            let last = i
                .meta
                .as_ref()
                .unwrap()
                .log_messages
                .last()
                .unwrap()
                .clone();
            i.meta.as_mut().unwrap().log_messages.push(last);
        }),
        ("payload_after_close", |i| {
            i.meta
                .as_mut()
                .unwrap()
                .log_messages
                .push("Program log: raydium".into())
        }),
        ("foreign_invoke", |i| {
            i.meta.as_mut().unwrap().log_messages.push(format!(
                "Program {} invoke [1]",
                bs58::encode([0xad; 32]).into_string()
            ))
        }),
    ] {
        mutate(label, f);
    }
    for label in [
        "block_hash",
        "parent_hash",
        "parent_slot",
        "block_time",
        "count",
        "coverage",
    ] {
        let mut h = pair.http.clone();
        match label {
            "block_hash" => h.blockhash.push('x'),
            "parent_hash" => h.parent_blockhash.push('x'),
            "parent_slot" => h.parent_slot -= 1,
            "block_time" => h.block_time.as_mut().unwrap().timestamp += 1,
            "count" => h.executed_transaction_count += 1,
            _ => {
                h.transactions.pop();
            }
        }
        assert!(!eligible(&h, &pair.grpc), "accepted {label}");
    }
    let mut h = pair.http.clone();
    let mut g = pair.grpc.clone();
    let gi = g
        .transactions
        .iter()
        .position(|i| i.index == h.transactions[index].index)
        .unwrap();
    h.transactions[index]
        .meta
        .as_mut()
        .unwrap()
        .pre_token_balances[0]
        .ui_token_amount
        .as_mut()
        .unwrap()
        .ui_amount = 0.0;
    g.transactions[gi].meta.as_mut().unwrap().pre_token_balances[0]
        .ui_token_amount
        .as_mut()
        .unwrap()
        .ui_amount = -0.0;
    assert!(!eligible(&h, &g), "IEEE signed zero cannot disappear");
    // Caller supplied log-only completion remains irrelevant to generic dedup.
    assert!(!identity::info_equal(
        &pair.http.transactions[index],
        &pair.grpc.transactions[gi]
    ));
    let prefix = g.transactions[gi].meta.as_mut().unwrap();
    prefix.log_messages.push("extra".into());
    assert!(
        !eligible(&pair.http, &g),
        "marker must be bare LAST available record"
    );
}
