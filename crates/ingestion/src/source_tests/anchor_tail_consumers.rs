//! Actual consumer projections, not a global log equivalence assumption.
use super::anchor_tail_pair::{execution_order, load};
use crate::source::{
    native_attribution::{self, Attribution},
    yellowstone_facts::decode_yellowstone_swap_facts,
    yellowstone_proto::{extract_program_ids_from_proto, proto_account_keys},
    HeliusWsSource,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use yellowstone_grpc_proto::prelude::{SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo};

// Explicit saved COPY08 bot.TEMPLATE program policy; both subscription lists were
// empty, so the unchanged source constructor selects this Raydium/Pump union.
pub(super) fn policy() -> (HashSet<String>, HashSet<String>, HashSet<String>) {
    let rays = HashSet::from([
        "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".to_owned(),
        "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C".to_owned(),
    ]);
    let pumps = HashSet::from(["pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA".to_owned()]);
    (rays.union(&pumps).cloned().collect(), rays, pumps)
}

fn sorted(set: HashSet<String>) -> Vec<String> {
    let mut out: Vec<_> = set.into_iter().collect();
    out.sort();
    out
}

pub(super) fn projection(info: &SubscribeUpdateTransactionInfo, slot: u64) -> Value {
    let (interest, rays, pumps) = policy();
    let meta = info.meta.as_ref().unwrap();
    let message = info.transaction.as_ref().unwrap().message.as_ref().unwrap();
    let keys = proto_account_keys(message, meta);
    let programs = extract_program_ids_from_proto(message, meta, &keys);
    let dex = HeliusWsSource::detect_dex_hint(&programs, &meta.log_messages, &rays, &pumps);
    let log_programs =
        HeliusWsSource::extract_program_ids(&json!({}), &json!({}), &meta.log_messages);
    let native = match native_attribution::proto::infer(message, meta, &keys[0], &pumps) {
        Attribution::Known(t) => json!({"status":"known","buy":t.buy,"target":t.target,
            "sol_raw":t.sol_raw,"target_raw":t.target_raw,"target_decimals":t.target_decimals}),
        Attribution::Unknown => json!({"status":"unknown"}),
        Attribution::NotApplicable => json!({"status":"not_applicable"}),
    };
    let decoded = decode_yellowstone_swap_facts(
        &SubscribeUpdateTransaction {
            slot,
            transaction: Some(info.clone()),
            ..Default::default()
        },
        &interest,
        &rays,
        &pumps,
    );
    let facts = match decoded.facts {
        Err(e) => json!({"status":"error","reason":format!("{e:#}")}),
        Ok(None) => json!({"status":"refused"}),
        Ok(Some(f)) => json!({"status":"checked","signature":f.signature,"slot":f.slot,
            "signer":f.signer,"token_in":f.token_in,"token_out":f.token_out,
            "amount_in_bits":f.amount_in.to_bits(),"amount_out_bits":f.amount_out.to_bits(),
            "exact_amounts":f.exact_amounts,"program_ids":sorted(f.program_ids.into_iter().collect()),
            "dex_hint":f.dex_hint}),
    };
    let floats = [&meta.pre_token_balances, &meta.post_token_balances].map(|rows| {
        rows.iter()
            .map(|row| row.ui_token_amount.as_ref().map(|a| a.ui_amount.to_bits()))
            .collect::<Vec<_>>()
    });
    json!({"execution_index":info.index,"programs":sorted(programs),"dex_hint":dex,
        "log_programs":sorted(log_programs),"native":native,"facts":facts,
        "used_program_fallback":decoded.used_program_fallback,"token_UI_bits":floats})
}

#[test]
#[ignore = "requires pinned COPY08 anchor pair; LOCAL no transport or clock"]
fn saved_anchor_tail_all_actual_consumers_preserve_semantics() {
    let pair = load();
    let grpc = execution_order(&pair.grpc);
    let http = execution_order(&pair.http);
    assert_eq!(grpc.len(), http.len());
    let mut rows = Vec::new();
    let mut log_differences = Vec::new();
    for (a, b) in grpc.into_iter().zip(http) {
        assert_eq!(a.signature, b.signature);
        let left = projection(a, pair.grpc.slot);
        let right = projection(b, pair.http.slot);
        assert_eq!(
            left, right,
            "actual consumer difference execution_index={}",
            a.index
        );
        if a.meta.as_ref().unwrap().log_messages != b.meta.as_ref().unwrap().log_messages {
            log_differences.push(a.index);
        }
        rows.push(left);
    }
    assert!(
        !log_differences.is_empty(),
        "this case must exercise the actual truncated tail"
    );
    let mut decisions = BTreeMap::<String, usize>::new();
    for row in &rows {
        for branch in ["facts", "native"] {
            *decisions
                .entry(format!(
                    "{branch}:{}",
                    row[branch]["status"].as_str().unwrap()
                ))
                .or_default() += 1;
        }
    }
    let moved = pair
        .grpc
        .transactions
        .iter()
        .enumerate()
        .filter(|(position, info)| *position as u64 != info.index)
        .count();
    let digest = format!("{:x}", Sha256::digest(serde_json::to_vec(&rows).unwrap()));
    let result = json!({"kind":"EXACT_SAVED_NORMALIZER_DECODER_NATIVE_CONSUMER_PROOF",
        "transactions":rows.len(),"log_difference_execution_indices":log_differences,
        "programs_dex_decisions_refusals_native_exact_amounts_UI_bits":"ALL_EQUAL",
        "HTTP_float_sidecar_reproduced_by_actual_normalizer":true,
        "grpc_float_sidecar_restored_before_execution_mapping":true,
        "decoder_and_native_decision_counts":decisions,"grpc_delivery_positions_not_execution_index":moved,
        "actual_consumer_projection_sha256":digest,"rows":rows,
        "slot":pair.manifest["slot"],"provider_requests":0,"freshness_or_trading_PASS":false});
    if let Ok(path) = std::env::var("COPYBOT_ANCHOR_CONSUMER_OUTPUT") {
        let mut out = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .unwrap();
        use std::io::Write;
        serde_json::to_writer_pretty(&mut out, &result).unwrap();
        out.write_all(b"\n").unwrap();
        out.sync_all().unwrap();
    }
    eprintln!("ANCHOR_CONSUMERS transactions={} log_differences={} projection_sha256={digest} real_normalizer=true real_decoder=true real_native_attribution=true provider_requests=0", result["transactions"], result["log_difference_execution_indices"]);
}

#[test]
#[ignore = "requires pinned COPY08 anchor pair; LOCAL mutation only"]
fn saved_anchor_arbitrary_hidden_log_tail_is_semantically_significant() {
    let pair = load();
    let original = execution_order(&pair.http)
        .into_iter()
        .find(|info| {
            info.meta
                .as_ref()
                .unwrap()
                .log_messages
                .last()
                .is_some_and(|s| s.ends_with(" success"))
        })
        .unwrap();
    let mut changed = (*original).clone();
    let program = bs58::encode([0xadu8; 32]).into_string();
    let expected = projection(original, pair.http.slot);
    assert!(!expected["programs"]
        .as_array()
        .unwrap()
        .iter()
        .any(|p| p == &program));
    changed
        .meta
        .as_mut()
        .unwrap()
        .log_messages
        .push(format!("Program {program} success"));
    let changed = projection(&changed, pair.http.slot);
    assert_ne!(expected["programs"], changed["programs"]);
    assert_ne!(expected["log_programs"], changed["log_programs"]);
    // This is why unverified tails cannot be omitted merely because execution fields match.
    assert_ne!(expected, changed);
    let mut altered_block = pair.http.clone();
    let altered = altered_block
        .transactions
        .iter_mut()
        .find(|info| info.index == original.index)
        .unwrap();
    altered
        .meta
        .as_mut()
        .unwrap()
        .log_messages
        .push(format!("Program {program} success"));
    assert!(!crate::source::http_recovery::identity::block_equivalent(
        &pair.grpc,
        &altered_block
    ));

    // A payload tail can change an actual classifier even when every execution
    // field, instruction, raw amount and balance remains untouched.
    let base = execution_order(&pair.http)
        .into_iter()
        .find(|info| projection(info, pair.http.slot)["dex_hint"] != "raydium")
        .unwrap();
    let before = projection(base, pair.http.slot);
    let mut payload_block = pair.http.clone();
    let altered = payload_block
        .transactions
        .iter_mut()
        .find(|info| info.index == base.index)
        .unwrap();
    altered
        .meta
        .as_mut()
        .unwrap()
        .log_messages
        .push("Program log: raydium".into());
    let after = projection(altered, pair.http.slot);
    assert_eq!(after["dex_hint"], "raydium");
    assert_ne!(before["dex_hint"], after["dex_hint"]);
    assert!(!crate::source::http_recovery::identity::block_equivalent(
        &pair.grpc,
        &payload_block
    ));
}
