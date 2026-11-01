//! Saved complete pair03: execution identity agrees; provider time estimates differ.
use super::{complete_anchor, identity};
use prost::Message;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use yellowstone_grpc_proto::prelude::{SubscribeUpdateBlock, TransactionStatusMeta, UiTokenAmount};

const MANIFEST: &str =
    include_str!("../../tests/fixtures/http_estimated_block_time_pair03/manifest.json");
const GRPC: &[u8] =
    include_bytes!("../../tests/fixtures/http_estimated_block_time_pair03/grpc_typed.pb");
const HTTP: &[u8] =
    include_bytes!("../../tests/fixtures/http_estimated_block_time_pair03/http_normalized.pb");
const GRPC_BITS: &[u8] =
    include_bytes!("../../tests/fixtures/http_estimated_block_time_pair03/grpc_ui_amount_bits.bin");
const HTTP_BITS: &[u8] =
    include_bytes!("../../tests/fixtures/http_estimated_block_time_pair03/http_ui_amount_bits.bin");
const HTTP_BODY: &[u8] =
    include_bytes!("../../tests/fixtures/http_estimated_block_time_pair03/http_response.json");

fn checked(name: &str, bytes: &[u8]) {
    let manifest: serde_json::Value = serde_json::from_str(MANIFEST).unwrap();
    assert_eq!(
        manifest["files"][name]["bytes"].as_u64().unwrap(),
        bytes.len() as u64
    );
    assert_eq!(
        manifest["files"][name]["sha256"].as_str().unwrap(),
        format!("{:x}", Sha256::digest(bytes))
    );
}

fn float_bytes(block: &SubscribeUpdateBlock) -> Vec<u8> {
    let mut bytes = Vec::new();
    for (position, info) in block.transactions.iter().enumerate() {
        let Some(meta) = &info.meta else { continue };
        for (side, rows) in [
            (0u8, &meta.pre_token_balances),
            (1, &meta.post_token_balances),
        ] {
            for (row, token) in rows.iter().enumerate() {
                if let Some(amount) = &token.ui_token_amount {
                    bytes.extend_from_slice(&(position as u32).to_le_bytes());
                    bytes.push(side);
                    bytes.extend_from_slice(&(row as u32).to_le_bytes());
                    bytes.extend_from_slice(&amount.ui_amount.to_bits().to_le_bytes());
                }
            }
        }
    }
    bytes
}

fn restore_bits(block: &mut SubscribeUpdateBlock, bytes: &[u8]) {
    assert_eq!(bytes.len() % 17, 0);
    let expected: HashSet<_> = float_bytes(block)
        .chunks_exact(17)
        .map(|r| r[..9].to_vec())
        .collect();
    let mut seen = HashSet::new();
    for record in bytes.chunks_exact(17) {
        assert!(seen.insert(record[..9].to_vec()), "duplicate float record");
        let position = u32::from_le_bytes(record[..4].try_into().unwrap()) as usize;
        let row = u32::from_le_bytes(record[5..9].try_into().unwrap()) as usize;
        let bits = u64::from_le_bytes(record[9..].try_into().unwrap());
        let meta = block.transactions[position].meta.as_mut().unwrap();
        let rows = match record[4] {
            0 => &mut meta.pre_token_balances,
            1 => &mut meta.post_token_balances,
            _ => panic!("invalid float side"),
        };
        rows[row].ui_token_amount.as_mut().unwrap().ui_amount = f64::from_bits(bits);
    }
    assert_eq!(seen, expected, "all known float rows are authoritative");
    assert_eq!(
        float_bytes(block),
        bytes,
        "original vector positions, not execution indexes"
    );
}

pub(crate) fn pair03() -> (SubscribeUpdateBlock, SubscribeUpdateBlock) {
    assert_eq!(
        format!("{:x}", Sha256::digest(MANIFEST.as_bytes())),
        "aa2bf5a6c74177c0eb12e4e0b3289c6fe37606652063f3103d280721d00d3a7a"
    );
    for (name, bytes) in [
        ("grpc_typed.pb", GRPC),
        ("http_normalized.pb", HTTP),
        ("grpc_ui_amount_bits.bin", GRPC_BITS),
        ("http_ui_amount_bits.bin", HTTP_BITS),
    ] {
        checked(name, bytes);
    }
    let mut grpc = SubscribeUpdateBlock::decode(GRPC).unwrap();
    let mut http = SubscribeUpdateBlock::decode(HTTP).unwrap();
    restore_bits(&mut grpc, GRPC_BITS);
    restore_bits(&mut http, HTTP_BITS);
    assert_eq!((grpc.slot, http.slot), (464224115, 464224115));
    assert_eq!(
        (grpc.transactions.len(), http.transactions.len()),
        (486, 486)
    );
    assert_eq!((GRPC_BITS.len() / 17, HTTP_BITS.len() / 17), (2446, 2446));
    (grpc, http)
}

pub(crate) fn pair03_http_body() -> Vec<u8> {
    checked("http_response.json", HTTP_BODY);
    HTTP_BODY.to_vec()
}

fn meta(block: &mut SubscribeUpdateBlock) -> &mut TransactionStatusMeta {
    block
        .transactions
        .iter_mut()
        .find_map(|i| i.meta.as_mut())
        .unwrap()
}

fn amount(block: &mut SubscribeUpdateBlock) -> &mut UiTokenAmount {
    block
        .transactions
        .iter_mut()
        .filter_map(|i| i.meta.as_mut())
        .flat_map(|m| {
            m.pre_token_balances
                .iter_mut()
                .chain(&mut m.post_token_balances)
        })
        .find_map(|t| t.ui_token_amount.as_mut())
        .unwrap()
}

fn mismatch(label: &str, change: impl FnOnce(&mut SubscribeUpdateBlock)) {
    let (grpc, mut http) = pair03();
    change(&mut http);
    assert_eq!(
        identity::block_comparison(&grpc, &http).label(),
        "MISMATCH",
        "{label}"
    );
    assert!(!identity::block_equivalent(&grpc, &http), "{label}");
}

/// Before the production fix this same test reports MISMATCH, so it is the RED witness.
#[test]
fn saved_pair03_complete_info_time_metadata_is_admitted_without_rewriting_times() {
    let (grpc, http) = pair03();
    let times = (grpc.block_time.clone(), http.block_time.clone());
    assert_eq!(times.0.as_ref().unwrap().timestamp, 1793564398);
    assert_eq!(times.1.as_ref().unwrap().timestamp, 1793564397);
    assert!(
        !identity::headers_equal(&grpc, &http),
        "strict header predicate stays strict"
    );
    let mut indexes = HashSet::new();
    for info in &grpc.transactions {
        assert!(indexes.insert(info.index));
        let other = http
            .transactions
            .iter()
            .find(|i| i.index == info.index)
            .unwrap();
        assert!(
            identity::info_equal(info, other),
            "execution index {}",
            info.index
        );
    }
    assert_eq!(indexes.len(), 486);
    assert_eq!(
        identity::block_comparison(&grpc, &http).label(),
        "ESTIMATED_BLOCK_TIME_METADATA"
    );
    assert_eq!(
        identity::block_comparison(&http, &grpc).label(),
        "ESTIMATED_BLOCK_TIME_METADATA"
    );
    assert_eq!((grpc.block_time, http.block_time), times);
}

#[test]
fn saved_pair03_execution_index_order_is_used_after_float_restoration() {
    let (grpc, mut http) = pair03();
    http.transactions.reverse();
    assert_eq!(
        identity::block_comparison(&grpc, &http).label(),
        "ESTIMATED_BLOCK_TIME_METADATA"
    );
}

#[test]
fn metadata_class_has_no_invented_time_tolerance_and_equal_time_stays_exact() {
    let (grpc, mut http) = pair03();
    http.block_time = grpc.block_time.clone();
    assert_eq!(identity::block_comparison(&grpc, &http).label(), "EXACT");
    for timestamp in [0, 1793564398 + 86400] {
        http.block_time.as_mut().unwrap().timestamp = timestamp;
        assert_eq!(
            identity::block_comparison(&grpc, &http).label(),
            "ESTIMATED_BLOCK_TIME_METADATA"
        );
        assert_eq!(http.block_time.as_ref().unwrap().timestamp, timestamp);
    }
}

#[test]
fn saved_pair03_time_metadata_does_not_hide_any_other_header_difference() {
    for (label, change) in [
        (
            "slot",
            (|b: &mut SubscribeUpdateBlock| b.slot += 1) as fn(&mut SubscribeUpdateBlock),
        ),
        ("parent_slot", |b| b.parent_slot += 1),
        ("blockhash", |b| b.blockhash.push('x')),
        ("parent_blockhash", |b| b.parent_blockhash.push('x')),
        ("block_height", |b| {
            b.block_height = if b.block_height.is_some() {
                None
            } else {
                Some(Default::default())
            }
        }),
        ("rewards", |b| {
            b.rewards
                .get_or_insert_default()
                .rewards
                .push(Default::default())
        }),
        ("executed_count", |b| b.executed_transaction_count += 1),
        ("missing_Info", |b| {
            b.transactions.pop();
        }),
        ("duplicate_execution_index", |b| {
            b.transactions[0].index = b.transactions[1].index
        }),
        ("out_of_range_execution_index", |b| {
            b.transactions[0].index = u64::MAX
        }),
    ] {
        mismatch(label, change);
    }
}

#[test]
fn saved_pair03_time_metadata_does_not_hide_info_log_meta_or_float_changes() {
    for (label, change) in [
        (
            "signature",
            (|b: &mut SubscribeUpdateBlock| b.transactions[0].signature[0] ^= 1)
                as fn(&mut SubscribeUpdateBlock),
        ),
        ("vote", |b| {
            b.transactions[0].is_vote = !b.transactions[0].is_vote
        }),
        ("missing_transaction", |b| {
            b.transactions[0].transaction = None
        }),
        ("message", |b| {
            b.transactions[0]
                .transaction
                .as_mut()
                .unwrap()
                .message
                .as_mut()
                .unwrap()
                .recent_blockhash[0] ^= 1
        }),
        ("missing_meta", |b| b.transactions[0].meta = None),
        ("fee", |b| meta(b).fee += 1),
        ("error", |b| {
            let m = meta(b);
            m.err = if m.err.is_some() {
                None
            } else {
                Some(Default::default())
            };
        }),
        ("logs", |b| {
            meta(b)
                .log_messages
                .push("Program log: changed known payload".into())
        }),
        ("log_none", |b| {
            let m = meta(b);
            m.log_messages_none = !m.log_messages_none;
        }),
        ("prebalance", |b| meta(b).pre_balances[0] += 1),
        ("CPI", |b| {
            meta(b).inner_instructions.push(Default::default())
        }),
        ("loaded_address", |b| {
            meta(b).loaded_readonly_addresses.push(vec![42; 32])
        }),
        ("return_none", |b| {
            let m = meta(b);
            m.return_data_none = !m.return_data_none;
        }),
        ("compute", |b| {
            let m = meta(b);
            m.compute_units_consumed = Some(m.compute_units_consumed.unwrap_or(0).wrapping_add(1));
        }),
        ("cost", |b| {
            let m = meta(b);
            m.cost_units = Some(m.cost_units.unwrap_or(0).wrapping_add(1));
        }),
        ("raw_amount", |b| amount(b).amount.push('0')),
        ("IEEE_float_bit", |b| {
            let a = amount(b);
            a.ui_amount = f64::from_bits(a.ui_amount.to_bits() ^ 1);
        }),
    ] {
        mismatch(label, change);
    }
}

#[test]
fn metadata_class_requires_both_known_nonnegative_chrono_domain_times() {
    mismatch("missing_HTTP_time", |b| b.block_time = None);
    mismatch("negative_HTTP_time", |b| {
        b.block_time.as_mut().unwrap().timestamp = -1
    });
    mismatch("out_of_range_HTTP_time", |b| {
        b.block_time.as_mut().unwrap().timestamp = i64::MAX
    });
    let (mut grpc, http) = pair03();
    for time in [None, Some(-1), Some(i64::MAX)] {
        grpc.block_time =
            time.map(|timestamp| yellowstone_grpc_proto::prelude::UnixTimestamp { timestamp });
        assert_eq!(identity::block_comparison(&grpc, &http).label(), "MISMATCH");
    }
}

#[test]
fn authoritative_float_sidecar_retains_signed_zero_and_comparator_rejects_its_change() {
    let (mut grpc, mut http) = pair03();
    amount(&mut grpc).ui_amount = -0.0;
    let bits = float_bytes(&grpc);
    let mut roundtrip = SubscribeUpdateBlock::decode(grpc.encode_to_vec().as_slice()).unwrap();
    restore_bits(&mut roundtrip, &bits);
    assert_eq!(
        amount(&mut roundtrip).ui_amount.to_bits(),
        (-0.0f64).to_bits()
    );
    // Mutate the HTTP Info with the same execution index, not its vector position.
    let selected = grpc
        .transactions
        .iter()
        .find(|i| {
            i.meta.as_ref().is_some_and(|m| {
                m.pre_token_balances
                    .iter()
                    .chain(&m.post_token_balances)
                    .any(|t| {
                        t.ui_token_amount
                            .as_ref()
                            .is_some_and(|a| a.ui_amount.to_bits() == (-0.0f64).to_bits())
                    })
            })
        })
        .unwrap();
    let other = http
        .transactions
        .iter_mut()
        .find(|i| i.index == selected.index)
        .unwrap();
    let other_meta = other.meta.as_mut().unwrap();
    let token = other_meta
        .pre_token_balances
        .iter_mut()
        .chain(&mut other_meta.post_token_balances)
        .find_map(|t| t.ui_token_amount.as_mut())
        .unwrap();
    token.ui_amount = 0.0;
    assert_eq!(
        identity::block_comparison(&roundtrip, &http).label(),
        "MISMATCH"
    );
}

#[test]
fn time_metadata_never_combines_with_truncated_log_completion() {
    let (_, base) = pair03();
    let (http, mut live) = base
        .transactions
        .iter()
        .find_map(|info| {
            let mut http = base.clone();
            http.executed_transaction_count = 1;
            http.transactions = vec![info.clone()];
            http.transactions[0].index = 0;
            let mut live = http.clone();
            live.transactions[0].meta.as_mut()?.log_messages = vec!["Log truncated".into()];
            complete_anchor::eligible(&http, &live).then_some((http, live))
        })
        .expect(
            "saved pair contains a complete successful trace for this LOCAL truncation control",
        );
    assert!(
        complete_anchor::eligible(&http, &live),
        "equal-time existing completion baseline"
    );
    live.block_time.as_mut().unwrap().timestamp += 1;
    assert_eq!(identity::block_comparison(&http, &live).label(), "MISMATCH");
    assert!(!complete_anchor::eligible(&http, &live));
}
