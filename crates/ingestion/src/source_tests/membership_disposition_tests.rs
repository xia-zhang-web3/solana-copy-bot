use super::capture_scope_fixture as fixture;
use crate::source::{self, FetchedObservation, RawSwapObservation, YellowstoneParsedUpdate};
use crate::transaction_membership::{self as membership, Disposition};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::process::Command;
use std::time::{Duration, Instant};
use yellowstone_grpc_proto::prelude::*;

fn chosen(n: u8) -> (Vec<u8>, String, u64) {
    let raw = vec![n; 64];
    let sig = bs58::encode(&raw).into_string();
    let slot = (1..100_000)
        .find(|s| membership::selected(&sig, *s))
        .unwrap();
    (raw, sig, slot)
}

fn input(n: u8) -> SubscribeUpdate {
    let (signature, _, slot) = chosen(n);
    let owner = bs58::encode([1; 32]).into_string();
    let mint = bs58::encode([4; 32]).into_string();
    let sol = "So11111111111111111111111111111111111111112";
    let balance = |index, mint: &str, raw: &str, decimals| TokenBalance {
        account_index: index,
        mint: mint.into(),
        owner: owner.clone(),
        program_id: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".into(),
        ui_token_amount: Some(UiTokenAmount {
            amount: raw.into(),
            decimals,
            ui_amount: raw.parse::<f64>().unwrap() / 10f64.powi(decimals as i32),
            ui_amount_string: (raw.parse::<f64>().unwrap() / 10f64.powi(decimals as i32))
                .to_string(),
            ..Default::default()
        }),
        ..Default::default()
    };
    SubscribeUpdate {
        created_at: Some(yellowstone_grpc_proto::prost_types::Timestamp {
            seconds: 1_790_000_000,
            nanos: 0,
        }),
        update_oneof: Some(subscribe_update::UpdateOneof::Transaction(
            SubscribeUpdateTransaction {
                slot,
                transaction: Some(SubscribeUpdateTransactionInfo {
                    signature,
                    transaction: Some(Transaction {
                        message: Some(Message {
                            header: Some(MessageHeader {
                                num_required_signatures: 1,
                                ..Default::default()
                            }),
                            account_keys: vec![
                                vec![1; 32],
                                bs58::decode("CPMMoo8L3F4NbTegBCKVN6DKuQh8fYfY4yR4j3uP9s5")
                                    .into_vec()
                                    .unwrap(),
                                vec![2; 32],
                                vec![3; 32],
                            ],
                            instructions: vec![CompiledInstruction {
                                program_id_index: 1,
                                accounts: vec![0, 2, 3],
                                data: vec![],
                            }],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    meta: Some(TransactionStatusMeta {
                        fee: 5000,
                        pre_balances: vec![1_000_000_000, 0, 0, 0],
                        post_balances: vec![999_995_000, 0, 0, 0],
                        pre_token_balances: vec![
                            balance(2, sol, "1000000000", 9),
                            balance(3, &mint, "0", 6),
                        ],
                        post_token_balances: vec![
                            balance(2, sol, "500000000", 9),
                            balance(3, &mint, "10000000", 6),
                        ],
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

fn observation(n: u8) -> RawSwapObservation {
    let update = input(n);
    let runtime = fixture::runtime(None);
    let debug_update = update.clone();
    let Some(YellowstoneParsedUpdate::Observation(raw)) =
        source::yellowstone::parse_yellowstone_update(update, &runtime).unwrap()
    else {
        let Some(subscribe_update::UpdateOneof::Transaction(tx)) =
            debug_update.update_oneof.as_ref()
        else {
            panic!()
        };
        let decoded = source::yellowstone_facts::decode_yellowstone_swap_facts(
            tx,
            &runtime.interested_program_ids,
            &runtime.raydium_program_ids,
            &runtime.pumpswap_program_ids,
        );
        panic!(
            "finite fixture refused: miss={:?} facts={:?}",
            decoded.miss, decoded.facts
        )
    };
    raw
}

fn note(n: u8) {
    membership::decoded_update(&input(n));
}

#[test]
fn membership_disposition_actual_source_paths_on_off() {
    let folder = tempfile::tempdir().unwrap();
    let child = format!("{}::child", module_path!().split_once("::").unwrap().1);
    let mut reports = Vec::new();
    for enabled in [false, true] {
        let path = folder.path().join(format!("{enabled}.json"));
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", &child, "--nocapture"])
            .env("COPYBOT_MEMBERSHIP_CHILD_REPORT", &path)
            .env_remove(membership::ENV);
        if enabled {
            command.env(membership::ENV, "1");
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        reports.push(serde_json::from_slice::<Value>(&std::fs::read(path).unwrap()).unwrap());
    }
    assert_eq!(reports[0]["business"], reports[1]["business"]);
    assert_eq!(reports[0]["snapshot"]["enabled"], false);
    let snapshot = &reports[1]["snapshot"];
    let rows = snapshot["records"].as_array().unwrap();
    let count = |n: u8, outcome: Disposition| {
        let (_, sig, _) = chosen(n);
        let row = rows.iter().find(|row| row["signature"] == sig).unwrap();
        row["outcomes"][outcome.code().to_string()]
            .as_u64()
            .unwrap_or(0)
    };
    for (n, outcome) in [
        (1, Disposition::CreatedAtUnavailable),
        (2, Disposition::ParseVote),
        (3, Disposition::ParseError),
        (4, Disposition::SwapParserRefused),
        (5, Disposition::OutputReplaced),
        (7, Disposition::OutputClosed),
        (8, Disposition::SourceDedup),
        (9, Disposition::ContextProtectedCapture),
    ] {
        assert_eq!(count(n, outcome), 1, "case {n}");
    }
    assert_eq!(count(6, Disposition::OutputReplaced), 0);
    let (_, sig, _) = chosen(8);
    assert_eq!(
        rows.iter().find(|row| row["signature"] == sig).unwrap()["pre_raw_resolution"],
        2
    );
    assert_eq!(snapshot["complete"], true);
    assert_eq!(snapshot["pre_raw_complete"], false);
}

#[test]
fn child() {
    let Ok(report) = std::env::var("COPYBOT_MEMBERSHIP_CHILD_REPORT") else {
        return;
    };
    membership::initialize_from_env();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let business = runtime.block_on(async {
        let runtime = fixture::runtime(None);
        let mut missing = input(1);
        missing.created_at = None;
        membership::decoded_update(&missing);
        assert!(
            source::yellowstone::parse_yellowstone_update(missing, &runtime)
                .unwrap()
                .is_none()
        );
        let mut vote = input(2);
        if let Some(subscribe_update::UpdateOneof::Transaction(tx)) = vote.update_oneof.as_mut() {
            tx.transaction.as_mut().unwrap().is_vote = true;
        }
        membership::decoded_update(&vote);
        assert!(
            source::yellowstone::parse_yellowstone_update(vote, &runtime)
                .unwrap()
                .is_none()
        );
        let mut error = input(3);
        if let Some(subscribe_update::UpdateOneof::Transaction(tx)) = error.update_oneof.as_mut() {
            tx.transaction.as_mut().unwrap().meta = None;
        }
        membership::decoded_update(&error);
        assert!(source::yellowstone::parse_yellowstone_update(error, &runtime).is_err());
        note(4);
        let mut invalid = observation(4);
        invalid.amount_in = 0.;
        assert!(crate::parser::SwapParser::new(vec![], vec![])
            .parse(invalid)
            .is_none());
        let queue = source::queue::OverflowQueue::new(1);
        for n in [5, 6] {
            note(n);
            queue
                .push_observed(
                    FetchedObservation {
                        raw: observation(n),
                        arrival_seq: n as u64,
                        fetch_latency_ms: 0,
                        enqueued_at: Instant::now(),
                    },
                    source::queue::QueueOverflowPolicy::DropOldest,
                    source::transaction_membership::output_discarded,
                )
                .await
                .unwrap();
        }
        let retained = queue.pop().await.unwrap().raw.signature;
        queue.close().await;
        note(7);
        assert!(queue
            .push_observed(
                FetchedObservation {
                    raw: observation(7),
                    arrival_seq: 7,
                    fetch_latency_ms: 0,
                    enqueued_at: Instant::now()
                },
                source::queue::QueueOverflowPolicy::DropOldest,
                source::transaction_membership::output_discarded
            )
            .await
            .is_none());
        note(8);
        note(8);
        let raw = observation(8);
        let now = Instant::now();
        let seen = HashMap::from([(raw.signature.clone(), now)]);
        assert!(source::transaction_membership::signature_is_seen(
            &seen,
            &raw,
            Duration::from_secs(1),
            now
        ));
        assert!(!source::transaction_membership::signature_is_seen(
            &seen,
            &raw,
            Duration::from_secs(1),
            now + Duration::from_secs(2)
        ));
        let folder = tempfile::tempdir().unwrap();
        let dbpath = folder.path().join("capture.db");
        let db = fixture::create(&dbpath, 100);
        let raw = observation(9);
        fixture::request(&db, &[raw.signer.clone()]);
        let protected = fixture::runtime(Some(&dbpath));
        source::scoped_capture::restore(protected.clone())
            .await
            .unwrap();
        source::scoped_capture::refresh(protected.clone())
            .await
            .unwrap();
        note(9);
        source::scoped_capture::process(input(9), protected)
            .await
            .unwrap()
            .unwrap();
        let stage: String = db
            .query_row("SELECT stage FROM capture_events", [], |r| r.get(0))
            .unwrap();
        json!({"parser_refusals":3,"swap_parser_refusals":1,"retained_signature":retained,
            "source_dedup":[true,false],"protected_stage":stage})
    });
    membership::finish();
    let snapshot: Value = serde_json::from_str(&membership::snapshot_json()).unwrap();
    std::fs::write(
        report,
        json!({"business":business,"snapshot":snapshot}).to_string(),
    )
    .unwrap();
}
