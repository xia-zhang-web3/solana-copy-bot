use super::*;
use crate::source::yellowstone_association::*;
use crate::source::yellowstone_message_time::{CreatedAtUnavailable, YellowstoneMessageTime};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

struct Run {
    accepted: BTreeSet<ResultId>,
    terminals: BTreeSet<ResultId>,
    cohort: BTreeSet<ResultId>,
    cohort_matched: usize,
    matched: usize,
    unresolved: usize,
    late: usize,
    not_checked: usize,
    transactions: Vec<Value>,
}
impl Run {
    fn new() -> Self {
        Self {
            accepted: BTreeSet::new(),
            terminals: BTreeSet::new(),
            cohort: BTreeSet::new(),
            cohort_matched: 0,
            matched: 0,
            unresolved: 0,
            late: 0,
            not_checked: 0,
            transactions: vec![],
        }
    }
    fn drain(&mut self, adapter: &mut YellowstoneAssociation<'_>, sequence: usize, limits: Limits) {
        loop {
            let batch = adapter.drain();
            assert!(batch.outcomes.len() <= limits.outputs.count);
            assert!(batch.charged_bytes <= limits.outputs.encoded_bytes);
            for out in batch.outcomes {
                match out {
                    Outcome::Terminal {
                        id,
                        checked,
                        resolution,
                        ..
                    } => {
                        assert!(self.accepted.contains(&id));
                        assert!(self.terminals.insert(id), "second financial result {id:?}");
                        let mut row = json!({"id":id.0,"signature":checked.facts.signature,
                            "facts":canonical(facts_json(&checked.facts)),"emitted_at_sequence":sequence,
                            "message_time":format!("{:?}",checked.message_time),
                            "original_offset_ns":checked.context.offset.as_nanos()});
                        match resolution {
                            Resolution::ProviderAsserted {
                                assertion,
                                block_time,
                            } => {
                                self.matched += 1;
                                if self.cohort.contains(&id) {
                                    self.cohort_matched += 1;
                                    assert_eq!(sequence, 142);
                                }
                                row["status"] = json!("matched");
                                row["provider_assertion"] = json!({
                                    "slot":assertion.slot,"blockhash":assertion.blockhash,
                                    "signature":bs58::encode(assertion.signature).into_string(),
                                    "transaction_index":assertion.transaction_index});
                                row["provider_block_time"] = json!(format!("{block_time:?}"));
                            }
                            Resolution::Unresolved(reason) => {
                                self.unresolved += 1;
                                row["status"] = json!("unresolved");
                                row["reason"] = json!(format!("{reason:?}"));
                            }
                        }
                        self.transactions.push(row);
                    }
                    Outcome::Late { id, .. } => {
                        assert!(self.terminals.contains(&id));
                        self.late += 1;
                    }
                }
            }
            if batch.complete {
                break;
            }
        }
    }
}

fn online(capture: &reader::Capture, mutation: &str, limits: Limits) -> Value {
    let session = Session {
        id: [88; 16],
        generation: 1,
    };
    let c = &capture.policy;
    let mut adapter = YellowstoneAssociation::new(
        session,
        limits,
        Programs {
            interested: &c.interested_program_ids,
            raydium: &c.raydium_program_ids,
            pumpswap: &c.pumpswap_program_ids,
        },
    )
    .unwrap();
    let mut run = Run::new();
    let mut raw_transactions = 0;
    let mut last_ns = 0;
    // Strict validated receive order. No block lookup/index or oracle in this loop.
    for (index, (ns, original)) in capture.messages.iter().enumerate() {
        last_ns = *ns;
        // Synthetic variants are disposable protobuf envelopes. Frozen bytes
        // remain untouched; only created_at differs after encode/decode.
        let mut disposable = original.clone();
        match mutation {
            "original" => {}
            "synthetic-removed-created-at" => disposable.created_at = None,
            "synthetic-invalid-created-at" => {
                disposable.created_at = Some(yellowstone_grpc_proto::prost_types::Timestamp {
                    seconds: i64::MAX,
                    nanos: -1,
                })
            }
            _ => panic!("explicit mutation label required"),
        }
        let bytes = disposable.encode_to_vec();
        let disposable = SubscribeUpdate::decode(bytes.as_slice()).unwrap();
        let time = YellowstoneMessageTime::from_created_at(disposable.created_at.as_ref());
        let input = match disposable.update_oneof.as_ref().unwrap() {
            subscribe_update::UpdateOneof::Transaction(tx) => {
                raw_transactions += 1;
                Input::Transaction(tx, time)
            }
            subscribe_update::UpdateOneof::Block(b) => Input::Block(b),
            _ => panic!("reader validates only saved transaction/block"),
        };
        let admission = adapter
            .push(
                Context {
                    session,
                    offset: Duration::from_nanos(*ns),
                },
                input,
            )
            .unwrap();
        match admission {
            Admission::Transaction(id) => {
                assert!(run.accepted.insert(id));
                if raw_transactions <= 64 {
                    run.cohort.insert(id);
                }
            }
            Admission::NotChecked { .. } => run.not_checked += 1,
            Admission::Block => {}
            other => panic!("unexpected frozen capture admission {other:?}"),
        }
        run.drain(&mut adapter, index + 1, limits);
    }
    adapter
        .push(
            Context {
                session,
                offset: Duration::from_nanos(last_ns),
            },
            Input::End,
        )
        .unwrap();
    run.drain(&mut adapter, capture.messages.len() + 1, limits);
    assert_eq!(run.accepted, run.terminals);
    assert_eq!(run.accepted.len(), 587);
    assert_eq!(run.not_checked, 1446);
    assert_eq!(raw_transactions, 2033);
    assert_eq!(run.cohort.len(), 26);
    let expected_message_time = match mutation {
        "synthetic-removed-created-at" => Some(YellowstoneMessageTime::UnresolvedCreatedAt(
            CreatedAtUnavailable::Missing,
        )),
        "synthetic-invalid-created-at" => Some(YellowstoneMessageTime::UnresolvedCreatedAt(
            CreatedAtUnavailable::InvalidNanos(-1),
        )),
        _ => None,
    };
    if let Some(t) = expected_message_time {
        for row in &run.transactions {
            assert_eq!(row["message_time"], format!("{t:?}"));
        }
    }
    json!({"mutation":mutation,"capture_complete":capture.manifest["complete"],
        "matched":run.matched,"unresolved":run.unresolved,"not_checked":run.not_checked,
        "cohort_matched":run.cohort_matched,"checked":run.accepted.len(),"late_notices":run.late,
        "transactions":run.transactions,"canonicality":"unproven","production_green":false})
}

#[test]
#[ignore = "requires frozen capture03; unchanged81/R1 reader validates request/hashes first"]
fn streaming_frozen_capture03() -> Result<()> {
    let capture = reader::read(&env_path("B88_CAPTURE_DIR"))?;
    assert_eq!(capture.manifest["complete"], false);
    let limits = super::super::streaming::limits();
    let original = online(&capture, "original", limits);
    assert_eq!(original["matched"], 583);
    assert_eq!(original["unresolved"], 4);
    assert_eq!(original["cohort_matched"], 26);
    assert_eq!(original["late_notices"], 0);
    for row in original["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["status"] == "unresolved")
    {
        assert_eq!(row["reason"], "EndOfStream");
    }
    // Existing full-set oracle is used only AFTER all online outputs exist.
    let control = replay::analyze(&capture);
    assert_eq!(control["counts"]["matched"], 583);
    assert_eq!(control["counts"]["unmatched"], 4);
    let by_signature: BTreeMap<_, _> = control["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| (r["signature"].as_str().unwrap(), r))
        .collect();
    for row in original["transactions"].as_array().unwrap() {
        let expected = by_signature[row["signature"].as_str().unwrap()];
        if row["status"] == "matched" {
            assert_eq!(row["facts"], expected["assertions"][0]["facts"]);
            assert_eq!(
                row["provider_assertion"],
                expected["assertions"][0]["provider_assertion"]
            );
            assert_eq!(
                row["provider_block_time"],
                expected["assertions"][0]["provider_block_time"]
            );
        } else {
            assert_eq!(expected["status"], "unmatched");
        }
    }
    let mut results = vec![original];
    for mutation in [
        "synthetic-removed-created-at",
        "synthetic-invalid-created-at",
    ] {
        let mutated = online(&capture, mutation, limits);
        let mut normalized = mutated.clone();
        normalized["mutation"] = json!("original");
        for (row, original) in normalized["transactions"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .zip(results[0]["transactions"].as_array().unwrap())
        {
            row["message_time"] = original["message_time"].clone();
        }
        assert_eq!(normalized, results[0]);
        results.push(mutated);
    }
    let mut small = limits;
    small.pending.count = 5;
    let pressure = online(&capture, "original", small);
    assert!(pressure["unresolved"].as_u64().unwrap() > 4);
    assert!(pressure["transactions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["reason"] == "PendingCapacity"));
    results.push(pressure);
    small = limits;
    small.pending_ttl = Duration::from_nanos(1);
    let expired = online(&capture, "original", small);
    assert!(expired["unresolved"].as_u64().unwrap() > 4);
    results.push(expired);
    write_json(
        &env_path("B88_REPLAY_OUT"),
        &json!({"runs":results,"control_counts":control["counts"],
        "input_validation":"unchanged81/R1 reader/request/hash","capture_complete":false,
        "limits":"explicit test-only: 60s pending/block,120s history; small count5,TTL1ns","rollout":false}),
    );
    Ok(())
}
