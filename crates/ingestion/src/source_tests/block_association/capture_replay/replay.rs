use super::*;
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn analyze(capture: &reader::Capture) -> Value {
    let manifest = &capture.manifest;
    let messages = &capture.messages;
    let c = &capture.policy;
    let mut counts = BTreeMap::<&str, u64>::new();
    let mut signatures = BTreeSet::new();
    let mut duplicates = 0;
    let mut unknown_times = 0;
    let mut transactions = vec![];
    for (tx_ns, update) in messages {
        let Some(subscribe_update::UpdateOneof::Transaction(tx)) = &update.update_oneof else {
            continue;
        };
        *counts.entry("raw_transactions").or_default() += 1;
        let signature = tx
            .transaction
            .as_ref()
            .map(|t| bs58::encode(&t.signature).into_string());
        let repeated = !signatures.insert(signature.clone());
        if repeated {
            duplicates += 1;
        }
        let facts = decode_yellowstone_swap_facts(
            tx,
            &c.interested_program_ids,
            &c.raydium_program_ids,
            &c.pumpswap_program_ids,
        );
        if !matches!(facts.facts, Ok(Some(_))) {
            *counts.entry("not_checked_swap").or_default() += 1;
            transactions.push(json!({"status":"not_checked_swap","signature":signature}));
            continue;
        }
        *counts.entry("checked_swap_subset").or_default() += 1;
        let mut assertions = vec![];
        let mut refusals = vec![];
        let mut hashes = BTreeSet::new();
        let mut has_not_found = false;
        let mut has_refusal = false;
        for (block_ns, envelope) in messages {
            let Some(subscribe_update::UpdateOneof::Block(block)) = &envelope.update_oneof else {
                continue;
            };
            if block.slot != tx.slot {
                continue;
            }
            let result = associate_yellowstone_transaction(
                tx,
                block,
                &c.interested_program_ids,
                &c.raydium_program_ids,
                &c.pumpswap_program_ids,
            );
            match &result {
                Ok(value) => {
                    hashes.insert(value.provider_assertion.blockhash.clone());
                    if !matches!(value.block_time, Time::AvailableBlockTime(_)) {
                        unknown_times += 1;
                    }
                    let mut output = result_json(&result);
                    // Only the existing set-valued program_ids are normalized.
                    output["facts"] = canonical(output["facts"].clone());
                    output["arrival_block_minus_tx_ns"] = json!(*block_ns as i64 - *tx_ns as i64);
                    assertions.push(output);
                }
                Err(error) => {
                    if matches!(error, Refusal::NotFoundInMessage) {
                        has_not_found = true;
                    } else {
                        has_refusal = true;
                    }
                    refusals.push(result_json(&result));
                }
            }
        }
        let status = if hashes.len() > 1 {
            "unknown"
        } else if has_refusal {
            "refused"
        } else if !assertions.is_empty() {
            "matched"
        } else {
            "unmatched"
        };
        *counts.entry(status).or_default() += 1;
        transactions.push(json!({"signature":signature,"status":status,"duplicate_envelope":repeated,
            "conflicting_provider_assertions":hashes.len()>1,"assertions":assertions,"refusals":refusals,
            "filtered_absence_seen":has_not_found,"created_at":update.created_at.map(|t|json!({"seconds":t.seconds,"nanos":t.nanos})),
            "canonical_fork":"unknown","timed_event_created":false,"sell_delivered":false}));
    }
    for k in [
        "raw_transactions",
        "checked_swap_subset",
        "matched",
        "unmatched",
        "refused",
        "unknown",
        "not_checked_swap",
    ] {
        counts.entry(k).or_default();
    }
    assert_eq!(
        counts["checked_swap_subset"],
        counts["matched"] + counts["unmatched"] + counts["refused"] + counts["unknown"]
    );
    json!({"session_id":manifest["session_id"],"counts":counts,"transactions":transactions,
        "duplicate_transaction_envelopes":duplicates,"unknown_block_time_assertions":unknown_times,
        "capture_complete":manifest["complete"],
        "capture_stop_reason":manifest["stop_reason"],"decoder_policy":c.description(),
        "ignored_message_kinds":capture.ignored_kinds,"dataset_coverage":"unknown",
        "production_green":false,"program_ids_normalization":"sorted existing set only"})
}
