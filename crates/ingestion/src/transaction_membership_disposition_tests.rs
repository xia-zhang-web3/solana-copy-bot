use super::*;

fn chosen(n: u16) -> (String, u64) {
    let mut raw = [9; 64];
    raw[..2].copy_from_slice(&n.to_le_bytes());
    let signature = bs58::encode(raw).into_string();
    let slot = (1..100_000).find(|s| selected(&signature, *s)).unwrap();
    (signature, slot)
}

#[test]
fn membership_disposition_repeated_unknown_and_unresolved_are_not_overwritten() {
    let c = Capture::new(true, 8);
    let (signature, slot) = chosen(1);
    c.decoded(&signature, slot);
    c.observe_disposition(&signature, slot, Disposition::RawInserted);
    c.decoded(&signature, slot);
    c.observe_disposition(&signature, slot, Disposition::SourceDedup);
    let (pending, pending_slot) = chosen(2);
    c.decoded(&pending, pending_slot);
    c.observe_disposition(&pending, pending_slot, Disposition::Buffered);
    let (unknown, unknown_slot) = chosen(3);
    c.decoded(&unknown, unknown_slot);
    c.observe_disposition(&unknown, unknown_slot, Disposition::RawUnknown);
    c.observe_disposition(&unknown, unknown_slot, Disposition::RawInserted);
    c.close();
    let v = c.snapshot();
    let rows = v["records"].as_array().unwrap();
    let row = |id: &str| rows.iter().find(|r| r["signature"] == id).unwrap();
    assert_eq!(row(&signature)["pre_raw_resolution"], 2);
    assert_eq!(
        row(&signature)["outcomes"][Disposition::RawInserted.code().to_string()],
        1
    );
    assert_eq!(
        row(&signature)["outcomes"][Disposition::SourceDedup.code().to_string()],
        1
    );
    assert_eq!(row(&pending)["pre_raw_resolution"], 0);
    assert_eq!(row(&unknown)["pre_raw_resolution"], 3);
    assert_eq!(v["pre_raw_complete"], false);
    assert_eq!(v["complete"], false);
    assert_eq!(v["absence_is_loss"], false);
    let legacy_count: u64 = rows
        .iter()
        .map(|r| {
            ["decoded", "journal_inserted", "journal_batch_unattributed"]
                .iter()
                .map(|stage| r[stage]["count"].as_u64().unwrap())
                .sum::<u64>()
        })
        .sum();
    assert_eq!(v["sequence_cut"], legacy_count);
    assert_eq!(v["outcomes_do_not_advance_legacy_sequence"], true);
}

#[test]
fn membership_disposition_uses_existing_close_lock_capacity_and_receipt_bounds() {
    let c = Capture::new(true, 1);
    let (signature, slot) = chosen(4);
    let guard = c.state.lock().unwrap();
    c.observe_disposition(&signature, slot, Disposition::ParseVote);
    drop(guard);
    c.observe_disposition(&signature, slot, Disposition::ParseVote);
    let (other, other_slot) = chosen(5);
    c.observe_disposition(&other, other_slot, Disposition::RawInserted);
    c.close();
    let before = c.snapshot();
    c.observe_disposition(&signature, slot, Disposition::RawInserted);
    assert_eq!(c.snapshot(), before);
    assert_eq!(before["gaps"], 1);
    assert_eq!(before["overflow"], 1);
    assert_eq!(before["complete"], false);
    let c = Capture::new(true, 2);
    let row = SwapEvent {
        signature,
        slot,
        wallet: "w".into(),
        dex: "d".into(),
        token_in: "i".into(),
        token_out: "o".into(),
        amount_in: 1.,
        amount_out: 2.,
        ts_utc: chrono::Utc::now(),
        exact_amounts: None,
    };
    c.observe_raw(std::slice::from_ref(&row), Some(&[]));
    c.close();
    assert_eq!(c.snapshot()["records"][0]["pre_raw_resolution"], 3);
    assert_eq!(c.snapshot()["complete"], false);
}

#[test]
fn membership_disposition_terminal_serialization_fits_existing_bound_or_is_incomplete() {
    let c = Capture::new(true, CAPACITY);
    for n in 0..CAPACITY as u16 {
        let (sig, slot) = chosen(n);
        c.decoded(&sig, slot);
        c.observe_disposition(&sig, slot, Disposition::BestEffortBudget);
    }
    c.close();
    let text = bounded_json(c.snapshot());
    assert!(text.len() <= MAX_OUTPUT_BYTES);
    let v: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(v["records"].as_array().unwrap().len(), CAPACITY);
    assert_eq!(v["pre_raw_complete"], true);
    // Extreme sparse-counter diversity may exceed the unchanged envelope; never truncate as complete.
    for row in c.state.lock().unwrap().rows.values_mut() {
        for code in Disposition::legend().keys() {
            row.outcomes.insert(*code, MAX_CALLS);
        }
    }
    let text = bounded_json(c.snapshot());
    assert!(text.len() <= MAX_OUTPUT_BYTES);
    let v: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(v["reason"], "output_bound_exceeded");
    assert_eq!(v["complete"], false);
}
