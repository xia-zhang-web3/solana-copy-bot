use super::*;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateTransaction,
    SubscribeUpdateTransactionInfo,
};

fn chosen(n: u8) -> (String, u64, Vec<u8>) {
    let raw = vec![n; 64];
    let signature = bs58::encode(&raw).into_string();
    let slot = (1..100_000)
        .find(|slot| selected(&signature, *slot))
        .unwrap();
    (signature, slot, raw)
}

fn update(raw: Vec<u8>, slot: u64) -> SubscribeUpdate {
    SubscribeUpdate {
        update_oneof: Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
            slot,
            transaction: Some(SubscribeUpdateTransactionInfo {
                signature: raw,
                ..Default::default()
            }),
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn swap(signature: String, slot: u64) -> SwapEvent {
    SwapEvent {
        signature,
        slot,
        wallet: "wallet".into(),
        dex: "dex".into(),
        token_in: "in".into(),
        token_out: "out".into(),
        amount_in: 1.0,
        amount_out: 2.0,
        ts_utc: chrono::Utc::now(),
        exact_amounts: None,
    }
}

#[test]
fn transaction_membership_selector_and_two_actual_boundaries_agree() {
    let a = Capture::new(true, CAPACITY);
    let b = Capture::new(true, CAPACITY);
    let (signature, slot, raw) = chosen(9);
    assert_eq!(slot, 389);
    let input = update(raw, slot);
    a.observe_update(&input);
    b.observe_update(&input);
    a.observe_update(&input);
    let rows = [swap(signature.clone(), slot)];
    a.observe_journal(
        &rows,
        JournalReceipt::Committed {
            processed: 1,
            inserted: 1,
        },
    );
    b.observe_journal(
        &rows,
        JournalReceipt::Committed {
            processed: 1,
            inserted: 1,
        },
    );
    a.close();
    b.close();
    let left = a.snapshot();
    let right = b.snapshot();
    assert_eq!(left["records"][0]["signature"], signature);
    assert_eq!(left["records"][0]["slot"], slot);
    assert_eq!(left["records"][0]["decoded"]["count"], 2);
    assert_eq!(right["records"][0]["decoded"]["count"], 1);
    assert_eq!(left["records"][0]["journal_inserted"]["count"], 1);
    assert_eq!(left["complete"], true);
    assert_eq!(right["complete"], true);
    assert_eq!(left["absence_is_loss"], false);
}

#[test]
fn transaction_membership_call_and_receipt_bounds_are_incomplete() {
    let c = Capture::new(true, 2);
    c.calls.store(MAX_CALLS, Ordering::Relaxed);
    let (_, slot, raw) = chosen(1);
    c.observe_update(&update(raw, slot));
    c.close();
    assert_eq!(c.snapshot()["overflow"], 1);
    assert_eq!(c.snapshot()["complete"], false);
    let c = Capture::new(true, 2);
    let (signature, slot, _) = chosen(2);
    let rows = vec![swap(signature, slot); MAX_ROWS_PER_RECEIPT + 1];
    c.observe_journal(
        &rows,
        JournalReceipt::Committed {
            processed: rows.len(),
            inserted: rows.len(),
        },
    );
    c.close();
    assert_eq!(c.snapshot()["overflow"], 1);
    assert_eq!(c.snapshot()["complete"], false);
    assert_eq!(c.snapshot()["records"].as_array().unwrap().len(), 0);
    assert_eq!(c.value(false)["records_included"], false);
    assert_eq!(c.value(false)["complete"], false);
}

#[test]
fn transaction_membership_default_off_is_no_capture() {
    let c = Capture::new(false, CAPACITY);
    let (_, slot, raw) = chosen(3);
    c.observe_update(&update(raw, slot));
    c.observe_journal(&[], JournalReceipt::Unknown);
    c.close();
    let v = c.snapshot();
    assert_eq!(v["records"].as_array().unwrap().len(), 0);
    assert_eq!(v["enabled"], false);
    assert_eq!(v["complete"], false);
    assert_eq!(v["gaps"], 0);
}

#[test]
fn transaction_membership_overflow_contention_and_interruption_are_unknown() {
    let c = Capture::new(true, 1);
    for n in [1, 2] {
        let (_, slot, raw) = chosen(n);
        c.observe_update(&update(raw, slot));
    }
    assert_eq!(c.snapshot()["closed"], false);
    c.close();
    assert_eq!(c.snapshot()["complete"], false);
    assert_eq!(c.snapshot()["records"].as_array().unwrap().len(), 1);
    assert_eq!(c.snapshot()["overflow"], 1);
    let c = Capture::new(true, 2);
    let (_, slot, raw) = chosen(8);
    let guard = c.state.lock().unwrap();
    c.observe_update(&update(raw, slot));
    drop(guard);
    c.close();
    assert_eq!(c.snapshot()["gaps"], 1);
    assert_eq!(c.snapshot()["complete"], false);
    let c = Capture::new(true, 2);
    let active = c.enter().unwrap();
    c.close();
    drop(active);
    assert_eq!(c.snapshot()["complete"], false);
}

#[test]
fn transaction_membership_invalid_identity_is_not_absence() {
    let c = Capture::new(true, 2);
    c.observe_update(&update(vec![1; 63], 1));
    c.close();
    assert_eq!(c.snapshot()["gaps"], 1);
    assert_eq!(c.snapshot()["complete"], false);
}

#[test]
fn transaction_membership_slots_and_conflicts_remain_distinct() {
    let (signature, slot, raw) = chosen(6);
    let other = (slot + 1..100_000)
        .find(|s| selected(&signature, *s))
        .unwrap();
    let c = Capture::new(true, 4);
    c.observe_update(&update(raw.clone(), slot));
    c.observe_update(&update(raw, other));
    c.observe_journal(
        &[swap(signature.clone(), slot), swap(signature, other)],
        JournalReceipt::Committed {
            processed: 2,
            inserted: 1,
        },
    );
    c.close();
    let v = c.snapshot();
    assert_eq!(v["records"].as_array().unwrap().len(), 2);
    for row in v["records"].as_array().unwrap() {
        assert_eq!(row["journal_inserted"]["count"], 0);
        assert_eq!(row["journal_batch_unattributed"]["count"], 1);
    }
}
