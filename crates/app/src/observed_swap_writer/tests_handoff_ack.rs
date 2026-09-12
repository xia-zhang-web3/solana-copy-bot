use super::super::{unpack_observed_swap_write_batch, ObservedSwapWriteRequest};
use super::*;
use copybot_storage_core::SourceSellCandidate as Candidate;

#[tokio::test]
async fn batch57_lost_ack_receiver_preserves_committed_handoff_and_reply_order() -> Result<()> {
    let path = std::env::temp_dir().join(format!(
        "batch57-ack-{}-{}.db",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap()
    ));
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let mut e = recent_raw_journal_backpressure_swap(1, Utc::now());
    std::mem::swap(&mut e.token_in, &mut e.token_out);
    let writer = ObservedSwapWriter::start_for_test(path.to_string_lossy().into(), 8, 8)?;
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    drop(reply_rx); // Actual receiver loss before commit; no claim that the worker stopped.
    writer
        .send_request(ObservedSwapWriteRequest {
            swap: e.clone(),
            candidate: Some(Candidate::new(&e, "original-p")),
            reply_tx: Some(reply_tx),
            enqueued_at: Instant::now(),
        })
        .await?;
    // Subsequent request ACK proves the preceding request reached the writer.
    let mut b = e.clone();
    b.signature.push_str("-b");
    assert!(
        writer
            .write_with_candidate(&b, Some(Candidate::new(&b, "b-p")))
            .await?
    );
    assert!(
        !writer
            .write_with_candidate(&e, Some(Candidate::new(&e, "replacement-q")))
            .await?
    );
    timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || writer.shutdown()),
    )
    .await???;
    drop(store);
    let store = SqliteStore::open(&path)?;
    let h = store
        .load_source_sell_handoff(&e.signature)?
        .context("lost ACK is still committed")?;
    assert_eq!(h.original_position_id.as_deref(), Some("original-p"));
    assert_eq!(h.disposition, "pending");
    assert_eq!(store.load_observed_swaps_since(e.ts_utc)?.len(), 2);
    assert_eq!(
        store
            .advance_source_sell_handoff()?
            .unwrap()
            .event
            .signature,
        b.signature
    );
    assert_eq!(
        store
            .advance_source_sell_handoff()?
            .unwrap()
            .event
            .signature,
        e.signature
    );
    drop(store);
    remove_sqlite_test_files(&path);
    Ok(())
}

#[test]
fn batch57_writer_unpack_keeps_candidate_and_reply_alignment_for_one_batch() -> Result<()> {
    let path = std::env::temp_dir().join(format!(
        "batch57-batch-{}-{}.db",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap()
    ));
    let mut store = SqliteStore::open(&path)?;
    store.run_migrations(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    let mut e = recent_raw_journal_backpressure_swap(1, Utc::now());
    std::mem::swap(&mut e.token_in, &mut e.token_out);
    let mut b = e.clone();
    b.signature.push_str("-b");
    let events = [e.clone(), e.clone(), b.clone(), b.clone()];
    let positions = [None, Some("late-p"), Some("first-p"), Some("later-q")];
    let mut requests = Vec::new();
    let mut receivers = Vec::new();
    for (event, position) in events.iter().zip(positions) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        receivers.push(rx);
        requests.push(ObservedSwapWriteRequest {
            swap: event.clone(),
            candidate: position.map(|p| Candidate::new(event, p)),
            reply_tx: Some(tx),
            enqueued_at: Instant::now(),
        });
    }
    let (swaps, candidates, replies, _) = unpack_observed_swap_write_batch(requests);
    let result = store.insert_observed_swaps_with_candidates(&swaps, &candidates)?;
    assert_eq!(result.inserted, vec![true, false, true, false]);
    for (reply, flag) in replies.into_iter().zip(result.inserted) {
        reply.unwrap().send(Ok(flag)).unwrap();
    }
    for (mut receiver, want) in receivers.into_iter().zip([true, false, true, false]) {
        assert_eq!(receiver.try_recv()??, want);
    }
    assert!(store
        .load_source_sell_handoff(&e.signature)?
        .unwrap()
        .original_position_id
        .is_none());
    assert_eq!(
        store
            .load_source_sell_handoff(&b.signature)?
            .unwrap()
            .original_position_id
            .as_deref(),
        Some("first-p")
    );
    drop(store);
    remove_sqlite_test_files(&path);
    Ok(())
}
