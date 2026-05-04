    #[test]
    fn recent_raw_journal_row_debt_moves_from_overflow_to_queue_and_inflight_stage1() -> Result<()>
    {
        let telemetry = ObservedSwapWriterTelemetry::default();
        telemetry
            .journal_overflow_row_debt_capacity
            .store(64, Ordering::Relaxed);
        let (journal_sender, journal_receiver) =
            std_mpsc::sync_channel::<super::RecentRawJournalWriteRequest>(2);
        let scenario_now = DateTime::parse_from_rfc3339("2026-04-28T20:10:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let mut journal_overflow = std::collections::VecDeque::new();
        let overflow_request = super::RecentRawJournalWriteRequest {
            inserted_swaps: (0..7usize)
                .map(|idx| recent_raw_journal_backpressure_swap(idx, scenario_now))
                .collect(),
        };
        let overflow_rows = overflow_request.row_count();
        journal_overflow.push_back(overflow_request);
        telemetry.note_journal_overflow_enqueued(overflow_rows);
        let overflow_snapshot = telemetry.snapshot();
        assert_eq!(overflow_snapshot.journal_overflow_depth_batches, 1);
        assert_eq!(overflow_snapshot.journal_overflow_row_debt, 7);
        assert_eq!(overflow_snapshot.journal_queue_row_debt, 0);
        assert_eq!(overflow_snapshot.journal_writer_inflight_rows, 0);

        super::drain_recent_raw_journal_overflow_nonblocking(
            &journal_sender,
            &mut journal_overflow,
            32,
            &telemetry,
        )?;
        let queued_snapshot = telemetry.snapshot();
        assert_eq!(queued_snapshot.journal_overflow_depth_batches, 0);
        assert_eq!(queued_snapshot.journal_overflow_row_debt, 0);
        assert_eq!(queued_snapshot.journal_queue_depth_batches, 1);
        assert_eq!(queued_snapshot.journal_queue_row_debt, 7);
        assert_eq!(queued_snapshot.journal_writer_inflight_rows, 0);

        let first_request = journal_receiver.recv()?;
        let collected_batch = super::collect_recent_raw_journal_write_batch(
            &journal_receiver,
            first_request,
            32,
            32,
            64,
            &telemetry,
        );
        let inserted_swaps = collected_batch.inserted_swaps;
        assert_eq!(inserted_swaps.len(), 7);
        let dequeued_snapshot = telemetry.snapshot();
        assert_eq!(dequeued_snapshot.journal_queue_depth_batches, 0);
        assert_eq!(dequeued_snapshot.journal_queue_row_debt, 0);
        assert_eq!(dequeued_snapshot.journal_writer_inflight_rows, 0);

        telemetry.note_journal_writer_inflight_started(inserted_swaps.len());
        let inflight_snapshot = telemetry.snapshot();
        assert_eq!(inflight_snapshot.journal_writer_inflight_rows, 7);
        telemetry.note_journal_writer_inflight_finished();
        assert_eq!(telemetry.snapshot().journal_writer_inflight_rows, 0);
        Ok(())
    }

    #[test]
    fn recent_raw_journal_overflow_drain_coalesces_fat_request_when_channel_has_capacity_stage1(
    ) -> Result<()> {
        let telemetry = ObservedSwapWriterTelemetry::default();
        telemetry
            .journal_overflow_row_debt_capacity
            .store(64, Ordering::Relaxed);
        let (journal_sender, journal_receiver) =
            std_mpsc::sync_channel::<super::RecentRawJournalWriteRequest>(4);
        let scenario_now = DateTime::parse_from_rfc3339("2026-04-28T20:20:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let mut journal_overflow = std::collections::VecDeque::new();
        for (start_idx, rows) in [(0usize, 2usize), (10, 3), (20, 4)] {
            let request = recent_raw_journal_write_request_for_test(start_idx, rows, scenario_now);
            telemetry.note_journal_overflow_enqueued(request.row_count());
            journal_overflow.push_back(request);
        }

        super::drain_recent_raw_journal_overflow_nonblocking(
            &journal_sender,
            &mut journal_overflow,
            8,
            &telemetry,
        )?;

        assert!(
            journal_overflow.is_empty(),
            "all overflow requests should be drained into one bounded fat channel request"
        );
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.journal_overflow_depth_batches, 0);
        assert_eq!(snapshot.journal_overflow_row_debt, 0);
        assert_eq!(snapshot.journal_queue_depth_batches, 1);
        assert_eq!(snapshot.journal_queue_row_debt, 9);

        let request = journal_receiver.recv()?;
        assert_eq!(
            request.row_count(),
            9,
            "overflow drain should send one fat request instead of replaying tiny batches"
        );
        let signatures = request
            .inserted_swaps
            .iter()
            .map(|swap| swap.signature.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            signatures,
            vec![
                "sig-journal-backpressure-00000",
                "sig-journal-backpressure-00001",
                "sig-journal-backpressure-00010",
                "sig-journal-backpressure-00011",
                "sig-journal-backpressure-00012",
                "sig-journal-backpressure-00020",
                "sig-journal-backpressure-00021",
                "sig-journal-backpressure-00022",
                "sig-journal-backpressure-00023",
            ],
            "coalesced overflow drain must preserve source order"
        );
        Ok(())
    }

    #[test]
    fn recent_raw_journal_overflow_drain_channel_full_restores_coalesced_front_without_loss_stage1(
    ) -> Result<()> {
        let telemetry = ObservedSwapWriterTelemetry::default();
        telemetry
            .journal_overflow_row_debt_capacity
            .store(64, Ordering::Relaxed);
        let (journal_sender, journal_receiver) =
            std_mpsc::sync_channel::<super::RecentRawJournalWriteRequest>(1);
        let scenario_now = DateTime::parse_from_rfc3339("2026-04-28T20:21:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let prefilled_request = recent_raw_journal_write_request_for_test(99, 1, scenario_now);
        let prefilled_rows = prefilled_request.row_count();
        journal_sender
            .try_send(prefilled_request)
            .map_err(|error| anyhow!("failed to prefill recent_raw journal channel: {error}"))?;
        telemetry.note_journal_queue_enqueued(prefilled_rows);

        let mut journal_overflow = std::collections::VecDeque::new();
        for (start_idx, rows) in [(0usize, 2usize), (10, 3), (20, 4)] {
            let request = recent_raw_journal_write_request_for_test(start_idx, rows, scenario_now);
            telemetry.note_journal_overflow_enqueued(request.row_count());
            journal_overflow.push_back(request);
        }
        let before = telemetry.snapshot();
        assert_eq!(before.journal_overflow_depth_batches, 3);
        assert_eq!(before.journal_overflow_row_debt, 9);

        super::drain_recent_raw_journal_overflow_nonblocking(
            &journal_sender,
            &mut journal_overflow,
            8,
            &telemetry,
        )?;

        let blocked_snapshot = telemetry.snapshot();
        assert_eq!(
            blocked_snapshot.journal_queue_depth_batches, 1,
            "full channel should keep the original queued request visible"
        );
        assert_eq!(
            blocked_snapshot.journal_overflow_depth_batches, 1,
            "full channel should restore one coalesced request to the overflow front"
        );
        assert_eq!(
            blocked_snapshot.journal_overflow_row_debt, 9,
            "full channel path must not lose overflow rows"
        );
        assert_eq!(
            super::recent_raw_journal_overflow_row_debt(&journal_overflow),
            9,
            "in-memory overflow row debt must match telemetry after coalesced restore"
        );

        let prefilled = journal_receiver.recv()?;
        telemetry.note_journal_queue_dequeued(prefilled.row_count());
        assert_eq!(prefilled.row_count(), 1);
        super::drain_recent_raw_journal_overflow_nonblocking(
            &journal_sender,
            &mut journal_overflow,
            8,
            &telemetry,
        )?;
        assert!(
            journal_overflow.is_empty(),
            "coalesced overflow request should drain after channel capacity returns"
        );
        let restored = journal_receiver.recv()?;
        assert_eq!(
            restored.row_count(),
            9,
            "all rows from the full-channel coalesced request must remain retryable"
        );
        let final_snapshot = telemetry.snapshot();
        assert_eq!(final_snapshot.journal_overflow_depth_batches, 0);
        assert_eq!(final_snapshot.journal_overflow_row_debt, 0);
        Ok(())
    }

    #[test]
    fn recent_raw_journal_adaptive_coalesce_collects_refilling_small_requests_stage1() -> Result<()>
    {
        let telemetry = ObservedSwapWriterTelemetry::default();
        telemetry
            .journal_queue_capacity_batches
            .store(64, Ordering::Relaxed);
        telemetry
            .journal_overflow_depth_batches
            .store(1, Ordering::Relaxed);
        let (journal_sender, journal_receiver) =
            std_mpsc::sync_channel::<super::RecentRawJournalWriteRequest>(64);
        let scenario_now = DateTime::parse_from_rfc3339("2026-04-28T20:12:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let first_request = super::RecentRawJournalWriteRequest {
            inserted_swaps: vec![recent_raw_journal_backpressure_swap(0, scenario_now)],
        };
        let first_rows = first_request.row_count();
        journal_sender.send(first_request)?;
        telemetry.note_journal_queue_enqueued(first_rows);

        let refill_sender = journal_sender.clone();
        let (refill_started_tx, refill_started_rx) = std_mpsc::channel::<()>();
        let producer = std::thread::spawn(move || -> Result<()> {
            let _ = refill_started_tx.send(());
            std::thread::sleep(StdDuration::from_millis(1));
            for idx in 1..17usize {
                let request = super::RecentRawJournalWriteRequest {
                    inserted_swaps: vec![recent_raw_journal_backpressure_swap(idx, scenario_now)],
                };
                refill_sender.send(request)?;
            }
            Ok(())
        });
        refill_started_rx.recv()?;

        let first_request = journal_receiver.recv()?;
        let collected_batch = super::collect_recent_raw_journal_write_batch(
            &journal_receiver,
            first_request,
            1,
            16,
            16,
            &telemetry,
        );
        producer
            .join()
            .expect("refill producer thread panicked")
            .context("refill producer failed")?;

        assert!(
            collected_batch.request_batches > 1,
            "adaptive recent_raw journal coalescing must collect requests that arrive shortly after the first dequeue: {collected_batch:?}"
        );
        assert_eq!(
            collected_batch.inserted_swaps.len(),
            collected_batch.request_batches
        );
        assert!(
            collected_batch.request_batches <= 16,
            "adaptive coalescing must remain bounded by the explicit adaptive limit: {collected_batch:?}"
        );
        assert!(
            collected_batch.coalesce_elapsed_ms
                <= super::OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_WINDOW
                    .as_millis()
                    .saturating_add(5) as u64,
            "adaptive coalescing must use only the tiny bounded wait window: {collected_batch:?}"
        );
        Ok(())
    }

    #[test]
    fn recent_raw_journal_writer_coalesces_queued_tiny_requests_to_row_cap_under_pressure_stage1(
    ) -> Result<()> {
        let telemetry = ObservedSwapWriterTelemetry::default();
        telemetry
            .journal_queue_capacity_batches
            .store(128, Ordering::Relaxed);
        telemetry
            .journal_overflow_depth_batches
            .store(1, Ordering::Relaxed);
        let (journal_sender, journal_receiver) =
            std_mpsc::sync_channel::<super::RecentRawJournalWriteRequest>(128);
        let scenario_now = DateTime::parse_from_rfc3339("2026-04-28T20:22:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        for idx in 0..64usize {
            let request = recent_raw_journal_write_request_for_test(idx, 1, scenario_now);
            let request_rows = request.row_count();
            journal_sender.send(request)?;
            telemetry.note_journal_queue_enqueued(request_rows);
        }

        let first_request = journal_receiver.recv()?;
        let collected_batch = super::collect_recent_raw_journal_write_batch(
            &journal_receiver,
            first_request,
            4,
            64,
            32,
            &telemetry,
        );

        assert_eq!(
            collected_batch.inserted_swaps.len(),
            32,
            "writer-side coalescing under pressure should drain queued tiny requests into a fat bounded row batch"
        );
        assert_eq!(collected_batch.request_batches, 32);
        assert_eq!(collected_batch.coalesce_limit_rows, 32);
        let snapshot = telemetry.snapshot();
        assert_eq!(
            snapshot.journal_queue_depth_batches, 32,
            "row-capped writer drain should leave the remaining queued requests visible"
        );
        assert_eq!(snapshot.journal_queue_row_debt, 32);
        Ok(())
    }
