use super::*;

#[test]
fn redacted_url_for_log_redacts_query_string() {
    assert_eq!(
        redacted_url_for_log("wss://mainnet.helius-rpc.com/?api-key=super-secret"),
        "wss://mainnet.helius-rpc.com/?<redacted>"
    );
    assert_eq!(
        redacted_url_for_log("https://rpc.example.com/v1?token=abc&x=1"),
        "https://rpc.example.com/v1?<redacted>"
    );
}

#[test]
fn redacted_url_for_log_redacts_userinfo_and_query() {
    assert_eq!(
        redacted_url_for_log("https://user:secret@rpc.example.com/v1?api-key=secret"),
        "https://rpc.example.com/v1?<redacted>"
    );
    assert_eq!(
        redacted_url_for_log("wss://token@mainnet.helius-rpc.com/socket"),
        "wss://mainnet.helius-rpc.com/socket"
    );
}

#[test]
fn redacted_url_for_log_preserves_non_query_url() {
    assert_eq!(
        redacted_url_for_log("https://rpc.example.com/v1"),
        "https://rpc.example.com/v1"
    );
    assert_eq!(redacted_url_for_log(""), "");
}

#[test]
fn from_config_rejects_helius_ingestion_source() {
    let mut config = IngestionConfig::default();
    config.source = "helius_ws".to_string();
    let error = match IngestionSource::from_config(&config) {
        Ok(_) => panic!("helius ingestion source must reject"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("ingestion.source=helius_ws is no longer supported"),
        "unexpected error: {error:#}"
    );
}

#[test]
fn infer_swap_prefers_sol_leg_with_lamport_delta() {
    let signer = "Leader111111111111111111111111111111111";
    let meta = json!({
        "preTokenBalances": [token_balance(signer, "TokenMintA", "0")],
        "postTokenBalances": [token_balance(signer, "TokenMintA", "100")],
        "preBalances": [2_000_000_000u64],
        "postBalances": [1_000_000_000u64]
    });

    let inferred = HeliusWsSource::infer_swap_from_json_balances(&meta, 0, signer)
        .expect("expected SOL buy inference");
    assert_eq!(inferred.0, SOL_MINT);
    assert!((inferred.1.amount - 1.0).abs() < 1e-9);
    assert_eq!(inferred.2, "TokenMintA");
    assert!((inferred.3.amount - 100.0).abs() < 1e-9);
    assert!(inferred.1.raw_amount.is_none());
    assert_eq!(inferred.3.raw_amount.as_deref(), Some("100000000"));
    assert!(HeliusWsSource::build_exact_swap_amounts(&inferred.1, &inferred.3).is_none());
}

#[test]
fn infer_swap_does_not_reconstruct_exact_raw_delta_from_partial_json_balances() {
    let signer = "Leader111111111111111111111111111111111";
    let meta = json!({
        "preTokenBalances": [token_balance_without_raw(signer, "TokenMintA", "80")],
        "postTokenBalances": [token_balance(signer, "TokenMintA", "90")],
        "preBalances": [2_000_000_000u64],
        "postBalances": [1_900_000_000u64]
    });

    let inferred = HeliusWsSource::infer_swap_from_json_balances(&meta, 0, signer)
        .expect("expected SOL buy inference");
    assert_eq!(inferred.2, "TokenMintA");
    assert!((inferred.3.amount - 10.0).abs() < 1e-9);
    assert!(inferred.3.raw_amount.is_none());
    assert!(HeliusWsSource::build_exact_swap_amounts(&inferred.1, &inferred.3).is_none());
}

#[test]
fn classify_parse_reject_reason_maps_known_patterns() {
    assert_eq!(
        classify_parse_reject_reason(&anyhow!("missing slot in yellowstone update")),
        "missing_slot"
    );
    assert_eq!(
        classify_parse_reject_reason(&anyhow!("missing status in yellowstone update")),
        "missing_status"
    );
    assert_eq!(
        classify_parse_reject_reason(&anyhow!("missing signer in yellowstone update")),
        "missing_signer"
    );
    assert_eq!(
        classify_parse_reject_reason(&anyhow!("missing program ids in yellowstone update")),
        "missing_program_ids"
    );
    assert_eq!(
        classify_parse_reject_reason(&anyhow!("missing transaction signature in update")),
        "missing_signature"
    );
    assert_eq!(
        classify_parse_reject_reason(&anyhow!("invalid timestamp nanos in payload")),
        "invalid_timestamp"
    );
    assert_eq!(
        classify_parse_reject_reason(&anyhow!("failed balance inference for signer")),
        "invalid_balance_inference"
    );
    assert_eq!(
        classify_parse_reject_reason(&anyhow!("account key index out of bounds")),
        "invalid_account_keys"
    );
    assert_eq!(
        classify_parse_reject_reason(&anyhow!("unexpected parser failure")),
        "other"
    );
}

#[test]
fn note_parse_rejected_tracks_reason_breakdown() {
    let telemetry = IngestionTelemetry::default();
    telemetry.note_parse_rejected(&anyhow!("missing signer in yellowstone update"));
    telemetry.note_parse_rejected(&anyhow!("missing transaction signature in update"));
    telemetry.note_parse_rejected(&anyhow!("unclassified parser issue"));

    assert_eq!(telemetry.parse_rejected_total.load(Ordering::Relaxed), 3);
    let reasons = telemetry
        .parse_rejected_by_reason
        .lock()
        .expect("parse_rejected_by_reason mutex should be available");
    assert_eq!(reasons.get("missing_signer"), Some(&1));
    assert_eq!(reasons.get("missing_signature"), Some(&1));
    assert_eq!(reasons.get("other"), Some(&1));
}

#[test]
fn note_parse_fallback_tracks_reason_breakdown() {
    let telemetry = IngestionTelemetry::default();
    telemetry.note_parse_fallback("missing_program_ids_fallback");
    telemetry.note_parse_fallback("missing_program_ids_fallback");

    let reasons = telemetry
        .parse_fallback_by_reason
        .lock()
        .expect("parse_fallback_by_reason mutex should be available");
    assert_eq!(reasons.get("missing_program_ids_fallback"), Some(&2));
}

#[test]
fn ingestion_runtime_snapshot_exposes_yellowstone_output_queue_metrics() {
    let telemetry = IngestionTelemetry::default();
    telemetry.note_yellowstone_output_queue_metrics(7, 10, 33);

    let snapshot = telemetry.snapshot();
    assert_eq!(snapshot.yellowstone_output_queue_depth, 7);
    assert_eq!(snapshot.yellowstone_output_queue_capacity, 10);
    assert_eq!(snapshot.yellowstone_output_oldest_age_ms, 33);
    assert!(
        (snapshot.yellowstone_output_queue_fill_ratio - 0.7).abs() < f64::EPSILON,
        "unexpected fill ratio: {}",
        snapshot.yellowstone_output_queue_fill_ratio
    );
}

#[test]
fn yellowstone_report_stage_queue_depths_route_output_backlog_to_fetch_to_output() {
    let (ws_to_fetch_queue_depth, fetch_to_output_queue_depth) =
        YellowstoneGrpcSource::report_stage_queue_depths(7);
    assert_eq!(ws_to_fetch_queue_depth, 0);
    assert_eq!(fetch_to_output_queue_depth, 7);
}

#[test]
fn yellowstone_report_stage_queue_depths_preserve_zero_depth() {
    let (ws_to_fetch_queue_depth, fetch_to_output_queue_depth) =
        YellowstoneGrpcSource::report_stage_queue_depths(0);
    assert_eq!(ws_to_fetch_queue_depth, 0);
    assert_eq!(fetch_to_output_queue_depth, 0);
}

#[test]
fn normalize_program_ids_or_fallback_tracks_missing_program_ids_fallback() -> Result<()> {
    let telemetry = IngestionTelemetry::default();
    let interested = HashSet::from([String::from("prog-1")]);
    let normalized = normalize_program_ids_or_fallback(
        HashSet::new(),
        &interested,
        &telemetry,
        "missing program ids in test",
    )?;
    let normalized = normalized.expect("missing program ids should fallback to interested set");
    assert!(normalized.contains("prog-1"));
    let reasons = telemetry
        .parse_fallback_by_reason
        .lock()
        .expect("parse_fallback_by_reason mutex should be available");
    assert_eq!(reasons.get("missing_program_ids_fallback"), Some(&1));
    Ok(())
}

#[test]
fn normalize_program_ids_or_fallback_drops_non_interested_programs() -> Result<()> {
    let telemetry = IngestionTelemetry::default();
    let interested = HashSet::from([String::from("prog-1")]);
    let extracted = HashSet::from([String::from("prog-2")]);
    let normalized = normalize_program_ids_or_fallback(
        extracted,
        &interested,
        &telemetry,
        "missing program ids in test",
    )?;
    assert!(
        normalized.is_none(),
        "non-interested program ids should be dropped"
    );
    let reasons = telemetry
        .parse_fallback_by_reason
        .lock()
        .expect("parse_fallback_by_reason mutex should be available");
    assert_eq!(
        reasons.get("missing_program_ids_fallback"),
        None,
        "drop path should not increment fallback counters"
    );
    Ok(())
}

#[test]
fn infer_swap_drops_ambiguous_multi_output_tx() {
    let signer = "Leader111111111111111111111111111111111";
    let meta = json!({
        "preTokenBalances": [
            token_balance(signer, "TokenMintA", "0"),
            token_balance(signer, "TokenMintB", "0")
        ],
        "postTokenBalances": [
            token_balance(signer, "TokenMintA", "100"),
            token_balance(signer, "TokenMintB", "40")
        ],
        "preBalances": [2_000_000_000u64],
        "postBalances": [1_000_000_000u64]
    });

    let inferred = HeliusWsSource::infer_swap_from_json_balances(&meta, 0, signer);
    assert!(inferred.is_none(), "ambiguous multi-hop should be rejected");
}

#[test]
fn reorder_releases_oldest_slot_signature() {
    let telemetry = Arc::new(IngestionTelemetry::default());
    let runtime_config = test_runtime_config(telemetry);

    let mut source = HeliusWsSource {
        runtime_config,
        fetch_concurrency: 1,
        ws_queue_capacity: 16,
        queue_overflow_policy: QueueOverflowPolicy::Block,
        output_queue_capacity: 16,
        reorder: ReorderBuffer::new(1, 8),
        telemetry_report_seconds: 30,
        pipeline: None,
    };

    source.push_reorder_entry(FetchedObservation {
        raw: RawSwapObservation {
            signature: "b".to_string(),
            slot: 20,
            signer: "w".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "t".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            program_ids: vec![],
            dex_hint: "raydium".to_string(),
            ts_utc: Utc::now(),
        },
        arrival_seq: 2,
        fetch_latency_ms: 10,
        enqueued_at: Instant::now(),
    });
    source.push_reorder_entry(FetchedObservation {
        raw: RawSwapObservation {
            signature: "a".to_string(),
            slot: 10,
            signer: "w".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "t".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            program_ids: vec![],
            dex_hint: "raydium".to_string(),
            ts_utc: Utc::now(),
        },
        arrival_seq: 1,
        fetch_latency_ms: 10,
        enqueued_at: Instant::now(),
    });

    // Force early release via buffer cap branch.
    source.reorder.set_max_buffer(1);
    let first = source
        .pop_ready_observation()
        .expect("first observation should be released");
    assert_eq!(first.slot, 10);
    assert_eq!(first.signature, "a");
}

#[test]
fn reorder_uses_arrival_sequence_within_same_slot() {
    let telemetry = Arc::new(IngestionTelemetry::default());
    let runtime_config = test_runtime_config(telemetry);

    let mut source = HeliusWsSource {
        runtime_config,
        fetch_concurrency: 1,
        ws_queue_capacity: 16,
        queue_overflow_policy: QueueOverflowPolicy::Block,
        output_queue_capacity: 16,
        reorder: ReorderBuffer::new(1, 8),
        telemetry_report_seconds: 30,
        pipeline: None,
    };

    source.push_reorder_entry(FetchedObservation {
        raw: RawSwapObservation {
            // Lexicographically smaller signature should NOT win inside same slot.
            signature: "A-signature".to_string(),
            slot: 50,
            signer: "wallet".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "mint".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            program_ids: vec![],
            dex_hint: "raydium".to_string(),
            ts_utc: Utc::now(),
        },
        arrival_seq: 2,
        fetch_latency_ms: 5,
        enqueued_at: Instant::now(),
    });
    source.push_reorder_entry(FetchedObservation {
        raw: RawSwapObservation {
            signature: "Z-signature".to_string(),
            slot: 50,
            signer: "wallet".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "mint".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            program_ids: vec![],
            dex_hint: "raydium".to_string(),
            ts_utc: Utc::now(),
        },
        arrival_seq: 1,
        fetch_latency_ms: 5,
        enqueued_at: Instant::now(),
    });

    source.reorder.set_max_buffer(1);
    let first = source.pop_ready_observation().expect("first observation");
    assert_eq!(first.signature, "Z-signature");
}

#[tokio::test]
async fn notification_queue_drop_oldest_keeps_freshest_items() {
    let queue = NotificationQueue::new(2);
    let build = |signature: &str| LogsNotification {
        signature: signature.to_string(),
        slot: 1,
        arrival_seq: 0,
        logs: vec![],
        is_failed: false,
        enqueued_at: Instant::now(),
    };

    assert!(matches!(
        queue
            .push(build("sig-1"), QueueOverflowPolicy::Block)
            .await
            .expect("queue open"),
        QueuePushResult::Enqueued { .. }
    ));
    assert!(matches!(
        queue
            .push(build("sig-2"), QueueOverflowPolicy::Block)
            .await
            .expect("queue open"),
        QueuePushResult::Enqueued { .. }
    ));
    assert!(matches!(
        queue
            .push(build("sig-3"), QueueOverflowPolicy::DropOldest)
            .await
            .expect("queue open"),
        QueuePushResult::ReplacedOldest
    ));

    let first = queue.pop().await.expect("first item");
    let second = queue.pop().await.expect("second item");
    assert_eq!(first.signature, "sig-2");
    assert_eq!(second.signature, "sig-3");
}

#[tokio::test]
async fn raw_observation_queue_snapshot_reports_depth_capacity_and_oldest_age() {
    let queue = RawObservationQueue::new(2);
    let build = |signature: &str, oldest_age_ms: u64| FetchedObservation {
        raw: RawSwapObservation {
            signature: signature.to_string(),
            slot: 1,
            signer: "wallet".to_string(),
            token_in: SOL_MINT.to_string(),
            token_out: "mint".to_string(),
            amount_in: 1.0,
            amount_out: 100.0,
            exact_amounts: None,
            program_ids: vec![],
            dex_hint: "raydium".to_string(),
            ts_utc: Utc::now(),
        },
        arrival_seq: 0,
        fetch_latency_ms: 0,
        enqueued_at: Instant::now() - Duration::from_millis(oldest_age_ms),
    };

    queue
        .push(build("sig-1", 25), QueueOverflowPolicy::Block)
        .await
        .expect("queue open");
    queue
        .push(build("sig-2", 0), QueueOverflowPolicy::Block)
        .await
        .expect("queue open");

    let snapshot = queue
        .try_snapshot(|item| {
            item.enqueued_at
                .elapsed()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64
        })
        .expect("queue snapshot should be available without awaiting");
    assert_eq!(snapshot.len, 2);
    assert_eq!(snapshot.capacity, 2);
    assert!(
        snapshot.oldest.unwrap_or(0) >= 20,
        "oldest queue age should reflect resident item time"
    );
}
