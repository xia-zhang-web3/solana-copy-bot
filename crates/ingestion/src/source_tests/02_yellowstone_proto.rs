use super::*;

#[test]
fn dedupe_ttl_prunes_expired_signatures() {
    let mut seen_signatures_map: HashMap<String, Instant> = HashMap::new();
    let mut seen_signatures_queue: VecDeque<SeenSignatureEntry> = VecDeque::new();
    let ttl = Duration::from_millis(100);
    let now = Instant::now();
    mark_seen_signature(
        &mut seen_signatures_map,
        &mut seen_signatures_queue,
        16,
        ttl,
        "sig-1".to_string(),
        now,
    );
    assert!(is_seen_signature(
        &seen_signatures_map,
        "sig-1",
        ttl,
        now + Duration::from_millis(50)
    ));

    prune_seen_signatures(
        &mut seen_signatures_map,
        &mut seen_signatures_queue,
        16,
        ttl,
        now + Duration::from_millis(150),
    );
    assert!(!is_seen_signature(
        &seen_signatures_map,
        "sig-1",
        ttl,
        now + Duration::from_millis(150)
    ));
}

#[test]
fn retry_delay_respects_retry_after_and_cap() {
    let delay = compute_retry_delay(100, 500, 50, 1, "signature-a", Some(Duration::from_secs(2)));
    assert!(delay >= Duration::from_secs(2));
    assert!(delay <= Duration::from_millis(2_050));
}

#[test]
fn effective_per_endpoint_limit_avoids_single_endpoint_self_throttle() {
    assert_eq!(effective_per_endpoint_rps_limit(16, 45, 1), 45);
    assert_eq!(effective_per_endpoint_rps_limit(0, 45, 1), 45);
    assert_eq!(effective_per_endpoint_rps_limit(16, 45, 3), 16);
}

#[test]
fn parse_logs_notification_ignores_subscribe_ack() {
    let ack = json!({
        "jsonrpc": "2.0",
        "id": 7,
        "result": 99,
    })
    .to_string();

    assert!(HeliusWsSource::parse_logs_notification(&ack).is_none());
}

#[test]
fn yellowstone_subscribe_request_uses_confirmed_commitment_and_program_filters() {
    let mut interested = HashSet::new();
    interested.insert("Program1111111111111111111111111111111111".to_string());
    let runtime_config = YellowstoneRuntimeConfig {
        grpc_url: "https://example.quicknode.com:10000".to_string(),
        x_token: "token".to_string(),
        connect_timeout_ms: 5_000,
        subscribe_timeout_ms: 15_000,
        reconnect_initial_ms: 500,
        reconnect_max_ms: 8_000,
        stream_buffer_capacity: 512,
        seen_signatures_limit: 5_000,
        seen_signatures_ttl: Duration::from_secs(60),
        interested_program_ids: interested,
        raydium_program_ids: HashSet::new(),
        pumpswap_program_ids: HashSet::new(),
        telemetry: Arc::new(IngestionTelemetry::default()),
    };

    let request = build_yellowstone_subscribe_request(&runtime_config);
    assert_eq!(request.commitment, Some(CommitmentLevel::Confirmed as i32));
    let tx_filter = request
        .transactions
        .get("copybot-swaps")
        .expect("transaction filter should be present");
    assert_eq!(tx_filter.vote, Some(false));
    assert_eq!(tx_filter.failed, Some(false));
    assert_eq!(tx_filter.account_include.len(), 1);
}

#[test]
fn infer_swap_from_proto_prefers_sol_leg_with_lamport_delta() {
    let signer = "Leader111111111111111111111111111111111";
    let pre_token = yellowstone_grpc_proto::prelude::TokenBalance {
        account_index: 0,
        mint: "TokenMintA".to_string(),
        ui_token_amount: Some(UiTokenAmount {
            ui_amount: 0.0,
            decimals: 6,
            amount: "0".to_string(),
            ui_amount_string: "0".to_string(),
        }),
        owner: signer.to_string(),
        program_id: String::new(),
    };
    let post_token = yellowstone_grpc_proto::prelude::TokenBalance {
        account_index: 0,
        mint: "TokenMintA".to_string(),
        ui_token_amount: Some(UiTokenAmount {
            ui_amount: 100.0,
            decimals: 6,
            amount: "100000000".to_string(),
            ui_amount_string: "100".to_string(),
        }),
        owner: signer.to_string(),
        program_id: String::new(),
    };
    let meta = TransactionStatusMeta {
        pre_balances: vec![2_000_000_000],
        post_balances: vec![1_000_000_000],
        pre_token_balances: vec![pre_token],
        post_token_balances: vec![post_token],
        ..Default::default()
    };

    let inferred =
        infer_swap_from_proto_balances(&meta, 0, signer).expect("expected SOL->token inference");
    assert_eq!(inferred.0, SOL_MINT);
    assert_eq!(inferred.2, "TokenMintA");
    assert!((inferred.1.amount - 1.0).abs() < 1e-9);
    assert!((inferred.3.amount - 100.0).abs() < 1e-9);
    assert!(inferred.1.raw_amount.is_none());
    assert_eq!(inferred.3.raw_amount.as_deref(), Some("100000000"));
    assert!(super::yellowstone_proto::build_exact_swap_amounts(&inferred.1, &inferred.3).is_none());
}

#[test]
fn infer_swap_from_proto_does_not_reconstruct_exact_raw_delta_from_partial_balances() {
    let signer = "Leader111111111111111111111111111111111";
    let pre_token = yellowstone_grpc_proto::prelude::TokenBalance {
        account_index: 0,
        mint: "TokenMintA".to_string(),
        ui_token_amount: Some(UiTokenAmount {
            ui_amount: 80.0,
            decimals: 6,
            amount: String::new(),
            ui_amount_string: "80".to_string(),
        }),
        owner: signer.to_string(),
        program_id: String::new(),
    };
    let post_token = yellowstone_grpc_proto::prelude::TokenBalance {
        account_index: 0,
        mint: "TokenMintA".to_string(),
        ui_token_amount: Some(UiTokenAmount {
            ui_amount: 90.0,
            decimals: 6,
            amount: "90000000".to_string(),
            ui_amount_string: "90".to_string(),
        }),
        owner: signer.to_string(),
        program_id: String::new(),
    };
    let meta = TransactionStatusMeta {
        pre_balances: vec![2_000_000_000],
        post_balances: vec![1_900_000_000],
        pre_token_balances: vec![pre_token],
        post_token_balances: vec![post_token],
        ..Default::default()
    };

    let inferred =
        infer_swap_from_proto_balances(&meta, 0, signer).expect("expected SOL->token inference");
    assert_eq!(inferred.2, "TokenMintA");
    assert!((inferred.3.amount - 10.0).abs() < 1e-9);
    assert!(inferred.3.raw_amount.is_none());
    assert!(super::yellowstone_proto::build_exact_swap_amounts(&inferred.1, &inferred.3).is_none());
}
