use super::*;
use crate::app_loop_irrelevant_swap::handle_irrelevant_observed_swap;
use copybot_ingestion::capture_replay::CaptureReplay;

#[tokio::test]
async fn capture_scope_real_ingestion_survives_legacy_inflight_and_cooldown_discard() -> Result<()>
{
    let (store, app_path) = make_test_store("capture-scope-pipeline")?;
    let capture_path = app_path.with_extension("capture.db");
    let capture = Connection::open(&capture_path)?;
    capture.execute_batch(copybot_storage_core::capture_scope::SCHEMA)?;
    capture.execute(
        "INSERT INTO capture_meta(id,max_rows,max_bytes) VALUES(1,8,1048576)",
        [],
    )?;
    capture.execute(
        "INSERT INTO capture_requests(request_key,payload,expires) VALUES('pipeline-fixture','{}',1e100)",
        [],
    )?;
    for wallet in [
        "BkXHpvTpA8LRB9D76YRp3WPpwpNesY5YXAgiaZdMR6rD",
        "ATtEeUyiFAjvw5dgcxxBDKht3EQVyyiTabJDSdFhp2e7",
    ] {
        capture.execute("INSERT INTO capture_members VALUES(1,?)", [wallet])?;
    }
    let mut config = copybot_config::AppConfig::default();
    config.ingestion.source = "yellowstone_grpc".into();
    config.ingestion.yellowstone_grpc_url = "http://127.0.0.1:1".into();
    config.ingestion.yellowstone_x_token = "offline-fixture".into();
    config.ingestion.capture_scope_db = Some(capture_path.to_string_lossy().into());
    assert!(!config.execution.enabled);
    assert!(!config.execution.canary_tiny_submit_enabled);
    copybot_config::validate_association_delivery(&config)?;
    // The replay opens only the local capture consumer. It never starts a
    // daemon, provider stream, HTTP client, quote worker or financial action.
    let replay = CaptureReplay::open(&config.ingestion).await?;
    replay.accept_pending().await?;
    assert_eq!(
        capture.query_row("SELECT state FROM capture_requests", [], |r| r
            .get::<_, String>(0))?,
        "ACKED"
    );
    let writer = ObservedSwapWriter::start_for_test(app_path.to_string_lossy().into(), 8, 8)?;
    let follow = Arc::new(FollowSnapshot::default());
    let runtime_lots = HashSet::new();
    let mut critical_mints = HashSet::new();
    let mut refresh = DiscoveryCriticalTargetBuyMintsBackpressureRefreshState::default();
    let mut budget = ZeroUniverseEmptyTargetNoncriticalBestEffortState::default();
    let mut pending = VecDeque::new();
    let mut signatures = HashSet::new();
    let mut signature_order = VecDeque::new();
    let mut telemetry = AppConsumerLoopTelemetry::default();
    let fixtures: [&[u8]; 2] = [
        include_bytes!("../../tests/fixtures/capture/quote_sol_wallet_06_sell.pb"),
        include_bytes!("../../tests/fixtures/capture/quote_sol_wallet_11_buy.pb"),
    ];
    for (index, bytes) in fixtures.into_iter().enumerate() {
        writer.set_capture_test_journal_inflight_rows(usize::from(index == 0));
        let swap = replay
            .push(bytes)
            .await?
            .context("saved supported swap decoded")?;
        if index == 0 {
            assert_eq!(swap.slot, 454_297_846);
            assert_eq!(swap.signature,
                "HrHLo3FJC8U8HN9PSq4dzsrvqsBBqfFkFxbfKHUZ7P5VYtp8A4nQujz4p71rEuK13MxtgrGtPidc5URYQfEWEbh");
            assert_eq!(swap.wallet, "BkXHpvTpA8LRB9D76YRp3WPpwpNesY5YXAgiaZdMR6rD");
            assert_eq!(
                swap.token_out,
                "So11111111111111111111111111111111111111112"
            );
            assert_eq!(
                swap.exact_amounts.as_ref().unwrap().amount_in_raw,
                "1085477023"
            );
            assert_eq!(
                swap.exact_amounts.as_ref().unwrap().amount_out_raw,
                "146964030"
            );
            assert!(
                !writer.try_enqueue(&swap)?,
                "original inflight refusal still applies"
            );
        } else {
            assert_eq!(swap.slot, 454_298_676);
            assert_eq!(swap.signature,
                "2pWUc4qNtx4WH2Kp4icg6FQ42dYKDQKBDpZqgnXaUQEEZo4C4hKRHHG63wAbFbwKoJzRjLQj9RTzAEKpy9adSmh7");
            assert_eq!(swap.wallet, "ATtEeUyiFAjvw5dgcxxBDKht3EQVyyiTabJDSdFhp2e7");
            assert_eq!(swap.token_in, "So11111111111111111111111111111111111111112");
            assert_eq!(
                swap.exact_amounts.as_ref().unwrap().amount_in_raw,
                "21616157"
            );
            assert_eq!(
                swap.exact_amounts.as_ref().unwrap().amount_out_raw,
                "115186657"
            );
            assert!(
                budget.exhausted(),
                "the BUY reaches the original active cooldown"
            );
        }
        let (stage, saved): (String, String) = capture.query_row(
            "SELECT stage,event_json FROM capture_events WHERE signature=?",
            [&swap.signature],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        assert_eq!(
            stage, "DURABLE",
            "capture commits before legacy writer admission"
        );
        let captured: SwapEvent = serde_json::from_str(&saved)?;
        assert_eq!(captured.signature, swap.signature);
        assert_eq!(captured.wallet, swap.wallet);
        assert_eq!(captured.token_in, swap.token_in);
        assert_eq!(captured.token_out, swap.token_out);
        assert_eq!(captured.exact_amounts, swap.exact_amounts);
        assert!(note_recent_swap_signature(
            &mut signatures,
            &mut signature_order,
            &swap.signature
        ));
        handle_irrelevant_observed_swap(
            &store,
            &writer,
            swap.clone(),
            IrrelevantObservedSwapBackpressureSourceBranch::NotFollowed,
            None,
            &follow,
            &runtime_lots,
            true,
            &mut critical_mints,
            &mut refresh,
            &mut budget,
            &mut pending,
            &mut signatures,
            &mut signature_order,
            &mut telemetry,
            StdInstant::now(),
        )
        .await?;
        assert!(budget.exhausted());
        assert!(pending.is_empty());
        assert!(!signatures.contains(&swap.signature));
        assert!(store.load_observed_swaps_since(swap.ts_utc)?.is_empty());
        assert_eq!(
            capture.query_row(
                "SELECT count(*) FROM capture_events WHERE stage='DURABLE'",
                [],
                |r| r.get::<_, i64>(0),
            )?,
            index as i64 + 1
        );
        // Same canonical transaction is decoded again but never creates a second
        // captured event, including after the legacy handler forgot its signature.
        assert!(replay.push(bytes).await?.is_some());
        assert_eq!(
            capture.query_row("SELECT count(*) FROM capture_events", [], |r| r
                .get::<_, i64>(0))?,
            index as i64 + 1
        );
    }
    writer.shutdown()?;
    drop(replay);
    drop(store);
    drop(capture);
    for path in [&app_path, &capture_path] {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }
    eprintln!(
        "capture joint replay: actual ingress decoder DURABLE=2 (saved SELL+BUY), \
               legacy inflight/cooldown durable=0; duplicates=2, captured distinct=2; \
               execution/tiny=false, no provider/daemon/HTTP/signature submission"
    );
    Ok(())
}
