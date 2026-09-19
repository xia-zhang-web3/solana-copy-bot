use super::*;
use crate::app_loop_irrelevant_swap::handle_irrelevant_observed_swap;

#[tokio::test]
async fn capture_scope_baseline_saved_sell_is_discarded_with_external_lot_and_inflight(
) -> Result<()> {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/capture/saved_sell_446979602.json"
    ))?;
    let swap: SwapEvent = serde_json::from_value(fixture["swap"].clone())?;
    assert_eq!(swap.slot, 446_979_602);
    assert_eq!(swap.wallet, fixture["external_lot"]["wallet"]);
    assert_eq!(swap.token_in, fixture["external_lot"]["mint"]);
    let pre: u64 = fixture["pre_token_raw"].as_str().unwrap().parse()?;
    let post: u64 = fixture["post_token_raw"].as_str().unwrap().parse()?;
    assert_eq!(pre - post, 1_144_131_279);
    assert!((swap.amount_in - (pre - post) as f64 / 1e6).abs() < 1e-8);
    assert_eq!(
        swap.amount_out,
        (fixture["post_native_lamports"].as_u64().unwrap()
            - fixture["pre_native_lamports"].as_u64().unwrap()) as f64
            / 1e9
    );

    // Separate external storage exists, but the original runtime knows neither
    // its admitted wallets nor its open lots. No original session DB is opened.
    let external = Connection::open_in_memory()?;
    external.execute_batch("CREATE TABLE virtual_lots(wallet TEXT, mint TEXT, status TEXT)")?;
    external.execute(
        "INSERT INTO virtual_lots VALUES (?1, ?2, 'OPEN')",
        params![swap.wallet, swap.token_in],
    )?;
    let external_open: u64 = external.query_row(
        "SELECT COUNT(*) FROM virtual_lots WHERE status='OPEN'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(external_open, 1);
    let (store, path) = make_test_store("capture-scope-baseline")?;
    let writer = ObservedSwapWriter::start_for_test(path.to_string_lossy().into(), 8, 8)?;
    let follow = Arc::new(FollowSnapshot::default());
    let runtime_lots = HashSet::new();
    let mut critical_mints = HashSet::new();
    let mut refresh = DiscoveryCriticalTargetBuyMintsBackpressureRefreshState::default();
    let mut budget = ZeroUniverseEmptyTargetNoncriticalBestEffortState::default();
    let mut pending = VecDeque::new();
    let mut signatures = HashSet::new();
    let mut signature_order = VecDeque::new();
    let mut telemetry = AppConsumerLoopTelemetry::default();
    assert!(
        !irrelevant_observed_swap_requires_discovery_critical_persistence(
            &swap,
            &follow,
            &runtime_lots,
            true,
            &critical_mints
        )
    );

    writer.set_capture_test_journal_inflight_rows(1);
    assert_eq!(writer.snapshot().journal_writer_inflight_rows, 1);
    assert_eq!(writer.snapshot().pending_requests, 0);
    assert!(
        !writer.try_enqueue(&swap)?,
        "actual writer refuses one inflight row"
    );
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

    // Removing inflight pressure alone does not restore the best-effort budget.
    writer.set_capture_test_journal_inflight_rows(0);
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
    assert_eq!(writer.snapshot().pending_requests, 0);
    assert!(store.load_observed_swaps_since(swap.ts_utc)?.is_empty());

    // Causal control: advance the existing cooldown clock, then the same real
    // handler accepts the saved event and the real writer commits exactly once.
    budget.refresh_after_writer_pressure_clears(
        &follow,
        &runtime_lots,
        true,
        &critical_mints,
        &writer.snapshot(),
        StdInstant::now() + StdDuration::from_secs(5),
    );
    assert!(!budget.exhausted());
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
    assert!(signatures.contains(&swap.signature));
    assert!(
        !writer.write(&swap).await?,
        "ordered duplicate ACK proves prior commit"
    );
    writer.shutdown()?;
    let rows = store.load_observed_swaps_since(swap.ts_utc)?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].signature, swap.signature);
    eprintln!(
        "capture baseline: saved SELL decoded fixture; external OPEN=1; runtime lots/follow=0; \
         injected inflight=1 -> actual try_enqueue=false, handler durable=0, pending=0; \
         inflight=0 inside cooldown -> durable=0; cooldown elapsed -> durable=1; \
         historical received/decoded facts remain UNKNOWN"
    );
    drop(store);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
    Ok(())
}
