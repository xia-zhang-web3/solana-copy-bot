#[test]
fn status_rejects_non_tradable_open_lot_as_publication_evidence() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    let good_token = "GoodOpenProofToken1111111111111111111111111";
    let rejected_token = "RejectedOpenToken11111111111111111111111111";
    let tail_token = "TailTokenOpen11111111111111111111111111111";
    store.insert_observed_swaps_batch(&[
        swap_with_token(
            "wallet_a",
            good_token,
            "sig-good-buy",
            10,
            now - Duration::minutes(20),
        ),
        sell_with_token(
            "wallet_a",
            good_token,
            "sig-good-sell",
            11,
            now - Duration::minutes(19),
        ),
        swap_with_token(
            "wallet_a",
            rejected_token,
            "sig-rejected-open",
            12,
            now - Duration::minutes(10),
        ),
        swap_with_token(
            "tail_wallet",
            tail_token,
            "sig-tail",
            13,
            now - Duration::minutes(2),
        ),
    ])?;
    insert_quality_for_token(&store, good_token, now, Some(1.0))?;
    insert_quality_for_token(&store, rejected_token, now, Some(0.1))?;
    insert_quality_for_token(&store, tail_token, now, Some(1.0))?;
    let (mut discovery, shadow) = strict_policy();
    discovery.min_buy_count = 2;

    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    let wallet = status
        .wallet_metrics
        .iter()
        .find(|metric| metric.wallet_id == "wallet_a")
        .expect("wallet_a metric");
    assert!(!status.production_green);
    assert!(wallet
        .reject_reasons
        .contains(&"open_position_required_missing".to_string()));
    Ok(())
}

#[test]
fn status_blocks_when_candidate_floor_is_not_met() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    store.insert_observed_swaps_batch(&[
        swap("wallet_a", "sig-a", 10, now - Duration::minutes(10)),
        tail_coverage_swap("sig-tail", 11, now - Duration::minutes(8)),
    ])?;
    insert_quality(&store, now, Some(1.0))?;
    let (mut discovery, shadow) = strict_policy();
    discovery.follow_top_n = 2;

    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    assert!(!status.production_green);
    assert_eq!(status.candidate_wallets, vec!["wallet_a".to_string()]);
    assert!(status
        .blockers
        .contains(&"discovery_v2_candidate_wallets_below_publish_floor".to_string()));
    Ok(())
}

#[test]
fn status_blocks_old_open_position_without_hold_history() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    let stale_open_token = "StaleOpenToken111111111111111111111111111";
    let tail_token = "TailToken222222222222222222222222222222222";
    store.insert_observed_swaps_batch(&[
        swap_with_token(
            "wallet_a",
            stale_open_token,
            "sig-stale-open",
            10,
            now - Duration::hours(2),
        ),
        swap_with_token(
            "tail_wallet",
            tail_token,
            "sig-tail",
            11,
            now - Duration::minutes(5),
        ),
    ])?;
    insert_quality_for_token(&store, stale_open_token, now, Some(1.0))?;
    insert_quality_for_token(&store, tail_token, now, Some(1.0))?;
    let (mut discovery, shadow) = strict_policy();
    discovery.follow_top_n = 2;

    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    let wallet = status
        .wallet_metrics
        .iter()
        .find(|metric| metric.wallet_id == "wallet_a")
        .expect("wallet_a metric");
    assert!(!status.production_green);
    assert!(wallet
        .reject_reasons
        .contains(&"open_position_required_missing".to_string()));
    Ok(())
}

#[test]
fn status_blocks_when_tail_is_future_dated() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    store.insert_observed_swap(&swap(
        "wallet_a",
        "sig-future",
        10,
        now + Duration::minutes(1),
    ))?;
    let (discovery, shadow) = strict_policy();

    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    assert!(!status.production_green);
    assert!(status.tail.as_ref().is_some_and(|tail| tail.future_dated));
    assert!(status
        .blockers
        .contains(&"observed_swaps_tail_future_dated".to_string()));
    Ok(())
}

#[test]
fn status_blocks_when_scan_time_budget_is_exhausted() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    store.insert_observed_swaps_batch(&[
        swap("wallet_a", "sig-a", 10, now - Duration::minutes(10)),
        tail_coverage_swap("sig-tail", 11, now - Duration::minutes(8)),
    ])?;
    insert_quality(&store, now, Some(1.0))?;
    let (discovery, shadow) = strict_policy();
    let mut options = options(now);
    options.time_budget_ms = 0;

    let status = build_discovery_v2_status(&store, &discovery, &shadow, options)?;

    assert!(!status.production_green);
    assert!(status.scan.time_budget_exhausted);
    assert!(status
        .blockers
        .contains(&"discovery_v2_scan_budget_exhausted".to_string()));
    Ok(())
}

#[test]
fn status_blocks_when_window_scan_is_truncated() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    let old_token = "OldToken111111111111111111111111111111111";
    let new_token = "NewToken111111111111111111111111111111111";
    store.insert_observed_swaps_batch(&[
        swap_with_token(
            "old_wallet",
            old_token,
            "sig-old",
            10,
            now - Duration::hours(20),
        ),
        swap_with_token(
            "new_wallet",
            new_token,
            "sig-new",
            11,
            now - Duration::minutes(2),
        ),
    ])?;
    insert_quality_for_token(&store, old_token, now, Some(1.0))?;
    insert_quality_for_token(&store, new_token, now, Some(1.0))?;
    let (discovery, shadow) = strict_policy();
    let mut options = options(now);
    options.max_rows = 1;

    let status = build_discovery_v2_status(&store, &discovery, &shadow, options)?;

    assert!(!status.production_green);
    assert_eq!(status.scan.rows_scanned, 1);
    assert!(status.scan.max_rows_exhausted);
    assert_eq!(status.scan.unique_wallets, 1);
    assert_eq!(status.wallet_metrics_total, 1);
    assert_eq!(status.wallet_metrics_returned, 0);
    assert!(status.wallet_metrics_truncated);
    assert!(status.wallet_metrics.is_empty());
    assert!(status
        .blockers
        .contains(&"discovery_v2_scan_budget_exhausted".to_string()));
    Ok(())
}

#[test]
fn status_prefilters_wallets_below_buy_gate_before_metric_scan() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    let token = "CandidatePrefilterToken11111111111111111111";
    let mut swaps = vec![
        tail_coverage_swap("sig-coverage-floor", 1, now - Duration::hours(25)),
        swap_with_token("wallet_a", token, "sig-a-buy-1", 10, now - Duration::minutes(20)),
        swap_with_token("wallet_a", token, "sig-a-buy-2", 11, now - Duration::minutes(19)),
        sell_with_token("wallet_a", token, "sig-a-sell", 12, now - Duration::minutes(18)),
    ];
    for index in 0..20 {
        swaps.push(swap_with_token(
            &format!("noise_wallet_{index}"),
            &format!("NoiseToken{index:02}111111111111111111111111"),
            &format!("sig-noise-{index}"),
            20 + index,
            now - Duration::minutes(10),
        ));
    }
    swaps.push(tail_coverage_swap("sig-tail", 100, now - Duration::minutes(1)));
    store.insert_observed_swaps_batch(&swaps)?;
    insert_quality_for_token(&store, token, now, Some(1.0))?;
    let (mut discovery, shadow) = strict_policy();
    discovery.min_buy_count = 2;

    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    assert!(status.production_green, "blockers={:?}", status.blockers);
    assert_eq!(status.candidate_wallets, vec!["wallet_a".to_string()]);
    assert_eq!(status.scan.rows_scanned, 3);
    assert_eq!(status.scan.unique_wallets, 1);
    assert_eq!(status.wallet_metrics_total, 1);
    Ok(())
}

#[test]
fn status_blocks_when_rug_lookahead_is_unevaluated() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    store.insert_observed_swap(&swap("wallet_a", "sig-a", 10, now - Duration::minutes(1)))?;
    insert_quality(&store, now, Some(1.0))?;
    let (mut discovery, shadow) = strict_policy();
    discovery.rug_lookahead_seconds = 900;

    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    assert!(!status.production_green);
    assert_eq!(status.wallet_metrics[0].rug_lookahead_evaluated, 0);
    assert_eq!(status.wallet_metrics[0].rug_lookahead_unevaluated, 1);
    assert!(status.wallet_metrics[0]
        .reject_reasons
        .contains(&"rug_lookahead_unevaluated".to_string()));
    Ok(())
}

#[test]
fn status_blocks_rug_lookahead_until_tail_covers_window() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    store.insert_observed_swaps_batch(&[
        swap_with_token(
            "wallet_a",
            TOKEN_MINT,
            "sig-a",
            10,
            now - Duration::minutes(15),
        ),
        swap_with_token(
            "tail_wallet",
            "TailToken111111111111111111111111111111111",
            "sig-tail",
            11,
            now - Duration::minutes(10),
        ),
    ])?;
    insert_quality(&store, now, Some(1.0))?;
    insert_quality_for_token(
        &store,
        "TailToken111111111111111111111111111111111",
        now,
        Some(1.0),
    )?;
    let (mut discovery, shadow) = strict_policy();
    discovery.rug_lookahead_seconds = 900;

    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    let wallet = status
        .wallet_metrics
        .iter()
        .find(|metric| metric.wallet_id == "wallet_a")
        .expect("wallet_a metric");
    assert!(!status.production_green);
    assert_eq!(wallet.rug_lookahead_unevaluated, 1);
    assert!(wallet
        .reject_reasons
        .contains(&"rug_lookahead_unevaluated".to_string()));
    Ok(())
}
