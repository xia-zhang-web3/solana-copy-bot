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
fn status_uses_non_future_tail_when_absolute_tail_has_small_clock_skew() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    store.insert_observed_swaps_batch(&[
        tail_coverage_swap("sig-coverage-floor", 9, now - Duration::hours(25)),
        swap("wallet_a", "sig-a", 10, now - Duration::minutes(10)),
        tail_coverage_swap("sig-tail-ready", 11, now - Duration::seconds(5)),
        tail_coverage_swap("sig-tail-small-skew", 12, now + Duration::seconds(15)),
    ])?;
    insert_quality(&store, now, Some(1.0))?;
    let (discovery, shadow) = strict_policy();

    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    assert!(status.production_green);
    assert!(status.blockers.is_empty());
    let tail = status.tail.as_ref().expect("tail status");
    assert_eq!(tail.cursor.signature, "sig-tail-ready");
    assert!(!tail.future_dated);
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

#[test]
fn shadow_feedback_rejects_losing_wallet_and_promotes_replacement() -> Result<()> {
    let dir = tempdir()?;
    let mut store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    ensure_discovery_v2_schema(&store)?;

    let now = DateTime::parse_from_rfc3339("2026-05-14T10:00:00Z")?.with_timezone(&Utc);
    let token_a = "ShadowFeedbackToken111111111111111111111111";
    let token_b = "ShadowFeedbackToken222222222222222222222222";
    let token_c = "ShadowFeedbackToken333333333333333333333333";
    let bad_wallet = "wallet_shadow_bad";
    let good_wallet = "wallet_shadow_good";
    let replacement_wallet = "wallet_shadow_replacement";

    let mut swaps = vec![tail_coverage_swap(
        "sig-shadow-coverage-floor",
        1,
        now - Duration::hours(25),
    )];
    for index in 0_u32..4 {
        let offset = i64::from(index);
        swaps.push(swap_with_token(
            bad_wallet,
            token_a,
            &format!("sig-shadow-bad-{index}"),
            10 + u64::from(index),
            now - Duration::seconds(410 - offset),
        ));
    }
    for index in 0_u32..2 {
        let offset = i64::from(index);
        swaps.push(swap_with_token(
            good_wallet,
            token_b,
            &format!("sig-shadow-good-{index}"),
            20 + u64::from(index),
            now - Duration::seconds(350 - offset),
        ));
        swaps.push(swap_with_token(
            replacement_wallet,
            token_c,
            &format!("sig-shadow-replacement-{index}"),
            30 + u64::from(index),
            now - Duration::seconds(290 - offset),
        ));
    }
    swaps.push(tail_coverage_swap(
        "sig-shadow-tail",
        99,
        now - Duration::minutes(2),
    ));
    store.insert_observed_swaps_batch(&swaps)?;
    insert_quality_for_token(&store, token_a, now, Some(1.0))?;
    insert_quality_for_token(&store, token_b, now, Some(1.0))?;
    insert_quality_for_token(&store, token_c, now, Some(1.0))?;
    for index in 0_u32..3 {
        store.insert_shadow_closed_trade(
            &format!("shadow-loss-{index}"),
            bad_wallet,
            token_a,
            1.0,
            0.10,
            0.06,
            -0.04,
            now - Duration::hours(2),
            now - Duration::hours(1) + Duration::minutes(i64::from(index)),
        )?;
    }

    let (mut discovery, shadow) = strict_policy();
    discovery.follow_top_n = 2;
    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    assert!(status.production_green, "{:?}", status.blockers);
    assert_eq!(status.candidate_wallets.len(), 2);
    assert!(!status.candidate_wallets.contains(&bad_wallet.to_string()));
    assert!(status.candidate_wallets.contains(&good_wallet.to_string()));
    assert!(status
        .candidate_wallets
        .contains(&replacement_wallet.to_string()));
    assert_eq!(
        status
            .filters
            .reject_breakdown
            .get("shadow_feedback_negative"),
        Some(&1)
    );
    let bad_metric = status
        .wallet_metrics
        .iter()
        .find(|metric| metric.wallet_id == bad_wallet)
        .expect("bad wallet metric retained");
    assert!(!bad_metric.eligible);
    assert!(bad_metric
        .reject_reasons
        .contains(&"shadow_feedback_negative".to_string()));
    assert_eq!(bad_metric.shadow_closed_trades_24h, Some(3));
    assert!(bad_metric
        .shadow_pnl_sol_24h
        .is_some_and(|pnl| (pnl + 0.12).abs() < 1e-9));
    assert!(bad_metric
        .shadow_roi_24h
        .is_some_and(|roi| roi <= -0.39 && roi >= -0.41));
    Ok(())
}

#[test]
fn shadow_feedback_rejects_single_catastrophic_loss() -> Result<()> {
    let dir = tempdir()?;
    let mut store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    ensure_discovery_v2_schema(&store)?;

    let now = DateTime::parse_from_rfc3339("2026-05-14T10:00:00Z")?.with_timezone(&Utc);
    let bad_wallet = "wallet_shadow_catastrophe";
    let good_wallet = "wallet_shadow_catastrophe_good";
    let replacement_wallet = "wallet_shadow_catastrophe_replacement";
    let token_a = "ShadowCatastropheToken11111111111111111111";
    let token_b = "ShadowCatastropheToken22222222222222222222";
    let token_c = "ShadowCatastropheToken33333333333333333333";

    let swaps = vec![
        tail_coverage_swap("sig-catastrophe-coverage", 1, now - Duration::hours(25)),
        swap_with_token(bad_wallet, token_a, "sig-catastrophe-bad", 10, now - Duration::minutes(8)),
        swap_with_token(good_wallet, token_b, "sig-catastrophe-good", 20, now - Duration::minutes(7)),
        swap_with_token(
            replacement_wallet,
            token_c,
            "sig-catastrophe-replacement",
            30,
            now - Duration::minutes(6),
        ),
        tail_coverage_swap("sig-catastrophe-tail", 99, now - Duration::minutes(2)),
    ];
    store.insert_observed_swaps_batch(&swaps)?;
    insert_quality_for_token(&store, token_a, now, Some(1.0))?;
    insert_quality_for_token(&store, token_b, now, Some(1.0))?;
    insert_quality_for_token(&store, token_c, now, Some(1.0))?;
    store.insert_shadow_closed_trade(
        "shadow-catastrophic-loss",
        bad_wallet,
        token_a,
        1.0,
        0.20,
        0.06,
        -0.14,
        now - Duration::hours(1),
        now - Duration::minutes(30),
    )?;

    let (mut discovery, shadow) = strict_policy();
    discovery.follow_top_n = 2;
    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    assert!(status.production_green, "{:?}", status.blockers);
    assert_eq!(status.candidate_wallets.len(), 2);
    assert!(!status.candidate_wallets.contains(&bad_wallet.to_string()));
    assert!(status.candidate_wallets.contains(&good_wallet.to_string()));
    assert!(status
        .candidate_wallets
        .contains(&replacement_wallet.to_string()));
    assert_eq!(
        status
            .filters
            .reject_breakdown
            .get("shadow_feedback_catastrophic_loss"),
        Some(&1)
    );
    let bad_metric = status
        .wallet_metrics
        .iter()
        .find(|metric| metric.wallet_id == bad_wallet)
        .expect("bad wallet metric retained");
    assert!(!bad_metric.eligible);
    assert!(bad_metric
        .reject_reasons
        .contains(&"shadow_feedback_catastrophic_loss".to_string()));
    assert_eq!(bad_metric.shadow_closed_trades_24h, Some(1));
    assert!(bad_metric
        .shadow_roi_24h
        .is_some_and(|roi| roi <= -0.69 && roi >= -0.71));
    Ok(())
}
