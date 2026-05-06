#[test]
fn status_blocks_when_shadow_quality_thresholds_are_disabled() -> Result<()> {
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    let cases: [(&str, fn(&mut ShadowConfig)); 5] = [
        ("discovery_v2_quality_token_age_gate_disabled", |shadow| {
            shadow.min_token_age_seconds = 0;
        }),
        ("discovery_v2_quality_holder_gate_disabled", |shadow| {
            shadow.min_holders = 0;
        }),
        ("discovery_v2_quality_liquidity_gate_disabled", |shadow| {
            shadow.min_liquidity_sol = 0.0;
        }),
        (
            "discovery_v2_quality_rolling_volume_gate_disabled",
            |shadow| {
                shadow.min_volume_5m_sol = 0.0;
            },
        ),
        (
            "discovery_v2_quality_rolling_trader_gate_disabled",
            |shadow| {
                shadow.min_unique_traders_5m = 0;
            },
        ),
    ];

    for (idx, (blocker, mutate_shadow)) in cases.into_iter().enumerate() {
        let (_dir, store) = test_store()?;
        let sig = format!("sig-disabled-quality-{idx}");
        let tail_sig = format!("sig-disabled-quality-tail-{idx}");
        store.insert_observed_swaps_batch(&[
            swap(
                "wallet_a",
                &sig,
                10 + idx as u64,
                now - Duration::minutes(10),
            ),
            tail_coverage_swap(&tail_sig, 20 + idx as u64, now - Duration::minutes(8)),
        ])?;
        insert_quality(&store, now, Some(1.0))?;
        let (discovery, mut shadow) = strict_policy();
        mutate_shadow(&mut shadow);

        let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

        assert!(!status.production_green, "{blocker}");
        assert!(status.blockers.contains(&blocker.to_string()), "{blocker}");
    }
    Ok(())
}

#[test]
fn status_blocks_when_liquidity_quality_evidence_is_missing() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    store.insert_observed_swaps_batch(&[
        swap("wallet_a", "sig-a", 10, now - Duration::minutes(10)),
        tail_coverage_swap("sig-tail", 11, now - Duration::minutes(8)),
    ])?;
    insert_quality(&store, now, None)?;
    let (discovery, shadow) = strict_policy();

    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    assert!(!status.production_green);
    assert!(status.candidate_wallets.is_empty());
    let wallet = status
        .wallet_metrics
        .iter()
        .find(|metric| metric.wallet_id == "wallet_a")
        .expect("wallet_a metric");
    assert_eq!(wallet.tradable_ratio, 0.0);
    assert!(wallet
        .reject_reasons
        .contains(&"low_tradable_ratio".to_string()));
    assert_eq!(wallet.missing_quality_evidence_buys, 1);
    assert!(wallet
        .reject_reasons
        .contains(&"token_quality_evidence_missing".to_string()));
    Ok(())
}

#[test]
fn status_blocks_when_token_quality_evidence_is_stale() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    store.insert_observed_swaps_batch(&[
        swap("wallet_a", "sig-a", 10, now - Duration::minutes(10)),
        tail_coverage_swap("sig-tail", 11, now - Duration::minutes(8)),
    ])?;
    insert_quality(&store, now - Duration::minutes(20), Some(1.0))?;
    let (discovery, shadow) = strict_policy();

    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    assert!(!status.production_green);
    let wallet = status
        .wallet_metrics
        .iter()
        .find(|metric| metric.wallet_id == "wallet_a")
        .expect("wallet_a metric");
    assert_eq!(wallet.missing_quality_evidence_buys, 1);
    assert!(wallet
        .reject_reasons
        .contains(&"token_quality_evidence_missing".to_string()));
    Ok(())
}

#[test]
fn status_blocks_when_token_quality_evidence_is_future_dated() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    store.insert_observed_swaps_batch(&[
        swap("wallet_a", "sig-a", 10, now - Duration::minutes(10)),
        tail_coverage_swap("sig-tail", 11, now - Duration::minutes(8)),
    ])?;
    insert_quality(&store, now + Duration::minutes(5), Some(1.0))?;
    let (discovery, shadow) = strict_policy();

    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    assert!(!status.production_green);
    let wallet = status
        .wallet_metrics
        .iter()
        .find(|metric| metric.wallet_id == "wallet_a")
        .expect("wallet_a metric");
    assert_eq!(wallet.missing_quality_evidence_buys, 1);
    assert!(wallet
        .reject_reasons
        .contains(&"token_quality_evidence_missing".to_string()));
    Ok(())
}

#[test]
fn status_blocks_partial_missing_quality_even_when_ratio_floor_passes() -> Result<()> {
    let (_dir, store) = test_store()?;
    let now = DateTime::parse_from_rfc3339("2026-05-03T10:00:00Z")?.with_timezone(&Utc);
    store.insert_observed_swaps_batch(&[
        swap_with_token(
            "wallet_a",
            "GoodToken11111111111111111111111111111111",
            "sig-a",
            10,
            now - Duration::minutes(10),
        ),
        swap_with_token(
            "wallet_a",
            "MissingToken11111111111111111111111111111",
            "sig-b",
            11,
            now - Duration::minutes(9),
        ),
        swap_with_token(
            "wallet_a",
            "MissingToken22222222222222222222222222222",
            "sig-c",
            12,
            now - Duration::minutes(8),
        ),
        swap_with_token(
            "wallet_a",
            "MissingToken33333333333333333333333333333",
            "sig-d",
            13,
            now - Duration::minutes(7),
        ),
    ])?;
    insert_quality_for_token(
        &store,
        "GoodToken11111111111111111111111111111111",
        now,
        Some(1.0),
    )?;
    let (discovery, shadow) = strict_policy();

    let status = build_discovery_v2_status(&store, &discovery, &shadow, options(now))?;

    assert!(!status.production_green);
    assert!(status.candidate_wallets.is_empty());
    assert_eq!(status.wallet_metrics[0].tradable_ratio, 0.25);
    assert_eq!(status.wallet_metrics[0].missing_quality_evidence_buys, 3);
    assert!(status.wallet_metrics[0]
        .reject_reasons
        .contains(&"token_quality_evidence_missing".to_string()));
    Ok(())
}
