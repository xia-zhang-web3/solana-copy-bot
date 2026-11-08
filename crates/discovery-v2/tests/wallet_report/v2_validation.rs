//! V2 proof reuse boundaries: no provider or wall-clock-dependent fixture.
#[path = "r1_layout.rs"]
mod r1_layout;
use super::{policy, proof, status, test_store};
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_discovery_v2::{
    build_discovery_v2_wallet_report, discovery_v2_policy_fingerprint,
    load_materialized_discovery_v2_status_for_publish, publish_discovery_v2_status,
    revalidate_discovery_v2_status, DiscoveryV2DecisionContext,
    DiscoveryV2PriceUnknownReason as Reason, DiscoveryV2Status, DiscoveryV2WalletReportOptions,
    Token2022ProfileCollectionFailure, TOKEN_2022_PROGRAM,
};

#[path = "../token2022_profile/fixture.rs"]
mod profile_fixture;

fn fixture() -> DiscoveryV2Status {
    let now = DateTime::parse_from_rfc3339("2026-11-08T10:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let mut status = status(now);
    let metric = &mut status.wallet_metrics[0];
    metric.wallet_id = profile_fixture::key(0);
    metric.live_sol_balance = Some(0.0);
    metric.live_token_value_sol = Some(0.05);
    metric.live_token_positions = Some(3);
    let valuation = metric.live_valuation.as_mut().unwrap();
    valuation.sol_balance_bits = 0.0f64.to_bits();
    for (row, price) in valuation.classic_positions.iter_mut().zip([0.02f64, 0.03]) {
        row.observation.as_mut().unwrap().price_sol = price;
        row.price_sol_bits = Some(price.to_bits());
    }
    let mut row = valuation.classic_positions[0].clone();
    row.token_program = TOKEN_2022_PROGRAM.into();
    row.mint = profile_fixture::key(1);
    row.token_amount = 2.0;
    row.token_amount_bits = 2.0f64.to_bits();
    row.token_2022_profile = Some(profile_fixture::profile(6, &[2_000_000]));
    row.observation.as_mut().unwrap().price_sol = 0.2;
    row.price_sol_bits = Some(0.2f64.to_bits());
    valuation.token_2022_positions = vec![row];
    valuation.token_2022_positive_positions = 1;
    valuation.positive_positions = 3;
    valuation.decision.known_classic_value_sol = 0.05;
    valuation.decision.known_token_2022_value_sol = 0.4;
    valuation.decision.known_supported_token_value_sol = 0.45;
    valuation.decision.valued_token_2022_positions = 1;
    valuation.decision.valued_supported_positions = 3;
    let inventory = metric.live_inventory.as_mut().unwrap();
    inventory.token_2022_accounts = 1;
    inventory.token_2022_positive_positions = 1;
    inventory.known_classic_value_sol = 0.05;
    inventory.known_token_2022_value_sol = 0.4;
    inventory.known_supported_token_value_sol = 0.45;
    inventory.valued_token_2022_positions = 1;
    inventory.valued_supported_positions = 3;
    status.candidate_wallets = vec![profile_fixture::key(0)];
    let (discovery, shadow) = policy();
    status.policy_fingerprint =
        discovery_v2_policy_fingerprint(&discovery, &shadow, &proof::options(now));
    status
}

fn reuse(status: DiscoveryV2Status, now: DateTime<Utc>) -> Result<DiscoveryV2Status> {
    let (discovery, shadow) = policy();
    revalidate_discovery_v2_status(
        status,
        DiscoveryV2DecisionContext::new(&discovery, &shadow, &proof::options(now)),
    )
}

fn unknown(status: &mut DiscoveryV2Status, reason: Reason) {
    let metric = &mut status.wallet_metrics[0];
    metric.live_sol_balance = Some(1.0);
    let valuation = metric.live_valuation.as_mut().unwrap();
    valuation.sol_balance_bits = 1.0f64.to_bits();
    valuation.token_2022_positions[0].initial_unknown_reason = Some(reason);
    valuation.decision.known_token_2022_value_sol = 0.0;
    valuation.decision.known_supported_token_value_sol = 0.05;
    valuation.decision.valued_token_2022_positions = 0;
    valuation.decision.valued_supported_positions = 2;
    valuation.decision.unvalued_positions = 1;
    valuation.decision.unknown_reasons = [(reason, 1)].into();
    let inventory = metric.live_inventory.as_mut().unwrap();
    inventory.known_token_2022_value_sol = 0.0;
    inventory.known_supported_token_value_sol = 0.05;
    inventory.valued_token_2022_positions = 0;
    inventory.valued_supported_positions = 2;
    inventory.unvalued_token_positions = 1;
}

#[test]
fn v2_supported_token2022_survives_materialization_publish_and_wallet_report() -> Result<()> {
    let status = fixture();
    let now = status.now;
    let (discovery, shadow) = policy();
    let (_dir, store) = test_store()?;
    store.persist_discovery_v2_status_snapshot(
        &status.policy_fingerprint,
        now,
        status.window_start,
        None,
        &serde_json::to_string(&status)?,
    )?;
    let (loaded, _) = load_materialized_discovery_v2_status_for_publish(
        &store,
        &discovery,
        &shadow,
        &proof::options(now),
    )?;
    let published = publish_discovery_v2_status(
        &store,
        loaded.clone(),
        false,
        168,
        DiscoveryV2DecisionContext::new(&discovery, &shadow, &proof::options(now)),
    )?;
    assert!(published.dry_run && !published.committed);
    let report = build_discovery_v2_wallet_report(
        &store,
        &discovery,
        &shadow,
        loaded,
        DiscoveryV2WalletReportOptions {
            now,
            limit: 10,
            include_rejected: true,
        },
        DiscoveryV2DecisionContext::new(&discovery, &shadow, &proof::options(now)),
    )?;
    let row = &report.wallets[0];
    assert!(row.filters.live_portfolio_pass);
    assert_eq!(row.live_token_value_sol, Some(0.05)); // Compatibility alias remains classic only.
    assert_eq!(row.live_valuation.as_ref().unwrap().observed_as_of, now);
    assert_eq!(
        row.live_inventory
            .as_ref()
            .unwrap()
            .known_token_2022_value_sol,
        0.4
    );
    assert_eq!(
        row.live_inventory
            .as_ref()
            .unwrap()
            .valued_supported_positions,
        3
    );
    Ok(())
}

#[test]
fn v2_rejects_forged_profile_program_amount_subtotals_versions_and_quality_context() {
    let changes: Vec<Box<dyn Fn(&mut DiscoveryV2Status)>> = vec![
        Box::new(|s| {
            s.wallet_metrics[0]
                .live_valuation
                .as_mut()
                .unwrap()
                .token_2022_positions[0]
                .token_2022_profile
                .as_mut()
                .unwrap()
                .mint_account
                .context_slot = 10;
        }),
        Box::new(|s| {
            s.wallet_metrics[0]
                .live_valuation
                .as_mut()
                .unwrap()
                .token_2022_positions[0]
                .token_2022_profile
                .as_mut()
                .unwrap()
                .accounts[0]
                .raw
                .context_slot = 10;
        }),
        Box::new(|s| {
            s.wallet_metrics[0]
                .live_valuation
                .as_mut()
                .unwrap()
                .token_2022_positions[0]
                .token_2022_collection_failure =
                Some(Token2022ProfileCollectionFailure::Unavailable);
        }),
        Box::new(|s| {
            s.wallet_metrics[0]
                .live_valuation
                .as_mut()
                .unwrap()
                .classic_positions[0]
                .token_2022_collection_failure = Some(Token2022ProfileCollectionFailure::Missing);
        }),
        Box::new(|s| {
            s.wallet_metrics[0]
                .live_valuation
                .as_mut()
                .unwrap()
                .token_2022_positions[0]
                .token_2022_profile = Some(profile_fixture::profile(6, &[1_000_000, 1_000_000]));
        }),
        Box::new(|s| {
            s.wallet_metrics[0]
                .live_inventory
                .as_mut()
                .unwrap()
                .contract_version = 1
        }),
        Box::new(|s| {
            s.wallet_metrics[0]
                .live_valuation
                .as_mut()
                .unwrap()
                .contract_version = 1
        }),
        Box::new(|s| {
            s.wallet_metrics[0]
                .live_valuation
                .as_mut()
                .unwrap()
                .token_2022_positions
                .clear()
        }),
        Box::new(|s| {
            s.wallet_metrics[0]
                .live_valuation
                .as_mut()
                .unwrap()
                .token_2022_positions[0]
                .token_program = "classic".into()
        }),
        Box::new(|s| {
            let row = &mut s.wallet_metrics[0]
                .live_valuation
                .as_mut()
                .unwrap()
                .token_2022_positions[0];
            row.token_amount = 3.0;
            row.token_amount_bits = 3.0f64.to_bits();
        }),
        Box::new(|s| {
            s.wallet_metrics[0]
                .live_valuation
                .as_mut()
                .unwrap()
                .decision
                .known_supported_token_value_sol = 0.9
        }),
        Box::new(|s| {
            s.wallet_metrics[0]
                .live_inventory
                .as_mut()
                .unwrap()
                .known_token_2022_value_sol = 0.9
        }),
        Box::new(|s| {
            s.wallet_metrics[0]
                .live_inventory
                .as_mut()
                .unwrap()
                .valued_supported_positions = 2
        }),
        Box::new(|s| {
            s.wallet_metrics[0]
                .live_valuation
                .as_mut()
                .unwrap()
                .token_2022_positions[0]
                .quality_gate_enabled = false
        }),
        Box::new(|s| {
            s.wallet_metrics[0]
                .live_valuation
                .as_mut()
                .unwrap()
                .token_2022_positions[0]
                .quality_fetched_at = None
        }),
        Box::new(|s| {
            s.wallet_metrics[0].wallet_id = profile_fixture::key(5);
            s.candidate_wallets[0] = profile_fixture::key(5);
        }),
        Box::new(|s| {
            s.wallet_metrics[0]
                .live_valuation
                .as_mut()
                .unwrap()
                .token_2022_positions[0]
                .token_2022_profile
                .as_mut()
                .unwrap()
                .accounts[0]
                .raw
                .data[64] ^= 1
        }),
    ];
    for (index, change) in changes.into_iter().enumerate() {
        let mut status = fixture();
        change(&mut status);
        let now = status.now;
        assert!(reuse(status, now).is_err(), "accepted forged case {index}");
    }
}

#[test]
fn v2_missing_and_unsupported_profiles_remain_unknown_with_independent_sol_floor() -> Result<()> {
    for reason in [
        Reason::Token2022ProfileMissing,
        Reason::Token2022ProfileUnsupported,
    ] {
        let mut status = fixture();
        let row = &mut status.wallet_metrics[0]
            .live_valuation
            .as_mut()
            .unwrap()
            .token_2022_positions[0];
        if reason == Reason::Token2022ProfileMissing {
            row.token_2022_profile = None;
            row.token_2022_collection_failure = Some(Token2022ProfileCollectionFailure::Missing);
        } else {
            row.token_2022_profile.as_mut().unwrap().contract_version = 255;
        }
        unknown(&mut status, reason);
        let now = status.now;
        let checked = reuse(status, now)?;
        let decision = &checked.wallet_metrics[0]
            .live_valuation
            .as_ref()
            .unwrap()
            .decision;
        assert_eq!(decision.known_token_2022_value_sol, 0.0);
        assert_eq!(decision.unvalued_positions, 1);
        assert_eq!(decision.unknown_reasons[&reason], 1);
    }
    Ok(())
}

#[test]
fn v2_quality_support_expires_without_refreshing_original_evidence() -> Result<()> {
    let mut status = fixture();
    let now = status.now;
    let fetched = now - Duration::seconds(7190);
    status.wallet_metrics[0]
        .live_valuation
        .as_mut()
        .unwrap()
        .token_2022_positions[0]
        .quality_fetched_at = Some(fetched);
    let at_boundary = reuse(status.clone(), now + Duration::seconds(10))?;
    assert_eq!(
        at_boundary.wallet_metrics[0]
            .live_valuation
            .as_ref()
            .unwrap()
            .token_2022_positions[0]
            .quality_fetched_at,
        Some(fetched)
    );
    let error = reuse(status, now + Duration::seconds(11)).unwrap_err();
    assert!(error.to_string().contains("price_support_expired"));
    Ok(())
}

#[test]
fn v2_future_price_never_promotes_when_later_time_reaches_it() -> Result<()> {
    let mut status = fixture();
    let now = status.now;
    status.wallet_metrics[0]
        .live_valuation
        .as_mut()
        .unwrap()
        .token_2022_positions[0]
        .observation
        .as_mut()
        .unwrap()
        .observed_at = now + Duration::seconds(1);
    unknown(&mut status, Reason::PriceFuture);
    let checked = reuse(status, now + Duration::seconds(2))?;
    let decision = &checked.wallet_metrics[0]
        .live_valuation
        .as_ref()
        .unwrap()
        .decision;
    assert_eq!(decision.known_token_2022_value_sol, 0.0);
    assert_eq!(decision.unknown_reasons[&Reason::PriceFuture], 1);
    Ok(())
}

#[test]
fn v2_quality_disabled_needs_no_fetched_time_but_requires_matching_policy() -> Result<()> {
    let mut status = fixture();
    let now = status.now;
    let (discovery, mut shadow) = policy();
    shadow.quality_gates_enabled = false;
    for metric in &mut status.wallet_metrics {
        let proof = metric.live_valuation.as_mut().unwrap();
        for row in proof
            .classic_positions
            .iter_mut()
            .chain(&mut proof.token_2022_positions)
        {
            row.quality_gate_enabled = false;
            row.quality_fetched_at = None;
        }
    }
    status.policy_fingerprint =
        discovery_v2_policy_fingerprint(&discovery, &shadow, &proof::options(now));
    assert!(
        revalidate_discovery_v2_status(
            status,
            DiscoveryV2DecisionContext::new(&discovery, &shadow, &proof::options(now))
        )?
        .production_green
    );
    Ok(())
}

#[test]
fn v2_rejects_missing_collection_provenance_and_reused_supported_account_pubkey() {
    let mut missing = fixture();
    missing.wallet_metrics[0]
        .live_valuation
        .as_mut()
        .unwrap()
        .token_2022_positions[0]
        .token_2022_profile = None;
    unknown(&mut missing, Reason::Token2022ProfileMissing);
    let now = missing.now;
    assert!(reuse(missing, now).is_err());

    let mut duplicated = fixture();
    let metric = &mut duplicated.wallet_metrics[0];
    let valuation = metric.live_valuation.as_mut().unwrap();
    let mut second = valuation.token_2022_positions[0].clone();
    second.mint = profile_fixture::key(6);
    let profile = second.token_2022_profile.as_mut().unwrap();
    profile.mint = second.mint.clone();
    profile.accounts[0].raw.data[31] = 6;
    valuation.token_2022_positions.push(second);
    valuation.token_2022_positive_positions = 2;
    valuation.positive_positions = 4;
    valuation.decision.known_token_2022_value_sol = 0.8;
    valuation.decision.known_supported_token_value_sol = 0.85;
    valuation.decision.valued_token_2022_positions = 2;
    valuation.decision.valued_supported_positions = 4;
    metric.live_token_positions = Some(4);
    let inventory = metric.live_inventory.as_mut().unwrap();
    inventory.token_2022_accounts = 2;
    inventory.token_2022_positive_positions = 2;
    inventory.known_token_2022_value_sol = 0.8;
    inventory.known_supported_token_value_sol = 0.85;
    inventory.valued_token_2022_positions = 2;
    inventory.valued_supported_positions = 4;
    let now = duplicated.now;
    assert!(reuse(duplicated, now).is_err());
}
