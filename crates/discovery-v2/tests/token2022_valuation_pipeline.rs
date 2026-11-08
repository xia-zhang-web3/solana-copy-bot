//! Same black-box test compiles on the exact old Discovery source: no new profile types.
use anyhow::Result;
use copybot_discovery_v2::{
    build_discovery_v2_status, load_materialized_discovery_v2_status_for_publish,
    materialize_discovery_v2_status, publish_discovery_v2_status, DiscoveryV2DecisionContext,
};
use serde_json::{json, Value};
#[path = "token2022_valuation_pipeline/fixture.rs"]
mod fixture;
#[path = "token2022_valuation_pipeline/raw_rpc.rs"]
mod raw_rpc;
#[path = "live_portfolio/rpc.rs"]
mod rpc;
use fixture::{metric, Fixture, PriceCase};
use raw_rpc::RawCase;

fn close(actual: &Value, expected: f64) {
    assert!(
        (actual.as_f64().unwrap() - expected).abs() < 1e-12,
        "{actual} != {expected}"
    );
}

fn original_tuples(row: &Value) -> Value {
    let proof = &row["live_valuation"];
    json!({"wallet":row["wallet_id"], "observed_as_of":proof["observed_as_of"],
        "observed_window_start":proof["observed_window_start"], "sol_balance_bits":proof["sol_balance_bits"],
        "token_2022_positions":proof["token_2022_positions"]})
}

#[test]
fn supported_token2022_actual_rpc_builder_materialize_load_and_dry_publish() -> Result<()> {
    for case in [RawCase::Supported165, RawCase::Supported170] {
        let f = Fixture::new(PriceCase::Fresh)?;
        let server = raw_rpc::start(0, 500, case); // .5 tokens x observed .1 SOL = literal .05 floor.
        let options = f.options(&server);
        let built = build_discovery_v2_status(&f.store, &f.discovery, &f.shadow, options.clone())?;
        let row = metric(&built);
        // Intended causal baseline RED: exact old aaba source blanket-excludes this same inventory.
        assert!(
            built.production_green,
            "CAUSAL_TOKEN2022_SUPPORTED_FLOOR_GREEN_REQUIRED {case:?}: {row} blockers={:?}",
            built.blockers
        );
        assert_eq!(built.candidate_wallets, vec![rpc::key('A')]);
        close(&row["live_token_value_sol"], 0.0); // historical alias remains classic-only.
        close(&row["live_inventory"]["known_classic_value_sol"], 0.0);
        close(&row["live_inventory"]["known_token_2022_value_sol"], 0.05);
        close(
            &row["live_inventory"]["known_supported_token_value_sol"],
            0.05,
        );
        assert_eq!(
            row["live_valuation"]["decision"]["valued_token_2022_positions"],
            1
        );
        assert_eq!(row["live_valuation"]["decision"]["unvalued_positions"], 0);
        let profile = &row["live_valuation"]["token_2022_positions"][0]["token_2022_profile"];
        assert_eq!(profile["wallet"], rpc::key('A'));
        assert_eq!(profile["mint"], rpc::key('E'));
        assert_eq!(profile["mint_account"]["context_slot"], 124);
        assert_eq!(
            profile["mint_account"]["data"].as_array().unwrap().len(),
            82
        );
        assert_eq!(profile["accounts"][0]["expected_raw_amount"], 500);
        assert_eq!(profile["accounts"][0]["expected_decimals"], 3);
        assert_eq!(
            profile["accounts"][0]["raw"]["data"]
                .as_array()
                .unwrap()
                .len(),
            if matches!(case, RawCase::Supported170) {
                170
            } else {
                165
            }
        );
        let original = original_tuples(&row);
        let (materialized, saved) =
            materialize_discovery_v2_status(&f.store, &f.discovery, &f.shadow, options.clone())?;
        assert!(saved.committed);
        assert!(materialized.production_green);
        assert_eq!(original_tuples(&metric(&materialized)), original);
        let (loaded, _) = load_materialized_discovery_v2_status_for_publish(
            &f.store,
            &f.discovery,
            &f.shadow,
            &options,
        )?;
        assert!(loaded.production_green);
        assert_eq!(original_tuples(&metric(&loaded)), original);
        let decoded = serde_json::from_slice(&serde_json::to_vec(&loaded)?)?;
        let published = publish_discovery_v2_status(
            &f.store,
            decoded,
            false,
            168,
            DiscoveryV2DecisionContext::new(&f.discovery, &f.shadow, &options),
        )?;
        assert!(published.dry_run && !published.committed && published.status.production_green);
        assert_eq!(published.published_wallet_count, 1);
        assert_eq!(original_tuples(&metric(&published.status)), original);
        assert!(f.store.list_active_follow_wallets()?.is_empty());
        raw_rpc::assert_calls(&server.finish(), 2);
    }
    Ok(())
}

#[test]
fn token_value_point04_and_sol_point02_do_not_add_to_floor_point05() -> Result<()> {
    let f = Fixture::new(PriceCase::Fresh)?;
    assert_eq!(f.discovery.min_live_sol_balance, 0.05);
    assert_eq!(f.discovery.min_live_portfolio_value_sol, 0.05);
    let server = raw_rpc::start(20_000_000, 400, RawCase::Supported165);
    let options = f.options(&server);
    let status = build_discovery_v2_status(&f.store, &f.discovery, &f.shadow, options.clone())?;
    let row = metric(&status);
    assert!(
        !status.production_green && status.candidate_wallets.is_empty(),
        "{row}"
    );
    close(&row["live_sol_balance"], 0.02);
    close(&row["live_token_value_sol"], 0.0);
    close(
        &row["live_valuation"]["decision"]["known_supported_token_value_sol"],
        0.04,
    );
    assert_eq!(row["live_valuation"]["decision"]["unvalued_positions"], 0);
    assert!(row["reject_reasons"]
        .as_array()
        .unwrap()
        .contains(&json!("only_dust_positions")));
    let published = publish_discovery_v2_status(
        &f.store,
        status,
        false,
        168,
        DiscoveryV2DecisionContext::new(&f.discovery, &f.shadow, &options),
    )?;
    assert_eq!(published.published_wallet_count, 0);
    assert!(published.dry_run && !published.committed);
    raw_rpc::assert_calls(&server.finish(), 1);
    Ok(())
}

#[test]
fn raw_collection_missing_unsupported_frozen_and_binding_cannot_fund_candidate() -> Result<()> {
    for (case, expected) in [
        (RawCase::MissingMint, "token2022_profile_missing"),
        (RawCase::Unsupported, "token2022_profile_unsupported"),
        (RawCase::Frozen, "token2022_profile_frozen"),
        (RawCase::AmountMismatch, "token2022_profile_binding"),
        (RawCase::WalletMismatch, "token2022_profile_binding"),
        (RawCase::ProgramMismatch, "token2022_profile_binding"),
    ] {
        let f = Fixture::new(PriceCase::Fresh)?;
        let server = raw_rpc::start(0, 500, case);
        let status =
            build_discovery_v2_status(&f.store, &f.discovery, &f.shadow, f.options(&server))?;
        let row = metric(&status);
        assert!(
            !status.production_green && status.candidate_wallets.is_empty(),
            "{case:?}: {row}"
        );
        close(
            &row["live_valuation"]["decision"]["known_supported_token_value_sol"],
            0.0,
        );
        assert_eq!(row["live_valuation"]["decision"]["unvalued_positions"], 1);
        assert_eq!(
            row["live_valuation"]["decision"]["unknown_reasons"][expected], 1,
            "{case:?}: {row}"
        );
        raw_rpc::assert_calls(&server.finish(), 1);
    }
    Ok(())
}

#[test]
fn supported_raw_profile_still_requires_current_observed_price_and_quality() -> Result<()> {
    for (case, expected) in [
        (PriceCase::Stale, "price_outside_window"),
        (PriceCase::Future, "price_missing"),
        (PriceCase::QualityFailed, "quality_not_eligible"),
    ] {
        let f = Fixture::new(case)?;
        let server = raw_rpc::start(0, 500, RawCase::Supported170);
        let status =
            build_discovery_v2_status(&f.store, &f.discovery, &f.shadow, f.options(&server))?;
        let row = metric(&status);
        assert!(
            !status.production_green && status.candidate_wallets.is_empty(),
            "{case:?}: {row}"
        );
        assert!(row["live_valuation"]["token_2022_positions"][0]["token_2022_profile"].is_object());
        close(
            &row["live_valuation"]["decision"]["known_supported_token_value_sol"],
            0.0,
        );
        assert_eq!(
            row["live_valuation"]["decision"]["unknown_reasons"][expected], 1,
            "{case:?}: {row}"
        );
        // A future-only observed row is excluded by the existing as-of SQL; never refreshed/fabricated.
        raw_rpc::assert_calls(&server.finish(), 1);
    }
    Ok(())
}
