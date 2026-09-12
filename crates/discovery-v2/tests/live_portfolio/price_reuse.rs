use super::{fixture, price_fixture as p, rpc};
use anyhow::Result;
use chrono::Duration;
use copybot_discovery_v2::{
    load_materialized_discovery_v2_status_for_publish, materialize_discovery_v2_status,
    publish_discovery_v2_status, reusable_materialized_discovery_v2_status_for_prepare,
    revalidate_discovery_v2_status, DiscoveryV2DecisionContext, DiscoveryV2Status,
};
use serde_json::json;

#[test]
fn b35_expired_capital_refuses_reuse_and_direct_publish_before_status_ttl() -> Result<()> {
    let mut f = p::fixture()?;
    p::observation(
        &f,
        'E',
        Duration::minutes(120) - Duration::seconds(10),
        0.1,
        true,
    )?;
    let server = fixture::responses(0, json!([p::token('E', 'J', "10000000")]), json!([]));
    let original_options =
        p::options(&f, f.now).with_live_portfolio_rpc_url(Some(server.url.clone()));
    let (original, _) = materialize_discovery_v2_status(
        &f.store,
        &f.discovery,
        &f.shadow,
        original_options.clone(),
    )?;
    rpc::assert_requests(&server.finish(), &rpc::key('A'), 3);
    assert!(original.production_green);
    assert!(
        publish_discovery_v2_status(
            &f.store,
            original.clone(),
            true,
            168,
            DiscoveryV2DecisionContext::new(&f.discovery, &f.shadow, &original_options)
        )?
        .committed
    );
    let before = serde_json::to_string(&f.store.discovery_publication_state_read_only()?)?;
    let saved_json = f
        .store
        .discovery_v2_status_snapshot_read_only()?
        .unwrap()
        .status_json;
    let reopened = f.reopen()?;
    let later = p::options(&f, f.now + Duration::seconds(20));
    for commit in [false, true] {
        let decoded: DiscoveryV2Status = serde_json::from_str(&saved_json)?;
        let error = publish_discovery_v2_status(
            &reopened,
            decoded,
            commit,
            168,
            DiscoveryV2DecisionContext::new(&f.discovery, &f.shadow, &later),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("price_support_expired_rebuild_required"),
            "{error}"
        );
        println!("B35 DIRECT_REFUSAL commit={commit} status_age=20 error={error}");
    }
    let error = load_materialized_discovery_v2_status_for_publish(
        &reopened,
        &f.discovery,
        &f.shadow,
        &later,
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("price_support_expired_rebuild_required"));
    assert!(reusable_materialized_discovery_v2_status_for_prepare(
        &reopened,
        &f.discovery,
        &f.shadow,
        &later
    )?
    .is_none());
    assert_eq!(
        reopened
            .discovery_v2_status_snapshot_read_only()?
            .unwrap()
            .status_json,
        saved_json
    );
    assert_eq!(
        serde_json::to_string(&reopened.discovery_publication_state_read_only()?)?,
        before
    );
    assert_eq!(reopened.list_active_follow_wallets()?.len(), 1);
    // A regular new RPC rebuild now explains the rejection and unchanged floor.
    f.now = later.now;
    let rebuilt = p::build(&f, 0, json!([p::token('E', 'J', "10000000")]), json!([]))?;
    p::assert_unknown(&rebuilt, 0.0);
    assert_eq!(rebuilt.filters.total_wallets, 2);
    assert_eq!(rebuilt.filters.eligible_wallets, 0);
    let report = p::report(&f, rebuilt.clone(), f.now)?;
    assert!(report.filter_impact.below_publish_floor);
    assert_eq!(report.filter_impact.publish_min_candidate_wallets, 1);
    let row = report
        .top_rejected_wallets
        .iter()
        .find(|row| row.wallet_id == rpc::key('A'))
        .unwrap();
    assert!(row.active_follow && !row.filters.live_portfolio_pass);
    assert!(publish_discovery_v2_status(
        &reopened,
        rebuilt,
        true,
        168,
        DiscoveryV2DecisionContext::new(&f.discovery, &f.shadow, &later)
    )
    .is_err());
    assert_eq!(
        serde_json::to_string(&reopened.discovery_publication_state_read_only()?)?,
        before
    );
    Ok(())
}

#[test]
fn b35_reuse_recomputes_remaining_fresh_capital_and_preserves_sol_control() -> Result<()> {
    for (sol, fresh_price, accepted) in
        [(0, 0.03, true), (0, 0.01, false), (300_000_000, 0.0, true)]
    {
        let f = p::fixture()?;
        p::observation(
            &f,
            'E',
            Duration::minutes(120) - Duration::seconds(10),
            0.1,
            true,
        )?;
        if fresh_price > 0.0 {
            p::observation(&f, 'F', Duration::minutes(20), fresh_price, true)?;
        }
        let mut rows = vec![p::token('E', 'J', "10000000")];
        if fresh_price > 0.0 {
            rows.push(p::token('F', 'K', "10000000"));
        }
        let server = fixture::responses(sol, json!(rows), json!([]));
        let options = p::options(&f, f.now).with_live_portfolio_rpc_url(Some(server.url.clone()));
        let (original, _) =
            materialize_discovery_v2_status(&f.store, &f.discovery, &f.shadow, options.clone())?;
        rpc::assert_requests(&server.finish(), &rpc::key('A'), 3);
        assert!(original.production_green);
        let stored = f
            .store
            .discovery_v2_status_snapshot_read_only()?
            .unwrap()
            .status_json;
        let later = p::options(&f, f.now + Duration::seconds(20));
        let reopened = f.reopen()?;
        let loaded = load_materialized_discovery_v2_status_for_publish(
            &reopened,
            &f.discovery,
            &f.shadow,
            &later,
        );
        assert_eq!(loaded.is_ok(), accepted);
        if let Ok((loaded, metadata)) = loaded {
            assert_eq!(metadata.status_age_seconds, 20);
            assert_eq!(metadata.max_status_age_seconds, 1800);
            let row = fixture::metric(&loaded);
            assert!(
                (row["live_token_value_sol"].as_f64().unwrap() - fresh_price * 10.0).abs() < 1e-12
            );
            assert_eq!(row["live_inventory"]["unvalued_token_positions"], 1);
            assert_eq!(
                row["live_valuation"]["classic_positions"],
                fixture::metric(&original)["live_valuation"]["classic_positions"]
            );
            assert_eq!(loaded.now, original.now);
            assert!(reusable_materialized_discovery_v2_status_for_prepare(
                &reopened,
                &f.discovery,
                &f.shadow,
                &later
            )?
            .is_some());
            let decoded = serde_json::from_str(&serde_json::to_string(&loaded)?)?;
            let committed = publish_discovery_v2_status(
                &reopened,
                decoded,
                true,
                168,
                DiscoveryV2DecisionContext::new(&f.discovery, &f.shadow, &later),
            )?;
            assert!(committed.committed);
            assert_eq!(
                reopened
                    .discovery_publication_state_read_only()?
                    .unwrap()
                    .last_published_at,
                Some(original.now)
            );
            p::report(&f, loaded, later.now)?;
            println!("B35 REUSED_STATUS {}", serde_json::to_string(&committed)?);
        }
        assert_eq!(
            reopened
                .discovery_v2_status_snapshot_read_only()?
                .unwrap()
                .status_json,
            stored
        );
    }
    Ok(())
}

#[test]
fn b35_legacy_incomplete_or_inconsistent_price_proof_requires_rebuild() -> Result<()> {
    let f = p::fixture()?;
    let server = fixture::responses(
        300_000_000,
        json!([p::token('D', 'J', "10000000")]),
        json!([]),
    );
    let options = p::options(&f, f.now).with_live_portfolio_rpc_url(Some(server.url.clone()));
    let (original, _) =
        materialize_discovery_v2_status(&f.store, &f.discovery, &f.shadow, options.clone())?;
    rpc::assert_requests(&server.finish(), &rpc::key('A'), 3);
    assert!(
        publish_discovery_v2_status(
            &f.store,
            original.clone(),
            true,
            168,
            DiscoveryV2DecisionContext::new(&f.discovery, &f.shadow, &options)
        )?
        .committed
    );
    let before = serde_json::to_string(&f.store.discovery_publication_state_read_only()?)?;
    let stored = f.store.discovery_v2_status_snapshot_read_only()?.unwrap();
    for case in [
        "global",
        "metric",
        "version",
        "truncated",
        "subtotal",
        "future",
        "observation-missing",
        "numeric-proof",
        "sol-proof",
        "floor",
    ] {
        let mut value = serde_json::to_value(&original)?;
        let index = value["wallet_metrics"]
            .as_array()
            .unwrap()
            .iter()
            .position(|row| row["wallet_id"] == rpc::key('A'))
            .unwrap();
        match case {
            "global" => {
                value["live_portfolio"]
                    .as_object_mut()
                    .unwrap()
                    .remove("valuation_contract_version");
            }
            "metric" => {
                value["wallet_metrics"][index]
                    .as_object_mut()
                    .unwrap()
                    .remove("live_valuation");
            }
            "version" => {
                value["wallet_metrics"][index]["live_valuation"]["contract_version"] = json!(2)
            }
            "truncated" => {
                value["wallet_metrics"][index]["live_valuation"]["classic_positions"] = json!([])
            }
            "numeric-proof" => {
                value["wallet_metrics"][index]["live_valuation"]["classic_positions"][0]
                    ["token_amount_bits"] = json!(0u64)
            }
            "sol-proof" => {
                value["wallet_metrics"][index]["live_valuation"]["sol_balance_bits"] =
                    json!(f64::INFINITY.to_bits())
            }
            "floor" => value["candidate_wallets"] = json!([]),
            "subtotal" => value["wallet_metrics"][index]["live_token_value_sol"] = json!(999.0),
            "future" => {
                value["wallet_metrics"][index]["live_valuation"]["classic_positions"][0]
                    ["observation"]["observed_at"] = json!(f.now + Duration::seconds(1))
            }
            _ => {
                value["wallet_metrics"][index]["live_valuation"]["classic_positions"][0]
                    ["observation"]
                    .as_object_mut()
                    .unwrap()
                    .remove("observed_at");
            }
        }
        f.store.persist_discovery_v2_status_snapshot(
            &original.policy_fingerprint,
            original.now,
            original.window_start,
            stored.runtime_cursor.as_ref(),
            &value.to_string(),
        )?;
        assert!(
            load_materialized_discovery_v2_status_for_publish(
                &f.store,
                &f.discovery,
                &f.shadow,
                &options
            )
            .is_err(),
            "{case}"
        );
        assert!(reusable_materialized_discovery_v2_status_for_prepare(
            &f.store,
            &f.discovery,
            &f.shadow,
            &options
        )?
        .is_none());
        if let Ok(decoded) = serde_json::from_value(value) {
            assert!(
                publish_discovery_v2_status(
                    &f.store,
                    decoded,
                    true,
                    168,
                    DiscoveryV2DecisionContext::new(&f.discovery, &f.shadow, &options)
                )
                .is_err(),
                "{case}"
            );
        }
        assert_eq!(
            serde_json::to_string(&f.store.discovery_publication_state_read_only()?)?,
            before
        );
    }
    // Current policy and clock must be explicit even for sufficient SOL.
    let mut changed = f.discovery.clone();
    changed.min_live_sol_balance /= 2.0;
    assert!(revalidate_discovery_v2_status(
        original.clone(),
        DiscoveryV2DecisionContext::new(&changed, &f.shadow, &options)
    )
    .is_err());
    let mut changed_options = options.clone();
    changed_options.window_minutes = 119;
    assert!(revalidate_discovery_v2_status(
        original.clone(),
        DiscoveryV2DecisionContext::new(&f.discovery, &f.shadow, &changed_options)
    )
    .is_err());
    changed_options = options.clone();
    changed_options.now -= Duration::seconds(1);
    assert!(revalidate_discovery_v2_status(
        original,
        DiscoveryV2DecisionContext::new(&f.discovery, &f.shadow, &changed_options)
    )
    .is_err());
    Ok(())
}

#[test]
fn b35_inventory_budget_has_complete_price_coverage_not_a_detail_sample() -> Result<()> {
    let f = p::fixture()?;
    let mut rows = Vec::new();
    for (mint, account) in "EFGHJKLM".chars().zip("abcdefgh".chars()) {
        p::observation(&f, mint, Duration::minutes(121), 0.1, true)?;
        rows.push(p::token(mint, account, "10000000"));
    }
    let status = p::build(&f, 300_000_000, json!(rows), json!([]))?;
    let metric = fixture::metric(&status);
    assert_eq!(
        metric["live_valuation"]["classic_positions"]
            .as_array()
            .unwrap()
            .len(),
        8
    );
    assert_eq!(metric["live_valuation"]["positive_positions"], 8);
    assert_eq!(
        metric["live_valuation"]["decision"]["unvalued_positions"],
        8
    );
    assert_eq!(metric["live_token_value_sol"], json!(0.0));
    let report = p::report(&f, status, f.now)?;
    assert_eq!(
        report.wallets[0]
            .live_valuation
            .as_ref()
            .unwrap()
            .classic_positions
            .len(),
        8
    );
    Ok(())
}
