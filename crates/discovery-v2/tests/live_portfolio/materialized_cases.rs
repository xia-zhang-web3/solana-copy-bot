use super::{
    fixture::*,
    rpc::{self, CLASSIC, TOKEN_2022},
};
use anyhow::Result;
use chrono::Duration;
use copybot_discovery_v2::{
    build_discovery_v2_wallet_report, load_materialized_discovery_v2_status_for_publish,
    materialize_discovery_v2_status, publish_discovery_v2_status,
    reusable_materialized_discovery_v2_status_for_prepare, DiscoveryV2Status,
    DiscoveryV2WalletReportOptions,
};
use serde_json::json;

#[test]
fn materialized_inventory_round_trip_and_wallet_report_keep_unknown_subtotal() -> Result<()> {
    let f = Fixture::new()?;
    let rpc = responses(
        0,
        json!([token(CLASSIC, "10000000")]),
        json!([token(TOKEN_2022, "10000000")]),
    );
    let options = f.options(&rpc);
    let (status, saved) =
        materialize_discovery_v2_status(&f.store, &f.discovery, &f.shadow, options.clone())?;
    rpc::assert_requests(&rpc.finish(), &rpc::key('A'), 3);
    assert!(saved.committed);
    assert_outcome(&status, None, false);
    let (loaded, _) = load_materialized_discovery_v2_status_for_publish(
        &f.store,
        &f.discovery,
        &f.shadow,
        &options,
    )?;
    assert_eq!(metric(&loaded), metric(&status));
    assert!(reusable_materialized_discovery_v2_status_for_prepare(
        &f.store,
        &f.discovery,
        &f.shadow,
        &options
    )?
    .is_some());
    assert!(
        publish_discovery_v2_status(
            &f.store,
            loaded.clone(),
            true,
            168,
            copybot_discovery_v2::DiscoveryV2DecisionContext::new(
                &f.discovery,
                &f.shadow,
                &super::options(f.now)
            )
        )?
        .committed
    );
    let report = build_discovery_v2_wallet_report(
        &f.store,
        &f.discovery,
        &f.shadow,
        loaded,
        DiscoveryV2WalletReportOptions {
            now: f.now,
            limit: 10,
            include_rejected: true,
        },
        copybot_discovery_v2::DiscoveryV2DecisionContext::new(
            &f.discovery,
            &f.shadow,
            &super::options(f.now),
        ),
    )?;
    assert!(report.wallets[0].filters.live_portfolio_pass);
    let report_json = serde_json::to_value(&report)?;
    assert_eq!(
        report_json["wallets"][0]["live_inventory"],
        metric(&status)["live_inventory"]
    );
    assert_eq!(
        report_json["wallets"][0]["live_inventory"]["unvalued_token_positions"],
        1
    );
    println!(
        "B29_MATERIALIZED_STATUS {}",
        serde_json::to_string(&status)?
    );
    println!("B29_WALLET_REPORT {}", serde_json::to_string(&report)?);
    Ok(())
}

#[test]
fn active_leader_is_revalidated_and_refusal_keeps_old_followlist() -> Result<()> {
    let mut f = Fixture::new()?;
    let rpc = responses(0, json!([token(CLASSIC, "10000000")]), json!([]));
    let status = f.build(&rpc)?;
    rpc::assert_requests(&rpc.finish(), &rpc::key('A'), 3);
    assert!(
        publish_discovery_v2_status(
            &f.store,
            status,
            true,
            168,
            copybot_discovery_v2::DiscoveryV2DecisionContext::new(
                &f.discovery,
                &f.shadow,
                &super::options(f.now)
            )
        )?
        .committed
    );
    let previous = serde_json::to_value(f.store.discovery_publication_state_read_only()?)?;
    for (modern, reason, failure) in [
        (json!([]), "capital_drained_after_window", false),
        (
            json!([token(TOKEN_2022, "10")]),
            "live_portfolio_token_2022_valuation_unknown",
            false,
        ),
        (
            json!([{"account":{"data":{}}}]),
            "live_portfolio_inventory_malformed",
            true,
        ),
    ] {
        f.now += Duration::seconds(5);
        let rpc = responses(0, json!([]), modern);
        let status = f.build(&rpc)?;
        rpc::assert_requests(&rpc.finish(), &rpc::key('A'), 3);
        assert_outcome(&status, Some(reason), failure);
        let report = build_discovery_v2_wallet_report(
            &f.store,
            &f.discovery,
            &f.shadow,
            status.clone(),
            DiscoveryV2WalletReportOptions {
                now: f.now,
                limit: 10,
                include_rejected: true,
            },
            copybot_discovery_v2::DiscoveryV2DecisionContext::new(
                &f.discovery,
                &f.shadow,
                &super::options(f.now),
            ),
        )?;
        let leader = report
            .top_rejected_wallets
            .iter()
            .find(|r| r.wallet_id == rpc::key('A'))
            .unwrap();
        assert!(leader.active_follow);
        assert!(!leader.filters.live_portfolio_pass);
        assert!(leader.reject_reasons.contains(&reason.to_string()));
        assert!(publish_discovery_v2_status(
            &f.store,
            status,
            true,
            168,
            copybot_discovery_v2::DiscoveryV2DecisionContext::new(
                &f.discovery,
                &f.shadow,
                &super::options(f.now)
            )
        )
        .is_err());
        assert_eq!(
            serde_json::to_value(f.store.discovery_publication_state_read_only()?)?,
            previous
        );
        assert_eq!(
            f.store
                .list_active_follow_wallets()?
                .into_iter()
                .collect::<Vec<_>>(),
            vec![rpc::key('A')]
        );
        println!(
            "B29_REJECTED_WALLET_REPORT {}",
            serde_json::to_string(&report)?
        );
    }
    Ok(())
}

#[test]
fn legacy_inventory_status_requires_rebuild_before_reuse_or_publish() -> Result<()> {
    let f = Fixture::new()?;
    let rpc = responses(1_000_000_000, json!([]), json!([]));
    let options = f.options(&rpc);
    let (status, _) =
        materialize_discovery_v2_status(&f.store, &f.discovery, &f.shadow, options.clone())?;
    rpc::assert_requests(&rpc.finish(), &rpc::key('A'), 3);
    assert!(
        publish_discovery_v2_status(
            &f.store,
            status.clone(),
            true,
            168,
            copybot_discovery_v2::DiscoveryV2DecisionContext::new(
                &f.discovery,
                &f.shadow,
                &super::options(f.now)
            )
        )?
        .committed
    );
    let before = serde_json::to_value(f.store.discovery_publication_state_read_only()?)?;
    let row = f.store.discovery_v2_status_snapshot_read_only()?.unwrap();
    for case in [
        "legacy",
        "missing-wallet-proof",
        "future-contract",
        "missing-global-proof",
    ] {
        let mut value = serde_json::to_value(&status)?;
        if case == "legacy" {
            value["live_portfolio"]
                .as_object_mut()
                .unwrap()
                .remove("inventory_contract_version");
        }
        if matches!(case, "legacy" | "missing-wallet-proof") {
            for wallet in value["wallet_metrics"].as_array_mut().unwrap() {
                wallet.as_object_mut().unwrap().remove("live_inventory");
            }
        }
        if case == "future-contract" {
            value["live_portfolio"]["inventory_contract_version"] = json!(2);
        }
        if case == "missing-global-proof" {
            value.as_object_mut().unwrap().remove("live_portfolio");
        }
        f.store.persist_discovery_v2_status_snapshot(
            &status.policy_fingerprint,
            status.now,
            status.window_start,
            row.runtime_cursor.as_ref(),
            &value.to_string(),
        )?;
        let error = load_materialized_discovery_v2_status_for_publish(
            &f.store,
            &f.discovery,
            &f.shadow,
            &options,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("coverage_unverified"),
            "{case}: {error}"
        );
        assert!(reusable_materialized_discovery_v2_status_for_prepare(
            &f.store,
            &f.discovery,
            &f.shadow,
            &options
        )?
        .is_none());
        let legacy: DiscoveryV2Status = serde_json::from_value(value)?;
        assert!(publish_discovery_v2_status(
            &f.store,
            legacy.clone(),
            true,
            168,
            copybot_discovery_v2::DiscoveryV2DecisionContext::new(
                &f.discovery,
                &f.shadow,
                &super::options(f.now)
            )
        )
        .is_err());
        assert!(build_discovery_v2_wallet_report(
            &f.store,
            &f.discovery,
            &f.shadow,
            legacy,
            DiscoveryV2WalletReportOptions {
                now: f.now,
                limit: 10,
                include_rejected: true
            },
            copybot_discovery_v2::DiscoveryV2DecisionContext::new(
                &f.discovery,
                &f.shadow,
                &super::options(f.now)
            )
        )
        .is_err());
        assert_eq!(
            serde_json::to_value(f.store.discovery_publication_state_read_only()?)?,
            before
        );
        println!("B29 legacy={case}: reuse=None publish-loader=coverage_unverified old-followlist-preserved");
    }
    let rpc = responses(1_000_000_000, json!([]), json!([]));
    materialize_discovery_v2_status(&f.store, &f.discovery, &f.shadow, f.options(&rpc))?;
    rpc::assert_requests(&rpc.finish(), &rpc::key('A'), 3);
    let (rebuilt, _) = load_materialized_discovery_v2_status_for_publish(
        &f.store,
        &f.discovery,
        &f.shadow,
        &options,
    )?;
    assert!(
        publish_discovery_v2_status(
            &f.store,
            rebuilt,
            true,
            168,
            copybot_discovery_v2::DiscoveryV2DecisionContext::new(
                &f.discovery,
                &f.shadow,
                &super::options(f.now)
            )
        )?
        .committed
    );
    Ok(())
}

#[test]
fn gate_disabled_legacy_standalone_path_uses_no_rpc() -> Result<()> {
    let mut f = Fixture::new()?;
    f.discovery.live_portfolio_gate_enabled = false;
    let rpc = rpc::RpcStub::start(|_| panic!("disabled gate must not request inventory"));
    let options = f.options(&rpc);
    let (status, _) =
        materialize_discovery_v2_status(&f.store, &f.discovery, &f.shadow, options.clone())?;
    assert_eq!(rpc.finish().len(), 0);
    assert!(status.production_green);
    assert!(status.live_portfolio.is_none());
    let row = f.store.discovery_v2_status_snapshot_read_only()?.unwrap();
    let mut legacy = serde_json::to_value(&status)?;
    for metric in legacy["wallet_metrics"].as_array_mut().unwrap() {
        metric.as_object_mut().unwrap().remove("live_inventory");
    }
    f.store.persist_discovery_v2_status_snapshot(
        &status.policy_fingerprint,
        status.now,
        status.window_start,
        row.runtime_cursor.as_ref(),
        &legacy.to_string(),
    )?;
    let (loaded, _) = load_materialized_discovery_v2_status_for_publish(
        &f.store,
        &f.discovery,
        &f.shadow,
        &options,
    )?;
    assert!(reusable_materialized_discovery_v2_status_for_prepare(
        &f.store,
        &f.discovery,
        &f.shadow,
        &options
    )?
    .is_some());
    assert!(
        publish_discovery_v2_status(
            &f.store,
            loaded.clone(),
            true,
            168,
            copybot_discovery_v2::DiscoveryV2DecisionContext::new(
                &f.discovery,
                &f.shadow,
                &super::options(f.now)
            )
        )?
        .committed
    );
    let report = build_discovery_v2_wallet_report(
        &f.store,
        &f.discovery,
        &f.shadow,
        loaded,
        DiscoveryV2WalletReportOptions {
            now: f.now,
            limit: 10,
            include_rejected: true,
        },
        copybot_discovery_v2::DiscoveryV2DecisionContext::new(
            &f.discovery,
            &f.shadow,
            &super::options(f.now),
        ),
    )?;
    assert!(report.wallets[0].filters.live_portfolio_pass);
    Ok(())
}

#[test]
fn b29_red_token_2022_only_is_not_drained() -> Result<()> {
    let f = Fixture::new()?;
    let rpc = responses(0, json!([]), json!([token(TOKEN_2022, "10000000")]));
    let status = f.build(&rpc)?;
    let calls = rpc.finish();
    let leader = metric(&status);
    assert!(
        leader["reject_reasons"]
            .as_array()
            .unwrap()
            .contains(&json!("live_portfolio_token_2022_valuation_unknown")),
        "actual={leader}"
    );
    rpc::assert_requests(&calls, &rpc::key('A'), 3);
    Ok(())
}

#[test]
fn b29_red_malformed_row_is_not_drained() -> Result<()> {
    let f = Fixture::new()?;
    let rpc = responses(0, json!([{"account":{"data":{}}}]), json!([]));
    let status = f.build(&rpc)?;
    let calls = rpc.finish();
    let leader = metric(&status);
    assert!(
        leader["reject_reasons"]
            .as_array()
            .unwrap()
            .contains(&json!("live_portfolio_inventory_malformed")),
        "actual={leader}"
    );
    rpc::assert_requests(&calls, &rpc::key('A'), 2);
    Ok(())
}
