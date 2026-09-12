use super::{fixture, price_fixture as p, rpc};
use anyhow::Result;
use chrono::Duration;
use copybot_discovery_v2::{
    build_discovery_v2_status, publish_discovery_v2_status, DiscoveryV2DecisionContext,
};
use serde_json::json;

#[test]
fn b35_closed_window_boundaries_both_orientations_and_quality_disabled() -> Result<()> {
    for quality_enabled in [true, false] {
        for buy in [true, false] {
            for (age, fresh) in [
                (Duration::minutes(120), true),
                (Duration::minutes(120) + Duration::nanoseconds(1), false),
                (Duration::zero(), true),
            ] {
                let mut f = p::fixture()?;
                f.shadow.quality_gates_enabled = quality_enabled;
                p::observation(&f, 'E', age, 0.1, buy)?;
                let status = p::build(&f, 0, json!([p::token('E', 'J', "10000000")]), json!([]))?;
                let row = fixture::metric(&status);
                assert_eq!(
                    status.candidate_wallets.len(),
                    usize::from(fresh),
                    "age={age} row={row}"
                );
                assert_eq!(
                    row["live_token_value_sol"],
                    json!(if fresh { 1.0 } else { 0.0 })
                );
                let proof = &row["live_valuation"];
                assert_eq!(
                    proof["classic_positions"][0]["observation"]["observed_at"],
                    json!(f.now - age)
                );
                assert_eq!(
                    proof["classic_positions"][0]["observation"]["signature"],
                    "price-E"
                );
                assert_eq!(proof["classic_positions"][0]["observation"]["slot"], 8);
                if !fresh {
                    p::assert_unknown(&status, 0.0);
                }
                p::report(&f, status, f.now)?;
            }
        }
    }
    Ok(())
}

#[test]
fn b35_known_subtotal_survives_unknown_tails_without_adding_their_value() -> Result<()> {
    for (sol, fresh_raw, accepted, known) in [
        (0, "10000000", true, 1.0),
        (0, "1000000", false, 0.1),
        (300_000_000, "0", true, 0.0),
    ] {
        let f = p::fixture()?;
        p::observation(&f, 'E', Duration::minutes(121), 50.0, true)?;
        p::observation(&f, 'F', Duration::minutes(20), 0.1, true)?;
        let status = p::build(
            &f,
            sol,
            json!([
                p::token('E', 'J', "10000000"),
                p::token('F', 'K', fresh_raw)
            ]),
            json!([]),
        )?;
        let row = fixture::metric(&status);
        assert_eq!(status.production_green, accepted);
        assert_eq!(row["live_token_value_sol"], json!(known));
        assert_eq!(row["live_inventory"]["unvalued_token_positions"], 1);
        if !accepted {
            p::assert_unknown(&status, known);
        }
        let published = publish_discovery_v2_status(
            &f.store,
            status.clone(),
            true,
            168,
            DiscoveryV2DecisionContext::new(&f.discovery, &f.shadow, &p::options(&f, f.now)),
        );
        assert_eq!(published.is_ok(), accepted);
        p::report(&f, status, f.now)?;
    }
    // Missing price is also Unknown; quality is present and explicitly disabled.
    let mut f = p::fixture()?;
    f.shadow.quality_gates_enabled = false;
    let status = p::build(&f, 0, json!([p::token('E', 'J', "10000000")]), json!([]))?;
    p::assert_unknown(&status, 0.0);
    assert_eq!(
        fixture::metric(&status)["live_valuation"]["decision"]["unknown_reasons"]["price_missing"],
        1
    );
    Ok(())
}

#[test]
fn b35_refreshing_quality_or_serializing_does_not_refresh_observation() -> Result<()> {
    let f = p::fixture()?;
    p::observation(&f, 'E', Duration::minutes(186), 0.1, true)?;
    let first = p::build(&f, 0, json!([p::token('E', 'J', "10000000")]), json!([]))?;
    p::assert_unknown(&first, 0.0);
    super::insert_quality_for_token(
        &f.store,
        &rpc::key('E'),
        f.now + Duration::seconds(1),
        Some(5.0),
    )?;
    let mut later = p::options(&f, f.now + Duration::seconds(1));
    let server = fixture::responses(0, json!([p::token('E', 'J', "10000000")]), json!([]));
    later.live_portfolio_rpc_url = Some(server.url.clone());
    let second = build_discovery_v2_status(&f.store, &f.discovery, &f.shadow, later)?;
    rpc::assert_requests(&server.finish(), &rpc::key('A'), 3);
    p::assert_unknown(&second, 0.0);
    let decoded = serde_json::from_str(&serde_json::to_string(&second)?)?;
    assert_eq!(
        fixture::metric(&first)["live_valuation"]["classic_positions"][0]["observation"],
        fixture::metric(&decoded)["live_valuation"]["classic_positions"][0]["observation"]
    );
    let report = p::report(&f, decoded, f.now + Duration::seconds(1))?;
    let proof = report
        .top_rejected_wallets
        .iter()
        .find(|row| row.wallet_id == rpc::key('A'))
        .unwrap()
        .live_valuation
        .as_ref()
        .unwrap();
    assert_eq!(
        (proof.decision.as_of
            - proof.classic_positions[0]
                .observation
                .as_ref()
                .unwrap()
                .observed_at)
            .num_seconds(),
        186 * 60 + 1
    );
    Ok(())
}

#[test]
fn b35_invalid_window_fails_before_rpc_and_never_proves_price() -> Result<()> {
    let f = p::fixture()?;
    for window in [0, u64::MAX] {
        let server = rpc::RpcStub::start(|_| panic!("invalid window must fail before RPC"));
        let mut options = p::options(&f, f.now);
        options.window_minutes = window;
        options.live_portfolio_rpc_url = Some(server.url.clone());
        let error =
            build_discovery_v2_status(&f.store, &f.discovery, &f.shadow, options).unwrap_err();
        assert!(error.to_string().contains("price_window_invalid"));
        assert!(server.finish().is_empty());
    }
    Ok(())
}
