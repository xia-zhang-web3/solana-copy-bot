use super::{fixture, price_fixture as p, rpc};
use anyhow::Result;
use chrono::Duration;
use copybot_discovery_v2::{
    load_materialized_discovery_v2_status_for_publish, materialize_discovery_v2_status,
    publish_discovery_v2_status, DiscoveryV2DecisionContext,
};
use serde_json::json;

#[test]
fn b35_numeric_observations_survive_materialized_json_round_trip() -> Result<()> {
    for (raw, decimals, price) in [
        ("18446744073709551615", 9, 0.123456789),
        ("100000000000000001", 18, 3.14159265),
        ("12345678910111213", 10, 0.00000033333),
        ("18446744073709551615", 0, 0.1),
        ("1", 255, 0.1),
    ] {
        let f = p::fixture()?;
        p::observation(&f, 'E', Duration::minutes(10), price, true)?;
        let server = fixture::responses(
            1_000_000_000,
            json!([rpc::account(
                &rpc::key('A'),
                &rpc::key('E'),
                &rpc::key('J'),
                rpc::CLASSIC,
                raw,
                decimals,
            )]),
            json!([]),
        );
        let options = p::options(&f, f.now).with_live_portfolio_rpc_url(Some(server.url.clone()));
        let (original, _) =
            materialize_discovery_v2_status(&f.store, &f.discovery, &f.shadow, options.clone())?;
        rpc::assert_requests(&server.finish(), &rpc::key('A'), 3);
        assert!(original.production_green);
        let (loaded, _) = load_materialized_discovery_v2_status_for_publish(
            &f.reopen()?,
            &f.discovery,
            &f.shadow,
            &options,
        )?;
        let committed = publish_discovery_v2_status(
            &f.store,
            loaded,
            true,
            168,
            DiscoveryV2DecisionContext::new(&f.discovery, &f.shadow, &options),
        )?;
        assert!(committed.committed);
        assert_eq!(
            fixture::metric(&committed.status)["live_tradable_token_positions"],
            1
        );
        println!("B35 NUMERIC_ROUND_TRIP raw={raw} decimals={decimals} price={price}");
    }
    Ok(())
}
