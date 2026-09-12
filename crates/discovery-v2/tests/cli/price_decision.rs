use super::*;
use serde_json::json;
use std::sync::{Arc, Mutex};

#[path = "../live_portfolio/rpc.rs"]
#[allow(dead_code)]
mod rpc;

#[test]
fn b35_actual_publish_cli_uses_clock_after_inventory_load() -> Result<()> {
    for (sol, expire, accepted) in [
        (0u64, true, false),
        (300_000_000, true, true),
        (0, false, true),
    ] {
        let dir = tempdir()?;
        let path = dir.path().join("runtime.db");
        let config_path = dir.path().join("local.toml");
        let store = SqliteDiscoveryStore::open(&path)?;
        ensure_discovery_v2_schema(&store)?;
        let seeded_at = Utc::now();
        store.insert_observed_swaps_batch(&[
            buy(
                "coverage",
                "coverage",
                "coverage",
                1,
                seeded_at - Duration::hours(25),
            ),
            buy(
                &rpc::key('A'),
                &rpc::key('D'),
                "leader-buy",
                10,
                seeded_at - Duration::minutes(4),
            ),
            buy("tail", "tail", "tail", 11, seeded_at - Duration::seconds(1)),
        ])?;
        for mint in ['D', 'E'] {
            store.upsert_token_quality_cache(
                &rpc::key(mint),
                Some(5),
                Some(1.0),
                Some(60),
                seeded_at,
            )?;
        }
        store.activate_follow_wallet("previous", seeded_at, "fixture")?;
        let before = serde_json::to_string(&store.discovery_publication_state_read_only()?)?;
        let source = Arc::new(Mutex::new(None));
        let captured_source = source.clone();
        let writer_path = path.clone();
        let server = rpc::RpcStub::start(move |request| {
            if request["method"] == "getBalance" {
                // The decision's initial now has already been sampled and the
                // scan completed. Add an observation inside that original window.
                let requested_at = Utc::now();
                let observed_at = if expire {
                    requested_at - Duration::minutes(120) + Duration::milliseconds(100)
                } else {
                    requested_at - Duration::minutes(60)
                };
                let writer = SqliteDiscoveryStore::open(&writer_path).unwrap();
                writer
                    .insert_observed_swap(&buy(
                        &rpc::key('Z'),
                        &rpc::key('E'),
                        "rpc-stage-price",
                        8,
                        observed_at,
                    ))
                    .unwrap();
                *captured_source.lock().unwrap() = Some(observed_at);
                let mut reply: rpc::Reply = rpc::result(json!(sol)).into();
                reply.delay = StdDuration::from_millis(400);
                return reply;
            }
            let rows = if request["params"][1]["programId"] == rpc::CLASSIC {
                json!([rpc::account(
                    &rpc::key('A'),
                    &rpc::key('E'),
                    &rpc::key('J'),
                    rpc::CLASSIC,
                    "10000000",
                    6
                )])
            } else {
                json!([])
            };
            rpc::result(rows).into()
        });
        write_green_config(&config_path, &path)?;
        let config = fs::read_to_string(&config_path)?.replace("[shadow]", &format!(
            "status_scan_window_minutes = 120\nlive_portfolio_gate_enabled = true\nlive_portfolio_max_wallets = 1\nmin_live_sol_balance = 0.25\nmin_live_portfolio_value_sol = 0.25\nhelius_http_url = \"{}\"\n\n[shadow]", server.url));
        fs::write(&config_path, config)?;
        let output = command_output_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_discovery_v2_publish"))
                .env_clear()
                .env("PATH", "/usr/bin:/bin")
                .args([
                    "--config",
                    config_path.to_str().unwrap(),
                    "--commit",
                    "--acknowledge-daemon-restart-required",
                ]),
        )?;
        rpc::assert_requests(&server.finish(), &rpc::key('A'), 3);
        let error = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            output.status.success(),
            accepted,
            "expire={expire} sol={sol}: {error}"
        );
        if accepted {
            let result: serde_json::Value = serde_json::from_slice(&output.stdout)?;
            let metric = result["status"]["wallet_metrics"]
                .as_array()
                .unwrap()
                .iter()
                .find(|row| row["wallet_id"] == rpc::key('A'))
                .unwrap();
            let proof = &metric["live_valuation"];
            assert_eq!(
                proof["classic_positions"][0]["observation"]["observed_at"],
                json!(source.lock().unwrap().unwrap())
            );
            let original = DateTime::parse_from_rfc3339(proof["observed_as_of"].as_str().unwrap())?;
            let decision =
                DateTime::parse_from_rfc3339(proof["decision"]["as_of"].as_str().unwrap())?;
            assert!(decision > original + Duration::milliseconds(300));
            assert_eq!(
                metric["live_token_value_sol"],
                json!(if expire { 0.0 } else { 1.0 })
            );
            assert_eq!(proof["decision"]["unvalued_positions"], usize::from(expire));
            assert_eq!(result["status"]["now"], proof["observed_as_of"]);
            println!(
                "B35 CLI_ACCEPTED {}",
                String::from_utf8_lossy(&output.stdout)
            );
        } else {
            assert!(
                error.contains("price_support_expired_rebuild_required"),
                "{error}"
            );
            assert_eq!(
                serde_json::to_string(&store.discovery_publication_state_read_only()?)?,
                before
            );
            assert_eq!(
                store
                    .list_active_follow_wallets()?
                    .into_iter()
                    .collect::<Vec<_>>(),
                vec!["previous"]
            );
            println!("B35 CLI_REFUSED {error}");
        }
    }
    Ok(())
}
