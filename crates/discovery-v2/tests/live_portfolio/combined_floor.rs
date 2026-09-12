use super::*;
#[path = "rpc.rs"]
#[allow(dead_code)]
mod rpc;

#[test]
fn b29_joint_survivors_preserve_denominator_and_refuse_publication() -> Result<()> {
    use copybot_discovery_v2::publish_discovery_v2_status;
    let dir = tempdir()?;
    let mut store = SqliteDiscoveryStore::open(dir.path().join("runtime.db"))?;
    store.run_migrations(std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations"
    )))?;
    ensure_discovery_v2_schema(&store)?;
    let now = DateTime::parse_from_rfc3339("2026-05-14T10:00:00Z")?.with_timezone(&Utc);
    store.insert_observed_swap(&swap(
        "coverage",
        "coverage-token",
        "coverage",
        1,
        now - Duration::hours(25),
    ))?;
    for w in 0..10 {
        let wallet = rpc::key("ABCDEFGHJK".chars().nth(w as usize).unwrap());
        let token = rpc::key("LMNPQRSTUV".chars().nth(w as usize).unwrap());
        store.insert_observed_swap(&swap(
            &wallet,
            &token,
            &format!("buy{w}"),
            10 + w,
            now - Duration::minutes(4),
        ))?;
        store.upsert_token_quality_cache(&token, Some(5), Some(1.0), Some(60), now)?;
        if w < 4 {
            for i in 0..10 {
                let opened = now - Duration::hours(1) + Duration::seconds(i);
                let closed = opened + Duration::seconds(30);
                let id = format!("close{w}-{i}");
                if w < 2 {
                    store.insert_shadow_closed_trade(
                        &id, &wallet, &token, 1000.0, 0.20, 0.21, 0.01, opened, closed,
                    )?;
                    let cid = close_id_for_signal(&store, &id)?;
                    store.record_execution_quote_canary_event(&quote_event(
                        &format!("qb{w}-{i}"),
                        Some(format!("bs{w}-{i}")),
                        None,
                        &wallet,
                        &token,
                        "buy",
                        opened,
                        Some(opened),
                        "10000000",
                        "1000",
                        "would_execute",
                        Some(0),
                    ))?;
                    store.record_execution_quote_canary_event(&quote_event(
                        &format!("qs{w}-{i}"),
                        Some(id),
                        Some(cid),
                        &wallet,
                        &token,
                        "sell",
                        closed,
                        Some(closed),
                        "1000",
                        "9000000",
                        "would_execute",
                        Some(0),
                    ))?;
                } else {
                    let stale = i < 3;
                    store.insert_shadow_closed_trade_exact_with_context(
                        &id,
                        &wallet,
                        &token,
                        1000.0,
                        None,
                        0.20,
                        if stale { 0.19 } else { 0.22 },
                        if stale { -0.01 } else { 0.02 },
                        if stale {
                            copybot_storage_core::SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE
                        } else {
                            "market"
                        },
                        opened,
                        closed,
                    )?;
                }
            }
        }
    }
    store.insert_observed_swap(&swap(
        "tail",
        "tail-token",
        "tail",
        99,
        now - Duration::minutes(1),
    ))?;
    let (mut discovery, shadow) = strict_policy();
    discovery.follow_top_n = 10;
    discovery.publish_min_candidate_wallets = 8;
    discovery.live_portfolio_gate_enabled = true;
    discovery.executable_wallet_filter_min_samples = 10;
    discovery.rug_wallet_filter_min_closed_trades = 7;
    discovery.rug_wallet_filter_max_stale_terminal_rate = 0.20;
    discovery.rug_wallet_filter_max_stale_terminal_pnl_sol = -0.30;
    for (exec, rug, expected) in [
        (false, false, 10),
        (true, false, 8),
        (false, true, 8),
        (true, true, 6),
    ] {
        discovery.executable_wallet_filter_enabled = exec;
        discovery.rug_wallet_filter_enabled = rug;
        let rpc = rpc::RpcStub::start(|request| {
            rpc::result(if request["method"] == "getBalance" {
                serde_json::json!(1_000_000_000u64)
            } else {
                serde_json::json!([])
            })
            .into()
        });
        let status = build_discovery_v2_status(
            &store,
            &discovery,
            &shadow,
            options(now).with_live_portfolio_rpc_url(Some(rpc.url.clone())),
        )?;
        let calls = rpc.finish();
        assert_eq!(calls.len(), expected * 3);
        let mut by_wallet = std::collections::BTreeMap::<String, Vec<serde_json::Value>>::new();
        for call in calls {
            by_wallet
                .entry(call["params"][0].as_str().unwrap().to_string())
                .or_default()
                .push(call);
        }
        for (wallet, calls) in by_wallet {
            rpc::assert_requests(&calls, &wallet, 3);
        }
        println!("B29 exec={exec} rug={rug} candidates={} total={} eligible={} rejected={} reasons={:?} blockers={:?}",status.candidate_wallets.len(),status.filters.total_wallets,status.filters.eligible_wallets,status.filters.rejected_wallets,status.filters.reject_breakdown,status.blockers);
        assert_eq!(status.candidate_wallets.len(), expected);
        assert_eq!(status.filters.total_wallets, 11);
        assert_eq!(status.filters.eligible_wallets, expected);
        assert_eq!(status.filters.rejected_wallets, 11 - expected);
        if !exec && !rug {
            assert!(
                publish_discovery_v2_status(
                    &store,
                    status,
                    true,
                    168,
                    copybot_discovery_v2::DiscoveryV2DecisionContext::new(
                        &discovery,
                        &shadow,
                        &options(now)
                    )
                )?
                .committed
            );
        } else if expected >= 8 {
            assert!(status.production_green);
        } else {
            let before = serde_json::to_string(&store.discovery_publication_state_read_only()?)?;
            assert!(publish_discovery_v2_status(
                &store,
                status,
                true,
                168,
                copybot_discovery_v2::DiscoveryV2DecisionContext::new(
                    &discovery,
                    &shadow,
                    &options(now)
                )
            )
            .is_err());
            assert_eq!(
                before,
                serde_json::to_string(&store.discovery_publication_state_read_only()?)?
            );
            assert_eq!(store.list_active_follow_wallets()?.len(), 10);
            println!("B29 refused commit left old publication and active followlist byte-equivalent; count=10");
        }
    }
    Ok(())
}
