mod quality_cache_support;
use anyhow::Result;
use chrono::Duration;
use copybot_config::ShadowConfig;
use quality_cache_support::{rpc::Rpc, *};
use serde_json::json;

#[test]
fn future_one_nanosecond_without_rpc_uses_only_proxy() -> Result<()> {
    let (_dir, store) = fixture(false)?;
    cache(&store, now() + Duration::nanoseconds(1))?;
    let before = read_cache(&store)?;
    let v = consume(&store, &buy(), now(), None, ShadowConfig::default())?;
    check("future-one-ns", &v, "low_liquidity", 0, Some("db_proxy"));
    assert_eq!(v["quality"]["holders"], 6);
    assert_eq!(v["quality"]["token_age_seconds"], 120);
    assert_eq!(v["quality"]["liquidity_sol"], 0.5);
    assert_eq!(read_cache(&store)?, before);
    Ok(())
}
#[test]
fn fresh_equality_ttl_and_stale_without_rpc_keep_cache() -> Result<()> {
    for (name, age, label) in [
        ("fresh", 1, "rpc_cache"),
        ("equal", 0, "rpc_cache"),
        ("ttl", 600_000_000_000, "rpc_cache"),
        ("stale-one-ns", 600_000_000_001, "rpc_cache_stale"),
        ("stale-old", 7_201_000_000_000, "rpc_cache_stale"),
    ] {
        let (_dir, store) = fixture(false)?;
        cache(&store, now() - Duration::nanoseconds(age))?;
        let before = read_cache(&store)?;
        let v = consume(&store, &buy(), now(), None, ShadowConfig::default())?;
        check(name, &v, "recorded", 1, Some(label));
        assert_eq!(read_cache(&store)?, before);
    }
    Ok(())
}
#[test]
fn stale_and_future_rpc_failure_use_only_permitted_fallback() -> Result<()> {
    for (name, ts, outcome, label, count) in [
        (
            "stale-rpc-failure",
            now() - Duration::seconds(601),
            "recorded",
            "rpc_cache_stale",
            1,
        ),
        (
            "future-rpc-failure",
            now() + Duration::nanoseconds(1),
            "low_liquidity",
            "db_proxy",
            0,
        ),
    ] {
        let (_dir, store) = fixture(false)?;
        cache(&store, ts)?;
        let before = read_cache(&store)?;
        let rpc = Rpc::start(false)?;
        let v = consume(
            &store,
            &buy(),
            now(),
            Some(rpc.url.clone()),
            ShadowConfig::default(),
        );
        let methods = rpc.finish()?;
        let mut v = v?;
        v["rpc_methods"] = json!(methods);
        check(name, &v, outcome, count, Some(label));
        assert_eq!(
            methods,
            vec!["getProgramAccounts", "getSignaturesForAddress"]
        );
        assert_eq!(read_cache(&store)?, before);
    }
    Ok(())
}
#[test]
fn future_does_not_blacklist_a_token_with_sufficient_proxy() -> Result<()> {
    let (_dir, store) = fixture(true)?;
    // Every future field would reject if used; all valid DB fields suffice.
    store.upsert_token_quality_cache(
        TOKEN,
        Some(1),
        Some(0.0),
        Some(0),
        now() + Duration::nanoseconds(1),
    )?;
    let before = read_cache(&store)?;
    let v = consume(&store, &buy(), now(), None, ShadowConfig::default())?;
    check(
        "future-sufficient-proxy",
        &v,
        "recorded",
        1,
        Some("db_proxy"),
    );
    assert_eq!(v["quality"]["holders"], 6);
    assert_eq!(v["quality"]["token_age_seconds"], 120);
    assert_eq!(v["quality"]["liquidity_sol"], 1.25);
    assert_eq!(read_cache(&store)?, before);
    Ok(())
}
#[test]
fn missing_cache_keeps_existing_proxy_policy() -> Result<()> {
    for sufficient in [false, true] {
        let (_dir, store) = fixture(sufficient)?;
        let v = consume(&store, &buy(), now(), None, ShadowConfig::default())?;
        check(
            if sufficient {
                "missing-good"
            } else {
                "missing-low"
            },
            &v,
            if sufficient {
                "recorded"
            } else {
                "low_liquidity"
            },
            usize::from(sufficient),
            Some("db_proxy"),
        );
    }
    Ok(())
}
#[test]
fn healthy_refresh_replaces_future_with_valid_evidence_at_evaluation_now() -> Result<()> {
    let (_dir, store) = fixture(true)?;
    store.upsert_token_quality_cache(
        TOKEN,
        Some(1),
        Some(0.0),
        Some(0),
        now() + Duration::nanoseconds(1),
    )?;
    let rpc = Rpc::start(true)?;
    let v = consume(
        &store,
        &buy(),
        now(),
        Some(rpc.url.clone()),
        ShadowConfig::default(),
    );
    let methods = rpc.finish()?;
    let mut v = v?;
    v["rpc_methods"] = json!(methods);
    check("healthy-refresh", &v, "recorded", 1, Some("rpc_cache"));
    assert_eq!(
        methods,
        vec!["getProgramAccounts", "getSignaturesForAddress"]
    );
    let row = store.get_token_quality_cache(TOKEN)?.unwrap();
    assert_eq!(row.fetched_at, now());
    assert_eq!(row.holders, Some(5));
    assert_eq!(row.liquidity_sol, None);
    assert!(row.token_age_seconds.unwrap() >= 30);
    assert_eq!(v["quality"]["liquidity_sol"], 1.25);
    Ok(())
}
#[test]
fn post_refresh_future_row_is_excluded_before_gate_and_label() -> Result<()> {
    let (_dir, mut store) = fixture(false)?;
    cache(&store, now() - Duration::seconds(601))?;
    let trigger = tempfile::tempdir()?;
    // Test-only damaged/concurrent writer control: replace the refreshed row in the
    // same SQLite statement, so the public resolver's final read deterministically sees future.
    std::fs::write(
        trigger.path().join("9999_test_future_refresh.sql"),
        format!(
            "CREATE TRIGGER test_future_refresh AFTER UPDATE ON token_quality_cache
         WHEN NEW.fetched_at = '{}' BEGIN
         UPDATE token_quality_cache SET fetched_at = '{}', holders=5, token_age_seconds=122,
         liquidity_sol=20.0 WHERE mint=NEW.mint; END;",
            now().to_rfc3339(),
            (now() + Duration::nanoseconds(1)).to_rfc3339()
        ),
    )?;
    store.run_migrations(trigger.path())?;
    let rpc = Rpc::start(true)?;
    let v = consume(
        &store,
        &buy(),
        now(),
        Some(rpc.url.clone()),
        ShadowConfig::default(),
    );
    let methods = rpc.finish()?;
    let mut v = v?;
    v["rpc_methods"] = json!(methods);
    check(
        "post-refresh-future",
        &v,
        "low_liquidity",
        0,
        Some("db_proxy"),
    );
    assert_eq!(
        methods,
        vec!["getProgramAccounts", "getSignaturesForAddress"]
    );
    assert_eq!(
        store.get_token_quality_cache(TOKEN)?.unwrap().fetched_at,
        now() + Duration::nanoseconds(1)
    );
    Ok(())
}
#[test]
fn quality_disabled_keeps_existing_behavior() -> Result<()> {
    let (_dir, store) = fixture(false)?;
    cache(&store, now() + Duration::nanoseconds(1))?;
    let mut config = ShadowConfig::default();
    config.quality_gates_enabled = false;
    let v = consume(&store, &buy(), now(), None, config)?;
    check("quality-disabled", &v, "recorded", 1, None);
    Ok(())
}
#[test]
fn open_risk_sell_does_not_consult_future_buy_quality() -> Result<()> {
    let (_dir, store) = fixture(false)?;
    cache(&store, now())?;
    let v = consume(&store, &buy(), now(), None, ShadowConfig::default())?;
    check("sell-setup-buy", &v, "recorded", 1, Some("rpc_cache"));
    cache(&store, now() + Duration::hours(1))?;
    let mut sell = buy();
    sell.token_in = TOKEN.into();
    sell.token_out = SOL.into();
    sell.amount_in = 10.0;
    sell.amount_out = 0.1;
    sell.signature = "owned-sell".into();
    sell.ts_utc = now() + Duration::seconds(1);
    let v = consume(
        &store,
        &sell,
        now() + Duration::seconds(120),
        None,
        ShadowConfig::default(),
    )?;
    check("open-risk-sell", &v, "recorded", 2, None);
    assert!(store.list_shadow_lots("leader", TOKEN)?.is_empty());
    Ok(())
}
