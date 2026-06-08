use super::*;

#[test]
fn risk_guard_rug_rate_uses_configured_sample_floor() -> Result<()> {
    let (store, db_path) = make_test_store("rug-rate-configured-sample-floor")?;
    let mut cfg = RiskConfig::default();
    cfg.shadow_rug_loss_window_minutes = 120;
    cfg.shadow_rug_loss_count_threshold = 3;
    cfg.shadow_rug_loss_rate_sample_size = 200;
    cfg.shadow_rug_loss_rate_threshold = 0.04;
    cfg.shadow_rug_loss_return_threshold = -0.70;
    cfg.shadow_drawdown_1h_stop_sol = -999.0;
    cfg.shadow_drawdown_6h_stop_sol = -999.0;
    cfg.shadow_drawdown_24h_stop_sol = -999.0;
    let mut guard = ShadowRiskGuard::new(cfg);
    let now = Utc::now();

    store.insert_shadow_closed_trade(
        "sig-rug-rate-one-rug",
        "wallet-rug",
        "token-rug",
        1.0,
        1.0,
        0.2,
        -0.8,
        now - chrono::Duration::minutes(30),
        now - chrono::Duration::minutes(20),
    )?;
    for index in 0..4 {
        store.insert_shadow_closed_trade(
            &format!("sig-rug-rate-small-win-{index}"),
            &format!("wallet-win-{index}"),
            &format!("token-win-{index}"),
            1.0,
            1.0,
            1.01,
            0.01,
            now - chrono::Duration::minutes(19 - index),
            now - chrono::Duration::minutes(18 - index),
        )?;
    }

    match guard.can_open_buy(&store, now, true) {
        BuyRiskDecision::Allow => {}
        other => panic!("rug-rate must wait for configured sample floor, got {other:?}"),
    }

    let _ = std::fs::remove_file(db_path);
    Ok(())
}
