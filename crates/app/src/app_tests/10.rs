    #[test]
    fn risk_guard_nine_nine_policy_still_blocks_smaller_or_missing_universe_stage1() -> Result<()> {
        let now = Utc::now();

        let (small_active_store, small_active_db_path) =
            make_test_store("universe-stop-nine-nine-small-active")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 9;
        cfg.shadow_universe_min_eligible_wallets = 9;
        cfg.shadow_universe_breach_cycles = 3;
        let mut small_active_guard = ShadowRiskGuard::new(cfg.clone());
        let small_active_output = live_like_degraded_published_universe_output(now, 8, 9);
        for minute in [0_i64, 3, 6] {
            small_active_guard.observe_discovery_cycle(
                &small_active_store,
                now + chrono::Duration::minutes(minute),
                small_active_output.eligible_wallets,
                small_active_output.active_follow_wallets,
                Some(&small_active_output),
            )?;
        }
        assert!(small_active_guard.universe_blocked);
        assert!(matches!(
            small_active_guard.can_open_buy(
                &small_active_store,
                now + chrono::Duration::minutes(7),
                true
            ),
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::Universe,
                ..
            }
        ));

        let (small_eligible_store, small_eligible_db_path) =
            make_test_store("universe-stop-nine-nine-small-eligible")?;
        let mut small_eligible_guard = ShadowRiskGuard::new(cfg.clone());
        let small_eligible_output = live_like_degraded_published_universe_output(now, 9, 8);
        for minute in [0_i64, 3, 6] {
            small_eligible_guard.observe_discovery_cycle(
                &small_eligible_store,
                now + chrono::Duration::minutes(minute),
                small_eligible_output.eligible_wallets,
                small_eligible_output.active_follow_wallets,
                Some(&small_eligible_output),
            )?;
        }
        assert!(small_eligible_guard.universe_blocked);
        assert!(matches!(
            small_eligible_guard.can_open_buy(
                &small_eligible_store,
                now + chrono::Duration::minutes(7),
                true
            ),
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::Universe,
                ..
            }
        ));

        let (missing_store, missing_db_path) =
            make_test_store("universe-stop-nine-nine-no-published-universe")?;
        let mut missing_guard = ShadowRiskGuard::new(cfg);
        let missing_output = discovery_output_for_catch_up_tests(false);
        for minute in [0_i64, 3, 6] {
            missing_guard.observe_discovery_cycle(
                &missing_store,
                now + chrono::Duration::minutes(minute),
                missing_output.eligible_wallets,
                missing_output.active_follow_wallets,
                Some(&missing_output),
            )?;
        }
        assert!(missing_guard.universe_blocked);
        assert!(matches!(
            missing_guard.can_open_buy(&missing_store, now + chrono::Duration::minutes(7), true),
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::Universe,
                ..
            }
        ));

        let _ = std::fs::remove_file(small_active_db_path);
        let _ = std::fs::remove_file(small_eligible_db_path);
        let _ = std::fs::remove_file(missing_db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_observe_discovery_cycle_persists_cap_truncation_context_in_universe_stop_details(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("universe-stop-cap-truncation-context")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 15;
        cfg.shadow_universe_min_eligible_wallets = 80;
        cfg.shadow_universe_breach_cycles = 1;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let guard_started_at = now - chrono::Duration::minutes(2);
        let floor_ts = now - chrono::Duration::minutes(9);
        let discovery_output = DiscoveryTaskOutput {
            active_wallets: std::collections::HashSet::new(),
            cycle_ts: now,
            eligible_wallets: 5,
            active_follow_wallets: 5,
            published: true,
            runtime_mode: DiscoveryRuntimeMode::Degraded,
            scoring_source: "raw_window",
            raw_window_cap_truncated: true,
            cap_truncation_deactivation_guard_active: false,
            cap_truncation_deactivation_guard_reason: Some("warm_load_truncated"),
            cap_truncation_deactivation_guard_started_at: Some(guard_started_at),
            cap_truncation_floor_ts_utc: Some(floor_ts),
            cap_truncation_floor_signature: Some("restart-noise-buy-1".to_string()),
            persisted_stream_catch_up_requested: false,
            persisted_stream_catch_up_pressure_override_requested: false,
        };

        guard.observe_discovery_cycle(&store, now, 5, 5, Some(&discovery_output))?;
        let universe_stops = store.list_risk_events_by_type_desc("shadow_risk_universe_stop")?;
        assert_eq!(universe_stops.len(), 1);
        let details_json = universe_stops[0]
            .details_json
            .as_deref()
            .expect("universe stop event must include details_json");
        let details: serde_json::Value = serde_json::from_str(details_json)
            .context("failed to parse universe stop details_json")?;
        assert_eq!(details["active_follow_wallets"], serde_json::json!(5));
        assert_eq!(details["eligible_wallets"], serde_json::json!(5));
        assert_eq!(details["raw_window_cap_truncated"], serde_json::json!(true));
        assert_eq!(
            details["cap_truncation_deactivation_guard_active"],
            serde_json::json!(false)
        );
        assert_eq!(
            details["cap_truncation_deactivation_guard_reason"],
            serde_json::json!("warm_load_truncated")
        );
        assert_eq!(
            details["cap_truncation_deactivation_guard_started_at"],
            serde_json::json!(guard_started_at.to_rfc3339())
        );
        assert_eq!(
            details["cap_truncation_floor_ts"],
            serde_json::json!(floor_ts.to_rfc3339())
        );
        assert_eq!(
            details["cap_truncation_floor_signature"],
            serde_json::json!("restart-noise-buy-1")
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_observe_discovery_cycle_persists_cap_truncation_context_for_persisted_stream_scoring(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("universe-stop-cap-truncation-context-persisted")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 15;
        cfg.shadow_universe_min_eligible_wallets = 80;
        cfg.shadow_universe_breach_cycles = 1;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let guard_started_at = now - chrono::Duration::minutes(2);
        let floor_ts = now - chrono::Duration::minutes(9);
        let discovery_output = DiscoveryTaskOutput {
            active_wallets: std::collections::HashSet::new(),
            cycle_ts: now,
            eligible_wallets: 5,
            active_follow_wallets: 5,
            published: true,
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            scoring_source: "raw_window_persisted_stream",
            raw_window_cap_truncated: true,
            cap_truncation_deactivation_guard_active: false,
            cap_truncation_deactivation_guard_reason: Some("warm_load_truncated"),
            cap_truncation_deactivation_guard_started_at: Some(guard_started_at),
            cap_truncation_floor_ts_utc: Some(floor_ts),
            cap_truncation_floor_signature: Some("restart-noise-buy-1".to_string()),
            persisted_stream_catch_up_requested: false,
            persisted_stream_catch_up_pressure_override_requested: false,
        };

        guard.observe_discovery_cycle(&store, now, 5, 5, Some(&discovery_output))?;
        let universe_stops = store.list_risk_events_by_type_desc("shadow_risk_universe_stop")?;
        assert_eq!(universe_stops.len(), 1);
        let details_json = universe_stops[0]
            .details_json
            .as_deref()
            .expect("universe stop event must include details_json");
        let details: serde_json::Value = serde_json::from_str(details_json)
            .context("failed to parse universe stop details_json")?;
        assert_eq!(details["raw_window_cap_truncated"], serde_json::json!(true));
        assert_eq!(
            details["cap_truncation_floor_signature"],
            serde_json::json!("restart-noise-buy-1")
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_observe_discovery_cycle_omits_cap_truncation_context_when_raw_window_not_truncated(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("universe-stop-no-cap-truncation-context")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 15;
        cfg.shadow_universe_min_eligible_wallets = 80;
        cfg.shadow_universe_breach_cycles = 1;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let discovery_output = DiscoveryTaskOutput {
            active_wallets: std::collections::HashSet::new(),
            cycle_ts: now,
            eligible_wallets: 5,
            active_follow_wallets: 5,
            published: true,
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            scoring_source: "aggregates",
            raw_window_cap_truncated: false,
            cap_truncation_deactivation_guard_active: false,
            cap_truncation_deactivation_guard_reason: Some("live_cap_eviction"),
            cap_truncation_deactivation_guard_started_at: Some(now),
            cap_truncation_floor_ts_utc: Some(now),
            cap_truncation_floor_signature: Some("cap-sig-001".to_string()),
            persisted_stream_catch_up_requested: false,
            persisted_stream_catch_up_pressure_override_requested: false,
        };

        guard.observe_discovery_cycle(&store, now, 5, 5, Some(&discovery_output))?;
        let universe_stops = store.list_risk_events_by_type_desc("shadow_risk_universe_stop")?;
        assert_eq!(universe_stops.len(), 1);
        let details_json = universe_stops[0]
            .details_json
            .as_deref()
            .expect("universe stop event must include details_json");
        let details: serde_json::Value = serde_json::from_str(details_json)
            .context("failed to parse universe stop details_json")?;
        assert!(
            details.get("raw_window_cap_truncated").is_none(),
            "non-truncated universe stop details must omit cap-truncation fields: {details_json}"
        );
        assert!(
            details
                .get("cap_truncation_deactivation_guard_reason")
                .is_none(),
            "non-truncated universe stop details must omit cap-truncation fields: {details_json}"
        );
        assert!(
            details.get("cap_truncation_floor_signature").is_none(),
            "non-truncated universe stop details must omit cap-truncation fields: {details_json}"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_observe_discovery_cycle_omits_cap_truncation_context_for_aggregate_scoring_with_lingering_floor(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("universe-stop-aggregate-no-cap-context")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 15;
        cfg.shadow_universe_min_eligible_wallets = 80;
        cfg.shadow_universe_breach_cycles = 1;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();
        let discovery_output = DiscoveryTaskOutput {
            active_wallets: std::collections::HashSet::new(),
            cycle_ts: now,
            eligible_wallets: 5,
            active_follow_wallets: 5,
            published: true,
            runtime_mode: DiscoveryRuntimeMode::Healthy,
            scoring_source: "aggregates",
            raw_window_cap_truncated: true,
            cap_truncation_deactivation_guard_active: true,
            cap_truncation_deactivation_guard_reason: Some("live_cap_eviction"),
            cap_truncation_deactivation_guard_started_at: Some(now),
            cap_truncation_floor_ts_utc: Some(now),
            cap_truncation_floor_signature: Some("aggregate-floor-sig".to_string()),
            persisted_stream_catch_up_requested: false,
            persisted_stream_catch_up_pressure_override_requested: false,
        };

        guard.observe_discovery_cycle(&store, now, 5, 5, Some(&discovery_output))?;
        let universe_stops = store.list_risk_events_by_type_desc("shadow_risk_universe_stop")?;
        assert_eq!(universe_stops.len(), 1);
        let details_json = universe_stops[0]
            .details_json
            .as_deref()
            .expect("universe stop event must include details_json");
        let details: serde_json::Value = serde_json::from_str(details_json)
            .context("failed to parse universe stop details_json")?;
        assert!(
            details.get("raw_window_cap_truncated").is_none(),
            "aggregate-scored universe stop details must omit raw-window truncation fields even if a truncation floor still lingers: {details_json}"
        );
        assert!(
            details
                .get("cap_truncation_deactivation_guard_reason")
                .is_none(),
            "aggregate-scored universe stop details must omit raw-window truncation fields even if a truncation floor still lingers: {details_json}"
        );
        assert!(
            details.get("cap_truncation_floor_signature").is_none(),
            "aggregate-scored universe stop details must omit raw-window truncation fields even if a truncation floor still lingers: {details_json}"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_observe_discovery_cycle_returns_error_on_fatal_universe_stop_risk_event_write(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("universe-stop-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 15;
        cfg.shadow_universe_min_eligible_wallets = 80;
        cfg.shadow_universe_breach_cycles = 1;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_universe_stop_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_universe_stop'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = guard
            .observe_discovery_cycle(&store, now, 70, 14, None)
            .expect_err(
                "fatal universe-stop risk event write must abort discovery-cycle observation",
            );
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "failed to persist shadow risk universe stop event with fatal sqlite I/O"
            ),
            "expected universe-stop fatal context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(
            !guard.universe_blocked,
            "fatal universe-stop write must not flip runtime blocked state"
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_universe_stop")?,
            0
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_observe_discovery_cycle_returns_error_on_fatal_universe_clear_risk_event_write(
    ) -> Result<()> {
        let (store, db_path) = make_test_store("universe-clear-risk-event-fatal")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_universe_min_active_follow_wallets = 15;
        cfg.shadow_universe_min_eligible_wallets = 80;
        cfg.shadow_universe_breach_cycles = 1;
        let mut guard = ShadowRiskGuard::new(cfg);
        let now = Utc::now();

        guard.observe_discovery_cycle(&store, now, 70, 14, None)?;
        assert!(guard.universe_blocked);
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_universe_stop")?,
            1
        );

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_shadow_risk_universe_clear_event_insert
             BEFORE INSERT ON risk_events
             WHEN NEW.type = 'shadow_risk_universe_cleared'
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = guard
            .observe_discovery_cycle(&store, now + chrono::Duration::minutes(3), 150, 30, None)
            .expect_err(
                "fatal universe-clear risk event write must abort discovery-cycle observation",
            );
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains(
                "failed to persist shadow risk universe clear event with fatal sqlite I/O"
            ),
            "expected universe-clear fatal context, got: {error_text}"
        );
        assert!(
            error_text.contains("failed to insert risk event"),
            "expected storage insert_risk_event context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite marker to survive error chain, got: {error_text}"
        );
        assert!(
            guard.universe_blocked,
            "fatal universe-clear write must preserve runtime blocked state"
        );
        assert_eq!(
            store.risk_event_count_by_type("shadow_risk_universe_cleared")?,
            0
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_hard_exposure_blocks_then_auto_clears() -> Result<()> {
        let (store, db_path) = make_test_store("hard-exposure-stop")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_hard_exposure_cap_sol = 1.0;
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        let mut guard = ShadowRiskGuard::new(cfg);
        let opened_ts = DateTime::parse_from_rfc3339("2026-02-17T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let lot_id = store.insert_shadow_lot("wallet-a", "token-a", 10.0, 1.2, opened_ts)?;
        let now = Utc::now();

        let decision = guard.can_open_buy(&store, now, true);
        match decision {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::ExposureCap,
                ..
            } => {}
            other => panic!("expected exposure-cap block, got {other:?}"),
        }

        store.delete_shadow_lot(lot_id)?;
        let decision_after_clear = guard.can_open_buy(
            &store,
            now + chrono::Duration::seconds((RISK_DB_REFRESH_MIN_SECONDS + 1).max(6)),
            true,
        );
        match decision_after_clear {
            BuyRiskDecision::Allow => {}
            other => panic!("expected auto-clear after exposure normalizes, got {other:?}"),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_hard_exposure_ignores_dust_shadow_lots() -> Result<()> {
        let (store, db_path) = make_test_store("hard-exposure-dust")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_hard_exposure_cap_sol = 1.0;
        cfg.shadow_soft_exposure_cap_sol = 0.5;
        let mut guard = ShadowRiskGuard::new(cfg);
        let opened_ts = DateTime::parse_from_rfc3339("2026-02-17T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let lot_id = store.insert_shadow_lot("wallet-a", "token-a", 10.0, 1.2, opened_ts)?;
        store.update_shadow_lot(lot_id, 1e-13, 1.2)?;
        let now = Utc::now();

        let decision = guard.can_open_buy(&store, now, true);
        match decision {
            BuyRiskDecision::Allow => {}
            other => panic!("expected dust lot to be ignored by hard exposure cap, got {other:?}"),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn risk_guard_hard_exposure_uses_lamport_normalized_cap() -> Result<()> {
        let (store, db_path) = make_test_store("hard-exposure-lamport-cap")?;
        let mut cfg = RiskConfig::default();
        cfg.shadow_hard_exposure_cap_sol = 0.1000000004;
        cfg.shadow_soft_exposure_cap_sol = 99.0;
        cfg.shadow_drawdown_1h_stop_sol = -999.0;
        cfg.shadow_drawdown_6h_stop_sol = -999.0;
        cfg.shadow_drawdown_24h_stop_sol = -999.0;
        let mut guard = ShadowRiskGuard::new(cfg);
        let opened_ts = DateTime::parse_from_rfc3339("2026-02-17T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        store.insert_shadow_lot("wallet-a", "token-a", 10.0, 0.1, opened_ts)?;
        let now = Utc::now();

        let decision = guard.can_open_buy(&store, now, true);
        match decision {
            BuyRiskDecision::Blocked {
                reason: BuyRiskBlockReason::ExposureCap,
                detail,
            } => assert!(
                detail.contains("hard_cap"),
                "expected hard cap detail, got {detail}"
            ),
            other => panic!("expected lamport-normalized exposure block, got {other:?}"),
        }

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }
