use super::*;

pub(crate) async fn run() -> Result<()> {
    let cli_config = parse_config_arg();
    let default_path = cli_config.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
    let (mut config, loaded_config_path) = load_from_env_or_default(&default_path)?;
    let (applied_source_override, invalid_source_override_error) =
        match load_ingestion_source_override() {
            Ok(source_override) => (
                apply_ingestion_source_override(&mut config.ingestion.source, source_override),
                None,
            ),
            Err(error) => (None, Some(error)),
        };
    init_tracing(&config.system.log_level, config.system.log_json)?;
    info!(
        config_path = %loaded_config_path.display(),
        env = %config.system.env,
        "configuration loaded"
    );
    if let Some(source_override) = applied_source_override.as_deref() {
        info!(
            source = %source_override,
            "applying ingestion source override from failover file (override has highest priority)"
        );
    }
    if let Some(error) = invalid_source_override_error.as_ref() {
        warn!(
            error = %error,
            "ignoring invalid ingestion source override file"
        );
    }
    let migrations_dir = resolve_migrations_dir(&loaded_config_path, &config.system.migrations_dir);
    let startup_reporter = build_startup_progress_reporter();
    run_inline_startup_step(
        &startup_reporter,
        "startup_config_validation",
        Some(STARTUP_SQLITE_AUX_STEP_TIMEOUT),
        || {
            validate_execution_runtime_contract(&config.execution, &config.system.env)?;
            validate_execution_risk_contract(&config.risk)?;
            validate_live_execution_policy_contract(
                &config.execution,
                &config.risk,
                &config.system.env,
            )?;
            Ok(())
        },
    )?;

    let sqlite_startup_policy = sqlite_startup_policy();
    let bootstrap = SqliteStore::open_and_migrate_for_startup(
        Path::new(&config.sqlite.path),
        &migrations_dir,
        &sqlite_startup_policy,
        Some(&startup_reporter),
    )
    .context("failed to initialize sqlite store")?;
    let store = bootstrap.store;
    let applied = bootstrap.applied_migrations;
    let deferred_migrations = bootstrap.deferred_migrations;
    info!(applied, "sqlite migrations applied");
    if !deferred_migrations.is_empty() {
        warn!(
            deferred_migrations = ?deferred_migrations,
            detail = "deferred_off_startup_critical_path",
            "startup deferred optional sqlite performance migrations; runtime may use bounded fallback query paths until they are applied offline"
        );
    }
    match perform_startup_wal_checkpoint(&startup_reporter) {
        StartupWalCheckpointOutcome::Deferred => {
            warn!(
                reason = STARTUP_WAL_CHECKPOINT_DEFER_REASON,
                "startup sqlite wal checkpoint deferred"
            );
        }
    }

    let (store, startup_wal_autocheckpoint_restore_pages) = run_observed_startup_step(
        "startup_sqlite_wal_autocheckpoint_defer",
        startup_step_policy(STARTUP_SQLITE_AUX_STEP_TIMEOUT),
        Some(&startup_reporter),
        move || defer_implicit_startup_sqlite_wal_autocheckpoint(store),
    )?;
    let startup_heartbeat_sqlite_path = config.sqlite.path.clone();
    let mut store = run_observed_startup_step(
        "startup_sqlite_heartbeat",
        startup_step_policy(STARTUP_SQLITE_AUX_STEP_TIMEOUT),
        Some(&startup_reporter),
        move || {
            let system_event_store =
                copybot_storage_core::SqliteStore::open(Path::new(&startup_heartbeat_sqlite_path))
                    .context("failed to open startup heartbeat storage core")?;
            system_event_store
                .record_heartbeat("copybot-app", "startup")
                .context("failed to write startup heartbeat")?;
            Ok(store)
        },
    )?;
    let alert_dispatcher = run_inline_startup_step(
        &startup_reporter,
        "startup_alert_dispatcher_init",
        Some(STARTUP_SQLITE_AUX_STEP_TIMEOUT),
        AlertDispatcher::from_env,
    )
    .context("failed to initialize alert delivery")?;
    if let Some(dispatcher) = &alert_dispatcher {
        let alert_sqlite_path = config.sqlite.path.clone();
        store = run_observed_startup_step(
            "startup_alert_delivery_cursor",
            startup_step_policy(STARTUP_SQLITE_AUX_STEP_TIMEOUT),
            Some(&startup_reporter),
            move || {
                let alert_store =
                    copybot_storage_core::SqliteStore::open(Path::new(&alert_sqlite_path))
                        .context("failed to open alert delivery storage core")?;
                alert_store
                    .ensure_alert_delivery_cursor("webhook")
                    .context("failed to initialize alert delivery cursor")?;
                Ok(store)
            },
        )?;
        if dispatcher.test_on_startup() {
            dispatcher
                .send_startup_test(&config.system.env)
                .await
                .context("failed sending startup alert delivery test")?;
            info!("startup alert delivery test sent");
        }
    } else {
        skip_inline_startup_step(
            &startup_reporter,
            "startup_alert_delivery_cursor",
            "alert_dispatcher_disabled",
        );
    }
    store = run_observed_startup_step(
        "startup_sqlite_wal_autocheckpoint_restore",
        startup_step_policy(STARTUP_SQLITE_AUX_STEP_TIMEOUT),
        Some(&startup_reporter),
        move || {
            restore_implicit_startup_sqlite_wal_autocheckpoint(
                store,
                startup_wal_autocheckpoint_restore_pages,
            )
        },
    )?;

    let (ingestion, discovery, shadow) = run_inline_startup_step(
        &startup_reporter,
        "startup_runtime_service_init",
        Some(STARTUP_SQLITE_AUX_STEP_TIMEOUT),
        || {
            let ingestion = IngestionService::build(&config.ingestion)
                .context("failed to initialize ingestion service")?;
            let discovery_http_url = select_role_helius_http_url(
                &config.discovery.helius_http_url,
                &config.ingestion.helius_http_url,
            );
            let shadow_http_url = select_role_helius_http_url(
                &config.shadow.helius_http_url,
                &config.ingestion.helius_http_url,
            );
            let discovery_http_url = enforce_quality_gate_http_url(
                "discovery",
                &config.system.env,
                config.shadow.quality_gates_enabled,
                discovery_http_url,
            )?;
            let shadow_http_url = enforce_quality_gate_http_url(
                "shadow",
                &config.system.env,
                config.shadow.quality_gates_enabled,
                shadow_http_url,
            )?;
            validate_shadow_quality_gate_contract(&config.shadow, &config.system.env)?;
            let discovery = DiscoveryService::new_with_helius(
                config.discovery.clone(),
                config.shadow.clone(),
                discovery_http_url.clone(),
            );
            let shadow = ShadowService::new_with_helius(config.shadow.clone(), shadow_http_url);
            Ok((ingestion, discovery, shadow))
        },
    )?;
    let app_loop_handoff_started = StdInstant::now();
    emit_inline_startup_progress(
        &startup_reporter,
        "startup_app_loop_handoff",
        StartupStepOutcome::Started,
        app_loop_handoff_started,
        None,
        None,
    );
    emit_inline_startup_progress(
        &startup_reporter,
        "startup_app_loop_handoff",
        StartupStepOutcome::Completed,
        app_loop_handoff_started,
        None,
        None,
    );
    run_app_loop(
        store,
        ingestion,
        discovery,
        shadow,
        config.risk.clone(),
        config.sqlite.path.clone(),
        config.system.heartbeat_seconds,
        config.history_retention.clone(),
        config.recent_raw_journal.clone(),
        config.discovery.fetch_refresh_seconds,
        config.discovery.refresh_seconds,
        config.discovery.observed_swaps_retention_days,
        config.ingestion.source.clone(),
        config.shadow.refresh_seconds,
        config.shadow.causal_holdback_enabled,
        config.shadow.causal_holdback_ms,
        config.system.pause_new_trades_on_outage,
        alert_dispatcher,
    )
    .await
}
