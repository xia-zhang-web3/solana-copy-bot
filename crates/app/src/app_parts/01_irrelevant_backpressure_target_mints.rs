#[derive(Debug, Deserialize)]
struct DiscoveryCriticalPersistedRebuildPayloadTargetMints {
    #[serde(default)]
    discovery_critical_target_buy_mints: Vec<String>,
    #[allow(dead_code)]
    #[serde(default)]
    unique_buy_mints: Vec<String>,
}

fn zero_universe_fail_closed_discovery_market_context_mode(
    follow_snapshot: &FollowSnapshot,
    open_shadow_lots: &HashSet<(String, String)>,
    shadow_strategy_fail_closed: bool,
) -> bool {
    shadow_strategy_fail_closed && follow_snapshot.active.is_empty() && open_shadow_lots.is_empty()
}

fn load_discovery_critical_target_buy_mints(store: &SqliteStore) -> Result<HashSet<String>> {
    let Some(state_row) = store.load_discovery_persisted_rebuild_state_read_only()? else {
        return Ok(HashSet::new());
    };
    let payload: DiscoveryCriticalPersistedRebuildPayloadTargetMints =
        serde_json::from_str(&state_row.state_json).context(
            "failed parsing discovery persisted rebuild payload while loading target buy mints for critical market-context persistence",
        )?;
    Ok(payload
        .discovery_critical_target_buy_mints
        .into_iter()
        .collect())
}

fn refresh_discovery_critical_target_buy_mints_or_warn(
    store: &SqliteStore,
    target_buy_mints: &mut HashSet<String>,
) -> Result<()> {
    match load_discovery_critical_target_buy_mints(store) {
        Ok(loaded) => {
            *target_buy_mints = loaded;
            Ok(())
        }
        Err(error) => {
            if is_fatal_sqlite_anyhow_error(&error) {
                return Err(error).context(
                    "failed refreshing discovery-critical target buy mints with fatal sqlite I/O",
                );
            }
            warn!(
                error = %error,
                "failed refreshing discovery-critical target buy mints; keeping the previous target set"
            );
            Ok(())
        }
    }
}

fn refresh_discovery_critical_target_buy_mints_for_backpressure_if_due(
    store: &SqliteStore,
    follow_snapshot: &FollowSnapshot,
    open_shadow_lots: &HashSet<(String, String)>,
    shadow_strategy_fail_closed: bool,
    discovery_critical_target_buy_mints: &mut HashSet<String>,
    backpressure_refresh_state: &mut DiscoveryCriticalTargetBuyMintsBackpressureRefreshState,
    refresh_now: StdInstant,
) -> Result<bool> {
    if !should_refresh_discovery_critical_target_buy_mints_for_backpressure(
        follow_snapshot,
        open_shadow_lots,
        shadow_strategy_fail_closed,
        Some(backpressure_refresh_state),
        refresh_now,
    ) {
        return Ok(false);
    }

    refresh_discovery_critical_target_buy_mints_or_warn(
        store,
        discovery_critical_target_buy_mints,
    )?;
    Ok(true)
}

fn should_refresh_discovery_critical_target_buy_mints_for_backpressure(
    follow_snapshot: &FollowSnapshot,
    open_shadow_lots: &HashSet<(String, String)>,
    shadow_strategy_fail_closed: bool,
    mut backpressure_refresh_state: Option<
        &mut DiscoveryCriticalTargetBuyMintsBackpressureRefreshState,
    >,
    refresh_now: StdInstant,
) -> bool {
    if !zero_universe_fail_closed_discovery_market_context_mode(
        follow_snapshot,
        open_shadow_lots,
        shadow_strategy_fail_closed,
    ) {
        return false;
    }

    let should_refresh = backpressure_refresh_state
        .as_ref()
        .map(|state| state.should_refresh(refresh_now))
        .unwrap_or(true);
    if !should_refresh {
        return false;
    }

    if let Some(state) = backpressure_refresh_state.as_deref_mut() {
        state.note_refresh_attempt(refresh_now);
    }
    true
}

fn refresh_discovery_critical_irrelevant_persistence_for_backpressure(
    store: &SqliteStore,
    swap: &SwapEvent,
    follow_snapshot: &FollowSnapshot,
    open_shadow_lots: &HashSet<(String, String)>,
    shadow_strategy_fail_closed: bool,
    discovery_critical_target_buy_mints: &mut HashSet<String>,
    backpressure_refresh_state: &mut DiscoveryCriticalTargetBuyMintsBackpressureRefreshState,
) -> Result<bool> {
    let _ = refresh_discovery_critical_target_buy_mints_for_backpressure_if_due(
        store,
        follow_snapshot,
        open_shadow_lots,
        shadow_strategy_fail_closed,
        discovery_critical_target_buy_mints,
        backpressure_refresh_state,
        StdInstant::now(),
    )?;
    Ok(
        irrelevant_observed_swap_requires_discovery_critical_persistence(
            swap,
            follow_snapshot,
            open_shadow_lots,
            shadow_strategy_fail_closed,
            discovery_critical_target_buy_mints,
        ),
    )
}
