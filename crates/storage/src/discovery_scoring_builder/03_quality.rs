use super::*;

pub(super) fn seed_market_windows_from_lookback(
    store: &SqliteStore,
    starting_cursor: CursorRef<'_>,
) -> Result<HashMap<String, RollingTokenMarketState>> {
    let lookback_start = starting_cursor.ts - Duration::minutes(5);
    let mut out = HashMap::<String, RollingTokenMarketState>::new();
    store.for_each_observed_swap_in_window(lookback_start, starting_cursor.ts, |swap| {
        if cmp_swap_to_cursor(&swap, starting_cursor) == std::cmp::Ordering::Greater {
            return Ok(());
        }
        let Some((token, sol_notional)) = sol_pair_market_event(&swap) else {
            return Ok(());
        };
        let state = out.entry(token.to_string()).or_default();
        state.evict_before(swap.ts_utc - Duration::minutes(5));
        state.push(&swap, sol_notional);
        Ok(())
    })?;
    Ok(out)
}

fn load_quality_cache_state(
    store: &SqliteStore,
    mint: &str,
) -> Result<Option<BuilderQualityCacheState>> {
    Ok(store
        .get_token_quality_cache(mint)?
        .map(|row: TokenQualityCacheRow| BuilderQualityCacheState {
            holders: row.holders,
            liquidity_sol: row.liquidity_sol,
            token_age_seconds: row.token_age_seconds,
            fetched_at: row.fetched_at,
        }))
}

pub(super) fn resolve_quality_snapshot(
    store: &SqliteStore,
    cache: &mut HashMap<String, Option<BuilderQualityCacheState>>,
    signal_ts: DateTime<Utc>,
    mint: &str,
    config: &DiscoveryAggregateWriteConfig,
    budget: &mut QualityFetchBudget,
) -> Result<(BuilderQualitySnapshot, Option<QualityCacheUpsert>)> {
    let cached = if let Some(cached) = cache.get(mint) {
        cached.clone()
    } else {
        let loaded = load_quality_cache_state(store, mint)?;
        cache.insert(mint.to_string(), loaded.clone());
        loaded
    };
    let ttl = Duration::seconds(QUALITY_CACHE_TTL_SECONDS);
    if let Some(row) = cached.as_ref() {
        if signal_ts.signed_duration_since(row.fetched_at) <= ttl {
            return Ok((
                BuilderQualitySnapshot {
                    source: WalletScoringQualitySource::Fresh,
                    token_age_seconds: row.token_age_seconds,
                    holders: row.holders,
                    liquidity_sol: row.liquidity_sol,
                },
                None,
            ));
        }
    }

    let stale_snapshot = |row: &BuilderQualityCacheState| BuilderQualitySnapshot {
        source: WalletScoringQualitySource::Stale,
        token_age_seconds: row.token_age_seconds.map(|age| {
            age.saturating_add(
                signal_ts
                    .signed_duration_since(row.fetched_at)
                    .num_seconds()
                    .max(0) as u64,
            )
        }),
        holders: row.holders,
        liquidity_sol: row.liquidity_sol,
    };

    let deferred_or_missing = || match cached.as_ref() {
        Some(row) => stale_snapshot(row),
        None => BuilderQualitySnapshot {
            source: if config.helius_http_url.is_some() {
                WalletScoringQualitySource::Deferred
            } else {
                WalletScoringQualitySource::Missing
            },
            token_age_seconds: None,
            holders: None,
            liquidity_sol: None,
        },
    };

    let Some(helius_http_url) = config.helius_http_url.as_deref() else {
        return Ok((deferred_or_missing(), None));
    };
    if budget.rpc_attempted >= QUALITY_MAX_FETCH_PER_BATCH {
        return Ok((deferred_or_missing(), None));
    }
    let started_at = budget.started_at.get_or_insert_with(Instant::now);
    if started_at.elapsed().as_millis() as u64 >= QUALITY_RPC_BUDGET_MS {
        return Ok((deferred_or_missing(), None));
    }

    budget.rpc_attempted = budget.rpc_attempted.saturating_add(1);
    match SqliteStore::fetch_token_quality_from_helius(
        helius_http_url,
        mint,
        QUALITY_RPC_TIMEOUT_MS,
        QUALITY_MAX_SIGNATURE_PAGES,
        config.min_token_age_hint_seconds,
    ) {
        Ok(fetched) => {
            let updated = BuilderQualityCacheState {
                holders: fetched.holders,
                liquidity_sol: fetched.liquidity_sol,
                token_age_seconds: fetched.token_age_seconds,
                fetched_at: signal_ts,
            };
            cache.insert(mint.to_string(), Some(updated));
            Ok((
                BuilderQualitySnapshot {
                    source: WalletScoringQualitySource::Fresh,
                    token_age_seconds: fetched.token_age_seconds,
                    holders: fetched.holders,
                    liquidity_sol: fetched.liquidity_sol,
                },
                Some(QualityCacheUpsert {
                    mint: mint.to_string(),
                    holders: fetched.holders,
                    liquidity_sol: fetched.liquidity_sol,
                    token_age_seconds: fetched.token_age_seconds,
                    fetched_at: signal_ts,
                }),
            ))
        }
        Err(_) => Ok((deferred_or_missing(), None)),
    }
}
