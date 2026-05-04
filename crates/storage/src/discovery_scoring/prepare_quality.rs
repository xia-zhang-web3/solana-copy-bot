fn resolve_quality_snapshot_on_conn_with_diagnostics(
    conn: &Connection,
    mint: &str,
    signal_ts: DateTime<Utc>,
    config: &DiscoveryAggregateWriteConfig,
    budget: &mut QualityFetchBudget,
    emit: &mut impl FnMut(String),
    deadline: Option<Instant>,
    swap_signature: Option<&str>,
) -> Result<(QualitySnapshot, Option<QualityCacheUpsert>)> {
    emit_prepare_stage_start(emit, "quality_cache_lookup", Some(mint), swap_signature);
    let cache_started_at = Instant::now();
    check_prepare_runtime_budget(
        deadline,
        emit,
        "quality_cache_lookup",
        Some(mint),
        swap_signature,
    )?;
    let cached = match load_token_quality_cache_on_conn(conn, mint) {
        Ok(cached) => {
            emit_prepare_stage_end(
                emit,
                "quality_cache_lookup",
                Some(mint),
                swap_signature,
                cache_started_at.elapsed().as_millis() as u64,
                "completed",
            );
            cached
        }
        Err(error) => {
            let error = prepare_stage_error(
                error,
                deadline,
                "quality_cache_lookup",
                Some(mint),
                swap_signature,
            );
            emit_prepare_stage_end(
                emit,
                "quality_cache_lookup",
                Some(mint),
                swap_signature,
                cache_started_at.elapsed().as_millis() as u64,
                if prepare_error_is_runtime_budget(&error) {
                    "runtime_budget_exhausted"
                } else {
                    "failed"
                },
            );
            return Err(error);
        }
    };
    let ttl = Duration::seconds(QUALITY_CACHE_TTL_SECONDS);
    if let Some(row) = cached.as_ref() {
        if signal_ts.signed_duration_since(row.fetched_at) <= ttl {
            emit_prepare_stage_skipped(
                emit,
                "quality_rpc_fetch",
                Some(mint),
                swap_signature,
                "fresh_cache_hit",
            );
            return Ok((
                QualitySnapshot {
                    source: WalletScoringQualitySource::Fresh,
                    token_age_seconds: row.token_age_seconds,
                    holders: row.holders,
                    liquidity_sol: row.liquidity_sol,
                },
                None,
            ));
        }
    }

    let Some(helius_http_url) = config.helius_http_url.as_deref() else {
        emit_prepare_stage_skipped(
            emit,
            "quality_rpc_fetch",
            Some(mint),
            swap_signature,
            "helius_http_url_absent",
        );
        return Ok((
            cached_quality_snapshot(cached, signal_ts, WalletScoringQualitySource::Missing),
            None,
        ));
    };

    if budget.rpc_attempted >= QUALITY_MAX_FETCH_PER_BATCH {
        emit_prepare_stage_skipped(
            emit,
            "quality_rpc_fetch",
            Some(mint),
            swap_signature,
            "quality_rpc_max_fetch_per_batch_exhausted",
        );
        return Ok((
            cached_quality_snapshot(cached, signal_ts, WalletScoringQualitySource::Deferred),
            None,
        ));
    }

    let started_at = budget.started_at.get_or_insert_with(Instant::now);
    if started_at.elapsed().as_millis() as u64 >= QUALITY_RPC_BUDGET_MS {
        emit_prepare_stage_skipped(
            emit,
            "quality_rpc_fetch",
            Some(mint),
            swap_signature,
            "quality_rpc_budget_exhausted",
        );
        return Ok((
            cached_quality_snapshot(cached, signal_ts, WalletScoringQualitySource::Deferred),
            None,
        ));
    }

    budget.rpc_attempted = budget.rpc_attempted.saturating_add(1);
    emit_prepare_stage_start(emit, "quality_rpc_fetch", Some(mint), swap_signature);
    let rpc_started_at = Instant::now();
    check_prepare_runtime_budget(
        deadline,
        emit,
        "quality_rpc_fetch",
        Some(mint),
        swap_signature,
    )?;
    let fetched = SqliteStore::fetch_token_quality_from_helius(
        helius_http_url,
        mint,
        QUALITY_RPC_TIMEOUT_MS,
        QUALITY_MAX_SIGNATURE_PAGES,
        config.min_token_age_hint_seconds,
    );
    match fetched {
        Ok(fetched) => {
            emit_prepare_stage_end(
                emit,
                "quality_rpc_fetch",
                Some(mint),
                swap_signature,
                rpc_started_at.elapsed().as_millis() as u64,
                "completed",
            );
            Ok((
                QualitySnapshot {
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
        Err(_) => {
            emit_prepare_stage_end(
                emit,
                "quality_rpc_fetch",
                Some(mint),
                swap_signature,
                rpc_started_at.elapsed().as_millis() as u64,
                "failed",
            );
            Ok((
                cached_quality_snapshot(cached, signal_ts, WalletScoringQualitySource::Missing),
                None,
            ))
        }
    }
}
