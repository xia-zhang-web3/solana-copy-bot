impl DiscoveryService {
    pub(super) fn resolve_token_quality_for_mints(
        &self,
        store: &SqliteStore,
        mints: &[String],
        now: DateTime<Utc>,
    ) -> Result<HashMap<String, TokenQualityResolution>> {
        if mints.is_empty() {
            return Ok(HashMap::new());
        }

        let mut out = HashMap::new();
        let mut progress = TokenQualityResolutionProgress::default();
        let deadline = Instant::now() + StdDuration::from_secs(3_600);
        while progress.next_mint_index < mints.len() {
            let outcome = self.resolve_token_quality_for_mints_chunk(
                store,
                mints,
                now,
                &mut out,
                &mut progress,
                usize::MAX,
                deadline,
            )?;
            if outcome.source_exhausted {
                break;
            }
            if outcome.processed_mints == 0 {
                break;
            }
        }

        info!(
            quality_source = if self.helius_http_url.is_some() {
                "cache+rpc+db_proxy"
            } else {
                "cache+db_proxy"
            },
            rpc_enabled = self.helius_http_url.is_some(),
            mints_total = mints.len(),
            resolved_total = out.len(),
            rpc_attempted = progress.rpc_attempted,
            rpc_budget_ms = QUALITY_RPC_BUDGET_MS,
            rpc_spent_ms = progress.rpc_spent_ms,
            "discovery token quality cache summary"
        );

        Ok(out)
    }

    pub(super) fn resolve_token_quality_for_mints_chunk(
        &self,
        store: &SqliteStore,
        mints: &[String],
        now: DateTime<Utc>,
        out: &mut HashMap<String, TokenQualityResolution>,
        progress: &mut TokenQualityResolutionProgress,
        max_mints: usize,
        deadline: Instant,
    ) -> Result<TokenQualityResolutionChunkOutcome> {
        if progress.next_mint_index >= mints.len() {
            return Ok(TokenQualityResolutionChunkOutcome {
                processed_mints: 0,
                source_exhausted: true,
            });
        }

        let ttl = Duration::seconds(QUALITY_CACHE_TTL_SECONDS);
        let helius_http_url = self.helius_http_url.as_deref();
        let mut processed_mints = 0usize;
        let max_mints = max_mints.max(1);

        while progress.next_mint_index < mints.len() {
            if processed_mints >= max_mints || Instant::now() >= deadline {
                break;
            }

            let mint = mints[progress.next_mint_index].clone();
            if out.contains_key(&mint) {
                progress.next_mint_index = progress.next_mint_index.saturating_add(1);
                processed_mints = processed_mints.saturating_add(1);
                continue;
            }

            match store.get_token_quality_cache(&mint) {
                Ok(Some(row)) => {
                    if now - row.fetched_at <= ttl {
                        out.insert(mint, TokenQualityResolution::Fresh(row));
                    } else if let Some(helius_http_url) = helius_http_url {
                        if progress.rpc_attempted >= QUALITY_MAX_FETCH_PER_CYCLE
                            || progress.rpc_spent_ms >= QUALITY_RPC_BUDGET_MS
                        {
                            out.insert(mint, TokenQualityResolution::Stale(row));
                        } else {
                            let rpc_started = Instant::now();
                            progress.rpc_attempted = progress.rpc_attempted.saturating_add(1);
                            match fetch_token_quality_from_helius_guarded(
                                helius_http_url,
                                &mint,
                                QUALITY_RPC_TIMEOUT_MS,
                                QUALITY_MAX_SIGNATURE_PAGES,
                                Some(self.shadow_quality.min_token_age_seconds),
                            ) {
                                Ok(fetched) => {
                                    progress.rpc_spent_ms = progress
                                        .rpc_spent_ms
                                        .saturating_add(rpc_started.elapsed().as_millis() as u64);
                                    if let Err(error) = store.upsert_token_quality_cache(
                                        &mint,
                                        fetched.holders,
                                        fetched.liquidity_sol,
                                        fetched.token_age_seconds,
                                        now,
                                    ) {
                                        if discovery_quality_cache_error_requires_abort(&error) {
                                            return Err(error).context(
                                                "discovery token quality cache refresh write failed with fatal sqlite I/O",
                                            );
                                        }
                                        warn!(
                                            error = %error,
                                            mint = %mint,
                                            "failed updating token quality cache"
                                        );
                                    }
                                    match store.get_token_quality_cache(&mint) {
                                        Ok(Some(refreshed_row)) => {
                                            out.insert(
                                                mint,
                                                TokenQualityResolution::Fresh(refreshed_row),
                                            );
                                        }
                                        Ok(None) => {
                                            warn!(
                                                mint = %mint,
                                                "token quality cache row missing after refresh"
                                            );
                                            out.insert(mint, TokenQualityResolution::Stale(row));
                                        }
                                        Err(error) => {
                                            if discovery_quality_cache_error_requires_abort(&error)
                                            {
                                                return Err(error).context(
                                                    "discovery token quality cache refresh readback failed with fatal sqlite I/O",
                                                );
                                            }
                                            warn!(
                                                error = %error,
                                                mint = %mint,
                                                "failed reading token quality cache after refresh"
                                            );
                                            out.insert(mint, TokenQualityResolution::Stale(row));
                                        }
                                    }
                                }
                                Err(error) => {
                                    progress.rpc_spent_ms = progress
                                        .rpc_spent_ms
                                        .saturating_add(rpc_started.elapsed().as_millis() as u64);
                                    warn!(
                                        error = %error,
                                        mint = %mint,
                                        "failed to refresh token quality via helius, using fallback"
                                    );
                                    out.insert(mint, TokenQualityResolution::Stale(row));
                                }
                            }
                        }
                    } else {
                        out.insert(mint, TokenQualityResolution::Stale(row));
                    }
                }
                Ok(None) => {
                    if let Some(helius_http_url) = helius_http_url {
                        if progress.rpc_attempted >= QUALITY_MAX_FETCH_PER_CYCLE
                            || progress.rpc_spent_ms >= QUALITY_RPC_BUDGET_MS
                        {
                            out.insert(mint, TokenQualityResolution::Deferred);
                        } else {
                            let rpc_started = Instant::now();
                            progress.rpc_attempted = progress.rpc_attempted.saturating_add(1);
                            match fetch_token_quality_from_helius_guarded(
                                helius_http_url,
                                &mint,
                                QUALITY_RPC_TIMEOUT_MS,
                                QUALITY_MAX_SIGNATURE_PAGES,
                                Some(self.shadow_quality.min_token_age_seconds),
                            ) {
                                Ok(fetched) => {
                                    progress.rpc_spent_ms = progress
                                        .rpc_spent_ms
                                        .saturating_add(rpc_started.elapsed().as_millis() as u64);
                                    if let Err(error) = store.upsert_token_quality_cache(
                                        &mint,
                                        fetched.holders,
                                        fetched.liquidity_sol,
                                        fetched.token_age_seconds,
                                        now,
                                    ) {
                                        if discovery_quality_cache_error_requires_abort(&error) {
                                            return Err(error).context(
                                                "discovery token quality cache refresh write failed with fatal sqlite I/O",
                                            );
                                        }
                                        warn!(
                                            error = %error,
                                            mint = %mint,
                                            "failed updating token quality cache"
                                        );
                                    }
                                    match store.get_token_quality_cache(&mint) {
                                        Ok(Some(row)) => {
                                            out.insert(mint, TokenQualityResolution::Fresh(row));
                                        }
                                        Ok(None) => {
                                            warn!(
                                                mint = %mint,
                                                "token quality cache row missing after refresh"
                                            );
                                            out.insert(mint, TokenQualityResolution::Missing);
                                        }
                                        Err(error) => {
                                            if discovery_quality_cache_error_requires_abort(&error)
                                            {
                                                return Err(error).context(
                                                    "discovery token quality cache refresh readback failed with fatal sqlite I/O",
                                                );
                                            }
                                            warn!(
                                                error = %error,
                                                mint = %mint,
                                                "failed reading token quality cache after refresh"
                                            );
                                            out.insert(mint, TokenQualityResolution::Missing);
                                        }
                                    }
                                }
                                Err(error) => {
                                    progress.rpc_spent_ms = progress
                                        .rpc_spent_ms
                                        .saturating_add(rpc_started.elapsed().as_millis() as u64);
                                    warn!(
                                        error = %error,
                                        mint = %mint,
                                        "failed to refresh token quality via helius, using fallback"
                                    );
                                    out.insert(mint, TokenQualityResolution::Missing);
                                }
                            }
                        }
                    } else {
                        out.insert(mint, TokenQualityResolution::Missing);
                    }
                }
                Err(error) => {
                    if discovery_quality_cache_error_requires_abort(&error) {
                        return Err(error).context(
                            "discovery token quality cache lookup failed with fatal sqlite I/O",
                        );
                    }
                    warn!(error = %error, mint = %mint, "failed reading token quality cache");
                }
            }

            progress.next_mint_index = progress.next_mint_index.saturating_add(1);
            processed_mints = processed_mints.saturating_add(1);
        }

        Ok(TokenQualityResolutionChunkOutcome {
            processed_mints,
            source_exhausted: progress.next_mint_index >= mints.len(),
        })
    }
}
