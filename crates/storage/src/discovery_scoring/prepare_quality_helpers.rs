use super::*;

pub(super) fn parse_ts(raw: &str, field: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .with_context(|| format!("invalid {field} rfc3339 value: {raw}"))
}

pub(super) fn sanitize_prepare_log_value(raw: &str) -> String {
    raw.chars()
        .map(|ch| {
            if ch.is_ascii_whitespace() || ch.is_control() {
                '_'
            } else {
                ch
            }
        })
        .collect()
}

pub(super) fn emit_prepare_stage_start(
    emit: &mut impl FnMut(String),
    stage: &str,
    token: Option<&str>,
    swap_signature: Option<&str>,
) {
    emit(format!(
        "event=backfill_prepare_stage_start stage={} token={} swap_signature={} elapsed_ms=0 outcome=started",
        stage,
        token.map(sanitize_prepare_log_value)
            .unwrap_or_else(|| "none".to_string()),
        swap_signature
            .map(sanitize_prepare_log_value)
            .unwrap_or_else(|| "none".to_string()),
    ));
}

pub(super) fn emit_prepare_stage_end(
    emit: &mut impl FnMut(String),
    stage: &str,
    token: Option<&str>,
    swap_signature: Option<&str>,
    elapsed_ms: u64,
    outcome: &str,
) {
    emit(format!(
        "event=backfill_prepare_stage_end stage={} token={} swap_signature={} elapsed_ms={} outcome={}",
        stage,
        token.map(sanitize_prepare_log_value)
            .unwrap_or_else(|| "none".to_string()),
        swap_signature
            .map(sanitize_prepare_log_value)
            .unwrap_or_else(|| "none".to_string()),
        elapsed_ms,
        outcome,
    ));
}

pub(super) fn emit_prepare_stage_skipped(
    emit: &mut impl FnMut(String),
    stage: &str,
    token: Option<&str>,
    swap_signature: Option<&str>,
    reason: &str,
) {
    emit(format!(
        "event=backfill_prepare_stage_skipped stage={} token={} swap_signature={} elapsed_ms=0 outcome=skipped reason={}",
        stage,
        token.map(sanitize_prepare_log_value)
            .unwrap_or_else(|| "none".to_string()),
        swap_signature
            .map(sanitize_prepare_log_value)
            .unwrap_or_else(|| "none".to_string()),
        reason,
    ));
}

pub(super) fn emit_prepare_batch_counts(emit: &mut impl FnMut(String), swaps: &[SwapEvent]) {
    let buy_count = swaps.iter().filter(|swap| is_sol_buy(swap)).count();
    let sell_count = swaps.iter().filter(|swap| is_sol_sell(swap)).count();
    let other_count = swaps
        .len()
        .saturating_sub(buy_count.saturating_add(sell_count));
    emit(format!(
        "event=backfill_prepare_batch_counts stage=prepare_discovery_scoring_swaps token=none swap_signature=none elapsed_ms=0 outcome=observed total_swaps={} buy_count={} sell_count={} other_count={}",
        swaps.len(),
        buy_count,
        sell_count,
        other_count,
    ));
}

#[cfg(debug_assertions)]
pub(super) fn forced_prepare_runtime_budget_exhausted() -> bool {
    DISCOVERY_SCORING_FORCE_PREPARE_RUNTIME_BUDGET_EXHAUSTED.with(|failpoint| {
        let fired = failpoint.get();
        failpoint.set(false);
        fired
    })
}

#[cfg(not(debug_assertions))]
pub(super) fn forced_prepare_runtime_budget_exhausted() -> bool {
    false
}

pub(super) fn prepare_runtime_budget_exhausted(deadline: Option<Instant>) -> bool {
    forced_prepare_runtime_budget_exhausted()
        || deadline.is_some_and(|deadline| Instant::now() >= deadline)
}

pub(super) fn prepare_runtime_budget_error(
    stage: &str,
    token: Option<&str>,
    swap_signature: Option<&str>,
) -> anyhow::Error {
    anyhow!(
        "{}:stage={}:token={}:swap_signature={}",
        DISCOVERY_SCORING_PREPARE_RUNTIME_BUDGET_EXHAUSTED_REASON,
        stage,
        token
            .map(sanitize_prepare_log_value)
            .unwrap_or_else(|| "none".to_string()),
        swap_signature
            .map(sanitize_prepare_log_value)
            .unwrap_or_else(|| "none".to_string())
    )
}

pub(super) fn check_prepare_runtime_budget(
    deadline: Option<Instant>,
    emit: &mut impl FnMut(String),
    stage: &str,
    token: Option<&str>,
    swap_signature: Option<&str>,
) -> Result<()> {
    if prepare_runtime_budget_exhausted(deadline) {
        emit_prepare_stage_end(
            emit,
            stage,
            token,
            swap_signature,
            0,
            "runtime_budget_exhausted",
        );
        return Err(prepare_runtime_budget_error(stage, token, swap_signature));
    }
    Ok(())
}

pub(super) fn prepare_error_is_runtime_budget(error: &anyhow::Error) -> bool {
    format!("{error:#}").contains(DISCOVERY_SCORING_PREPARE_RUNTIME_BUDGET_EXHAUSTED_REASON)
}

pub(super) fn prepare_stage_error(
    error: anyhow::Error,
    deadline: Option<Instant>,
    stage: &str,
    token: Option<&str>,
    swap_signature: Option<&str>,
) -> anyhow::Error {
    if prepare_runtime_budget_exhausted(deadline) {
        prepare_runtime_budget_error(stage, token, swap_signature).context(error)
    } else {
        error
    }
}

pub(super) fn token_market_stats_on_conn(
    conn: &Connection,
    token: &str,
    as_of: DateTime<Utc>,
) -> Result<TokenMarketStats> {
    let window_start = (as_of - Duration::minutes(5)).to_rfc3339();
    let window_end = as_of.to_rfc3339();
    let (volume_5m_sol, liquidity_sol_proxy, unique_traders_5m_raw): (f64, f64, i64) = conn
        .query_row(
            "SELECT
                COALESCE(SUM(sol_notional), 0.0) AS volume_5m_sol,
                COALESCE(MAX(sol_notional), 0.0) AS liquidity_sol_proxy,
                COUNT(DISTINCT wallet_id) AS unique_traders_5m
             FROM (
                SELECT wallet_id, qty_out AS sol_notional
                FROM observed_swaps INDEXED BY idx_observed_swaps_token_in_out_ts
                WHERE token_in = ?1
                  AND token_out = ?2
                  AND ts >= ?3
                  AND ts <= ?4
                UNION ALL
                SELECT wallet_id, qty_in AS sol_notional
                FROM observed_swaps INDEXED BY idx_observed_swaps_token_out_in_ts
                WHERE token_out = ?1
                  AND token_in = ?2
                  AND ts >= ?3
                  AND ts <= ?4
             )",
            params![token, SOL_MINT, window_start, window_end],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .context("failed querying token 5m market stats for discovery scoring")?;

    Ok(TokenMarketStats {
        first_seen: None,
        holders_proxy: 0,
        liquidity_sol_proxy,
        volume_5m_sol,
        unique_traders_5m: unique_traders_5m_raw.max(0) as u64,
    })
}

pub(super) fn load_token_quality_cache_on_conn(
    conn: &Connection,
    mint: &str,
) -> Result<Option<QualityCacheRowLocal>> {
    let row: Option<(Option<i64>, Option<f64>, Option<i64>, String)> = conn
        .query_row(
            "SELECT holders, liquidity_sol, token_age_seconds, fetched_at
             FROM token_quality_cache
             WHERE mint = ?1",
            params![mint],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .context("failed querying token_quality_cache in discovery scoring write path")?;

    row.map(
        |(holders_raw, liquidity_sol, token_age_seconds_raw, fetched_at_raw)| -> Result<_> {
            Ok(QualityCacheRowLocal {
                holders: holders_raw.map(|value| value.max(0) as u64),
                liquidity_sol,
                token_age_seconds: token_age_seconds_raw.map(|value| value.max(0) as u64),
                fetched_at: parse_ts(&fetched_at_raw, "token_quality_cache.fetched_at")?,
            })
        },
    )
    .transpose()
}

pub(super) fn upsert_token_quality_cache_on_conn(
    conn: &Connection,
    mint: &str,
    holders: Option<u64>,
    liquidity_sol: Option<f64>,
    token_age_seconds: Option<u64>,
    fetched_at: DateTime<Utc>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO token_quality_cache(
            mint,
            holders,
            liquidity_sol,
            token_age_seconds,
            fetched_at
         ) VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(mint) DO UPDATE SET
            holders = excluded.holders,
            liquidity_sol = excluded.liquidity_sol,
            token_age_seconds = excluded.token_age_seconds,
            fetched_at = excluded.fetched_at",
        params![
            mint,
            holders.map(|value| value as i64),
            liquidity_sol,
            token_age_seconds.map(|value| value as i64),
            fetched_at.to_rfc3339(),
        ],
    )
    .context("failed upserting token_quality_cache row in discovery scoring write path")?;
    Ok(())
}

pub(super) fn cached_quality_snapshot(
    cached: Option<QualityCacheRowLocal>,
    signal_ts: DateTime<Utc>,
    missing_source: WalletScoringQualitySource,
) -> QualitySnapshot {
    match cached {
        Some(row) => QualitySnapshot {
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
        },
        None => QualitySnapshot {
            source: missing_source,
            token_age_seconds: None,
            holders: None,
            liquidity_sol: None,
        },
    }
}
