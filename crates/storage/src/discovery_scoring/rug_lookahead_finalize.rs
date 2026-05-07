use super::*;

pub(super) fn rug_lookahead_stats_on_conn(
    conn: &Connection,
    token: &str,
    buy_ts: DateTime<Utc>,
    lookahead_end: DateTime<Utc>,
) -> Result<(f64, u32)> {
    note_rug_lookahead_stats_call_for_tests();
    if rug_lookahead_unknown_failpoint_triggered() {
        return Err(anyhow!(
            "test failpoint: discovery scoring rug lookahead unknown failure"
        ))
        .context("failed querying discovery scoring rug lookahead stats");
    }
    if rug_lookahead_budget_failpoint_triggered() {
        return Err(anyhow!(
            DISCOVERY_AGGREGATE_REPAIR_LOCK_FIRST_BUDGET_EXHAUSTED_WITHOUT_PROGRESS
        ))
        .context("failed querying discovery scoring rug lookahead stats");
    }
    let (volume_sol, unique_traders_raw): (f64, i64) = conn
        .query_row(
            RUG_LOOKAHEAD_STATS_QUERY,
            params![
                token,
                SOL_MINT,
                buy_ts.to_rfc3339(),
                lookahead_end.to_rfc3339(),
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .context("failed querying discovery scoring rug lookahead stats")?;
    Ok((volume_sol.max(0.0), unique_traders_raw.max(0) as u32))
}

pub(super) fn finalize_mature_rug_facts_on_conn(
    conn: &Connection,
    watermark_ts: DateTime<Utc>,
) -> Result<()> {
    let mut stmt = conn
        .prepare(
            "SELECT buy_signature, token, ts, rug_check_after_ts
             FROM wallet_scoring_buy_facts
             WHERE rug_volume_lookahead_sol IS NULL
               AND rug_check_after_ts <= ?1
             ORDER BY rug_check_after_ts ASC, ts ASC",
        )
        .context("failed preparing pending rug facts query")?;
    let mut rows = stmt
        .query(params![watermark_ts.to_rfc3339()])
        .context("failed querying pending rug facts")?;
    let mut pending = Vec::new();
    while let Some(row) = rows.next().context("failed iterating pending rug facts")? {
        let buy_ts_raw: String = row
            .get(2)
            .context("failed reading wallet_scoring_buy_facts.ts")?;
        let check_after_raw: String = row
            .get(3)
            .context("failed reading wallet_scoring_buy_facts.rug_check_after_ts")?;
        pending.push((
            row.get::<_, String>(0)
                .context("failed reading wallet_scoring_buy_facts.buy_signature")?,
            row.get::<_, String>(1)
                .context("failed reading wallet_scoring_buy_facts.token")?,
            parse_ts(&buy_ts_raw, "wallet_scoring_buy_facts.ts")?,
            parse_ts(
                &check_after_raw,
                "wallet_scoring_buy_facts.rug_check_after_ts",
            )?,
        ));
    }

    for (buy_signature, token, buy_ts, check_after_ts) in pending {
        let (volume_sol, unique_traders) =
            rug_lookahead_stats_on_conn(conn, &token, buy_ts, check_after_ts)?;
        conn.execute(
            "UPDATE wallet_scoring_buy_facts
             SET rug_volume_lookahead_sol = ?2,
                 rug_unique_traders_lookahead = ?3
             WHERE buy_signature = ?1",
            params![buy_signature, volume_sol, unique_traders as i64],
        )
        .context("failed updating matured rug facts")?;
    }
    Ok(())
}

pub(super) fn finalize_repair_prefix_rug_facts_defer_budget_hotspot_on_conn(
    conn: &Connection,
    swaps: &[SwapEvent],
) -> Result<RugLookaheadFinalizeOutcome> {
    let facts = load_current_repair_prefix_pending_rug_facts_on_conn(conn, swaps)?;
    if facts.is_empty() {
        return Ok(RugLookaheadFinalizeOutcome::default());
    }
    let mut outcome = RugLookaheadFinalizeOutcome {
        batch_prefetch_used: true,
        ..RugLookaheadFinalizeOutcome::default()
    };
    let batch_stats = repair_prefix_rug_lookahead_stats_batch_on_conn(conn, &facts);
    let stats_by_signature = match batch_stats {
        Ok(stats) => stats,
        Err(error) if rug_lookahead_error_is_budget_hotspot(&error) => {
            outcome.deferred_due_to_budget_hotspot = true;
            for fact in &facts {
                conn.execute(
                    "UPDATE wallet_scoring_buy_facts
                     SET rug_volume_lookahead_sol = 0.0,
                         rug_unique_traders_lookahead = 0
                     WHERE buy_signature = ?1",
                    params![fact.buy_signature.as_str()],
                )
                .context("failed deferring current repair rug fact")?;
            }
            outcome.deferred_count = facts.len();
            return Ok(outcome);
        }
        Err(error) => return Err(error),
    };
    for fact in &facts {
        let (volume_sol, unique_traders) = stats_by_signature
            .get(fact.buy_signature.as_str())
            .copied()
            .unwrap_or((0.0, 0));
        conn.execute(
            "UPDATE wallet_scoring_buy_facts
             SET rug_volume_lookahead_sol = ?2,
                 rug_unique_traders_lookahead = ?3
             WHERE buy_signature = ?1",
            params![
                fact.buy_signature.as_str(),
                volume_sol,
                unique_traders as i64
            ],
        )
        .context("failed updating current repair rug fact")?;
        outcome.exact_count = outcome.exact_count.saturating_add(1);
    }
    Ok(outcome)
}

fn load_current_repair_prefix_pending_rug_facts_on_conn(
    conn: &Connection,
    swaps: &[SwapEvent],
) -> Result<Vec<RepairRugFact>> {
    let mut facts = Vec::new();
    for swap in swaps.iter().filter(|swap| is_sol_buy(swap)) {
        let pending = conn
            .query_row(
                "SELECT token, ts, rug_check_after_ts
                 FROM wallet_scoring_buy_facts
                 WHERE buy_signature = ?1
                   AND (rug_volume_lookahead_sol IS NULL
                        OR rug_unique_traders_lookahead IS NULL)",
                params![swap.signature.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .context("failed loading current repair rug fact by buy signature")?;
        let Some((token, buy_ts_raw, check_after_raw)) = pending else {
            continue;
        };
        let buy_ts = parse_ts(&buy_ts_raw, "wallet_scoring_buy_facts.ts")?;
        let check_after_ts = parse_ts(
            &check_after_raw,
            "wallet_scoring_buy_facts.rug_check_after_ts",
        )?;
        facts.push(RepairRugFact {
            buy_signature: swap.signature.clone(),
            token,
            buy_ts,
            check_after_ts,
        });
    }
    Ok(facts)
}

fn repair_prefix_rug_lookahead_stats_batch_on_conn(
    conn: &Connection,
    facts: &[RepairRugFact],
) -> Result<HashMap<String, (f64, u32)>> {
    if rug_lookahead_unknown_failpoint_triggered() {
        return Err(anyhow!(
            "test failpoint: discovery scoring rug lookahead unknown failure"
        ))
        .context("failed querying discovery scoring rug lookahead stats batch");
    }
    if rug_lookahead_batch_budget_failpoint_triggered() {
        return Err(anyhow!(
            DISCOVERY_AGGREGATE_REPAIR_LOCK_FIRST_BUDGET_EXHAUSTED_WITHOUT_PROGRESS
        ))
        .context("failed querying discovery scoring rug lookahead stats batch");
    }

    let mut token_windows: HashMap<String, (DateTime<Utc>, DateTime<Utc>)> = HashMap::new();
    for fact in facts {
        token_windows
            .entry(fact.token.clone())
            .and_modify(|(start, end)| {
                *start = (*start).min(fact.buy_ts);
                *end = (*end).max(fact.check_after_ts);
            })
            .or_insert((fact.buy_ts, fact.check_after_ts));
    }

    let mut events_by_token: HashMap<String, Vec<RepairRugLookaheadEvent>> = HashMap::new();
    for (token, (window_start, window_end)) in token_windows {
        events_by_token.insert(
            token.clone(),
            load_repair_rug_lookahead_events_for_token_on_conn(
                conn,
                &token,
                window_start,
                window_end,
            )?,
        );
    }

    let mut stats_by_signature = HashMap::new();
    for fact in facts {
        let mut volume_sol = 0.0f64;
        let mut unique_wallets = HashSet::new();
        if let Some(events) = events_by_token.get(&fact.token) {
            for event in events {
                if event.ts >= fact.buy_ts && event.ts <= fact.check_after_ts {
                    volume_sol += event.sol_notional.max(0.0);
                    unique_wallets.insert(event.wallet_id.as_str());
                }
            }
        }
        stats_by_signature.insert(
            fact.buy_signature.clone(),
            (volume_sol.max(0.0), unique_wallets.len() as u32),
        );
    }
    Ok(stats_by_signature)
}

fn load_repair_rug_lookahead_events_for_token_on_conn(
    conn: &Connection,
    token: &str,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> Result<Vec<RepairRugLookaheadEvent>> {
    let mut stmt = conn
        .prepare(
            "SELECT wallet_id, ts, sol_notional
             FROM (
                SELECT wallet_id, ts, qty_out AS sol_notional
                FROM observed_swaps INDEXED BY idx_observed_swaps_token_in_out_ts
                WHERE token_in = ?1
                  AND token_out = ?2
                  AND ts >= ?3
                  AND ts <= ?4
                UNION ALL
                SELECT wallet_id, ts, qty_in AS sol_notional
                FROM observed_swaps INDEXED BY idx_observed_swaps_token_out_in_ts
                WHERE token_out = ?1
                  AND token_in = ?2
                  AND ts >= ?3
                  AND ts <= ?4
             )",
        )
        .context("failed preparing discovery scoring rug lookahead stats batch query")?;
    let mut rows = stmt
        .query(params![
            token,
            SOL_MINT,
            window_start.to_rfc3339(),
            window_end.to_rfc3339()
        ])
        .context("failed querying discovery scoring rug lookahead stats batch")?;
    let mut events = Vec::new();
    while let Some(row) = rows
        .next()
        .context("failed iterating discovery scoring rug lookahead stats batch")?
    {
        let ts_raw: String = row
            .get(1)
            .context("failed reading rug lookahead batch ts")?;
        events.push(RepairRugLookaheadEvent {
            wallet_id: row
                .get(0)
                .context("failed reading rug lookahead batch wallet_id")?,
            ts: parse_ts(&ts_raw, "observed_swaps.ts")?,
            sol_notional: row
                .get::<_, f64>(2)
                .context("failed reading rug lookahead batch sol_notional")?,
        });
    }
    Ok(events)
}

fn rug_lookahead_error_is_budget_hotspot(error: &anyhow::Error) -> bool {
    let lowered = format!("{error:#}").to_ascii_lowercase();
    lowered.contains(DISCOVERY_AGGREGATE_REPAIR_LOCK_FIRST_BUDGET_EXHAUSTED_WITHOUT_PROGRESS)
        || lowered.contains("interrupted")
}
