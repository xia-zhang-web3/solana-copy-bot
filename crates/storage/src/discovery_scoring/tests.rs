use super::*;

const TEST_TOKEN: &str = "TokenRugLookahead111111111111111111111111";

fn test_ts(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .expect("valid test timestamp")
        .with_timezone(&Utc)
}

fn setup_rug_lookahead_conn() -> Result<Connection> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch(
        "CREATE TABLE observed_swaps (
                wallet_id TEXT NOT NULL,
                token_in TEXT NOT NULL,
                token_out TEXT NOT NULL,
                qty_in REAL NOT NULL,
                qty_out REAL NOT NULL,
                ts TEXT NOT NULL
             );
             CREATE INDEX idx_observed_swaps_token_in_out_ts
                ON observed_swaps(token_in, token_out, ts);
             CREATE INDEX idx_observed_swaps_token_out_in_ts
                ON observed_swaps(token_out, token_in, ts);",
    )?;
    Ok(conn)
}

fn insert_observed_swap(
    conn: &Connection,
    wallet: &str,
    token_in: &str,
    token_out: &str,
    qty_in: f64,
    qty_out: f64,
    ts: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO observed_swaps(wallet_id, token_in, token_out, qty_in, qty_out, ts)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![wallet, token_in, token_out, qty_in, qty_out, ts],
    )?;
    Ok(())
}

#[test]
fn rug_lookahead_stats_preserve_sol_leg_volume_and_unique_wallet_semantics() -> Result<()> {
    let conn = setup_rug_lookahead_conn()?;
    insert_observed_swap(
        &conn,
        "wallet-a",
        TEST_TOKEN,
        SOL_MINT,
        10.0,
        2.0,
        "2026-04-29T00:00:00+00:00",
    )?;
    insert_observed_swap(
        &conn,
        "wallet-b",
        SOL_MINT,
        TEST_TOKEN,
        3.0,
        30.0,
        "2026-04-29T00:01:00+00:00",
    )?;
    insert_observed_swap(
        &conn,
        "wallet-a",
        SOL_MINT,
        TEST_TOKEN,
        5.0,
        50.0,
        "2026-04-29T00:02:00+00:00",
    )?;
    insert_observed_swap(
        &conn,
        "wallet-irrelevant-token",
        "OtherToken111111111111111111111111111111",
        SOL_MINT,
        1.0,
        99.0,
        "2026-04-29T00:02:00+00:00",
    )?;
    insert_observed_swap(
        &conn,
        "wallet-before",
        TEST_TOKEN,
        SOL_MINT,
        1.0,
        99.0,
        "2026-04-28T23:59:59+00:00",
    )?;
    insert_observed_swap(
        &conn,
        "wallet-after",
        SOL_MINT,
        TEST_TOKEN,
        99.0,
        1.0,
        "2026-04-29T00:05:01+00:00",
    )?;

    let (volume_sol, unique_wallets) = rug_lookahead_stats_on_conn(
        &conn,
        TEST_TOKEN,
        test_ts("2026-04-29T00:00:00Z"),
        test_ts("2026-04-29T00:05:00Z"),
    )?;

    assert!((volume_sol - 10.0).abs() < 1e-9);
    assert_eq!(unique_wallets, 2);
    Ok(())
}

#[test]
fn rug_lookahead_query_plan_uses_existing_token_pair_indexes() -> Result<()> {
    let conn = setup_rug_lookahead_conn()?;
    let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {RUG_LOOKAHEAD_STATS_QUERY}"))?;
    let mut rows = stmt.query(params![
        TEST_TOKEN,
        SOL_MINT,
        "2026-04-29T00:00:00+00:00",
        "2026-04-29T00:05:00+00:00",
    ])?;
    let mut details = Vec::new();
    while let Some(row) = rows.next()? {
        details.push(row.get::<_, String>(3)?);
    }
    let plan = details.join("\n");
    assert!(
        plan.contains("idx_observed_swaps_token_in_out_ts"),
        "{plan}"
    );
    assert!(
        plan.contains("idx_observed_swaps_token_out_in_ts"),
        "{plan}"
    );
    Ok(())
}
