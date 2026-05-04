fn wallet_scoring_carryover_lot_count_on_conn(conn: &Connection) -> Result<usize> {
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM wallet_scoring_carryover_lots",
            [],
            |row| row.get(0),
        )
        .context("failed counting wallet_scoring_carryover_lots")?;
    Ok(count.max(0) as usize)
}

fn wallet_scoring_open_lot_count_on_conn(conn: &Connection) -> Result<usize> {
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM wallet_scoring_open_lots", [], |row| {
            row.get(0)
        })
        .context("failed counting wallet_scoring_open_lots")?;
    Ok(count.max(0) as usize)
}

fn load_wallet_scoring_boundary_seed_lots_on_conn(
    conn: &Connection,
) -> Result<Vec<DiscoveryScoringBoundarySeedLot>> {
    let mut stmt = conn
        .prepare(
            "SELECT buy_signature, wallet_id, token, qty, cost_sol, opened_ts
             FROM wallet_scoring_open_lots
             ORDER BY wallet_id ASC, token ASC, opened_ts ASC, buy_signature ASC",
        )
        .context("failed preparing wallet_scoring boundary seed lot query")?;
    let mut rows = stmt
        .query([])
        .context("failed querying wallet_scoring boundary seed lots")?;
    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .context("failed iterating wallet_scoring boundary seed lots")?
    {
        let opened_ts_raw: String = row
            .get(5)
            .context("failed reading wallet_scoring boundary seed lot opened_ts")?;
        out.push(DiscoveryScoringBoundarySeedLot {
            buy_signature: row
                .get(0)
                .context("failed reading wallet_scoring boundary seed lot buy_signature")?,
            wallet_id: row
                .get(1)
                .context("failed reading wallet_scoring boundary seed lot wallet_id")?,
            token: row
                .get(2)
                .context("failed reading wallet_scoring boundary seed lot token")?,
            qty: row
                .get(3)
                .context("failed reading wallet_scoring boundary seed lot qty")?,
            cost_sol: row
                .get(4)
                .context("failed reading wallet_scoring boundary seed lot cost_sol")?,
            opened_ts: parse_ts(&opened_ts_raw, "wallet_scoring boundary seed lot opened_ts")?,
        });
    }
    Ok(out)
}

fn parse_day(raw: &str, field: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(raw, "%Y-%m-%d")
        .with_context(|| format!("invalid {field} date value: {raw}"))
}
