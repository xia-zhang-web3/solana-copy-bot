fn load_all_open_lots(conn: &Connection) -> Result<HashMap<WalletTokenKey, VecDeque<BuilderLot>>> {
    let mut stmt = conn
        .prepare(
            "SELECT buy_signature, wallet_id, token, qty, cost_sol, opened_ts
             FROM wallet_scoring_open_lots
             ORDER BY wallet_id ASC, token ASC, opened_ts ASC, buy_signature ASC",
        )
        .context("failed preparing wallet_scoring_open_lots builder bootstrap query")?;
    let mut rows = stmt
        .query([])
        .context("failed querying wallet_scoring_open_lots for builder bootstrap")?;
    let mut out: HashMap<WalletTokenKey, VecDeque<BuilderLot>> = HashMap::new();
    while let Some(row) = rows
        .next()
        .context("failed iterating wallet_scoring_open_lots for builder bootstrap")?
    {
        let wallet_id: String = row.get(1).context("failed reading builder lot wallet_id")?;
        let token: String = row.get(2).context("failed reading builder lot token")?;
        out.entry(WalletTokenKey {
            wallet_id: wallet_id.clone(),
            token: token.clone(),
        })
        .or_default()
        .push_back(BuilderLot {
            buy_signature: row
                .get(0)
                .context("failed reading builder lot buy_signature")?,
            wallet_id,
            token,
            qty: row.get(3).context("failed reading builder lot qty")?,
            cost_sol: row.get(4).context("failed reading builder lot cost_sol")?,
            opened_ts: parse_ts(
                &row.get::<_, String>(5)
                    .context("failed reading builder lot opened_ts")?,
                "wallet_scoring_open_lots.opened_ts",
            )?,
        });
    }
    Ok(out)
}

fn load_open_lots_for_wallet_token(
    conn: &Connection,
    wallet_id: &str,
    token: &str,
) -> Result<VecDeque<BuilderLot>> {
    let mut stmt = conn
        .prepare(
            "SELECT buy_signature, qty, cost_sol, opened_ts
             FROM wallet_scoring_open_lots
             WHERE wallet_id = ?1
               AND token = ?2
             ORDER BY opened_ts ASC, buy_signature ASC",
        )
        .context("failed preparing wallet_scoring_open_lots lazy builder query")?;
    let mut rows = stmt
        .query(params![wallet_id, token])
        .context("failed querying wallet_scoring_open_lots for lazy builder bootstrap")?;
    let mut out = VecDeque::new();
    while let Some(row) = rows
        .next()
        .context("failed iterating wallet_scoring_open_lots for lazy builder bootstrap")?
    {
        out.push_back(BuilderLot {
            buy_signature: row
                .get(0)
                .context("failed reading lazy builder lot buy_signature")?,
            wallet_id: wallet_id.to_string(),
            token: token.to_string(),
            qty: row.get(1).context("failed reading lazy builder lot qty")?,
            cost_sol: row
                .get(2)
                .context("failed reading lazy builder lot cost_sol")?,
            opened_ts: parse_ts(
                &row.get::<_, String>(3)
                    .context("failed reading lazy builder lot opened_ts")?,
                "wallet_scoring_open_lots.opened_ts",
            )?,
        });
    }
    Ok(out)
}

fn load_boundary_seed_lots_into_open_lots(
    seed_lots: &[DiscoveryScoringBoundarySeedLot],
) -> HashMap<WalletTokenKey, VecDeque<BuilderLot>> {
    let mut ordered = seed_lots.to_vec();
    ordered.sort_by(|left, right| {
        left.wallet_id
            .cmp(&right.wallet_id)
            .then_with(|| left.token.cmp(&right.token))
            .then_with(|| left.opened_ts.cmp(&right.opened_ts))
            .then_with(|| left.buy_signature.cmp(&right.buy_signature))
    });

    let mut out: HashMap<WalletTokenKey, VecDeque<BuilderLot>> = HashMap::new();
    for lot in ordered {
        out.entry(WalletTokenKey {
            wallet_id: lot.wallet_id.clone(),
            token: lot.token.clone(),
        })
        .or_default()
        .push_back(BuilderLot {
            buy_signature: lot.buy_signature,
            wallet_id: lot.wallet_id,
            token: lot.token,
            qty: lot.qty,
            cost_sol: lot.cost_sol,
            opened_ts: lot.opened_ts,
        });
    }
    out
}

fn load_persisted_boundary_lot_builder_state_on_conn(
    conn: &Connection,
) -> Result<Option<PersistedBoundaryLotBuilderState>> {
    let raw: Option<String> = conn
        .query_row(
            "SELECT state_value
             FROM discovery_scoring_state
             WHERE state_key = ?1",
            params![BOUNDARY_LOT_BUILDER_STATE_KEY],
            |row| row.get(0),
        )
        .optional()
        .context("failed querying persisted boundary lot builder state")?;
    raw.map(|raw| {
        serde_json::from_str::<PersistedBoundaryLotBuilderState>(&raw)
            .context("failed decoding persisted boundary lot builder state json")
    })
    .transpose()
}

fn upsert_persisted_boundary_lot_builder_state_on_conn(
    conn: &Connection,
    progress_start_ts: DateTime<Utc>,
    progress_cursor: &DiscoveryRuntimeCursor,
    builder: &DiscoveryScoringBoundaryLotBuilder,
    updated_at: &str,
) -> Result<()> {
    let state = PersistedBoundaryLotBuilderState {
        progress_start_ts,
        cursor: progress_cursor.clone(),
        open_lots: export_boundary_seed_lots_from_open_lots(&builder.open_lots),
    };
    let state_json = serde_json::to_string(&state)
        .context("failed encoding persisted boundary lot builder state json")?;
    conn.execute(
        "INSERT INTO discovery_scoring_state(state_key, state_value, updated_at)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(state_key) DO UPDATE SET
            state_value = excluded.state_value,
            updated_at = excluded.updated_at",
        params![BOUNDARY_LOT_BUILDER_STATE_KEY, state_json, updated_at],
    )
    .context("failed upserting persisted boundary lot builder state")?;
    Ok(())
}
