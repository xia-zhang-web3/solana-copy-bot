impl SqliteStore {
    pub fn load_wallet_scoring_buy_facts_since(
        &self,
        window_start: DateTime<Utc>,
    ) -> Result<Vec<WalletScoringBuyFactRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT wallet_id, token, ts, notional_sol,
                        market_volume_5m_sol, market_unique_traders_5m, market_liquidity_proxy_sol,
                        quality_source, quality_token_age_seconds, quality_holders, quality_liquidity_sol,
                        rug_check_after_ts, rug_volume_lookahead_sol, rug_unique_traders_lookahead
                 FROM wallet_scoring_buy_facts
                 WHERE ts >= ?1
                 ORDER BY ts ASC, wallet_id ASC",
            )
            .context("failed to prepare wallet_scoring_buy_facts query")?;
        let mut rows = stmt
            .query(params![window_start.to_rfc3339()])
            .context("failed querying wallet_scoring_buy_facts")?;
        let mut out = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating wallet_scoring_buy_facts rows")?
        {
            let ts_raw: String = row
                .get(2)
                .context("failed reading wallet_scoring_buy_facts.ts")?;
            let source_raw: String = row
                .get(7)
                .context("failed reading wallet_scoring_buy_facts.quality_source")?;
            let rug_check_after_raw: String = row
                .get(11)
                .context("failed reading wallet_scoring_buy_facts.rug_check_after_ts")?;
            let market_unique_traders_raw: i64 = row
                .get(5)
                .context("failed reading wallet_scoring_buy_facts.market_unique_traders_5m")?;
            let rug_unique_traders_raw: Option<i64> = row
                .get(13)
                .context("failed reading wallet_scoring_buy_facts.rug_unique_traders_lookahead")?;
            out.push(WalletScoringBuyFactRow {
                wallet_id: row
                    .get(0)
                    .context("failed reading wallet_scoring_buy_facts.wallet_id")?,
                token: row
                    .get(1)
                    .context("failed reading wallet_scoring_buy_facts.token")?,
                ts: parse_ts(&ts_raw, "wallet_scoring_buy_facts.ts")?,
                notional_sol: row
                    .get(3)
                    .context("failed reading wallet_scoring_buy_facts.notional_sol")?,
                market_volume_5m_sol: row
                    .get(4)
                    .context("failed reading wallet_scoring_buy_facts.market_volume_5m_sol")?,
                market_unique_traders_5m: market_unique_traders_raw.max(0) as u32,
                market_liquidity_proxy_sol: row.get(6).context(
                    "failed reading wallet_scoring_buy_facts.market_liquidity_proxy_sol",
                )?,
                quality_source: match source_raw.as_str() {
                    "fresh" => WalletScoringQualitySource::Fresh,
                    "stale" => WalletScoringQualitySource::Stale,
                    "deferred" => WalletScoringQualitySource::Deferred,
                    "missing" => WalletScoringQualitySource::Missing,
                    other => {
                        return Err(anyhow!(
                            "invalid wallet_scoring_buy_facts.quality_source value: {other}"
                        ));
                    }
                },
                quality_token_age_seconds: row
                    .get::<_, Option<i64>>(8)
                    .context("failed reading wallet_scoring_buy_facts.quality_token_age_seconds")?
                    .map(|value| value.max(0) as u64),
                quality_holders: row
                    .get::<_, Option<i64>>(9)
                    .context("failed reading wallet_scoring_buy_facts.quality_holders")?
                    .map(|value| value.max(0) as u64),
                quality_liquidity_sol: row
                    .get(10)
                    .context("failed reading wallet_scoring_buy_facts.quality_liquidity_sol")?,
                rug_check_after_ts: parse_ts(
                    &rug_check_after_raw,
                    "wallet_scoring_buy_facts.rug_check_after_ts",
                )?,
                rug_volume_lookahead_sol: row
                    .get(12)
                    .context("failed reading wallet_scoring_buy_facts.rug_volume_lookahead_sol")?,
                rug_unique_traders_lookahead: rug_unique_traders_raw
                    .map(|value| value.max(0) as u32),
            });
        }
        Ok(out)
    }
}
