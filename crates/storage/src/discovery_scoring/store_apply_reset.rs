use super::*;

impl SqliteStore {
    pub fn export_discovery_scoring_boundary_seed_snapshot(
        &self,
        boundary_start_ts: DateTime<Utc>,
        boundary_cursor: &DiscoveryRuntimeCursor,
    ) -> Result<DiscoveryScoringBoundarySeedSnapshot> {
        let carryover_lot_count = wallet_scoring_carryover_lot_count_on_conn(&self.conn)?;
        if carryover_lot_count != 0 {
            anyhow::bail!(
                "exact seeded boundary export does not support wallet_scoring_carryover_lots; carryover_lot_count={carryover_lot_count}"
            );
        }
        Ok(DiscoveryScoringBoundarySeedSnapshot {
            boundary_start_ts,
            boundary_cursor: boundary_cursor.clone(),
            open_lots: load_wallet_scoring_boundary_seed_lots_on_conn(&self.conn)?,
        })
    }

    pub fn count_discovery_scoring_boundary_seed_open_lots(&self) -> Result<usize> {
        let carryover_lot_count = wallet_scoring_carryover_lot_count_on_conn(&self.conn)?;
        if carryover_lot_count != 0 {
            anyhow::bail!(
                "exact seeded boundary export does not support wallet_scoring_carryover_lots; carryover_lot_count={carryover_lot_count}"
            );
        }
        wallet_scoring_open_lot_count_on_conn(&self.conn)
    }

    pub fn reset_discovery_scoring_tables_and_install_boundary_seed_snapshot(
        &self,
        snapshot: &DiscoveryScoringBoundarySeedSnapshot,
        preserved_gap_cursor: Option<&DiscoveryRuntimeCursor>,
    ) -> Result<()> {
        self.with_immediate_transaction_retry("discovery scoring seeded reset install", |conn| {
            let carryover_lot_count = wallet_scoring_carryover_lot_count_on_conn(conn)?;
            if carryover_lot_count != 0 {
                anyhow::bail!(
                    "exact seeded boundary install does not support wallet_scoring_carryover_lots; carryover_lot_count={carryover_lot_count}"
                );
            }

            conn.execute("DELETE FROM wallet_scoring_buy_facts", [])
                .context("failed clearing wallet_scoring_buy_facts")?;
            conn.execute("DELETE FROM wallet_scoring_close_facts", [])
                .context("failed clearing wallet_scoring_close_facts")?;
            conn.execute("DELETE FROM wallet_scoring_open_lots", [])
                .context("failed clearing wallet_scoring_open_lots")?;
            conn.execute("DELETE FROM wallet_scoring_carryover_lots", [])
                .context("failed clearing wallet_scoring_carryover_lots")?;
            conn.execute("DELETE FROM wallet_scoring_tx_minutes", [])
                .context("failed clearing wallet_scoring_tx_minutes")?;
            conn.execute("DELETE FROM wallet_scoring_days", [])
                .context("failed clearing wallet_scoring_days")?;
            conn.execute("DELETE FROM discovery_scoring_state", [])
                .context("failed clearing discovery_scoring_state")?;

            for lot in &snapshot.open_lots {
                conn.execute(
                    "INSERT OR IGNORE INTO wallet_scoring_open_lots(
                        buy_signature,
                        wallet_id,
                        token,
                        qty,
                        cost_sol,
                        opened_ts
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        &lot.buy_signature,
                        &lot.wallet_id,
                        &lot.token,
                        lot.qty,
                        lot.cost_sol,
                        lot.opened_ts.to_rfc3339(),
                    ],
                )
                .context("failed inserting discovery scoring boundary seed open_lot")?;
            }

            let updated_at = Utc::now().to_rfc3339();
            upsert_discovery_scoring_backfill_progress_on_conn(
                conn,
                snapshot.boundary_start_ts,
                &snapshot.boundary_cursor,
                &updated_at,
            )?;
            upsert_discovery_scoring_seed_boundary_install_marker_on_conn(
                conn,
                snapshot.boundary_start_ts,
                &snapshot.boundary_cursor,
                &updated_at,
            )?;
            if let Some(gap_cursor) = preserved_gap_cursor {
                upsert_discovery_scoring_materialization_gap_cursor_on_conn(
                    conn,
                    gap_cursor,
                    &updated_at,
                )?;
            }
            Ok(snapshot.open_lots.len())
        })?;
        Ok(())
    }

    pub fn reset_discovery_scoring_tables_and_commit_existing_boundary_open_lots(
        &self,
        boundary_start_ts: DateTime<Utc>,
        boundary_cursor: &DiscoveryRuntimeCursor,
        preserved_gap_cursor: Option<&DiscoveryRuntimeCursor>,
    ) -> Result<usize> {
        self.with_immediate_transaction_retry("discovery scoring seeded reset install", |conn| {
            let carryover_lot_count = wallet_scoring_carryover_lot_count_on_conn(conn)?;
            if carryover_lot_count != 0 {
                anyhow::bail!(
                    "exact seeded boundary install does not support wallet_scoring_carryover_lots; carryover_lot_count={carryover_lot_count}"
                );
            }
            let open_lot_count = wallet_scoring_open_lot_count_on_conn(conn)?;

            conn.execute("DELETE FROM wallet_scoring_buy_facts", [])
                .context("failed clearing wallet_scoring_buy_facts")?;
            conn.execute("DELETE FROM wallet_scoring_close_facts", [])
                .context("failed clearing wallet_scoring_close_facts")?;
            conn.execute("DELETE FROM wallet_scoring_carryover_lots", [])
                .context("failed clearing wallet_scoring_carryover_lots")?;
            conn.execute("DELETE FROM wallet_scoring_tx_minutes", [])
                .context("failed clearing wallet_scoring_tx_minutes")?;
            conn.execute("DELETE FROM wallet_scoring_days", [])
                .context("failed clearing wallet_scoring_days")?;
            conn.execute("DELETE FROM discovery_scoring_state", [])
                .context("failed clearing discovery_scoring_state")?;

            let updated_at = Utc::now().to_rfc3339();
            upsert_discovery_scoring_backfill_progress_on_conn(
                conn,
                boundary_start_ts,
                boundary_cursor,
                &updated_at,
            )?;
            upsert_discovery_scoring_seed_boundary_install_marker_on_conn(
                conn,
                boundary_start_ts,
                boundary_cursor,
                &updated_at,
            )?;
            if let Some(gap_cursor) = preserved_gap_cursor {
                upsert_discovery_scoring_materialization_gap_cursor_on_conn(
                    conn,
                    gap_cursor,
                    &updated_at,
                )?;
            }
            Ok(open_lot_count)
        })
    }

    pub fn reset_discovery_scoring_tables(&self) -> Result<()> {
        self.with_immediate_transaction_retry("discovery scoring reset", |conn| {
            conn.execute("DELETE FROM wallet_scoring_buy_facts", [])
                .context("failed clearing wallet_scoring_buy_facts")?;
            conn.execute("DELETE FROM wallet_scoring_close_facts", [])
                .context("failed clearing wallet_scoring_close_facts")?;
            conn.execute("DELETE FROM wallet_scoring_open_lots", [])
                .context("failed clearing wallet_scoring_open_lots")?;
            conn.execute("DELETE FROM wallet_scoring_carryover_lots", [])
                .context("failed clearing wallet_scoring_carryover_lots")?;
            conn.execute("DELETE FROM wallet_scoring_tx_minutes", [])
                .context("failed clearing wallet_scoring_tx_minutes")?;
            conn.execute("DELETE FROM wallet_scoring_days", [])
                .context("failed clearing wallet_scoring_days")?;
            conn.execute("DELETE FROM discovery_scoring_state", [])
                .context("failed clearing discovery_scoring_state")?;
            Ok(0usize)
        })?;
        Ok(())
    }
}
