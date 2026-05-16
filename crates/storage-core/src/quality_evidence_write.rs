use crate::{DiscoveryV2QualityPrepareUpsert, SqliteDiscoveryStore};
use anyhow::{Context, Result};
use rusqlite::params;

impl SqliteDiscoveryStore {
    pub fn insert_discovery_v2_quality_observed_evidence_batch(
        &self,
        evidence: &[DiscoveryV2QualityPrepareUpsert],
    ) -> Result<usize> {
        if evidence.is_empty() {
            return Ok(0);
        }
        self.begin_discovery_v2_quality_prepare_update()?;
        let mut transaction_open = true;
        let result = (|| {
            let mut inserted = 0usize;
            {
                let mut stmt = self
                    .conn
                    .prepare_cached(
                        "INSERT OR IGNORE INTO discovery_v2_quality_observed_evidence(
                            signature, mint, wallet_id, ts, slot, sol_notional, is_buy
                         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    )
                    .context("failed preparing discovery v2 quality evidence batch insert")?;
                for row in evidence {
                    let changed = stmt
                        .execute(params![
                            row.signature.as_str(),
                            row.mint.as_str(),
                            row.wallet_id.as_str(),
                            row.ts_utc.to_rfc3339(),
                            row.slot as i64,
                            row.sol_notional,
                            if row.is_buy { 1_i64 } else { 0_i64 },
                        ])
                        .context("failed inserting discovery v2 quality evidence batch row")?;
                    if changed > 0 {
                        inserted = inserted.saturating_add(1);
                    }
                }
            }
            self.commit_discovery_v2_quality_prepare_update()?;
            transaction_open = false;
            Ok(inserted)
        })();
        if transaction_open {
            self.rollback_discovery_v2_quality_prepare_update();
        }
        result
    }
}
