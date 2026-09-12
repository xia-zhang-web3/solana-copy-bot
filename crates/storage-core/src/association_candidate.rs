use crate::SqliteStore;
use anyhow::Result;
use copybot_core_types::association_delivery::{CandidateGeneration, CheckedFacts};
impl SqliteStore {
    /// App-local observation only. No source/receipt ownership, FIFO or ordering claim.
    /// Failed/unprovable reads remain Unknown; no write and no late re-selection.
    pub fn association_candidate(&self, f: &CheckedFacts) -> CandidateGeneration {
        let read = || -> Result<CandidateGeneration> {
            let token = if f.token_out == "So11111111111111111111111111111111111111112" {
                &f.token_in
            } else if f.token_in == "So11111111111111111111111111111111111111112" {
                &f.token_out
            } else {
                return Ok(CandidateGeneration::Unknown);
            };
            let mut q=self.conn.prepare("SELECT position_id,opened_ts FROM positions WHERE token=?1 AND state='open' AND accounting_bucket=?2 LIMIT 2")?;
            let rows = q
                .query_map(
                    rusqlite::params![token, crate::EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            match rows.as_slice() {
                [(id, opened)]
                    if !id.is_empty() && chrono::DateTime::parse_from_rfc3339(opened).is_ok() =>
                {
                    Ok(CandidateGeneration::AppObserved {
                        position_id: id.clone(),
                        opened_ts: opened.clone(),
                        token: token.clone(),
                    })
                }
                _ => Ok(CandidateGeneration::Unknown),
            }
        };
        read().unwrap_or(CandidateGeneration::Unknown)
    }
}
