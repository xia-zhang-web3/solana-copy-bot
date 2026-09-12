use super::*;
use anyhow::ensure;
use std::collections::HashSet;

pub(super) const RAYDIUM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
pub(super) const PUMPSWAP: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";

// Explicit offline decoder mapping. No default IngestionConfig, fixture config,
// network lookup, or silently added interest. Other programs require a new mapping.
pub(super) struct DecoderPolicy {
    pub interested_program_ids: HashSet<String>,
    pub raydium_program_ids: HashSet<String>,
    pub pumpswap_program_ids: HashSet<String>,
}
impl DecoderPolicy {
    pub fn from_recorded(programs: &[String]) -> Result<Self> {
        ensure!(
            !programs.is_empty() && programs.len() <= 32,
            "invalid recorded program bounds"
        );
        ensure!(
            programs
                .iter()
                .all(|p| matches!(p.as_str(), RAYDIUM_V4 | PUMPSWAP)),
            "unsupported decoder policy program"
        );
        let interested_program_ids: HashSet<_> = programs.iter().cloned().collect();
        let subset = |p: &str| {
            programs
                .iter()
                .filter(|v| v.as_str() == p)
                .cloned()
                .collect()
        };
        Ok(Self {
            interested_program_ids,
            raydium_program_ids: subset(RAYDIUM_V4),
            pumpswap_program_ids: subset(PUMPSWAP),
        })
    }
    pub fn description(&self) -> Value {
        let sorted = |set: &HashSet<String>| {
            let mut v: Vec<_> = set.iter().cloned().collect();
            v.sort();
            v
        };
        json!({"selection":"recorded-program-mapping-v1",
            "interested_program_ids":sorted(&self.interested_program_ids),
            "raydium_program_ids":sorted(&self.raydium_program_ids),
            "pumpswap_program_ids":sorted(&self.pumpswap_program_ids),
            "canonical_fork":"unproven"})
    }
}
