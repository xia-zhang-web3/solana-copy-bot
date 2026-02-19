use anyhow::{anyhow, Result};

use crate::intent::ExecutionIntent;

#[derive(Debug, Clone)]
pub struct SimulationResult {
    pub accepted: bool,
    pub detail: String,
}

pub trait IntentSimulator {
    fn simulate(&self, intent: &ExecutionIntent) -> Result<SimulationResult>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PaperIntentSimulator;

impl IntentSimulator for PaperIntentSimulator {
    fn simulate(&self, intent: &ExecutionIntent) -> Result<SimulationResult> {
        if !intent.notional_sol.is_finite() || intent.notional_sol <= 0.0 {
            return Err(anyhow!("invalid notional for signal {}", intent.signal_id));
        }

        Ok(SimulationResult {
            accepted: true,
            detail: "paper_simulation_ok".to_string(),
        })
    }
}
