use crate::source::RawSwapObservation;
use copybot_core_types::SwapEvent;

pub struct SwapParser {
    raydium_program_ids: Vec<String>,
    pumpswap_program_ids: Vec<String>,
}

impl SwapParser {
    pub fn new(raydium_program_ids: Vec<String>, pumpswap_program_ids: Vec<String>) -> Self {
        Self {
            raydium_program_ids,
            pumpswap_program_ids,
        }
    }

    pub fn parse(&self, raw: RawSwapObservation) -> Option<SwapEvent> {
        if raw.signer.is_empty()
            || raw.signature.is_empty()
            || raw.token_in.is_empty()
            || raw.token_out.is_empty()
        {
            return None;
        }
        if raw.amount_in <= 0.0 || raw.amount_out <= 0.0 {
            return None;
        }

        let dex = self.detect_dex(&raw)?;
        Some(SwapEvent {
            wallet: raw.signer,
            dex,
            token_in: raw.token_in,
            token_out: raw.token_out,
            amount_in: raw.amount_in,
            amount_out: raw.amount_out,
            signature: raw.signature,
            slot: raw.slot,
            ts_utc: raw.ts_utc,
        })
    }

    fn detect_dex(&self, raw: &RawSwapObservation) -> Option<String> {
        for program in &raw.program_ids {
            if self.raydium_program_ids.iter().any(|id| id == program) {
                return Some("raydium".to_string());
            }
            if self.pumpswap_program_ids.iter().any(|id| id == program) {
                return Some("pumpswap".to_string());
            }
        }

        let hint = raw.dex_hint.to_lowercase();
        if hint.contains("raydium") {
            return Some("raydium".to_string());
        }
        if hint.contains("pump") {
            return Some("pumpswap".to_string());
        }
        None
    }
}
