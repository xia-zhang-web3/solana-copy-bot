//! Test-only native BUY external observations and mock controls.
use crate::execution_canary::ExecutionCanaryRunner;
use crate::execution_canary_route::NativeBuyGuard;

#[derive(Debug, Default)]
pub(crate) struct NativeBuyMockCounts {
    pub source_finality: usize,
    pub priority: usize,
    pub initial_quote: usize,
    pub fresh_quote: usize,
    pub unsigned_build: usize,
    pub signing_envelope: usize,
    pub quote: usize,
    pub initial_sol: usize,
    pub fee: usize,
    pub send: usize,
    pub confirmation: usize,
    pub receipt: usize,
}

/// Only external observations are mocked. The same guards, dispatch claim,
/// receipt parser and canonical settlement continue to run in the test.
#[derive(Debug)]
pub(crate) struct NativeBuyMockIo {
    pub runner: Option<NativeBuyRunnerOperands>,
    pub initial_sol: crate::execution_native_rpc::rent_types::ClassicAtaFundingFacts,
    pub fee_lamports: u64,
    pub fee_slot: u64,
    pub expected_message_sha256: String,
    pub submit_signature: Option<String>,
    pub confirmation: serde_json::Value,
    pub receipt: serde_json::Value,
    pub counts: std::sync::Arc<std::sync::Mutex<NativeBuyMockCounts>>,
}

#[derive(Debug)]
pub(crate) struct NativeBuyRunnerOperands {
    pub finalized_genesis: String,
    pub finalized_transaction: serde_json::Value,
    pub mint_account: serde_json::Value,
    pub initial_quote: crate::execution_quote_canary_helpers::QuoteSample,
    pub fresh_quote: crate::execution_quote_canary_helpers::QuoteSample,
    pub priority: crate::execution_quote_canary_helpers::PriorityFeeSample,
    pub adapter: NativeBuyMockAdapter,
}

#[path = "native_buy_mock_adapter.rs"]
mod native_buy_mock_adapter;
pub(crate) use native_buy_mock_adapter::NativeBuyMockAdapter;

impl NativeBuyMockIo {
    pub(crate) fn count(&self, add: impl FnOnce(&mut NativeBuyMockCounts)) {
        if let Ok(mut counts) = self.counts.lock() {
            add(&mut counts);
        }
    }
}

impl NativeBuyGuard {
    pub(crate) fn with_mock_io(mut self, io: std::sync::Arc<NativeBuyMockIo>) -> Self {
        self.mock_io = Some(io);
        self
    }

    pub(crate) fn mock_io(&self) -> Option<&NativeBuyMockIo> {
        self.mock_io.as_deref()
    }
}

impl ExecutionCanaryRunner {
    pub(crate) fn with_native_buy_mock(
        mut self,
        mock: std::sync::Arc<crate::execution_canary_route::NativeBuyMockIo>,
    ) -> Self {
        self.native_buy_mock = Some(mock);
        self
    }
}
