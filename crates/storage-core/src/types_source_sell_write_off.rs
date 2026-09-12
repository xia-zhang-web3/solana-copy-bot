use crate::{ExecutionCanaryOrder, ExecutionCanaryPositionCloseResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionSourceSellWriteOffKind {
    TerminalSimulation { max_attempts: u32 },
    TerminalNoRoute { max_attempts: u32 },
    DustNoRoute,
}
impl ExecutionSourceSellWriteOffKind {
    pub fn reason(self) -> &'static str {
        match self {
            Self::TerminalSimulation { .. } => "terminal_failed_sell_simulation_written_off",
            Self::TerminalNoRoute { .. } | Self::DustNoRoute => {
                "terminal_failed_sell_no_route_written_off"
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionSourceSellWriteOffOutcome {
    NotPromoted,
    Refused(&'static str),
    WrittenOff {
        position_id: String,
        close_result: ExecutionCanaryPositionCloseResult,
        order: ExecutionCanaryOrder,
    },
}
