use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::TokenQuantity;
use copybot_storage_core::{
    ExecutionCanaryPositionRecordOutcome, SqliteStore, EXECUTION_CANARY_POSITION_CLOSE_CLOSED,
    EXECUTION_CANARY_POSITION_CLOSE_DUST_CLOSED, EXECUTION_CANARY_POSITION_CLOSE_NO_POSITION,
    EXECUTION_CANARY_POSITION_CLOSE_PARTIAL,
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ExecutionConfirmedFill {
    Buy(ExecutionConfirmedBuyFill),
    Sell(ExecutionConfirmedSellFill),
}

impl ExecutionConfirmedFill {
    pub(crate) fn order_id(&self) -> &str {
        match self {
            Self::Buy(fill) => &fill.order_id,
            Self::Sell(fill) => &fill.order_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExecutionConfirmedBuyFill {
    pub(crate) order_id: String,
    pub(crate) token: String,
    pub(crate) qty: f64,
    pub(crate) qty_exact: Option<TokenQuantity>,
    pub(crate) cost_sol: f64,
    pub(crate) fill_ts: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExecutionConfirmedSellFill {
    pub(crate) order_id: String,
    pub(crate) token: String,
    pub(crate) target_qty: f64,
    pub(crate) target_qty_exact: Option<TokenQuantity>,
    pub(crate) exit_price_sol: f64,
    pub(crate) dust_qty_epsilon: f64,
    pub(crate) fill_ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ExecutionConfirmedFillAccountingOutcome {
    pub(crate) buy_opened: usize,
    pub(crate) buy_existing: usize,
    pub(crate) sell_closed: usize,
    pub(crate) sell_partial: usize,
    pub(crate) sell_dust_closed: usize,
    pub(crate) sell_no_position: usize,
    pub(crate) position_id: Option<String>,
    pub(crate) close_status: Option<String>,
    pub(crate) closed_qty: f64,
    pub(crate) pnl_sol: f64,
}

pub(crate) fn record_confirmed_fill_accounting(
    store: &SqliteStore,
    fill: ExecutionConfirmedFill,
) -> Result<ExecutionConfirmedFillAccountingOutcome> {
    match fill {
        ExecutionConfirmedFill::Buy(fill) => record_confirmed_buy_fill_accounting(store, fill),
        ExecutionConfirmedFill::Sell(fill) => record_confirmed_sell_fill_accounting(store, fill),
    }
}

pub(crate) fn record_confirmed_fill_accounting_and_status(
    store: &SqliteStore,
    fill: ExecutionConfirmedFill,
    confirmed_at: DateTime<Utc>,
) -> Result<(
    copybot_storage_core::ExecutionCanaryOrder,
    ExecutionConfirmedFillAccountingOutcome,
)> {
    match fill {
        ExecutionConfirmedFill::Buy(fill) => {
            let (order, result) = store.confirm_execution_canary_buy_fill(
                &fill.order_id,
                &fill.token,
                fill.qty,
                fill.qty_exact,
                fill.cost_sol,
                fill.fill_ts,
                confirmed_at,
            )?;
            Ok((
                order,
                ExecutionConfirmedFillAccountingOutcome {
                    buy_opened: usize::from(
                        result.outcome == ExecutionCanaryPositionRecordOutcome::Inserted,
                    ),
                    buy_existing: usize::from(
                        result.outcome != ExecutionCanaryPositionRecordOutcome::Inserted,
                    ),
                    position_id: Some(result.position.position_id),
                    ..ExecutionConfirmedFillAccountingOutcome::default()
                },
            ))
        }
        ExecutionConfirmedFill::Sell(fill) => {
            let (order, result) = store.confirm_execution_canary_sell_fill(
                &fill.order_id,
                &fill.token,
                fill.target_qty,
                fill.target_qty_exact,
                fill.exit_price_sol,
                fill.dust_qty_epsilon,
                fill.fill_ts,
                confirmed_at,
            )?;
            Ok((order, sell_fill_accounting_outcome(result)))
        }
    }
}

fn record_confirmed_buy_fill_accounting(
    store: &SqliteStore,
    fill: ExecutionConfirmedBuyFill,
) -> Result<ExecutionConfirmedFillAccountingOutcome> {
    let result = store.record_execution_canary_confirmed_buy_fill(
        &fill.order_id,
        &fill.token,
        fill.qty,
        fill.qty_exact,
        fill.cost_sol,
        fill.fill_ts,
    )?;
    Ok(ExecutionConfirmedFillAccountingOutcome {
        buy_opened: usize::from(result.outcome == ExecutionCanaryPositionRecordOutcome::Inserted),
        buy_existing: usize::from(result.outcome != ExecutionCanaryPositionRecordOutcome::Inserted),
        position_id: Some(result.position.position_id),
        ..ExecutionConfirmedFillAccountingOutcome::default()
    })
}

fn record_confirmed_sell_fill_accounting(
    store: &SqliteStore,
    fill: ExecutionConfirmedSellFill,
) -> Result<ExecutionConfirmedFillAccountingOutcome> {
    let result = store.close_execution_canary_confirmed_sell_fill(
        &fill.order_id,
        &fill.token,
        fill.target_qty,
        fill.target_qty_exact,
        fill.exit_price_sol,
        fill.dust_qty_epsilon,
        fill.fill_ts,
    )?;
    Ok(sell_fill_accounting_outcome(result))
}

fn sell_fill_accounting_outcome(
    result: copybot_storage_core::ExecutionCanaryPositionCloseResult,
) -> ExecutionConfirmedFillAccountingOutcome {
    let mut outcome = ExecutionConfirmedFillAccountingOutcome {
        position_id: result.position_id,
        close_status: Some(result.close_status.clone()),
        closed_qty: result.closed_qty,
        pnl_sol: result.pnl_sol,
        ..ExecutionConfirmedFillAccountingOutcome::default()
    };
    match result.close_status.as_str() {
        EXECUTION_CANARY_POSITION_CLOSE_NO_POSITION => outcome.sell_no_position = 1,
        EXECUTION_CANARY_POSITION_CLOSE_PARTIAL => {
            outcome.sell_closed = 1;
            outcome.sell_partial = 1;
        }
        EXECUTION_CANARY_POSITION_CLOSE_CLOSED => outcome.sell_closed = 1,
        EXECUTION_CANARY_POSITION_CLOSE_DUST_CLOSED => {
            outcome.sell_closed = 1;
            outcome.sell_dust_closed = 1;
        }
        _ => {}
    }
    outcome
}
