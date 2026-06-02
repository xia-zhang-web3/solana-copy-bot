use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::SqliteStore;
use std::path::Path;

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ExecutionCanarySafetySnapshot {
    pub(crate) blocked_reason: Option<&'static str>,
    pub(crate) open_positions: u64,
    pub(crate) daily_loss_sol: f64,
}

pub(crate) fn pre_submit_safety_snapshot(
    config: &ExecutionConfig,
    store: &SqliteStore,
    now: DateTime<Utc>,
) -> Result<ExecutionCanarySafetySnapshot> {
    if Path::new(&config.canary_kill_switch_path).exists() {
        return Ok(ExecutionCanarySafetySnapshot {
            blocked_reason: Some("kill_switch_active"),
            ..ExecutionCanarySafetySnapshot::default()
        });
    }

    let open_positions = store.execution_canary_open_position_count()?;
    if open_positions >= u64::from(config.canary_max_open_positions) {
        return Ok(ExecutionCanarySafetySnapshot {
            blocked_reason: Some("max_open_positions"),
            open_positions,
            ..ExecutionCanarySafetySnapshot::default()
        });
    }

    if config.canary_max_daily_loss_sol <= 0.0 {
        return Ok(ExecutionCanarySafetySnapshot {
            blocked_reason: Some("daily_loss_cap_zero"),
            open_positions,
            ..ExecutionCanarySafetySnapshot::default()
        });
    }

    let daily_loss_sol = store.execution_canary_realized_loss_sol_since(day_start_utc(now))?;
    if daily_loss_sol >= config.canary_max_daily_loss_sol {
        return Ok(ExecutionCanarySafetySnapshot {
            blocked_reason: Some("max_daily_loss"),
            open_positions,
            daily_loss_sol,
        });
    }

    Ok(ExecutionCanarySafetySnapshot {
        blocked_reason: None,
        open_positions,
        daily_loss_sol,
    })
}

fn day_start_utc(now: DateTime<Utc>) -> DateTime<Utc> {
    let midnight = now
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .expect("midnight must be a valid time");
    DateTime::<Utc>::from_naive_utc_and_offset(midnight, Utc)
}
