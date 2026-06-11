use crate::execution_route_plan::route_plan_has_pump_fun_amm;
use anyhow::Result;
use copybot_storage_core::SqliteStore;

const EXECUTION_CANARY_POSITION_PREFIX: &str = "exec-canary-pos:";
const EXECUTION_CANARY_RECOVERY_ORPHAN_PREFIX: &str = "exec-canary-pos:recovery-orphan:";

pub(crate) fn load_entry_route_plan_json_for_sell(
    store: &SqliteStore,
    token: &str,
    side: &str,
) -> Result<Option<String>> {
    if !side.eq_ignore_ascii_case("sell") {
        return Ok(None);
    }
    let Some(position) = store.load_execution_canary_open_position(token)? else {
        return Ok(None);
    };
    let Some(entry_order_id) = entry_order_id_from_position_id(&position.position_id) else {
        return Ok(None);
    };
    let Some(metadata) = store.load_execution_canary_build_plan_metadata(entry_order_id)? else {
        return Ok(None);
    };
    Ok(metadata
        .route_plan_json
        .filter(|raw| route_plan_has_pump_fun_amm(Some(raw.as_str()))))
}

fn entry_order_id_from_position_id(position_id: &str) -> Option<&str> {
    if position_id.starts_with(EXECUTION_CANARY_RECOVERY_ORPHAN_PREFIX) {
        return None;
    }
    position_id
        .strip_prefix(EXECUTION_CANARY_POSITION_PREFIX)
        .filter(|order_id| !order_id.trim().is_empty())
}
