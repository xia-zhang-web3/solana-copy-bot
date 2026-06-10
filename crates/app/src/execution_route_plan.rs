use serde_json::Value;

pub(crate) fn route_plan_has_pump_fun_amm(route_plan_json: Option<&str>) -> bool {
    route_plan_has_label(route_plan_json, "Pump.fun Amm")
}

fn route_plan_has_label(route_plan_json: Option<&str>, expected: &str) -> bool {
    let Some(raw) = route_plan_json.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return false;
    };
    let Ok(value) = serde_json::from_str::<Value>(raw) else {
        return false;
    };
    value.as_array().is_some_and(|items| {
        items.iter().any(|item| {
            item.pointer("/swapInfo/label")
                .and_then(Value::as_str)
                .is_some_and(|label| label.eq_ignore_ascii_case(expected))
        })
    })
}
