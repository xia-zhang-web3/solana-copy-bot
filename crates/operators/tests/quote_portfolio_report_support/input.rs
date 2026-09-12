use serde_json::{json, Value};

pub const TS: &str = "2026-06-02T12:00:00+00:00";
pub const MS: u64 = 1_780_401_600_000;
pub const SOL: &str = "So11111111111111111111111111111111111111112";
pub fn mint(n: u8) -> String {
    bs58::encode([n; 32]).into_string()
}
pub fn origin() -> Value {
    json!({"synthetic":"explicit fixture, not live or charged fees"})
}
pub fn amount(n: u64) -> Value {
    json!({"value":{"known":n.to_string()},"provenance":origin()})
}
pub fn costs(id: &str, pos: &str, side: &str) -> Value {
    json!({"event_id":id,"position_id":pos,"side":side,
        "base":amount(0),"priority":amount(0),"setup":amount(0),"exit":amount(0)})
}
pub fn reference(id: &str, pos: &str, n: u8, side: &str, input: u64, output: u64) -> Value {
    json!({"event_id":id,"wallet_id":"fixture-wallet","signal_id":null,
        "shadow_closed_trade_id":null,"request_ts":TS,"position_id":pos,
        "mint":mint(n),"decimals":0,"side":side,"input_raw":input.to_string(),
        "output_raw":output.to_string(),"provenance":origin()})
}
pub fn event(id: &str, pos: &str, seq: u64, action: Value) -> Value {
    json!({"id":id,"position_id":pos,"sequence":seq.to_string(),"unix_ms":(MS+seq).to_string(),
        "identity_provenance":origin(),"action":action})
}
pub fn buy(id: &str, pos: &str, seq: u64, n: u8, input: u64, output: u64) -> Value {
    event(
        id,
        pos,
        seq,
        json!({"kind":"buy","mint":mint(n),"decimals":0,
        "input_lamports":input.to_string(),"quote":reference(id,pos,n,"buy",input,output),
        "costs":costs(id,pos,"buy"),"rent_deposit":amount(0)}),
    )
}
pub fn sell(id: &str, pos: &str, seq: u64, raw: u64, output: u64) -> Value {
    event(
        id,
        pos,
        seq,
        json!({"kind":"sell","raw":raw.to_string(),
        "quote":reference(id,pos,1,"sell",raw,output),"costs":costs(id,pos,"sell")}),
    )
}
pub fn scenario(cash: u64, cap: u64, events: Vec<Value>) -> Value {
    json!({"version":1,"scenario_provenance":origin(),
        "initial":{"cash_lamports":amount(cash),"max_open_positions":amount(cap),
            "inventory_raw":amount(0),"external_transfers_lamports":amount(0)},
        "window":{"start_unix_ms":MS.to_string(),"end_unix_ms":(MS+1000).to_string(),
            "input_complete":true,"input_provenance":origin(),
            "source_coverage_assertion":"synthetic listed events only; no full source-window evidence"},
        "events":events})
}
pub fn oracle(cash: u64, cap: u64) -> Value {
    let mut a = buy("buy-a", "A", 1, 1, 600_000_000, 100);
    let mut b = buy("buy-b", "B", 2, 2, 600_000_000, 100);
    for e in [&mut a, &mut b] {
        e["action"]["costs"]["base"] = amount(5_000);
        e["action"]["costs"]["priority"] = amount(7_003);
        e["action"]["costs"]["setup"] = amount(10_000);
        e["action"]["rent_deposit"] = amount(2_039_280);
    }
    let mut s = sell("sell-a40", "A", 3, 40, 312_500_000);
    s["action"]["costs"]["base"] = amount(5_000);
    s["action"]["costs"]["priority"] = amount(3_007);
    s["action"]["costs"]["exit"] = amount(1_000);
    scenario(cash, cap, vec![a, b, s])
}
