// Independent fixture decoder of the cached upstream wire definitions. Production
// uses the opposite direction (parsed operands -> wire); compare the full events.
use serde_json::{json, Value};
const SYSTEM: &str = "11111111111111111111111111111111";
const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

pub(super) fn converted(input: &Value) -> Value {
    let mut f = input.clone();
    for ix in f["result"]["transaction"]["message"]["instructions"]
        .as_array_mut()
        .unwrap()
    {
        convert(ix);
    }
    if let Some(groups) = f["result"]["meta"]["innerInstructions"].as_array_mut() {
        for group in groups {
            for ix in group["instructions"].as_array_mut().unwrap() {
                convert(ix);
            }
        }
    }
    f
}

fn convert(ix: &mut Value) {
    let program = ix["programId"].as_str().unwrap().to_owned();
    if program != SYSTEM && program != TOKEN {
        return;
    }
    let d = bs58::decode(ix["data"].as_str().unwrap())
        .into_vec()
        .unwrap();
    let a = ix["accounts"].as_array().unwrap();
    let n = |offset: usize| u64::from_le_bytes(d[offset..offset + 8].try_into().unwrap());
    let key = |start: usize| bs58::encode(&d[start..start + 32]).into_string();
    let (kind, info) = match (program.as_str(), d[0]) {
        (SYSTEM, 3) => {
            let len = n(36) as usize;
            (
                "createAccountWithSeed",
                json!({"source":a[0],"newAccount":a[1],"base":key(4),
                "seed":std::str::from_utf8(&d[44..44+len]).unwrap(),"lamports":n(44+len),"space":n(52+len),"owner":key(60+len)}),
            )
        }
        (SYSTEM, 2) => (
            "transfer",
            json!({"source":a[0],"destination":a[1],"lamports":n(4)}),
        ),
        (TOKEN, 18) => (
            "initializeAccount3",
            json!({"account":a[0],"mint":a[1],"owner":key(1)}),
        ),
        (TOKEN, 17) => ("syncNative", json!({"account":a[0]})),
        (TOKEN, 9) => (
            "closeAccount",
            json!({"account":a[0],"destination":a[1],"owner":a[2]}),
        ),
        (TOKEN, 3) => (
            "transfer",
            json!({"source":a[0],"destination":a[1],"authority":a[2],"amount":n(1).to_string()}),
        ),
        _ => return,
    };
    let depth = ix["stackHeight"].clone();
    *ix = json!({"programId":program,"program":if program==SYSTEM {"system"} else {"spl-token"},
        "parsed":{"type":kind,"info":info},"stackHeight":depth});
}
