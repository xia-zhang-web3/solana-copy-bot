//! Test-only expansion of raw RPC instructions into the jsonParsed production view.
use super::*;
pub(super) fn expanded(r: &Value, parsed: bool) -> Value {
    let mut r = r.clone();
    let keys = keys(&r);
    let signers = r["transaction"]["message"]["header"]["numRequiredSignatures"]
        .as_u64()
        .unwrap() as usize;
    r["transaction"]["message"]["accountKeys"] = json!(keys
        .iter()
        .enumerate()
        .map(|(i, k)| json!({"pubkey":k,"signer":i<signers}))
        .collect::<Vec<_>>());
    let convert = |ix: &mut Value| {
        let p = keys[ix["programIdIndex"].as_u64().unwrap() as usize].clone();
        let a: Vec<_> = ix["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap() as usize)
            .collect();
        let d = raw(ix);
        let depth = ix.get("stackHeight").cloned();
        *ix = json!({"programId":p,"accounts":a.iter().map(|i|&keys[*i]).collect::<Vec<_>>(),"data":bs58::encode(&d).into_string()});
        if let Some(depth) = depth {
            ix["stackHeight"] = depth;
        }
        if !parsed {
            return;
        }
        let info = match p.as_str() {
            TOKEN => match d.as_slice() {
                [3, ..] if d.len() == 9 && a.len() == 3 => Some((
                    "transfer",
                    json!({"source":keys[a[0]],"destination":keys[a[1]],"authority":keys[a[2]],"amount":u64::from_le_bytes(d[1..9].try_into().unwrap()).to_string()}),
                )),
                [12, ..] if d.len() == 10 && a.len() == 4 => Some((
                    "transferChecked",
                    json!({"source":keys[a[0]],"mint":keys[a[1]],"destination":keys[a[2]],"authority":keys[a[3]],"tokenAmount":{"amount":u64::from_le_bytes(d[1..9].try_into().unwrap()).to_string(),"decimals":d[9]}}),
                )),
                [1] if a.len() == 4 => Some((
                    "initializeAccount",
                    json!({"account":keys[a[0]],"mint":keys[a[1]],"owner":keys[a[2]],"rentSysvar":keys[a[3]]}),
                )),
                _ => None,
            },
            native_attribution::ATA if d.is_empty() && a.len() == 7 => Some((
                "create",
                json!({"source":keys[a[0]],"account":keys[a[1]],"wallet":keys[a[2]],"mint":keys[a[3]],"systemProgram":keys[a[4]],"tokenProgram":keys[a[5]],"rentSysvar":keys[a[6]]}),
            )),
            _ => None,
        };
        if let Some((kind, info)) = info {
            ix.as_object_mut().unwrap().remove("accounts");
            ix.as_object_mut().unwrap().remove("data");
            ix["parsed"] = json!({"type":kind,"info":info});
        }
    };
    for ix in r["transaction"]["message"]["instructions"]
        .as_array_mut()
        .unwrap()
    {
        convert(ix);
    }
    for g in r["meta"]["innerInstructions"].as_array_mut().unwrap() {
        for ix in g["instructions"].as_array_mut().unwrap() {
            convert(ix);
        }
    }
    r
}
