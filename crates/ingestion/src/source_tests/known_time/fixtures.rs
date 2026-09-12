use super::*;

pub(super) fn base(sell: bool, native: bool, label: &str) -> Value {
    let mut f = inventory::healthy(sell, native, false, false);
    inventory::rename(&mut f, label);
    let mut signature = bs58::decode(f["signature"].as_str().unwrap())
        .into_vec()
        .unwrap();
    signature[0] = 51;
    f["signature"] = json!(bs58::encode(signature).into_string());
    f["result"]["transaction"]["signatures"][0] = f["signature"].clone();
    f
}

pub(super) fn valid(f: &mut Value, kind: usize) {
    match kind {
        0 => {}
        1..=3 => {
            let seconds = [0, 1_600_000_000, -1][kind - 1];
            f["created_at"] = json!({"seconds":seconds,"nanos":0});
            f["result"]["blockTime"] = json!(seconds);
        }
        4 => f["created_at"]["nanos"] = json!(123_456_789),
        _ => unreachable!(),
    }
}

pub(super) fn damaged(f: &Value, provider: &str, kind: usize) -> Value {
    let mut input = f.clone();
    if provider == "yellowstone" {
        match kind {
            0 => {
                input.as_object_mut().unwrap().remove("created_at");
            }
            1 => input["created_at"]["nanos"] = json!(-1),
            2 => input["created_at"]["nanos"] = json!(1_000_000_000),
            3 => input["created_at"]["seconds"] = json!(i64::MAX),
            _ => unreachable!(),
        }
    } else {
        match kind {
            0 => {
                input["result"].as_object_mut().unwrap().remove("blockTime");
            }
            1 => input["result"]["blockTime"] = Value::Null,
            2 => input["result"]["blockTime"] = json!("1788781800"),
            3 => input["result"]["blockTime"] = json!(i64::MAX),
            _ => unreachable!(),
        }
    }
    assert!(converted(&input, provider).is_none());
    assert_eq!(restore(&input, f, provider), *f);
    input
}

pub(super) fn restore(input: &Value, original: &Value, provider: &str) -> Value {
    let mut restored = input.clone();
    if provider == "yellowstone" {
        restored["created_at"] = original["created_at"].clone();
    } else {
        restored["result"]["blockTime"] = original["result"]["blockTime"].clone();
    }
    assert_eq!(
        serde_json::to_vec(&restored).unwrap(),
        serde_json::to_vec(original).unwrap()
    );
    restored
}

// The exact existing conversion policy, without its current-clock fallback.
pub(super) fn converted(f: &Value, provider: &str) -> Option<DateTime<Utc>> {
    if provider == "yellowstone" {
        let timestamp = f.get("created_at")?;
        let nanos = timestamp.get("nanos")?.as_i64()?;
        if nanos < 0 || nanos >= 1_000_000_000 {
            return None;
        }
        DateTime::<Utc>::from_timestamp(timestamp.get("seconds")?.as_i64()?, nanos as u32)
    } else {
        f["result"]
            .get("blockTime")
            .and_then(Value::as_i64)
            .and_then(|seconds| DateTime::<Utc>::from_timestamp(seconds, 0))
    }
}
