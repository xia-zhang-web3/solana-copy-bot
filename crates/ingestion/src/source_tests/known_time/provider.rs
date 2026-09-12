use super::*;
use yellowstone_grpc_proto::prelude::SubscribeUpdate;
use yellowstone_grpc_proto::prost_types::Timestamp;

pub(super) fn capture(
    original: &Value,
    input: &Value,
    provider: &str,
    phase: &str,
) -> Result<Value> {
    let converted = fixtures::converted(input, provider);
    let started = Utc::now();
    let mut wire = None;
    let mut protobuf_time = Value::Null;
    let (raw, http) = if provider == "yellowstone" {
        let original_proto = super::super::super::super::proto::update(original);
        let mut mutated = original_proto.clone();
        // Deliberately mutate the protobuf operand before encode/decode;
        // do not rely on the old JSON adapter to preserve missing/invalid time.
        mutated.created_at = input
            .get("created_at")
            .filter(|v| !v.is_null())
            .map(|v| Timestamp {
                seconds: v["seconds"].as_i64().unwrap(),
                nanos: v["nanos"].as_i64().unwrap() as i32,
            });
        let bytes = mutated.encode_to_vec();
        let decoded = SubscribeUpdate::decode(bytes.as_slice())?;
        assert_eq!(decoded, mutated);
        protobuf_time = decoded
            .created_at
            .as_ref()
            .map(|t| json!({"seconds":t.seconds,"nanos":t.nanos}))
            .unwrap_or(Value::Null);
        assert_eq!(
            protobuf_time,
            input.get("created_at").cloned().unwrap_or(Value::Null)
        );
        let mut restored = decoded.clone();
        restored.created_at = original_proto.created_at.clone();
        assert_eq!(restored.encode_to_vec(), original_proto.encode_to_vec());
        let source = YellowstoneGrpcSource::new(&config())?;
        let raw = match yellowstone::parse_yellowstone_update(decoded, &source.runtime_config)? {
            Some(YellowstoneParsedUpdate::Observation(raw)) => Some(raw),
            None => None,
            _ => panic!("unexpected ping"),
        };
        wire = Some(bytes);
        (raw, Value::Null)
    } else if provider == "helius_fetch" {
        http::fetch(input, &config())?
    } else {
        (parse(input, provider)?, Value::Null)
    };
    let ended = Utc::now();
    let raw_present = raw.is_some();
    let event = raw.and_then(|r| SwapParser::new(vec![RAY.into()], vec![PUMP.into()]).parse(r));
    assert_eq!(raw_present, event.is_some());
    if let Some(event) = &event {
        if let Some(ts) = converted {
            assert_eq!(event.ts_utc, ts);
        } else {
            assert!(
                started <= event.ts_utc && event.ts_utc <= ended,
                "fallback clock outside call interval"
            );
        }
    }
    let record = json!({"case":original["case"],"provider":provider,"phase":phase,"input":input,
        "conversion":converted,"protobuf_created_at_after_decode":protobuf_time,
        "raw_present":raw_present,"event":event,"call_started":started,"call_ended":ended,"http":http});
    if let Ok(dir) = std::env::var("B51_CAPTURE_DIR") {
        let dir = std::path::Path::new(&dir);
        std::fs::create_dir_all(dir)?;
        let stem = format!("{}-{phase}-{provider}", original["case"].as_str().unwrap());
        std::fs::write(
            dir.join(format!("{stem}.json")),
            serde_json::to_vec_pretty(&record)?,
        )?;
        if let Some(bytes) = wire {
            std::fs::write(dir.join(format!("{stem}.pb")), bytes)?;
        }
    }
    Ok(record)
}
