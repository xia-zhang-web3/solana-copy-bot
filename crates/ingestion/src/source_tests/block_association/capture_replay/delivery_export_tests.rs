use super::*;
use std::io::Write;
#[test]
#[ignore = "requires frozen capture03, unchanged81/R1 request/hash validation"]
fn b89_validate_export_frozen_transport_frames() -> Result<()> {
    let capture = reader::read(&env_path("B89_CAPTURE_DIR"))?;
    assert_eq!(capture.manifest["complete"], false);
    let dir = env_path("B89_FIXTURE_DIR");
    std::fs::create_dir_all(&dir)?;
    // Only the transport is replaced by deterministic sequence/offset frames. No
    // future block index, oracle, or terminal outcomes are exported to the consumer.
    for variant in ["original", "missing", "invalid"] {
        let mut file = std::fs::File::create(dir.join(format!("frozen-{variant}.frames")))?;
        for (ns, u) in &capture.messages {
            let mut u = u.clone();
            if variant == "missing" {
                u.created_at = None;
            } else if variant == "invalid" {
                u.created_at = Some(yellowstone_grpc_proto::prost_types::Timestamp {
                    seconds: i64::MAX,
                    nanos: -1,
                });
            }
            let bytes = u.encode_to_vec();
            file.write_all(&ns.to_le_bytes())?;
            file.write_all(&(bytes.len() as u32).to_le_bytes())?;
            file.write_all(&bytes)?;
        }
    }
    let sorted = |set: &std::collections::HashSet<String>| {
        let mut v = set.iter().cloned().collect::<Vec<_>>();
        v.sort();
        v
    };
    let mut cohort = vec![];
    let mut raw = 0;
    for (_, u) in &capture.messages {
        if let Some(subscribe_update::UpdateOneof::Transaction(tx)) = &u.update_oneof {
            raw += 1;
            if raw <= 64 {
                let d = crate::source::yellowstone_facts::decode_yellowstone_swap_facts(
                    tx,
                    &capture.policy.interested_program_ids,
                    &capture.policy.raydium_program_ids,
                    &capture.policy.pumpswap_program_ids,
                );
                if let Some(f) = d.facts? {
                    cohort.push(f.signature);
                }
            }
        }
    }
    assert_eq!(raw, 2033);
    assert_eq!(cohort.len(), 26);
    std::fs::write(
        dir.join("frozen.json"),
        serde_json::to_vec_pretty(
            &json!({"capture_complete":false,"messages":capture.messages.len(),"raw":raw,"cohort":cohort,"programs":sorted(&capture.policy.interested_program_ids),"raydium":sorted(&capture.policy.raydium_program_ids),"pumpswap":sorted(&capture.policy.pumpswap_program_ids)}),
        )?,
    )?;
    Ok(())
}
