//! Opt-in byte-exact replay of all saved full blocks through the actual raw adapter.
use super::base58_equivalence_tests::{walk_saved_fields, DifferentialTimings};
use super::{envelope, ConfirmedHttpRecovery, RawRecoveredBlock, RawResponse};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

#[test]
#[ignore = "requires sealed saved06 HTTP evidence directory; no provider requests"]
fn saved_06_all_1124_exact_raw_bodies_use_ordered_adapter_normalization() {
    let root = PathBuf::from(std::env::var("COPYBOT_RECOVERY_06_HTTP_EVIDENCE_DIR").unwrap());
    let client = ConfirmedHttpRecovery::new(
        "http://127.0.0.1:1",
        None,
        1024,
        16 << 20,
        Duration::from_secs(1),
    )
    .unwrap();
    let mut count = 0;
    let mut total_bytes = 0;
    for profile in [
        include_str!("../../tests/fixtures/recovery_06_profile_465.json"),
        include_str!("../../tests/fixtures/recovery_06_profile_659.json"),
    ] {
        let profile: Value = serde_json::from_str(profile).unwrap();
        let mut timings = DifferentialTimings::default();
        for record in profile["records"].as_array().unwrap() {
            let filename = record["body_file"].as_str().unwrap();
            assert_eq!(
                PathBuf::from(filename).file_name().unwrap().to_str(),
                Some(filename)
            );
            let bytes = std::fs::read(root.join(filename)).unwrap();
            let slot = record["slot"].as_u64().unwrap();
            assert!(
                bytes.len() <= 16 << 20,
                "saved response exceeds accepted bound slot={slot}"
            );
            assert_eq!(
                bytes.len() as u64,
                record["body_bytes"].as_u64().unwrap(),
                "slot={slot}"
            );
            assert_eq!(
                format!("{:x}", Sha256::digest(&bytes)),
                record["body_sha256"].as_str().unwrap(),
                "slot={slot}"
            );
            let id = record["rpc_id"].as_u64().unwrap();
            envelope::validate(200, id, "getBlock", &bytes).unwrap();
            let raw = RawRecoveredBlock {
                slot,
                response: RawResponse {
                    id,
                    status: 200,
                    bytes,
                },
            };
            let started = Instant::now();
            let recovered = client.normalize_raw_block(raw).unwrap();
            timings.normalization(started.elapsed());
            assert_eq!(recovered.block.slot, slot);
            assert_eq!(
                recovered.block.transactions.len() as u64,
                record["transaction_count"].as_u64().unwrap()
            );
            let original: Value = serde_json::from_slice(&recovered.raw_response).unwrap();
            assert_eq!(original["jsonrpc"], "2.0");
            assert_eq!(original["id"].as_u64(), Some(id));
            walk_saved_fields(&original["result"], &recovered.block, &mut timings);
            total_bytes += recovered.raw_response.len() as u64;
            count += 1;
        }
        eprintln!("SAVED_BASE58_DIFFERENTIAL profile_bodies={} sha_checked_all=true rpc_identity_all=true real_normalizer=true old_predicate_reference=true timings={timings:?} provider_requests=0", profile["records"].as_array().unwrap().len());
    }
    assert_eq!(count, 1124);
    assert_eq!(total_bytes, 4_488_326_271);
    eprintln!("SAVED_RAW_ADAPTER_REPLAY bodies={count} exact_bytes={total_bytes} checksums=ALL_MATCH normalization=ALL_COMPLETE provider_requests=0 status=200_success_evidence headers_not_archived=true performance_acceptance=false");
}
