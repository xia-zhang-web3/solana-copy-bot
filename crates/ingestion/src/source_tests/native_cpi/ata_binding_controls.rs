use super::{
    ata_fixture::{as_parsed, built},
    harness::{capture, capture_proto, known},
    proto,
};
use anyhow::Result;
use serde_json::json;
use yellowstone_grpc_proto::prelude::subscribe_update;

#[test]
fn complete_other_account_binding_preserves_ata_but_partial_or_invalid_binding_does_not(
) -> Result<()> {
    // Same R1 binding control, now with a derived ATA and complete creation CPI.
    let healthy = built("rent", true);
    let r = healthy["roles"].clone();
    let mut failures = Vec::new();
    for provider in ["rpc_backfill", "helius_fetch", "yellowstone"] {
        for repr in ["raw", "parsed"] {
            if provider == "yellowstone" && repr == "parsed" {
                continue;
            }
            let h = if repr == "raw" {
                healthy.clone()
            } else {
                as_parsed(&healthy)
            };
            let good = capture(&format!("r1-other-{repr}-healthy"), &h, provider)?;
            let result = if provider == "yellowstone" {
                let mut update = proto::update(&healthy);
                if let Some(subscribe_update::UpdateOneof::Transaction(tx)) =
                    &mut update.update_oneof
                {
                    // A nonempty compiled list with an unresolved account index.
                    tx.transaction
                        .as_mut()
                        .unwrap()
                        .transaction
                        .as_mut()
                        .unwrap()
                        .message
                        .as_mut()
                        .unwrap()
                        .instructions[6]
                        .accounts[1] = 255;
                }
                capture_proto("r1-other-invalid-proto-index", &healthy, update)?
            } else {
                let mut damaged = h.clone();
                let create = &mut damaged["result"]["transaction"]["message"]["instructions"][6];
                if repr == "raw" {
                    create["accounts"] = json!([r["user"]]);
                } else {
                    create["parsed"]["info"]
                        .as_object_mut()
                        .unwrap()
                        .remove("newAccount");
                }
                capture(&format!("r1-other-{repr}-damaged"), &damaged, provider)?
            };
            let restored = capture(&format!("r1-other-{repr}-restored"), &h, provider)?;
            known(&good, true);
            assert_eq!(restored, good);
            if !result.is_null() {
                failures.push(format!("{provider}-{repr}"));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "incomplete binding was treated as irrelevant: {failures:?}"
    );
    Ok(())
}
