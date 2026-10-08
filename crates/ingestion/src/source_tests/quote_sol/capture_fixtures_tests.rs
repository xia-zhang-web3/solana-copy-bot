//! Regenerate app capture envelopes from the unmodified, saved real RPC seeds.
use super::{assert_exact, fixture, rpc};
use prost::Message;
use std::io::Write;
use yellowstone_grpc_proto::prelude::{subscribe_update::UpdateOneof, SubscribeUpdate};

#[test]
#[ignore = "writes two fixtures only to an explicitly selected empty output directory"]
fn encode_saved_quote_sol_capture_fixtures() {
    let output = std::path::PathBuf::from(
        std::env::var("COPYBOT_QUOTE_CAPTURE_FIXTURE_DIR").expect("explicit output directory"),
    );
    assert!(output.is_dir(), "output directory must already exist");
    for (wallet, name) in [
        (6, "quote_sol_wallet_06_sell.pb"),
        (11, "quote_sol_wallet_11_buy.pb"),
    ] {
        let raw = fixture(wallet);
        // The fixture must first pass the common production facts decoder,
        // including the executed amounts and the SOL-relative direction.
        assert_exact(&raw, wallet);
        let envelope = SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Transaction(rpc::update(&raw).unwrap())),
            // Synthetic transport timestamp from the saved chain blockTime;
            // this fixture proves capture ordering, not actual gRPC latency.
            created_at: Some(yellowstone_grpc_proto::prost_types::Timestamp {
                seconds: raw["blockTime"].as_i64().unwrap(),
                nanos: 0,
            }),
            ..Default::default()
        };
        let bytes = envelope.encode_to_vec();
        assert_eq!(SubscribeUpdate::decode(bytes.as_slice()).unwrap(), envelope);
        std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(output.join(name))
            .unwrap()
            .write_all(&bytes)
            .unwrap();
    }
}
