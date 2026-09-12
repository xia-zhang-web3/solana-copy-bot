use super::*;

#[tokio::test]
#[ignore = "R1 actual CLI pair with supported ignored metadata kinds"]
async fn r1_actual_cli_capture_ignored_metadata() {
    let d = PathBuf::from(std::env::var("B81_FIXTURE_DIR").unwrap()).join("buy-native-tx-first");
    let mut messages = vec![ping()];
    for item in [
        subscribe_update::UpdateOneof::Pong(Default::default()),
        subscribe_update::UpdateOneof::Slot(Default::default()),
        subscribe_update::UpdateOneof::BlockMeta(Default::default()),
        subscribe_update::UpdateOneof::TransactionStatus(Default::default()),
    ] {
        messages.push(SubscribeUpdate {
            update_oneof: Some(item),
            ..Default::default()
        });
    }
    for n in ["000002.pb", "000003.pb"] {
        messages
            .push(SubscribeUpdate::decode(std::fs::read(d.join(n)).unwrap().as_slice()).unwrap());
    }
    let r = cli_case("r1-metadata", messages, "close", &[]).await;
    assert_eq!(r["capture"]["manifest"]["messages_received"], 7);
    assert_eq!(r["capture"]["manifest"]["envelopes_written"], 2);
    assert_eq!(r["capture"]["manifest"]["stop_reason"], "stream_closed");
}
