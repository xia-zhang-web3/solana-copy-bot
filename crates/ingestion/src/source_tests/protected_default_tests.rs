//! The new evidence path is opt-in; old live/HTTP clocks remain unchanged.
use super::*;
use crate::source::durable::recovery::RecoveryCursor;

#[tokio::test]
async fn nonprotected_recovery_preserves_live_missing_and_http_recovered_clocks() -> Result<()> {
    let mut config = IngestionConfig::default();
    config.source = "yellowstone_grpc".into();
    config.yellowstone_grpc_url = "http://127.0.0.1:1".into();
    config.yellowstone_x_token = "LOCAL-default-regression".into();
    let mut tx: serde_json::Value = serde_json::from_str(include_str!(
        "../../../storage-core/tests/fixtures/canary31-source-buy-raw.json"
    ))?;
    tx["transactionIndex"] = serde_json::json!(0);
    let wallet = tx["transaction"]["message"]["accountKeys"][0]
        .as_str()
        .unwrap()
        .to_owned();
    let wrapper = serde_json::json!({"parentSlot":41,"previousBlockhash":hash(41),"blockhash":hash(42),
        "blockTime":1700000000,"blockHeight":42,"transactions":[tx],"rewards":[]});
    let block = crate::normalize_confirmed_http_block(42, &wrapper)?;
    let wallets = HashSet::from([wallet]);
    let scope = crate::replay_scope(&config, &wallets)?;
    let source = crate::source::YellowstoneGrpcSource::new(&config)?;
    let mut runtime = (*source.runtime_config).clone();
    runtime.admission_wallets = Some(wallets);
    for http in [false, true] {
        let limits = limits();
        let cursor = RecoveryCursor::new(scope.clone(), None, limits.history.clone())?;
        let (send, mut receive) = queue::channel(16, 8 << 20);
        let mut bridge = Bridge::new(
            &runtime,
            &limits,
            "LOCAL-default".into(),
            send,
            None,
            Arc::new(Default::default()),
        )?;
        bridge.enable_recovery(cursor, &limits)?;
        bridge.begin_replay()?;
        if http {
            bridge
                .http_block_with_evidence(
                    1,
                    &block,
                    b"unused in nonprotected path",
                    Some(1700000000),
                )
                .await?;
        } else {
            bridge
                .update(
                    1,
                    &SubscribeUpdate {
                        update_oneof: Some(
                            yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Block(
                                block.clone(),
                            ),
                        ),
                        ..Default::default()
                    },
                )
                .await?;
        }
        let mut observed = None;
        while let Ok(e) = receive.try_recv() {
            assert!(!matches!(
                e.delivery.event,
                DeliveryEvent::ProtectedEvidence(_) | DeliveryEvent::ProtectedObservation(_)
            ));
            if let DeliveryEvent::Admission(a) = e.delivery.event {
                observed = Some(a.message_time);
            }
        }
        assert_eq!(
            observed,
            Some(if http {
                MessageTime::RecoveredBlock {
                    block_time: Some(1700000000),
                }
            } else {
                MessageTime::Missing
            })
        );
    }
    Ok(())
}
