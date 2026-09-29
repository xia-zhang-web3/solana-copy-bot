//! Constructor for the external no-yield Reader lifetime test.
use super::*;
impl Reader {
    pub(in crate::source) fn pending_for_lifetime_test(scope: Arc<CaptureScope>) -> Self {
        Self::start(
            futures_util::stream::pending(),
            futures_util::sink::drain(),
            1,
            1 << 20,
            1 << 20,
            Arc::new(Default::default()),
            scope,
        )
        .unwrap()
    }
}

#[tokio::test]
async fn ordinary_full_block_is_queued_as_bytes_then_reconstructed() {
    let block = SubscribeUpdateBlock {
        slot: 42,
        blockhash: "x".repeat(1_000_000),
        ..Default::default()
    };
    let update = SubscribeUpdate {
        update_oneof: Some(subscribe_update::UpdateOneof::Block(block)),
        ..Default::default()
    };
    let size = update.encoded_len();
    let mut reader = Reader::start(
        futures_util::stream::iter([Ok(update)]),
        futures_util::sink::drain(),
        2,
        size + 1024,
        size + 1024,
        Arc::new(Default::default()),
        CaptureScope::new(&[], std::iter::empty(), Default::default(), None),
    )
    .unwrap();
    let mut captured = reader.next().await.unwrap();
    assert!(matches!(&captured.value, CapturedValue::EncodedBlock(_)));
    assert!(captured.is_block());
    assert_eq!(captured.update().unwrap().encoded_len(), size);
    captured.dequeue();
}

#[tokio::test]
async fn negative_zero_full_block_stays_exact_in_live_queue() {
    let mut info = SubscribeUpdateTransactionInfo::default();
    let mut meta = TransactionStatusMeta::default();
    let mut balance = TokenBalance::default();
    let mut amount = UiTokenAmount::default();
    amount.ui_amount = -0.0;
    balance.ui_token_amount = Some(amount);
    meta.pre_token_balances.push(balance);
    info.meta = Some(meta);
    let update = SubscribeUpdate {
        update_oneof: Some(subscribe_update::UpdateOneof::Block(SubscribeUpdateBlock {
            slot: 43,
            transactions: vec![info],
            ..Default::default()
        })),
        ..Default::default()
    };
    let mut reader = Reader::start(
        futures_util::stream::iter([Ok(update)]),
        futures_util::sink::drain(),
        2,
        1 << 20,
        1 << 20,
        Arc::new(Default::default()),
        CaptureScope::new(&[], std::iter::empty(), Default::default(), None),
    )
    .unwrap();
    let mut captured = reader.next().await.unwrap();
    assert!(matches!(&captured.value, CapturedValue::Exact(_)));
    let Some(subscribe_update::UpdateOneof::Block(block)) =
        captured.update().unwrap().update_oneof.as_ref()
    else {
        panic!("block required")
    };
    assert_eq!(
        block.transactions[0]
            .meta
            .as_ref()
            .unwrap()
            .pre_token_balances[0]
            .ui_token_amount
            .as_ref()
            .unwrap()
            .ui_amount
            .to_bits(),
        (-0.0_f64).to_bits()
    );
}

#[tokio::test]
async fn nan_payload_full_block_roundtrips_through_compact_live_queue() {
    let bits = 0x7ff8_0000_0000_0001_u64;
    let mut info = SubscribeUpdateTransactionInfo::default();
    let mut meta = TransactionStatusMeta::default();
    let mut balance = TokenBalance::default();
    let mut amount = UiTokenAmount::default();
    amount.ui_amount = f64::from_bits(bits);
    balance.ui_token_amount = Some(amount);
    meta.pre_token_balances.push(balance);
    info.meta = Some(meta);
    let update = SubscribeUpdate {
        update_oneof: Some(subscribe_update::UpdateOneof::Block(SubscribeUpdateBlock {
            slot: 44,
            transactions: vec![info],
            ..Default::default()
        })),
        ..Default::default()
    };
    let mut reader = Reader::start(
        futures_util::stream::iter([Ok(update)]),
        futures_util::sink::drain(),
        2,
        1 << 20,
        1 << 20,
        Arc::new(Default::default()),
        CaptureScope::new(&[], std::iter::empty(), Default::default(), None),
    )
    .unwrap();
    let mut captured = reader.next().await.unwrap();
    assert!(matches!(&captured.value, CapturedValue::EncodedBlock(_)));
    let Some(subscribe_update::UpdateOneof::Block(block)) =
        captured.update().unwrap().update_oneof.as_ref() else { panic!("block required") };
    assert_eq!(block.transactions[0].meta.as_ref().unwrap()
        .pre_token_balances[0].ui_token_amount.as_ref().unwrap()
        .ui_amount.to_bits(), bits);
}
