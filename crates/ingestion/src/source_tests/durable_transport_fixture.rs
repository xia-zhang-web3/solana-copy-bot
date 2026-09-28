//! Local tonic fixture with a precise terminal boundary, no external service.
use futures_util::{stream, StreamExt};
use std::pin::Pin;
use yellowstone_grpc_proto::prelude::*;
#[derive(Clone)]
pub(super) struct Fixture {
    pub messages: Vec<SubscribeUpdate>,
    pub terminal: Option<tonic::Code>,
    pub reject_subscribe: Option<tonic::Code>,
}
#[tonic::async_trait]
impl geyser_server::Geyser for Fixture {
    type SubscribeStream =
        Pin<Box<dyn futures_util::Stream<Item = Result<SubscribeUpdate, tonic::Status>> + Send>>;
    type SubscribeDeshredStream = Pin<
        Box<dyn futures_util::Stream<Item = Result<SubscribeUpdateDeshred, tonic::Status>> + Send>,
    >;
    async fn subscribe(
        &self,
        request: tonic::Request<tonic::Streaming<SubscribeRequest>>,
    ) -> Result<tonic::Response<Self::SubscribeStream>, tonic::Status> {
        let req = request.into_inner().message().await?.unwrap();
        assert_eq!(req.commitment, Some(CommitmentLevel::Confirmed as i32));
        assert_eq!(req.blocks.len(), 1);
        if let Some(code) = self.reject_subscribe {
            assert!(req.from_slot.is_some());
            return Err(tonic::Status::new(
                code,
                "controlled replay request unavailable",
            ));
        }
        let terminal = self.terminal.map(|code| {
            Err(tonic::Status::new(
                code,
                "controlled local terminal boundary",
            ))
        });
        Ok(tonic::Response::new(Box::pin(
            stream::iter(self.messages.clone().into_iter().map(Ok).chain(terminal)).then(
                |item| async move {
                    tokio::task::yield_now().await;
                    item
                },
            ),
        )))
    }
    async fn subscribe_deshred(
        &self,
        _: tonic::Request<tonic::Streaming<SubscribeDeshredRequest>>,
    ) -> Result<tonic::Response<Self::SubscribeDeshredStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("local fixture"))
    }
    async fn subscribe_replay_info(
        &self,
        _: tonic::Request<SubscribeReplayInfoRequest>,
    ) -> Result<tonic::Response<SubscribeReplayInfoResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("local fixture"))
    }
    async fn ping(
        &self,
        _: tonic::Request<PingRequest>,
    ) -> Result<tonic::Response<PongResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("local fixture"))
    }
    async fn get_latest_blockhash(
        &self,
        _: tonic::Request<GetLatestBlockhashRequest>,
    ) -> Result<tonic::Response<GetLatestBlockhashResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("local fixture"))
    }
    async fn get_block_height(
        &self,
        _: tonic::Request<GetBlockHeightRequest>,
    ) -> Result<tonic::Response<GetBlockHeightResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("local fixture"))
    }
    async fn get_slot(
        &self,
        _: tonic::Request<GetSlotRequest>,
    ) -> Result<tonic::Response<GetSlotResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("local fixture"))
    }
    async fn is_blockhash_valid(
        &self,
        _: tonic::Request<IsBlockhashValidRequest>,
    ) -> Result<tonic::Response<IsBlockhashValidResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("local fixture"))
    }
    async fn get_version(
        &self,
        _: tonic::Request<GetVersionRequest>,
    ) -> Result<tonic::Response<GetVersionResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("local fixture"))
    }
}
