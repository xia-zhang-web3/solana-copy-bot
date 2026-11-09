//! One local subscription, finite captured inputs and an open tail; no provider.
use futures_util::{stream, StreamExt};
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use yellowstone_grpc_proto::prelude::*;
#[derive(Clone)]
pub(super) struct Fixture {
    pub messages: Vec<SubscribeUpdate>,
    pub subscriptions: Arc<AtomicUsize>,
}
#[tonic::async_trait]
impl geyser_server::Geyser for Fixture {
    // The fixture does not expose the new optional gossip RPC.
    type SubscribeGossipStream = std::pin::Pin<
        Box<
            dyn futures_util::Stream<
                    Item = std::result::Result<SubscribeUpdateGossip, tonic::Status>,
                > + Send,
        >,
    >;
    async fn subscribe_gossip(
        &self,
        _: tonic::Request<SubscribeGossipRequest>,
    ) -> std::result::Result<tonic::Response<Self::SubscribeGossipStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "fixture gossip is not subscribed",
        ))
    }
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
        assert_eq!(
            req.from_slot, None,
            "one initial complete-block subscription"
        );
        self.subscriptions.fetch_add(1, Ordering::SeqCst);
        Ok(tonic::Response::new(Box::pin(
            stream::iter(self.messages.clone().into_iter().map(Ok::<_, tonic::Status>))
                .chain(stream::pending())
                .then(|item| async move {
                    tokio::task::yield_now().await;
                    item
                }),
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

pub(super) struct Server {
    pub url: String,
    pub subscriptions: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}
impl Server {
    pub async fn start(messages: Vec<SubscribeUpdate>) -> anyhow::Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let subscriptions = Arc::new(AtomicUsize::new(0));
        let service = Fixture {
            messages,
            subscriptions: subscriptions.clone(),
        };
        let incoming = stream::unfold(listener, |listener| async move {
            let next = listener.accept().await.map(|(socket, peer)| {
                assert!(peer.ip().is_loopback());
                socket
            });
            Some((next, listener))
        });
        let task = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(geyser_server::GeyserServer::new(service))
                .serve_with_incoming(incoming),
        );
        Ok(Self {
            url,
            subscriptions,
            task,
        })
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
