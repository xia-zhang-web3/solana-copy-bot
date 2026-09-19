//! Network-free replay seam for the same capture consumer used by the stream.
use crate::source::{
    scoped_capture, YellowstoneGrpcSource, YellowstoneParsedUpdate, YellowstoneRuntimeConfig,
};
use anyhow::{ensure, Result};
use copybot_config::IngestionConfig;
use copybot_core_types::SwapEvent;
use prost::Message;
use std::sync::Arc;

#[doc(hidden)]
pub struct CaptureReplay {
    runtime: Arc<YellowstoneRuntimeConfig>,
    parser: crate::parser::SwapParser,
}
impl CaptureReplay {
    pub async fn open(config: &IngestionConfig) -> Result<Self> {
        copybot_config::validate_delivery_source(config)?;
        ensure!(
            config.capture_scope_db.is_some(),
            "replay requires explicit capture database"
        );
        let runtime = YellowstoneGrpcSource::new(config)?.runtime_config;
        scoped_capture::restore(runtime.clone()).await?;
        Ok(Self {
            runtime,
            parser: crate::parser::SwapParser::new(
                config.raydium_program_ids.clone(),
                config.pumpswap_program_ids.clone(),
            ),
        })
    }
    pub async fn accept_pending(&self) -> Result<()> {
        scoped_capture::refresh(self.runtime.clone()).await
    }
    pub async fn push(&self, bytes: &[u8]) -> Result<Option<SwapEvent>> {
        ensure!(bytes.len() <= 8_388_608, "replay envelope bound");
        let update = yellowstone_grpc_proto::prelude::SubscribeUpdate::decode(bytes)?;
        Ok(
            match scoped_capture::process(update, self.runtime.clone()).await?? {
                Some(YellowstoneParsedUpdate::Observation(raw)) => self.parser.parse(raw),
                _ => None,
            },
        )
    }
}
