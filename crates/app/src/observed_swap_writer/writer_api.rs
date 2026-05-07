use super::*;

#[path = "writer_api_base.rs"]
mod writer_api_base;
#[path = "writer_api_enqueue.rs"]
mod writer_api_enqueue;
#[path = "writer_api_shutdown.rs"]
mod writer_api_shutdown;
#[path = "writer_api_start.rs"]
mod writer_api_start;

pub(crate) use writer_api_base::ObservedSwapWriter;
