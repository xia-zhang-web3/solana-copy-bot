use crate::execution_signing_envelope::ExecutionSerializedTransactionPayload;
use anyhow::Result;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub(crate) struct ExecutionSerializedTransactionPayloadSlot {
    payload: Arc<Mutex<Option<ExecutionSerializedTransactionPayload>>>,
}

impl ExecutionSerializedTransactionPayloadSlot {
    pub(crate) fn new() -> Self {
        Self {
            payload: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn store(&self, payload: ExecutionSerializedTransactionPayload) -> Result<()> {
        let mut guard = self
            .payload
            .lock()
            .map_err(|_| anyhow::anyhow!("serialized transaction payload slot poisoned"))?;
        *guard = Some(payload);
        Ok(())
    }

    pub(crate) fn load(&self) -> Result<Option<ExecutionSerializedTransactionPayload>> {
        let guard = self
            .payload
            .lock()
            .map_err(|_| anyhow::anyhow!("serialized transaction payload slot poisoned"))?;
        Ok(guard.clone())
    }
}
