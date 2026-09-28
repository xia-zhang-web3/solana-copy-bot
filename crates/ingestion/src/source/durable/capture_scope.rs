//! Cheap scoped intake, retaining changed-signature evidence before admission.
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};
use yellowstone_grpc_proto::prelude::SubscribeUpdateTransactionInfo;
pub(in crate::source) struct CaptureScope {
    wallets: HashSet<Vec<u8>>,
    durable: HashSet<Vec<u8>>,
    known: Mutex<HashSet<Vec<u8>>>,
    pending: Mutex<HashMap<Vec<u8>, usize>>,
    hold: Option<Arc<std::sync::atomic::AtomicBool>>,
    interrupted: std::sync::atomic::AtomicBool,
}
impl CaptureScope {
    pub fn new(
        wallets: &[String],
        durable: impl Iterator<Item = Vec<u8>>,
        known: HashSet<Vec<u8>>,
        hold: Option<Arc<std::sync::atomic::AtomicBool>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            wallets: wallets
                .iter()
                .filter_map(|s| bs58::decode(s).into_vec().ok())
                .collect(),
            durable: durable.collect(),
            known: Mutex::new(known),
            pending: Mutex::new(HashMap::new()),
            hold,
            interrupted: std::sync::atomic::AtomicBool::new(false),
        })
    }
    pub fn interrupted(&self) {
        self.interrupted
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(flag) = &self.hold {
            flag.store(true, std::sync::atomic::Ordering::Release);
        }
    }
    pub fn clear_verified(&self) {
        use std::sync::atomic::Ordering::SeqCst;
        if let Some(flag) = &self.hold {
            if !self.interrupted.load(SeqCst) {
                flag.store(false, SeqCst);
                if self.interrupted.load(SeqCst) {
                    flag.store(true, SeqCst);
                }
            }
        }
    }
    pub fn wallet(&self, info: &SubscribeUpdateTransactionInfo) -> bool {
        info.transaction
            .as_ref()
            .and_then(|t| t.message.as_ref())
            .is_some_and(|m| {
                m.header.as_ref().is_some_and(|h| {
                    m.account_keys
                        .iter()
                        .take(h.num_required_signatures as usize)
                        .any(|key| self.wallets.contains(key))
                })
            })
    }
    pub fn keep(&self, info: &SubscribeUpdateTransactionInfo) -> bool {
        let malformed = info.signature.len() != 64
            || info
                .transaction
                .as_ref()
                .and_then(|t| t.message.as_ref())
                .is_none_or(|m| {
                    m.header
                        .as_ref()
                        .is_none_or(|h| h.num_required_signatures as usize > m.account_keys.len())
                });
        malformed
            || self.wallet(info)
            || self.durable.contains(&info.signature)
            || self
                .known
                .lock()
                .expect("capture known mutex")
                .contains(&info.signature)
            || self
                .pending
                .lock()
                .expect("capture pending mutex")
                .contains_key(&info.signature)
    }
    pub fn replace_known(&self, known: HashSet<Vec<u8>>) {
        *self.known.lock().expect("capture known mutex") = known;
    }
    pub fn track(self: &Arc<Self>, signature: &[u8]) -> Pending {
        let signature = signature.to_vec();
        *self
            .pending
            .lock()
            .expect("capture pending mutex")
            .entry(signature.clone())
            .or_default() += 1;
        Pending {
            scope: self.clone(),
            signature,
        }
    }
}
pub(in crate::source) struct Pending {
    scope: Arc<CaptureScope>,
    signature: Vec<u8>,
}
impl Drop for Pending {
    fn drop(&mut self) {
        let mut pending = self.scope.pending.lock().expect("capture pending mutex");
        if let Some(count) = pending.get_mut(&self.signature) {
            *count -= 1;
            if *count == 0 {
                pending.remove(&self.signature);
            }
        }
    }
}
