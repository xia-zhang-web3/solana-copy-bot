//! Cheap scoped intake, retaining changed-signature evidence before admission.
use super::continuity_telemetry::{
    diagnostic_time, ReaderObservation, TransitionKind, TransitionStart,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};
use yellowstone_grpc_proto::prelude::SubscribeUpdateTransactionInfo;
pub(in crate::source) struct CaptureScope {
    wallets: HashSet<Vec<u8>>,
    durable: Mutex<HashSet<Vec<u8>>>,
    known: Mutex<HashSet<Vec<u8>>>,
    pending: Mutex<HashMap<Vec<u8>, usize>>,
    hold: Option<Arc<std::sync::atomic::AtomicBool>>,
    interrupted: std::sync::atomic::AtomicBool,
    observation: Option<ReaderObservation>,
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
            durable: Mutex::new(durable.collect()),
            known: Mutex::new(known),
            pending: Mutex::new(HashMap::new()),
            hold,
            interrupted: std::sync::atomic::AtomicBool::new(false),
            observation: None,
        })
    }
    pub fn observed(mut self: Arc<Self>, observation: ReaderObservation) -> Arc<Self> {
        Arc::get_mut(&mut self)
            .expect("new capture scope")
            .observation = Some(observation);
        self
    }
    fn transition(&self) -> Option<TransitionStart> {
        let began = diagnostic_time();
        self.observation.as_ref().map(|o| {
            o.begin_tracked(
                began,
                self.hold.as_ref(),
                self.interrupted.load(std::sync::atomic::Ordering::SeqCst),
            )
        })
    }
    fn recorded(&self, kind: TransitionKind, start: Option<TransitionStart>) {
        if let (Some(observation), Some(start)) = (&self.observation, start) {
            let interrupted = self.interrupted.load(std::sync::atomic::Ordering::SeqCst);
            observation.finish_tracked(kind, start, self.hold.as_ref(), interrupted);
        }
    }
    pub(super) fn diagnostic_identity(
        &self,
    ) -> Option<super::continuity_telemetry::ReaderIdentity> {
        self.observation.as_ref().map(|o| o.identity())
    }
    pub fn frontier(&self, slot: u64) {
        if let Some(observation) = &self.observation {
            observation.frontier(slot);
        }
    }
    pub fn interrupted(&self) {
        self.interrupted_as(TransitionKind::TaskGuardDrop);
    }
    pub fn reader_dropped(&self) {
        self.interrupted_as(TransitionKind::ReaderDrop);
    }
    fn interrupted_as(&self, kind: TransitionKind) {
        // Cover the earlier interruption flag as well as the following hold
        // assertion. Observation must not certify the pre-store pause window.
        let _phase = self.observation.as_ref().map(|o| o.interruption_phase());
        let observation = self.transition();
        self.interrupted
            .store(true, std::sync::atomic::Ordering::SeqCst);
        #[cfg(test)]
        super::clock_qualification_tests::ack_hook::after_interrupted();
        if let Some(flag) = &self.hold {
            self.write_hold(flag, true, std::sync::atomic::Ordering::Release);
        }
        self.recorded(kind, observation);
    }
    fn write_hold(
        &self,
        flag: &std::sync::atomic::AtomicBool,
        value: bool,
        order: std::sync::atomic::Ordering,
    ) {
        if let Some(observation) = &self.observation {
            observation.write_hold(flag, value, order);
        } else {
            flag.store(value, order);
        }
    }
    pub fn clear_verified(&self) {
        let observation = self.transition();
        use std::sync::atomic::Ordering::SeqCst;
        if let Some(flag) = &self.hold {
            if !self.interrupted.load(SeqCst) {
                self.write_hold(flag, false, SeqCst);
                #[cfg(test)]
                super::clock_qualification_tests::ack_hook::after_clear_false();
                if self.interrupted.load(SeqCst) {
                    self.write_hold(flag, true, SeqCst);
                }
            }
        }
        self.recorded(TransitionKind::VerifiedClear, observation);
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
            || self
                .durable
                .lock()
                .expect("capture durable mutex")
                .contains(&info.signature)
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
    pub fn replace_durable(&self, durable: impl Iterator<Item = Vec<u8>>) {
        *self.durable.lock().expect("capture durable mutex") = durable.collect();
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
