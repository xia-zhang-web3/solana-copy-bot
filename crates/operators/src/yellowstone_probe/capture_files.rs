use super::{capture_config::CaptureConfig, capture_terminal::TerminalStatus};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
};
use yellowstone_grpc_proto::{
    prelude::{subscribe_update::UpdateOneof, SubscribeUpdate},
    prost::Message,
};

pub(crate) struct CaptureFiles {
    pub config: CaptureConfig,
    pub session: String,
    pub rows: Vec<Value>,
    pub kinds: BTreeMap<&'static str, u64>,
    pub received: u64,
    pub written: u64,
    pub payload_bytes: u64,
    pub pending: Vec<(usize, Box<[u8]>)>,
    pub buffered_bytes: u64,
    pub terminal_status: Option<TerminalStatus>,
}
impl CaptureFiles {
    pub fn create(config: &CaptureConfig) -> std::io::Result<Self> {
        // Atomic mkdir refuses existing evidence (including symlinks).
        std::fs::create_dir(&config.output)?;
        let mut random = [0u8; 16];
        File::open("/dev/urandom")?.read_exact(&mut random)?;
        Ok(Self {
            config: config.clone(),
            session: random.iter().map(|b| format!("{b:02x}")).collect(),
            rows: Vec::with_capacity(config.messages as usize),
            pending: Vec::with_capacity(config.messages as usize),
            buffered_bytes: 0,
            kinds: BTreeMap::new(),
            received: 0,
            written: 0,
            payload_bytes: 0,
            terminal_status: None,
        })
    }
    pub fn record(
        &mut self,
        message: &SubscribeUpdate,
        offset_ns: u64,
    ) -> Result<(), &'static str> {
        if self.received >= self.config.messages {
            return Err("message_count_limit");
        }
        self.received += 1;
        let (kind, save, heavy) = match message.update_oneof.as_ref() {
            Some(UpdateOneof::Transaction(_)) => ("transaction", true, false),
            Some(UpdateOneof::Block(b)) => (
                "block",
                true,
                !b.accounts.is_empty() || !b.entries.is_empty(),
            ),
            Some(UpdateOneof::Account(_)) => ("account", false, true),
            Some(UpdateOneof::Entry(_)) => ("entry", false, true),
            Some(UpdateOneof::Ping(_)) => ("ping", false, false),
            Some(UpdateOneof::Pong(_)) => ("pong", false, false),
            Some(UpdateOneof::Slot(_)) => ("slot", false, false),
            Some(UpdateOneof::BlockMeta(_)) => ("block_meta", false, false),
            Some(UpdateOneof::TransactionStatus(_)) => ("transaction_status", false, false),
            None => ("none", false, false),
        };
        *self.kinds.entry(kind).or_default() += 1;
        let mut row = json!({"session_id":self.session,"sequence":self.received,
            "arrival_offset_ns":offset_ns,"kind":kind,"saved":false});
        let result = (|| {
            if heavy {
                return Err("unexpected_heavy_input");
            }
            // Transport already bounded allocation. Check decoded length before
            // re-encoding. Only bounded encoded bytes survive this call.
            let size = message.encoded_len() as u64;
            row["encoded_bytes"] = json!(size);
            if size > self.config.message_bytes {
                return Err("message_byte_limit");
            }
            if !save {
                return Ok(());
            }
            if self.buffered_bytes + size + self.config.metadata_reserve() > self.config.total_bytes
            {
                return Err("total_output_limit");
            }
            let bytes = message.encode_to_vec();
            // Retained byte capacity equals encoded length; decoded message and
            // transient encoding allocation are one additional in-flight envelope.
            self.pending
                .push((self.rows.len(), bytes.into_boxed_slice()));
            self.buffered_bytes += size;
            Ok(())
        })();
        if let Err(reason) = result {
            row["refused"] = json!(reason);
        }
        self.rows.push(row);
        result
    }
    pub fn manifest(&self, reason: &str, request: Value, elapsed_ns: u64) -> Value {
        let mut manifest = json!({"schema":1,"mode":"association-capture","session_id":self.session,
            "representation":"re-encoded decoded protobuf envelopes; not original transport wire bytes",
            "request":request,"limits":self.config,"transport_decode_bytes":self.config.transport_bytes(),
            "metadata_reserve_bytes":self.config.metadata_reserve(),"payload_bytes":self.payload_bytes,
            "counter_scope":"all decoded updates, including ignored and refused; transport decode failures are terminal errors",
            "messages_received":self.received,"envelopes_written":self.written,
            "message_kinds":self.kinds,"messages":self.rows,"elapsed_ns":elapsed_ns,
            "complete":reason == "stream_closed","stop_reason":reason,
            "terminal_status":self.terminal_status,
            "provider_coverage":"unmeasured","association_verdict":"not_evaluated",
            "production_green":false});
        if let Some(profile) = self.config.profile_name() {
            manifest["capture_profile"] = json!(profile);
        }
        manifest
    }
}
pub(super) fn write_new(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}
