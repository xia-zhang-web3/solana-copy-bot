//! Typed, explicitly synthetic control at the real SQLite/native admission boundary.
//! It never renames a saved BUY or claims a market decoder/submit/receipt result.
use chrono::{DateTime, Duration, Utc};
use copybot_core_types::association_delivery::{
    BlockTime, CandidateGeneration, Delivery, DeliveryEvent, InfoIdentity, MessageTime,
    ProviderAssertion, Terminal,
};
use copybot_storage_core::{
    association_inbox::AssociationInbox,
    native_buy::{NativeBuyCandidate, NativeBuyFence, TechnicalCohortAuthority, SPL_TOKEN_PROGRAM},
    SqliteStore,
};
use std::path::{Path, PathBuf};

#[path = "../../../storage-core/tests/common/association.rs"]
mod facts_fixture;

#[derive(Default)]
struct MockQuoteAdapter {
    calls: Vec<String>,
}
impl MockQuoteAdapter {
    fn quote(
        &mut self,
        store: &SqliteStore,
        candidate: &NativeBuyCandidate,
        observed: DateTime<Utc>,
    ) -> &'static str {
        assert!(store
            .native_buy_recheck(&candidate.signal_id, &candidate.decision_id, observed, 120)
            .unwrap());
        assert!(candidate.amount_lamports <= 10_000_000);
        assert!(!self.calls.contains(&candidate.signal_id));
        self.calls.push(candidate.signal_id.clone());
        // This explicit test adapter never simulates, signs, submits or claims a receipt.
        "MOCK_ONLY_NO_SUBMIT"
    }
}

pub struct Control {
    path: PathBuf,
    session: String,
    fence_slot: u64,
    wallet: String,
    deadline: DateTime<Utc>,
}
impl Control {
    pub fn register(
        inbox: &mut AssociationInbox,
        path: &Path,
        session: &str,
        fence_slot: u64,
        activated: DateTime<Utc>,
        sampled: DateTime<Utc>,
    ) -> Self {
        let saved: serde_json::Value = serde_json::from_str(include_str!(
            "../../../storage-core/tests/fixtures/recovery_06.json"
        ))
        .unwrap();
        let wallet = saved["authority"]["wallet_ids"][0]
            .as_str()
            .unwrap()
            .to_string();
        let deadline = activated + Duration::hours(4);
        inbox
            .register_technical_cohort_authority(&TechnicalCohortAuthority {
                run_id: "MOCK-OFFLINE-RECOVERY-10-32".into(),
                wallet_ids: vec![wallet.clone()],
                mint_policy: saved["authority"]["mint_policy"].as_str().unwrap().into(),
                activated_at: activated,
                deadline,
                max_buy_count: 1,
                policy_identity: "mock-offline-policy".into(),
            })
            .unwrap();
        inbox
            .record_native_buy_fence_epoch(&NativeBuyFence {
                session: session.into(),
                processed_slot: fence_slot,
                sampled_at: sampled,
                genesis_hash: "mock-offline-genesis".into(),
                policy_identity: "mock-offline-policy".into(),
            })
            .unwrap();
        Self {
            path: path.into(),
            session: session.into(),
            fence_slot,
            wallet,
            deadline,
        }
    }
    pub fn fresh(&self, sequence: u64, created: DateTime<Utc>) -> Delivery {
        let mut a = facts_fixture::facts();
        a.facts.signature = format!("MOCK-new-after-recovery-{sequence}");
        a.facts.slot = self.fence_slot + 1;
        a.facts.wallet = self.wallet.clone();
        a.facts.token_in = "So11111111111111111111111111111111111111112".into();
        a.facts.token_out = "MOCK-classic-spl-mint".into();
        a.facts.amount_in_bits = 0.01f64.to_bits();
        a.facts.amount_out_bits = 0.012345f64.to_bits();
        let exact = a.facts.exact_amounts.as_mut().unwrap();
        exact.amount_in_raw = "10000000".into();
        exact.amount_in_decimals = 9;
        exact.amount_out_raw = "12345".into();
        exact.amount_out_decimals = 6;
        a.info = InfoIdentity {
            encoded: format!("MOCK-distinct-new-info-{sequence}").into_bytes(),
            float_bits: vec![],
        };
        a.message_time = MessageTime::CreatedAt {
            seconds: created.timestamp(),
            nanos: created.timestamp_subsec_nanos(),
        };
        Delivery {
            session: self.session.clone(),
            sequence,
            arrival_offset_ns: sequence,
            event: DeliveryEvent::Admission(a),
        }
    }
    pub fn assert_fresh_once(
        &self,
        inbox: &mut AssociationInbox,
        sequence: u64,
        created: DateTime<Utc>,
        observed: DateTime<Utc>,
    ) -> &'static str {
        assert!(observed >= created && observed - created < Duration::seconds(120));
        assert!(
            observed < self.deadline,
            "offline immutable authority deadline"
        );
        let live = self.fresh(sequence, created);
        inbox
            .persist_at(&live, &CandidateGeneration::Unknown, observed)
            .unwrap();
        inbox
            .persist_at(&live, &CandidateGeneration::Unknown, observed)
            .unwrap();
        let DeliveryEvent::Admission(a) = &live.event else {
            unreachable!()
        };
        let terminal = Delivery {
            sequence: sequence + 1,
            event: DeliveryEvent::Terminal {
                signature: a.facts.signature.clone(),
                expected: a.clone(),
                result: Terminal::ProviderAsserted(ProviderAssertion {
                    slot: a.facts.slot,
                    blockhash: "MOCK-local-confirmed-hash".into(),
                    signature: a.facts.signature.clone(),
                    transaction_index: 0,
                    block_time: BlockTime::Missing,
                }),
            },
            ..live.clone()
        };
        inbox
            .persist_at(&terminal, &CandidateGeneration::Unknown, observed)
            .unwrap();
        let store = SqliteStore::open(&self.path).unwrap();
        assert!(store
            .native_buy_record_finalized(
                &a.facts.signature,
                a.facts.slot,
                SPL_TOKEN_PROGRAM,
                observed
            )
            .unwrap());
        let ready = store
            .native_buy_ready(&a.facts.signature, observed, 120)
            .unwrap()
            .unwrap();
        let mut adapter = MockQuoteAdapter::default();
        // Sample at the adapter boundary, including actual prior SQLite work.
        let quoted_at = Utc::now();
        assert!(quoted_at - created < Duration::seconds(1));
        let outcome = adapter.quote(&store, &ready, quoted_at);
        assert_eq!(adapter.calls.len(), 1);
        let second = self.fresh(sequence + 2, observed);
        inbox
            .persist_at(&second, &CandidateGeneration::Unknown, observed)
            .unwrap();
        let sql = rusqlite::Connection::open(&self.path).unwrap();
        for table in [
            "native_buy_decisions",
            "native_buy_cohort_decisions",
            "copy_signals",
        ] {
            let count: u64 = sql
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
                .unwrap();
            assert_eq!(count, 1, "single synthetic candidate: {table}");
        }
        for table in ["orders", "fills", "positions"] {
            let count: u64 = sql
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
                .unwrap();
            assert_eq!(count, 0, "no financial execution: {table}");
        }
        outcome
    }
}
