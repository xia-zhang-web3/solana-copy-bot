#![allow(dead_code)]
#[path = "buy_attribution_fixture.rs"]
mod financial;
use anyhow::Result;
use copybot_core_types::{association_delivery::*, ExactSwapAmounts};
use copybot_storage_core::association_inbox::{AssociationInbox, InboxLimits};
use copybot_storage_core::association_sell_preparation::*;
pub const SOL: &str = "So11111111111111111111111111111111111111112";
pub struct F {
    pub db: financial::Db,
    pub inbox: AssociationInbox,
    pub seq: u64,
    pub order: String,
    pub our: String,
}
pub fn limits() -> InboxLimits {
    InboxLimits {
        count: 1000,
        bytes: 8 << 20,
        busy_ms: 10,
    }
}
pub fn facts(sig: &str, wallet: &str, buy: bool) -> AdmissionFacts {
    AdmissionFacts {
        facts: CheckedFacts {
            signature: sig.into(),
            slot: 42,
            wallet: wallet.into(),
            token_in: if buy { SOL } else { "mint" }.into(),
            token_out: if buy { "mint" } else { SOL }.into(),
            amount_in_bits: 0.00000095f64.to_bits(),
            amount_out_bits: 7.0f64.to_bits(),
            exact_amounts: Some(if buy {
                ExactSwapAmounts {
                    amount_in_raw: "950".into(),
                    amount_in_decimals: 9,
                    amount_out_raw: "7000".into(),
                    amount_out_decimals: 3,
                }
            } else {
                ExactSwapAmounts {
                    amount_in_raw: "7000".into(),
                    amount_in_decimals: 3,
                    amount_out_raw: "950".into(),
                    amount_out_decimals: 9,
                }
            }),
            programs: vec!["fixture".into()],
            dex: "fixture".into(),
            program_fallback: false,
        },
        info: InfoIdentity {
            encoded: sig.as_bytes().to_vec(),
            float_bits: vec![],
        },
        message_time: MessageTime::Missing,
    }
}
impl F {
    pub fn new() -> Result<Self> {
        let db = financial::Db::new()?;
        let order = db.seed("shadow:leaderbuy:leader:buy:mint", "leader", "buy")?;
        db.buy(&order)?;
        let our = format!("sig:{order}");
        let inbox = AssociationInbox::open(&db.path, limits())?;
        Ok(Self {
            db,
            inbox,
            seq: 0,
            order,
            our,
        })
    }
    pub fn event(&mut self, event: DeliveryEvent, candidate: CandidateGeneration) -> Result<()> {
        let d = Delivery {
            session: "synthetic".into(),
            sequence: self.seq,
            arrival_offset_ns: self.seq,
            event,
        };
        self.seq += 1;
        self.inbox.persist_at(
            &d,
            &candidate,
            chrono::DateTime::from_timestamp(10, 0).unwrap(),
        )
    }
    pub fn admit(&mut self, a: AdmissionFacts) -> Result<()> {
        let candidate = self.db.store.association_candidate(&a.facts);
        self.event(DeliveryEvent::Admission(a), candidate)
    }
    pub fn terminal(
        &mut self,
        a: &AdmissionFacts,
        index: u64,
        slot: u64,
        hash: &str,
    ) -> Result<()> {
        self.event(
            DeliveryEvent::Terminal {
                signature: a.facts.signature.clone(),
                expected: a.clone(),
                result: Terminal::ProviderAsserted(ProviderAssertion {
                    signature: a.facts.signature.clone(),
                    slot,
                    blockhash: hash.into(),
                    transaction_index: index,
                    block_time: BlockTime::Missing,
                }),
            },
            CandidateGeneration::Unknown,
        )
    }
    pub fn anchors(&mut self) -> Result<()> {
        let source = facts("leaderbuy", "leader", true);
        let our = facts(&self.our, "execution-wallet", true);
        for (a, index) in [(source, 1), (our, 2)] {
            self.admit(a.clone())?;
            self.terminal(&a, index, 42, "block")?;
        }
        Ok(())
    }
    pub fn sell(&mut self) -> Result<()> {
        let sell = facts("sell", "leader", false);
        self.admit(sell.clone())?;
        self.terminal(&sell, 3, 42, "block")
    }
    pub fn read(&self) -> Result<ValidatedPreparation> {
        Ok(self.inbox.sell_preparation("sell")?.unwrap())
    }
    pub fn drain(&mut self) -> Result<usize> {
        let mut count = 0;
        while self.inbox.has_sell_preparation_work()? {
            self.inbox.recover_sell_preparation()?;
            count += 1;
            assert!(count < 1000);
        }
        Ok(count)
    }
    pub fn conflict(&mut self, sig: &str) -> Result<()> {
        let i = self.inbox.identity(sig)?.unwrap();
        self.event(
            DeliveryEvent::Late {
                signature: sig.into(),
                original: i.terminal.unwrap(),
                evidence: Late::ConflictingTransaction,
            },
            CandidateGeneration::Unknown,
        )
    }
}
