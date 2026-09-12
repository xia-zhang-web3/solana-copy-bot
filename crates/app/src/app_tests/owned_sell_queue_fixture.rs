use super::open_risk_sell_fixture::TOKEN;
use super::owned_sell_intake_fixture::Intake;
use anyhow::Result;
use chrono::Duration;
use copybot_core_types::TokenQuantity;
use copybot_shadow::ShadowSignalResult;
use copybot_storage_core::*;

pub(super) struct Queue {
    pub intake: Intake,
    pub a: Vec<ShadowSignalResult>,
    pub b: ShadowSignalResult,
    pub blocker: String,
    pub token_b: String,
}
impl Queue {
    pub async fn finish(&mut self) -> Result<()> {
        self.intake.finish().await
    }

    pub fn allow_blocker_receipt(&self, remaining: &str) {
        let wallet = &self.intake.f.config.canary_wallet_pubkey;
        let balance = |raw: &str| {
            serde_json::json!({"accountIndex":1,"owner":wallet,"mint":TOKEN,
            "uiTokenAmount":{"amount":raw,"decimals":3}})
        };
        *self.intake.f.receipt.lock().unwrap() = Some(serde_json::json!({
            "slot":1,"blockTime":(self.intake.f.now-chrono::Duration::hours(1)).timestamp(),
            "transaction":{"signatures":["prior-synthetic-tx"],"message":{"accountKeys":[
                {"pubkey":wallet,"signer":true,"writable":true},
                {"pubkey":"token-account-A","signer":false,"writable":true}]}},
            "meta":{"err":null,"fee":5000,"preBalances":[2_000_000_000u64,2_039_280],
                "postBalances":[2_010_000_000u64,2_039_280],
                "preTokenBalances":[balance("7000")],"postTokenBalances":[balance(remaining)]}
        }));
    }
    pub async fn new(old_count: usize, limit: u32) -> Result<Self> {
        let mut f = Intake::legacy(0.1, 120, 100_000).await?;
        f.f.config.canary_batch_limit = limit;
        let now = f.f.now;
        // The old pending SELL follows the original BUY; only its receipt arrives late.
        let opened = (now - Duration::hours(2)).to_rfc3339();
        f.conn()?
            .execute("UPDATE positions SET opened_ts = ?1", [&opened])?;
        f.conn()?
            .execute("UPDATE followlist SET added_at = ?1", [&opened])?;
        let blocker = f.f.prior_order("sell", now - Duration::hours(1), true)?;
        f.f.store.mark_execution_canary_confirmed_unreconciled(
            &blocker,
            &ExecutionCanaryReceiptProof {
                tx_signature: "prior-synthetic-tx".into(),
                wallet_pubkey: f.f.config.canary_wallet_pubkey.clone(),
                token: TOKEN.into(),
                side: "sell".into(),
                confirmation_status: "confirmed".into(),
                slot: Some(1),
                confirmed_at: now - Duration::hours(1),
                reason: "receipt_unavailable".into(),
            },
            now,
        )?;
        let mut a = Vec::new();
        for index in 0..old_count {
            f.f.swap.signature = format!("raw-A-{index}");
            f.f.swap.ts_utc = now - Duration::seconds(120 - index as i64);
            f.dispatch(false, false).await?;
            a.push(f.drain().await?.expect("actual raw A intent"));
        }
        let token_b = bs58::encode([21u8; 32]).into_string();
        super::tiny_parent_fixture::seed(
            &f.f.store,
            &f.conn()?,
            "owned-B",
            "leader",
            &token_b,
            &f.f.config.canary_wallet_pubkey,
            TokenQuantity::new(7000, 3),
            7_000_000,
            now - Duration::minutes(5),
        )?;
        f.f.swap.token_in = token_b.clone();
        f.f.swap.signature = "raw-B".into();
        f.f.swap.ts_utc = now - Duration::seconds(60);
        f.dispatch(false, false).await?;
        let b = f.drain().await?.expect("actual raw B intent");
        assert_eq!(f.counts()?, (0, 0, old_count as u64 + 3, 2));
        f.f.reopen()?;
        Ok(Self {
            intake: f,
            a,
            b,
            blocker,
            token_b,
        })
    }
    pub fn quote_count(&self) -> usize {
        self.intake
            .f
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _)| p.starts_with("GET /quote?"))
            .count()
    }
    pub fn quote_id(signal: &ShadowSignalResult) -> String {
        format!("quote:owned-close:{}", signal.signal_id)
    }
    pub fn assert_a_pending(&self) -> Result<()> {
        let f = &self.intake;
        assert!(!f.f.store.execution_canary_fill_exists(&self.blocker)?);
        assert_eq!(
            f.f.store
                .load_execution_canary_order(&self.blocker)?
                .unwrap()
                .status,
            EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED
        );
        for (index, a) in self.a.iter().enumerate() {
            assert!(f
                .f
                .store
                .load_execution_canary_order_by_signal(&a.signal_id)?
                .is_none());
            let s =
                f.f.store
                    .load_copy_signal_by_signal_id(&a.signal_id)?
                    .unwrap();
            assert_eq!(s.status, EXECUTION_SELL_INTENT_STATUS);
            assert_eq!(s.ts, f.f.now - Duration::seconds(120 - index as i64));
        }
        assert_eq!(
            f.f.store
                .load_execution_canary_open_position(TOKEN)?
                .unwrap()
                .qty_exact
                .unwrap()
                .raw(),
            7000
        );
        assert_eq!(f.f.store.shadow_open_lots_count()?, 0);
        assert_eq!(f.counts()?.1, 0);
        Ok(())
    }
    pub fn assert_b_submitted(&self) -> Result<()> {
        let f = &self.intake;
        let order =
            f.f.store
                .load_execution_canary_order_by_signal(&self.b.signal_id)?
                .expect("B must progress to real runner submit");
        assert_eq!(order.status, EXECUTION_STATUS_CANARY_SUBMITTED);
        assert!(!f.f.store.execution_canary_fill_exists(&order.order_id)?);
        let metadata =
            f.f.store
                .load_execution_canary_build_plan_metadata(&order.order_id)?
                .unwrap();
        assert_eq!(metadata.signal_id, self.b.signal_id);
        assert_eq!(
            metadata.quote_event_id.as_deref(),
            Some(Self::quote_id(&self.b).as_str())
        );
        assert_eq!(metadata.quote_in_amount_raw.as_deref(), Some("7000"));
        let quote: serde_json::Value =
            serde_json::from_str(metadata.quote_response_json.as_deref().unwrap())?;
        assert_eq!(quote["inputMint"], self.token_b);
        let event =
            f.f.store
                .load_execution_quote_canary_event_by_id(&Self::quote_id(&self.b))?
                .unwrap();
        assert_eq!(event.signal_ts, Some(f.f.now - Duration::seconds(60)));
        assert_eq!(event.shadow_closed_trade_id, None);
        let fee: serde_json::Value =
            serde_json::from_str(metadata.priority_fee_json.as_deref().unwrap())?;
        let calls = f.f.calls.lock().unwrap();
        let sent: Vec<_> = calls
            .iter()
            .filter(|(_, b)| b["method"] == "sendTransaction")
            .collect();
        assert_eq!(sent.len(), 1);
        use base64::Engine;
        use sha2::{Digest, Sha256};
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(sent[0].1["params"][0].as_str().unwrap())?;
        assert_eq!(
            fee["fee_proof"]["transaction_sha256"],
            format!("{:x}", Sha256::digest(bytes))
        );
        assert_eq!(
            f.f.store
                .load_execution_canary_open_position(&self.token_b)?
                .unwrap()
                .qty_exact
                .unwrap()
                .raw(),
            7000
        );
        Ok(())
    }
}
