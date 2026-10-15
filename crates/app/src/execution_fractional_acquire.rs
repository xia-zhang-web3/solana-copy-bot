//! Request-bound reads share the caller's transport and cumulative body budget.
use super::{
    readiness::{self, Clock},
    transport,
};
use anyhow::{ensure, Context, Result};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    ordered_sell_quote::{
        fractional::inventory::{Evidence, Page, TOKEN, TOKEN22},
        QuoteClaim,
    },
    SqliteStore,
};
use serde_json::{json, Value};

pub(crate) struct Reader<'a, T, C> {
    pub store: &'a mut SqliteStore,
    pub config: &'a ExecutionConfig,
    pub claim: &'a QuoteClaim,
    pub rpc: &'a mut T,
    pub clock: &'a C,
}
impl<T: transport::Transport, C: Clock + Sync> Reader<'_, T, C> {
    pub(crate) async fn read(&mut self, method: &str, params: Value) -> Result<Value> {
        self.store
            .recheck_fractional_collection_progress(self.claim, self.clock.now())?;
        let mut check = || readiness::guard(self.config, self.claim, self.clock);
        check()?;
        let request =
            json!({"jsonrpc":"2.0","id":"fractional-inventory-v1","method":method,"params":params});
        let mut response = self.rpc.read(request.clone(), &mut check).await?;
        check()?;
        self.store
            .recheck_fractional_collection_progress(self.claim, self.clock.now())?;
        ensure!(
            response["jsonrpc"] == "2.0"
                && response["id"] == request["id"]
                && response.get("error").is_none()
                && response.get("result").is_some_and(|v| !v.is_null()),
            "fraction_rpc_binding"
        );
        Ok(response["result"].take())
    }
    pub(crate) async fn pages(
        &mut self,
        slot: u64,
        block: Value,
        parent: Value,
    ) -> Result<Evidence> {
        let parent_slot = block["parentSlot"]
            .as_u64()
            .context("fraction_parent_missing")?;
        let mut pages = vec![];
        for program in [TOKEN22, TOKEN] {
            let mut cursor: Option<String> = None;
            let mut seen = std::collections::BTreeSet::new();
            loop {
                ensure!(pages.len() < 32, "fraction_pages_bound");
                let mut cfg = json!({"slot":parent_slot,"pageLimit":1000});
                if let Some(key) = &cursor {
                    cfg["pageKey"] = json!(key);
                }
                let response = self
                    .read(
                        "getTokenAccountsByOwnerAtSlot",
                        json!([self.claim.binding.source_wallet,{"programId":program},cfg]),
                    )
                    .await?;
                let next = response
                    .get("pageKey")
                    .context("fraction_page_terminal_missing")?;
                let next = if next.is_null() {
                    None
                } else {
                    Some(
                        next.as_str()
                            .filter(|s| !s.is_empty() && s.len() <= 1024)
                            .context("fraction_cursor")?
                            .to_owned(),
                    )
                };
                if let Some(key) = &next {
                    ensure!(seen.insert(key.clone()), "fraction_cursor_cycle");
                }
                pages.push(Page {
                    program: program.into(),
                    cursor,
                    response,
                });
                cursor = next;
                if cursor.is_none() {
                    break;
                }
            }
        }
        let execution_accounts = self
            .read(
                "getTokenAccountsByOwner",
                json!([
            self.config.canary_wallet_pubkey,{"mint":self.claim.binding.mint},
            {"encoding":"jsonParsed","commitment":"finalized"}]),
            )
            .await?;
        Ok(Evidence {
            version: 1,
            slot,
            block,
            parent,
            pages,
            execution_accounts,
        })
    }
}
