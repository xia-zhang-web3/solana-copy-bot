use super::{receipt_rpc_server::serve, receipt_rpc_task_fixture::Tasks};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{sync::oneshot, task::JoinHandle};

pub(super) const RECEIPT_TEST_BUDGET_MS: u64 = 2000;
#[derive(Clone, Copy)]
pub(super) struct ServerPolicy {
    pub io: Duration,
    pub finish: Duration,
}
impl Default for ServerPolicy {
    fn default() -> Self {
        Self {
            io: Duration::from_secs(2),
            finish: Duration::from_secs(3),
        }
    }
}
#[derive(Clone, Copy)]
pub(super) enum Fault {
    Panic,
    Error,
}
#[derive(Clone)]
pub(super) struct Shared {
    pub calls: Arc<Mutex<Vec<String>>>,
    pub response: Arc<Mutex<(u16, String, u64)>>,
    pub status: Arc<Mutex<Value>>,
    pub accounts: Arc<Mutex<Value>>,
    pub tasks: Tasks,
    pub cancel_next: Arc<AtomicBool>,
    pub cancellations_requested: Arc<AtomicUsize>,
    pub cancellations_completed: Arc<AtomicUsize>,
    pub fault: Arc<Mutex<Option<Fault>>>,
}
struct Owner {
    task: JoinHandle<Result<()>>,
    stop: Option<oneshot::Sender<()>>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.task.abort();
    }
}
pub(super) struct Rpc {
    pub url: String,
    pub calls: Arc<Mutex<Vec<String>>>,
    pub response: Arc<Mutex<(u16, String, u64)>>,
    pub status: Arc<Mutex<Value>>,
    pub accounts: Arc<Mutex<Value>>,
    shared: Shared,
    owner: Mutex<Option<Owner>>,
    policy: ServerPolicy,
    context: Mutex<String>,
}
impl Rpc {
    pub async fn new(value: Value) -> Result<Self> {
        Self::with_policy(value, ServerPolicy::default()).await
    }
    pub async fn with_policy(value: Value, policy: ServerPolicy) -> Result<Self> {
        ensure!(
            !policy.io.is_zero() && policy.io <= Duration::from_secs(2),
            "invalid receipt fixture I/O budget"
        );
        ensure!(
            !policy.finish.is_zero() && policy.finish <= Duration::from_secs(3),
            "invalid receipt fixture finish budget"
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let shared = Shared {
            calls: Arc::new(Mutex::new(Vec::new())),
            response: Arc::new(Mutex::new((200, value.to_string(), 0))),
            status: Arc::new(Mutex::new(
                json!({"result":{"value":[{"err":null,"slot":42,"confirmationStatus":"confirmed"}]}}),
            )),
            accounts: Arc::new(Mutex::new(json!({"result":{"value":[]}}))),
            tasks: Tasks::default(),
            cancel_next: Arc::new(AtomicBool::new(false)),
            cancellations_requested: Arc::new(AtomicUsize::new(0)),
            cancellations_completed: Arc::new(AtomicUsize::new(0)),
            fault: Arc::new(Mutex::new(None)),
        };
        let (stop, stopped) = oneshot::channel();
        let active = shared.tasks.listener();
        let server = shared.clone();
        let task = tokio::spawn(async move {
            let _active = active;
            serve(listener, stopped, server, policy).await
        });
        Ok(Self {
            url,
            calls: shared.calls.clone(),
            response: shared.response.clone(),
            status: shared.status.clone(),
            accounts: shared.accounts.clone(),
            shared,
            owner: Mutex::new(Some(Owner {
                task,
                stop: Some(stop),
            })),
            policy,
            context: Mutex::new(String::new()),
        })
    }
    pub fn set(&self, value: Value) {
        *self.response.lock().unwrap() = (200, value.to_string(), 0);
    }
    pub fn context(&self, context: impl Into<String>) {
        *self.context.lock().unwrap() = context.into();
    }
    pub fn diagnostics(&self) -> String {
        format!(
            "{} RPC={:?}",
            self.context.lock().unwrap(),
            self.calls.lock().unwrap()
        )
    }
    pub fn tasks(&self) -> Tasks {
        self.shared.tasks.clone()
    }
    pub fn expect_receipt_cancellation(&self) {
        assert!(
            !self.shared.cancel_next.swap(true, Ordering::SeqCst),
            "unconsumed cancellation"
        );
        self.shared
            .cancellations_requested
            .fetch_add(1, Ordering::SeqCst);
    }
    pub fn cancellations(&self) -> usize {
        self.shared.cancellations_completed.load(Ordering::SeqCst)
    }
    pub fn inject(&self, fault: Fault) {
        assert!(self.shared.fault.lock().unwrap().replace(fault).is_none());
    }
    pub async fn finish(&self) -> Result<()> {
        // Move ownership out before await: no MutexGuard crosses a suspension point.
        let mut owner = self
            .owner
            .lock()
            .unwrap()
            .take()
            .context("receipt fixture already finished")?;
        owner
            .stop
            .take()
            .context("receipt fixture missing stop sender")?
            .send(())
            .ok();
        let result = match tokio::time::timeout(self.policy.finish, &mut owner.task).await {
            Ok(joined) => joined
                .context("receipt fixture listener join")
                .and_then(|v| v),
            Err(error) => {
                owner.task.abort();
                // Abort drops the listener's JoinSet, cancelling every owned handler.
                let joined = tokio::time::timeout(self.policy.finish, &mut owner.task).await;
                Err(anyhow::anyhow!(
                    "receipt fixture finish timeout: {error}; listener={joined:?}"
                ))
            }
        };
        self.shared.tasks.quiescent().await?;
        result.with_context(|| self.diagnostics())
    }
}
