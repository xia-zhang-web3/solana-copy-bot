use serde_json::Value;

use crate::route_backend::UpstreamAction;
use crate::route_executor::RouteExecutorKind;
use crate::submit_deadline::SubmitDeadline;
use crate::upstream_forward::forward_to_upstream;
use crate::{AppState, Reject};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PaperRouteExecutor;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RpcRouteExecutor;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct JitoRouteExecutor;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FastlaneRouteExecutor;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RouteAdapter {
    Paper(PaperRouteExecutor),
    Rpc(RpcRouteExecutor),
    Jito(JitoRouteExecutor),
    Fastlane(FastlaneRouteExecutor),
}

impl RouteAdapter {
    pub(crate) fn from_kind(kind: RouteExecutorKind) -> Self {
        match kind {
            RouteExecutorKind::Paper => Self::Paper(PaperRouteExecutor),
            RouteExecutorKind::Rpc => Self::Rpc(RpcRouteExecutor),
            RouteExecutorKind::Jito => Self::Jito(JitoRouteExecutor),
            RouteExecutorKind::Fastlane => Self::Fastlane(FastlaneRouteExecutor),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Paper(_) => "paper",
            Self::Rpc(_) => "rpc",
            Self::Jito(_) => "jito",
            Self::Fastlane(_) => "fastlane",
        }
    }

    pub(crate) async fn execute(
        self,
        state: &AppState,
        route: &str,
        action: UpstreamAction,
        raw_body: &[u8],
        submit_deadline: Option<&SubmitDeadline>,
    ) -> std::result::Result<Value, Reject> {
        match self {
            Self::Paper(adapter) => {
                adapter
                    .execute(state, route, action, raw_body, submit_deadline)
                    .await
            }
            Self::Rpc(adapter) => {
                adapter
                    .execute(state, route, action, raw_body, submit_deadline)
                    .await
            }
            Self::Jito(adapter) => {
                adapter
                    .execute(state, route, action, raw_body, submit_deadline)
                    .await
            }
            Self::Fastlane(adapter) => {
                adapter
                    .execute(state, route, action, raw_body, submit_deadline)
                    .await
            }
        }
    }
}

impl PaperRouteExecutor {
    async fn execute(
        self,
        state: &AppState,
        route: &str,
        action: UpstreamAction,
        raw_body: &[u8],
        submit_deadline: Option<&SubmitDeadline>,
    ) -> std::result::Result<Value, Reject> {
        forward_to_upstream(state, route, action, raw_body, submit_deadline).await
    }
}

impl RpcRouteExecutor {
    async fn execute(
        self,
        state: &AppState,
        route: &str,
        action: UpstreamAction,
        raw_body: &[u8],
        submit_deadline: Option<&SubmitDeadline>,
    ) -> std::result::Result<Value, Reject> {
        forward_to_upstream(state, route, action, raw_body, submit_deadline).await
    }
}

impl JitoRouteExecutor {
    async fn execute(
        self,
        state: &AppState,
        route: &str,
        action: UpstreamAction,
        raw_body: &[u8],
        submit_deadline: Option<&SubmitDeadline>,
    ) -> std::result::Result<Value, Reject> {
        forward_to_upstream(state, route, action, raw_body, submit_deadline).await
    }
}

impl FastlaneRouteExecutor {
    async fn execute(
        self,
        state: &AppState,
        route: &str,
        action: UpstreamAction,
        raw_body: &[u8],
        submit_deadline: Option<&SubmitDeadline>,
    ) -> std::result::Result<Value, Reject> {
        forward_to_upstream(state, route, action, raw_body, submit_deadline).await
    }
}

#[cfg(test)]
mod tests {
    use super::RouteAdapter;
    use crate::route_executor::RouteExecutorKind;

    #[test]
    fn route_adapter_from_kind_maps_expected_label() {
        assert_eq!(
            RouteAdapter::from_kind(RouteExecutorKind::Paper).as_str(),
            "paper"
        );
        assert_eq!(
            RouteAdapter::from_kind(RouteExecutorKind::Rpc).as_str(),
            "rpc"
        );
        assert_eq!(
            RouteAdapter::from_kind(RouteExecutorKind::Jito).as_str(),
            "jito"
        );
        assert_eq!(
            RouteAdapter::from_kind(RouteExecutorKind::Fastlane).as_str(),
            "fastlane"
        );
    }
}
