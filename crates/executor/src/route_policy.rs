#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RouteKind {
    Paper,
    Rpc,
    Jito,
    Fastlane,
    Other,
}

pub(crate) fn classify_normalized_route(route: &str) -> RouteKind {
    match route {
        "paper" => RouteKind::Paper,
        "rpc" => RouteKind::Rpc,
        "jito" => RouteKind::Jito,
        "fastlane" => RouteKind::Fastlane,
        _ => RouteKind::Other,
    }
}

pub(crate) fn classify_route(route: &str) -> RouteKind {
    let normalized = route.trim().to_ascii_lowercase();
    classify_normalized_route(normalized.as_str())
}

pub(crate) fn requires_submit_fastlane_enabled(route: &str) -> bool {
    matches!(classify_route(route), RouteKind::Fastlane)
}

pub(crate) fn apply_submit_tip_policy(
    route: &str,
    requested_tip_lamports: u64,
) -> (u64, Option<&'static str>) {
    if matches!(classify_route(route), RouteKind::Rpc) && requested_tip_lamports > 0 {
        return (0, Some("rpc_tip_forced_zero"));
    }
    (requested_tip_lamports, None)
}

#[cfg(test)]
mod tests {
    use super::{
        apply_submit_tip_policy, classify_normalized_route, classify_route,
        requires_submit_fastlane_enabled, RouteKind,
    };

    #[test]
    fn classify_normalized_route_maps_known_and_unknown_values() {
        assert_eq!(classify_normalized_route("paper"), RouteKind::Paper);
        assert_eq!(classify_normalized_route("rpc"), RouteKind::Rpc);
        assert_eq!(classify_normalized_route("jito"), RouteKind::Jito);
        assert_eq!(classify_normalized_route("fastlane"), RouteKind::Fastlane);
        assert_eq!(classify_normalized_route("custom"), RouteKind::Other);
    }

    #[test]
    fn classify_route_maps_known_and_unknown_values() {
        assert_eq!(classify_route("paper"), RouteKind::Paper);
        assert_eq!(classify_route("RPC"), RouteKind::Rpc);
        assert_eq!(classify_route("jito"), RouteKind::Jito);
        assert_eq!(classify_route("fastlane"), RouteKind::Fastlane);
        assert_eq!(classify_route("custom"), RouteKind::Other);
    }

    #[test]
    fn requires_submit_fastlane_enabled_is_case_insensitive() {
        assert!(requires_submit_fastlane_enabled("fastlane"));
        assert!(requires_submit_fastlane_enabled("FASTLANE"));
        assert!(!requires_submit_fastlane_enabled("rpc"));
    }

    #[test]
    fn apply_submit_tip_policy_forces_rpc_tip_only() {
        assert_eq!(
            apply_submit_tip_policy("rpc", 1000),
            (0, Some("rpc_tip_forced_zero"))
        );
        assert_eq!(apply_submit_tip_policy("rpc", 0), (0, None));
        assert_eq!(apply_submit_tip_policy("jito", 1000), (1000, None));
        assert_eq!(apply_submit_tip_policy("paper", 1000), (1000, None));
    }
}
