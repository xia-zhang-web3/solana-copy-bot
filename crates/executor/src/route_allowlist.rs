use std::collections::HashSet;

use anyhow::{anyhow, Result};

use crate::{route_normalization::normalize_route, route_policy::requires_submit_fastlane_enabled};

const KNOWN_ROUTES: &[&str] = &["paper", "rpc", "jito", "fastlane"];

pub(crate) fn parse_route_allowlist(csv: String) -> Result<HashSet<String>> {
    let mut routes = HashSet::new();
    for raw in csv.split(',') {
        let route = normalize_route(raw);
        if route.is_empty() {
            continue;
        }
        if !KNOWN_ROUTES.iter().any(|known| *known == route) {
            return Err(anyhow!(
                "COPYBOT_EXECUTOR_ROUTE_ALLOWLIST contains unsupported route={} (supported: paper,rpc,jito,fastlane)",
                route
            ));
        }
        routes.insert(route);
    }
    Ok(routes)
}

pub(crate) fn validate_fastlane_route_policy(
    route_allowlist: &HashSet<String>,
    submit_fastlane_enabled: bool,
) -> Result<()> {
    if !submit_fastlane_enabled {
        for route in route_allowlist {
            if requires_submit_fastlane_enabled(route.as_str()) {
                return Err(anyhow!(
                    "COPYBOT_EXECUTOR_ROUTE_ALLOWLIST includes fastlane but COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED is false"
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{parse_route_allowlist, validate_fastlane_route_policy};

    #[test]
    fn route_allowlist_parse_accepts_known_routes() {
        let routes =
            parse_route_allowlist(" rpc , JITO , fastlane ".to_string()).expect("must parse");
        assert!(routes.contains("rpc"));
        assert!(routes.contains("jito"));
        assert!(routes.contains("fastlane"));
    }

    #[test]
    fn route_allowlist_parse_rejects_unknown_route() {
        let error = parse_route_allowlist("rpc,unknown".to_string())
            .expect_err("unknown route must be rejected");
        assert!(
            error
                .to_string()
                .contains("contains unsupported route=unknown"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn route_allowlist_fastlane_policy_rejects_when_disabled() {
        let routes = parse_route_allowlist("rpc,fastlane".to_string()).expect("must parse");
        let error = validate_fastlane_route_policy(&routes, false)
            .expect_err("fastlane requires explicit feature flag");
        assert!(
            error
                .to_string()
                .contains("COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED is false"),
            "unexpected error: {}",
            error
        );
    }
}
