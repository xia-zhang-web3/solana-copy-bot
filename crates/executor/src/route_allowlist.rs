use std::collections::HashSet;

use anyhow::{anyhow, Result};

use crate::{route_normalization::normalize_route, route_policy::requires_submit_fastlane_enabled};

const KNOWN_ROUTES: &[&str] = &["paper", "rpc", "jito", "fastlane"];

fn levenshtein_distance(left: &str, right: &str) -> usize {
    if left == right {
        return 0;
    }
    let right_len = right.chars().count();
    if right_len == 0 {
        return left.chars().count();
    }
    let left_len = left.chars().count();
    if left_len == 0 {
        return right_len;
    }
    let mut prev: Vec<usize> = (0..=right_len).collect();
    let mut curr = vec![0usize; right_len + 1];
    for (i, left_char) in left.chars().enumerate() {
        curr[0] = i + 1;
        for (j, right_char) in right.chars().enumerate() {
            let cost = usize::from(left_char != right_char);
            curr[j + 1] = (prev[j + 1] + 1).min(curr[j] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[right_len]
}

fn known_route_suggestion(route: &str) -> Option<&'static str> {
    let mut best: Option<(&str, usize)> = None;
    for known in KNOWN_ROUTES {
        let distance = levenshtein_distance(route, known);
        match best {
            Some((_, best_distance)) if best_distance <= distance => {}
            _ => best = Some((known, distance)),
        }
    }
    best.and_then(|(known, distance)| {
        let threshold = (route.len().max(known.len()) / 4).clamp(1, 3);
        if distance <= threshold {
            Some(known)
        } else {
            None
        }
    })
}

pub(crate) fn sorted_routes(route_allowlist: &HashSet<String>) -> Vec<String> {
    let mut routes: Vec<String> = route_allowlist.iter().cloned().collect();
    routes.sort_unstable();
    routes
}

pub(crate) fn parse_route_allowlist(csv: String) -> Result<HashSet<String>> {
    let mut routes = HashSet::new();
    for raw in csv.split(',') {
        let route = normalize_route(raw);
        if route.is_empty() {
            return Err(anyhow!(
                "COPYBOT_EXECUTOR_ROUTE_ALLOWLIST contains empty route entry"
            ));
        }
        if !KNOWN_ROUTES.iter().any(|known| *known == route) {
            let suggestion = known_route_suggestion(route.as_str());
            return Err(anyhow!(
                "COPYBOT_EXECUTOR_ROUTE_ALLOWLIST contains unsupported route={}{} (supported: paper,rpc,jito,fastlane)",
                route,
                suggestion
                    .map(|value| format!(" (did you mean route={}?)", value))
                    .unwrap_or_default()
            ));
        }
        if routes.contains(route.as_str()) {
            return Err(anyhow!(
                "COPYBOT_EXECUTOR_ROUTE_ALLOWLIST contains duplicate route={}",
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
    if !submit_fastlane_enabled
        && route_allowlist
            .iter()
            .any(|route| requires_submit_fastlane_enabled(route.as_str()))
    {
        return Err(anyhow!(
            "COPYBOT_EXECUTOR_ROUTE_ALLOWLIST includes fastlane but COPYBOT_EXECUTOR_SUBMIT_FASTLANE_ENABLED is false (allowlist={})",
            sorted_routes(route_allowlist).join(",")
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{parse_route_allowlist, sorted_routes, validate_fastlane_route_policy};

    #[test]
    fn route_allowlist_parse_accepts_known_routes() {
        let routes =
            parse_route_allowlist(" rpc , JITO , fastlane ".to_string()).expect("must parse");
        assert!(routes.contains("rpc"));
        assert!(routes.contains("jito"));
        assert!(routes.contains("fastlane"));
    }

    #[test]
    fn parse_route_allowlist_normalizes() {
        let routes = parse_route_allowlist("RPC, jito ,fastlane".to_string()).expect("must parse");
        assert!(routes.contains("rpc"));
        assert!(routes.contains("jito"));
        assert!(routes.contains("fastlane"));
        assert_eq!(routes.len(), 3);
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
    fn parse_route_allowlist_rejects_unknown_route() {
        let error = parse_route_allowlist("rpc,unknown_route".to_string())
            .expect_err("unknown route must fail closed");
        assert!(
            error
                .to_string()
                .contains("unsupported route=unknown_route"),
            "error={}",
            error
        );
        assert!(
            !error.to_string().contains("did you mean route="),
            "error={}",
            error
        );
    }

    #[test]
    fn parse_route_allowlist_rejects_unknown_route_with_suggestion() {
        let error = parse_route_allowlist("rpc,faslane".to_string())
            .expect_err("typo route must fail closed with suggestion");
        assert!(
            error
                .to_string()
                .contains("unsupported route=faslane"),
            "error={}",
            error
        );
        assert!(
            error
                .to_string()
                .contains("did you mean route=fastlane?"),
            "error={}",
            error
        );
    }

    #[test]
    fn parse_route_allowlist_rejects_duplicate_route() {
        let error = parse_route_allowlist("rpc, RPC".to_string())
            .expect_err("duplicate route entries must reject");
        assert!(
            error
                .to_string()
                .contains("contains duplicate route=rpc"),
            "error={}",
            error
        );
    }

    #[test]
    fn parse_route_allowlist_rejects_empty_route_entry() {
        let error = parse_route_allowlist("rpc,".to_string())
            .expect_err("empty route entry must fail closed");
        assert!(
            error
                .to_string()
                .contains("contains empty route entry"),
            "error={}",
            error
        );
    }

    #[test]
    fn sorted_routes_returns_deterministic_order() {
        let routes = parse_route_allowlist("rpc, jito, paper".to_string()).expect("must parse");
        assert_eq!(sorted_routes(&routes), vec!["jito", "paper", "rpc"]);
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
        assert!(
            error.to_string().contains("allowlist=fastlane,rpc"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn validate_fastlane_route_policy_enforces_feature_gate() {
        let routes = parse_route_allowlist("rpc,fastlane".to_string()).expect("must parse");
        assert!(validate_fastlane_route_policy(&routes, false).is_err());
        assert!(validate_fastlane_route_policy(&routes, true).is_ok());
    }
}
