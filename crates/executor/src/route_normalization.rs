pub(crate) fn normalize_route(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::normalize_route;

    #[test]
    fn normalize_route_trims_and_lowercases() {
        assert_eq!(normalize_route("  RPC "), "rpc");
        assert_eq!(normalize_route("Jito"), "jito");
    }
}
