impl ShadowService {
    pub fn new(config: ShadowConfig) -> Self {
        Self {
            copy_notional_lamports: sol_to_lamports_floor(config.copy_notional_sol),
            min_leader_notional_lamports: sol_to_lamports_ceil(config.min_leader_notional_sol),
            config,
            helius_http_url: None,
        }
    }

    pub fn new_with_helius(config: ShadowConfig, helius_http_url: Option<String>) -> Self {
        let helius_http_url = helius_http_url
            .map(|url| url.trim().to_string())
            .filter(|url| !url.is_empty() && !url.contains("REPLACE_ME"));
        Self {
            copy_notional_lamports: sol_to_lamports_floor(config.copy_notional_sol),
            min_leader_notional_lamports: sol_to_lamports_ceil(config.min_leader_notional_sol),
            config,
            helius_http_url,
        }
    }
}
