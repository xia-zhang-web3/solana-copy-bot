#[tokio::main]
async fn main() {
    std::process::exit(copybot_operators::yellowstone_probe::run_from_env().await);
}
