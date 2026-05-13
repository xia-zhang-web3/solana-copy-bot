fn main() {
    let exit_code = match copybot_discovery_ops::v2_watchdog::main_entry() {
        Ok(exit_code) => exit_code,
        Err(error) => {
            eprintln!("{error:?}");
            2
        }
    };
    std::process::exit(exit_code);
}
