#[path = "../export.rs"]
mod export;

fn main() {
    let options = match export::parse_export_args(std::env::args().skip(1)) {
        Ok(options) => options,
        Err(error) => {
            eprintln!("{error}");
            eprintln!("{}", export::usage());
            std::process::exit(2);
        }
    };
    match export::export_snapshots(options) {
        Ok(report) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report).expect("export report must serialize")
            );
        }
        Err(error) => {
            eprintln!("{error:#}");
            std::process::exit(1);
        }
    }
}
