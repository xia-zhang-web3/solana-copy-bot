#![cfg(unix)]

#[path = "../app_loop/shutdown_signal.rs"]
mod shutdown_signal;

use std::{
    io::{BufRead, BufReader, Write},
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};

struct ReapedChild(Child);

impl Drop for ReapedChild {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

fn assert_signal_boundary(mode: &str, ready: &str) {
    let child_test = module_path!()
        .split_once("::")
        .map(|(_, module)| format!("{module}::shutdown_signal_fixture_child"))
        .unwrap_or_else(|| "shutdown_signal_fixture_child".to_string());
    let mut child = ReapedChild(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &child_test,
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("COPYBOT_SHUTDOWN_FIXTURE_MODE", mode)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let stdout = child.0.stdout.take().unwrap();
    let (send, receive) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if send.send(line.unwrap()).is_err() {
                break;
            }
        }
    });
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut events = Vec::new();
    let mut sent = false;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "child timed out: {events:?}");
        match receive.recv_timeout(remaining) {
            Ok(line) => {
                if line == ready {
                    assert!(!sent, "duplicate readiness");
                    assert!(Command::new("kill")
                        .args(["-INT", &child.0.id().to_string()])
                        .status()
                        .unwrap()
                        .success());
                    sent = true;
                }
                events.push(line);
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(error) => panic!("child did not finish: {error}; {events:?}"),
        }
    }
    reader.join().unwrap();
    let status = child.0.wait().unwrap();
    assert!(status.success(), "child failed: {status}; {events:?}");
    assert!(sent, "signal was not sent after the selected boundary");
    assert!(events.iter().any(|line| line == "SIGNAL_OBSERVED"));
    assert!(events.iter().any(|line| line == "MAIN_LISTENER_RECEIVED"));
    if mode == "busy" {
        assert!(events
            .iter()
            .any(|line| line == "SIGNAL_OBSERVED_DURING_AWAIT"));
    }
    println!("mode={mode}, signal_sent_after={ready}, events={events:?}, child_reaped=true");
}

#[test]
fn shutdown_listener_receives_sigint_during_selected_await_before_first_poll() {
    assert_signal_boundary("busy", "SELECTED_BRANCH");
}

#[test]
fn shutdown_listener_receives_ordinary_sigint() {
    assert_signal_boundary("ordinary", "LISTENER_READY");
}

fn announce(event: &str) {
    println!("\n{event}");
    std::io::stdout().flush().unwrap();
}

#[test]
#[ignore = "subprocess fixture; parent sends SIGINT at the selected boundary"]
fn shutdown_signal_fixture_child() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let stop = shutdown_signal::listen().unwrap();
            tokio::pin!(stop);
            let mut observer =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).unwrap();
            let mut observed_during_await = false;
            match std::env::var("COPYBOT_SHUTDOWN_FIXTURE_MODE")
                .unwrap()
                .as_str()
            {
                "busy" => {
                    tokio::select! {
                        biased;
                        _ = std::future::ready(()) => {
                            announce("SELECTED_BRANCH");
                            tokio::time::timeout(Duration::from_millis(500), observer.recv())
                                .await.unwrap().unwrap();
                            observed_during_await = true;
                            announce("SIGNAL_OBSERVED_DURING_AWAIT");
                        }
                        result = &mut stop => panic!("premature shutdown: {result:?}"),
                    }
                }
                "ordinary" => announce("LISTENER_READY"),
                mode => panic!("unknown fixture mode: {mode}"),
            }
            tokio::time::timeout(Duration::from_millis(500), &mut stop)
                .await
                .expect("SIGINT was lost")
                .unwrap();
            if !observed_during_await {
                tokio::time::timeout(Duration::from_millis(100), observer.recv())
                    .await
                    .unwrap()
                    .unwrap();
            }
            announce("SIGNAL_OBSERVED");
            announce("MAIN_LISTENER_RECEIVED");
        });
}
