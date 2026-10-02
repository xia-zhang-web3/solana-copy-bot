use std::{future::Future, io};

pub(super) fn listen() -> io::Result<impl Future<Output = io::Result<()>>> {
    // Register synchronously: a ready select branch may enter an await before
    // this future's first poll. Keep its receiver for the whole app loop.
    #[cfg(unix)]
    let mut listener = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    #[cfg(windows)]
    let mut listener = tokio::signal::windows::ctrl_c()?;
    Ok(async move {
        listener.recv().await;
        Ok(())
    })
}
