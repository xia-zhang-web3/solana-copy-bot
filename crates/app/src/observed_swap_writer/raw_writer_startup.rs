use super::*;

pub(in crate::observed_swap_writer) fn poll_observed_swap_writer_downstream_startups(
    journal_startup_receiver: &mut Option<std_mpsc::Receiver<std::result::Result<(), String>>>,
) -> Result<()> {
    poll_observed_swap_writer_startup_receiver(
        journal_startup_receiver,
        "observed swap writer stopping after recent raw journal startup failure",
        "recent raw journal writer startup channel closed",
        "observed swap writer stopping after recent raw journal startup channel closed",
    )
}

pub(in crate::observed_swap_writer) fn wait_observed_swap_writer_downstream_startup(
    startup_receiver: Option<std_mpsc::Receiver<std::result::Result<(), String>>>,
) -> Result<()> {
    let Some(receiver) = startup_receiver else {
        return Ok(());
    };
    match receiver.recv_timeout(OBSERVED_SWAP_RECENT_RAW_JOURNAL_STARTUP_TIMEOUT) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(message)) => {
            Err(anyhow!(message)).context("observed swap writer failed recent raw journal startup")
        }
        Err(std_mpsc::RecvTimeoutError::Timeout) => Err(anyhow!(
            "recent raw journal writer did not report startup within {:?}",
            OBSERVED_SWAP_RECENT_RAW_JOURNAL_STARTUP_TIMEOUT
        ))
        .context("observed swap writer failed recent raw journal startup"),
        Err(std_mpsc::RecvTimeoutError::Disconnected) => {
            Err(anyhow!("recent raw journal writer startup channel closed"))
                .context("observed swap writer failed recent raw journal startup")
        }
    }
}

pub(in crate::observed_swap_writer) fn poll_observed_swap_writer_startup_receiver(
    startup_receiver: &mut Option<std_mpsc::Receiver<std::result::Result<(), String>>>,
    failure_context: &'static str,
    closed_message: &'static str,
    closed_context: &'static str,
) -> Result<()> {
    let poll_result = match startup_receiver.as_ref() {
        Some(receiver) => receiver.try_recv(),
        None => return Ok(()),
    };
    match poll_result {
        Ok(Ok(())) => {
            *startup_receiver = None;
            Ok(())
        }
        Ok(Err(message)) => Err(anyhow!(message)).context(failure_context),
        Err(std_mpsc::TryRecvError::Empty) => Ok(()),
        Err(std_mpsc::TryRecvError::Disconnected) => {
            Err(anyhow!("{closed_message}: receiving on a closed channel")).context(closed_context)
        }
    }
}

pub(in crate::observed_swap_writer) fn observed_swap_writer_downstream_startup_pending(
    journal_startup_receiver: &Option<std_mpsc::Receiver<std::result::Result<(), String>>>,
) -> bool {
    journal_startup_receiver.is_some()
}
