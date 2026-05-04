fn poll_observed_swap_writer_downstream_startups(
    aggregate_startup_receiver: &mut Option<std_mpsc::Receiver<std::result::Result<(), String>>>,
    journal_startup_receiver: &mut Option<std_mpsc::Receiver<std::result::Result<(), String>>>,
) -> Result<()> {
    poll_observed_swap_writer_startup_receiver(
        aggregate_startup_receiver,
        "observed swap writer stopping after aggregate startup replay failure",
        "discovery aggregate writer startup channel closed",
        "observed swap writer stopping after aggregate startup replay channel closed",
    )?;
    poll_observed_swap_writer_startup_receiver(
        journal_startup_receiver,
        "observed swap writer stopping after recent raw journal startup failure",
        "recent raw journal writer startup channel closed",
        "observed swap writer stopping after recent raw journal startup channel closed",
    )
}

fn poll_observed_swap_writer_startup_receiver(
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

fn observed_swap_writer_downstream_startup_pending(
    aggregate_startup_receiver: &Option<std_mpsc::Receiver<std::result::Result<(), String>>>,
    journal_startup_receiver: &Option<std_mpsc::Receiver<std::result::Result<(), String>>>,
) -> bool {
    aggregate_startup_receiver.is_some() || journal_startup_receiver.is_some()
}
