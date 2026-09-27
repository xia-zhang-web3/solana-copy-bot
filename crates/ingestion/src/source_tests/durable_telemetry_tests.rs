use crate::TransportClass;
use anyhow::Context;
use std::io::{Error, ErrorKind};

#[test]
fn transport_error_classes_follow_typed_causes_without_messages() {
    for (kind, expected) in [
        (ErrorKind::ConnectionRefused, TransportClass::Unavailable),
        (ErrorKind::ConnectionReset, TransportClass::Unavailable),
        (ErrorKind::TimedOut, TransportClass::Deadline),
        (ErrorKind::Other, TransportClass::Other),
    ] {
        let error = Err::<(), _>(Error::new(kind, "private endpoint and token"))
            .context("private URL")
            .unwrap_err();
        assert_eq!(TransportClass::error(&error), expected);
    }
    let status = anyhow::Error::new(tonic::Status::resource_exhausted("private body"));
    assert_eq!(TransportClass::error(&status), TransportClass::Resource);
}
