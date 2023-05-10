use std::string::FromUtf8Error;

/// Error for describing protocol errors.
#[derive(Debug)]
pub struct MicrobatProtocolError {
    pub msg: String,
}

impl From<std::io::Error> for MicrobatProtocolError {
    fn from(err: std::io::Error) -> Self {
        MicrobatProtocolError {
            msg: err.to_string(),
        }
    }
}

impl From<FromUtf8Error> for MicrobatProtocolError {
    fn from(err: FromUtf8Error) -> Self {
        MicrobatProtocolError {
            msg: err.to_string(),
        }
    }
}
