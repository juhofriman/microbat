use crate::{
    data_representation::*, read_message_type, static_values as values, MicrobatMessage,
    MicrobatProtocolError,
};
use std::io::{Read, Write};

#[derive(Debug, PartialEq)]
pub enum MicrobatClientMessage {
    Handshake,
    Query(String),
    Disconnect,
}

impl MicrobatMessage for MicrobatClientMessage {
    fn as_bytes(&self) -> Vec<u8> {
        match self {
            MicrobatClientMessage::Handshake => {
                let mut bytes: Vec<u8> = vec![];
                bytes.push(values::CLIENT_MSG_TYPE_HANDSHAKE);
                bytes.append(&mut self.str_with_length(values::CLIENT_HANDSHAKE_PAYLOAD));
                bytes
            }
            MicrobatClientMessage::Disconnect => {
                let mut bytes: Vec<u8> = vec![];
                bytes.push(values::CLIENT_MSG_TYPE_DISCONNECT);
                bytes.append(&mut self.str_with_length(values::CLIENT_DISCONNECT_PAYLOAD));
                bytes
            }
            MicrobatClientMessage::Query(query) => {
                let mut bytes: Vec<u8> = vec![];
                bytes.push(values::CLIENT_MSG_TYPE_QUERY);
                bytes.append(&mut self.str_with_length(query));
                bytes
            }
        }
    }
}

pub fn deserialize_client_message(
    message_type: u8,
    length: usize,
    bytes: &[u8],
) -> Result<MicrobatClientMessage, MicrobatProtocolError> {
    if length != bytes.len() {
        return Err(MicrobatProtocolError {
            msg: format!(
                "Byte mismatch error. Expecting {} bytes but received {} bytes",
                length,
                bytes.len()
            ),
        });
    }
    match message_type {
        values::CLIENT_MSG_TYPE_HANDSHAKE => Ok(MicrobatClientMessage::Handshake),
        values::CLIENT_MSG_TYPE_DISCONNECT => Ok(MicrobatClientMessage::Disconnect),
        values::CLIENT_MSG_TYPE_QUERY => Ok(MicrobatClientMessage::Query(String::from_utf8(
            bytes.to_vec(),
        )?)),
        unknown => Err(MicrobatProtocolError {
            msg: format!(
                "Received unknown message type: {} (ascii: {})",
                unknown,
                char::from(unknown)
            ),
        }),
    }
}
