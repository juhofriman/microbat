use crate::{static_values as values, MicrobatMessage, MicrobatProtocolError};

/// Enum of messages that can originate from the client
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

#[cfg(test)]
mod client_message_tests {

    use super::*;
    use crate::serialization_test_util::assert_serialisation;

    #[test]
    fn test_client_handshake_deserialization() {
        let handshake_bytes = MicrobatClientMessage::Handshake.as_bytes();
        let length = u32::from_le_bytes(handshake_bytes[1..5].try_into().unwrap()) as usize;
        let deserialized =
            deserialize_client_message(handshake_bytes[0], length, &handshake_bytes[5..]).unwrap();
        assert_eq!(deserialized, MicrobatClientMessage::Handshake);
    }

    #[test]
    fn test_client_disconnect_deserialization() {
        let disconnect_bytes = MicrobatClientMessage::Disconnect.as_bytes();
        let length = u32::from_le_bytes(disconnect_bytes[1..5].try_into().unwrap()) as usize;
        println!("length: {}", length);
        let deserialized =
            deserialize_client_message(disconnect_bytes[0], length, &disconnect_bytes[5..])
                .unwrap();
        assert_eq!(deserialized, MicrobatClientMessage::Disconnect);
    }

    #[test]
    fn test_client_query_deserialization() {
        let query = "hello world!";
        let query_bytes = MicrobatClientMessage::Query(String::from(query)).as_bytes();
        let length = u32::from_le_bytes(query_bytes[1..5].try_into().unwrap()) as usize;
        let deserialized =
            deserialize_client_message(query_bytes[0], length, &query_bytes[5..]).unwrap();
        assert_eq!(
            deserialized,
            MicrobatClientMessage::Query(String::from("hello world!"))
        );
        match deserialized {
            MicrobatClientMessage::Query(deserialized_query) => {
                assert_eq!(deserialized_query, query)
            }
            _ => panic!("This shouldn't happen as deserialized is asserted to be Query"),
        }
    }

    #[test]
    fn test_client_message_serialisation() {
        assert_serialisation(
            "client handshake",
            MicrobatClientMessage::Handshake.as_bytes(),
            values::CLIENT_MSG_TYPE_HANDSHAKE,
            values::CLIENT_HANDSHAKE_PAYLOAD.len(),
            Some(values::CLIENT_HANDSHAKE_PAYLOAD),
        );
        assert_serialisation(
            "client disconnect",
            MicrobatClientMessage::Disconnect.as_bytes(),
            values::CLIENT_MSG_TYPE_DISCONNECT,
            values::CLIENT_DISCONNECT_PAYLOAD.len(),
            Some(values::CLIENT_DISCONNECT_PAYLOAD),
        );
        assert_serialisation(
            "client query",
            MicrobatClientMessage::Query(String::from("abba")).as_bytes(),
            values::CLIENT_MSG_TYPE_QUERY,
            4,
            Some("abba"),
        );
        assert_serialisation(
            "client query",
            MicrobatClientMessage::Query(String::from("abba kabba")).as_bytes(),
            values::CLIENT_MSG_TYPE_QUERY,
            10,
            Some("abba kabba"),
        );
    }

    #[test]
    fn test_invalid_client_deserialization() {
        assert!(deserialize_client_message(b'\0', 0, &[]).is_err());
        assert!(deserialize_client_message(b'h', 0, &[]).is_err());
        assert!(deserialize_client_message(values::CLIENT_MSG_TYPE_HANDSHAKE, 0, &[b't']).is_err());
        assert!(deserialize_client_message(values::CLIENT_MSG_TYPE_HANDSHAKE, 5, &[b't']).is_err());
        assert!(deserialize_client_message(values::CLIENT_MSG_TYPE_QUERY, 2, &[0, 159]).is_err());
    }

    #[test]
    fn test_deserialization_fails_if_length_and_bytes_do_not_match() {
        assert!(deserialize_client_message(values::CLIENT_MSG_TYPE_QUERY, 5, &[b'0', 1]).is_err());
        assert!(deserialize_client_message(values::CLIENT_MSG_TYPE_QUERY, 5, &[b'0', 10]).is_err());
    }

    #[test]
    fn test_deserialization_fails_for_unknown_marker_bytes() {
        assert!(
            deserialize_client_message(values::SERVER_MSG_TYPE_READY_FOR_QUERY, 5, &[b'0', 5])
                .is_err()
        );
        assert!(
            deserialize_client_message(values::SERVER_MSG_TYPE_HANDSHAKE, 5, &[b'0', 5]).is_err()
        );
    }
}
