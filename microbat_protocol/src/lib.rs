extern crate core;

pub mod client_messages;
pub mod server_messages;
mod static_values;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::str;
use std::string::FromUtf8Error;

fn read_message_type(
    stream: &mut (impl Read + Write + Unpin),
) -> Result<u8, MicrobatProtocolError> {
    let mut message_type = [b'\0'];
    stream.read(&mut message_type)?;
    Ok(message_type[0])
}

pub trait MicrobatMessage {
    fn str_with_length(&self, payload: &str) -> Vec<u8> {
        let mut bytes: Vec<u8> = vec![];
        bytes.append(&mut (payload.len() as u32).to_le_bytes().to_vec());
        bytes.append(&mut payload.as_bytes().to_vec());
        bytes
    }

    fn as_bytes(&self) -> Vec<u8>;

    fn send(
        &self,
        stream: &mut (impl Read + Write + Unpin),
    ) -> Result<usize, MicrobatProtocolError> {
        let bytes = self.as_bytes();
        println!(
            "Sending {} bytes, msgId: {}",
            bytes.len(),
            char::from(bytes[0])
        );
        stream.write(bytes.as_slice())?;
        Ok(bytes.len())
    }
}

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

#[derive(PartialEq, Debug)]
pub struct DataColumns {
    pub columns: Vec<Data>,
}

#[derive(PartialEq, Debug)]
pub enum Data {
    Integer(u32),
    Varchar(String),
}

impl Data {
    fn bytes(&self) -> Vec<u8> {
        match self {
            Data::Varchar(value) => value.as_bytes().to_vec(),
            Data::Integer(value) => value.to_be_bytes().to_vec(),
        }
    }
    fn length_and_bytes(&self) -> Vec<u8> {
        let mut bytes = self.bytes();
        bytes.insert(0, bytes.len() as u8);
        bytes
    }
}

#[derive(PartialEq, Debug)]
pub struct RowDescription {
    pub rows: Vec<Column>,
}

#[derive(PartialEq, Debug)]
pub struct Column {
    pub name: String,
}

pub fn read_message<T>(
    stream: &mut (impl Read + Write + Unpin),
    deserializer: fn(u8, usize, &[u8]) -> Result<T, MicrobatProtocolError>,
) -> Result<T, MicrobatProtocolError> {
    let message_type = read_message_type(stream)?;
    if message_type == b'\0' {
        println!("Received null byte");
        return Err(MicrobatProtocolError {
            msg: String::from("unexpected hangup"),
        });
    }
    let mut length_bytes = [b'\0', b'\0', b'\0', b'\0'];
    stream.read_exact(&mut length_bytes)?;
    let length = u32::from_le_bytes(length_bytes) as usize;
    let mut message_buffer = vec![0; length];
    stream.read_exact(&mut message_buffer).unwrap();

    println!(
        "Reading {} bytes, msgId: {}",
        message_buffer.len() + 1 + 4,
        char::from(message_type)
    );

    deserializer(message_type, length, message_buffer.as_slice())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_messages::{deserialize_client_message, MicrobatClientMessage};
    use std::cmp::min;
    use std::error::Error;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    struct MockTcpStream {
        read_data: Vec<u8>,
        write_data: Vec<u8>,
    }

    impl Read for MockTcpStream {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let size: usize = min(self.read_data.len(), buf.len());
            buf[..size].copy_from_slice(&self.read_data[..size]);
            Ok(size)
        }
    }

    impl Write for MockTcpStream {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.write_data.append(&mut Vec::from(buf));
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_handshake_via_mock_stream() {
        let mut write_stream = MockTcpStream {
            read_data: vec![],
            write_data: vec![],
        };
        MicrobatClientMessage::Handshake
            .send(&mut write_stream)
            .unwrap();
        assert!(write_stream.write_data.len() > 0);

        let mut read_stream = MockTcpStream {
            read_data: write_stream.write_data,
            write_data: vec![],
        };

        let result = read_message(&mut read_stream, deserialize_client_message);
        assert!(result.is_ok());
        match result.unwrap() {
            MicrobatClientMessage::Handshake => (),
            value => panic!("Expecting Handshake but got {:?}", value),
        }
    }
}

#[cfg(test)]
mod serialization_tests {
    use super::*;
    use crate::client_messages::{deserialize_client_message, MicrobatClientMessage};
    use crate::server_messages::{deserialize_server_message, MicrobatServerMessage};
    use crate::static_values as values;

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
    fn test_server_message_serialisation() {
        assert_serialisation(
            "server handshake",
            MicrobatServerMessage::Handshake.as_bytes(),
            values::SERVER_MSG_TYPE_HANDSHAKE,
            values::SERVER_HANDSHAKE_PAYLOAD.len(),
            Some(values::SERVER_HANDSHAKE_PAYLOAD),
        );
        assert_serialisation(
            "server ready",
            MicrobatServerMessage::Ready.as_bytes(),
            values::SERVER_MSG_TYPE_READY_FOR_QUERY,
            values::SERVER_READY_PAYLOAD.len(),
            Some(values::SERVER_READY_PAYLOAD),
        );
        assert_serialisation(
            "server error",
            MicrobatServerMessage::Error(String::from("error")).as_bytes(),
            values::SERVER_MSG_TYPE_ERROR,
            5,
            Some("error"),
        );
        assert_serialisation(
            "server row description",
            MicrobatServerMessage::RowDescription(RowDescription {
                rows: vec![Column {
                    name: String::from("foo"),
                }],
            })
            .as_bytes(),
            values::SERVER_MSG_TYPE_ROW_DESCRIPTION,
            7, // We just know this..
            None,
        );
        assert_serialisation(
            "server row description",
            MicrobatServerMessage::DataRow(DataColumns {
                columns: vec![Data::Varchar(String::from("foo"))],
            })
            .as_bytes(),
            values::SERVER_MSG_TYPE_DATA_ROW,
            7, // We just know this..
            None,
        );
    }

    #[test]
    fn test_invalid_client_deserialization() {
        assert!(deserialize_client_message(b'\0', 0, &[]).is_err());
        assert!(deserialize_client_message(b'x', 0, &[]).is_err());
        assert!(deserialize_client_message(values::CLIENT_MSG_TYPE_HANDSHAKE, 0, &[b't']).is_err());
        assert!(deserialize_client_message(values::CLIENT_MSG_TYPE_HANDSHAKE, 5, &[b't']).is_err());
        assert!(deserialize_client_message(values::CLIENT_MSG_TYPE_QUERY, 2, &[0, 159]).is_err());
    }

    #[test]
    fn test_invalid_server_deserialization() {
        assert!(deserialize_server_message(b'\0', 0, &[]).is_err());
        assert!(deserialize_server_message(b'x', 0, &[]).is_err());
        assert!(deserialize_server_message(values::SERVER_MSG_TYPE_HANDSHAKE, 0, &[b't']).is_err());
        assert!(deserialize_server_message(values::SERVER_MSG_TYPE_HANDSHAKE, 5, &[b't']).is_err());
        assert!(deserialize_server_message(values::SERVER_MSG_TYPE_ERROR, 2, &[0, 159]).is_err());
    }

    #[test]
    fn test_client_handshake_deserialisation() {
        let handshake_bytes = MicrobatClientMessage::Handshake.as_bytes();
        let length = u32::from_le_bytes(handshake_bytes[1..5].try_into().unwrap()) as usize;
        let deserialized =
            deserialize_client_message(handshake_bytes[0], length, &handshake_bytes[5..]).unwrap();
        assert_eq!(deserialized, MicrobatClientMessage::Handshake);
    }

    #[test]
    fn test_client_disconnect_deserialisation() {
        let disconnect_bytes = MicrobatClientMessage::Disconnect.as_bytes();
        let length = u32::from_le_bytes(disconnect_bytes[1..5].try_into().unwrap()) as usize;
        println!("length: {}", length);
        let deserialized =
            deserialize_client_message(disconnect_bytes[0], length, &disconnect_bytes[5..])
                .unwrap();
        assert_eq!(deserialized, MicrobatClientMessage::Disconnect);
    }

    #[test]
    fn test_client_query_deserialisation() {
        let query = "hello world!";
        let query_bytes = MicrobatClientMessage::Query(String::from(query)).as_bytes();
        let length = u32::from_le_bytes(query_bytes[1..5].try_into().unwrap()) as usize;
        println!("length: {}", length);
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
    fn test_server_handshake_deserialisation() {
        let handshake_bytes = MicrobatServerMessage::Handshake.as_bytes();
        let length = u32::from_le_bytes(handshake_bytes[1..5].try_into().unwrap()) as usize;
        let deserialized =
            deserialize_server_message(handshake_bytes[0], length, &handshake_bytes[5..]).unwrap();
        assert_eq!(deserialized, MicrobatServerMessage::Handshake);
    }

    // TODO: cleanly assert all serialize->deserialize streams...

    #[test]
    fn test_server_datarow_deserialisation() {
        let data_row = DataColumns {
            columns: vec![Data::Varchar(String::from("hello"))],
        };
        let message_bytes = MicrobatServerMessage::DataRow(data_row).as_bytes();
        let length = u32::from_le_bytes(message_bytes[1..5].try_into().unwrap()) as usize;
        let deserialized =
            deserialize_server_message(message_bytes[0], length, &message_bytes[5..]).unwrap();
        let expected_data_row = DataColumns {
            columns: vec![Data::Varchar(String::from("hello"))],
        };
        assert_eq!(
            deserialized,
            MicrobatServerMessage::DataRow(expected_data_row)
        );
    }

    fn assert_serialisation(
        message_name_for_failures: &str,
        bytes: Vec<u8>,
        expected_message_type: u8,
        expected_length: usize,
        check_payload: Option<&str>,
    ) {
        assert_eq!(
            bytes[0], expected_message_type,
            "{} did not contain expected message type {} as the first byte. Was: {}",
            message_name_for_failures, expected_message_type, bytes[0]
        );
        let length = u32::from_le_bytes(bytes[1..5].try_into().unwrap()) as usize;
        assert_eq!(length, expected_length, "Expected length did not match.");
        assert_eq!(
            length,
            bytes[5..].len(),
            "{} did not contain exactly length amount of bytes after the length.",
            message_name_for_failures
        );
        if let Some(expected_payload) = check_payload {
            let deserialized = String::from_utf8(bytes[5..].to_vec()).unwrap();
            assert_eq!(
                expected_payload, deserialized,
                "{} did not deserialize as expected.",
                message_name_for_failures
            );
        }
    }
}
