extern crate core;

pub mod client_messages;
pub mod data_representation;
pub mod protocol_error;
pub mod server_messages;
mod static_values;

use crate::protocol_error::MicrobatProtocolError;
use std::io::{Read, Write};
use std::str;

/// Defines MicrobatMessage and offers utility methods for message deserialization and serialization.
///
/// Messages are separated in client_messages.rs and server_messages.rs and new message should be
/// constructed using ClientMessage and ServerMessage enums which implement this trait.
pub trait MicrobatMessage {
    /// Sends this message to given stream, which presumably is a TcpStream
    ///
    /// Technically this method can be overridden but in reality this implementation
    /// should be used.
    fn send(
        &self,
        stream: &mut (impl Read + Write + Unpin),
    ) -> Result<usize, MicrobatProtocolError> {
        let bytes = self.as_bytes();
        // println!(
        //     ">> Sending {} bytes, msgId: {}",
        //     bytes.len(),
        //     char::from(bytes[0])
        // );
        stream.write(bytes.as_slice())?;
        Ok(bytes.len())
    }

    /// Implementations must define how given message is serialized as bytes. The implementation
    /// must return the whole byte stream, i.e [MESSAGE_ID, LENGTH, ...BYTES_OF_LENGTH]
    fn as_bytes(&self) -> Vec<u8>;

    /// Utility method for serialising &str with length
    /// Returns [LENGTH, STR_BYTES]
    fn str_with_length(&self, payload: &str) -> Vec<u8> {
        let mut bytes: Vec<u8> = vec![];
        bytes.append(&mut (payload.len() as u32).to_le_bytes().to_vec());
        bytes.append(&mut payload.as_bytes().to_vec());
        bytes
    }
}

/// Reads message from given stream using given deserializer
///
/// Returns generic type of Result<T, MicrobatProtocolError> in which T
/// should be enum of client or server messages.
///
/// Client read_message should use server deserializer and vice versa.
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

    let length = read_message_length(stream)?;

    let mut message_buffer = vec![0; length];
    stream.read_exact(&mut message_buffer).unwrap();

    // println!(
    // ">> Reading {} bytes, msgId: {}",
    // message_buffer.len() + 1 + 4,
    // char::from(message_type)
    // );

    deserializer(message_type, length, message_buffer.as_slice())
}

/// Utility fn for reading next byte as message type.
fn read_message_type(
    stream: &mut (impl Read + Write + Unpin),
) -> Result<u8, MicrobatProtocolError> {
    let mut message_type = [b'\0'];
    stream.read(&mut message_type)?;
    Ok(message_type[0])
}

/// Utility fn for reading next four bytes as message length.
fn read_message_length(
    stream: &mut (impl Read + Write + Unpin),
) -> Result<usize, MicrobatProtocolError> {
    let mut length_bytes = [b'\0', b'\0', b'\0', b'\0'];
    stream.read_exact(&mut length_bytes)?;
    Ok(u32::from_le_bytes(length_bytes) as usize)
}

#[cfg(test)]
mod mocked_tcp_stream_tests {
    use super::*;
    use crate::client_messages::{deserialize_client_message, MicrobatClientMessage};
    use std::cmp::min;

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
mod serialization_test_util {
    use super::*;

    pub fn assert_serialisation(
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
