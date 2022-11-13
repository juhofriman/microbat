use std::io::{Read, Write};
use std::net::TcpStream;
use std::str;

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

const MSG_TYPE_HANDSHAKE: u8 = b'a';
const MSG_TYPE_QUERY: u8 = b'q';
const MSG_TYPE_ERROR: u8 = b'e';
const MSG_TYPE_DISCONNECT: u8 = b'd';

#[derive(PartialEq, Debug)]
pub enum MicrobatMessages {
    ClientHandshake,
    Query(String),
    Disconnect,
    Error(String),
}

impl MicrobatMessages {
    pub fn send(
        &self,
        stream: &mut (impl Read + Write + Unpin),
    ) -> Result<usize, MicrobatProtocolError> {
        return match self {
            MicrobatMessages::ClientHandshake => {
                let payload = "microbat-client";
                stream.write(&[MSG_TYPE_HANDSHAKE])?;
                stream.write(&[payload.len() as u8])?;
                stream.write(payload.as_bytes())?;
                Ok(1)
            }
            MicrobatMessages::Query(payload) => {
                stream.write(&[MSG_TYPE_QUERY])?;
                stream.write(&[payload.len() as u8])?;
                stream.write(payload.as_bytes())?;
                Ok(1)
            }
            MicrobatMessages::Disconnect => {
                let payload = "bye";
                stream.write(&[MSG_TYPE_DISCONNECT])?;
                stream.write(&[payload.len() as u8])?;
                stream.write(payload.as_bytes())?;
                Ok(1)
            }
            MicrobatMessages::Error(payload) => {
                stream.write(&[MSG_TYPE_ERROR])?;
                stream.write(&[payload.len() as u8])?;
                stream.write(payload.as_bytes())?;
                Ok(1)
            }
        };
    }
}

pub fn read_message(stream: &mut (impl Read + Write + Unpin)) -> MicrobatMessages {
    let mut message_type = [b'\0'];
    stream.read(&mut message_type).unwrap();
    match message_type[0] {
        MSG_TYPE_HANDSHAKE => {
            let mut length = [b'\0'];
            stream.read(&mut length).unwrap();
            let mut message_buffer = vec![0; length[0] as usize];
            stream.read_exact(&mut message_buffer).unwrap();
            MicrobatMessages::ClientHandshake
        }
        MSG_TYPE_QUERY => {
            let mut length = [b'\0'];
            stream.read(&mut length).unwrap();
            let mut message_buffer = vec![0; length[0] as usize];
            stream.read_exact(&mut message_buffer).unwrap();
            MicrobatMessages::Query(String::from_utf8(message_buffer.clone()).unwrap())
        }
        MSG_TYPE_ERROR => {
            let mut length = [b'\0'];
            stream.read(&mut length).unwrap();
            let mut message_buffer = vec![0; length[0] as usize];
            stream.read_exact(&mut message_buffer).unwrap();
            MicrobatMessages::Error(String::from_utf8(message_buffer.clone()).unwrap())
        }
        MSG_TYPE_DISCONNECT => {
            let mut length = [b'\0'];
            stream.read(&mut length).unwrap();
            let mut message_buffer = vec![0; length[0] as usize];
            stream.read_exact(&mut message_buffer).unwrap();
            MicrobatMessages::Disconnect
        }
        m => {
            panic!("Unknown message {}", m);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn test_handshake() {
        let mut serialize_stream = MockTcpStream {
            read_data: vec![],
            write_data: vec![],
        };
        MicrobatMessages::ClientHandshake
            .send(&mut serialize_stream)
            .unwrap();
        println!("{:?}", serialize_stream.write_data);
        assert_eq!(serialize_stream.write_data[0], MSG_TYPE_HANDSHAKE);
        assert_eq!(serialize_stream.write_data[1], 15);
        assert!(
            serialize_stream.write_data.len() > 2,
            "Looks like the data was not written to the stream"
        );

        let mut deserialize_stream = MockTcpStream {
            read_data: serialize_stream.write_data.clone(),
            write_data: vec![],
        };
        let response = read_message(&mut deserialize_stream);
        assert_eq!(response, MicrobatMessages::ClientHandshake);
    }
}
