use crate::render_result::{
    MutationKind, QueryExecutionResult, RenderableMutationResult, RenderableQueryResult,
};
use microbat_protocol::client_messages::MicrobatClientMessage;
use microbat_protocol::data::MData;
use microbat_protocol::protocol_error::MicrobatProtocolError;
use microbat_protocol::server_messages::{deserialize_server_message, MicrobatServerMessage};
use microbat_protocol::{read_message, MicrobatMessage};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Instant;

#[derive(Debug)]
pub struct MicroBatClientError {
    pub msg: String,
}

impl From<MicrobatProtocolError> for MicroBatClientError {
    fn from(error: MicrobatProtocolError) -> Self {
        MicroBatClientError { msg: error.msg }
    }
}

/// Options for microbat client instance
pub struct MicrobatClientOpts {
    pub host: String,
    pub port: u32,
}

/// MicrobatTcpClient for communicating with microbat server
/// Use MicrobatTcpClient::connect(opts) to acquire working connection
pub struct MicroBatTcpClient {
    stream: TcpStream,
}

impl MicroBatTcpClient {
    /// Creates new connected socket to microbat instance
    /// Errors if TcpStream cannot be established or handshake is not succesfull
    pub fn connect(opts: MicrobatClientOpts) -> Result<Self, MicroBatClientError> {
        let connect_string = format!("{}:{}", opts.host, opts.port);
        println!("MICROBAT CLIENT");
        println!("connecting to {}", connect_string);
        println!();
        match TcpStream::connect(&connect_string) {
            Ok(stream) => {
                let mut client = MicroBatTcpClient { stream };
                match client.handshake() {
                    Ok(_) => {
                        println!("Handshake OK [{}]", client.describe());
                        Ok(client)
                    }
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(MicroBatClientError {
                msg: format!("Unable to connect {} [{}]", connect_string, err.to_string()),
            }),
        }
    }

    pub fn describe(&self) -> String {
        match self.stream.peer_addr() {
            Ok(address) => address.to_string(),
            Err(err) => format!("UNKNOWN [{}]", err.to_string()),
        }
    }

    pub fn handshake(&mut self) -> Result<(), MicroBatClientError> {
        MicrobatClientMessage::Handshake.send(&mut self.stream)?;
        read_handshake(&mut self.stream)?;
        read_ready(&mut self.stream)
    }

    pub fn disconnect(&mut self) -> Result<(), MicroBatClientError> {
        MicrobatClientMessage::Disconnect.send(&mut self.stream)?;
        Ok(())
    }
    pub fn query(&mut self, sql: String) -> Result<QueryExecutionResult, MicroBatClientError> {
        let start = Instant::now();

        MicrobatClientMessage::Query(sql).send(&mut self.stream)?;

        match read_message(&mut self.stream, deserialize_server_message)? {
            MicrobatServerMessage::DataDescription(data_description) => {
                let rows = read_data_rows_until_ready(&mut self.stream)?;
                Ok(QueryExecutionResult::DataTable(RenderableQueryResult::new(
                    data_description.columns,
                    rows,
                    start.elapsed(),
                )))
            }
            MicrobatServerMessage::InsertResult(rows) => {
                read_ready(&mut self.stream)?;
                Ok(QueryExecutionResult::Mutation(
                    RenderableMutationResult::new(MutationKind::INSERT, rows, start.elapsed()),
                ))
            }
            MicrobatServerMessage::Error(error) => {
                read_ready(&mut self.stream)?;
                Err(MicroBatClientError { msg: error })
            }
            message => Err(MicroBatClientError {
                msg: format!(
                    "Expecting 'DataDescription' from server but got '{}'",
                    message
                ),
            }),
        }
    }
}

fn read_handshake(stream: &mut (impl Read + Write + Unpin)) -> Result<(), MicroBatClientError> {
    match read_message(stream, deserialize_server_message)? {
        MicrobatServerMessage::Handshake => Ok(()),
        MicrobatServerMessage::Error(error) => Err(MicroBatClientError { msg: error }),
        message => Err(MicroBatClientError {
            msg: format!("Expecting 'Handshake' from server but got '{}'", message),
        }),
    }
}

fn read_ready(stream: &mut (impl Read + Write + Unpin)) -> Result<(), MicroBatClientError> {
    match read_message(stream, deserialize_server_message)? {
        MicrobatServerMessage::Ready => Ok(()),
        MicrobatServerMessage::Error(error) => Err(MicroBatClientError { msg: error }),
        message => Err(MicroBatClientError {
            msg: format!("Expecting 'Ready' from server but got '{}'", message),
        }),
    }
}

fn read_data_rows_until_ready(
    stream: &mut (impl Read + Write + Unpin),
) -> Result<Vec<Vec<MData>>, MicroBatClientError> {
    let mut rows: Vec<Vec<MData>> = vec![];
    loop {
        match read_message(stream, deserialize_server_message)? {
            MicrobatServerMessage::DataRow(row) => {
                rows.push(row.columns);
            }
            MicrobatServerMessage::Error(error) => return Err(MicroBatClientError { msg: error }),
            MicrobatServerMessage::Ready => return Ok(rows),
            message => {
                return Err(MicroBatClientError {
                    msg: format!("Expecting 'DataRow' from server but got '{}'", message),
                })
            }
        }
    }
}

#[cfg(test)]
mod expect_message_tests {
    use super::*;
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

    // #[test]
    // fn test_simple_read_expected() {
    //     let mut write_stream = MockTcpStream {
    //         read_data: vec![],
    //         write_data: vec![],
    //     };
    //     MicrobatServerMessage::Handshake
    //         .send(&mut write_stream)
    //         .unwrap();
    //     assert!(write_stream.write_data.len() > 0);
    //
    //     let mut read_stream = MockTcpStream {
    //         read_data: write_stream.write_data,
    //         write_data: vec![],
    //     };
    //
    //     let result = expect_message(&mut read_stream, MicrobatServerMessage::Handshake);
    //     assert!(result.is_ok());
    //     assert_eq!(result.unwrap(), MicrobatServerMessage::Handshake);
    // }
}
