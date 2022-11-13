use microbat_protocol::{read_message, MicrobatMessages, MicrobatProtocolError};
use rustyline::error::ReadlineError;
use rustyline::{Editor, Result};
use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::str;

struct MicroBatTcpClient {
    stream: TcpStream,
}

#[derive(Debug)]
struct MicroBatClientError {
    msg: String,
}

impl From<MicrobatProtocolError> for MicroBatClientError {
    fn from(error: MicrobatProtocolError) -> Self {
        MicroBatClientError { msg: error.msg }
    }
}

impl MicroBatTcpClient {
    fn handshake(&mut self) -> std::result::Result<(), MicroBatClientError> {
        MicrobatMessages::ClientHandshake.send(&mut self.stream)?;
        match read_message(&mut self.stream) {
            MicrobatMessages::ClientHandshake => {
                println!("Received server handshake");
                Ok(())
            }
            _ => {
                panic!("Received unknown message");
            }
        }
    }
    fn disconnect(&mut self) -> std::result::Result<(), MicroBatClientError> {
        MicrobatMessages::Disconnect.send(&mut self.stream)?;
        Ok(())
    }
    fn query(&mut self, sql: String) -> std::result::Result<(), MicroBatClientError> {
        MicrobatMessages::Query(sql).send(&mut self.stream)?;
        match read_message(&mut self.stream) {
            MicrobatMessages::ClientHandshake => {
                println!("Received server handshake");
                Ok(())
            }
            MicrobatMessages::Error(msg) => {
                println!("ERROR: {}", msg);
                Ok(())
            }
            MicrobatMessages::RowDescription(rows) => {
                print!("|");
                for row in &rows.rows {
                    print!(" {} |", row.name);
                }
                println!();
                Ok(())
            }
            _ => {
                panic!("Received unknown message");
            }
        }
    }
}

fn main() {
    let connect_string = String::from("localhost:7878");
    let stream = TcpStream::connect(&connect_string).expect("Failed to connect to microbat");
    let mut client = MicroBatTcpClient { stream };
    match client.handshake() {
        Ok(_) => {
            println!("Connected to {}", connect_string);
            println!();
        }
        Err(err) => {
            println!("Error {:?}", err);
        }
    }

    let mut rl = Editor::<()>::new().unwrap();
    loop {
        let readline = rl.readline("microbat> ");
        match readline {
            Ok(line) => {
                client.query(line).unwrap();
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                client.disconnect().unwrap();
                println!("Disconnected");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
