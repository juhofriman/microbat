use microbat_protocol::client_messages::MicrobatClientMessage;
use microbat_protocol::server_messages::{deserialize_server_message, MicrobatServerMessage};
use microbat_protocol::{read_message, Column, Data, MicrobatMessage, MicrobatProtocolError};
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

struct QueryResult {
    columns: Vec<Column>,
    rows: Vec<Vec<Data>>,
}

impl QueryResult {
    fn paddings(&self) -> Vec<usize> {
        let mut paddings: Vec<usize> = vec![];

        let mut longest = 0;
        for (index, column) in self.columns.iter().enumerate() {
            longest = column.name.len();
            for data in &self.rows {
                match &data[index] {
                    Data::Varchar(d) => {
                        if d.len() > longest {
                            longest = d.len();
                        }
                    }
                    _ => (),
                }
            }
            paddings.push(longest + 1);
        }

        paddings
    }

    fn render(&self) {
        let paddings = self.paddings();
        for (index, column) in self.columns.iter().enumerate() {
            print!("--{}", "-".repeat(paddings[index]));
        }
        println!("-");
        for (index, column) in self.columns.iter().enumerate() {
            print!("| {}", column.name);
            let padding = paddings[index] - column.name.len();
            if padding > 0 {
                print!("{}", " ".repeat(padding));
            }
        }
        println!("|");
        for (index, column) in self.columns.iter().enumerate() {
            print!("--{}", "-".repeat(paddings[index]));
        }
        println!("-");
        for (index, row) in self.rows.iter().enumerate() {
            for (index, column) in row.iter().enumerate() {
                match column {
                    Data::Varchar(data) => {
                        print!("| {}", data);
                        let padding = paddings[index] - data.len();
                        if padding > 0 {
                            print!("{}", " ".repeat(padding));
                        }
                    }
                    _ => (),
                }
            }
            println!("|");
        }
        for (index, column) in self.columns.iter().enumerate() {
            print!("--{}", "-".repeat(paddings[index]));
        }
        println!("-");
    }
}

impl MicroBatTcpClient {
    fn handshake(&mut self) -> std::result::Result<(), MicroBatClientError> {
        MicrobatClientMessage::Handshake.send(&mut self.stream)?;
        match read_message(&mut self.stream, deserialize_server_message)? {
            MicrobatServerMessage::Handshake => {
                println!("Received server handshake");
                Ok(())
            }
            _ => {
                panic!("Received unknown message");
            }
        }
    }
    fn disconnect(&mut self) -> std::result::Result<(), MicroBatClientError> {
        MicrobatClientMessage::Disconnect.send(&mut self.stream)?;
        Ok(())
    }
    fn query(&mut self, sql: String) -> std::result::Result<QueryResult, MicroBatClientError> {
        MicrobatClientMessage::Query(sql).send(&mut self.stream)?;
        match read_message(&mut self.stream, deserialize_server_message)? {
            MicrobatServerMessage::Error(msg) => Err(MicroBatClientError { msg }),
            MicrobatServerMessage::RowDescription(rows) => {
                let mut result = QueryResult {
                    columns: rows.rows,
                    rows: vec![],
                };
                loop {
                    match read_message(&mut self.stream, deserialize_server_message)? {
                        MicrobatServerMessage::DataRow(row) => {
                            result.rows.push(row.columns);
                        }
                        _ => {
                            break;
                        }
                    }
                }
                Ok(result)
            }
            _ => {
                panic!("Received unknown message here");
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
            Ok(line) => match client.query(line) {
                Ok(result) => {
                    result.render();
                }
                Err(err) => {
                    println!("{}", err.msg);
                }
            },
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
