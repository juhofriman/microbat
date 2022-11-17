use crate::SqlLexer;
use microbat_protocol::client_messages::{deserialize_client_message, MicrobatClientMessage};
use microbat_protocol::server_messages::MicrobatServerMessage;
use microbat_protocol::{read_message, Column, Data, DataColumns, MicrobatMessage, RowDescription};
use std::{
    io::{prelude::*, BufReader},
    net::{TcpListener, TcpStream},
    str, thread,
};

pub fn run() {
    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();
    println!("MICROBAT EXTRAVAGANZA \"DB\" BOUND ON 7878");
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        thread::spawn(|| {
            handle_connection(stream);
        });
    }
}

fn handle_connection(mut stream: TcpStream) {
    loop {
        match read_message(&mut stream, deserialize_client_message) {
            Ok(message) => match message {
                MicrobatClientMessage::Handshake => {
                    println!("Received client handshake!");
                    MicrobatServerMessage::Handshake.send(&mut stream).unwrap();
                }
                MicrobatClientMessage::Disconnect => {
                    println!("Received client disconnect!");
                    break;
                }
                MicrobatClientMessage::Query(query) => {
                    println!("Executing query: {}", query);
                    let rows = RowDescription {
                        rows: vec![
                            Column {
                                name: String::from("foo"),
                            },
                            Column {
                                name: String::from("bar"),
                            },
                            Column {
                                name: String::from("baz"),
                            },
                        ],
                    };
                    MicrobatServerMessage::RowDescription(rows)
                        .send(&mut stream)
                        .unwrap();
                    MicrobatServerMessage::DataRow(DataColumns {
                        columns: vec![
                            Data::Varchar(String::from("This is")),
                            Data::Varchar(String::from("streaming")),
                            Data::Varchar(String::from("data")),
                        ],
                    })
                    .send(&mut stream)
                    .unwrap();
                    MicrobatServerMessage::DataRow(DataColumns {
                        columns: vec![
                            Data::Varchar(String::from("in")),
                            Data::Varchar(String::from("action.")),
                            Data::Varchar(String::from("Cool!")),
                        ],
                    })
                    .send(&mut stream)
                    .unwrap();
                    MicrobatServerMessage::Handshake.send(&mut stream).unwrap();
                }
            },
            Err(err) => {
                println!("Error while deserializing: {:?}", err);
                break;
            }
        }
    }
}
