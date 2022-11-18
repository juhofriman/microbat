use crate::sql::lexer;
use crate::sql::tokens::{Token, TokenTypes};
use crate::SqlLexer;
use microbat_protocol::client_messages::{deserialize_client_message, MicrobatClientMessage};
use microbat_protocol::server_messages::MicrobatServerMessage;
use microbat_protocol::{read_message, Column, Data, DataColumns, MicrobatMessage, RowDescription};
use std::{
    io::{prelude::*, BufReader},
    net::{TcpListener, TcpStream},
    str, thread, time,
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
                    let mut identifiers: Vec<Data> = vec![];
                    let mut lexer = SqlLexer::new(&query);
                    while let Ok(token) = lexer.next() {
                        match token {
                            Some(token) => match token.token_type {
                                TokenTypes::IDENTIFIER(value) => {
                                    identifiers.push(Data::Varchar(value));
                                }
                                _ => (),
                            },
                            None => break,
                        }
                    }

                    let rows = RowDescription {
                        rows: vec![Column {
                            name: String::from("identifiers_for_query"),
                        }],
                    };
                    MicrobatServerMessage::RowDescription(rows)
                        .send(&mut stream)
                        .unwrap();

                    for identifier in identifiers {
                        MicrobatServerMessage::DataRow(DataColumns {
                            columns: vec![identifier],
                        })
                        .send(&mut stream)
                        .unwrap();
                    }

                    MicrobatServerMessage::Ready.send(&mut stream).unwrap();
                }
            },
            Err(err) => {
                println!("Error while deserializing: {:?}", err);
                break;
            }
        }
    }
}
