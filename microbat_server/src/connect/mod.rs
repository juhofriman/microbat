use crate::SqlLexer;
use microbat_protocol::{read_message, Column, Data, MicrobatMessages, RowDescription};
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
        match read_message(&mut stream) {
            MicrobatMessages::ClientHandshake => {
                println!("Received client handshake");
                MicrobatMessages::ClientHandshake.send(&mut stream).unwrap();
            }
            MicrobatMessages::Query(query) => {
                println!("Executing: {}", query);
                let mut lexer = SqlLexer::new(&query);
                loop {
                    match lexer.next() {
                        Ok(token_option) => match token_option {
                            Some(token) => println!("{}", token),
                            None => break,
                        },
                        Err(err) => {
                            println!();
                            println!("{}", err);
                            println!("{}", query);
                            println!("{}^", "-".repeat(err.location.column as usize - 1));
                            MicrobatMessages::Error(format!("{}", err))
                                .send(&mut stream)
                                .unwrap();
                            break;
                        }
                    }
                }
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
                        Column {
                            name: String::from("boz"),
                        },
                    ],
                };
                MicrobatMessages::RowDescription(rows)
                    .send(&mut stream)
                    .unwrap();
            }
            MicrobatMessages::Error(msg) => {
                println!("Weird... Client sent ERROR: {}", msg);
            }
            MicrobatMessages::RowDescription(_) => {
                println!("Weird... Client sent RowDesscription");
            }
            MicrobatMessages::Disconnect => {
                println!("Disconnecting");
                break;
            }
        }
    }
}
