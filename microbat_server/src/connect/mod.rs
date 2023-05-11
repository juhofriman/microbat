use microbat_protocol::client_messages::{deserialize_client_message, MicrobatClientMessage};
use microbat_protocol::server_messages::MicrobatServerMessage;
use microbat_protocol::{read_message, MicrobatMessage};
use std::net::{TcpListener, TcpStream};
use std::thread;

use crate::db::{execute_sql, QueryResult};

pub struct MicrobatServerOpts {
    pub bind: String,
}

pub fn run_microbat(server_opts: MicrobatServerOpts) {
    let listener = TcpListener::bind(server_opts.bind).expect("Can't start microbat");
    println!("Microbat is running");

    let mut thread_id = 1;
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        thread::Builder::new()
            .name(format!("microbat-t-{}", thread_id))
            .spawn(move || {
                handle_connection(stream);
            })
            .expect("Thread spawn failure");
        thread_id = thread_id + 1;
    }
}

fn handle_connection(mut stream: TcpStream) {
    loop {
        match read_message(&mut stream, deserialize_client_message) {
            Ok(message) => match message {
                MicrobatClientMessage::Handshake => {
                    println!("Received handshake");
                    MicrobatServerMessage::Handshake.send(&mut stream).unwrap();
                    MicrobatServerMessage::Ready.send(&mut stream).unwrap();
                }
                MicrobatClientMessage::Disconnect => {
                    println!("Disconnect");
                    break;
                }
                MicrobatClientMessage::Query(query) => {
                    println!("Executing {}", query);
                    match execute_sql(query) {
                        Ok(result) => match result {
                            QueryResult::Table(description, data) => {
                                MicrobatServerMessage::DataDescription(description)
                                    .send(&mut stream)
                                    .unwrap();
                                for row in data.into_iter() {
                                    MicrobatServerMessage::DataRow(row)
                                        .send(&mut stream)
                                        .unwrap();
                                }
                            }
                        },
                        Err(err) => {
                            MicrobatServerMessage::Error(err.msg)
                                .send(&mut stream)
                                .unwrap();
                        }
                    }
                    MicrobatServerMessage::Ready.send(&mut stream).unwrap();
                }
            },
            Err(err) => {
                println!("ERROR");
                break;
            }
        }
    }
}
