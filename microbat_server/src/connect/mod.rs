use std::net::{TcpListener, TcpStream};
use std::thread;
use microbat_protocol::data_representation::{DataDescription, Column, DataType};
use microbat_protocol::{read_message, MicrobatMessage};
use microbat_protocol::client_messages::{deserialize_client_message, MicrobatClientMessage};
use microbat_protocol::server_messages::MicrobatServerMessage;

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
            Ok(message) => {
                match message {
                    MicrobatClientMessage::Handshake => {
                        println!("Received handshake");
                        MicrobatServerMessage::Handshake.send(&mut stream).unwrap();
                        MicrobatServerMessage::Ready.send(&mut stream).unwrap();
                    },
                    MicrobatClientMessage::Disconnect => {
                        println!("Disconnect");
                        break;
                    },
                    MicrobatClientMessage::Query(query) => {
                        println!("Executing {}", query);
                        let desc = DataDescription {
                            columns: vec![Column { name: String::from("foo"), data_type: DataType::Integer }]

                        };
                        MicrobatServerMessage::DataDescription(desc).send(&mut stream).unwrap();
                        MicrobatServerMessage::Ready.send(&mut stream).unwrap();

                    },
                }
            },
            Err(err) => {
                println!("ERROR");
                break;
            }
        }
    }
}
