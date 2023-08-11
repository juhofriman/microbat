use microbat_protocol::messages::client_messages::{deserialize_client_message, MicrobatClientMessage};
use microbat_protocol::data::{Column, MDataType, MData};
use microbat_protocol::messages::server_messages::MicrobatServerMessage;
use microbat_protocol::messages::{read_message, MicrobatMessage};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::thread;

use crate::db::manager::{DatabaseManager, InMemoryManager};
use crate::db::{execute_sql, QueryResult};

pub struct MicrobatServerOpts {
    pub bind: String,
}

pub fn run_microbat(server_opts: MicrobatServerOpts) {
    let listener = TcpListener::bind(server_opts.bind).expect("Can't start microbat");
    println!("Microbat is running");
    let database = Arc::new(RwLock::new(InMemoryManager::new()));
    let mut init_db = database.write().unwrap();
    init_db
        .create_table(
            String::from("PEOPLE"),
            vec![
                Column {
                    name: String::from("id"),
                    data_type: MDataType::Integer,
                },
                Column {
                    name: String::from("name"),
                    data_type: MDataType::Varchar,
                },
                Column {
                    name: String::from("age"),
                    data_type: MDataType::Integer,
                },
                Column {
                    name: String::from("quote"),
                    data_type: MDataType::Varchar,
                },
            ],
        )
        .unwrap();
    init_db.insert("PEOPLE", vec![
        MData::Integer(1),
        MData::Varchar(String::from("Juho")),
        MData::Integer(19),
        MData::Varchar(String::from("Life is life")),
    ]).unwrap();
    init_db.insert("PEOPLE", vec![
        MData::Integer(2),
        MData::Varchar(String::from("Simo")),
        MData::Integer(19),
        MData::Varchar(String::from("Only death is real")),
    ]).unwrap();
    init_db
        .create_table(
            String::from("DEPARTMENTS"),
            vec![
                Column {
                    name: String::from("id"),
                    data_type: MDataType::Integer,
                },
                Column {
                    name: String::from("name"),
                    data_type: MDataType::Varchar,
                },
            ],
        )
        .unwrap();
    drop(init_db);
    let mut thread_id = 1;
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let db_arc = Arc::clone(&database);
        thread::Builder::new()
            .name(format!("microbat-t-{}", thread_id))
            .spawn(move || {
                handle_connection(stream, &db_arc);
            })
            .expect("Thread spawn failure");
        thread_id = thread_id + 1;
    }
}

fn handle_connection(mut stream: TcpStream, manager: &Arc<RwLock<impl DatabaseManager>>) {
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
                    match execute_sql(query, manager) {
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
