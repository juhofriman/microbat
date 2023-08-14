use microbat_protocol::data::data_values::{MData, MDataType};
use microbat_protocol::data::table_model::Column;
use microbat_protocol::messages::client_messages::{
    deserialize_client_message, MicrobatClientMessage,
};
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
                Column::new(String::from("id"), MDataType::Integer),
                Column::new(String::from("name"), MDataType::Varchar),
                Column::new(String::from("age"), MDataType::Integer),
            ],
        )
        .unwrap();
    init_db
        .insert(
            "PEOPLE",
            vec![
                MData::Integer(1),
                MData::Varchar(String::from("Juho")),
                MData::Integer(40),
            ],
        )
        .unwrap();
    init_db
        .insert(
            "PEOPLE",
            vec![
                MData::Integer(2),
                MData::Varchar(String::from("Simo")),
                MData::Integer(19),
            ],
        )
        .unwrap();
    init_db
        .insert(
            "PEOPLE",
            vec![
                MData::Integer(3),
                MData::Varchar(String::from("Hermanni")),
                MData::Integer(48),
            ],
        )
        .unwrap();
    init_db
        .insert(
            "PEOPLE",
            vec![
                MData::Integer(4),
                MData::Varchar(String::from("Taavetti")),
                MData::Integer(32),
            ],
        )
        .unwrap();
    init_db
        .insert(
            "PEOPLE",
            vec![
                MData::Integer(5),
                MData::Varchar(String::from("Metusalem")),
                MData::Integer(85),
            ],
        )
        .unwrap();

    init_db
        .create_table(
            String::from("DEPARTMENTS"),
            vec![
                Column::new(String::from("id_dep"), MDataType::Integer),
                Column::new(String::from("name_dep"), MDataType::Varchar),
            ],
        )
        .unwrap();
    init_db
        .insert(
            "DEPARTMENTS",
            vec![MData::Integer(1), MData::Varchar(String::from("Rustland"))],
        )
        .unwrap();
    init_db
        .insert(
            "DEPARTMENTS",
            vec![MData::Integer(2), MData::Varchar(String::from("Goland"))],
        )
        .unwrap();
    init_db
        .insert(
            "DEPARTMENTS",
            vec![MData::Integer(3), MData::Varchar(String::from("Javaland"))],
        )
        .unwrap();
    init_db
        .insert(
            "DEPARTMENTS",
            vec![MData::Integer(4), MData::Varchar(String::from("Cppland"))],
        )
        .unwrap();
    init_db
        .insert(
            "DEPARTMENTS",
            vec![
                MData::Integer(5),
                MData::Varchar(String::from("Nodejsland")),
            ],
        )
        .unwrap();
    init_db
        .create_table(
            String::from("MODES"),
            vec![
                Column::new(String::from("id_mode"), MDataType::Integer),
                Column::new(String::from("name_mode"), MDataType::Varchar),
            ],
        )
        .unwrap();
    init_db
        .insert(
            "MODES",
            vec![MData::Integer(1), MData::Varchar(String::from("soft"))],
        )
        .unwrap();
    init_db
        .insert(
            "MODES",
            vec![MData::Integer(2), MData::Varchar(String::from("medium"))],
        )
        .unwrap();
    init_db
        .insert(
            "MODES",
            vec![MData::Integer(3), MData::Varchar(String::from("hard"))],
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
                println!("{:?}", err);
                break;
            }
        }
    }
}
