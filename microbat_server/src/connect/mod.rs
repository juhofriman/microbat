use crate::SqlLexer;
use microbat_protocol::{error_message, startup_response, MSG_TYPE_QUERY, MSG_TYPE_STARTUP};
use std::{
    io::{prelude::*, BufReader},
    net::{TcpListener, TcpStream},
    str,
};

pub fn run() {
    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();
    println!("MICROBAT EXTRAVAGANZA \"DB\" BOUND ON 7878");
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        handle_connection(stream);
    }
}

fn handle_connection(mut stream: TcpStream) {
    let mut message_type = [b'x'];

    stream.read(&mut message_type).unwrap();

    match message_type[0] {
        MSG_TYPE_STARTUP => {
            let mut length = [b'x'];
            stream.read(&mut length).unwrap();
            let mut byte_buffer = vec![0; length[0] as usize];
            stream.read_exact(&mut byte_buffer).unwrap();
            println!("RECEIVED: {}", str::from_utf8(&byte_buffer).unwrap());

            stream.write(&startup_response()).unwrap();
        }
        MSG_TYPE_QUERY => {
            let mut length = [b'x'];
            stream.read(&mut length).unwrap();
            let mut byte_buffer = vec![0; length[0] as usize];
            stream.read_exact(&mut byte_buffer).unwrap();
            let query = str::from_utf8(&byte_buffer).unwrap();
            println!("Received query: {}", query);
            println!("Lexing...");
            let mut lexer = SqlLexer::new(query);
            loop {
                match lexer.next() {
                    Ok(token_option) => match token_option {
                        Some(token) => println!("\t{}", token),
                        None => {
                            stream.write(&startup_response()).unwrap();
                            break;
                        }
                    },
                    Err(err) => {
                        println!();
                        println!("ERROR WHILE LEXING");
                        println!("{}", err);
                        println!("{}", query);
                        println!("Sending error to client...");
                        stream.write(&error_message(format!("{}", err))).unwrap();
                        break;
                    }
                }
            }
            stream.write(&startup_response()).unwrap();
        }
        _ => {
            println!("Unknown message type {:?}", message_type);
        }
    }
}
