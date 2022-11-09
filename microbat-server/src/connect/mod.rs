use crate::SqlLexer;
use std::{
    io::{prelude::*, BufReader},
    net::{TcpListener, TcpStream},
};

pub fn run() {
    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();

    for stream in listener.incoming() {
        let stream = stream.unwrap();

        handle_connection(stream);
    }
}

fn handle_connection(mut stream: TcpStream) {
    let buf_reader = BufReader::new(&mut stream);
    let http_request: Vec<_> = buf_reader
        .lines()
        .map(|result| result.unwrap())
        .take_while(|line| !line.is_empty())
        .collect();

    let query = http_request.join(&String::from(""));
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
                break;
            }
        }
    }
}
