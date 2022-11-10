use microbat_protocol::{query_message, startup_message, MSG_TYPE_ERROR, MSG_TYPE_STARTUP};
use rustyline::error::ReadlineError;
use rustyline::{Editor, Result};
use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::str;

fn main() -> Result<()> {
    // `()` can be used when no completer is required
    let mut rl = Editor::<()>::new()?;
    loop {
        let readline = rl.readline("microbat> ");
        match readline {
            Ok(line) => {
                send_with_new_connection(line);
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
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
    rl.save_history("history.txt")
}

fn send_with_new_connection(query: String) {
    match TcpStream::connect("localhost:7878") {
        Ok(mut stream) => {
            stream.write(&query_message(query)[..]).unwrap();

            let mut message_type = [b'x'];

            stream.read(&mut message_type).unwrap();

            match message_type[0] {
                MSG_TYPE_STARTUP => {
                    let mut length = [b'x'];
                    stream.read(&mut length).unwrap();
                    let mut byte_buffer = vec![0; length[0] as usize];
                    stream.read_exact(&mut byte_buffer).unwrap();
                    println!("RECEIVED: {}", str::from_utf8(&byte_buffer).unwrap());
                }
                MSG_TYPE_ERROR => {
                    let mut length = [b'x'];
                    stream.read(&mut length).unwrap();
                    let mut byte_buffer = vec![0; length[0] as usize];
                    stream.read_exact(&mut byte_buffer).unwrap();
                    println!("ERROR: {}", str::from_utf8(&byte_buffer).unwrap());
                }
                _ => {
                    println!("Unknown message type {:?}", message_type);
                }
            }
        }
        Err(e) => {
            println!("Failed to connect: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_test() {
        assert!(true)
    }
}
