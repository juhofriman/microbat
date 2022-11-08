mod sql;

use crate::sql::lexer::SqlLexer;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    let default_input = String::from("");
    let mut lexer = SqlLexer::new(args.get(1).unwrap_or(&default_input));
    loop {
        match lexer.next() {
            Ok(token_option) => match token_option {
                Some(token) => println!("{}", token),
                None => break,
            },
            Err(err) => {
                println!("{}", err);
                break;
            }
        }
    }
}
