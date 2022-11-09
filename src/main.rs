mod sql;

use crate::sql::lexer::SqlLexer;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    let default_input = String::from("");
    let input = args.get(1).unwrap_or(&default_input);
    let mut lexer = SqlLexer::new(input);
    loop {
        match lexer.next() {
            Ok(token_option) => match token_option {
                Some(token) => println!("{}", token),
                None => break,
            },
            Err(err) => {
                println!();
                println!("{}", err);
                println!("{}", input);
                println!("{}^", "-".repeat(err.location.column as usize - 1));
                break;
            }
        }
    }
}
