mod sql;

use crate::sql::lexer::SqlLexer;

fn main() {
    let mut lexer = SqlLexer::new("SELECT a, b, c FROM foo;");
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
