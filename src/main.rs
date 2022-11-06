mod sql;

use crate::sql::lexer::Lexer;

fn main() {
    match Lexer::new("SELECT a, b, c FROM foo") {
        Ok(mut lexer) => {
            while let Some(token) = lexer.next() {
                println!("{}", token);
            }
        }
        Err(err) => {
            println!("{}", err)
        }
    }
}
