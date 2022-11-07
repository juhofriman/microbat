mod sql;

use crate::sql::lexer::SqlLexer;

fn main() {
    match SqlLexer::new("SELECT a, b, c FROM foo") {
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
