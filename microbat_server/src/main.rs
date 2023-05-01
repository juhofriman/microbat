use crate::lexer::Lexer;

mod lexer;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut lexer = match Lexer::with_input(args[1].clone()) {
        Ok(lexer) => lexer,
        Err(err) => panic!("Can't lex that {:?}", err.kind),
    };
    while lexer.has_next() {
        println!("{:?}", lexer.next());
    }
    println!("has_next() -> {}", lexer.has_next());
    println!("{:?}", lexer.next());
}
