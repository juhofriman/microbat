use crate::{lexer::Lexer, parser::parse_sql};

mod expression;
mod lexer;
pub mod parser;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    println!("Parsing '{}'", args[1]);
    let ast = parse_sql(args[1].to_owned()).expect("Can't parse");
    match ast {
        parser::SqlClause::ShowTables(_) => todo!(),
        parser::SqlClause::Select(projection) => {
            println!("Select exprs evaluate to");
            for expr in projection {
                println!("{:?}", expr.eval());
            } 
        }, 
    }
}
