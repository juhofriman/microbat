mod connect;
mod sql;

use crate::sql::lexer::SqlLexer;
use std::env;

fn main() {
    connect::run()
}
