use crate::sql::lexer::SourceRef;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub struct Token {
    pub token_type: TokenTypes,
    pub source_ref: SourceRef,
}

impl Display for Token {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} {}", self.token_type, self.source_ref)
    }
}

#[derive(Debug, PartialEq)]
pub enum TokenTypes {
    SELECT,
    UPDATE,
    INSERT,
    DELETE,
    WHERE,
    FROM,

    COMMA,

    EQ,
    LT,
    GT,

    PLUS,

    LPAR,
    RPAR,

    IDENTIFIER(String),
    STRING(String),
    INTEGER,
    FLOAT,
}
