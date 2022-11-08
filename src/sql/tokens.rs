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
    // Reserved words
    SELECT,
    UPDATE,
    INSERT,
    DELETE,
    WHERE,
    FROM,
    SET,

    // Separators
    COMMA,
    DOT,
    LPARENS,
    RPARENS,

    // Operators
    EQ,
    LT,
    GT,
    LTE,
    GTE,

    PLUS,

    // Values
    IDENTIFIER(String),
    STRING(String),

    TERMINATE,
}
