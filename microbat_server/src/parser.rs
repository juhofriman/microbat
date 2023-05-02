use std::vec;

use crate::{lexer::{Lexer, LexingError, LexingErrorKind, self, Token}, expression::{ LeafExpression, Expression }};


enum SqlClause {
    ShowTables(String),
    Select(Vec<Box<dyn Expression>>)
}

struct Parser {
    lexer: Lexer,
}

#[derive(Debug)]
struct ParseError {
    kind: ParseErrorKind,
}

#[derive(Debug)]
enum ParseErrorKind {
    LexingError(LexingErrorKind),
    EndOfTokens,
}

impl From<LexingError> for ParseError {
    fn from(value: LexingError) -> Self {
        Self {
            kind: ParseErrorKind::LexingError(value.kind),
        }
    }
}

impl Parser {

    fn new(input: String) -> Result<Self, ParseError> {
        Ok(Self {
            lexer: Lexer::with_input(input)?,
        })
    }

    fn parse(&mut self) -> Result<SqlClause, ParseError> {

        match self.lexer.next() {
            Token::SELECT => {
                self.parse_select()
            },
            _ => panic!("nonono")
        }
    }

    fn parse_select(&mut self) -> Result<SqlClause, ParseError> {
        if self.lexer.has_next() {
            return Ok(SqlClause::Select(vec![self.parse_expression(0)?]));
        }
        Err(ParseError { kind: ParseErrorKind::EndOfTokens })
    }

    fn parse_expression(&mut self, rpb: usize) -> Result<Box<dyn Expression>, ParseError> {
        let expression = self.lexer.next().nud();

        expression
    }
}

impl Token {
   fn nud(&self) -> Result<Box<dyn Expression>, ParseError> {
        match self {
            Token::INTEGER(value) => Ok(Box::new(LeafExpression::new(*value))),
            _ => panic!("Can't parse nud")
        }
    } 
    fn led(&self, left: Box<dyn Expression>) -> Result<Box<dyn Expression, ParseError>> {
        match self {
            _ => panic!("Can't parse led")
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::expression::Value;

    use super::*;
    
    // #[test]
    fn test_parsing() {
        let mut parser = Parser::new(String::from("select 1 + 1")).expect("Can't parse");
        match parser.parse().expect("Can't parse") {
            SqlClause::Select(from) => {
                assert_eq!(from.len(), 1);
                match from[0].eval() {
                    Ok(val) => {
                        match val {
                            Value::Integer(v) => assert_eq!(v, 1),
                            _ => panic!(),
                        }
                    },
                    Err(_) => panic!(),
                }

            },
            _ => panic!("Expecting select clause")
        }
    }
}
