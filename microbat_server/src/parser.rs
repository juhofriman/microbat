use crate::{
    expression::{Expression, LeafExpression, Operation, OperationExpression},
    lexer::{Lexer, LexingError, LexingErrorKind, Token},
};

enum SqlClause {
    ShowTables(String),
    Select(Vec<Box<dyn Expression>>),
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
            Token::SELECT => self.parse_select(),
            _ => panic!("nonono"),
        }
    }

    fn parse_select(&mut self) -> Result<SqlClause, ParseError> {
        Err(ParseError {
            kind: ParseErrorKind::EndOfTokens,
        })
    }
}

fn nud(lexer: &mut Lexer) -> Result<Box<dyn Expression>, ParseError> {
    match lexer.next() {
        Token::INTEGER(v) => Ok(Box::new(LeafExpression::new(*v))),
        Token::LPARENS => parse_expression(lexer, 0), 
        token => panic!("Can't nud: {:?}", token),
    }
}

fn led(lexer: &mut Lexer, left: Box<dyn Expression>) -> Result<Box<dyn Expression>, ParseError> {
    match lexer.next() {
        Token::PLUS => {
            let right = parse_expression(lexer, 0)?;
            Ok(Box::new(OperationExpression {
                operation: Operation::Plus,
                left,
                right,
            }))
        },
        Token::MINUS => {
            let right = parse_expression(lexer, 0)?;
            Ok(Box::new(OperationExpression {
                operation: Operation::Minus,
                left,
                right,
            }))
        },
        Token::RPARENS => Ok(left),
        token => panic!("Can't led: {:?}", token),
    }
}

impl Token {
    fn rbp(&self) -> usize {
        match self {
            Token::INTEGER(_) => 1,
            Token::PLUS => 5,
            Token::MINUS => 5,
            Token::LPARENS => 50,
            Token::RPARENS => 1,
            _ => 0, 
        }
    }
}

/// Parses next expression from the lexer
fn parse_expression(lexer: &mut Lexer, rbp: usize) -> Result<Box<dyn Expression>, ParseError> {
    let mut left = nud(lexer)?;
    while lexer.peek().unwrap().rbp() > rbp {
        left = led(lexer, left)?;
    }
    Ok(left)
}

#[cfg(test)]
mod tests {

    use crate::expression::Value;

    use super::*;

    macro_rules! assert_expression_parsing {
        ($s:literal, $e:expr) => {
            string_expr_evaluates_to(String::from($s), $e);
        };
    }

    #[test]
    fn test_parsing() {
        assert_expression_parsing!("1;", Value::Integer(1));
        assert_expression_parsing!("1+1;", Value::Integer(2));
        assert_expression_parsing!("5+100;", Value::Integer(105));
        assert_expression_parsing!("1-1;", Value::Integer(0));
    }
    

    #[test]
    fn test_nested_expressions() {
        assert_expression_parsing!("1 + (5 - 2) ;", Value::Integer(4));
    }
    
    fn string_expr_evaluates_to(input: String, evals_to: Value) {
        let mut lexer = Lexer::with_input(input.clone()).expect("Can't parse");
        match parse_expression(&mut lexer, 1) {
            Ok(expr) => match expr.eval() {
                Ok(val) => {
                    assert_eq!(val, evals_to, "{} did not eval as expected", input);
                }
                Err(_) => panic!("Can't eval expression"),
            },
            Err(_) => panic!("Can't parse expression"),
        }
    }

}
