use super::expression::{Expression, LeafExpression, Operation, OperationExpression};
use super::lexer::{Lexer, LexingError, LexingErrorKind, Token};

pub enum SqlClause {
    ShowTables(String),
    Select(Vec<Box<dyn Expression>>),
}

#[derive(Debug)]
pub struct ParseError {
    kind: ParseErrorKind,
}

#[derive(Debug, PartialEq)]
enum ParseErrorKind {
    LexingError(LexingErrorKind),
    UnexpectedToken,
    EndOfTokens,
}

impl From<LexingError> for ParseError {
    fn from(value: LexingError) -> Self {
        Self {
            kind: ParseErrorKind::LexingError(value.kind),
        }
    }
}

pub fn parse_sql(sql: String) -> Result<SqlClause, ParseError> {
    let mut lexer = Lexer::with_input(sql)?;
    match lexer.next() {
        Token::SELECT => {
            let mut exprs = vec![];
            exprs.push(parse_expression(&mut lexer, 0)?);
            while lexer.peek() == Some(&Token::COMMA) {
                lexer.next();
                exprs.push(parse_expression(&mut lexer, 0)?);
            }
            if lexer.peek() == Some(&Token::FROM) {
                lexer.next();
                
            }

            Ok(SqlClause::Select(exprs))
        },
        _ => Err(ParseError {
            kind: ParseErrorKind::UnexpectedToken,
        }),
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
    let token = lexer.next();
    let rbp = token.rbp();
    match token {
        Token::PLUS => {
            let right = parse_expression(lexer, rbp)?;
            Ok(Box::new(OperationExpression {
                operation: Operation::Plus,
                left,
                right,
            }))
        }
        Token::MINUS => {
            let right = parse_expression(lexer, rbp)?;
            Ok(Box::new(OperationExpression {
                operation: Operation::Minus,
                left,
                right,
            }))
        }
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
    while lexer.peek().ok_or(ParseError { kind: ParseErrorKind::EndOfTokens })?.rbp() > rbp {
        left = led(lexer, left)?;
    }
    Ok(left)
}

#[cfg(test)]
mod tests {

    use std::panic;


    use crate::sql::expression::Value;

    use super::*;

    macro_rules! assert_expression_error {
        ($s:literal, $e:expr) => {
            string_expr_fails(String::from($s), $e);
        }
    }

    macro_rules! assert_expression_parsing {
        ($s:literal, $e:expr) => {
            string_expr_evaluates_to(String::from($s), $e);
        };
    }

    #[test]
    fn test_parsing_error() {
        assert_expression_error!("112", ParseErrorKind::EndOfTokens);
        assert_expression_error!("112 + 11", ParseErrorKind::EndOfTokens);
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
        assert_expression_parsing!("1 + 2 + 3;", Value::Integer(6));
        assert_expression_parsing!("1 + (5 - 2);", Value::Integer(4));
        assert_expression_parsing!("10 - (2 + 2);", Value::Integer(6));
        assert_expression_parsing!("10 - 5 - 2;", Value::Integer(3));
        assert_expression_parsing!("(10 - 5) - 2;", Value::Integer(3));
        assert_expression_parsing!("10 - (5 - 2);", Value::Integer(7));
    }

    fn string_expr_evaluates_to(input: String, evals_to: Value) {
        let mut lexer = Lexer::with_input(input.clone()).expect("Can't parse");
        match parse_expression(&mut lexer, 1) {
            Ok(expr) => match expr.eval() {
                Ok(val) => {
                    assert_eq!(val, evals_to, "{} did not eval as expected {}", input, expr.visualize());
                }
                Err(_) => panic!("Can't eval expression"),
            },
            Err(_) => panic!("Can't parse expression"),
        }
    }

    fn string_expr_fails(input: String, expected_error: ParseErrorKind) {
        let mut lexer = Lexer::with_input(input.clone()).expect("nonono");
        let result = parse_expression(&mut lexer, 0);
        assert!(result.is_err(), "Expected \"{}\" to error but it succeeded", input);
        match result {
            Ok(_) => assert!(false, "Expected \"{}\" to error but it succeeded", input),
            Err(error) => assert_eq!(error.kind, expected_error),
        }
    }

    #[test]
    fn test_sql_parsing_only_with_projection() {
        assert_select_parsing("select 1;", vec![Value::Integer(1)]);
        assert_select_parsing("select 1 + 52;", vec![Value::Integer(53)]);
        assert_select_parsing("select 1, 2;", vec![Value::Integer(1), Value::Integer(2)]);
        assert_select_parsing(
            "select 1, 2, 3, 4;",
            vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
                Value::Integer(4),
            ],
        );
        assert_select_parsing(
            "select (1 + 1), (6 - (2 + 3));",
            vec![Value::Integer(2), Value::Integer(1)],
        );
    }

    fn assert_select_parsing(input: &str, expr_results: Vec<Value>) {
        let sql_ast = parse_sql(input.to_owned()).expect(format!("Can't parse {}", input).as_str());
        match sql_ast {
            SqlClause::Select(exprs) => {
                assert_eq!(exprs.len(), expr_results.len());
                for (index, expecter_result) in expr_results.into_iter().enumerate() {
                    assert_eq!(exprs[index].eval().expect("Can't eval"), expecter_result);
                }
            }

            _ => panic!(),
        }
    }
}