use std::fmt::Display;

use super::expression::{
    Expression, LeafExpression, NegateExpression, Operation, OperationExpression, ReferenceExpression,
};
use super::lexer::{Lexer, LexingError, LexingErrorKind, Token};

pub enum SqlClause {
    ShowTables,
    Select(Vec<Box<dyn Expression>>, Vec<String>),
}

#[derive(Debug)]
pub struct ParseError {
    pub kind: ParseErrorKind,
}

#[derive(Debug, PartialEq)]
pub enum ParseErrorKind {
    LexingError(LexingErrorKind),
    UnexpectedToken,
    EndOfTokens,
    NoNud(String),
    NoLed(String),
}

impl Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            ParseErrorKind::LexingError(le) => write!(f, "{}", le),
            ParseErrorKind::UnexpectedToken => write!(f, "Unexpected token... somewhere"),
            ParseErrorKind::EndOfTokens => write!(f, "Unexpected end of tokens"),
            ParseErrorKind::NoNud(token) => write!(f, "No nud {}", token),
            ParseErrorKind::NoLed(token) => write!(f, "No led {}", token),
        }
    }
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
        Token::SHOW => {
            if lexer.next() == &Token::TABLES {
                return Ok(SqlClause::ShowTables);
            }
            Err(ParseError {
                kind: ParseErrorKind::UnexpectedToken,
            })
        }
        Token::SELECT => {
            let mut exprs = vec![];
            let mut from = vec![];
            exprs.push(parse_expression(&mut lexer, 0)?);
            while lexer.peek() == Some(&Token::COMMA) {
                lexer.next();
                exprs.push(parse_expression(&mut lexer, 0)?);
            }
            if lexer.peek_is(&Token::FROM) {
                lexer.next();
                from.push(lexer.next_identifier()?);
                while lexer.peek() == Some(&Token::COMMA) {
                    lexer.next();
                    match lexer.next() {
                        Token::IDENTIFIER(name) => {
                            from.push(name.to_owned());
                        }
                        _ => {
                            return Err(ParseError {
                                kind: ParseErrorKind::UnexpectedToken,
                            })
                        }
                    }
                }
            }

            Ok(SqlClause::Select(exprs, from))
        }
        _ => Err(ParseError {
            kind: ParseErrorKind::UnexpectedToken,
        }),
    }
}

fn nud(lexer: &mut Lexer) -> Result<Box<dyn Expression>, ParseError> {
    let token = lexer.next();
    let rbp = token.rbp();
    match token {
        Token::IDENTIFIER(v) => Ok(Box::new(ReferenceExpression::new(v.clone()))),
        Token::INTEGER(v) => Ok(Box::new(LeafExpression::new(*v))),
        Token::LPARENS => parse_expression(lexer, 0),
        Token::MINUS => Ok(Box::new(NegateExpression {
            expression: parse_expression(lexer, rbp)?,
        })),
        token => Err(ParseError {
            kind: ParseErrorKind::NoNud(format!("{:?}", token)),
        }),
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
        token => Err(ParseError {
            kind: ParseErrorKind::NoLed(format!("{:?}", token)),
        }),
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
    while lexer
        .peek()
        .ok_or(ParseError {
            kind: ParseErrorKind::EndOfTokens,
        })?
        .rbp()
        > rbp
    {
        left = led(lexer, left)?;
    }
    Ok(left)
}

#[cfg(test)]
mod tests {

    use std::panic;

    use microbat_protocol::data::data_values::MData;

    use super::*;

    macro_rules! assert_expression_error {
        ($s:literal, $e:expr) => {
            string_expr_fails(String::from($s), $e);
        };
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
        assert_expression_parsing!("1;", MData::Integer(1));
        assert_expression_parsing!("1+1;", MData::Integer(2));
        assert_expression_parsing!("5+100;", MData::Integer(105));
        assert_expression_parsing!("1-1;", MData::Integer(0));
    }

    #[test]
    fn test_nested_expressions() {
        assert_expression_parsing!("1 + 2 + 3;", MData::Integer(6));
        assert_expression_parsing!("1 + (5 - 2);", MData::Integer(4));
        assert_expression_parsing!("10 - (2 + 2);", MData::Integer(6));
        assert_expression_parsing!("10 - 5 - 2;", MData::Integer(3));
        assert_expression_parsing!("(10 - 5) - 2;", MData::Integer(3));
        assert_expression_parsing!("10 - (5 - 2);", MData::Integer(7));
    }

    #[test]
    fn test_negatives() {
        assert_expression_parsing!("2-10;", MData::Integer(-8));
        assert_expression_parsing!("-5 + 5;", MData::Integer(0));
    }

    fn string_expr_evaluates_to(input: String, evals_to: MData) {
        let mut lexer = Lexer::with_input(input.clone()).expect("Can't parse");
        let expr = parse_expression(&mut lexer, 1).unwrap();
        match expr.eval() {
            Ok(val) => {
                assert_eq!(
                    val,
                    evals_to,
                    "{} did not eval as expected {}",
                    input,
                    expr.visualize()
                );
            }
            Err(_) => panic!("Can't eval expression"),
        }
    }

    fn string_expr_fails(input: String, expected_error: ParseErrorKind) {
        let mut lexer = Lexer::with_input(input.clone()).expect("nonono");
        let result = parse_expression(&mut lexer, 0);
        assert!(
            result.is_err(),
            "Expected \"{}\" to error but it succeeded",
            input
        );
        match result {
            Ok(_) => assert!(false, "Expected \"{}\" to error but it succeeded", input),
            Err(error) => assert_eq!(error.kind, expected_error),
        }
    }

    #[test]
    fn test_show_table_parsing() {
        let sql_ast = parse_sql("SHOW TABLES;".to_owned()).expect("Can't parse SHOW TABLES");
        match sql_ast {
            SqlClause::ShowTables => {}
            _ => panic!("Didn't parse to ShowTables"),
        }
    }

    #[test]
    fn test_sql_parsing_only_with_projection() {
        assert_parsing("select 1;", vec![MData::Integer(1)], vec![]);
        assert_parsing("select 1 + 52;", vec![MData::Integer(53)], vec![]);
        assert_parsing(
            "select 1, 2;",
            vec![MData::Integer(1), MData::Integer(2)],
            vec![],
        );
        assert_parsing(
            "select 1, 2, 3, 4;",
            vec![
                MData::Integer(1),
                MData::Integer(2),
                MData::Integer(3),
                MData::Integer(4),
            ],
            vec![],
        );
        assert_parsing(
            "select (1 + 1), (6 - (2 + 3));",
            vec![MData::Integer(2), MData::Integer(1)],
            vec![],
        );
    }

    #[test]
    fn test_from_parsing() {
        assert_parsing(
            "select 1 from bar",
            vec![MData::Integer(1)],
            vec![String::from("BAR")],
        );
        assert_parsing(
            "select 1 from foo, bar",
            vec![MData::Integer(1)],
            vec![String::from("FOO"), String::from("BAR")],
        );
    }

    fn assert_parsing(input: &str, expected_projections: Vec<MData>, expected_from: Vec<String>) {
        let sql_ast = parse_sql(input.to_owned()).expect(format!("Can't parse {}", input).as_str());
        match sql_ast {
            SqlClause::Select(projections, from) => {
                assert_eq!(projections.len(), expected_projections.len());
                for (index, expecter_result) in expected_projections.into_iter().enumerate() {
                    assert_eq!(
                        projections[index].eval().expect("Can't eval"),
                        expecter_result
                    );
                }
                if expected_from.len() > 0 {
                    assert_eq!(from, expected_from);
                }
            }

            _ => panic!(),
        }
    }
}
