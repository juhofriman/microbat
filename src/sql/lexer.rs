use crate::sql::tokens::Token;
use std::fmt::{Display, Formatter};

/// SourceRef describes a location in parsed input.
#[derive(Debug)]
pub struct SourceRef {
    column: u32,
    line: u32,
}

impl Display for SourceRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}:{}]", self.line, self.column)
    }
}

/// General lexing error occurred during the lexing phase
#[derive(Debug)]
pub struct LexingError {
    msg: LexingErrors,
    location: SourceRef,
}

impl Display for LexingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Lexing Error: {} @ {}", self.msg, self.location)
    }
}

/// All possible lexing errors
#[derive(Debug)]
enum LexingErrors {
    StringNotTerminated,
}

impl Display for LexingErrors {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LexingErrors::StringNotTerminated => write!(f, "String was not terminated"),
        }
    }
}

/// Consumable lexer
///
/// Bear in mind that every single character is lower cased. Every identifier
/// becomes lower case internally FOO => foo and so on.
pub struct SqlLexer {
    tokens: Vec<Token>,
    pointer: usize,
}

impl SqlLexer {
    pub fn new(source: &str) -> Result<SqlLexer, LexingError> {
        let mut buffer = lexing_buffer::LexBuffer::new();
        let mut tokens: Vec<Token> = vec![];
        let mut character_iter = source.chars().peekable();

        while let Some(char) = character_iter.next() {
            if let Some(token) = buffer.push_char(char, character_iter.peek())? {
                tokens.push(token);
            }
        }

        Ok(SqlLexer { pointer: 0, tokens })
    }

    /// Advance the lexer and get the next Token
    pub fn next(&mut self) -> Option<&Token> {
        let token = self.tokens.get(self.pointer);
        self.pointer += 1;
        token
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::tokens::TokenTypes;

    #[test]
    fn test_lexer_for_tokens() {
        lexes_to("SELECT", vec![TokenTypes::SELECT]);
        lexes_to("UPDATE", vec![TokenTypes::UPDATE]);
        lexes_to("INSERT", vec![TokenTypes::INSERT]);
        lexes_to("DELETE", vec![TokenTypes::DELETE]);
        lexes_to("WHERE", vec![TokenTypes::WHERE]);
        lexes_to("FROM", vec![TokenTypes::FROM]);
        lexes_to("select", vec![TokenTypes::SELECT]);
        lexes_to("update", vec![TokenTypes::UPDATE]);
        lexes_to("insert", vec![TokenTypes::INSERT]);
        lexes_to("delete", vec![TokenTypes::DELETE]);
        lexes_to("where", vec![TokenTypes::WHERE]);
        lexes_to("from", vec![TokenTypes::FROM]);
        lexes_to("Select", vec![TokenTypes::SELECT]);
        lexes_to("upDAte", vec![TokenTypes::UPDATE]);
        lexes_to("inserT", vec![TokenTypes::INSERT]);
        lexes_to("deLEte", vec![TokenTypes::DELETE]);
        lexes_to("WHERe", vec![TokenTypes::WHERE]);
        lexes_to("fRoM", vec![TokenTypes::FROM]);

        lexes_to(",", vec![TokenTypes::COMMA]);

        lexes_to("=", vec![TokenTypes::EQ]);
        lexes_to("<", vec![TokenTypes::LT]);
        lexes_to(">", vec![TokenTypes::GT]);
        lexes_to("<=", vec![TokenTypes::LTE]);
        lexes_to("<=", vec![TokenTypes::LTE]);

        lexes_to(";", vec![TokenTypes::TERMINATE]);
    }

    #[test]
    fn test_lexer_corner_cases() {
        lexes_to(
            "a=b",
            vec![
                TokenTypes::IDENTIFIER(String::from("a")),
                TokenTypes::EQ,
                TokenTypes::IDENTIFIER(String::from("b")),
            ],
        );
        lexes_to(
            "a<b",
            vec![
                TokenTypes::IDENTIFIER(String::from("a")),
                TokenTypes::LT,
                TokenTypes::IDENTIFIER(String::from("b")),
            ],
        );
        lexes_to(
            "a>b",
            vec![
                TokenTypes::IDENTIFIER(String::from("a")),
                TokenTypes::GT,
                TokenTypes::IDENTIFIER(String::from("b")),
            ],
        );
        lexes_to(
            "a<=b",
            vec![
                TokenTypes::IDENTIFIER(String::from("a")),
                TokenTypes::LTE,
                TokenTypes::IDENTIFIER(String::from("b")),
            ],
        );
        lexes_to(
            "a>=b",
            vec![
                TokenTypes::IDENTIFIER(String::from("a")),
                TokenTypes::GTE,
                TokenTypes::IDENTIFIER(String::from("b")),
            ],
        );
    }

    #[test]
    fn test_lexer_for_real_sql() {
        lexes_to(
            "SELECT a, b, c FROM foo WHERE a = b;",
            vec![
                TokenTypes::SELECT,
                TokenTypes::IDENTIFIER(String::from("a")),
                TokenTypes::COMMA,
                TokenTypes::IDENTIFIER(String::from("b")),
                TokenTypes::COMMA,
                TokenTypes::IDENTIFIER(String::from("c")),
                TokenTypes::FROM,
                TokenTypes::IDENTIFIER(String::from("foo")),
                TokenTypes::WHERE,
                TokenTypes::IDENTIFIER(String::from("a")),
                TokenTypes::EQ,
                TokenTypes::IDENTIFIER(String::from("b")),
                TokenTypes::TERMINATE,
            ],
        );
        lexes_to(
            "select a from bar where b <= c;",
            vec![
                TokenTypes::SELECT,
                TokenTypes::IDENTIFIER(String::from("a")),
                TokenTypes::FROM,
                TokenTypes::IDENTIFIER(String::from("bar")),
                TokenTypes::WHERE,
                TokenTypes::IDENTIFIER(String::from("b")),
                TokenTypes::LTE,
                TokenTypes::IDENTIFIER(String::from("c")),
                TokenTypes::TERMINATE,
            ],
        );
    }

    fn lexes_to(input: &str, expected_tokens: Vec<TokenTypes>) {
        let mut lexer = SqlLexer::new(input).unwrap();
        for (index, expected_token) in expected_tokens.iter().enumerate() {
            assert_eq!(
                lexer.next().unwrap().token_type,
                *expected_token,
                "`{}` did to lex to {:?} at token index {}",
                input,
                expected_token,
                index
            );
        }
        let more = lexer.next();
        assert!(
            more.is_none(),
            "Lexer had more tokens than expected: {:?}",
            more.unwrap().token_type
        );
    }
}

mod lexing_buffer {
    use crate::sql::lexer::{LexingError, LexingErrors, SourceRef};
    use crate::sql::tokens::{Token, TokenTypes};

    /// State enum for lexer. Lexer behaves differently in different modes
    #[derive(PartialEq, Debug)]
    enum LexingState {
        Normal,
        ForcePop,
        Integer,
        Float,
        String,
    }

    /// LexBuffer is used for stateful lexing the given input.
    /// Pushing character to LexBuffer returns Ok(Some(Token)) when new
    /// complete token is created.
    ///
    /// LexingErrors are rare, but for an example non terminated strings fail
    /// lexing with LexingError.
    ///
    /// Use LexBuffer::new for constructing new instance.
    pub struct LexBuffer {
        buffer: String,
        mode: LexingState,
        current_line: u32,
        current_column: u32,
        token_column_marker: u32,
    }

    impl LexBuffer {
        /// Create new LexBuffer
        pub fn new() -> LexBuffer {
            LexBuffer {
                buffer: String::new(),
                mode: LexingState::Normal,
                current_line: 1,
                current_column: 1,
                token_column_marker: 1,
            }
        }

        /// Pushes new character into buffer. LexBuffer needs to be able to
        /// peek next character as well and this Option<&char> must be passed in.
        ///
        /// If peek is None, it is considered to be the final character in input.
        pub fn push_char(
            &mut self,
            current_char: char,
            peek: Option<&char>,
        ) -> Result<Option<Token>, LexingError> {
            self.proceed_counters();
            if current_char.is_whitespace() {
                self.token_column_marker += 1;
                return Ok(None);
            }

            self.buffer.push(current_char.to_ascii_lowercase());

            if self.mode == LexingState::ForcePop {
                self.mode = LexingState::Normal;
                return self.pop_token();
            }

            match peek {
                Some(peek_value) => {
                    if LexBuffer::makes_continuity_token(&current_char, peek_value) {
                        self.mode = LexingState::ForcePop;
                        return Ok(None);
                    }
                    if LexBuffer::is_delimiting(&current_char) {
                        return self.pop_token();
                    }
                    if LexBuffer::is_delimiting(peek_value) {
                        return self.pop_token();
                    }
                    return Ok(None);
                }
                None => self.pop_token(),
            }
        }

        fn is_continuity(character: &char) -> bool {
            return *character == '=';
        }

        fn is_delimiting(character: &char) -> bool {
            return character.is_whitespace()
                || *character == ';'
                || *character == '+'
                || *character == ','
                || *character == '='
                || *character == '<'
                || *character == '>';
        }

        fn makes_continuity_token(current: &char, peek: &char) -> bool {
            if *peek == '=' {
                return *current == '<' || *current == '>';
            }
            return false;
        }

        fn pop_token(&mut self) -> Result<Option<Token>, LexingError> {
            match self.buffer.as_str() {
                // Reserved words
                "select" => self.create_token_and_reset(TokenTypes::SELECT),
                "update" => self.create_token_and_reset(TokenTypes::UPDATE),
                "insert" => self.create_token_and_reset(TokenTypes::INSERT),
                "delete" => self.create_token_and_reset(TokenTypes::DELETE),
                "where" => self.create_token_and_reset(TokenTypes::WHERE),
                "from" => self.create_token_and_reset(TokenTypes::FROM),
                // Separators
                "," => self.create_token_and_reset(TokenTypes::COMMA),
                // Operators
                "=" => self.create_token_and_reset(TokenTypes::EQ),
                "<" => self.create_token_and_reset(TokenTypes::LT),
                ">" => self.create_token_and_reset(TokenTypes::GT),
                "<=" => self.create_token_and_reset(TokenTypes::LTE),
                ">=" => self.create_token_and_reset(TokenTypes::GTE),
                "+" => self.create_token_and_reset(TokenTypes::PLUS),
                ";" => self.create_token_and_reset(TokenTypes::TERMINATE),
                _ => self.create_token_and_reset(TokenTypes::IDENTIFIER(self.buffer.clone())),
            }
        }

        fn proceed_counters(&mut self) {
            self.current_column += 1
        }

        fn create_token_and_reset(
            &mut self,
            token_type: TokenTypes,
        ) -> Result<Option<Token>, LexingError> {
            self.buffer.clear();
            let new_token = Token {
                token_type,
                source_ref: self.current_source_marker(),
            };
            self.token_column_marker = self.current_column;
            Ok(Some(new_token))
        }

        fn current_source_marker(&self) -> SourceRef {
            SourceRef {
                column: self.token_column_marker,
                line: self.current_line,
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::sql::tokens::TokenTypes::{IDENTIFIER, PLUS, SELECT, WHERE};

        #[test]
        fn test_pops_from_end_of_stream() {
            let mut buffer = LexBuffer::new();
            does_not_pop(buffer.push_char('A', Some(&'B')));
            does_not_pop(buffer.push_char('B', Some(&'C')));
            pops_token(
                buffer.push_char('C', None),
                IDENTIFIER(String::from("abc")),
                1,
            );
        }

        #[test]
        fn test_pops_with_whitespace() {
            let mut buffer = LexBuffer::new();
            does_not_pop(buffer.push_char('A', Some(&'B')));
            does_not_pop(buffer.push_char('B', Some(&'C')));
            pops_token(
                buffer.push_char('C', Some(&' ')),
                IDENTIFIER(String::from("abc")),
                1,
            );
            does_not_pop(buffer.push_char('D', Some(&'E')));
            does_not_pop(buffer.push_char('E', Some(&'F')));
            pops_token(
                buffer.push_char('F', None),
                IDENTIFIER(String::from("def")),
                4,
            );
        }

        #[test]
        fn test_does_not_pop_too_early() {
            let mut buffer = LexBuffer::new();
            does_not_pop(buffer.push_char('S', Some(&'E')));
            does_not_pop(buffer.push_char('E', Some(&'L')));
            does_not_pop(buffer.push_char('L', Some(&'E')));
            does_not_pop(buffer.push_char('E', Some(&'C')));
            does_not_pop(buffer.push_char('C', Some(&'T')));
            does_not_pop(buffer.push_char('T', Some(&'E')));
            does_not_pop(buffer.push_char('E', Some(&'D')));
            pops_token(
                buffer.push_char('D', None),
                IDENTIFIER(String::from("selected")),
                1,
            );
        }

        #[test]
        fn test_support_for_delimiting_tokens() {
            let mut buffer = LexBuffer::new();
            pops_token(
                buffer.push_char('a', Some(&'+')),
                IDENTIFIER(String::from("a")),
                1,
            );
            pops_token(buffer.push_char('+', Some(&'b')), PLUS, 2);
            pops_token(
                buffer.push_char('b', None),
                IDENTIFIER(String::from("b")),
                3,
            );
        }

        #[test]
        fn test_support_for_delimiting_tokens_with_whitespace() {
            let mut buffer = LexBuffer::new();
            pops_token(
                buffer.push_char('a', Some(&' ')),
                IDENTIFIER(String::from("a")),
                1,
            );
            does_not_pop(buffer.push_char(' ', Some(&'+')));
            pops_token(buffer.push_char('+', Some(&' ')), PLUS, 3);
            does_not_pop(buffer.push_char(' ', Some(&'b')));
            pops_token(
                buffer.push_char('b', None),
                IDENTIFIER(String::from("b")),
                5,
            );
        }

        #[test]
        fn test_filling_and_auto_popping_ready_buffer() {
            let mut buffer = LexBuffer::new();
            does_not_pop(buffer.push_char('S', Some(&'E')));
            does_not_pop(buffer.push_char('E', Some(&'L')));
            does_not_pop(buffer.push_char('L', Some(&'E')));
            does_not_pop(buffer.push_char('E', Some(&'C')));
            does_not_pop(buffer.push_char('C', Some(&'T')));
            pops_token(buffer.push_char('T', Some(&' ')), SELECT, 1);
            // Whitespace
            does_not_pop(buffer.push_char(' ', Some(&'W')));
            // Here the buffer must be reset
            does_not_pop(buffer.push_char('W', Some(&'H')));
            does_not_pop(buffer.push_char('H', Some(&'E')));
            does_not_pop(buffer.push_char('E', Some(&'R')));
            does_not_pop(buffer.push_char('R', Some(&'E')));
            pops_token(buffer.push_char('E', None), WHERE, 8);
        }

        fn does_not_pop(result: Result<Option<Token>, LexingError>) {
            assert!(
                result.is_ok(),
                "Expecting result to be Ok, but was Err: {:?}",
                result.err().unwrap()
            );
            let ok_result = result.unwrap();
            assert!(
                ok_result.is_none(),
                "Expecting result to be None but was Some {:?}",
                ok_result
            );
        }

        fn pops_token(
            result: Result<Option<Token>, LexingError>,
            expected_type: TokenTypes,
            expected_column: u32,
        ) {
            assert!(
                result.is_ok(),
                "Expecting result to be Ok, but was Err: {:?}",
                result.err().unwrap()
            );
            let ok_result = result.unwrap();
            assert!(
                ok_result.is_some(),
                "Expecting result to be Some but was None"
            );
            let token = ok_result.unwrap();
            assert_eq!(token.token_type, expected_type);
            assert_eq!(
                token.source_ref.column, expected_column,
                "Token column was wrong"
            );
            assert_eq!(token.source_ref.line, 1, "Current line was wrong");
        }
    }
}
