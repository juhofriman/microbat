/// Tokens available for parser
#[derive(Debug, PartialEq)]
pub enum Token {
    CREATE,
    TABLE,
    VALUES,

    SELECT,
    INSERT,
    UPDATE,
    DELETE,
    FROM,
    AS,

    COMMA,
    LPARENS,
    RPARENS,
    PLUS,
    MINUS,
    MULTIPLICATION,
    DIVISION,

    STRING(String),
    INTEGER(u32),
    FLOAT(f32),

    IDENTIFIER(String),
}

/// Stateful lexer instance for lexing a piece od SQL.
///
/// Note that the lexer will panic of next() is called on fully
/// consumed lexer. Always check with has_next().
///
/// next() returns a reference and not owned Token and thus make hard copies
/// of the data while parsing.
#[derive(Debug)]
pub struct Lexer {
    current_position: usize,
    tokens: Vec<Token>,
}

impl Lexer {

    /// Creates a new lexer instance with given input.
    ///
    /// Lexing happens eagerly and thus this returns a Result.
    pub fn with_input(sql: String) -> Result<Self, LexingError> {
        let mut tokens = vec![];
        let mut buffer = buffer::LexerBuffer::new();
        let mut chars = sql.chars().peekable();
        while let Some(char) = chars.next() {
            if let Some(token) = buffer.push_char(char, chars.peek()) {
                tokens.push(token?)
            }
        }
        if tokens.len() == 0 {
            return Err(LexingError::new(LexingErrorKind::NoTokens));
        }
        Ok(Lexer {
            tokens,
            current_position: 0,
        })
    }

    /// Returns a reference to the next token.
    ///
    /// Panics if lexer is consumed, thus use has_next to check if there
    /// actually is a next token.
    pub fn next(&mut self) -> &Token {
        if self.current_position + 1 > self.tokens.len() {
            panic!("Lexer already consumed to the end");
        }
        let token = &self.tokens[self.current_position];
        self.current_position += 1;
        token
    }

    /// Checks if lexer has more tokens
    pub fn has_next(&self) -> bool {
        self.current_position < self.tokens.len()
    }
}

/// Error occuring during the lexing phase
#[derive(Debug)]
pub struct LexingError {
    pub kind: LexingErrorKind,
}

impl LexingError {
    fn new(kind: LexingErrorKind) -> Self {
        Self { kind }
    }
}

#[derive(Debug, PartialEq)]
pub enum LexingErrorKind {
    NoTokens,
    NotInteger,
    StringNotTerminated,
}

/// Internal buffer module for pushing characters and popping tokens.
mod buffer {

    use super::*;

    #[derive(Debug, PartialEq)]
    enum LexingMode {
        Normal,
        String,
        Integer,
        Float,
    }

    pub struct LexerBuffer {
        mode: LexingMode,
        buffer: String,
    }

    impl LexerBuffer {

        /// Creates a new LexerBuffer instance
        pub fn new() -> Self {
            Self {
                buffer: String::new(),
                mode: LexingMode::Normal,
            }
        }

        /// Pushes a new character to the buffer. Returns None if there is no ready token.
        ///
        /// Note that Some value is a Result as there might be an error during lexing.
        pub fn push_char(
            &mut self,
            char: char,
            peek: Option<&char>,
        ) -> Option<Result<Token, LexingError>> {
            // Toggle integer mode if char is digit, current lexing mode is normal and buffer is empty
            // This allows digits inside identifiers, but identifier can't start with a digit
            if char.is_numeric() && self.mode == LexingMode::Normal && self.buffer.is_empty() {
                self.mode = LexingMode::Integer;
            }
            if char == '.' && self.mode == LexingMode::Integer {
                self.mode = LexingMode::Float;
            }
            if char == '\'' && self.mode != LexingMode::String {
                self.mode = LexingMode::String;
                return None;
            }
            match self.mode {
                LexingMode::Normal => {
                    if char.is_whitespace() {
                        return None;
                    }
                    self.buffer.push(char);
                    if self.is_delimiting(Some(&char)) {
                        return Some(Ok(self.pop_token()));
                    }
                    match self.is_delimiting(peek) {
                        true => Some(Ok(self.pop_token())),
                        false => None,
                    }
                }
                LexingMode::Integer => {
                    if !char.is_numeric() {
                        return Some(Err(LexingError::new(LexingErrorKind::NotInteger)));
                    }
                    self.buffer.push(char);
                    match self.is_delimiting(peek) {
                        true => Some(Ok(self.pop_token())),
                        false => None,
                    }
                }
                LexingMode::Float => {
                    if !char.is_numeric() && char != '.' {
                        return Some(Err(LexingError::new(LexingErrorKind::NotInteger)));
                    }
                    self.buffer.push(char);
                    match self.is_delimiting(peek) {
                        true => Some(Ok(self.pop_token())),
                        false => None,
                    }
                }
                LexingMode::String => {
                    // The string ends here
                    if char == '\'' {
                        return Some(Ok(self.pop_token()));
                    }
                    // Reached the end of input and string is not terminated
                    if peek.is_none() {
                        return Some(Err(LexingError::new(LexingErrorKind::StringNotTerminated)));
                    }
                    self.buffer.push(char);
                    None
                }
            }
        }

        /// Tells if given character is delimitting.
        fn is_delimiting(&self, char: Option<&char>) -> bool {
            if let Some(c) = char {
                if c.is_whitespace() {
                    return true;
                }
                return match c {
                    ',' => true,
                    '(' => true,
                    ')' => true,
                    '+' => true,
                    '-' => true,
                    '*' => true,
                    '/' => true,
                    _ => false,
                };
            }
            true
        }

        /// Pops a new Token out of this buffer and resets the buffer.
        fn pop_token(&mut self) -> Token {
            let token = match self.mode {
                LexingMode::Normal => match self.buffer.to_uppercase().as_str() {
                    "CREATE" => Token::CREATE,
                    "TABLE" => Token::TABLE,
                    "VALUES" => Token::VALUES,
                    "SELECT" => Token::SELECT,
                    "INSERT" => Token::INSERT,
                    "UPDATE" => Token::UPDATE,
                    "DELETE" => Token::DELETE,
                    "FROM" => Token::FROM,
                    "AS" => Token::AS,
                    "," => Token::COMMA,
                    "(" => Token::LPARENS,
                    ")" => Token::RPARENS,
                    "+" => Token::PLUS,
                    "-" => Token::MINUS,
                    "*" => Token::MULTIPLICATION,
                    "/" => Token::DIVISION,
                    value => Token::IDENTIFIER(value.to_string()),
                },
                LexingMode::String => {
                    Token::STRING(self.buffer.to_owned())
                }
                LexingMode::Integer => {
                    Token::INTEGER(self.buffer.parse().expect("This won't happen"))
                }
                LexingMode::Float => Token::FLOAT(self.buffer.parse().expect("This won't happen")),
            };
            self.buffer = String::new();
            self.mode = LexingMode::Normal;
            token
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! assert_lexer_errors_on {
        ($s:literal, $e:expr) => {
            assert_lexer_error(String::from($s), $e);
        };
    }

    macro_rules! assert_lexing {
        ( $s:literal, $( $x:expr ),* ) => {
            {
                let mut expected_tokens = Vec::new();
                $(
                    expected_tokens.push($x);
                )*
                assert_lexer_test(String::from($s), expected_tokens);
            }
        };
    }

    #[test]
    fn test_lexing_errors() {
        assert_lexer_errors_on!("", LexingErrorKind::NoTokens);
        assert_lexer_errors_on!(" ", LexingErrorKind::NoTokens);
        assert_lexer_errors_on!("   ", LexingErrorKind::NoTokens);
        assert_lexer_errors_on!("\t", LexingErrorKind::NoTokens);

        assert_lexer_errors_on!("1d", LexingErrorKind::NotInteger);
        assert_lexer_errors_on!("12foo", LexingErrorKind::NotInteger);

        assert_lexer_errors_on!("'foo", LexingErrorKind::StringNotTerminated);
        assert_lexer_errors_on!("'foo bar", LexingErrorKind::StringNotTerminated);

        // TODO: Corner cases
        // assert_lexer_errors_on!("foo'", LexingErrorKind::StringNotTerminated);
    }

    #[test]
    fn test_lexing_single_token() {
        // Reserved words
        assert_lexing!("select", Token::SELECT);
        assert_lexing!("SELECT", Token::SELECT);
        assert_lexing!("SeLeCt", Token::SELECT);
        assert_lexing!("insert", Token::INSERT);

        assert_lexing!("create", Token::CREATE);
        assert_lexing!("table", Token::TABLE);
        assert_lexing!("values", Token::VALUES);
        assert_lexing!("select", Token::SELECT);
        assert_lexing!("insert", Token::INSERT);
        assert_lexing!("update", Token::UPDATE);
        assert_lexing!("delete", Token::DELETE);
        assert_lexing!("from", Token::FROM);
        assert_lexing!("as", Token::AS);

        // Dividers
        assert_lexing!(",", Token::COMMA);

        // Operators
        assert_lexing!("(", Token::LPARENS);
        assert_lexing!(")", Token::RPARENS);
        assert_lexing!("+", Token::PLUS);
        assert_lexing!("-", Token::MINUS);
        assert_lexing!("*", Token::MULTIPLICATION);
        assert_lexing!("/", Token::DIVISION);

        // Integers
        assert_lexing!("1", Token::INTEGER(1));
        assert_lexing!("1234", Token::INTEGER(1234));
        assert_lexing!("666", Token::INTEGER(666));

        // Floats
        assert_lexing!("1.1", Token::FLOAT(1.1));
        assert_lexing!("1134.531", Token::FLOAT(1134.531));
        assert_lexing!("42.53135", Token::FLOAT(42.53135));
        assert_lexing!("123.", Token::FLOAT(123.0)); // inconvenience but makes things more simple

        // Strings
        assert_lexing!("''", Token::STRING(String::from("")));
        assert_lexing!("'Foo'", Token::STRING(String::from("Foo")));
        assert_lexing!("'Foo bar'", Token::STRING(String::from("Foo bar")));

        // Identifiers
        assert_lexing!("foo", Token::IDENTIFIER(String::from("FOO")));
        assert_lexing!("foo1", Token::IDENTIFIER(String::from("FOO1")));
    }

    #[test]
    fn test_token_continuations() {
        assert_lexing!(
            "foo,bar",
            Token::IDENTIFIER(String::from("FOO")),
            Token::COMMA,
            Token::IDENTIFIER(String::from("BAR"))
        );

        assert_lexing!(
            "( 1 + 1 )",
            Token::LPARENS,
            Token::INTEGER(1),
            Token::PLUS,
            Token::INTEGER(1),
            Token::RPARENS
        );

        assert_lexing!(
            "(1+1)",
            Token::LPARENS,
            Token::INTEGER(1),
            Token::PLUS,
            Token::INTEGER(1),
            Token::RPARENS
        );

        assert_lexing!(
            "(1 * (2+ 5) )",
            Token::LPARENS,
            Token::INTEGER(1),
            Token::MULTIPLICATION,
            Token::LPARENS,
            Token::INTEGER(2),
            Token::PLUS,
            Token::INTEGER(5),
            Token::RPARENS,
            Token::RPARENS
        );
    }

    #[test]
    fn test_multi_token_clauses() {
        assert_lexing!(
            "select foo, bar from baz",
            Token::SELECT,
            Token::IDENTIFIER(String::from("FOO")),
            Token::COMMA,
            Token::IDENTIFIER(String::from("BAR")),
            Token::FROM,
            Token::IDENTIFIER(String::from("BAZ"))
        );
        assert_lexing!(
            "SELECT foo",
            Token::SELECT,
            Token::IDENTIFIER(String::from("FOO"))
        );
        assert_lexing!(
            "select fOo",
            Token::SELECT,
            Token::IDENTIFIER(String::from("FOO"))
        );
        assert_lexing!(
            "select 123, 42 as foo",
            Token::SELECT,
            Token::INTEGER(123),
            Token::COMMA,
            Token::INTEGER(42),
            Token::AS,
            Token::IDENTIFIER(String::from("FOO"))
        );
    }

    #[test]
    fn test_has_next() {
        let mut lexer = Lexer::with_input(String::from("select insert update")).expect("No");
        assert!(lexer.has_next());
        lexer.next();
        assert!(lexer.has_next());
        lexer.next();
        assert!(lexer.has_next());
        lexer.next();
        assert!(!lexer.has_next(), "Lexer says has_next when all consumed");
    }

    #[test]
    #[should_panic(expected="Lexer already consumed to the end")]
    fn test_lexer_next_panics() {
        let mut lexer = Lexer::with_input(String::from("select")).expect("No");
        lexer.next();
        lexer.next();
    }

    fn assert_lexer_test(input: String, expected_tokens: Vec<Token>) {
        let mut lexer = Lexer::with_input(input.clone()).expect(
            format!(
                "Could not construct lexer from given input: '{}'. Error: ",
                input.clone()
            )
            .as_str(),
        );
        let expected_token_count = expected_tokens.len().to_owned();
        for (position, expected_token) in expected_tokens.into_iter().enumerate() {
            assert_eq!(
                *lexer.next(),
                expected_token,
                "Tokens did not match at position {:?} for input <{}>",
                position,
                input
            );
            if position < expected_token_count - 1 {
                assert!(
                    lexer.has_next(),
                    "Expecting more tokens but has_next() returns false"
                );
            } else {
                assert!(
                    !lexer.has_next(),
                    "After final expected token lexer has more tokens"
                );
            }
        }
    }

    fn assert_lexer_error(input: String, expected_kind: LexingErrorKind) {
        let error = Lexer::with_input(input.clone())
            .expect_err(format!("Expecting error for input '{}', but got Ok", input).as_str());
        assert_eq!(
            error.kind, expected_kind,
            "Received unexpected error, input '{}'",
            input
        );
    }
}
