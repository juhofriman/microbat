use microbat_protocol::data_representation::{Column, Data};
use std::fmt::{Display, Formatter};
use std::time::Duration;

/// Renderable result received from the server
pub enum QueryExecutionResult {
    DataTable(RenderableQueryResult),
    Mutation(RenderableMutationResult),
}

#[allow(dead_code)]
pub enum MutationKind {
    INSERT,
    UPDATE,
    DELETE,
}

impl Display for MutationKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MutationKind::INSERT => write!(f, "Inserted"),
            MutationKind::UPDATE => write!(f, "Updated"),
            MutationKind::DELETE => write!(f, "Deleted"),
        }
    }
}

pub struct RenderableMutationResult {
    kind: MutationKind,
    rows_affected: u32,
    time: Duration,
}

impl Display for RenderableMutationResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} rows\n\n", self.kind, self.rows_affected)?;
        write!(f, "Query took {} ms.", self.time.as_millis())
    }
}

impl RenderableMutationResult {
    /// Creates new RenderableMutationResult
    pub fn new(kind: MutationKind, rows_affected: u32, time: Duration) -> Self {
        RenderableMutationResult {
            kind,
            rows_affected,
            time,
        }
    }
}

/// Renderable query result that is a table
pub struct RenderableQueryResult {
    columns: Vec<Column>,
    rows: Vec<Vec<Data>>,
    time: Duration,
    paddings: Vec<usize>,
}

/// RenderableQueryResult implements Display
impl Display for RenderableQueryResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.top_and_bottom_line(f)?;
        self.columns(f)?;
        self.top_and_bottom_line(f)?;
        self.data_rows(f)?;
        self.top_and_bottom_line(f)?;
        write!(f, "\n({} rows)\n\n", self.row_count())?;
        write!(f, "Query took {} ms.", self.time.as_millis())
    }
}

/// Utility methods for calculating paddings and such
impl RenderableQueryResult {
    /// Creates new RenderableQueryResults and calculates paddings for each column based
    /// on the lenght of the data in guven column.
    pub fn new(columns: Vec<Column>, rows: Vec<Vec<Data>>, time: Duration) -> Self {
        let paddings = RenderableQueryResult::paddings(&columns, &rows);
        RenderableQueryResult {
            columns,
            rows,
            time,
            paddings,
        }
    }

    /// How any rows are in this result
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    fn paddings(columns: &Vec<Column>, rows: &Vec<Vec<Data>>) -> Vec<usize> {
        let mut paddings: Vec<usize> = vec![];

        for (index, column) in columns.iter().enumerate() {
            let mut longest = column.name.len();
            for data in rows {
                match &data[index] {
                    Data::Varchar(d) => {
                        if d.len() > longest {
                            longest = d.len();
                        }
                    }
                    Data::Integer(value) => {
                        let lenght = value.to_string().len();
                        if lenght > longest {
                            longest = lenght;
                        }
                    }
                    Data::Null => {
                        if 4 > longest {
                            longest = 4
                        }
                    }
                }
            }
            paddings.push(longest + 1);
        }

        paddings
    }

    fn top_and_bottom_line(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "-")?;
        for (index, _column) in self.columns.iter().enumerate() {
            write!(f, "-{}-", "-".repeat(self.paddings[index]))?;
        }
        writeln!(f)
    }

    fn columns(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for (index, column) in self.columns.iter().enumerate() {
            write!(f, "|")?;
            write!(f, " {}", column.name)?;
            let padding = self.paddings[index] - column.name.len();
            if padding > 0 {
                write!(f, "{}", " ".repeat(padding))?;
            }
        }
        writeln!(f, "|")
    }

    fn data_rows(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for (_index, row) in self.rows.iter().enumerate() {
            for (index, column) in row.iter().enumerate() {
                match column {
                    Data::Null => {
                        write!(f, "| null")?;
                        let padding = self.paddings[index] - 4;
                        if padding > 0 {
                            write!(f, "{}", " ".repeat(padding))?;
                        }
                    }
                    Data::Varchar(data) => {
                        write!(f, "| {}", data)?;
                        let padding = self.paddings[index] - data.len();
                        if padding > 0 {
                            write!(f, "{}", " ".repeat(padding))?;
                        }
                    }
                    Data::Integer(data) => {
                        write!(f, "| {}", data)?;
                        let padding = self.paddings[index] - data.to_string().len();
                        if padding > 0 {
                            write!(f, "{}", " ".repeat(padding))?;
                        }
                    }
                }
            }
            writeln!(f, "|")?;
        }
        write!(f, "")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use microbat_protocol::data_representation::DataType;

    fn assert_expected_rendering(rendered: String, expected: Vec<&str>) {
        for (index, line) in rendered.split("\n").enumerate() {
            println!("{}", line);
            assert_eq!(
                expected[index], line,
                "Expected lines did not match at index {} \n \nrendered result was\n{}",
                index, rendered
            )
        }
    }

    #[test]
    fn test_render_insert_mutation_result() {
        let result = RenderableMutationResult::new(MutationKind::INSERT, 5, Duration::from_secs(1));

        #[rustfmt::skip]
            let expected = vec![
            "Inserted 5 rows",
            "",
            "Query took 1000 ms.",
            ""
        ];
        assert_expected_rendering(result.to_string(), expected);
    }

    #[test]
    fn test_render_update_mutation_result() {
        let result =
            RenderableMutationResult::new(MutationKind::UPDATE, 10, Duration::from_secs(1));

        #[rustfmt::skip]
            let expected = vec![
            "Updated 10 rows",
            "",
            "Query took 1000 ms.",
            ""
        ];
        assert_expected_rendering(result.to_string(), expected);
    }

    #[test]
    fn test_render_delete_mutation_result() {
        let result = RenderableMutationResult::new(MutationKind::DELETE, 5, Duration::from_secs(1));

        #[rustfmt::skip]
            let expected = vec![
            "Deleted 5 rows",
            "",
            "Query took 1000 ms.",
            ""
        ];
        assert_expected_rendering(result.to_string(), expected);
    }

    #[test]
    fn test_render_empty_result_set_with_one_column() {
        let result = RenderableQueryResult::new(
            vec![Column {
                name: String::from("foo"),
                data_type: DataType::Integer,
            }],
            vec![],
            Duration::from_secs(1),
        );

        #[rustfmt::skip]
        let expected = vec![
            "-------",
            "| foo |",
            "-------",
            "-------",
            "",
            "(0 rows)",
            "",
            "Query took 1000 ms.",
            ""
        ];
        assert_expected_rendering(result.to_string(), expected);
    }

    #[test]
    fn test_render_empty_result_set_with_longer_name() {
        let result = RenderableQueryResult::new(
            vec![Column {
                name: String::from("this_is_long_name"),
                data_type: DataType::Integer,
            }],
            vec![],
            Duration::from_secs(1),
        );

        #[rustfmt::skip]
            let expected = vec![
            "---------------------",
            "| this_is_long_name |",
            "---------------------",
            "---------------------",
            "",
            "(0 rows)",
            "",
            "Query took 1000 ms.",
            ""
        ];
        assert_expected_rendering(result.to_string(), expected);
    }

    #[test]
    fn test_render_result_set_with_one_column_and_one_row() {
        let result = RenderableQueryResult::new(
            vec![Column {
                name: String::from("foo"),
                data_type: DataType::Integer,
            }],
            vec![vec![Data::Integer(1)]],
            Duration::from_secs(1),
        );

        #[rustfmt::skip]
        let expected = vec![
            "-------",
            "| foo |",
            "-------",
            "| 1   |",
            "-------",
            "",
            "(1 rows)",
            "",
            "Query took 1000 ms.",
            ""
        ];
        assert_expected_rendering(result.to_string(), expected);
    }

    #[test]
    fn test_null_value_rendering() {
        let result = RenderableQueryResult::new(
            vec![Column {
                name: String::from("foo"),
                data_type: DataType::Integer,
            }],
            vec![vec![Data::Null]],
            Duration::from_secs(1),
        );

        #[rustfmt::skip]
            let expected = vec![
            "--------",
            "| foo  |",
            "--------",
            "| null |",
            "--------",
            "",
            "(1 rows)",
            "",
            "Query took 1000 ms.",
            ""
        ];
        assert_expected_rendering(result.to_string(), expected);
    }

    #[test]
    fn test_render_result_set_with_long_name() {
        let result = RenderableQueryResult::new(
            vec![Column {
                name: String::from("longer_name"),
                data_type: DataType::Integer,
            }],
            vec![vec![Data::Integer(1)]],
            Duration::from_secs(1),
        );

        #[rustfmt::skip]
        let expected = vec![
            "---------------",
            "| longer_name |",
            "---------------",
            "| 1           |",
            "---------------",
            "",
            "(1 rows)",
            "",
            "Query took 1000 ms.",
            ""
        ];
        assert_expected_rendering(result.to_string(), expected);
    }

    #[test]
    fn test_long_integer_rendering() {
        let result = RenderableQueryResult::new(
            vec![Column {
                name: String::from("a"),
                data_type: DataType::Integer,
            }],
            vec![vec![Data::Integer(24252)]],
            Duration::from_secs(1),
        );

        #[rustfmt::skip]
            let expected = vec![
            "---------",
            "| a     |",
            "---------",
            "| 24252 |",
            "---------",
            "",
            "(1 rows)",
            "",
            "Query took 1000 ms.",
            ""
        ];
        assert_expected_rendering(result.to_string(), expected);
    }

    #[test]
    fn test_render_result_set_with_long_value() {
        let result = RenderableQueryResult::new(
            vec![Column {
                name: String::from("longer_name"),
                data_type: DataType::Varchar,
            }],
            vec![vec![Data::Varchar(String::from(
                "This is even longer value",
            ))]],
            Duration::from_secs(1),
        );

        #[rustfmt::skip]
            let expected = vec![
            "-----------------------------",
            "| longer_name               |",
            "-----------------------------",
            "| This is even longer value |",
            "-----------------------------",
            "",
            "(1 rows)",
            "",
            "Query took 1000 ms.",
            ""
        ];
        assert_expected_rendering(result.to_string(), expected);
    }

    #[test]
    fn test_multiple_columns() {
        let result = RenderableQueryResult::new(
            vec![
                Column {
                    name: String::from("a"),
                    data_type: DataType::Integer,
                },
                Column {
                    name: String::from("a_value"),
                    data_type: DataType::Integer,
                },
            ],
            vec![
                vec![Data::Integer(3), Data::Integer(1234)],
                vec![Data::Integer(5555), Data::Integer(984948)],
            ],
            Duration::from_secs(1),
        );

        #[rustfmt::skip]
            let expected = vec![
            "------------------",
            "| a    | a_value |",
            "------------------",
            "| 3    | 1234    |",
            "| 5555 | 984948  |",
            "------------------",
            "",
            "(2 rows)",
            "",
            "Query took 1000 ms.",
            ""
        ];
        assert_expected_rendering(result.to_string(), expected);
    }
}
