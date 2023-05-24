pub mod manager;

use std::{sync::{Arc, RwLock}, vec};

use crate::sql::parser::{
    parse_sql, ParseError,
    SqlClause::{Select, ShowTables},
};
use microbat_protocol::data_representation::{Column, Data, DataDescription, DataRow, DataType};

use self::manager::{DatabaseManager, MicrobatDataError};

pub struct MicrobatQueryError {
    pub msg: String,
}

impl From<ParseError> for MicrobatQueryError {
    fn from(value: ParseError) -> Self {
        MicrobatQueryError {
            msg: format!("{}", value),
        }
    }
}

impl From<MicrobatDataError> for MicrobatQueryError {
    fn from(value: MicrobatDataError) -> Self {
        MicrobatQueryError {
            msg: format!("{}", value.msg),
        }
    }
}

pub enum QueryResult {
    Table(DataDescription, Vec<DataRow>),
}

pub fn execute_sql(
    sql: String,
    manager: &Arc<RwLock<impl DatabaseManager>>,
) -> Result<QueryResult, MicrobatQueryError> {
    match parse_sql(sql)? {
        ShowTables => {
            let database = manager.read().expect("RwLock poisoned");
            let mut rows = vec![];
            for table in database.get_tables()? {
                rows.push(DataRow {
                    columns: vec![Data::Varchar(table)],
                })
            }

            Ok(QueryResult::Table(DataDescription {
                columns: vec![Column {
                    name: String::from("table"),
                    data_type: DataType::Varchar,
                }]
            }, rows)) 
        }
        Select(projection, from) => {
            let database = manager.read().expect("RwLock poisoned");

            let table = database.fetch(from.get(0).unwrap())?;
            let mut columns: Vec<Column> = vec![];
            let mut data_rows: Vec<Data> = vec![];
            for row in table.into_iter() {
                for (index, expr) in projection.iter().enumerate() {
                    match expr.eval() {
                        Ok(val) => match val {
                            Data::Integer(v) => {
                                let mut name = String::from("column_");
                                name.push_str(index.to_string().as_str());
                                columns.push(Column {
                                    name,
                                    data_type: DataType::Integer,
                                });
                                data_rows.push(Data::Integer(v))
                            }
                            _ => panic!(),
                        },
                        Err(_) => panic!(),
                    }
                }
            }
            println!("{:?}", from);
            return Ok(QueryResult::Table(
                DataDescription { columns },
                vec![DataRow { columns: data_rows }],
            ));
        }
    }
}
