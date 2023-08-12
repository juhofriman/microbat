pub mod manager;

use std::{
    sync::{Arc, RwLock},
    vec,
};

use microbat_protocol::data::{
    data_values::{MData, MDataType, DataError},
    table_model::{Column, DataRow, TableSchema, RelationTable},
};

use crate::sql::parser::{
    parse_sql, ParseError,
    SqlClause::{Select, ShowTables},
};

use self::manager::{DatabaseManager};

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

impl From<DataError> for MicrobatQueryError {
    fn from(value: DataError) -> Self {
        MicrobatQueryError {
            msg: format!("{}", value.msg),
        }
    }
}

pub enum QueryResult {
    Table(TableSchema, Vec<DataRow>),
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
                    columns: vec![MData::Varchar(table)],
                })
            }

            Ok(QueryResult::Table(
                TableSchema {
                    columns: vec![Column {
                        name: String::from("table"),
                        data_type: MDataType::Varchar,
                    }],
                },
                rows,
            ))
        }
        Select(_projection, from) => {
            let database = manager.read().expect("RwLock poisoned");
            let table_meta = database.get_table_meta(from.get(0).unwrap())?;
            let mut relation = RelationTable::new(table_meta.schema.clone());
            let rows = database.fetch(from.get(0).unwrap())?;
            for row in rows.iter() {
                relation.push_row(row.clone())?;
            }
            return Ok(QueryResult::Table(
                relation.schema, 
                relation.rows,
            ));
        }
    }
}
