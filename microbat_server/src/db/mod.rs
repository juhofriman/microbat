use microbat_protocol::data_representation::{DataDescription, DataRow, DataType, Column, Data};
use crate::sql::{parser::{parse_sql, SqlClause::{ShowTables, Select}}};

pub enum QueryResult {
    Table(DataDescription, Vec<DataRow>)
}
pub fn execute_sql(sql: String) -> Result<QueryResult, String> {   
    match parse_sql(sql) {
        Ok(ast) => {
            match ast {
                ShowTables(_) => todo!(),
                Select(projection) => {
                    let mut columns: Vec<Column> = vec![];
                    let mut data_rows: Vec<Data> = vec![];
                    for (index, expr) in projection.into_iter().enumerate() {
                        match expr.eval() {
                            Ok(val) => {
                                match val {
                                   Data::Integer(v) => {
                                        let mut name = String::from("column_");
                                        name.push_str(index.to_string().as_str());
                                        columns.push(Column {
                                            name,
                                            data_type: DataType::Integer,
                                        });
                                        data_rows.push(Data::Integer(v))
                                    },
                                    _ => panic!(),
                                }
                            }, 
                            Err(_) => panic!(),
                        }
                    }
                    return Ok(QueryResult::Table(DataDescription { columns, }, vec![
                        DataRow {
                            columns: data_rows,
                        }
                    ]))
                }
            }
        }, 
        Err(_) => panic!(),
    }
}
