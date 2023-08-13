use std::collections::HashMap;

use microbat_protocol::data::{
    data_values::{DataError, MData},
    table_model::{Column, RelationTable, TableSchema},
};

use crate::sql::expression::{EvaluationError, Expression};

pub trait DatabaseManager {
    fn get_tables(&self) -> Result<Vec<String>, DataError>;
    fn get_table_meta(&self, name: &str) -> Result<&TableMetadata, DataError>;
    fn create_table(&mut self, name: String, columns: Vec<Column>) -> Result<(), DataError>;
    fn insert(&mut self, table_name: &str, colums: Vec<MData>) -> Result<(), DataError>;
    fn fetch(&self, table_name: &str) -> Result<Vec<Vec<MData>>, DataError>;
    fn query(
        &self,
        table_name: &str,
        projection: Vec<Box<dyn Expression>>,
    ) -> Result<RelationTable, DataError>;
}

#[derive(Debug)]
pub struct TableMetadata {
    pub name: String,
    pub schema: TableSchema,
}

pub struct InMemoryManager {
    tables: HashMap<String, TableMetadata>,
    data: HashMap<String, Vec<Vec<MData>>>,
}

impl InMemoryManager {
    pub fn new() -> InMemoryManager {
        InMemoryManager {
            tables: HashMap::new(),
            data: HashMap::new(),
        }
    }
}

impl DatabaseManager for InMemoryManager {
    fn get_tables(&self) -> Result<Vec<String>, DataError> {
        let mut tables: Vec<String> = vec![];
        for (_, table) in self.tables.keys().enumerate() {
            tables.push(table.clone());
        }
        Ok(tables)
    }

    fn get_table_meta(&self, name: &str) -> Result<&TableMetadata, DataError> {
        match self.tables.get(name) {
            Some(table_metadata) => Ok(table_metadata),
            None => Err(DataError {
                msg: format!("No such table: {}", name),
            }),
        }
    }

    fn create_table(&mut self, name: String, columns: Vec<Column>) -> Result<(), DataError> {
        if self.tables.contains_key(&name) {
            return Err(DataError {
                msg: format!("Table already exists: {}", name),
            });
        }
        let table_metadata = TableMetadata {
            name: name.clone(),
            schema: TableSchema::new(columns)?,
        };
        self.tables.insert(name.clone(), table_metadata);
        self.data.insert(name.clone(), vec![]);
        Ok(())
    }

    fn insert(&mut self, table_name: &str, colums: Vec<MData>) -> Result<(), DataError> {
        let table_metadata = self.get_table_meta(table_name)?;
        for (index, column) in table_metadata.schema.columns.iter().enumerate() {
            match colums.get(index) {
                Some(data) => {
                    if column.data_type != data.matcher() {
                        return Err(DataError {
                            msg: String::from("Can't put this here"),
                        });
                    }
                }
                None => {
                    return Err(DataError {
                        msg: String::from("Column count mismatch"),
                    })
                }
            }
        }
        self.data.get_mut(table_name).unwrap().push(colums);
        Ok(())
    }

    fn fetch(&self, table_name: &str) -> Result<Vec<Vec<MData>>, DataError> {
        self.get_table_meta(table_name)?;
        let mut result: Vec<Vec<MData>> = vec![];
        for row in self.data.get(table_name).unwrap() {
            let mut clone_row: Vec<MData> = vec![];
            for item in row {
                clone_row.push(item.clone());
            }
            result.push(clone_row);
        }
        Ok(result)
    }

    fn query(
        &self,
        table_name: &str,
        projection: Vec<Box<dyn Expression>>,
    ) -> Result<RelationTable, DataError> {
        let table_meta = self.get_table_meta(table_name)?;

        let mut schema_columns = vec![];

        for (index, expr) in projection.iter().enumerate() {
            schema_columns.push(expr.schema_column(&table_meta.schema, index)?);
        }

        let mut relation = RelationTable::new(TableSchema::new(schema_columns)?);

        for row in self.fetch(table_name)?.iter() {
            let mut relation_row = vec![];
            for expr in projection.iter() {
                relation_row.push(expr.eval(&table_meta.schema, row)?);
            }
            relation.push_row(relation_row)?;
        }
        Ok(relation)
    }
}

impl From<EvaluationError> for DataError {
    fn from(value: EvaluationError) -> Self {
        Self { msg: value.msg }
    }
}

#[cfg(test)]
mod in_memory_db_tests {
    use super::*;
    use microbat_protocol::data::data_values::MDataType;

    #[test]
    fn test_no_such_table() {
        let manager = InMemoryManager::new();
        let res = manager.get_table_meta("foo");
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().msg, "No such table: foo");
    }

    #[test]
    fn test_fetching_table() {
        let mut manager = InMemoryManager::new();

        let create_res = manager.create_table(
            String::from("foo"),
            vec![Column {
                name: String::from("id"),
                data_type: MDataType::Integer,
            }],
        );
        assert!(create_res.is_ok());

        let res = manager.get_table_meta("foo");
        assert!(res.is_ok());
        let table_metadata = res.unwrap();
        assert_eq!(table_metadata.name, "foo");
        assert_eq!(table_metadata.schema.len(), 1);
    }

    #[test]
    fn test_can_not_create_existing_table() {
        let mut manager = InMemoryManager::new();

        let create_res = manager.create_table(
            String::from("foo"),
            vec![Column {
                name: String::from("id"),
                data_type: MDataType::Integer,
            }],
        );
        assert!(create_res.is_ok());

        let fails = manager.create_table(
            String::from("foo"),
            vec![Column {
                name: String::from("id"),
                data_type: MDataType::Integer,
            }],
        );
        assert!(fails.is_err());
        assert_eq!(fails.unwrap_err().msg, "Table already exists: foo");
    }

    #[test]
    fn test_insert() {
        let mut manager = InMemoryManager::new();

        let create_res = manager.create_table(
            String::from("foo"),
            vec![Column {
                name: String::from("id"),
                data_type: MDataType::Integer,
            }],
        );
        assert!(create_res.is_ok());

        let insert_result = manager.insert("foo", vec![MData::Integer(1)]);
        assert!(insert_result.is_ok());
        let table_data = manager.fetch("foo").unwrap();
        assert_eq!(table_data.len(), 1);
    }

    #[test]
    fn test_insert_when_schema_does_not_match() {
        let mut manager = InMemoryManager::new();

        let create_res = manager.create_table(
            String::from("foo"),
            vec![Column {
                name: String::from("id"),
                data_type: MDataType::Integer,
            }],
        );
        assert!(create_res.is_ok());

        let insert_result = manager.insert("foo", vec![MData::Varchar(String::from("hello"))]);
        assert!(insert_result.is_err());
        assert_eq!(insert_result.unwrap_err().msg, "Can't put this here");
    }
}
