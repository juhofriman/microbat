use super::data_values::{DataError, MData, MDataType};

/// Serializable data description of incoming rows in result set.
#[derive(PartialEq, Debug)]
pub struct TableSchema {
    // TODO: this should be private
    pub columns: Vec<Column>,
}
impl TableSchema {
    pub fn new(columns: Vec<Column>) -> Result<Self, DataError> {
        if columns.is_empty() {
            return Err(DataError {
                msg: String::from("Can't build empty schema"),
            });
        }
        Ok(TableSchema { columns })
    }

    pub fn matches_at(&self, index: usize, data_type: MDataType) -> bool {
        match self.columns.get(index) {
            Some(column) => column.data_type == data_type,
            None => false, // Ok, this is bad
        }
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn join(&self, other: TableSchema) -> Result<Self, DataError> {
        let mut columns = vec![];
        for c in self.columns.iter() {
            columns.push(Column::new(c.name.clone(), c.data_type.clone()));
        }
        for c in other.columns.iter() {
            columns.push(Column::new(c.name.clone(), c.data_type.clone()));
        }
        Self::new(columns)
    }
}

impl Clone for TableSchema {
    fn clone(&self) -> Self {
        Self {
            columns: self.columns.clone(),
        }
    }
}

/// Column in result relation
#[derive(PartialEq, Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: MDataType,
}

impl Column {
    pub fn new(name: String, data_type: MDataType) -> Self {
        Column { name, data_type }
    }
}

/// One row in result set
#[derive(PartialEq, Debug)]
pub struct DataRow {
    pub columns: Vec<MData>,
}
impl DataRow {
    pub fn new(columns: Vec<MData>) -> DataRow {
        DataRow { columns }
    }
}

pub struct RelationTable {
    pub schema: TableSchema,
    pub rows: Vec<DataRow>,
}

impl RelationTable {
    pub fn new(schema: TableSchema) -> Self {
        RelationTable {
            schema,
            rows: vec![],
        }
    }

    pub fn push_row(&mut self, row: Vec<MData>) -> Result<(), DataError> {
        if row.len() != self.schema.len() {
            return Err(DataError {
                msg: format!(
                    "Trying to put {} columns but schema has {} columns",
                    row.len(),
                    self.schema.len()
                ),
            });
        }
        for (index, data) in row.iter().enumerate() {
            if !self.schema.matches_at(index, data.matcher()) {
                return Err(DataError {
                    msg: format!("Can't put {:?} into index {}", data.matcher(), index),
                });
            }
        }
        self.rows.push(DataRow::new(row));
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }
}

#[cfg(test)]
mod tests {

    macro_rules! t_schema {
        ($ ( $e:expr),+ ) => {
            {
                let mut columns = Vec::new();
                $(
                    columns.push($e);
                )*
                TableSchema::new(columns).unwrap()
        }
        };
    }

    macro_rules! column {
        ( $s:literal, $e:expr) => {
            Column::new(String::from($s), $e)
        };
    }

    use crate::{m_int, m_varchar};

    use super::*;

    mod schema_tests {
        use crate::data::{
            data_values::MDataType,
            table_model::{Column, TableSchema},
        };

        #[test]
        fn test_empty_schema_errors() {
            let error = TableSchema::new(vec![]).unwrap_err();
            assert_eq!(error.msg, "Can't build empty schema")
        }

        #[test]
        fn test_building_ok_schema() {
            let schema = t_schema!(column!("foo", MDataType::Integer));
            assert!(schema.matches_at(0, MDataType::Integer));
            assert!(!schema.matches_at(1, MDataType::Varchar));
        }
    }

    #[test]
    fn test_filling_relation() {
        let mut relation = RelationTable::new(t_schema!(column!("foo", MDataType::Integer)));

        assert_eq!(relation.len(), 0);
        relation.push_row(vec![m_int!(1)]).unwrap();
        assert_eq!(relation.len(), 1);
        relation.push_row(vec![m_int!(2)]).unwrap();
        assert_eq!(relation.len(), 2);
    }

    #[test]
    fn test_adding_unmatching_data_fails() {
        let mut relation = RelationTable::new(t_schema!(column!("foo", MDataType::Integer)));

        assert!(
            relation.push_row(vec![]).is_err(),
            "Expecting putting empty row fails"
        );
        assert!(
            relation.push_row(vec![m_int!(1), m_int!(2)]).is_err(),
            "Expecting putting too many values to fail but it succeeded"
        );
        assert!(
            relation.push_row(vec![m_varchar!("Hello")]).is_err(),
            "Expected pushing varchar to int fail but it succeeded"
        );
    }
}
