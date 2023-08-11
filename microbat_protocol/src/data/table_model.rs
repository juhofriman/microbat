use super::data_values::{MData, MDataType};

/// Serializable data description of incoming rows in result set.
#[derive(PartialEq, Debug)]
pub struct DataDescription {
    pub columns: Vec<Column>,
}

/// Column in result relation
#[derive(PartialEq, Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: MDataType,
}

/// One row in result set
#[derive(PartialEq, Debug)]
pub struct DataRow {
    pub columns: Vec<MData>,
}

