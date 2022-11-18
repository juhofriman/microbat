/// Serializable row description which currently
/// defines the names of columns in indexes
#[derive(PartialEq, Debug)]
pub struct RowDescription {
    pub rows: Vec<Column>,
}

/// Column in result relation, has only name at the moment.
#[derive(PartialEq, Debug)]
pub struct Column {
    pub name: String,
}

/// One line in result set
#[derive(PartialEq, Debug)]
pub struct DataRow {
    pub columns: Vec<Data>,
}

/// Piece of data, data must be able to tell it's represented bytes.
#[derive(PartialEq, Debug)]
pub enum Data {
    Integer(u32),
    Varchar(String),
}

impl Data {
    pub fn bytes(&self) -> Vec<u8> {
        match self {
            Data::Varchar(value) => value.as_bytes().to_vec(),
            Data::Integer(value) => value.to_be_bytes().to_vec(),
        }
    }
    pub fn length_and_bytes(&self) -> Vec<u8> {
        let mut bytes = self.bytes();
        bytes.insert(0, bytes.len() as u8);
        bytes
    }
}

pub const DATA_MARKER_INTEGER: u8 = b'i';
pub const DATA_MARKER_VARCHAR: u8 = b'v';
