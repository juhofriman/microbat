use crate::MicrobatProtocolError;

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
    // TODO: maybe this should be i32 :D
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
    pub fn marker_byte(&self) -> u8 {
        match self {
            Data::Varchar(_) => DATA_MARKER_VARCHAR,
            Data::Integer(_) => DATA_MARKER_INTEGER,
        }
    }
}

pub const DATA_MARKER_INTEGER: u8 = b'i';
pub const DATA_MARKER_VARCHAR: u8 = b'v';

pub fn deserialize_data_column(marker: u8, bytes: &[u8]) -> Result<Data, MicrobatProtocolError> {
    match marker {
        DATA_MARKER_INTEGER => {
            let value = u32::from_be_bytes(bytes.try_into().unwrap());
            Ok(Data::Integer(value))
        }
        DATA_MARKER_VARCHAR => {
            let value = String::from_utf8(bytes.to_vec())?;
            Ok(Data::Varchar(value))
        }
        unknown => Err(MicrobatProtocolError {
            msg: format!("Unknown data column marker {}", char::from(unknown)),
        }),
    }
}
