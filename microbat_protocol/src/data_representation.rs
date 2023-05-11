use crate::static_values::{TYPE_BYTE_INTEGER, TYPE_BYTE_NULL, TYPE_BYTE_VARCHAR};
use crate::MicrobatProtocolError;
use std::fmt::{Display, Formatter};

/// Serializable data description of incoming rows in result set.
#[derive(PartialEq, Debug)]
pub struct DataDescription {
    pub columns: Vec<Column>,
}

/// Column in result relation
#[derive(PartialEq, Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

/// One row in result set
#[derive(PartialEq, Debug)]
pub struct DataRow {
    pub columns: Vec<Data>,
}

#[derive(Debug)]
pub struct DataError {
    pub msg: String,
}

impl Display for DataError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Expression error: {}", self.msg)
    }
}

/// All datatypes without the actual values. This is usable for pattern matching different values.
///
/// See `matcher(&self)` in `Data` implementation.
#[derive(Debug, PartialEq, Clone)]
pub enum DataType {
    Null,
    Integer,
    Varchar,
}

/// The serializable data types of microbat. This is one field in microbat, like an integer.
///
/// This enum knows how to represent field as bytes, see `bytes(&self)`. It also must be able
/// to return corresponding marker byte constant.
#[derive(PartialEq, Debug, Clone)]
pub enum Data {
    Null,
    // TODO: maybe this should be i32 :D
    Integer(u32),
    Varchar(String),
}

impl Data {
    pub fn bytes(&self) -> Vec<u8> {
        match self {
            Data::Null => vec![],
            Data::Varchar(value) => value.as_bytes().to_vec(),
            Data::Integer(value) => value.to_be_bytes().to_vec(),
        }
    }

    pub fn type_byte(&self) -> u8 {
        match self {
            Data::Null => TYPE_BYTE_NULL,
            Data::Varchar(_) => TYPE_BYTE_VARCHAR,
            Data::Integer(_) => TYPE_BYTE_INTEGER,
        }
    }
    pub fn matcher(&self) -> DataType {
        match self {
            Data::Null => DataType::Null,
            Data::Integer(_) => DataType::Integer,
            Data::Varchar(_) => DataType::Varchar,
        }
    }

    pub fn apply_plus(&self, right: Data) -> Result<Data, DataError> {
        match (self, &right) {
            (Data::Integer(l_value), Data::Integer(r_value)) => {
                Ok(Data::Integer(l_value + r_value))
            }
            _ => Err(DataError {
                msg: format!("Can't apply {:?} + {:?}", self, right),
            }),
        }
    }

    pub fn apply_minus(&self, right: Data) -> Result<Data, DataError> {
        match (self, &right) {
            (Data::Integer(l_value), Data::Integer(r_value)) => {
                Ok(Data::Integer(l_value - r_value))
            }
            _ => Err(DataError {
                msg: format!("Can't apply {:?} + {:?}", self, right),
            }),
        }
    }
}

pub fn deserialize_data_column(marker: u8, bytes: &[u8]) -> Result<Data, MicrobatProtocolError> {
    match marker {
        TYPE_BYTE_NULL => Ok(Data::Null),
        TYPE_BYTE_INTEGER => {
            let value = u32::from_be_bytes(bytes.try_into().unwrap());
            Ok(Data::Integer(value))
        }
        TYPE_BYTE_VARCHAR => {
            let value = String::from_utf8(bytes.to_vec())?;
            Ok(Data::Varchar(value))
        }
        unknown => Err(MicrobatProtocolError {
            msg: format!("Unknown data column marker {}", char::from(unknown)),
        }),
    }
}

#[cfg(test)]
mod serialization_tests {
    use super::*;

    // TODO Impl Display to display results (possibly in client?)

    #[test]
    fn test_type_bytes() {
        assert_eq!(
            Data::Varchar(String::from("")).type_byte(),
            TYPE_BYTE_VARCHAR
        );
        assert_eq!(
            Data::Varchar(String::from("foo")).type_byte(),
            TYPE_BYTE_VARCHAR
        );
        assert_eq!(Data::Integer(1).type_byte(), TYPE_BYTE_INTEGER);
    }

    #[test]
    fn test_bytes() {
        assert_eq!(Data::Null.bytes().len(), 0);
        assert_eq!(Data::Varchar(String::from("")).bytes().len(), 0);
        assert_eq!(Data::Varchar(String::from("foo")).bytes().len(), 3);
        assert_eq!(Data::Integer(1).bytes().len(), 4);
        assert_eq!(Data::Integer(5).bytes().len(), 4);
    }

    #[test]
    fn test_serialize_and_deserialize_null() {
        let bytes = Data::Null.bytes();
        let deserialized = deserialize_data_column(TYPE_BYTE_NULL, &bytes);
        assert!(deserialized.is_ok());
        if let Data::Null = deserialized.unwrap() {
        } else {
            panic!("Null deserialized to something else than null");
        }
    }

    #[test]
    fn test_serialize_and_deserialize_varchar() {
        let value = "abba";
        let bytes = Data::Varchar(String::from(value)).bytes();
        let deserialized = deserialize_data_column(TYPE_BYTE_VARCHAR, &bytes);
        assert!(deserialized.is_ok());
        if let Data::Varchar(des_value) = deserialized.unwrap() {
            assert_eq!(des_value, value);
        } else {
            panic!("Varchar deserialized to something else than varchar");
        }
    }

    #[test]
    fn test_serialize_and_deserialize_integer() {
        let value = 123;
        let bytes = Data::Integer(value).bytes();
        let deserialized = deserialize_data_column(TYPE_BYTE_INTEGER, &bytes);
        assert!(deserialized.is_ok());
        if let Data::Integer(des_value) = deserialized.unwrap() {
            assert_eq!(des_value, value);
        } else {
            panic!("Integer deserialized to something else than varchar");
        }
    }
}
