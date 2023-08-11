use crate::{
    static_values as values, MicrobatProtocolError, data::{table_model::{DataDescription, Column, DataRow}, data_values::{MDataType, deserialize_data_column}}
};
use std::fmt::{Display, Formatter};

use super::MicrobatMessage;

/// Enum of messages that can originate from the server
#[derive(Debug, PartialEq)]
pub enum MicrobatServerMessage {
    Handshake,
    Error(String),
    DataDescription(DataDescription),
    DataRow(DataRow),
    InsertResult(u32),
    Ready,
}

impl Display for MicrobatServerMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MicrobatServerMessage::Handshake => write!(f, "Handshake"),
            MicrobatServerMessage::Error(_) => write!(f, "Error"),
            MicrobatServerMessage::DataDescription(_) => write!(f, "DataDescription"),
            MicrobatServerMessage::DataRow(_) => write!(f, "DataRow"),
            MicrobatServerMessage::InsertResult(_) => write!(f, "InsertResult"),
            MicrobatServerMessage::Ready => write!(f, "Ready")
        }
    }
}

impl MicrobatMessage for MicrobatServerMessage {
    fn as_bytes(&self) -> Vec<u8> {
        match self {
            MicrobatServerMessage::Handshake => {
                let mut bytes: Vec<u8> = vec![];
                bytes.push(values::SERVER_MSG_TYPE_HANDSHAKE);
                bytes.append(&mut self.str_with_length(values::SERVER_HANDSHAKE_PAYLOAD));
                bytes
            }
            MicrobatServerMessage::Ready => {
                let mut bytes: Vec<u8> = vec![];
                bytes.push(values::SERVER_MSG_TYPE_READY_FOR_QUERY);
                bytes.append(&mut self.str_with_length(values::SERVER_READY_PAYLOAD));
                bytes
            }
            MicrobatServerMessage::Error(error) => {
                let mut bytes: Vec<u8> = vec![];
                bytes.push(values::SERVER_MSG_TYPE_ERROR);
                bytes.append(&mut self.str_with_length(error));
                bytes
            }
            MicrobatServerMessage::DataDescription(row_descriptption) => {
                let mut bytes: Vec<u8> = vec![];
                bytes.push(values::SERVER_MSG_TYPE_ROW_DESCRIPTION);

                let mut column_bytes: Vec<u8> = vec![];
                for column in &row_descriptption.columns {
                    column_bytes.append(&mut self.str_with_length(&column.name));
                }
                bytes.append(&mut (column_bytes.len() as u32).to_le_bytes().to_vec());
                bytes.append(&mut column_bytes);
                bytes
            }
            MicrobatServerMessage::DataRow(data_row) => {
                let mut bytes: Vec<u8> = vec![];
                bytes.push(values::SERVER_MSG_TYPE_DATA_ROW);

                let mut column_bytes: Vec<u8> = vec![];
                for column in &data_row.columns {
                    let mut data_bytes = column.bytes();
                    column_bytes.push(column.type_byte());
                    column_bytes.append(&mut (data_bytes.len() as u32).to_le_bytes().to_vec());
                    column_bytes.append(&mut data_bytes);
                }
                bytes.append(&mut (column_bytes.len() as u32).to_le_bytes().to_vec());
                bytes.append(&mut column_bytes);
                bytes
            }
            MicrobatServerMessage::InsertResult(size) => {
                let mut bytes: Vec<u8> = vec![];
                bytes.push(values::SERVER_MSG_TYPE_INSERT_RESULT);
                let byte_arr = size.to_le_bytes();
                bytes.append(&mut (byte_arr.len() as u32).to_le_bytes().to_vec());
                bytes.append(&mut byte_arr.to_vec());
                bytes
            }
        }
    }
}

pub fn deserialize_server_message(
    message_type: u8,
    length: usize,
    bytes: &[u8],
) -> Result<MicrobatServerMessage, MicrobatProtocolError> {
    if length != bytes.len() {
        return Err(MicrobatProtocolError {
            msg: format!(
                "Byte mismatch error. Expecting {} bytes but received {} bytes",
                length,
                bytes.len()
            ),
        });
    }
    match message_type {
        values::SERVER_MSG_TYPE_HANDSHAKE => Ok(MicrobatServerMessage::Handshake),
        values::SERVER_MSG_TYPE_READY_FOR_QUERY => Ok(MicrobatServerMessage::Ready),
        values::SERVER_MSG_TYPE_ERROR => Ok(MicrobatServerMessage::Error(String::from_utf8(
            bytes.to_vec(),
        )?)),
        values::SERVER_MSG_TYPE_ROW_DESCRIPTION => {
            let mut rows = DataDescription { columns: vec![] };
            let mut pointer: usize = 0;
            while pointer < bytes.len() {
                let column_length =
                    u32::from_le_bytes(bytes[pointer..pointer + 4].try_into().unwrap()) as usize;
                let name =
                    String::from_utf8(bytes[pointer + 4..(pointer + 4 + column_length)].to_vec())?;
                rows.columns.push(Column {
                    name,
                    data_type: MDataType::Integer,
                }); // TODO: this is WRONG!s
                pointer += column_length + 4;
            }
            Ok(MicrobatServerMessage::DataDescription(rows))
        }
        values::SERVER_MSG_TYPE_DATA_ROW => {
            let mut rows = DataRow { columns: vec![] };
            let mut pointer: usize = 0;
            while pointer < bytes.len() {
                let column_type = bytes[pointer];
                let column_length =
                    u32::from_le_bytes(bytes[pointer + 1..pointer + 5].try_into().unwrap())
                        as usize;
                rows.columns.push(deserialize_data_column(
                    column_type,
                    &bytes[pointer + 5..(pointer + 5 + column_length)],
                )?);
                pointer += column_length + 5;
            }
            Ok(MicrobatServerMessage::DataRow(rows))
        }
        values::SERVER_MSG_TYPE_INSERT_RESULT => Ok(MicrobatServerMessage::InsertResult(
            u32::from_le_bytes(bytes.try_into().unwrap()),
        )),
        unknown => Err(MicrobatProtocolError {
            msg: format!(
                "Received unknown message type: {} (ascii: {})",
                unknown,
                char::from(unknown)
            ),
        }),
    }
}

#[cfg(test)]
mod server_message_tests {

    use crate::{messages::serialization_test_util::assert_serialisation, data::data_values::MData};

    use super::*;

    #[test]
    fn test_server_message_serialisation() {
        assert_serialisation(
            "server handshake",
            MicrobatServerMessage::Handshake.as_bytes(),
            values::SERVER_MSG_TYPE_HANDSHAKE,
            values::SERVER_HANDSHAKE_PAYLOAD.len(),
            Some(values::SERVER_HANDSHAKE_PAYLOAD),
        );
        assert_serialisation(
            "server ready",
            MicrobatServerMessage::Ready.as_bytes(),
            values::SERVER_MSG_TYPE_READY_FOR_QUERY,
            values::SERVER_READY_PAYLOAD.len(),
            Some(values::SERVER_READY_PAYLOAD),
        );
        assert_serialisation(
            "server error",
            MicrobatServerMessage::Error(String::from("error")).as_bytes(),
            values::SERVER_MSG_TYPE_ERROR,
            5,
            Some("error"),
        );
        assert_serialisation(
            "server row description",
            MicrobatServerMessage::DataDescription(DataDescription {
                columns: vec![Column {
                    name: String::from("foo"),
                    data_type: MDataType::Varchar,
                }],
            })
            .as_bytes(),
            values::SERVER_MSG_TYPE_ROW_DESCRIPTION,
            7, // We just know this expected size of 7 bytes
            None,
        );
        assert_serialisation(
            "server row description",
            MicrobatServerMessage::DataRow(DataRow {
                columns: vec![MData::Varchar(String::from("foo"))],
            })
            .as_bytes(),
            values::SERVER_MSG_TYPE_DATA_ROW,
            8, // We just know this..
            None,
        );
        assert_serialisation(
            "server row description with null",
            MicrobatServerMessage::DataRow(DataRow {
                columns: vec![MData::Null, MData::Varchar(String::from("foo"))],
            })
            .as_bytes(),
            values::SERVER_MSG_TYPE_DATA_ROW,
            13, // We just know this..
            None,
        );
        assert_serialisation(
            "Insert result",
            MicrobatServerMessage::InsertResult(1).as_bytes(),
            values::SERVER_MSG_TYPE_INSERT_RESULT,
            4,
            None,
        )
    }

    #[test]
    fn test_server_handshake_deserialisation() {
        let handshake_bytes = MicrobatServerMessage::Handshake.as_bytes();
        let length = u32::from_le_bytes(handshake_bytes[1..5].try_into().unwrap()) as usize;
        let deserialized =
            deserialize_server_message(handshake_bytes[0], length, &handshake_bytes[5..]).unwrap();
        assert_eq!(deserialized, MicrobatServerMessage::Handshake);
    }

    // TODO: cleanly assert all serialize->deserialize streams...

    #[test]
    fn test_server_datarow_deserialization_varchar() {
        let data_row = DataRow {
            columns: vec![MData::Varchar(String::from("hello"))],
        };
        let message_bytes = MicrobatServerMessage::DataRow(data_row).as_bytes();
        let length = u32::from_le_bytes(message_bytes[1..5].try_into().unwrap()) as usize;
        let deserialized =
            deserialize_server_message(message_bytes[0], length, &message_bytes[5..]).unwrap();
        let expected_data_row = DataRow {
            columns: vec![MData::Varchar(String::from("hello"))],
        };
        assert_eq!(
            deserialized,
            MicrobatServerMessage::DataRow(expected_data_row)
        );
    }

    #[test]
    fn test_server_datarow_deserialization_integer() {
        let data_row = DataRow {
            columns: vec![MData::Integer(83728)],
        };
        let message_bytes = MicrobatServerMessage::DataRow(data_row).as_bytes();
        let length = u32::from_le_bytes(message_bytes[1..5].try_into().unwrap()) as usize;
        let deserialized =
            deserialize_server_message(message_bytes[0], length, &message_bytes[5..]).unwrap();
        let expected_data_row = DataRow {
            columns: vec![MData::Integer(83728)],
        };
        assert_eq!(
            deserialized,
            MicrobatServerMessage::DataRow(expected_data_row)
        );
    }

    #[test]
    fn test_invalid_server_deserialization() {
        assert!(deserialize_server_message(b'\0', 0, &[]).is_err());
        assert!(deserialize_server_message(b'h', 0, &[]).is_err());
        assert!(deserialize_server_message(values::SERVER_MSG_TYPE_HANDSHAKE, 0, &[b't']).is_err());
        assert!(deserialize_server_message(values::SERVER_MSG_TYPE_HANDSHAKE, 5, &[b't']).is_err());
        assert!(deserialize_server_message(values::SERVER_MSG_TYPE_ERROR, 2, &[0, 159]).is_err());
    }

    #[test]
    fn test_deserialization_fails_if_length_and_bytes_do_not_match() {
        assert!(
            deserialize_server_message(values::SERVER_MSG_TYPE_HANDSHAKE, 5, &[b'0', 1]).is_err()
        );
        assert!(
            deserialize_server_message(values::SERVER_MSG_TYPE_HANDSHAKE, 5, &[b'0', 10]).is_err()
        );
    }

    #[test]
    fn test_deserialization_fails_for_unknown_marker_bytes() {
        assert!(
            deserialize_server_message(values::CLIENT_MSG_TYPE_HANDSHAKE, 5, &[b'0', 5]).is_err()
        );
        assert!(
            deserialize_server_message(values::CLIENT_MSG_TYPE_DISCONNECT, 5, &[b'0', 5]).is_err()
        );
    }
}
