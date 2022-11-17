use crate::{
    read_message_type, static_values as values, Column, Data, DataColumns, MicrobatMessage,
    MicrobatProtocolError, RowDescription,
};
use std::io::{Read, Write};

#[derive(Debug, PartialEq)]
pub enum MicrobatServerMessage {
    Handshake,
    Error(String),
    RowDescription(RowDescription),
    DataRow(DataColumns),
    Ready,
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
            MicrobatServerMessage::RowDescription(row_descriptption) => {
                let mut bytes: Vec<u8> = vec![];
                bytes.push(values::SERVER_MSG_TYPE_ROW_DESCRIPTION);

                let mut column_bytes: Vec<u8> = vec![];
                for column in &row_descriptption.rows {
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
                    column_bytes.append(&mut (data_bytes.len() as u32).to_le_bytes().to_vec());
                    column_bytes.append(&mut data_bytes);
                }
                bytes.append(&mut (column_bytes.len() as u32).to_le_bytes().to_vec());
                bytes.append(&mut column_bytes);
                bytes
            }
            _ => panic!("Not yet"),
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
        values::SERVER_MSG_TYPE_ERROR => Ok(MicrobatServerMessage::Error(String::from_utf8(
            bytes.to_vec(),
        )?)),
        values::SERVER_MSG_TYPE_ROW_DESCRIPTION => {
            let mut rows = RowDescription { rows: vec![] };
            let mut pointer: usize = 0;
            while pointer < bytes.len() {
                let column_length =
                    u32::from_le_bytes(bytes[pointer..pointer + 4].try_into().unwrap()) as usize;
                let name =
                    String::from_utf8(bytes[pointer + 4..(pointer + 4 + column_length)].to_vec())?;
                rows.rows.push(Column { name });
                pointer += column_length + 4;
            }
            Ok(MicrobatServerMessage::RowDescription(rows))
        }
        values::SERVER_MSG_TYPE_DATA_ROW => {
            let mut rows = DataColumns { columns: vec![] };
            let mut pointer: usize = 0;
            while pointer < bytes.len() {
                let column_length =
                    u32::from_le_bytes(bytes[pointer..pointer + 4].try_into().unwrap()) as usize;
                let name =
                    String::from_utf8(bytes[pointer + 4..(pointer + 4 + column_length)].to_vec())?;
                rows.columns.push(Data::Varchar(name));
                pointer += column_length + 4;
            }
            Ok(MicrobatServerMessage::DataRow(rows))
        }
        unknown => Err(MicrobatProtocolError {
            msg: format!(
                "Received unknown message type: {} (ascii: {})",
                unknown,
                char::from(unknown)
            ),
        }),
    }
}
