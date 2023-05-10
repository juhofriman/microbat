// This file contains the marker bytes and default messages sent on wire in handshakes

pub const CLIENT_MSG_TYPE_HANDSHAKE: u8 = b'a';
pub const CLIENT_MSG_TYPE_QUERY: u8 = b'q';
pub const CLIENT_MSG_TYPE_DISCONNECT: u8 = b'd';

pub const CLIENT_HANDSHAKE_PAYLOAD: &str = "hello microbat";
pub const CLIENT_DISCONNECT_PAYLOAD: &str = "bye and so on";

pub const SERVER_MSG_TYPE_HANDSHAKE: u8 = b'b';
pub const SERVER_MSG_TYPE_READY_FOR_QUERY: u8 = b'x';
pub const SERVER_MSG_TYPE_ERROR: u8 = b'e';
pub const SERVER_MSG_TYPE_ROW_DESCRIPTION: u8 = b'r';
pub const SERVER_MSG_TYPE_DATA_ROW: u8 = b'd';
pub const SERVER_MSG_TYPE_INSERT_RESULT: u8 = b'i';

pub const SERVER_HANDSHAKE_PAYLOAD: &str = "hello client";
pub const SERVER_READY_PAYLOAD: &str = "shoot";

pub const TYPE_BYTE_NULL: u8 = b'n';
pub const TYPE_BYTE_INTEGER: u8 = b'i';
pub const TYPE_BYTE_VARCHAR: u8 = b'v';
