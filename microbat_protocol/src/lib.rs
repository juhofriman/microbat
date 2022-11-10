pub fn something() -> u32 {
    return 123;
}

pub const MSG_TYPE_STARTUP: u8 = b'1';
pub const MSG_TYPE_QUERY: u8 = b'2';
pub const MSG_TYPE_ERROR: u8 = b'3';
pub const MSG_TYPE_ROW_DESC: u8 = b'4';

pub fn startup_message() -> Vec<u8> {
    let msg = "Microbat client 0.1.0";
    let mut bytes = vec![MSG_TYPE_STARTUP, msg.len() as u8];
    for byte in msg.as_bytes() {
        bytes.push(*byte);
    }
    return bytes;
}

pub fn startup_response() -> Vec<u8> {
    let msg = "Microbat server 0.1.0";
    let mut bytes = vec![MSG_TYPE_STARTUP, msg.len() as u8];
    for byte in msg.as_bytes() {
        bytes.push(*byte);
    }
    return bytes;
}

pub fn query_message(sql: String) -> Vec<u8> {
    let mut bytes = vec![MSG_TYPE_QUERY, sql.len() as u8];
    for byte in sql.as_bytes() {
        bytes.push(*byte);
    }
    return bytes;
}

pub fn error_message(message: String) -> Vec<u8> {
    let mut bytes = vec![MSG_TYPE_ERROR, message.len() as u8];
    for byte in message.as_bytes() {
        bytes.push(*byte);
    }
    return bytes;
}
