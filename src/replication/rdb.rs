use bytes::Bytes;

pub const EMPTY_RDB_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d086265fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff62278d2db30eb16";

pub fn get_empty_rdb_bytes() -> Bytes {
    let hex = EMPTY_RDB_HEX;
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for i in (0..hex.len()).step_by(2) {
        let byte = u8::from_str_radix(&hex[i..i + 2], 16).unwrap_or(0);
        bytes.push(byte);
    }
    Bytes::from(bytes)
}
