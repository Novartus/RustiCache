use bytes::{Buf, Bytes, BytesMut};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Option<Vec<Value>>),
}

impl Value {
    pub fn ok() -> Self {
        Value::SimpleString("OK".to_string())
    }

    pub fn pong() -> Self {
        Value::SimpleString("PONG".to_string())
    }

    pub fn null_bulk() -> Self {
        Value::BulkString(None)
    }

    #[allow(dead_code)]
    pub fn null_array() -> Self {
        Value::Array(None)
    }

    pub fn error(msg: impl Into<String>) -> Self {
        Value::Error(msg.into())
    }

    pub fn string(s: impl Into<String>) -> Self {
        let s = s.into();
        Value::BulkString(Some(Bytes::from(s)))
    }

    pub fn as_bytes(&self) -> Option<&Bytes> {
        match self {
            Value::BulkString(Some(b)) => Some(b),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::SimpleString(s) => Some(s.as_str()),
            Value::BulkString(Some(b)) => std::str::from_utf8(b).ok(),
            _ => None,
        }
    }

    pub fn serialize(&self, buf: &mut BytesMut) {
        match self {
            Value::SimpleString(s) => {
                buf.extend_from_slice(b"+");
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Value::Error(s) => {
                buf.extend_from_slice(b"-ERR ");
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Value::Integer(i) => {
                buf.extend_from_slice(b":");
                buf.extend_from_slice(i.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Value::BulkString(None) => {
                buf.extend_from_slice(b"$-1\r\n");
            }
            Value::BulkString(Some(data)) => {
                buf.extend_from_slice(b"$");
                buf.extend_from_slice(data.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            Value::Array(None) => {
                buf.extend_from_slice(b"*-1\r\n");
            }
            Value::Array(Some(elements)) => {
                buf.extend_from_slice(b"*");
                buf.extend_from_slice(elements.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for elem in elements {
                    elem.serialize(buf);
                }
            }
        }
    }

    pub fn parse(src: &mut BytesMut) -> Result<Option<Value>, RespParseError> {
        if src.is_empty() {
            return Ok(None);
        }

        match src[0] {
            b'+' => parse_simple_string(src),
            b'-' => parse_error(src),
            b':' => parse_integer(src),
            b'$' => parse_bulk_string(src),
            b'*' => parse_array(src),
            other => Err(RespParseError::UnknownPrefix(other)),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RespParseError {
    #[error("Unknown RESP prefix byte: {0}")]
    UnknownPrefix(u8),
    #[error("Invalid integer in protocol")]
    InvalidInteger,
    #[error("Protocol error: {0}")]
    Protocol(String),
}

fn get_line<'a>(src: &'a [u8]) -> Option<(&'a [u8], usize)> {
    for i in 0..src.len().saturating_sub(1) {
        if src[i] == b'\r' && src[i + 1] == b'\n' {
            return Some((&src[..i], i + 2));
        }
    }
    None
}

fn parse_simple_string(src: &mut BytesMut) -> Result<Option<Value>, RespParseError> {
    if let Some((line, consumed)) = get_line(src) {
        let content = std::str::from_utf8(&line[1..])
            .map_err(|e| RespParseError::Protocol(e.to_string()))?
            .to_string();
        src.advance(consumed);
        Ok(Some(Value::SimpleString(content)))
    } else {
        Ok(None)
    }
}

fn parse_error(src: &mut BytesMut) -> Result<Option<Value>, RespParseError> {
    if let Some((line, consumed)) = get_line(src) {
        let content = std::str::from_utf8(&line[1..])
            .map_err(|e| RespParseError::Protocol(e.to_string()))?
            .to_string();
        src.advance(consumed);
        Ok(Some(Value::Error(content)))
    } else {
        Ok(None)
    }
}

fn parse_integer(src: &mut BytesMut) -> Result<Option<Value>, RespParseError> {
    if let Some((line, consumed)) = get_line(src) {
        let s = std::str::from_utf8(&line[1..])
            .map_err(|e| RespParseError::Protocol(e.to_string()))?;
        let int_val = s.parse::<i64>().map_err(|_| RespParseError::InvalidInteger)?;
        src.advance(consumed);
        Ok(Some(Value::Integer(int_val)))
    } else {
        Ok(None)
    }
}

fn parse_bulk_string(src: &mut BytesMut) -> Result<Option<Value>, RespParseError> {
    let (length, line_len) = match get_line(src) {
        Some((line, consumed)) => {
            let s = std::str::from_utf8(&line[1..])
                .map_err(|e| RespParseError::Protocol(e.to_string()))?;
            let len = s.parse::<isize>().map_err(|_| RespParseError::InvalidInteger)?;
            (len, consumed)
        }
        None => return Ok(None),
    };

    if length == -1 {
        src.advance(line_len);
        return Ok(Some(Value::BulkString(None)));
    }

    if length < 0 {
        return Err(RespParseError::Protocol("Negative bulk string length other than -1".into()));
    }

    let length = length as usize;
    let total_required = line_len + length + 2; // header + payload + \r\n
    if src.len() < total_required {
        return Ok(None);
    }

    src.advance(line_len);
    let data = src.split_to(length).freeze();

    if src.len() < 2 || src[0] != b'\r' || src[1] != b'\n' {
        return Err(RespParseError::Protocol("Expected CRLF after bulk string data".into()));
    }
    src.advance(2);

    Ok(Some(Value::BulkString(Some(data))))
}

fn parse_array(src: &mut BytesMut) -> Result<Option<Value>, RespParseError> {
    let (count, line_len) = match get_line(src) {
        Some((line, consumed)) => {
            let s = std::str::from_utf8(&line[1..])
                .map_err(|e| RespParseError::Protocol(e.to_string()))?;
            let num = s.parse::<isize>().map_err(|_| RespParseError::InvalidInteger)?;
            (num, consumed)
        }
        None => return Ok(None),
    };

    if count == -1 {
        src.advance(line_len);
        return Ok(Some(Value::Array(None)));
    }

    if count < 0 {
        return Err(RespParseError::Protocol("Negative array count other than -1".into()));
    }

    // Try parsing all elements without permanently consuming `src` unless whole array is present
    let mut temp = src.clone();
    temp.advance(line_len);

    let mut elements = Vec::with_capacity(count as usize);
    for _ in 0..count {
        match Value::parse(&mut temp)? {
            Some(elem) => elements.push(elem),
            None => return Ok(None), // Not enough data yet
        }
    }

    let consumed_total = src.len() - temp.len();
    src.advance(consumed_total);

    Ok(Some(Value::Array(Some(elements))))
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::SimpleString(s) => write!(f, "\"{}\"", s),
            Value::Error(s) => write!(f, "(error) {}", s),
            Value::Integer(i) => write!(f, "(integer) {}", i),
            Value::BulkString(None) => write!(f, "(nil)"),
            Value::BulkString(Some(b)) => {
                if let Ok(s) = std::str::from_utf8(b) {
                    write!(f, "\"{}\"", s)
                } else {
                    write!(f, "{:?}", b)
                }
            }
            Value::Array(None) => write!(f, "(nil)"),
            Value::Array(Some(items)) => {
                writeln!(f, "[")?;
                for (i, item) in items.iter().enumerate() {
                    writeln!(f, "  {}: {}", i + 1, item)?;
                }
                write!(f, "]")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string() {
        let mut buf = BytesMut::from("+OK\r\n");
        let val = Value::parse(&mut buf).unwrap();
        assert_eq!(val, Some(Value::SimpleString("OK".to_string())));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_error() {
        let mut buf = BytesMut::from("-ERR unknown command\r\n");
        let val = Value::parse(&mut buf).unwrap();
        assert_eq!(val, Some(Value::Error("ERR unknown command".to_string())));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_integer() {
        let mut buf = BytesMut::from(":42\r\n");
        let val = Value::parse(&mut buf).unwrap();
        assert_eq!(val, Some(Value::Integer(42)));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_bulk_string() {
        let mut buf = BytesMut::from("$5\r\nhello\r\n");
        let val = Value::parse(&mut buf).unwrap();
        assert_eq!(val, Some(Value::BulkString(Some(Bytes::from_static(b"hello")))));
        assert!(buf.is_empty());

        let mut null_buf = BytesMut::from("$-1\r\n");
        let null_val = Value::parse(&mut null_buf).unwrap();
        assert_eq!(null_val, Some(Value::BulkString(None)));
        assert!(null_buf.is_empty());
    }

    #[test]
    fn test_parse_array() {
        let mut buf = BytesMut::from("*2\r\n$4\r\nECHO\r\n$5\r\nworld\r\n");
        let val = Value::parse(&mut buf).unwrap();
        assert_eq!(
            val,
            Some(Value::Array(Some(vec![
                Value::BulkString(Some(Bytes::from_static(b"ECHO"))),
                Value::BulkString(Some(Bytes::from_static(b"world"))),
            ])))
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn test_partial_parse() {
        let mut buf = BytesMut::from("*2\r\n$4\r\nECHO\r\n$5\r\nwor");
        let val = Value::parse(&mut buf).unwrap();
        assert_eq!(val, None);
        // Original buffer must not be prematurely consumed
        assert_eq!(buf.len(), 21);

        // Feed remainder
        buf.extend_from_slice(b"ld\r\n");
        let val2 = Value::parse(&mut buf).unwrap();
        assert!(val2.is_some());
        assert!(buf.is_empty());
    }

    #[test]
    fn test_serialize_and_roundtrip() {
        let original = Value::Array(Some(vec![
            Value::BulkString(Some(Bytes::from_static(b"SET"))),
            Value::BulkString(Some(Bytes::from_static(b"key"))),
            Value::BulkString(Some(Bytes::from_static(b"value"))),
        ]));

        let mut buf = BytesMut::new();
        original.serialize(&mut buf);

        let parsed = Value::parse(&mut buf).unwrap().unwrap();
        assert_eq!(original, parsed);
        assert!(buf.is_empty());
    }
}

