use bytes::Bytes;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct Entry {
    pub value: Bytes,
    pub expires_at: Option<Instant>,
    pub last_accessed: Instant,
    pub approx_size_bytes: usize,
}

impl Entry {
    pub fn new(key_len: usize, value: Bytes, expires_at: Option<Instant>) -> Self {
        let approx_size_bytes = key_len + value.len() + std::mem::size_of::<Self>();
        Self {
            value,
            expires_at,
            last_accessed: Instant::now(),
            approx_size_bytes,
        }
    }

    pub fn is_expired(&self) -> bool {
        if let Some(expires_at) = self.expires_at {
            Instant::now() >= expires_at
        } else {
            false
        }
    }

    pub fn touch(&mut self) {
        self.last_accessed = Instant::now();
    }
}
