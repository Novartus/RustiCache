use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub tcp_nodelay: bool,
    pub max_connections: usize,
    pub max_payload_size: usize,
    pub requirepass: Option<String>,
    pub masterauth: Option<String>,
    pub worker_threads: usize,
    pub shard_count: usize,
    pub max_memory_bytes: usize,
    pub ttl_interval: Duration,
    pub ttl_sample_size: usize,
    #[allow(dead_code)]
    pub replicaof: Option<(String, u16)>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 6379,
            tcp_nodelay: true,
            max_connections: 10000,
            max_payload_size: 512 * 1024 * 1024, // 512 MB
            requirepass: None,
            masterauth: None,
            worker_threads: 0,
            shard_count: 128,
            max_memory_bytes: 0,
            ttl_interval: Duration::from_millis(100),
            ttl_sample_size: 20,
            replicaof: None,
        }
    }
}
