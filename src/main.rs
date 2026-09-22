#![allow(non_snake_case)]

use clap::Parser;
use std::time::Duration;
use tracing::info;
use tracing_subscriber::EnvFilter;

mod cluster;
mod commands;
mod config;
mod connection;
mod protocol;
mod replication;
mod server;
mod storage;

use config::ServerConfig;
use replication::ReplicationState;
use server::Server;

#[derive(Parser, Debug)]
#[command(
    name = "RustiCache",
    version,
    about = "Enterprise Redis-compatible replica and cache engine in Rust"
)]
struct Cli {
    /// Host address to bind to
    #[arg(long, env = "RUSTICACHE_HOST", default_value = "0.0.0.0")]
    host: String,

    /// Port to listen on (default: 6379)
    #[arg(short, long, env = "RUSTICACHE_PORT", default_value_t = 6379)]
    port: u16,

    /// TCP_NODELAY socket option
    #[arg(long, env = "RUSTICACHE_TCP_NODELAY", default_value_t = true)]
    tcp_nodelay: bool,

    /// Maximum concurrent client connections
    #[arg(long, env = "RUSTICACHE_MAX_CONNECTIONS", default_value_t = 10000)]
    max_connections: usize,

    /// Maximum payload size per request in bytes
    #[arg(long, env = "RUSTICACHE_MAX_PAYLOAD_SIZE_BYTES", default_value_t = 536870912)]
    max_payload_size: usize,

    /// Require clients to authenticate with password
    #[arg(long, env = "RUSTICACHE_REQUIREPASS")]
    requirepass: Option<String>,

    /// Master authentication password when acting as replica
    #[arg(long, env = "RUSTICACHE_MASTERAUTH")]
    masterauth: Option<String>,

    /// Tokio worker threads (0 for auto-detect based on CPU cores)
    #[arg(long, env = "RUSTICACHE_WORKER_THREADS", default_value_t = 0)]
    worker_threads: usize,

    /// Number of storage shards (power of 2, e.g. 64, 128, 256)
    #[arg(long, env = "RUSTICACHE_SHARD_COUNT", default_value_t = 128)]
    shard_count: usize,

    /// Maximum memory in bytes before LRU eviction triggers (0 = unlimited)
    #[arg(long, env = "RUSTICACHE_MAXMEMORY_BYTES", default_value_t = 0)]
    max_memory_bytes: usize,

    /// Active TTL eviction interval in milliseconds
    #[arg(long, env = "RUSTICACHE_TTL_INTERVAL_MS", default_value_t = 100)]
    ttl_interval_ms: u64,

    /// Active TTL eviction sample size per shard
    #[arg(long, env = "RUSTICACHE_TTL_SAMPLE_SIZE", default_value_t = 20)]
    ttl_sample_size: usize,

    /// Act as replica of <MASTER_HOST> <MASTER_PORT> via CLI
    #[arg(long, num_args = 2, value_names = ["MASTER_HOST", "MASTER_PORT"])]
    replicaof: Option<Vec<String>>,

    /// Act as replica specified via environment (e.g. "127.0.0.1 6379")
    #[arg(long, env = "RUSTICACHE_REPLICAOF")]
    replicaof_env: Option<String>,

    /// Enable Redis Cluster mode
    #[arg(long, env = "RUSTICACHE_CLUSTER_ENABLED", default_value_t = false)]
    cluster_enabled: bool,

    /// Unique cluster node ID (40 hex characters)
    #[arg(long, env = "RUSTICACHE_CLUSTER_NODE_ID")]
    cluster_node_id: Option<String>,

    /// Announced IP for cluster redirection
    #[arg(long, env = "RUSTICACHE_CLUSTER_ANNOUNCE_IP")]
    cluster_announce_ip: Option<String>,

    /// Announced client port for cluster redirection
    #[arg(long, env = "RUSTICACHE_CLUSTER_ANNOUNCE_PORT")]
    cluster_announce_port: Option<u16>,

    /// Announced cluster bus port
    #[arg(long, env = "RUSTICACHE_CLUSTER_ANNOUNCE_BUS_PORT")]
    cluster_announce_bus_port: Option<u16>,

    /// Slots assigned to this cluster node (e.g. "0-5460" or "0-16383")
    #[arg(long, env = "RUSTICACHE_CLUSTER_SLOTS")]
    cluster_slots: Option<String>,
}

fn load_dotenv() {
    if let Ok(content) = std::fs::read_to_string(".env") {
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            if let Some((key, val)) = trimmed.split_once('=') {
                let key = key.trim();
                let val = val.trim().trim_matches('"').trim_matches('\'');
                if !key.is_empty() && std::env::var(key).is_err() {
                    unsafe {
                        std::env::set_var(key, val);
                    }
                }
            }
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    load_dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let replica_cfg = cli.replicaof.or_else(|| {
        cli.replicaof_env.and_then(|val| {
            let parts: Vec<String> = val
                .split_whitespace()
                .map(|s| s.to_string())
                .collect();
            if parts.len() >= 2 {
                Some(parts)
            } else {
                None
            }
        })
    });

    let replica_parsed = replica_cfg.map(|args| {
        let host = args[0].clone();
        let port = args[1].parse::<u16>().unwrap_or(6379);
        (host, port)
    });

    let repl_state = if let Some((ref master_host, master_port)) = replica_parsed {
        info!("Starting in REPLICA mode syncing from {}:{}", master_host, master_port);
        ReplicationState::new_replica(master_host.clone(), master_port)
    } else {
        info!("Starting in MASTER / STANDALONE mode");
        ReplicationState::new_master(None)
    };

    let announce_ip = cli.cluster_announce_ip.unwrap_or_else(|| {
        if cli.host == "0.0.0.0" {
            "127.0.0.1".to_string()
        } else {
            cli.host.clone()
        }
    });
    let announce_port = cli.cluster_announce_port.unwrap_or(cli.port);
    let announce_bus_port = cli.cluster_announce_bus_port.unwrap_or(announce_port + 10000);

    let config = ServerConfig {
        host: cli.host,
        port: cli.port,
        tcp_nodelay: cli.tcp_nodelay,
        max_connections: cli.max_connections,
        max_payload_size: cli.max_payload_size,
        requirepass: cli.requirepass.filter(|p| !p.is_empty()),
        masterauth: cli.masterauth.filter(|p| !p.is_empty()),
        worker_threads: cli.worker_threads,
        shard_count: cli.shard_count,
        max_memory_bytes: cli.max_memory_bytes,
        ttl_interval: Duration::from_millis(cli.ttl_interval_ms),
        ttl_sample_size: cli.ttl_sample_size,
        replicaof: replica_parsed,
        cluster_enabled: cli.cluster_enabled,
        cluster_node_id: cli.cluster_node_id,
        cluster_announce_ip: announce_ip,
        cluster_announce_port: announce_port,
        cluster_announce_bus_port: announce_bus_port,
        cluster_slots: cli.cluster_slots,
    };

    // Configure multi-threaded Tokio runtime according to enterprise settings
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if config.worker_threads > 0 {
        builder.worker_threads(config.worker_threads);
        info!("Configured Tokio multi-thread runtime with {} worker threads", config.worker_threads);
    } else {
        info!("Configured Tokio multi-thread runtime auto-scaling to available CPU cores");
    }

    let runtime = builder.build()?;
    runtime.block_on(async move {
        let server = Server::with_server_config(config, repl_state);
        server.run().await
    })
}
