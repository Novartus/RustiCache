use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Semaphore};
use tracing::{error, info, warn};

use crate::cluster::{ClusterManager, ClusterNode, ClusterTopology};
use crate::config::ServerConfig;
use crate::connection::Connection;
use crate::replication::{run_replica_loop, ReplicationState, ServerRole};
use crate::storage::ttl::run_active_ttl_eviction;
use crate::storage::Db;

pub struct Server {
    config: ServerConfig,
    db: Db,
    repl_state: ReplicationState,
    cluster_manager: Option<ClusterManager>,
}

impl Server {
    #[allow(dead_code)]
    pub fn new(port: u16, repl_state: ReplicationState) -> Self {
        let mut config = ServerConfig::default();
        config.port = port;
        Self::with_server_config(config, repl_state)
    }

    pub fn with_server_config(config: ServerConfig, repl_state: ReplicationState) -> Self {
        let db = Db::with_config(config.shard_count, config.max_memory_bytes);

        let cluster_manager = if config.cluster_enabled {
            let node_id = config.cluster_node_id.clone().unwrap_or_else(|| {
                let mut h = DefaultHasher::new();
                format!("{}:{}", config.cluster_announce_ip, config.cluster_announce_port).hash(&mut h);
                format!("{:040x}", h.finish())
            });

            let myself = ClusterNode::new(
                node_id.clone(),
                config.cluster_announce_ip.clone(),
                config.cluster_announce_port,
                config.cluster_announce_bus_port,
                true,
                true,
            );
            let mut topo = ClusterTopology::new(myself);

            if let Some(ref slots_spec) = config.cluster_slots {
                if let Err(e) = topo.parse_and_assign_slots(&node_id, slots_spec) {
                    warn!("Failed to parse cluster slots specification '{}': {}", slots_spec, e);
                }
            } else {
                let _ = topo.add_slots_range(&node_id, 0, 16383);
            }

            Some(Arc::new(parking_lot::RwLock::new(topo)))
        } else {
            None
        };

        Self {
            config,
            db,
            repl_state,
            cluster_manager,
        }
    }

    #[allow(dead_code)]
    pub fn db(&self) -> &Db {
        &self.db
    }

    #[allow(dead_code)]
    pub fn repl_state(&self) -> &ReplicationState {
        &self.repl_state
    }

    #[allow(dead_code)]
    pub fn cluster_manager(&self) -> Option<ClusterManager> {
        self.cluster_manager.clone()
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            info!("Received SIGINT/Ctrl+C. Initiating graceful shutdown...");
            let _ = shutdown_tx.send(());
        });

        self.run_with_shutdown(shutdown_rx).await
    }

    pub async fn run_with_shutdown(
        self,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let addr: SocketAddr = format!("{}:{}", self.config.host, self.config.port).parse()?;
        let listener = TcpListener::bind(addr).await?;
        info!(
            "RustiCache enterprise server listening on {} [Max Conn: {}, Shards: {}, Cluster: {}]",
            addr,
            self.config.max_connections,
            self.config.shard_count,
            if self.config.cluster_enabled { "ENABLED" } else { "DISABLED" }
        );

        // Spawn background TTL eviction task
        let db_for_ttl = self.db.clone();
        let interval = self.config.ttl_interval;
        let sample_size = self.config.ttl_sample_size;
        tokio::spawn(async move {
            run_active_ttl_eviction(db_for_ttl, interval, sample_size).await;
        });

        // If configured as replica, spawn replica synchronization loop to master
        if let ServerRole::Replica {
            master_host,
            master_port,
        } = &self.repl_state.role
        {
            let db_for_repl = self.db.clone();
            let repl_state_for_task = self.repl_state.clone();
            let host = master_host.clone();
            let port = *master_port;
            let my_port = self.config.port;
            let masterauth = self.config.masterauth.clone();

            tokio::spawn(async move {
                run_replica_loop(host, port, my_port, db_for_repl, repl_state_for_task, masterauth).await;
            });
        }

        let connection_semaphore = Arc::new(Semaphore::new(self.config.max_connections));
        let (client_shutdown_tx, _) = broadcast::channel(1024);
        let requirepass_arc = self.config.requirepass.clone().map(Arc::from);
        let max_payload = self.config.max_payload_size;
        let nodelay = self.config.tcp_nodelay;
        let cluster_mgr = self.cluster_manager.clone();

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Shutting down listener; signaling active client connections...");
                    let _ = client_shutdown_tx.send(());
                    break;
                }
                accept_res = listener.accept() => {
                    match accept_res {
                        Ok((stream, client_addr)) => {
                            let permit = match connection_semaphore.clone().try_acquire_owned() {
                                Ok(p) => p,
                                Err(_) => {
                                    warn!("Max concurrent connections ({}) reached. Rejecting client {}", self.config.max_connections, client_addr);
                                    continue;
                                }
                            };

                            let db = self.db.clone();
                            let repl_state = self.repl_state.clone();
                            let requirepass = requirepass_arc.clone();
                            let conn = Connection::new(stream, client_addr);
                            let client_rx = client_shutdown_tx.subscribe();
                            let cluster_for_conn = cluster_mgr.clone();

                            tokio::spawn(async move {
                                let _permit = permit;
                                if let Err(e) = conn.handle(
                                    db,
                                    repl_state,
                                    requirepass,
                                    max_payload,
                                    nodelay,
                                    client_rx,
                                    cluster_for_conn,
                                ).await {
                                    error!("Connection error with {}: {}", client_addr, e);
                                }
                            });
                        }
                        Err(e) => {
                            error!("TCP accept failed: {}", e);
                        }
                    }
                }
            }
        }

        info!("RustiCache server stopped cleanly.");
        Ok(())
    }
}
