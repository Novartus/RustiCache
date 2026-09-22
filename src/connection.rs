use bytes::BytesMut;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};

use crate::cluster::{key_slot, ClusterManager};
use crate::commands::Command;
use crate::protocol::Value;
use crate::replication::rdb::get_empty_rdb_bytes;
use crate::replication::ReplicationState;
use crate::storage::Db;

pub struct Connection {
    stream: TcpStream,
    addr: SocketAddr,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream, addr: SocketAddr) -> Self {
        Self {
            stream,
            addr,
            buffer: BytesMut::with_capacity(4096),
        }
    }

    pub async fn handle(
        mut self,
        db: Db,
        repl_state: ReplicationState,
        requirepass: Option<Arc<str>>,
        max_payload_size: usize,
        tcp_nodelay: bool,
        mut shutdown_rx: broadcast::Receiver<()>,
        cluster_manager: Option<ClusterManager>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!("Accepted client connection from {}", self.addr);

        if tcp_nodelay {
            if let Err(e) = self.stream.set_nodelay(true) {
                warn!("Failed to set TCP_NODELAY on {}: {}", self.addr, e);
            }
        }

        let mut is_authenticated = requirepass.is_none();
        let mut is_downstream_replica = false;
        let (downstream_tx, mut downstream_rx) = mpsc::unbounded_channel::<bytes::Bytes>();

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    debug!("Server shutdown signal received; closing connection with {}", self.addr);
                    break;
                }
                Some(replicated_bytes) = downstream_rx.recv(), if is_downstream_replica => {
                    self.stream.write_all(&replicated_bytes).await?;
                    self.stream.flush().await?;
                }
                read_res = self.stream.read_buf(&mut self.buffer) => {
                    let n = read_res?;
                    if n == 0 {
                        debug!("Client disconnected: {}", self.addr);
                        break;
                    }

                    // Anti-DoS payload size bound check
                    if self.buffer.len() > max_payload_size {
                        error!("Client {} exceeded max payload size ({} bytes). Terminating connection.", self.addr, self.buffer.len());
                        let mut out = BytesMut::new();
                        Value::error("ERR max payload size exceeded").serialize(&mut out);
                        let _ = self.stream.write_all(&out).await;
                        let _ = self.stream.flush().await;
                        break;
                    }
                }
            }

            // Process any complete RESP messages in the buffer
            let mut close_after_reply = false;
            while !self.buffer.is_empty() {
                let initial_len = self.buffer.len();
                let mut cursor = self.buffer.clone();

                match Value::parse(&mut cursor) {
                    Ok(Some(val)) => {
                        let consumed = initial_len - cursor.len();
                        let raw_cmd = self.buffer.split_to(consumed);

                        match Command::from_value(&val) {
                            Ok(Command::Psync { repl_id, offset }) => {
                                info!("Received PSYNC from {}: repl_id={}, offset={}", self.addr, repl_id, offset);
                                let fullresync_msg = format!("+FULLRESYNC {} 0\r\n", repl_state.get_replid());
                                self.stream.write_all(fullresync_msg.as_bytes()).await?;

                                let rdb_bytes = get_empty_rdb_bytes();
                                let rdb_header = format!("${}\r\n", rdb_bytes.len());
                                self.stream.write_all(rdb_header.as_bytes()).await?;
                                self.stream.write_all(&rdb_bytes).await?;
                                self.stream.flush().await?;

                                is_downstream_replica = true;
                                repl_state.register_downstream(downstream_tx.clone());
                            }
                            Ok(Command::Quit) => {
                                let mut out = BytesMut::new();
                                Value::ok().serialize(&mut out);
                                self.stream.write_all(&out).await?;
                                self.stream.flush().await?;
                                close_after_reply = true;
                                break;
                            }
                            Ok(cmd) => {
                                // Check for cluster slot redirection if cluster mode is active
                                if let Some(ref cluster) = cluster_manager {
                                    if let Some(target_key) = cmd.target_key() {
                                        // Only redirect if client is authenticated (or no auth required)
                                        if is_authenticated {
                                            let slot = key_slot(target_key.as_bytes());
                                            let redirect = {
                                                let topo = cluster.read();
                                                if !topo.owns_slot(slot) {
                                                    Some(topo.get_node_for_slot(slot))
                                                } else {
                                                    None
                                                }
                                            };

                                            if let Some(target_opt) = redirect {
                                                if let Some(owner) = target_opt {
                                                    let moved_resp = format!("-MOVED {} {}\r\n", slot, owner.endpoint());
                                                    self.stream.write_all(moved_resp.as_bytes()).await?;
                                                    self.stream.flush().await?;
                                                    continue;
                                                } else {
                                                    let down_resp = b"-CLUSTERDOWN The cluster is down\r\n";
                                                    self.stream.write_all(down_resp).await?;
                                                    self.stream.flush().await?;
                                                    continue;
                                                }
                                            }
                                        }
                                    }
                                }

                                if let Some(reply) = cmd.execute(
                                    &db,
                                    &repl_state,
                                    Some(&raw_cmd),
                                    requirepass.as_deref(),
                                    &mut is_authenticated,
                                    cluster_manager.as_ref(),
                                ) {
                                    let mut out = BytesMut::new();
                                    reply.serialize(&mut out);
                                    self.stream.write_all(&out).await?;
                                    self.stream.flush().await?;
                                }
                            }
                            Err(e) => {
                                let mut out = BytesMut::new();
                                Value::error(e).serialize(&mut out);
                                self.stream.write_all(&out).await?;
                                self.stream.flush().await?;
                            }
                        }
                    }
                    Ok(None) => {
                        // More data needed
                        break;
                    }
                    Err(e) => {
                        error!("Protocol parse error from {}: {}", self.addr, e);
                        let mut out = BytesMut::new();
                        Value::error(format!("ERR protocol error: {}", e)).serialize(&mut out);
                        self.stream.write_all(&out).await?;
                        self.stream.flush().await?;
                        self.buffer.clear();
                        break;
                    }
                }
            }

            if close_after_reply {
                break;
            }
        }

        Ok(())
    }
}
