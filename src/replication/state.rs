use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerRole {
    Master,
    Replica { master_host: String, master_port: u16 },
}

#[derive(Clone)]
pub struct ReplicationState {
    pub role: ServerRole,
    pub master_replid: Arc<RwLock<String>>,
    pub master_repl_offset: Arc<AtomicU64>,
    pub replica_bytes_processed: Arc<AtomicU64>,
    pub connected_replicas: Arc<AtomicUsize>,
    pub downstream_senders: Arc<Mutex<Vec<mpsc::UnboundedSender<Bytes>>>>,
}

impl ReplicationState {
    pub fn new_master(replid: Option<String>) -> Self {
        let replid = replid.unwrap_or_else(|| {
            // Standard 40-char pseudo-random hex string for Redis replid
            "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string()
        });

        Self {
            role: ServerRole::Master,
            master_replid: Arc::new(RwLock::new(replid)),
            master_repl_offset: Arc::new(AtomicU64::new(0)),
            replica_bytes_processed: Arc::new(AtomicU64::new(0)),
            connected_replicas: Arc::new(AtomicUsize::new(0)),
            downstream_senders: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn new_replica(master_host: String, master_port: u16) -> Self {
        Self {
            role: ServerRole::Replica {
                master_host,
                master_port,
            },
            master_replid: Arc::new(RwLock::new("?".to_string())),
            master_repl_offset: Arc::new(AtomicU64::new(0)),
            replica_bytes_processed: Arc::new(AtomicU64::new(0)),
            connected_replicas: Arc::new(AtomicUsize::new(0)),
            downstream_senders: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn is_replica(&self) -> bool {
        matches!(self.role, ServerRole::Replica { .. })
    }

    pub fn get_replid(&self) -> String {
        self.master_replid.read().clone()
    }

    pub fn set_replid(&self, id: String) {
        *self.master_replid.write() = id;
    }

    pub fn info_replication(&self) -> String {
        let replid = self.get_replid();
        match &self.role {
            ServerRole::Master => {
                let offset = self.master_repl_offset.load(Ordering::Relaxed);
                let connected = self.connected_replicas.load(Ordering::Relaxed);
                format!(
                    "# Replication\r\nrole:master\r\nconnected_slaves:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\nsecond_repl_offset:-1\r\nrepl_backlog_active:0\r\n",
                    connected, replid, offset
                )
            }
            ServerRole::Replica {
                master_host,
                master_port,
            } => {
                let offset = self.replica_bytes_processed.load(Ordering::Relaxed);
                format!(
                    "# Replication\r\nrole:slave\r\nmaster_host:{}\r\nmaster_port:{}\r\nmaster_link_status:up\r\nmaster_last_io_seconds_ago:0\r\nmaster_sync_in_progress:0\r\nslave_repl_offset:{}\r\nslave_priority:100\r\nslave_read_only:1\r\nconnected_slaves:0\r\nmaster_replid:{}\r\n",
                    master_host, master_port, offset, replid
                )
            }
        }
    }

    pub fn register_downstream(&self, tx: mpsc::UnboundedSender<Bytes>) {
        self.downstream_senders.lock().push(tx);
        self.connected_replicas.fetch_add(1, Ordering::SeqCst);
    }

    pub fn propagate_downstream(&self, raw_resp: Bytes) {
        let mut senders = self.downstream_senders.lock();
        let len = raw_resp.len() as u64;
        self.master_repl_offset.fetch_add(len, Ordering::SeqCst);
        senders.retain(|tx| tx.send(raw_resp.clone()).is_ok());
    }
}
