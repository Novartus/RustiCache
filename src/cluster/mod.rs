pub mod slot;
pub mod topology;

use parking_lot::RwLock;
use std::sync::Arc;

#[allow(unused_imports)]
pub use slot::{key_slot, HASH_SLOTS};
pub use topology::{ClusterNode, ClusterTopology};

pub type ClusterManager = Arc<RwLock<ClusterTopology>>;
