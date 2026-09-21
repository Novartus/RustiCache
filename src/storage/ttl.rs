use std::time::Duration;
use tokio::time::sleep;
use tracing::debug;

use super::db::Db;

pub async fn run_active_ttl_eviction(db: Db, interval: Duration, sample_per_shard: usize) {
    loop {
        sleep(interval).await;
        let purged = db.purge_expired_sample(sample_per_shard);
        if purged > 0 {
            debug!("Active TTL eviction purged {} expired keys", purged);
        }
    }
}
