use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use super::entry::Entry;

#[derive(Clone)]
pub struct Db {
    shards: Arc<Vec<RwLock<HashMap<String, Entry>>>>,
    shard_mask: usize,
    used_memory: Arc<AtomicUsize>,
    max_memory_bytes: usize,
}

impl Db {
    pub fn new() -> Self {
        Self::with_config(128, 0)
    }

    pub fn with_config(shard_count: usize, max_memory_bytes: usize) -> Self {
        let count = shard_count.max(1).next_power_of_two();
        let mut shards = Vec::with_capacity(count);
        for _ in 0..count {
            shards.push(RwLock::new(HashMap::new()));
        }
        Self {
            shards: Arc::new(shards),
            shard_mask: count - 1,
            used_memory: Arc::new(AtomicUsize::new(0)),
            max_memory_bytes,
        }
    }

    #[inline]
    fn shard_index(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) & self.shard_mask
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        let idx = self.shard_index(key);
        {
            let mut shard = self.shards[idx].write();
            if let Some(entry) = shard.get_mut(key) {
                if !entry.is_expired() {
                    entry.touch();
                    return Some(entry.value.clone());
                }
            } else {
                return None;
            }
        }

        // Lazy cleanup for expired key
        let mut shard = self.shards[idx].write();
        if let Some(entry) = shard.get(key) {
            if entry.is_expired() {
                let size = entry.approx_size_bytes;
                shard.remove(key);
                self.used_memory.fetch_sub(size, Ordering::Relaxed);
            }
        }
        None
    }

    pub fn set(&self, key: String, value: Bytes, expires_at: Option<Instant>) {
        if self.max_memory_bytes > 0 {
            self.ensure_memory_capacity();
        }

        let key_len = key.len();
        let new_entry = Entry::new(key_len, value, expires_at);
        let new_size = new_entry.approx_size_bytes;

        let idx = self.shard_index(&key);
        let mut shard = self.shards[idx].write();
        if let Some(old) = shard.insert(key, new_entry) {
            self.used_memory.fetch_sub(old.approx_size_bytes, Ordering::Relaxed);
        }
        self.used_memory.fetch_add(new_size, Ordering::Relaxed);
    }

    pub fn del(&self, key: &str) -> bool {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write();
        if let Some(entry) = shard.remove(key) {
            self.used_memory.fetch_sub(entry.approx_size_bytes, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub fn exists(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    pub fn keys(&self) -> Vec<String> {
        let mut all_keys = Vec::new();
        let now = Instant::now();
        for shard_lock in self.shards.iter() {
            let shard = shard_lock.read();
            for (key, entry) in shard.iter() {
                if entry.expires_at.map_or(true, |exp| exp > now) {
                    all_keys.push(key.clone());
                }
            }
        }
        all_keys
    }

    #[allow(dead_code)]
    pub fn memory_used(&self) -> usize {
        self.used_memory.load(Ordering::Relaxed)
    }

    pub fn purge_expired_sample(&self, sample_per_shard: usize) -> usize {
        let mut purged = 0;
        let now = Instant::now();

        for shard_lock in self.shards.iter() {
            let keys_to_remove: Vec<(String, usize)> = {
                let shard = shard_lock.read();
                shard
                    .iter()
                    .take(sample_per_shard)
                    .filter_map(|(k, entry)| {
                        if entry.expires_at.map_or(false, |exp| exp <= now) {
                            Some((k.clone(), entry.approx_size_bytes))
                        } else {
                            None
                        }
                    })
                    .collect()
            };

            if !keys_to_remove.is_empty() {
                let mut shard = shard_lock.write();
                for (k, size) in keys_to_remove {
                    if shard.remove(&k).is_some() {
                        self.used_memory.fetch_sub(size, Ordering::Relaxed);
                        purged += 1;
                    }
                }
            }
        }

        purged
    }

    fn ensure_memory_capacity(&self) {
        let max = self.max_memory_bytes;
        while self.used_memory.load(Ordering::Relaxed) >= max {
            // Find the least recently used entry across sample
            let mut oldest: Option<(usize, String, Instant)> = None;

            for (idx, shard_lock) in self.shards.iter().enumerate() {
                let shard = shard_lock.read();
                for (k, entry) in shard.iter().take(5) {
                    if oldest.as_ref().map_or(true, |(_, _, ts)| entry.last_accessed < *ts) {
                        oldest = Some((idx, k.clone(), entry.last_accessed));
                    }
                }
            }

            if let Some((idx, key, _)) = oldest {
                let mut shard = self.shards[idx].write();
                if let Some(removed) = shard.remove(&key) {
                    self.used_memory.fetch_sub(removed.approx_size_bytes, Ordering::Relaxed);
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }
}

impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}
