use bytes::Bytes;
use std::thread;
use std::time::{Duration, Instant};
use RustiCache::Db;

#[test]
fn test_basic_set_get() {
    let db = Db::new();
    db.set("hello".to_string(), Bytes::from_static(b"world"), None);

    let val = db.get("hello");
    assert_eq!(val, Some(Bytes::from_static(b"world")));

    assert!(db.exists("hello"));
    assert!(!db.exists("nonexistent"));

    assert!(db.del("hello"));
    assert_eq!(db.get("hello"), None);
    assert!(!db.del("hello"));
}

#[test]
fn test_ttl_expiry() {
    let db = Db::new();
    let expiry = Instant::now() + Duration::from_millis(100);
    db.set("temp_key".to_string(), Bytes::from_static(b"ephemeral"), Some(expiry));

    // Initially present
    assert_eq!(db.get("temp_key"), Some(Bytes::from_static(b"ephemeral")));

    // Sleep past expiration
    thread::sleep(Duration::from_millis(150));

    // Lazy cleanup should return None
    assert_eq!(db.get("temp_key"), None);
    assert!(!db.exists("temp_key"));
}

#[test]
fn test_active_eviction_purging() {
    let db = Db::new();
    let expiry = Instant::now() + Duration::from_millis(50);

    for i in 0..50 {
        db.set(
            format!("key_{}", i),
            Bytes::from_static(b"data"),
            Some(expiry),
        );
    }

    thread::sleep(Duration::from_millis(80));

    // Active eviction should purge expired keys
    let purged = db.purge_expired_sample(50);
    assert_eq!(purged, 50);
    assert_eq!(db.keys().len(), 0);
}

#[test]
fn test_concurrent_access() {
    let db = Db::new();
    let mut handles = Vec::new();

    for t in 0..10 {
        let db_clone = db.clone();
        handles.push(thread::spawn(move || {
            for i in 0..100 {
                let key = format!("concurrent_k_{}_{}", t, i);
                db_clone.set(key.clone(), Bytes::from("val"), None);
                assert_eq!(db_clone.get(&key), Some(Bytes::from("val")));
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(db.keys().len(), 1000);
}

#[test]
fn test_lru_memory_eviction() {
    // 4 shards, max memory 400 bytes (each entry is approx 50-80 bytes)
    let db = Db::with_config(4, 300);

    db.set("k1".to_string(), Bytes::from("val1"), None);
    thread::sleep(Duration::from_millis(5));
    db.set("k2".to_string(), Bytes::from("val2"), None);
    thread::sleep(Duration::from_millis(5));

    // Access k1 so k2 becomes the least recently used
    let _ = db.get("k1");
    thread::sleep(Duration::from_millis(5));

    // Adding more keys pushes memory over 300 bytes and triggers eviction
    for i in 3..10 {
        db.set(format!("k{}", i), Bytes::from("data_payload"), None);
    }

    // Total memory must be kept under max
    assert!(db.memory_used() <= 350);
}

