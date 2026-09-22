use RustiCache::cluster::slot::{crc16, extract_hashtag, key_slot, HASH_SLOTS};

#[test]
fn test_crc16_standard_vectors() {
    // Official Redis test vector: "123456789" -> 0x31C3 = 12739
    assert_eq!(crc16(b"123456789"), 12739);
    assert_eq!(key_slot(b"123456789"), 12739);

    // "foo" -> CRC16 = 44950, Slot = 44950 & 16383 = 12182
    assert_eq!(crc16(b"foo"), 44950);
    assert_eq!(key_slot(b"foo"), 12182);
}

#[test]
fn test_hashtags_co_location() {
    // Keys with the same hashtag must map to the exact same slot
    let user_profile = b"{user100}:profile";
    let user_orders = b"{user100}:orders";
    let user_settings = b"{user100}:settings";

    assert_eq!(key_slot(user_profile), key_slot(user_orders));
    assert_eq!(key_slot(user_profile), key_slot(user_settings));
    assert_eq!(key_slot(user_profile), key_slot(b"user100"));
}

#[test]
fn test_hashtag_edge_cases() {
    // Empty hashtag: "{}" -> whole key is hashed
    assert_eq!(extract_hashtag(b"{}user100"), b"{}user100");
    assert_ne!(key_slot(b"{}user100"), key_slot(b"user100"));

    // No closing brace: "{user100" -> whole key is hashed
    assert_eq!(extract_hashtag(b"{user100"), b"{user100");

    // Multiple braces: "{a}{b}" -> only "a" is hashed
    assert_eq!(extract_hashtag(b"{a}{b}"), b"a");
    assert_eq!(key_slot(b"{a}{b}"), key_slot(b"a"));

    // Nested braces: "{a{b}}" -> "a{b" is hashed
    assert_eq!(extract_hashtag(b"{a{b}}"), b"a{b");
}

#[test]
fn test_slot_range_boundary() {
    for i in 0..1000 {
        let key = format!("benchmark_key_{}", i);
        let slot = key_slot(key.as_bytes());
        assert!(slot < HASH_SLOTS, "Slot {} exceeded 16383", slot);
    }
}
