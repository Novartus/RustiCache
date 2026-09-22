use parking_lot::RwLock;
use std::sync::Arc;
use RustiCache::{
    key_slot, ClusterNode, ClusterTopology, Command, Db, ReplicationState, Value,
};

fn setup_cluster_node() -> (Db, ReplicationState, Arc<RwLock<ClusterTopology>>) {
    let db = Db::new();
    let repl = ReplicationState::new_master(None);

    let myself = ClusterNode::new(
        "1111111111111111111111111111111111111111".to_string(),
        "127.0.0.1".to_string(),
        7000,
        17000,
        true,
        true,
    );
    let mut topo = ClusterTopology::new(myself);
    topo.add_slots_range("1111111111111111111111111111111111111111", 0, 8191)
        .unwrap();

    let peer = ClusterNode::new(
        "2222222222222222222222222222222222222222".to_string(),
        "127.0.0.1".to_string(),
        7001,
        17001,
        false,
        true,
    );
    topo.add_node(peer);
    topo.add_slots_range("2222222222222222222222222222222222222222", 8192, 16383)
        .unwrap();

    let cluster = Arc::new(RwLock::new(topo));
    (db, repl, cluster)
}

#[test]
fn test_cluster_keyslot_command() {
    let (db, repl, cluster) = setup_cluster_node();
    let mut auth = true;

    let cmd_val = Value::Array(Some(vec![
        Value::string("CLUSTER"),
        Value::string("KEYSLOT"),
        Value::string("{user}:data"),
    ]));
    let cmd = Command::from_value(&cmd_val).unwrap();
    let res = cmd
        .execute(&db, &repl, None, None, &mut auth, Some(&cluster))
        .unwrap();

    let expected_slot = key_slot(b"user");
    assert_eq!(res, Value::Integer(expected_slot as i64));
}

#[test]
fn test_cluster_info_command() {
    let (db, repl, cluster) = setup_cluster_node();
    let mut auth = true;

    let cmd_val = Value::Array(Some(vec![
        Value::string("CLUSTER"),
        Value::string("INFO"),
    ]));
    let cmd = Command::from_value(&cmd_val).unwrap();
    let res = cmd
        .execute(&db, &repl, None, None, &mut auth, Some(&cluster))
        .unwrap();

    let info_str = res.as_str().unwrap();
    assert!(info_str.contains("cluster_state:ok"));
    assert!(info_str.contains("cluster_slots_assigned:16384"));
    assert!(info_str.contains("cluster_slots_ok:16384"));
    assert!(info_str.contains("cluster_known_nodes:2"));
    assert!(info_str.contains("cluster_size:2"));
}

#[test]
fn test_cluster_nodes_command() {
    let (db, repl, cluster) = setup_cluster_node();
    let mut auth = true;

    let cmd_val = Value::Array(Some(vec![
        Value::string("CLUSTER"),
        Value::string("NODES"),
    ]));
    let cmd = Command::from_value(&cmd_val).unwrap();
    let res = cmd
        .execute(&db, &repl, None, None, &mut auth, Some(&cluster))
        .unwrap();

    let nodes_str = res.as_str().unwrap();
    assert!(nodes_str.contains("1111111111111111111111111111111111111111 127.0.0.1:7000@17000 myself,master - 0 0 1 connected 0-8191"));
    assert!(nodes_str.contains("2222222222222222222222222222222222222222 127.0.0.1:7001@17001 master - 0 0 1 connected 8192-16383"));
}

#[test]
fn test_cluster_slots_command() {
    let (db, repl, cluster) = setup_cluster_node();
    let mut auth = true;

    let cmd_val = Value::Array(Some(vec![
        Value::string("CLUSTER"),
        Value::string("SLOTS"),
    ]));
    let cmd = Command::from_value(&cmd_val).unwrap();
    let res = cmd
        .execute(&db, &repl, None, None, &mut auth, Some(&cluster))
        .unwrap();

    match res {
        Value::Array(Some(ranges)) => {
            assert_eq!(ranges.len(), 2);
            // First range: [0, 8191, ["127.0.0.1", 7000, ...]]
            if let Value::Array(Some(first_range)) = &ranges[0] {
                assert_eq!(first_range[0], Value::Integer(0));
                assert_eq!(first_range[1], Value::Integer(8191));
            } else {
                panic!("Expected first range array");
            }

            // Second range: [8192, 16383, ["127.0.0.1", 7001, ...]]
            if let Value::Array(Some(second_range)) = &ranges[1] {
                assert_eq!(second_range[0], Value::Integer(8192));
                assert_eq!(second_range[1], Value::Integer(16383));
            } else {
                panic!("Expected second range array");
            }
        }
        _ => panic!("Expected Value::Array for CLUSTER SLOTS"),
    }
}

#[test]
fn test_cluster_disabled_error() {
    let db = Db::new();
    let repl = ReplicationState::new_master(None);
    let mut auth = true;

    let cmd_val = Value::Array(Some(vec![
        Value::string("CLUSTER"),
        Value::string("INFO"),
    ]));
    let cmd = Command::from_value(&cmd_val).unwrap();
    let res = cmd
        .execute(&db, &repl, None, None, &mut auth, None)
        .unwrap();

    assert_eq!(
        res,
        Value::error("ERR This instance has cluster support disabled")
    );
}
