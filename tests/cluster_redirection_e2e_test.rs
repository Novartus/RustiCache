use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use RustiCache::{
    key_slot, ClusterNode, ReplicationState, Server, ServerConfig,
};

#[tokio::test]
async fn test_cluster_moved_redirection_e2e() {
    let port_a = 18379;
    let port_b = 18380;

    // 1. Configure Node A (slots 0-8191)
    let mut cfg_a = ServerConfig::default();
    cfg_a.port = port_a;
    cfg_a.cluster_enabled = true;
    cfg_a.cluster_node_id = Some("aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111".to_string());
    cfg_a.cluster_announce_ip = "127.0.0.1".to_string();
    cfg_a.cluster_announce_port = port_a;
    cfg_a.cluster_slots = Some("0-8191".to_string());

    let server_a = Server::with_server_config(cfg_a, ReplicationState::new_master(None));
    let cluster_a = server_a.cluster_manager().expect("Cluster manager expected");

    // Register Node B in Node A's topology
    {
        let peer_b = ClusterNode::new(
            "bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222".to_string(),
            "127.0.0.1".to_string(),
            port_b,
            port_b + 10000,
            false,
            true,
        );
        let mut topo = cluster_a.write();
        topo.add_node(peer_b);
        topo.add_slots_range("bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222", 8192, 16383).unwrap();
    }

    tokio::spawn(async move {
        let _ = server_a.run().await;
    });

    // 2. Configure Node B (slots 8192-16383)
    let mut cfg_b = ServerConfig::default();
    cfg_b.port = port_b;
    cfg_b.cluster_enabled = true;
    cfg_b.cluster_node_id = Some("bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222".to_string());
    cfg_b.cluster_announce_ip = "127.0.0.1".to_string();
    cfg_b.cluster_announce_port = port_b;
    cfg_b.cluster_slots = Some("8192-16383".to_string());

    let server_b = Server::with_server_config(cfg_b, ReplicationState::new_master(None));
    let cluster_b = server_b.cluster_manager().expect("Cluster manager expected");

    // Register Node A in Node B's topology
    {
        let peer_a = ClusterNode::new(
            "aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111".to_string(),
            "127.0.0.1".to_string(),
            port_a,
            port_a + 10000,
            false,
            true,
        );
        let mut topo = cluster_b.write();
        topo.add_node(peer_a);
        topo.add_slots_range("aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111", 0, 8191).unwrap();
    }

    tokio::spawn(async move {
        let _ = server_b.run().await;
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Find keys targeting slot < 8192 (Node A) and slot >= 8192 (Node B)
    let mut key_for_a = String::new();
    let mut key_for_b = String::new();
    let mut slot_a = 0;
    let mut slot_b = 0;

    for i in 0..1000 {
        let k = format!("test_key_{}", i);
        let s = key_slot(k.as_bytes());
        if s < 8192 && key_for_a.is_empty() {
            key_for_a = k;
            slot_a = s;
        } else if s >= 8192 && key_for_b.is_empty() {
            key_for_b = k;
            slot_b = s;
        }
        if !key_for_a.is_empty() && !key_for_b.is_empty() {
            break;
        }
    }

    // 3. Connect to Node A
    let mut client_a = TcpStream::connect(format!("127.0.0.1:{}", port_a))
        .await
        .expect("Failed to connect to Node A");

    // Query key_for_a on Node A -> Success (+OK)
    let set_a_cmd = format!(
        "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$5\r\nalpha\r\n",
        key_for_a.len(),
        key_for_a
    );
    client_a.write_all(set_a_cmd.as_bytes()).await.unwrap();

    let mut buf = [0u8; 1024];
    let n = client_a.read(&mut buf).await.unwrap();
    assert_eq!(std::str::from_utf8(&buf[..n]).unwrap(), "+OK\r\n");

    // Query key_for_b on Node A -> Must redirect with -MOVED <slot_b> 127.0.0.1:18380
    let set_b_cmd = format!(
        "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$4\r\nbeta\r\n",
        key_for_b.len(),
        key_for_b
    );
    client_a.write_all(set_b_cmd.as_bytes()).await.unwrap();

    let n = client_a.read(&mut buf).await.unwrap();
    let moved_resp = std::str::from_utf8(&buf[..n]).unwrap();
    let expected_moved = format!("-MOVED {} 127.0.0.1:{}\r\n", slot_b, port_b);
    assert_eq!(moved_resp, expected_moved);

    // 4. Connect to Node B
    let mut client_b = TcpStream::connect(format!("127.0.0.1:{}", port_b))
        .await
        .expect("Failed to connect to Node B");

    // Query key_for_a on Node B -> Must redirect with -MOVED <slot_a> 127.0.0.1:18379
    let get_a_cmd = format!(
        "*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n",
        key_for_a.len(),
        key_for_a
    );
    client_b.write_all(get_a_cmd.as_bytes()).await.unwrap();

    let n = client_b.read(&mut buf).await.unwrap();
    let moved_resp_b = std::str::from_utf8(&buf[..n]).unwrap();
    let expected_moved_a = format!("-MOVED {} 127.0.0.1:{}\r\n", slot_a, port_a);
    assert_eq!(moved_resp_b, expected_moved_a);

    // Query key_for_b on Node B -> Success
    client_b.write_all(set_b_cmd.as_bytes()).await.unwrap();
    let n = client_b.read(&mut buf).await.unwrap();
    assert_eq!(std::str::from_utf8(&buf[..n]).unwrap(), "+OK\r\n");
}
