use bytes::{Bytes, BytesMut};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use RustiCache::{ReplicationState, Server, Value};

#[tokio::test]
async fn test_master_replica_replication_e2e() {
    let master_port = 16379;
    let replica_port = 16380;

    // 1. Start Master Server
    let master_repl_state = ReplicationState::new_master(Some("test_master_replid_1234".to_string()));
    let master_server = Server::new(master_port, master_repl_state);
    tokio::spawn(async move {
        let _ = master_server.run().await;
    });

    // Allow master server to bind and listen
    tokio::time::sleep(Duration::from_millis(150)).await;

    // 2. Start Replica Server connecting to Master
    let replica_repl_state = ReplicationState::new_replica("127.0.0.1".to_string(), master_port);
    let replica_server = Server::new(replica_port, replica_repl_state);
    tokio::spawn(async move {
        let _ = replica_server.run().await;
    });

    // Allow replica to connect, perform handshake and RDB transfer
    tokio::time::sleep(Duration::from_millis(300)).await;

    // 3. Connect client to Master and execute SET
    let mut master_client = TcpStream::connect(format!("127.0.0.1:{}", master_port))
        .await
        .expect("Failed to connect to master");

    // SET greeting "hello_from_master"
    let set_cmd = b"*3\r\n$3\r\nSET\r\n$8\r\ngreeting\r\n$17\r\nhello_from_master\r\n";
    master_client.write_all(set_cmd).await.unwrap();

    let mut buf = [0u8; 1024];
    let n = master_client.read(&mut buf).await.unwrap();
    let resp = std::str::from_utf8(&buf[..n]).unwrap();
    assert_eq!(resp, "+OK\r\n");

    // Allow propagation to replica
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 4. Connect client to Replica and execute GET
    let mut replica_client = TcpStream::connect(format!("127.0.0.1:{}", replica_port))
        .await
        .expect("Failed to connect to replica");

    // GET greeting
    let get_cmd = b"*2\r\n$3\r\nGET\r\n$8\r\ngreeting\r\n";
    replica_client.write_all(get_cmd).await.unwrap();

    let n = replica_client.read(&mut buf).await.unwrap();
    let mut read_buf = BytesMut::from(&buf[..n]);
    let val = Value::parse(&mut read_buf).unwrap().expect("Expected response");

    assert_eq!(
        val,
        Value::BulkString(Some(Bytes::from_static(b"hello_from_master")))
    );

    // 5. Query INFO replication from Replica
    let info_cmd = b"*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n";
    replica_client.write_all(info_cmd).await.unwrap();

    let n = replica_client.read(&mut buf).await.unwrap();
    let mut info_buf = BytesMut::from(&buf[..n]);
    let info_val = Value::parse(&mut info_buf).unwrap().expect("Expected info response");

    let info_str = info_val.as_str().unwrap();
    assert!(info_str.contains("role:slave"));
    assert!(info_str.contains("master_host:127.0.0.1"));
    assert!(info_str.contains("master_port:16379"));
}

#[tokio::test]
async fn test_master_replica_authenticated_replication_e2e() {
    use RustiCache::ServerConfig;

    let master_port = 17379;
    let replica_port = 17380;
    let password = "enterprise_secret_pass".to_string();

    // 1. Master server with requirepass
    let mut master_cfg = ServerConfig::default();
    master_cfg.port = master_port;
    master_cfg.requirepass = Some(password.clone());

    let master_repl_state = ReplicationState::new_master(Some("auth_master_replid_5678".to_string()));
    let master_server = Server::with_server_config(master_cfg, master_repl_state);
    tokio::spawn(async move {
        let _ = master_server.run().await;
    });

    tokio::time::sleep(Duration::from_millis(150)).await;

    // 2. Replica server with masterauth configured
    let mut replica_cfg = ServerConfig::default();
    replica_cfg.port = replica_port;
    replica_cfg.masterauth = Some(password.clone());
    replica_cfg.replicaof = Some(("127.0.0.1".to_string(), master_port));

    let replica_repl_state = ReplicationState::new_replica("127.0.0.1".to_string(), master_port);
    let replica_server = Server::with_server_config(replica_cfg, replica_repl_state);
    tokio::spawn(async move {
        let _ = replica_server.run().await;
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    // 3. Connect client to Master, authenticate and write key
    let mut master_client = TcpStream::connect(format!("127.0.0.1:{}", master_port))
        .await
        .expect("Failed to connect to auth master");

    // AUTH enterprise_secret_pass
    let auth_cmd = format!("*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n", password.len(), password);
    master_client.write_all(auth_cmd.as_bytes()).await.unwrap();

    let mut buf = [0u8; 1024];
    let n = master_client.read(&mut buf).await.unwrap();
    let resp = std::str::from_utf8(&buf[..n]).unwrap();
    assert_eq!(resp, "+OK\r\n");

    // SET secret_data "classified"
    let set_cmd = b"*3\r\n$3\r\nSET\r\n$11\r\nsecret_data\r\n$10\r\nclassified\r\n";
    master_client.write_all(set_cmd).await.unwrap();
    let n = master_client.read(&mut buf).await.unwrap();
    assert_eq!(std::str::from_utf8(&buf[..n]).unwrap(), "+OK\r\n");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // 4. Connect client to Replica and read replicated data
    let mut replica_client = TcpStream::connect(format!("127.0.0.1:{}", replica_port))
        .await
        .expect("Failed to connect to auth replica");

    // GET secret_data
    let get_cmd = b"*2\r\n$3\r\nGET\r\n$11\r\nsecret_data\r\n";
    replica_client.write_all(get_cmd).await.unwrap();

    let n = replica_client.read(&mut buf).await.unwrap();
    let mut read_buf = BytesMut::from(&buf[..n]);
    let val = Value::parse(&mut read_buf).unwrap().expect("Expected response");

    assert_eq!(
        val,
        Value::BulkString(Some(Bytes::from_static(b"classified")))
    );
}

