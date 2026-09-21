use bytes::Bytes;
use std::sync::atomic::Ordering;
use RustiCache::{Command, Db, ReplicationState, Value};

#[test]
fn test_ping_command() {
    let db = Db::new();
    let repl = ReplicationState::new_master(None);
    let mut auth = true;

    let ping_val = Value::Array(Some(vec![Value::string("PING")]));
    let cmd = Command::from_value(&ping_val).unwrap();
    let res = cmd.execute(&db, &repl, None, None, &mut auth).unwrap();
    assert_eq!(res, Value::pong());

    let ping_msg = Value::Array(Some(vec![Value::string("PING"), Value::string("hello")]));
    let cmd_msg = Command::from_value(&ping_msg).unwrap();
    let res_msg = cmd_msg.execute(&db, &repl, None, None, &mut auth).unwrap();
    assert_eq!(res_msg, Value::string("hello"));
}

#[test]
fn test_echo_command() {
    let db = Db::new();
    let repl = ReplicationState::new_master(None);
    let mut auth = true;

    let echo_val = Value::Array(Some(vec![Value::string("ECHO"), Value::string("test message")]));
    let cmd = Command::from_value(&echo_val).unwrap();
    let res = cmd.execute(&db, &repl, None, None, &mut auth).unwrap();
    assert_eq!(res, Value::string("test message"));
}

#[test]
fn test_set_and_get_command() {
    let db = Db::new();
    let repl = ReplicationState::new_master(None);
    let mut auth = true;

    // SET key value
    let set_val = Value::Array(Some(vec![
        Value::string("SET"),
        Value::string("mykey"),
        Value::string("myval"),
    ]));
    let cmd = Command::from_value(&set_val).unwrap();
    let res = cmd.execute(&db, &repl, None, None, &mut auth).unwrap();
    assert_eq!(res, Value::ok());

    // GET key
    let get_val = Value::Array(Some(vec![Value::string("GET"), Value::string("mykey")]));
    let cmd_get = Command::from_value(&get_val).unwrap();
    let res_get = cmd_get.execute(&db, &repl, None, None, &mut auth).unwrap();
    assert_eq!(res_get, Value::BulkString(Some(Bytes::from_static(b"myval"))));

    // GET non-existent
    let get_missing = Value::Array(Some(vec![Value::string("GET"), Value::string("missing")]));
    let cmd_missing = Command::from_value(&get_missing).unwrap();
    let res_missing = cmd_missing.execute(&db, &repl, None, None, &mut auth).unwrap();
    assert_eq!(res_missing, Value::null_bulk());
}

#[test]
fn test_info_replication_command() {
    let db = Db::new();
    let master_repl = ReplicationState::new_master(Some("dummy_replid_12345".to_string()));
    let mut auth = true;

    let info_val = Value::Array(Some(vec![Value::string("INFO"), Value::string("replication")]));
    let cmd = Command::from_value(&info_val).unwrap();
    let res = cmd.execute(&db, &master_repl, None, None, &mut auth).unwrap();

    let info_str = res.as_str().unwrap();
    assert!(info_str.contains("role:master"));
    assert!(info_str.contains("master_replid:dummy_replid_12345"));
    assert!(info_str.contains("connected_slaves:0"));

    // Test replica info output
    let replica_repl = ReplicationState::new_replica("127.0.0.1".to_string(), 6379);
    let res_replica = cmd.execute(&db, &replica_repl, None, None, &mut auth).unwrap();
    let replica_str = res_replica.as_str().unwrap();
    assert!(replica_str.contains("role:slave"));
    assert!(replica_str.contains("master_host:127.0.0.1"));
    assert!(replica_str.contains("master_port:6379"));
}

#[test]
fn test_replconf_getack_command() {
    let db = Db::new();
    let repl = ReplicationState::new_replica("127.0.0.1".to_string(), 6379);
    repl.replica_bytes_processed.store(154, Ordering::SeqCst);
    let mut auth = true;

    let getack_val = Value::Array(Some(vec![
        Value::string("REPLCONF"),
        Value::string("GETACK"),
        Value::string("*"),
    ]));
    let cmd = Command::from_value(&getack_val).unwrap();
    let res = cmd.execute(&db, &repl, None, None, &mut auth).unwrap();

    assert_eq!(
        res,
        Value::Array(Some(vec![
            Value::BulkString(Some(Bytes::from_static(b"REPLCONF"))),
            Value::BulkString(Some(Bytes::from_static(b"ACK"))),
            Value::string("154"),
        ]))
    );
}

#[test]
fn test_auth_security() {
    let db = Db::new();
    let repl = ReplicationState::new_master(None);
    let requirepass = Some("secret123");
    let mut authenticated = false;

    // 1. Unauthenticated SET must fail with NOAUTH
    let set_val = Value::Array(Some(vec![
        Value::string("SET"),
        Value::string("k"),
        Value::string("v"),
    ]));
    let cmd_set = Command::from_value(&set_val).unwrap();
    let res_err = cmd_set.execute(&db, &repl, None, requirepass, &mut authenticated).unwrap();
    assert_eq!(res_err, Value::error("NOAUTH Authentication required."));

    // 2. AUTH with wrong password must fail
    let auth_wrong = Value::Array(Some(vec![
        Value::string("AUTH"),
        Value::string("wrongpass"),
    ]));
    let cmd_auth_wrong = Command::from_value(&auth_wrong).unwrap();
    let res_wrong = cmd_auth_wrong.execute(&db, &repl, None, requirepass, &mut authenticated).unwrap();
    assert_eq!(res_wrong, Value::error("WRONGPASS invalid username-password pair or user is disabled."));
    assert!(!authenticated);

    // 3. AUTH with correct password succeeds
    let auth_correct = Value::Array(Some(vec![
        Value::string("AUTH"),
        Value::string("secret123"),
    ]));
    let cmd_auth_ok = Command::from_value(&auth_correct).unwrap();
    let res_ok = cmd_auth_ok.execute(&db, &repl, None, requirepass, &mut authenticated).unwrap();
    assert_eq!(res_ok, Value::ok());
    assert!(authenticated);

    // 4. Now SET succeeds
    let res_set_ok = cmd_set.execute(&db, &repl, None, requirepass, &mut authenticated).unwrap();
    assert_eq!(res_set_ok, Value::ok());
}
