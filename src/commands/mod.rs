use bytes::{Bytes, BytesMut};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::protocol::Value;
use crate::replication::ReplicationState;
use crate::storage::Db;

#[derive(Debug, Clone)]
pub enum Command {
    Ping(Option<String>),
    Echo(String),
    Get(String),
    Set {
        key: String,
        value: Bytes,
        px_ms: Option<u64>,
    },
    Del(Vec<String>),
    Exists(Vec<String>),
    Keys(String),
    Info(Option<String>),
    ReplConf(Vec<String>),
    Psync { repl_id: String, offset: String },
    Auth {
        #[allow(dead_code)]
        username: Option<String>,
        password: String,
    },
    Quit,
    Command,
}

impl Command {
    pub fn from_value(val: &Value) -> Result<Command, String> {
        let items = match val {
            Value::Array(Some(items)) => items,
            _ => return Err("Expected Array frame for Redis command".to_string()),
        };

        if items.is_empty() {
            return Err("Empty command array".to_string());
        }

        let cmd_name = items[0]
            .as_str()
            .ok_or_else(|| "First element must be string command name".to_string())?
            .to_ascii_uppercase();

        match cmd_name.as_str() {
            "PING" => {
                let msg = if items.len() > 1 {
                    items[1].as_str().map(|s| s.to_string())
                } else {
                    None
                };
                Ok(Command::Ping(msg))
            }
            "ECHO" => {
                if items.len() < 2 {
                    return Err("ERR wrong number of arguments for 'echo' command".to_string());
                }
                let msg = items[1]
                    .as_str()
                    .unwrap_or_default()
                    .to_string();
                Ok(Command::Echo(msg))
            }
            "GET" => {
                if items.len() < 2 {
                    return Err("ERR wrong number of arguments for 'get' command".to_string());
                }
                let key = items[1]
                    .as_str()
                    .ok_or_else(|| "Key must be string".to_string())?
                    .to_string();
                Ok(Command::Get(key))
            }
            "SET" => {
                if items.len() < 3 {
                    return Err("ERR wrong number of arguments for 'set' command".to_string());
                }
                let key = items[1]
                    .as_str()
                    .ok_or_else(|| "Key must be string".to_string())?
                    .to_string();
                let val_bytes = items[2]
                    .as_bytes()
                    .cloned()
                    .unwrap_or_else(|| Bytes::from(items[2].as_str().unwrap_or("").to_string()));

                let mut px_ms = None;
                let mut idx = 3;
                while idx < items.len() {
                    let opt = items[idx].as_str().unwrap_or("").to_ascii_uppercase();
                    if opt == "PX" && idx + 1 < items.len() {
                        if let Some(ms_str) = items[idx + 1].as_str() {
                            px_ms = ms_str.parse::<u64>().ok();
                        }
                        idx += 2;
                    } else {
                        idx += 1;
                    }
                }

                Ok(Command::Set {
                    key,
                    value: val_bytes,
                    px_ms,
                })
            }
            "DEL" => {
                if items.len() < 2 {
                    return Err("ERR wrong number of arguments for 'del' command".to_string());
                }
                let keys = items[1..]
                    .iter()
                    .filter_map(|item| item.as_str().map(|s| s.to_string()))
                    .collect();
                Ok(Command::Del(keys))
            }
            "EXISTS" => {
                if items.len() < 2 {
                    return Err("ERR wrong number of arguments for 'exists' command".to_string());
                }
                let keys = items[1..]
                    .iter()
                    .filter_map(|item| item.as_str().map(|s| s.to_string()))
                    .collect();
                Ok(Command::Exists(keys))
            }
            "KEYS" => {
                let pattern = if items.len() > 1 {
                    items[1].as_str().unwrap_or("*").to_string()
                } else {
                    "*".to_string()
                };
                Ok(Command::Keys(pattern))
            }
            "INFO" => {
                let section = items.get(1).and_then(|v| v.as_str()).map(|s| s.to_string());
                Ok(Command::Info(section))
            }
            "REPLCONF" => {
                let args = items[1..]
                    .iter()
                    .filter_map(|item| item.as_str().map(|s| s.to_string()))
                    .collect();
                Ok(Command::ReplConf(args))
            }
            "PSYNC" => {
                let repl_id = items.get(1).and_then(|v| v.as_str()).unwrap_or("?").to_string();
                let offset = items.get(2).and_then(|v| v.as_str()).unwrap_or("-1").to_string();
                Ok(Command::Psync { repl_id, offset })
            }
            "AUTH" => {
                if items.len() == 2 {
                    let password = items[1].as_str().unwrap_or("").to_string();
                    Ok(Command::Auth {
                        username: None,
                        password,
                    })
                } else if items.len() >= 3 {
                    let username = items[1].as_str().map(|s| s.to_string());
                    let password = items[2].as_str().unwrap_or("").to_string();
                    Ok(Command::Auth { username, password })
                } else {
                    Err("ERR wrong number of arguments for 'auth' command".to_string())
                }
            }
            "QUIT" => Ok(Command::Quit),
            "COMMAND" => Ok(Command::Command),
            other => Err(format!("unknown command '{}'", other)),
        }
    }

    pub fn execute(
        &self,
        db: &Db,
        repl_state: &ReplicationState,
        raw_cmd: Option<&BytesMut>,
        requirepass: Option<&str>,
        is_authenticated: &mut bool,
    ) -> Option<Value> {
        // Enforce AUTH if requirepass is set
        if let Some(pass) = requirepass {
            if !*is_authenticated {
                match self {
                    Command::Auth { password, .. } => {
                        if password == pass {
                            *is_authenticated = true;
                            return Some(Value::ok());
                        } else {
                            return Some(Value::error("WRONGPASS invalid username-password pair or user is disabled."));
                        }
                    }
                    Command::Quit => return Some(Value::ok()),
                    _ => {
                        return Some(Value::error("NOAUTH Authentication required."));
                    }
                }
            }
        }

        match self {
            Command::Auth { .. } => {
                // If no requirepass is set, AUTH always succeeds
                *is_authenticated = true;
                Some(Value::ok())
            }
            Command::Quit => Some(Value::ok()),
            Command::Ping(msg) => {
                if let Some(m) = msg {
                    Some(Value::string(m.clone()))
                } else {
                    Some(Value::pong())
                }
            }
            Command::Echo(msg) => Some(Value::string(msg.clone())),
            Command::Get(key) => match db.get(key) {
                Some(bytes) => Some(Value::BulkString(Some(bytes))),
                None => Some(Value::null_bulk()),
            },
            Command::Set { key, value, px_ms } => {
                let expires_at = px_ms.map(|ms| Instant::now() + Duration::from_millis(ms));
                db.set(key.clone(), value.clone(), expires_at);

                // Propagate write commands to downstream replicas if master
                if !repl_state.is_replica() {
                    if let Some(raw) = raw_cmd {
                        repl_state.propagate_downstream(raw.clone().freeze());
                    }
                }

                Some(Value::ok())
            }
            Command::Del(keys) => {
                let mut count = 0;
                for k in keys {
                    if db.del(k) {
                        count += 1;
                    }
                }
                if count > 0 && !repl_state.is_replica() {
                    if let Some(raw) = raw_cmd {
                        repl_state.propagate_downstream(raw.clone().freeze());
                    }
                }
                Some(Value::Integer(count))
            }
            Command::Exists(keys) => {
                let mut count = 0;
                for k in keys {
                    if db.exists(k) {
                        count += 1;
                    }
                }
                Some(Value::Integer(count))
            }
            Command::Keys(_pattern) => {
                let keys = db.keys();
                let values = keys.into_iter().map(Value::string).collect();
                Some(Value::Array(Some(values)))
            }
            Command::Info(_section) => {
                let info = repl_state.info_replication();
                Some(Value::string(info))
            }
            Command::ReplConf(args) => {
                if args.is_empty() {
                    return Some(Value::error("ERR REPLCONF requires arguments"));
                }
                let sub = args[0].to_ascii_uppercase();
                if sub == "GETACK" {
                    let offset = repl_state.replica_bytes_processed.load(Ordering::SeqCst);
                    let resp = Value::Array(Some(vec![
                        Value::BulkString(Some(Bytes::from_static(b"REPLCONF"))),
                        Value::BulkString(Some(Bytes::from_static(b"ACK"))),
                        Value::string(offset.to_string()),
                    ]));
                    return Some(resp);
                } else if sub == "ACK" {
                    return None;
                }
                Some(Value::ok())
            }
            Command::Psync { .. } => {
                let fullresync = format!("FULLRESYNC {} 0", repl_state.get_replid());
                Some(Value::SimpleString(fullresync))
            }
            Command::Command => Some(Value::Array(Some(vec![]))),
        }
    }
}
