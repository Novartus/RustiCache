use bytes::{Buf, BytesMut};
use std::sync::atomic::Ordering;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{error, info, warn};

use super::state::ReplicationState;
use crate::commands::Command;
use crate::protocol::Value;
use crate::storage::Db;

pub async fn run_replica_loop(
    master_host: String,
    master_port: u16,
    my_port: u16,
    db: Db,
    repl_state: ReplicationState,
    masterauth: Option<String>,
) {
    let addr = format!("{}:{}", master_host, master_port);
    info!("Replica connecting to master at {}", addr);

    let mut stream = match TcpStream::connect(&addr).await {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to connect to master at {}: {}", addr, e);
            return;
        }
    };

    let mut buffer = BytesMut::with_capacity(64 * 1024);

    // 1. Send PING
    if let Err(e) = stream.write_all(b"*1\r\n$4\r\nPING\r\n").await {
        error!("Failed to send PING to master: {}", e);
        return;
    }
    if let Err(e) = read_simple_response(&mut stream, &mut buffer).await {
        error!("Failed reading PONG from master: {}", e);
        return;
    }

    // 1.5 Authenticate with master if masterauth is set
    if let Some(pass) = masterauth {
        let auth_cmd = format!(
            "*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n",
            pass.len(),
            pass
        );
        if let Err(e) = stream.write_all(auth_cmd.as_bytes()).await {
            error!("Failed sending AUTH to master: {}", e);
            return;
        }
        if let Err(e) = read_simple_response(&mut stream, &mut buffer).await {
            error!("Failed authenticating with master: {}", e);
            return;
        }
    }

    // 2. Send REPLCONF listening-port <my_port>
    let port_str = my_port.to_string();
    let replconf_port = format!(
        "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",
        port_str.len(),
        port_str
    );
    if let Err(e) = stream.write_all(replconf_port.as_bytes()).await {
        error!("Failed sending REPLCONF listening-port: {}", e);
        return;
    }
    if let Err(e) = read_simple_response(&mut stream, &mut buffer).await {
        error!("Failed reading OK for REPLCONF listening-port: {}", e);
        return;
    }

    // 3. Send REPLCONF capa eof capa psync2
    let replconf_capa = "*5\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
    if let Err(e) = stream.write_all(replconf_capa.as_bytes()).await {
        error!("Failed sending REPLCONF capa: {}", e);
        return;
    }
    if let Err(e) = read_simple_response(&mut stream, &mut buffer).await {
        error!("Failed reading OK for REPLCONF capa: {}", e);
        return;
    }

    // 4. Send PSYNC ? -1
    let psync_cmd = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    if let Err(e) = stream.write_all(psync_cmd.as_bytes()).await {
        error!("Failed sending PSYNC: {}", e);
        return;
    }
    if let Err(e) = read_fullresync_response(&mut stream, &mut buffer, &repl_state).await {
        error!("Failed reading FULLRESYNC from master: {}", e);
        return;
    }

    // 5. Ingest RDB file
    if let Err(e) = read_rdb_file(&mut stream, &mut buffer).await {
        error!("Failed reading RDB dump from master: {}", e);
        return;
    }

    info!("Replication handshake complete! Now streaming commands from master.");

    // 6. Continuous Command Replication Stream
    let mut total_bytes_processed: u64 = 0;

    loop {
        // Try parsing any full commands in the buffer
        while !buffer.is_empty() {
            let initial_len = buffer.len();
            let mut cursor = buffer.clone();

            match Value::parse(&mut cursor) {
                Ok(Some(val)) => {
                    let consumed = initial_len - cursor.len();
                    buffer.advance(consumed);

                    // Execute command
                    match Command::from_value(&val) {
                        Ok(Command::ReplConf(args)) => {
                            if !args.is_empty() && args[0].to_ascii_uppercase() == "GETACK" {
                                // ACK current offset including GETACK bytes
                                total_bytes_processed += consumed as u64;
                                repl_state
                                    .replica_bytes_processed
                                    .store(total_bytes_processed, Ordering::SeqCst);

                                let offset_str = total_bytes_processed.to_string();
                                let ack_reply = format!(
                                    "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
                                    offset_str.len(),
                                    offset_str
                                );
                                if let Err(e) = stream.write_all(ack_reply.as_bytes()).await {
                                    error!("Failed sending REPLCONF ACK to master: {}", e);
                                    return;
                                }
                                continue;
                            }
                        }
                        Ok(cmd) => {
                            let mut auth_ok = true;
                            cmd.execute(&db, &repl_state, None, None, &mut auth_ok, None);
                        }
                        Err(e) => {
                            warn!("Unknown command in replication stream: {}", e);
                        }
                    }

                    total_bytes_processed += consumed as u64;
                    repl_state
                        .replica_bytes_processed
                        .store(total_bytes_processed, Ordering::SeqCst);
                }
                Ok(None) => {
                    // Need more bytes
                    break;
                }
                Err(e) => {
                    error!("Error parsing replication stream frame: {}", e);
                    // Discard 1 byte to prevent endless loop on corrupted data
                    buffer.advance(1);
                    break;
                }
            }
        }

        // Read more data from master
        let mut temp = [0u8; 8192];
        match stream.read(&mut temp).await {
            Ok(0) => {
                warn!("Master closed replication connection");
                break;
            }
            Ok(n) => {
                buffer.extend_from_slice(&temp[..n]);
            }
            Err(e) => {
                error!("Error reading replication stream from master: {}", e);
                break;
            }
        }
    }
}

async fn read_simple_response(
    stream: &mut TcpStream,
    buffer: &mut BytesMut,
) -> Result<(), String> {
    loop {
        if let Some(pos) = buffer.windows(2).position(|w| w == b"\r\n") {
            buffer.advance(pos + 2);
            return Ok(());
        }
        let mut temp = [0u8; 1024];
        let n = stream
            .read(&mut temp)
            .await
            .map_err(|e| format!("Socket read error: {}", e))?;
        if n == 0 {
            return Err("Unexpected EOF from master".to_string());
        }
        buffer.extend_from_slice(&temp[..n]);
    }
}

async fn read_fullresync_response(
    stream: &mut TcpStream,
    buffer: &mut BytesMut,
    repl_state: &ReplicationState,
) -> Result<(), String> {
    loop {
        if let Some(pos) = buffer.windows(2).position(|w| w == b"\r\n") {
            let line = buffer.split_to(pos + 2);
            let s = std::str::from_utf8(&line).map_err(|e| e.to_string())?;
            if s.starts_with("+FULLRESYNC") {
                let parts: Vec<&str> = s.trim().split_whitespace().collect();
                if parts.len() >= 3 {
                    // parts[1] is repl_id, parts[2] is offset
                    let id = parts[1].to_string();
                    info!("Full resync received: ID={}, offset={}", id, parts[2]);
                    repl_state.set_replid(id);
                }
                return Ok(());
            } else {
                return Err(format!("Expected +FULLRESYNC, got: {}", s.trim()));
            }
        }
        let mut temp = [0u8; 1024];
        let n = stream
            .read(&mut temp)
            .await
            .map_err(|e| format!("Socket read error: {}", e))?;
        if n == 0 {
            return Err("Unexpected EOF reading FULLRESYNC".to_string());
        }
        buffer.extend_from_slice(&temp[..n]);
    }
}

async fn read_rdb_file(
    stream: &mut TcpStream,
    buffer: &mut BytesMut,
) -> Result<(), String> {
    // RDB format: $<length>\r\n<payload> (without trailing CRLF)
    loop {
        if !buffer.is_empty() && buffer[0] == b'$' {
            if let Some(pos) = buffer.windows(2).position(|w| w == b"\r\n") {
                let header = buffer.split_to(pos + 2);
                let len_str = std::str::from_utf8(&header[1..pos]).map_err(|e| e.to_string())?;
                let rdb_len: usize = len_str.parse().map_err(|_| "Invalid RDB length")?;

                while buffer.len() < rdb_len {
                    let mut temp = [0u8; 8192];
                    let n = stream
                        .read(&mut temp)
                        .await
                        .map_err(|e| format!("Error reading RDB: {}", e))?;
                    if n == 0 {
                        return Err("EOF while reading RDB payload".to_string());
                    }
                    buffer.extend_from_slice(&temp[..n]);
                }

                // Consume the RDB binary payload
                buffer.advance(rdb_len);
                info!("Successfully loaded RDB snapshot of {} bytes", rdb_len);
                return Ok(());
            }
        }

        let mut temp = [0u8; 1024];
        let n = stream
            .read(&mut temp)
            .await
            .map_err(|e| format!("Error reading RDB header: {}", e))?;
        if n == 0 {
            return Err("EOF before RDB header".to_string());
        }
        buffer.extend_from_slice(&temp[..n]);
    }
}
