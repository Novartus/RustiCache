# RustiCache 🦀⚡

RustiCache is a high-performance, Redis-compatible in-memory datastore and replication engine implemented in Rust using the `tokio` asynchronous runtime.

This document details the architectural design, concurrency model, RESP protocol pipeline, and Redis master-replica replication subsystem.

---

## 1. High-Level Architecture

The system is decoupled into four primary layers:
1. **Network & Session Layer**: Asynchronous I/O processing powered by Tokio. Handles client TCP connections and upstream master connections.
2. **Protocol Engine (RESP2/RESP3)**: Zero-copy parser translating raw byte streams into typed commands and serializing responses.
3. **Storage & State Engine**: Sharded concurrent in-memory key-value store supporting TTL expiration, eviction policies, and atomic operations.
4. **Replication Subsystem**: Implements the Redis replication protocol (handshake, PSYNC, RDB ingestion, continuous replication stream, offset ACK tracking).

```mermaid
graph TB
    subgraph Clients["Clients & External Nodes"]
        CLI["redis-cli / Client Apps"]
        Master["Redis Master Server"]
        DownstreamReplica["Downstream Replicas"]
    end

    subgraph RustiCache["RustiCache Instance (Rust / Tokio)"]
        subgraph Net["Network & I/O Layer"]
            Listener["Async TCP Listener (Port 6379)"]
            ConnMgr["Connection Manager & Session Context"]
            MasterConn["Master Client Connection (Port Outbound)"]
        end

        subgraph Parser["Protocol Pipeline"]
            RESP["RESP2/RESP3 Streaming Parser"]
            Encoder["RESP Serializer / Formatter"]
        end

        subgraph Router["Execution & Dispatch Engine"]
            Dispatcher["Command Dispatcher & Router"]
            TxMgr["MULTI / EXEC Transaction State"]
        end

        subgraph Storage["In-Memory Storage Core"]
            ShardedStore["Sharded Key-Value Store\n(Partitioned Concurrent Map)"]
            TTLIndex["TTL Eviction Wheel / Active Expiry"]
        end

        subgraph Repl["Replication Engine"]
            ReplMaster["Replica Role Manager\n(Handshake, PSYNC, Offset Tracking)"]
            ReplBacklog["Replication Backlog Ring Buffer\n(Circular Buffer + Accumulator)"]
            RdbLoader["RDB Snapshot Deserializer"]
        end
    end

    CLI -->|TCP / RESP| Listener
    Listener --> ConnMgr
    ConnMgr --> RESP
    RESP --> Dispatcher
    Dispatcher --> TxMgr
    Dispatcher --> ShardedStore
    ShardedStore --> TTLIndex
    Dispatcher --> Encoder
    Encoder --> ConnMgr

    %% Master to Replica Replication
    MasterConn -->|PING / REPLCONF / PSYNC| Master
    Master -->|RDB Dump| MasterConn
    Master -->|Replication Stream| MasterConn
    MasterConn --> ReplMaster
    ReplMaster -->|Load Initial State| RdbLoader
    RdbLoader --> ShardedStore
    ReplMaster -->|Apply Command Stream| Dispatcher
    ReplMaster -->|REPLCONF ACK offset| MasterConn

    %% Replica propagation to downstream
    Dispatcher -->|Propagate Writes| ReplBacklog
    ReplBacklog --> DownstreamReplica
```

---

## 2. Replication Subsystem Architecture

RustiCache implements the Redis Replication Protocol to operate as a replica to an upstream master (or serve downstream replicas):

1. **Replication Handshake**:
   - `PING`: Verifies master liveness.
   - `REPLCONF listening-port <port>`: Informs the master of this node's port.
   - `REPLCONF capa eof capa psync2`: Advertises replication capabilities.
2. **Synchronization Negotiation (`PSYNC`)**:
   - Sends `PSYNC <repl_id> <offset>`.
   - If first sync: `PSYNC ? -1` triggers **Full Resync** (`+FULLRESYNC <repl_id> <offset>`).
   - If reconnecting: Attempts **Partial Resync** (`+CONTINUE <repl_id>`).
3. **RDB Snapshot Transfer & Ingestion**:
   - Parses incoming bulk payload containing the binary RDB snapshot and loads keys directly into the storage engine.
4. **Continuous Command Propagation**:
   - Master forwards all write commands in RESP format.
   - RustiCache executes them without returning client response packets.
   - Responds to `REPLCONF GETACK *` with `REPLCONF ACK <processed_bytes_offset>`.

### Replication Flow Sequence

```mermaid
sequenceDiagram
    autonumber
    participant R as RustiCache (Replica)
    participant M as Redis Master

    Note over R,M: 1. TCP Connection & Handshake
    R->>M: TCP Connect
    R->>M: *1\r\n$4\r\nPING\r\n
    M-->>R: +PONG\r\n
    R->>M: *3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n
    M-->>R: +OK\r\n
    R->>M: *5\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n
    M-->>R: +OK\r\n

    Note over R,M: 2. Synchronization Handshake
    R->>M: *3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n
    M-->>R: +FULLRESYNC <repl_id> <initial_offset>\r\n

    Note over R,M: 3. Full Resync: Snapshot Streaming
    M-->>R: $<rdb_length>\r\n<RDB_BINARY_PAYLOAD>
    R->>R: Ingest RDB dump into Storage Engine

    Note over R,M: 4. Continuous Command Streaming
    loop Replication Stream
        M->>R: *3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n
        R->>R: Apply SET k v to local storage
        R->>R: Accumulate processed byte offset
        
        opt Heartbeat / Ack Check
            M->>R: *3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n
            R->>M: *3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$<len>\r\n<processed_offset>\r\n
        end
    end
```

---

## 3. Concurrency & Storage Engine Model

To achieve high throughput with minimal contention:

```mermaid
flowchart LR
    subgraph Ingress["Ingress & Request Handlers"]
        ClientReq["Incoming Client Tasks<br/>(Tokio Async Streams)"]
        ReplReq["Replication Stream Task"]
    end

    Router{"Hash Router<br/>hash & mask"}

    subgraph Sharding["Sharded Partitions (64 or 128 Shards)"]
        S0["Shard 0<br/>RwLock HashMap"]
        S1["Shard 1<br/>RwLock HashMap"]
        S2["Shard 2<br/>RwLock HashMap"]
        Sn["Shard N...<br/>RwLock HashMap"]
    end

    subgraph Expiry["Expiry Manager"]
        Wheel["Active TTL Eviction Task"]
    end

    ClientReq --> Router
    ReplReq --> Router

    Router --> S0
    Router --> S1
    Router --> S2
    Router --> Sn

    Wheel -.->|"Periodic Key Eviction"| S0
    Wheel -.->|"Periodic Key Eviction"| Sn
```

### Storage Entry Structure
Each stored entry encapsulates:
- `value`: `DataType` enum (`String`, `List`, `Hash`, `Set`, `SortedSet`, `Stream`).
- `expires_at`: Optional timestamp (`Instant` / unix epoch ms) for TTL tracking.
- `version` / `last_accessed`: For LRU/LFU cache eviction policies.

---

## 4. Module Decomposition

```
RustiCache/
├── Cargo.toml
├── README.md
└── src/
    ├── main.rs                 # CLI arguments, config parsing & startup
    ├── server.rs               # Server loop and task supervision
    ├── connection.rs           # Framed TCP socket wrapper
    ├── protocol/
    │   ├── mod.rs
    │   ├── resp.rs             # RESP2 & RESP3 parser & types
    │   └── error.rs            # Protocol & decoding errors
    ├── commands/
    │   ├── mod.rs              # Command enum & router
    │   ├── strings.rs          # GET, SET, INCR, DECR, etc.
    │   ├── server.rs           # PING, ECHO, INFO, COMMAND
    │   └── replication.rs      # REPLCONF, PSYNC, WAIT
    ├── storage/
    │   ├── mod.rs
    │   ├── db.rs               # Database interface & shard manager
    │   ├── entry.rs            # Value types & metadata
    │   └── ttl.rs              # Passive & active expiry runner
    └── replication/
        ├── mod.rs
        ├── master.rs           # Logic when RustiCache runs as Master
        ├── replica.rs          # Client connecting to upstream Redis Master
        ├── handshake.rs        # Handshake sequence machine
        ├── rdb.rs              # RDB snapshot parser & loader
        └── backlog.rs          # In-memory circular buffer for partial resync
```

---

## 5. Technology Stack & Crates

- **Runtime**: [`tokio`](https://crates.io/crates/tokio) (Configurable multi-threaded runtime with dedicated worker pools)
- **Networking & Framing**: [`tokio-util`](https://crates.io/crates/tokio-util) (`codec` for RESP framing), [`bytes`](https://crates.io/crates/bytes) (Zero-copy byte buffers)
- **Concurrency**: [`parking_lot`](https://crates.io/crates/parking_lot) with power-of-two bitmask sharded hash partitions
- **CLI & Config**: [`clap`](https://crates.io/crates/clap) with environment variable bindings
- **Observability**: [`tracing`](https://crates.io/crates/tracing) & [`tracing-subscriber`](https://crates.io/crates/tracing-subscriber)

---

## 6. Enterprise Standards & Features

1. **Security & Authentication**:
   - **`AUTH` & `requirepass`**: Supports password-protected client connections. Unauthenticated commands are blocked with `-NOAUTH Authentication required.`.
   - **`masterauth`**: Automatic upstream master authentication during replica handshake.
   - **Connection Limiting**: Bounded concurrent connections (`RUSTICACHE_MAX_CONNECTIONS`) via an atomic semaphore to prevent exhaustion attacks.
   - **Buffer & Payload Guards**: Maximum single frame size limit (`RUSTICACHE_MAX_PAYLOAD_SIZE_BYTES`) preventing memory exhaustion attacks.
   - **`TCP_NODELAY`**: Enabled by default to disable Nagle's algorithm and provide minimal socket latency.

2. **Concurrency & Multiprocessing**:
   - **Multi-Threaded Runtime**: Explicit worker pool control (`RUSTICACHE_WORKER_THREADS`, 0 for auto-core detection) via `tokio::runtime::Builder::new_multi_thread()`.
   - **Power-of-Two Bitmask Sharding**: Scalable storage partitioning (`RUSTICACHE_SHARD_COUNT`) utilizing fast bitwise masking `(hash & (N - 1))` instead of modulo arithmetic.

3. **Memory Management & Eviction**:
   - **LRU Memory Capacity**: Configurable memory threshold (`RUSTICACHE_MAXMEMORY_BYTES`). When memory limit is reached, least recently used keys are evicted.
   - **Dual-Tier Expiry**: Passive/lazy cleanup on `GET` plus background active eviction sampling.

4. **Lifecycle & Reliability**:
   - **Graceful Shutdown**: Traps `SIGINT`/`Ctrl+C` and broadcasts coordinated termination to active client connections and background replication loops.

---

## 7. Configuration & Environment Variables

RustiCache supports configuration via both environment variables (or a local `.env` file) and CLI flags. CLI flags take precedence over `.env` values.

### Setting up `.env`:
```bash
cp .env.example .env
```

### Supported Configuration Options:

| Environment Variable | CLI Flag | Default | Description |
| :--- | :--- | :--- | :--- |
| `RUSTICACHE_PORT` | `-p, --port` | `6379` | Port to bind and listen on |
| `RUSTICACHE_HOST` | `--host` | `0.0.0.0` | Network interface / IP to bind to |
| `RUSTICACHE_REQUIREPASS` | `--requirepass` | *None* | Require clients to authenticate with `AUTH <password>` |
| `RUSTICACHE_MASTERAUTH` | `--masterauth` | *None* | Password for authenticating with master when running as replica |
| `RUSTICACHE_MAX_CONNECTIONS` | `--max-connections` | `10000` | Maximum concurrent client connections |
| `RUSTICACHE_MAX_PAYLOAD_SIZE_BYTES`| `--max-payload-size` | `536870912` (512MB) | Maximum payload size per request (anti-DoS guard) |
| `RUSTICACHE_TCP_NODELAY` | `--tcp-nodelay` | `true` | Enable TCP_NODELAY socket optimization |
| `RUSTICACHE_WORKER_THREADS` | `--worker-threads` | `0` (auto) | Tokio multi-thread worker count (0 = auto-detect CPU cores) |
| `RUSTICACHE_SHARD_COUNT` | `--shard-count` | `128` | Number of concurrent storage shards (power of 2) |
| `RUSTICACHE_MAXMEMORY_BYTES` | `--max-memory-bytes` | `0` (unlimited) | Max memory in bytes before LRU eviction triggers |
| `RUSTICACHE_TTL_INTERVAL_MS` | `--ttl-interval-ms` | `100` | Background active TTL eviction frequency in ms |
| `RUSTICACHE_TTL_SAMPLE_SIZE` | `--ttl-sample-size` | `20` | Number of keys sampled per shard during active eviction |
| `RUSTICACHE_REPLICAOF` | `--replicaof <host> <port>` | *None* | Set to `"<master_host> <master_port>"` to start in replica mode |
| `RUST_LOG` | *N/A* | `info` | Logging verbosity (`error`, `warn`, `info`, `debug`, `trace`) |

---

## 8. Running RustiCache

### Standalone / Master Mode:
```bash
# Using .env defaults (port 6379)
cargo run

# Or specifying port and password via CLI flags
cargo run -- --port 6379 --requirepass "mypassword"
```

### Replica Mode:
```bash
# Via CLI flag (syncing from master on port 6379 with authentication):
cargo run -- --port 6380 --replicaof 127.0.0.1 6379 --masterauth "mypassword"

# Or via .env:
# RUSTICACHE_PORT=6380
# RUSTICACHE_REPLICAOF="127.0.0.1 6379"
# RUSTICACHE_MASTERAUTH="mypassword"
cargo run
```

### Running Tests:
```bash
cargo test
```

