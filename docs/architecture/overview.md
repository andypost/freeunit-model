# FreeUnit Subsystem Architecture & Lifecycle

This document provides a detailed breakdown of FreeUnit's runtime architecture, data plane mechanics, and how each subsystem is modeled in `freeunit-model`.

---

## Process Topology

FreeUnit operates as a process hierarchy designed for fault tolerance and zero-downtime reconfiguration:

```mermaid
graph TD
    Main["Main Process (Supervisor)<br/>Privileged, sets up ports, handles signals"]
    Controller["Controller Process<br/>REST API, JSON configuration, validation"]
    Router["Router Process<br/>Multi-threaded event loop, TLS, routing, proxy"]
    
    subgraph Workers["Worker Process Pools"]
        direction TB
        AppPoolA["Application Pool: 'web'<br/>Worker processes (PHP / Python / Go)"]
        AppPoolB["Application Pool: 'jobs'<br/>Isolated worker pool (Drupal cron, etc.)"]
    end

    Main -->|Spawns &amp; Monitors| Controller
    Main -->|Spawns &amp; Monitors| Router
    Main -->|Spawns &amp; Monitors| AppPoolA
    Main -->|Spawns &amp; Monitors| AppPoolB
    Controller -.->|Applies Config via IPC| Router

    classDef proc fill:#e8f0fe,stroke:#4285f4,stroke-width:2px;
    classDef pool fill:#e6f4ea,stroke:#34a853,stroke-width:2px;
    class Main,Controller,Router proc;
    class AppPoolA,AppPoolB pool;
```

---

## Request Lifecycle Sequence

When an external client initiates an HTTP request, the data travels through the following sequence across threads and processes:

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Listener as Listener (*:8080)
    participant Engine as Router Engine (Thread)
    participant SHM as Shared Memory Pool
    participant AppQueue as App Queue (Ring Buffer)
    participant Worker as App Worker (libunit)
    participant SAPI as App Runtime (PHP/Python)

    Client->>Listener: TCP SYN + Handshake
    Listener->>Engine: EPOLLEXCLUSIVE Accept
    Client->>Engine: HTTP Request Bytes
    Engine->>Engine: Parse HTTP/1.1 or H2 Headers
    Engine->>SHM: Allocate 16 KiB chunk (under mmaps->mutex)
    SHM-->>Engine: Chunk Pointer & mmap_id
    Engine->>SHM: Serialize nxt_unit_request_t & headers
    
    alt Request fits in 31 bytes
        Engine->>AppQueue: Push nxt_app_queue_item_t
        Note over Engine,AppQueue: Enqueue lockless ring buffer item
    else Body exceeds queue threshold
        Engine->>Worker: Send REQ_HEADERS via Unix Datagram Socket
    end

    Worker->>AppQueue: Drain item from queue
    Worker->>SHM: Read nxt_unit_request_t
    Worker->>SAPI: Invoke request handler
    SAPI->>SAPI: Execute user application logic
    SAPI-->>Worker: Emit HTTP Response (status, headers, body)
    
    Worker->>SHM: Write nxt_unit_response_t
    Worker->>Engine: Send RESP_HEADERS via Unix Datagram Socket
    Engine->>Client: Send HTTP response to client socket
    Worker->>Engine: Release SHM chunk (SHM_ACK)
    Engine->>SHM: Return chunk to free list
```

---

## Subsystem Details

### 1. Listener (`src/unit_model/engine/listener.py`)
- FreeUnit uses `EPOLLEXCLUSIVE` on Linux to avoid the thundering herd problem when multiple router threads wait on the listening socket.
- Connections are tracked via explicit state machines (`INIT`, `ACCEPTED`, `HANDSHAKE`, `READING`, `PROCESSING`, `SENDING`, `KEEPALIVE_IDLE`, `CLOSING`, `CLOSED`).
- Idle connections in keep-alive state are managed with connection-scoped tasks (`c->task`) and deferred release timers (`r->timer`), ensuring no dangling pointer dereferences occur when an application timeout fires.

### 2. Router Engine (`src/unit_model/engine/router.py`)
- Operates $N$ event engines (`nxt_event_engine_t`), each running a non-blocking event loop.
- Manages an RPC stream hash table (`RpcStream`), tracking in-flight requests from creation until client response completion.
- Dispatches requests to workers using a dual-channel strategy:
  - If the request descriptor fits within the 31-byte inline payload of an `nxt_app_queue_item_t`, it is placed directly onto the lockless ring buffer.
  - If the request requires socket datagram transport, it falls back to Unix domain sockets.

### 3. Shared Memory Pool (`src/unit_model/engine/shm_pool.py`)
- Request and response bodies are transferred via pre-allocated shared memory segments partitioned into 16 KiB chunks.
- Allocations are protected by `mmaps->mutex`.
- Implements Out-Of-Shared-Memory (OOSM) backpressure: when free chunks are exhausted, workers and router engines enter probe-mode or defer writes until `SHM_ACK` messages return freed chunks.

### 4. App Worker Runtime (`src/unit_model/engine/worker.py`)
- Built on `libunit` C client library.
- Workers poll both the private socket pair and the shared-memory queue ring buffer.
- To prevent race conditions, workers enforce a strict **queue drain barrier**: they must completely drain any pending descriptors in the shared-memory ring buffer before falling back to socket datagram processing.
