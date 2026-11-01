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

    classDef proc fill:#172554,stroke:#3b82f6,stroke-width:2px,color:#bfdbfe;
    classDef pool fill:#064e3b,stroke:#10b981,stroke-width:2px,color:#a7f3d0;
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
    Engine->>Engine: Parse HTTP/1.1 Headers (HTTP/2 is in open PRs, not master)
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
    
    Worker->>SHM: Release request chunk (request released)
    Worker->>SHM: Write nxt_unit_response_t + first write (piggyback)
    Worker->>Engine: DATA message (port queue if <= 31 bytes, else socket)
    Engine->>Client: Header once, then the piggyback body part
    loop one message per further application write
        Worker->>Engine: DATA message (shm chunk or plain)
        Engine->>Client: Body part
        Engine->>SHM: Release chunk after the client write
    end
    Worker->>Engine: Empty DATA, last=1
    Engine->>Client: End of response
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
- Implements Out-Of-Shared-Memory (OOSM) backpressure as `libunit` does: a buffer of at most 1024 bytes probes for a chunk and falls back to plain memory; a larger one sends `OOSM` and blocks until `SHM_ACK` (`src/nxt_unit.c:5296-5346`, `5402-5470`), which the router broadcasts when it frees a chunk or already sees one free.

### 4. App Worker Runtime (`src/unit_model/engine/worker.py`)
- Built on `libunit` C client library.
- Workers poll both the private socket pair and the shared-memory queue ring buffer.
- Responses are sent one port message per application write (`nxt_unit_response_write_nb`): header + piggyback content first, a buffer per further write, an empty last message at the end. Request chunks are released by the worker when it releases the request.
- To prevent race conditions, workers enforce a strict **queue drain barrier**: they must completely drain any pending descriptors in the shared-memory ring buffer before falling back to socket datagram processing.
