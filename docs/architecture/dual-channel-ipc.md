# Dual-Channel IPC & Invariants

A critical design characteristic of FreeUnit is its dual-channel IPC architecture between the router process and application workers. This document explains the operational dynamics, historical race conditions, and how `freeunit-model` formally verifies message ordering.

---

## Channels Comparison

| Property | Application Queue (`nxt_app_queue_t`) | Unix Datagram Socket (`nxt_port_socket.c`) |
|---|---|---|
| **Underlying Mechanism** | Lockless circular ring buffer in shared memory | `AF_UNIX` datagram socket pair (`SOCK_DGRAM`) |
| **Descriptor Payload** | Maximum 31 bytes (`size` + `data[31]` + `tracking`) | Unbounded socket datagrams + ancillary `SCM_RIGHTS` |
| **Signaling** | Atomic `notified` flag / Doorbell (`DOORDEL`) | Socket readiness wake-up via `epoll` / `poll` |
| **Typical Use** | Fast-path request dispatch (GET/HEAD without large bodies) | Large headers, multi-chunk bodies, control messages (`SHM_ACK`) |

---

## Historical Race Condition: Message Inversion (#573, #597)

Prior to FreeUnit commits `39700f29` and `66048b7b`, a subtle timing hazard existed when requests transitioned between the shared-memory queue and the Unix datagram socket:

```mermaid
sequenceDiagram
    autonumber
    participant Router as Router Engine
    participant Queue as App Queue (Ring Buffer)
    participant Socket as Unix Datagram Socket
    participant Worker as Worker Process (libunit)

    Note over Router,Worker: Request 1: Small GET (sent via Queue)
    Router->>Queue: Enqueue Request 1 descriptor
    
    Note over Router,Worker: Request 2: Large POST (sent via Socket)
    Router->>Socket: Send Request 2 REQ_HEADERS
    
    Note over Worker: Worker wakes up on Socket event first!
    Worker->>Socket: Read Request 2 REQ_HEADERS
    Worker->>Worker: Starts processing Request 2
    
    Note over Worker: Hazard: Request 1 was overtaken by Request 2!
    Worker->>Queue: Drains Request 1 from queue (out of order!)
```

### The Failure Mode
If the worker process checked the socket first while descriptors were still pending in the queue ring buffer, a later datagram message could overtake an earlier queued message. In HTTP streaming or WebSocket sessions, this resulted in response bodies being parsed as headers, triggering spurious `HTTP 503 Service Unavailable` errors.

---

## The Solution: Queue Drain Barrier & Request FSM

To guarantee strict First-In, First-Out (FIFO) ordering across both transport channels, FreeUnit introduced:
1. **Explicit Queue State Machine**: Each request transitions strictly across `UNQUEUED` $\to$ `QUEUED` $\to$ `SOCKET`.
2. **Queue Drain Barrier**: Before reading any new datagram from the Unix socket, workers must unconditionally drain the shared-memory ring buffer to exhaustion.

```mermaid
stateDiagram-v2
    [*] --> Idle: Worker waiting for events

    Idle --> CheckingQueue: Doorbell received or poll wake
    CheckingQueue --> ProcessingQueueItem: Queue not empty
    ProcessingQueueItem --> CheckingQueue: Drain next item
    
    CheckingQueue --> PollingSocket: Queue is completely EMPTY (Barrier satisfied)
    PollingSocket --> ProcessingSocketMsg: Datagram available
    ProcessingSocketMsg --> CheckingQueue: Check queue before next socket read
    
    PollingSocket --> Idle: No messages on socket
```

---

## Verification in `freeunit-model`

In `freeunit-model`, this ordering contract is proven by **`AdversarialInterleaver`** (`src/unit_model/analyzer/interleaver.py`) and validated by `tests/test_dual_channel.py`.

The test simulates:
- 30 rapid sequential requests dispatched across both queue and socket channels.
- Randomized arrival delays injected into the socket channel.
- The invariant checker confirms that response stream IDs arrive at the client in strict monotonic sequence, proving that the queue drain barrier eliminates the dual-channel inversion hazard.
