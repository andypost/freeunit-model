# Model B - Adversarial Verification & Chaos Invariant Engine

Model B uses combinatorial and property-based exploration to verify that FreeUnit preserves fundamental safety, liveness, and isolation invariants under extreme concurrency, network jitter, and unexpected worker crashes.

---

## Verification Architecture

```mermaid
flowchart TD
    Config["Test Scenario Configuration<br/>(requests, workers, engines, seed)"]
    Explorer["StateSpaceExplorer<br/>(unit_model.analyzer.explorer)"]
    Loop["SimEventLoop (Deterministic Clock)"]
    
    subgraph Adversaries["Adversarial Fault Injectors"]
        Interleaver["AdversarialInterleaver<br/>Permutes socket vs queue delivery timing"]
        Chaos["ChaosFaultInjector<br/>Worker SIGKILL, OOSM starvation, client RST"]
    end

    subgraph Verification["Invariant Checker (unit_model.analyzer.invariants)"]
        INV1["INV-1: Header once, before any body byte"]
        INV2["INV-2: Process Safety &amp; No UAF"]
        INV3["INV-3: Zero Shared-Memory Leaks"]
        INV4["INV-4: Deadlock &amp; Progress Freedom"]
        INV5["INV-5: Connection FSM Safety"]
        INV6["INV-6: Worker Application State Isolation"]
    end

    Result["Exploration Result<br/>(Passed / Violations Report)"]

    Config --> Explorer
    Explorer --> Loop
    Explorer --> Interleaver
    Explorer --> Chaos
    Interleaver &amp; Chaos --> Loop
    Loop --> Verification
    Verification --> Result

    classDef blue fill:#172554,stroke:#3b82f6,stroke-width:2px,color:#bfdbfe;
    classDef yellow fill:#451a03,stroke:#f59e0b,stroke-width:2px,color:#fde68a;
    classDef green fill:#064e3b,stroke:#10b981,stroke-width:2px,color:#a7f3d0;
    class Config,Explorer,Loop blue;
    class Interleaver,Chaos yellow;
    class INV1,INV2,INV3,INV4,INV5,INV6,Result green;
```

---

## The 6 Core Invariants

The invariants describe FreeUnit master `42251f7c9e366df31a2e70a5da3679a2fa855b3d`. INV-1, INV-3, INV-4 and INV-5 are checked by `InvariantChecker.verify_trace()` on the recorded trace; INV-2 and INV-6 are asserted by the chaos and framework tests.

### 1. `INV-1-HEADER-ONCE` (Header once, before any body byte)
- **Statement**: For every stream the router sends the status line and fields to the client exactly once, and before any body byte; body parts follow in message order.
- **Master**: `nxt_router_response_ready_handler` parses the header on the first message and sends it before the body parts (`src/nxt_router.c:6543-6618`); later messages take the `r->header_sent` branch and only append body (`:6512-6540`).
- **Adversarial pressure**: socket datagrams are delayed relative to queue descriptors; a header-after-body or a second header is a violation.

### 2. `INV-2-NO-UAF` (Process Lifecycle & No Use-After-Free)
- **Statement**: When an application worker dies abruptly (e.g., via `SIGKILL`), no router thread or peer worker may dereference its port descriptors or attempt to post messages to a closed file descriptor.
- **Historical fix**: Issue #195 (commit `4d81e916`), where `nxt_runtime_process_ref()` and atomic CAS loops on `use_count` prevented double-free transitions.

### 3. `INV-3-NO-LEAK` (Shared-Memory Conservation)
- **Statement**: Once the system is quiescent, no shared memory chunk is still marked busy:
  $$\text{AllocatedChunks}(\text{quiescent}) = 0$$
- **Master release points**: the worker releases request chunks when it releases the request (`src/nxt_unit.c:2621-2627`) and the unused tail of a response buffer after the send (`:3661-3680`); the router releases response chunks when the buffer completes after the client write (`nxt_port_mmap_buf_completion`, `src/nxt_port_memory.c:167-240`), or at once for a header buffer that carries no piggyback content (`src/nxt_router.c:6543-6555`).

### 4. `INV-4-PROGRESS` (Response Completes)
- **Statement**: Every dispatched stream ends with the message that carries the last flag (`stream_completed`) or with an error status (503 before the header, a closed connection after it: `nxt_http_request_error`, `src/nxt_router.c:6676`).

### 5. `INV-5-FSM-SAFETY` (No I/O after close)
- **Statement**: A connection in `CLOSED` state receives no header, body part or flush.

### 6. `INV-6-ISOLATION` (Application Worker State Isolation)
- **Statement**: Across consecutive requests executed on the same persistent worker process, no state mutations (database transactions, authentication tokens, superglobals) leak from request $A$ into request $B$.

---

## Open Questions and Proposals (not in master)

The items below are **proposals**. They are not in FreeUnit master `42251f7c9e366df31a2e70a5da3679a2fa855b3d` and the model does not assert them. They are listed so that nobody reads them as a description of master.

### Compression hold invariants (HOLD-1 .. HOLD-6)

Master decides compression on the first response message and sends the header at once (`nxt_http_comp_check_acceptable`, `nxt_http_comp_apply_compression`, `src/nxt_router.c:6569-6586`); it never holds body bytes back to decide. A router that held the first bytes until it knows whether the body reaches `min_length` would have to keep these invariants:

- **HOLD-1** Held state: the header is not sent, the held bytes are fewer than `min_length`, and the last message has not arrived.
- **HOLD-2** The header is sent exactly once and before any body byte (INV-1 continues to hold while bytes are held).
- **HOLD-3** The hold commits or aborts within the hold timer.
- **HOLD-4** Held buffers are released on commit, on close, on application crash, on timeout and on reconfigure.
- **HOLD-5** The coding decision is made once and is consistent between the header and every body part.
- **HOLD-6** `1xx`/`101`, `HEAD`, `204` and `304` responses are never held.

### Other proposals mentioned in this repository

- **K2 single-buffer response framing** (`NXT_PHP_RESPONSE_ROOM`): not in master (`git grep NXT_PHP_RESPONSE_ROOM origin/master` is empty). Master's framing is one message per write, see [Model A](model-a-simulation.md#the-response-path-libunit-to-router).
- **K1 worker-side baton pass**: not in master. The doorbell is decided by the producer (`nxt_app_queue_send_bound`, `src/nxt_app_queue.h:82-144`: `cmp_set` on `notified` for one reader, `pos < depth_bound` for more); no worker rings another worker.
- **Lock-free per-engine shared memory slabs** (`synthesis/lockfree_shm_synth.py`): a proposal; master allocates under `lib->outgoing.mutex` / `mmaps->mutex`.
- **HTTP/2** (`protocols/h2.py`): in open FreeUnit pull requests (#463, #471, #479, #483); master speaks HTTP/1.x.
