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
        INV1["INV-1: Strict FIFO Stream Ordering"]
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

Model B checks 6 fundamental properties against the recorded event trace:

### 1. `INV-1-FIFO-ORDER` (Stream Ordering Invariant)
- **Statement**: For any single client connection or HTTP stream $S$, all response headers and data chunks must be received by the client in strict monotonically non-decreasing order:
  $$\forall i < j: \text{seq}(chunk_i) < \text{seq}(chunk_j)$$
- **Adversarial Pressure**: Socket datagrams are delayed or reordered relative to lockless ring buffer descriptors.

### 2. `INV-2-NO-UAF` (Process Lifecycle & No Use-After-Free)
- **Statement**: When an application worker dies abruptly (e.g., via `SIGKILL`), no router thread or peer worker may dereference its port descriptors or attempt to post messages to a closed file descriptor.
- **Historical Fix**: Issue #195 (Commit `4d81e916`), where `nxt_runtime_process_ref()` and atomic CAS loops on `use_count` prevented double-free transitions.

### 3. `INV-3-NO-LEAK` (Shared-Memory Conservation)
- **Statement**: Across all completed or aborted requests, the total number of shared memory chunks currently marked as allocated must return exactly to zero once the system returns to the idle quiescent state:
  $$\text{AllocatedChunks}(\text{quiescent}) = 0$$

### 4. `INV-4-PROGRESS` (Deadlock & Liveness Freedom)
- **Statement**: The system never enters an unrecoverable stall. Every admitted request must either finish with an HTTP response or terminate with an explicit error status (e.g., HTTP 500/503 or connection reset) within a bounded simulated duration.

### 5. `INV-5-FSM-SAFETY` (Connection State Transition Legality)
- **Statement**: Client connections transition only along valid directed edges of the HTTP lifecycle state graph. In particular, a connection in `CLOSED` state must never receive subsequent read or write operations.

### 6. `INV-6-ISOLATION` (Application Worker State Isolation)
- **Statement**: Across consecutive requests executed on the same persistent worker process, no state mutations (e.g. database transactions, authentication tokens, global superglobals) may leak from Request $A$ into Request $B$.
