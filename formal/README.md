# Formal Verification Models (`formal/`)

This directory contains formal specifications proving FreeUnit's core concurrency and IPC invariants.

---

## Specifications

### 1. `FreeUnitIPC.tla`
Formally specifies the router -> application queue of FreeUnit master
`42251f7c9e366df31a2e70a5da3679a2fa855b3d` (`src/nxt_app_queue.h`):
- The lockless shared-memory ring buffer (`nxt_app_queue_t`) coupled with the `READ_QUEUE` doorbell datagram.
- The producer-side doorbell decision (`nxt_app_queue_send_bound`, `src/nxt_app_queue.h:82-144`): with one reader, ring on the `0 -> 1` transition of `notified`; with more readers, ring when the item lands at a position below `depth_bound`:
  $$\text{Notify}(pos, W) \equiv (pos < 0) \lor (pos < W)$$
- The reader clears the flag when it takes a notification and drains the queue before it sleeps.
- Checks `FIFOPreserved` (items complete in enqueue order), `DoorbellBounded` and `Liveness`.

A worker-side "baton pass" (a worker re-ringing peers when depth remains) is **not in master**; it was removed from this specification and is listed as a proposal in `docs/models/model-b-verification.md`.

### 2. `FreeUnitNNCQ.tla`
Formally specifies:
- The 32-bit atomic counter wrap behavior across `NXT_APP_NNCQ_CYCLES`.
- Proves `NoCycleCollision`: slot entry cycle markers never collide or wrap into invalid states.

---

## Running the TLC Model Checker

If you have TLA+ tools installed (e.g. `tla2tools.jar`):

```bash
# Check Dual-Channel IPC specification
java -cp tla2tools.jar tlc2.TLC formal/tla/FreeUnitIPC.tla -config formal/tla/FreeUnitIPC.cfg

# Check NNCQ Index Wrap specification
java -cp tla2tools.jar tlc2.TLC formal/tla/FreeUnitNNCQ.tla -config formal/tla/FreeUnitNNCQ.cfg
```
