# Formal Verification Models (`formal/`)

This directory contains formal specifications proving FreeUnit's core concurrency and IPC invariants.

---

## Specifications

### 1. `FreeUnitIPC.tla`
Formally specifies:
- The dual-channel IPC mechanism: lockless shared-memory ring buffer (`nxt_app_queue_t`) coupled with Unix datagram doorbell signals (`DOORDEL`).
- The K1 bounded doorbell decision rule:
  $$\text{Notify}(pos, W) \equiv (pos < W)$$
- The baton-pass protocol ensuring an idle or waking worker re-signals peer workers if unprocessed queue depth remains after servicing an item.
- Proves `FIFOPreserved` and `Liveness` properties across arbitrary interleavings.

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
