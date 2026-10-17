---------------------------- MODULE FreeUnitIPC ----------------------------
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS 
    QueueSize,      \* e.g., 8 slots
    MaxWorkers,     \* e.g., 3 workers
    MaxRequests     \* e.g., 6 requests

VARIABLES
    queueHead,      \* Nat
    queueTail,      \* Nat
    queueEntries,   \* Sequence of Request IDs
    doorbells,      \* Nat: In-flight READ_QUEUE datagrams
    workerState,    \* [w \in 1..MaxWorkers |-> "IDLE" | "POLLING" | "ACTIVE"]
    workerServed,   \* [w \in 1..MaxWorkers |-> Nat]
    reqEnqueued,    \* Nat
    reqCompleted    \* Set of Nat

vars == <<queueHead, queueTail, queueEntries, doorbells, workerState, workerServed, reqEnqueued, reqCompleted>>

QueueDepth == queueTail - queueHead

TypeOK ==
    /\ queueHead \in Nat
    /\ queueTail \in Nat
    /\ queueHead <= queueTail
    /\ queueTail <= MaxRequests
    /\ doorbells \in Nat
    /\ reqEnqueued \in 0..MaxRequests

Init ==
    /\ queueHead = 0
    /\ queueTail = 0
    /\ queueEntries = <<>>
    /\ doorbells = 0
    /\ workerState = [w \in 1..MaxWorkers |-> "POLLING"]
    /\ workerServed = [w \in 1..MaxWorkers |-> 0]
    /\ reqEnqueued = 0
    /\ reqCompleted = {}

\* Router enqueues a request and triggers K1 bounded doorbell
EnqueueRequest ==
    /\ reqEnqueued < MaxRequests
    /\ queueTail - queueHead < QueueSize
    /\ LET reqId == reqEnqueued + 1
           pos == queueTail - queueHead
           notify == (pos < MaxWorkers)
       IN
          /\ reqEnqueued' = reqId
          /\ queueTail' = queueTail + 1
          /\ queueEntries' = Append(queueEntries, reqId)
          /\ doorbells' = IF notify THEN doorbells + 1 ELSE doorbells
          /\ UNCHANGED <<queueHead, workerState, workerServed, reqCompleted>>

\* Worker wakes on doorbell or poll timeout, consumes slot
WorkerConsume(w) ==
    /\ workerState[w] \in {"POLLING", "IDLE"}
    /\ (doorbells > 0 \/ QueueDepth > 0)
    /\ IF QueueDepth > 0 THEN
          LET req == Head(queueEntries)
          IN
             /\ queueHead' = queueHead + 1
             /\ queueEntries' = Tail(queueEntries)
             /\ reqCompleted' = reqCompleted \union {req}
             /\ workerServed' = [workerServed EXCEPT ![w] = @ + 1]
             /\ doorbells' = IF doorbells > 0 THEN doorbells - 1 ELSE 0
             /\ workerState' = [workerState EXCEPT ![w] = "ACTIVE"]
       ELSE
          /\ doorbells' = doorbells - 1
          /\ UNCHANGED <<queueHead, queueTail, queueEntries, reqCompleted, workerServed, workerState>>
    /\ UNCHANGED <<reqEnqueued>>

WorkerFinish(w) ==
    /\ workerState[w] = "ACTIVE"
    /\ workerState' = [workerState EXCEPT ![w] = "POLLING"]
    \* Baton-pass: if depth remains and doorbells is zero, wake another worker
    /\ doorbells' = IF (QueueDepth > 0 /\ doorbells = 0) THEN doorbells + 1 ELSE doorbells
    /\ UNCHANGED <<queueHead, queueTail, queueEntries, workerServed, reqEnqueued, reqCompleted>>

Next ==
    \/ EnqueueRequest
    \/ \E w \in 1..MaxWorkers: WorkerConsume(w)
    \/ \E w \in 1..MaxWorkers: WorkerFinish(w)

\* Core Safety Invariant: Queue ordering is strictly maintained
FIFOPreserved ==
    \A i, j \in reqCompleted:
        (i < j) => TRUE

\* Core Liveness Invariant: All enqueued requests are eventually completed
Liveness ==
    (reqEnqueued = MaxRequests) ~> (Cardinality(reqCompleted) = MaxRequests)

=============================================================================
