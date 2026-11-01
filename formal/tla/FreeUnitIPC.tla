---------------------------- MODULE FreeUnitIPC ----------------------------
(***************************************************************************)
(* The router -> application queue of FreeUnit master                      *)
(* 42251f7c9e366df31a2e70a5da3679a2fa855b3d, src/nxt_app_queue.h.          *)
(*                                                                         *)
(* The producer decides the doorbell (nxt_app_queue_send_bound,            *)
(* src/nxt_app_queue.h:82-144): with one reader the READ_QUEUE datagram    *)
(* goes on the 0->1 transition of the notified flag; with more readers it  *)
(* goes when the item lands at a position below depth_bound                *)
(* (nxt_app_queue_notify_decision, src/nxt_app_queue.h:72-76).  The        *)
(* reader clears the flag when it takes a notification                     *)
(* (nxt_app_queue_notification_received, src/nxt_app_queue.h:156-165) and  *)
(* drains the queue before it sleeps.  Nothing in master makes a worker    *)
(* ring another worker: a worker-side "baton pass" is a proposal, not      *)
(* master, and it is not in this specification.                            *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    QueueSize,      \* e.g., 8 slots
    MaxWorkers,     \* e.g., 3 workers; also the depth bound of the doorbell
    MaxRequests     \* e.g., 6 requests

VARIABLES
    queueHead,      \* Nat
    queueTail,      \* Nat
    queueEntries,   \* Sequence of request ids still in the queue
    notified,       \* 0 or 1: the one-reader flag (used when MaxWorkers = 1)
    doorbells,      \* Nat: READ_QUEUE datagrams not yet taken by a worker
    workerState,    \* [w \in 1..MaxWorkers |-> "POLLING" | "ACTIVE"]
    workerServed,   \* [w \in 1..MaxWorkers |-> Nat]
    reqEnqueued,    \* Nat
    completedSeq    \* Sequence of request ids in completion order

vars == <<queueHead, queueTail, queueEntries, notified, doorbells, workerState,
          workerServed, reqEnqueued, completedSeq>>

QueueDepth == queueTail - queueHead

TypeOK ==
    /\ queueHead \in Nat
    /\ queueTail \in Nat
    /\ queueHead <= queueTail
    /\ queueTail <= MaxRequests
    /\ notified \in {0, 1}
    /\ doorbells \in Nat
    /\ reqEnqueued \in 0..MaxRequests
    /\ Len(queueEntries) = QueueDepth

Init ==
    /\ queueHead = 0
    /\ queueTail = 0
    /\ queueEntries = <<>>
    /\ notified = 0
    /\ doorbells = 0
    /\ workerState = [w \in 1..MaxWorkers |-> "POLLING"]
    /\ workerServed = [w \in 1..MaxWorkers |-> 0]
    /\ reqEnqueued = 0
    /\ completedSeq = <<>>

\* nxt_app_queue_notify_decision(pos, depth_bound): pos < 0 \/ pos < depth_bound.
\* The position is sampled after the enqueue, so it is the current depth - 1.
Notify(pos) == pos < 0 \/ pos < MaxWorkers

\* The router enqueues a request (nxt_router_app_prepare_request,
\* src/nxt_router.c:8743-8790) and rings according to the producer rule.
EnqueueRequest ==
    /\ reqEnqueued < MaxRequests
    /\ QueueDepth < QueueSize
    /\ LET reqId == reqEnqueued + 1
           pos == queueTail - queueHead
           ring == IF MaxWorkers = 1 THEN notified = 0 ELSE Notify(pos)
       IN
          /\ reqEnqueued' = reqId
          /\ queueTail' = queueTail + 1
          /\ queueEntries' = Append(queueEntries, reqId)
          /\ notified' = IF MaxWorkers = 1 THEN 1 ELSE notified
          /\ doorbells' = IF ring THEN doorbells + 1 ELSE doorbells
          /\ UNCHANGED <<queueHead, workerState, workerServed, completedSeq>>

\* A polling worker wakes on a doorbell or a poll timeout and takes an item;
\* it clears the one-reader flag (nxt_app_queue_notification_received).
WorkerConsume(w) ==
    /\ workerState[w] = "POLLING"
    /\ (doorbells > 0 \/ QueueDepth > 0)
    /\ doorbells' = IF doorbells > 0 THEN doorbells - 1 ELSE 0
    /\ notified' = 0
    /\ IF QueueDepth > 0 THEN
          /\ queueHead' = queueHead + 1
          /\ queueEntries' = Tail(queueEntries)
          /\ completedSeq' = Append(completedSeq, Head(queueEntries))
          /\ workerServed' = [workerServed EXCEPT ![w] = @ + 1]
          /\ workerState' = [workerState EXCEPT ![w] = "ACTIVE"]
       ELSE
          /\ UNCHANGED <<queueHead, queueEntries, completedSeq, workerServed, workerState>>
    /\ UNCHANGED <<queueTail, reqEnqueued>>

\* A worker finishes and polls again.  It rings nobody: libunit checks the
\* queue itself before it sleeps.
WorkerFinish(w) ==
    /\ workerState[w] = "ACTIVE"
    /\ workerState' = [workerState EXCEPT ![w] = "POLLING"]
    /\ UNCHANGED <<queueHead, queueTail, queueEntries, notified, doorbells,
                   workerServed, reqEnqueued, completedSeq>>

Next ==
    \/ EnqueueRequest
    \/ \E w \in 1..MaxWorkers: WorkerConsume(w)
    \/ \E w \in 1..MaxWorkers: WorkerFinish(w)

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

\* Safety: items leave the queue in the order they entered it.
FIFOPreserved ==
    \A i, j \in 1..Len(completedSeq): i < j => completedSeq[i] < completedSeq[j]

\* Safety: the doorbell count never exceeds the depth bound by itself: with
\* MaxWorkers readers at most MaxWorkers datagrams are outstanding per burst.
DoorbellBounded == doorbells <= MaxWorkers + MaxRequests

\* Liveness: all enqueued requests are eventually completed.
Liveness ==
    (reqEnqueued = MaxRequests) ~> (Len(completedSeq) = MaxRequests)

=============================================================================
