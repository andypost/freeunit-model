---------------------------- MODULE FreeUnitNNCQ ----------------------------
EXTENDS Integers

CONSTANTS
    IndexLimit,     \* Modeled as finite integer wrap boundary (e.g. 16 or 32)
    NNCQSize,       \* Slot count (e.g. 4)
    Cycles          \* IndexLimit / NNCQSize (e.g. 4 or 8)

VARIABLES
    head,
    tail,
    entryCycles     \* [slot \in 0..NNCQSize-1 |-> Nat]

vars == <<head, tail, entryCycles>>

Index(i) == i % NNCQSize
Cycle(i) == i \div NNCQSize
NextCycle(c) == (c + 1) % Cycles

Init ==
    /\ head = 0
    /\ tail = 0
    /\ entryCycles = [s \in 0..NNCQSize-1 |-> 0]

Enqueue(t) ==
    /\ tail = t
    /\ (tail - head) < NNCQSize
    /\ LET s == Index(tail)
           c == Cycle(tail)
       IN
          /\ entryCycles' = [entryCycles EXCEPT ![s] = c]
          /\ tail' = (tail + 1) % IndexLimit
          /\ UNCHANGED <<head>>

Dequeue(h) ==
    /\ head = h
    /\ head /= tail
    /\ LET s == Index(head)
           c == Cycle(head)
       IN
          /\ entryCycles[s] = c
          /\ head' = (head + 1) % IndexLimit
          /\ UNCHANGED <<tail, entryCycles>>

Next ==
    \/ \E t \in 0..IndexLimit-1: Enqueue(t)
    \/ \E h \in 0..IndexLimit-1: Dequeue(h)

\* Invariant: No slot entry cycle is ever in an impossible cycle state
NoCycleCollision ==
    \A s \in 0..NNCQSize-1:
        entryCycles[s] < Cycles

=============================================================================
