"""FreeUnit Invariant Verification Engine (Model B).

The invariants describe FreeUnit master 42251f7c9e366df31a2e70a5da3679a2fa855b3d:
1. INV-1-HEADER-ONCE: a response header goes to the client exactly once and
   before any body byte (nxt_router_response_ready_handler parses the header
   on the first message and sends it before the body parts,
   src/nxt_router.c:6543-6618; later messages take the r->header_sent branch,
   src/nxt_router.c:6512-6540).  The FIFO order of a stream's parts is the
   same property: body parts are delivered in message order.
2. INV-2-NO-UAF: process teardown leaves no dangling port references
   (issue #195); asserted by the chaos tests, not by this trace walk.
3. INV-3-NO-LEAK: every shared memory chunk is released once the system is
   quiescent.  The worker releases request chunks (src/nxt_unit.c:2621-2627),
   the router releases response chunks after the client write
   (nxt_port_mmap_buf_completion, src/nxt_port_memory.c:167-240).
4. INV-4-PROGRESS: every dispatched stream completes (last message) or fails
   with an error status.
5. INV-5-CONN-FSM: no write to a connection after it closed.
6. INV-6-ISOLATION: framework state does not leak across requests; asserted
   by tests/test_frameworks.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from unit_model.engine.event_loop import TraceRecord
from unit_model.engine.listener import Connection, ConnState
from unit_model.engine.shm_pool import SharedMemoryPool


# Trace actions that write to a client connection.
CLIENT_WRITE_ACTIONS = (
    "response_header_sent",
    "response_body_part",
    "response_flushed",
    "response_sent",
)


@dataclass
class InvariantViolation:
    invariant_id: str
    description: str
    trace_step: Optional[TraceRecord] = None
    context: Dict[str, Any] = field(default_factory=dict)


class InvariantChecker:
    """Verifies architectural invariants across execution traces."""

    def __init__(self, shm_pool: SharedMemoryPool):
        self.shm_pool = shm_pool
        self.violations: List[InvariantViolation] = []

    def check_shm_leak(self) -> bool:
        """Invariant 3: Zero shared-memory leaks."""
        leaked_chunks = 0
        for seg in self.shm_pool.segments.values():
            if seg.free_chunks != seg.total_chunks:
                leaked_chunks += (seg.total_chunks - seg.free_chunks)

        if leaked_chunks > 0:
            self.violations.append(
                InvariantViolation(
                    invariant_id="INV-3-NO-LEAK",
                    description=f"Shared memory leak detected: {leaked_chunks} chunks not freed.",
                    context={"leaked_chunks": leaked_chunks},
                )
            )
            return False
        return True

    def verify_trace(self, trace: List[TraceRecord]) -> List[InvariantViolation]:
        """Analyzes execution trace for safety violations."""
        self.violations.clear()
        active_streams: Set[int] = set()
        completed_streams: Set[int] = set()
        closed_conns: Set[int] = set()
        header_sent: Dict[int, int] = {}      # stream -> count of headers
        body_before_header: Set[int] = set()

        for rec in trace:
            # Invariant 5: no I/O on closed connections.
            if rec.action in CLIENT_WRITE_ACTIONS:
                conn_id = rec.details.get("conn_id")
                if conn_id in closed_conns:
                    self.violations.append(
                        InvariantViolation(
                            invariant_id="INV-5-CONN-FSM",
                            description=f"I/O executed on closed connection {conn_id}.",
                            trace_step=rec,
                        )
                    )

            if rec.action == "conn_closed":
                conn_id = rec.details.get("conn_id")
                if conn_id is not None:
                    closed_conns.add(conn_id)

            # Invariant 1: the header exactly once, before any body byte.
            if rec.action == "response_header_sent":
                stream = rec.details.get("stream")
                if stream is not None:
                    header_sent[stream] = header_sent.get(stream, 0) + 1
                    if header_sent[stream] > 1:
                        self.violations.append(
                            InvariantViolation(
                                invariant_id="INV-1-HEADER-ONCE",
                                description=f"Response header sent twice on stream {stream}.",
                                trace_step=rec,
                            )
                        )

            if rec.action == "response_body_part":
                stream = rec.details.get("stream")
                if stream is not None and header_sent.get(stream, 0) == 0 and stream not in body_before_header:
                    body_before_header.add(stream)
                    self.violations.append(
                        InvariantViolation(
                            invariant_id="INV-1-HEADER-ONCE",
                            description=f"Body bytes sent before the header on stream {stream}.",
                            trace_step=rec,
                        )
                    )

            # Invariant 4: stream completion tracking.
            if rec.action == "route_dispatch":
                stream = rec.details.get("stream")
                if stream is not None:
                    active_streams.add(stream)

            if rec.action in ("stream_completed", "stream_failed", "stream_dropped_conn_closed",
                              "app_not_found_503", "dispatch_failed_oosm"):
                stream = rec.details.get("stream")
                if stream is not None:
                    active_streams.discard(stream)
                    completed_streams.add(stream)

        if active_streams:
            self.violations.append(
                InvariantViolation(
                    invariant_id="INV-4-PROGRESS",
                    description=f"Streams left dangling / uncompleted: {sorted(active_streams)}.",
                    context={"dangling_streams": sorted(active_streams)},
                )
            )

        self.check_shm_leak()
        return self.violations

    def is_clean(self) -> bool:
        return len(self.violations) == 0
