"""Deterministic Trace Replay Engine.

Recreates exact concurrent thread interleavings from normalized trace events
inside SimEventLoop to detect and isolate concurrency anomalies.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from unit_model.c_types import HttpHeaderField, UnitRequest, UnitResponse
from unit_model.engine import (
    AppQueue,
    AppWorker,
    Listener,
    Router,
    SharedMemoryPool,
    SimEventLoop,
    UnixSocketPair,
    WorkerState,
)
from unit_model.replay.ingest import TraceEvent


@dataclass
class ReplaySummary:
    """Summary of deterministic replay execution and detected anomalies."""

    total_events: int
    replayed_events: int
    final_clock_ns: int
    streams_completed: int
    anomalies: List[Dict[str, Any]] = field(default_factory=list)
    worker_states: Dict[int, str] = field(default_factory=dict)


class DeterministicReplayer:
    """Replays chronological trace events within the discrete-event simulation model."""

    def __init__(self, num_engines: int = 2, num_workers: int = 2) -> None:
        self.loop = SimEventLoop()
        self.shm_pool = SharedMemoryPool(self.loop, max_segments=4)
        self.listener = Listener(self.loop, num_engines=num_engines)
        self.router = Router(self.loop, self.listener, self.shm_pool, num_engines=num_engines)

        self.workers: Dict[int, AppWorker] = {}
        for w_id in range(num_workers):
            q = AppQueue(self.loop)
            sock = UnixSocketPair(self.loop, name=f"worker_sock_{w_id}")
            w = AppWorker(
                self.loop,
                worker_id=w_id + 100,
                app_id="replay_app",
                app_queue=q,
                socket_pair=sock,
                shm_pool=self.shm_pool,
            )
            self.router.register_worker("replay_app", w_id + 100, q, sock)
            w.on_send_response_to_router = self.router.on_worker_response_msg
            self.workers[w_id + 100] = w

        self.events: List[TraceEvent] = []
        self.completed_streams: Set[int] = set()
        self.detected_anomalies: List[Dict[str, Any]] = []

    def load_events(self, events: List[TraceEvent]) -> None:
        """Load and schedule normalized trace events."""
        self.events = events

    def replay_all(self) -> ReplaySummary:
        """Executes all loaded events in monotonic virtual timestamp order."""
        for ev in self.events:
            self._schedule_event(ev)

        self.loop.run_until_idle()

        # Audit trace log for anomalies (503s, OOSM, crash drops)
        for t in self.loop.trace:
            if "503" in t.action or "oosm" in t.action:
                self.detected_anomalies.append(
                    {
                        "type": "error_response",
                        "time_ns": t.time_ns,
                        "actor": t.actor,
                        "action": t.action,
                        "details": t.details,
                    }
                )
            if "stream_completed" in t.action:
                stream_id = t.details.get("stream")
                if stream_id is not None:
                    self.completed_streams.add(stream_id)

        worker_states = {
            w_id: w.state.name for w_id, w in self.workers.items()
        }

        return ReplaySummary(
            total_events=len(self.events),
            replayed_events=len(self.events),
            final_clock_ns=self.loop.now(),
            streams_completed=len(self.completed_streams),
            anomalies=self.detected_anomalies,
            worker_states=worker_states,
        )

    def _schedule_event(self, ev: TraceEvent) -> None:
        """Translates a TraceEvent into a discrete-event simulator action."""
        delay = max(0, ev.timestamp_ns - self.loop.now())

        if "start" in ev.event_name:
            def on_start():
                conn = self.listener.accept_client("127.0.0.1")
                req = UnitRequest(
                    method=ev.attributes.get("http.method", "GET"),
                    path=ev.attributes.get("http.route", "/"),
                    headers=[],
                )
                self.router.on_listener_request(
                    engine_id=conn.engine_id,
                    conn=conn,
                    req=req,
                    app_id="replay_app",
                )
            self.loop.schedule(delay, "replay_engine", "replay_request_start", on_start)

        elif "crash" in ev.event_name or "sigkill" in ev.event_name:
            def on_crash():
                target_w = ev.worker_id or next(iter(self.workers.keys()))
                if target_w in self.workers:
                    self.workers[target_w].crash_sigkill()
                    self.loop.record_trace("replay_engine", "injected_worker_crash", worker_id=target_w)
            self.loop.schedule(delay, "replay_engine", "replay_worker_crash", on_crash)

        elif "oosm" in ev.event_name:
            def on_oosm():
                # Simulate memory starvation
                self.loop.record_trace("replay_engine", "injected_shm_oosm")
            self.loop.schedule(delay, "replay_engine", "replay_shm_oosm", on_oosm)
