"""Deterministic Discrete-Event Simulation Loop.

Provides simulated time, event scheduling, cooperative multitasking turns,
and complete execution trace capture for FreeUnit Model A & Model B.
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple


class EventPriority(Enum):
    HIGH = 0      # Synchronous signal handlers, immediate CAS completion
    NORMAL = 1    # I/O readiness, socket datagram delivery, memory ops
    TIMER = 2     # Epoll timeouts, application request deadlines
    DEFERRED = 3  # 0ms deferred release timers, idle economy recycling


@dataclass(order=True)
class ScheduledEvent:
    time_ns: int
    priority: int
    sequence: int
    actor: str = field(compare=False)
    action: str = field(compare=False)
    callback: Callable[[], Any] = field(compare=False)
    cancelled: bool = field(default=False, compare=False)


@dataclass
class TraceRecord:
    time_ns: int
    actor: str
    action: str
    details: Dict[str, Any] = field(default_factory=dict)


class SimEventLoop:
    """Deterministic simulated clock and event dispatcher."""

    def __init__(self, time_slice_ns: int = 1_000):
        self.current_time_ns: int = 0
        self.time_slice_ns: int = time_slice_ns
        self._seq: int = 0
        self._heap: List[ScheduledEvent] = []
        self._trace: List[TraceRecord] = []
        self.running: bool = False
        self._step_limit: Optional[int] = None
        self._step_count: int = 0

    def now(self) -> int:
        """Current simulated time in nanoseconds."""
        return self.current_time_ns

    def schedule(
        self,
        delay_ns: int,
        actor: str,
        action: str,
        callback: Callable[[], Any],
        priority: EventPriority = EventPriority.NORMAL,
    ) -> ScheduledEvent:
        """Schedule an action after delay_ns."""
        self._seq += 1
        ev = ScheduledEvent(
            time_ns=self.current_time_ns + max(0, delay_ns),
            priority=priority.value,
            sequence=self._seq,
            actor=actor,
            action=action,
            callback=callback,
        )
        heapq.heappush(self._heap, ev)
        return ev

    def schedule_immediate(
        self,
        actor: str,
        action: str,
        callback: Callable[[], Any],
        priority: EventPriority = EventPriority.HIGH,
    ) -> ScheduledEvent:
        """Schedule an action in the current time slot."""
        return self.schedule(0, actor, action, callback, priority=priority)

    def record_trace(self, actor: str, action: str, **details: Any) -> None:
        """Record an architectural event trace entry."""
        rec = TraceRecord(
            time_ns=self.current_time_ns,
            actor=actor,
            action=action,
            details=details,
        )
        self._trace.append(rec)

    def get_trace(self) -> List[TraceRecord]:
        return list(self._trace)

    def clear_trace(self) -> None:
        self._trace.clear()

    def step(self) -> bool:
        """Execute the next scheduled event. Returns False if heap is empty."""
        while self._heap:
            ev = heapq.heappop(self._heap)
            if ev.cancelled:
                continue

            # Advance clock monotonically
            if ev.time_ns > self.current_time_ns:
                self.current_time_ns = ev.time_ns

            self._step_count += 1
            self.record_trace(ev.actor, ev.action)
            ev.callback()
            return True

        return False

    def run_until_idle(self, max_steps: int = 100_000) -> int:
        """Run all pending events until queue is empty or max_steps reached."""
        self.running = True
        count = 0
        while count < max_steps and self.step():
            count += 1
        self.running = False
        return count

    def run_until_time(self, until_ns: int, max_steps: int = 100_000) -> int:
        """Advance simulation until the specified time."""
        self.running = True
        count = 0
        while count < max_steps and self._heap and self._heap[0].time_ns <= until_ns:
            if not self.step():
                break
            count += 1
        self.current_time_ns = max(self.current_time_ns, until_ns)
        self.running = False
        return count
