"""Shared-Memory App Queue (nxt_app_queue_t & nxt_app_nncq_t) Simulation.

Faithfully models:
- Lockless ring buffer circular queue of 131,072 entries.
- Per-item payload budget capped at 31 bytes (NXT_APP_QUEUE_MSG_SIZE).
- Atomic doorbell notification suppression (q->notified cmp_set flag).
- Request state machine: UNQUEUED -> QUEUED -> SOCKET.
- Inversion barriers and queue draining before socket fallback.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple

from unit_model.c_types import (
    NXT_APP_QUEUE_MSG_SIZE,
    NXT_APP_QUEUE_SIZE,
    QueueItem,
)
from unit_model.engine.event_loop import SimEventLoop


class QueueItemState(Enum):
    UNQUEUED = auto()
    QUEUED = auto()
    SOCKET = auto()
    DELIVERED = auto()


@dataclass
class SimulatedQueueSlot:
    index: int
    data: bytes = b""
    tracking: int = 0
    state: QueueItemState = QueueItemState.UNQUEUED


class AppQueue:
    """Simulation of FreeUnit's nxt_app_queue_t."""

    def __init__(self, loop: SimEventLoop, capacity: int = NXT_APP_QUEUE_SIZE):
        self.loop = loop
        self.capacity = capacity
        # We model the ring buffer
        self.slots: List[SimulatedQueueSlot] = [
            SimulatedQueueSlot(index=i) for i in range(capacity)
        ]
        self.free_list: List[int] = list(range(capacity))
        self.queue: List[int] = []
        self.notified: int = 0
        self.total_enqueued: int = 0
        self.total_dequeued: int = 0
        self.total_doorbells_sent: int = 0
        self.total_doorbells_suppressed: int = 0

    @staticmethod
    def notify_decision(pos: int, depth_bound: int) -> bool:
        """nxt_app_queue_notify_decision parity:
        pos < 0 (head advanced past producer) or pos < depth_bound.
        """
        return pos < 0 or pos < max(1, depth_bound)

    def send_bound(
        self, data: bytes, tracking: int, depth_bound: int = 1
    ) -> Tuple[bool, bool, int]:
        """nxt_app_queue_send_bound simulation.
        
        Args:
            data: Payload bytes (capped at NXT_APP_QUEUE_MSG_SIZE = 31 bytes).
            tracking: Request stream tracking ID.
            depth_bound: Doorbell bound (max(processes * threads, registered contexts)).
                         When depth_bound == 1 (single worker/thread), notifications
                         are coalesced on the `notified` flag 0->1 transition.
                         When depth_bound > 1, notification is decided by queue position.

        Returns:
            (success, need_notify, cookie)
        """
        if len(data) > NXT_APP_QUEUE_MSG_SIZE:
            self.loop.record_trace("app_queue", "send_oversized_rejected", size=len(data))
            return False, False, -1

        if not self.free_list:
            self.loop.record_trace("app_queue", "queue_full_again")
            return False, False, -1

        slot_idx = self.free_list.pop(0)
        slot = self.slots[slot_idx]
        slot.data = data
        slot.tracking = tracking
        slot.state = QueueItemState.QUEUED

        self.queue.append(slot_idx)
        self.total_enqueued += 1

        need_notify = False
        if depth_bound == 1:
            # One reader: coalesce on the flag, ring only on 0->1 transition
            if self.notified == 0:
                self.notified = 1
                need_notify = True
                self.total_doorbells_sent += 1
            else:
                self.total_doorbells_suppressed += 1
        else:
            # Multi-reader / threads: position relative to queue head
            pos = len(self.queue) - 1
            need_notify = self.notify_decision(pos, depth_bound)
            if need_notify:
                self.total_doorbells_sent += 1
            else:
                self.total_doorbells_suppressed += 1

        self.loop.record_trace(
            "app_queue",
            "enqueued",
            slot=slot_idx,
            tracking=tracking,
            depth_bound=depth_bound,
            need_notify=need_notify,
        )
        return True, need_notify, slot_idx

    def send(self, data: bytes, tracking: int) -> Tuple[bool, bool, int]:
        """Legacy send simulation (defaults to depth_bound=1)."""
        return self.send_bound(data, tracking, depth_bound=1)

    def recv(self) -> Optional[QueueItem]:
        """nxt_app_queue_recv simulation.
        
        Faithfully simulates queue draining and automatic re-arming of the
        one-reader doorbell (`notified` flag reset to 0) when the queue is emptied,
        preventing sticky doorbell deadlocks if a worker dequeues without reading
        the doorbell or dies abruptly.
        """
        if not self.queue:
            return None

        slot_idx = self.queue.pop(0)
        slot = self.slots[slot_idx]
        item = QueueItem(data=slot.data, tracking=slot.tracking)

        slot.state = QueueItemState.DELIVERED
        slot.data = b""
        self.free_list.append(slot_idx)
        self.total_dequeued += 1

        # Re-arm the one-reader doorbell once the queue is drained
        if len(self.queue) == 0 and self.notified == 1:
            self.notified = 0
            self.loop.record_trace("app_queue", "queue_drained_rearmed_doorbell")

        self.loop.record_trace(
            "app_queue",
            "dequeued",
            slot=slot_idx,
            tracking=item.tracking,
            remaining_depth=len(self.queue),
        )
        return item

    def notification_received(self) -> None:
        """nxt_app_queue_notification_received: worker clears notified flag."""
        self.notified = 0
        self.loop.record_trace("app_queue", "notification_cleared")

    def is_empty(self) -> bool:
        return len(self.queue) == 0

    def pending_count(self) -> int:
        return len(self.queue)
